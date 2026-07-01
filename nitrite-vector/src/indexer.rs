//! The vector indexer: implements Nitrite's `NitriteIndexerProvider` so an
//! HNSW index can be created, maintained, and queried through the standard
//! collection API.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use nitrite::collection::{FindPlan, NitriteId};
use nitrite::common::{FieldValues, Fields, NitritePlugin, NitritePluginProvider};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::{IndexDescriptor, NitriteIndexerProvider};
use nitrite::nitrite_config::NitriteConfig;

use crate::filter::{value_to_vector, VectorNearestFilter, VECTOR_INDEX};
use crate::vector_index::{derive_vector_map_name, VectorIndex, VectorIndexConfig};

/// Indexer that manages HNSW vector indexes for a Nitrite database.
#[derive(Clone)]
pub struct VectorIndexer {
    inner: Arc<VectorIndexerInner>,
}

struct VectorIndexerInner {
    registry: RwLock<HashMap<String, VectorIndex>>,
    params: VectorIndexConfig,
}

impl VectorIndexer {
    /// Creates a new indexer whose new indexes use the given parameters.
    pub fn new(params: VectorIndexConfig) -> Self {
        VectorIndexer {
            inner: Arc::new(VectorIndexerInner {
                registry: RwLock::new(HashMap::new()),
                params,
            }),
        }
    }

    fn get_or_open(
        &self,
        descriptor: &IndexDescriptor,
        config: &NitriteConfig,
    ) -> NitriteResult<VectorIndex> {
        let name = derive_vector_map_name(descriptor);
        if let Some(index) = self.inner.registry.read().get(&name) {
            return Ok(index.clone());
        }
        let index = VectorIndex::open(descriptor, config, &self.inner.params)?;
        self.inner.registry.write().insert(name, index.clone());
        Ok(index)
    }

    fn field_vector(
        &self,
        field_values: &FieldValues,
    ) -> Option<(u64, Vec<f32>)> {
        let field_names = field_values.fields().field_names();
        let field = field_names.first()?;
        let value = field_values.get_value(field)?;
        let vector = value_to_vector(value)?;
        Some((field_values.nitrite_id().id_value(), vector))
    }
}

impl NitriteIndexerProvider for VectorIndexer {
    fn index_type(&self) -> String {
        VECTOR_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        false
    }

    fn validate_index(&self, fields: &Fields) -> NitriteResult<()> {
        if fields.field_names().len() != 1 {
            return Err(NitriteError::new(
                "Vector index can only be created on a single field",
                ErrorKind::IndexingError,
            ));
        }
        Ok(())
    }

    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let base = derive_vector_map_name(index_descriptor);
        let removed = self.inner.registry.write().remove(&base);
        let index = match removed {
            Some(index) => index,
            None => VectorIndex::open(index_descriptor, nitrite_config, &self.inner.params)?,
        };
        index.destroy(&base, nitrite_config)
    }

    fn write_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        // No vector on the document for this field: nothing to index.
        let Some((id, vector)) = self.field_vector(field_values) else {
            return Ok(());
        };
        let index = self.get_or_open(index_descriptor, nitrite_config)?;
        index.insert(id, vector)
    }

    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let index = self.get_or_open(index_descriptor, nitrite_config)?;
        index.remove(field_values.nitrite_id().id_value())
    }

    fn find_by_filter(
        &self,
        find_plan: &FindPlan,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let descriptor = find_plan.index_descriptor().ok_or_else(|| {
            NitriteError::new("No index descriptor in find plan", ErrorKind::FilterError)
        })?;
        let scan = find_plan.index_scan_filter().ok_or_else(|| {
            NitriteError::new("No vector filter in find plan", ErrorKind::FilterError)
        })?;
        let filters = scan.filters();
        let filter = filters.first().ok_or_else(|| {
            NitriteError::new("Empty vector index scan", ErrorKind::FilterError)
        })?;
        let knn = filter
            .as_any()
            .downcast_ref::<VectorNearestFilter>()
            .ok_or_else(|| {
                NitriteError::new(
                    "Vector index requires a nearest-neighbour filter",
                    ErrorKind::FilterError,
                )
            })?;

        let index = self.get_or_open(&descriptor, nitrite_config)?;
        let metric = index.metric();
        let hits = index.search(knn.query(), knn.k(), knn.ef())?;

        let ids = hits
            .into_iter()
            .filter(|(_, dist)| match knn.min_score() {
                Some(min) => metric.score(*dist) >= min,
                None => true,
            })
            .map(|(id, _)| id)
            .collect();
        Ok(ids)
    }
}

impl NitritePluginProvider for VectorIndexer {
    fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
        // Indexes are opened lazily against the config passed to each call.
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        // Flush any disk-resident indexes (writes the DiskANN sidecar) before
        // dropping the in-memory registry.
        let mut registry = self.inner.registry.write();
        for index in registry.values() {
            index.flush()?;
        }
        registry.clear();
        Ok(())
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(self.clone())
    }
}
