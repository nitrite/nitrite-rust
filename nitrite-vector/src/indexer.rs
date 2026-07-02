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
    /// Default parameters for indexes without a dedicated config.
    params: VectorIndexConfig,
    /// Per-index overrides keyed by `(collection, field)`, so collections with
    /// different embedding dimensions/metrics can coexist in one database.
    /// Immutable after module construction — no lock needed.
    per_index: HashMap<(String, String), VectorIndexConfig>,
}

impl VectorIndexer {
    /// Creates a new indexer whose new indexes use the given parameters.
    pub fn new(params: VectorIndexConfig) -> Self {
        VectorIndexer {
            inner: Arc::new(VectorIndexerInner {
                registry: RwLock::new(HashMap::new()),
                params,
                per_index: HashMap::new(),
            }),
        }
    }

    /// Creates an indexer with a default config plus per-`(collection, field)`
    /// overrides (see [`crate::VectorModuleBuilder::index_config`]).
    pub fn with_configs(
        params: VectorIndexConfig,
        per_index: HashMap<(String, String), VectorIndexConfig>,
    ) -> Self {
        VectorIndexer {
            inner: Arc::new(VectorIndexerInner {
                registry: RwLock::new(HashMap::new()),
                params,
                per_index,
            }),
        }
    }

    /// The config for a given index: the per-index override if one was
    /// registered, else the module default. (For an existing index the
    /// persisted header still wins on structural parameters.)
    fn params_for(&self, descriptor: &IndexDescriptor) -> VectorIndexConfig {
        let collection = descriptor.collection_name();
        let field = descriptor.index_fields().field_names().join("_");
        self.inner
            .per_index
            .get(&(collection, field))
            .copied()
            .unwrap_or(self.inner.params)
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
        // Double-checked under the write lock: two racing opens must not
        // create two live instances over the same storage (two divergent HNSW
        // graphs, or two mutable mmaps of the same DiskANN file).
        let mut registry = self.inner.registry.write();
        if let Some(index) = registry.get(&name) {
            return Ok(index.clone());
        }
        let index = VectorIndex::open(descriptor, config, &self.params_for(descriptor))?;
        registry.insert(name, index.clone());
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
            None => VectorIndex::open(
                index_descriptor,
                nitrite_config,
                &self.params_for(index_descriptor),
            )?,
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
        // A score cutoff discards hits after the fact, so over-fetch to still
        // return up to k results that clear it.
        let fetch = if knn.min_score().is_some() {
            knn.k().saturating_mul(4)
        } else {
            knn.k()
        };
        let hits = index.search(knn.query(), fetch, knn.ef())?;

        let ids = hits
            .into_iter()
            .filter(|(_, dist)| match knn.min_score() {
                Some(min) => metric.score(*dist) >= min,
                None => true,
            })
            .map(|(id, _)| id)
            .take(knn.k())
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
