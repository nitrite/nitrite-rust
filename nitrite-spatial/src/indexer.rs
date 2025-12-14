//! Spatial indexer implementation for Nitrite.
//!
//! This module provides the `SpatialIndexer` that integrates with Nitrite's
//! plugin system to enable spatial indexing on collections.
//!
//! ## Two-Phase Query Execution
//!
//! The spatial indexer uses a two-phase approach for accurate queries:
//!
//! 1. **Phase 1 (R-tree bounding box search)**: Fast but may include false positives
//!    due to bounding box approximation. The R-tree stores bounding boxes, not
//!    exact geometries.
//!
//! 2. **Phase 2 (Geometry refinement)**: Precise geometric operations eliminate
//!    false positives. This phase retrieves the actual geometry from each
//!    candidate document and applies the exact spatial predicate.
//!
//! This approach balances performance (fast R-tree lookup) with accuracy
//! (precise geometry matching).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use nitrite::collection::{FindPlan, NitriteId};
use nitrite::common::{FieldValues, Fields, NitritePlugin, NitritePluginProvider};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::{IndexDescriptor, NitriteIndexerProvider};
use nitrite::nitrite_config::NitriteConfig;

use crate::index::{SpatialIndex, derive_index_map_name};
use crate::filter::{SPATIAL_INDEX};

/// The spatial indexer that manages spatial indexes in Nitrite.
///
/// This indexer uses an R-tree data structure for efficient spatial queries.
/// It supports:
/// - Point and geometry storage
/// - Intersection queries
/// - Containment queries
/// - Proximity queries (near/geoNear)
#[derive(Clone)]
pub struct SpatialIndexer {
    inner: Arc<SpatialIndexerInner>,
}

struct SpatialIndexerInner {
    index_registry: RwLock<HashMap<String, SpatialIndex>>,
    base_path: RwLock<Option<PathBuf>>,
    in_memory: AtomicBool,
}

impl SpatialIndexer {
    /// Creates a new spatial indexer.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SpatialIndexerInner {
                index_registry: RwLock::new(HashMap::new()),
                base_path: RwLock::new(None),
                in_memory: AtomicBool::new(false),
            }),
        }
    }

    /// Sets the base path for index storage.
    pub fn set_base_path(&self, path: PathBuf) {
        if let Ok(mut base_path) = self.inner.base_path.write() {
            *base_path = Some(path);
        }
    }

    fn get_or_create_index(
        &self,
        index_descriptor: &IndexDescriptor,
    ) -> NitriteResult<SpatialIndex> {
        let index_name = derive_index_map_name(index_descriptor);
        
        // Check if index already exists
        {
            let registry = self.inner.index_registry.read().map_err(|_| {
                NitriteError::new("Lock poisoned", ErrorKind::InternalError)
            })?;
            if let Some(index) = registry.get(&index_name) {
                return Ok(index.clone());
            }
        }

        // Create new index
        let base_path = self.inner.base_path.read().map_err(|_| {
            NitriteError::new("Lock poisoned", ErrorKind::InternalError)
        })?;
        
        let index = if self.inner.in_memory.load(std::sync::atomic::Ordering::Relaxed) {
            SpatialIndex::new(index_descriptor.clone(), None)?
        } else {
            SpatialIndex::new(index_descriptor.clone(), base_path.clone())?
        };
        
        // Store in registry
        {
            let mut registry = self.inner.index_registry.write().map_err(|_| {
                NitriteError::new("Lock poisoned", ErrorKind::InternalError)
            })?;
            registry.insert(index_name, index.clone());
        }
        
        Ok(index)
    }
}

impl Default for SpatialIndexer {
    fn default() -> Self {
        Self::new()
    }
}

impl NitriteIndexerProvider for SpatialIndexer {
    fn index_type(&self) -> String {
        SPATIAL_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        false // Spatial indexes are not unique
    }

    fn validate_index(&self, fields: &Fields) -> NitriteResult<()> {
        if fields.field_names().len() > 1 {
            return Err(NitriteError::new(
                "Spatial index can only be created on a single field",
                ErrorKind::IndexingError,
            ));
        }
        Ok(())
    }

    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let index_name = derive_index_map_name(index_descriptor);
        
        // Remove from registry
        let index = {
            let mut registry = self.inner.index_registry.write().map_err(|_| {
                NitriteError::new("Lock poisoned", ErrorKind::InternalError)
            })?;
            registry.remove(&index_name)
        };
        
        // Drop the index
        if let Some(idx) = index {
            idx.drop()?;
        }
        
        Ok(())
    }

    fn write_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let index = self.get_or_create_index(index_descriptor)?;
        index.write(field_values)
    }

    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let index = self.get_or_create_index(index_descriptor)?;
        index.remove(field_values)
    }

    fn find_by_filter(
        &self,
        find_plan: &FindPlan,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let index_descriptor = find_plan.index_descriptor().ok_or_else(|| {
            NitriteError::new("No index descriptor in find plan", ErrorKind::FilterError)
        })?;
        
        let index = self.get_or_create_index(&index_descriptor)?;
        index.find_nitrite_ids(find_plan, nitrite_config)
    }
}

impl NitritePluginProvider for SpatialIndexer {
    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        // Set base path from config if available
        // Use the full db_path (database directory) for storing rtree files alongside the database
        if let Some(path) = config.db_path() {
            self.set_base_path(std::path::PathBuf::from(&path));
            self.inner.in_memory.store(false, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        // Close all indexes
        let registry = self.inner.index_registry.read().map_err(|_| {
            NitriteError::new("Lock poisoned", ErrorKind::InternalError)
        })?;
        
        for index in registry.values() {
            index.close()?;
        }
        
        Ok(())
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(SpatialIndexer::new())
    }
}


#[cfg(test)]
mod tests {
    use nitrite::common::NitriteModule;

    use crate::SpatialModule;

    use super::*;

    #[test]
    fn test_spatial_indexer_index_type() {
        let indexer = SpatialIndexer::new();
        assert_eq!(indexer.index_type(), SPATIAL_INDEX);
    }

    #[test]
    fn test_spatial_indexer_is_not_unique() {
        let indexer = SpatialIndexer::new();
        assert!(!indexer.is_unique());
    }

    #[test]
    fn test_spatial_indexer_validate_single_field() {
        let indexer = SpatialIndexer::new();
        let fields = Fields::with_names(vec!["location"]).unwrap();
        assert!(indexer.validate_index(&fields).is_ok());
    }

    #[test]
    fn test_spatial_indexer_validate_multi_field_fails() {
        let indexer = SpatialIndexer::new();
        let fields = Fields::with_names(vec!["lat", "lon"]).unwrap();
        assert!(indexer.validate_index(&fields).is_err());
    }

    #[test]
    fn test_spatial_module_plugins() {
        let module = SpatialModule;
        let plugins = module.plugins().unwrap();
        assert_eq!(plugins.len(), 1);
    }
}