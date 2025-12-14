//! FTS indexer implementation for Nitrite.
//!
//! This module provides the `FtsIndexer` that integrates with Nitrite's
//! plugin system to enable full-text search indexing on collections.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use nitrite::collection::{FindPlan, NitriteId};
use nitrite::common::{FieldValues, Fields, NitritePlugin, NitritePluginProvider};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::{IndexDescriptor, NitriteIndexerProvider};
use nitrite::nitrite_config::NitriteConfig;

use crate::config::FtsConfig;
use crate::filter::{is_fts_filter, FTS_INDEX};
use crate::index::{derive_index_map_name, FtsIndex};

/// The FTS indexer that manages full-text search indexes in Nitrite.
///
/// This indexer uses Tantivy for efficient full-text search. It supports:
/// - Text field indexing with tokenization
/// - Term and phrase queries
/// - Fuzzy matching
/// - BM25 ranking
#[derive(Clone)]
pub struct FtsIndexer {
    inner: Arc<FtsIndexerInner>,
}

struct FtsIndexerInner {
    index_registry: RwLock<HashMap<String, FtsIndex>>,
    base_path: RwLock<Option<PathBuf>>,
    in_memory: AtomicBool,
    config: FtsConfig,
}

impl FtsIndexer {
    /// Creates a new FTS indexer with default configuration.
    pub fn new() -> Self {
        Self::with_config(FtsConfig::default())
    }

    /// Creates a new FTS indexer with the given configuration.
    pub fn with_config(config: FtsConfig) -> Self {
        Self {
            inner: Arc::new(FtsIndexerInner {
                index_registry: RwLock::new(HashMap::new()),
                base_path: RwLock::new(None),
                in_memory: AtomicBool::new(true),
                config,
            }),
        }
    }

    /// Returns the configuration for this indexer.
    #[inline]
    pub fn config(&self) -> &FtsConfig {
        &self.inner.config
    }

    /// Sets the base path for index storage.
    pub fn set_base_path(&self, path: PathBuf) {
        if let Ok(mut base_path) = self.inner.base_path.write() {
            *base_path = Some(path);
        }
    }

    fn get_or_create_index(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<FtsIndex> {
        let index_name = derive_index_map_name(index_descriptor);

        // Check if index already exists
        {
            let registry = self
                .inner
                .index_registry
                .read()
                .map_err(|_| NitriteError::new("Lock poisoned", ErrorKind::InternalError))?;
            if let Some(index) = registry.get(&index_name) {
                return Ok(index.clone());
            }
        }

        // Create new index
        let base_path = self
            .inner
            .base_path
            .read()
            .map_err(|_| NitriteError::new("Lock poisoned", ErrorKind::InternalError))?;

        let index = if self
            .inner
            .in_memory
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            FtsIndex::new(index_descriptor.clone(), None, &self.inner.config)?
        } else {
            FtsIndex::new(
                index_descriptor.clone(),
                base_path.clone(),
                &self.inner.config,
            )?
        };

        // Store in registry
        {
            let mut registry = self
                .inner
                .index_registry
                .write()
                .map_err(|_| NitriteError::new("Lock poisoned", ErrorKind::InternalError))?;
            registry.insert(index_name, index.clone());
        }

        Ok(index)
    }
}

impl Default for FtsIndexer {
    fn default() -> Self {
        Self::new()
    }
}

impl NitriteIndexerProvider for FtsIndexer {
    fn index_type(&self) -> String {
        FTS_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        false // FTS indexes are not unique
    }

    fn validate_index(&self, fields: &Fields) -> NitriteResult<()> {
        if fields.field_names().len() > 1 {
            return Err(NitriteError::new(
                "FTS index can only be created on a single text field",
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
            let mut registry = self
                .inner
                .index_registry
                .write()
                .map_err(|_| NitriteError::new("Lock poisoned", ErrorKind::InternalError))?;
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
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let index_descriptor = find_plan.index_descriptor().ok_or_else(|| {
            NitriteError::new("No index descriptor in find plan", ErrorKind::FilterError)
        })?;

        let index = self.get_or_create_index(&index_descriptor)?;

        // Get the filter from find plan
        let index_scan_filter = find_plan
            .index_scan_filter()
            .ok_or_else(|| NitriteError::new("No FTS filter found", ErrorKind::FilterError))?;

        let filters = index_scan_filter.filters();

        if filters.is_empty() {
            return Err(NitriteError::new(
                "No FTS filter found",
                ErrorKind::FilterError,
            ));
        }

        let filter = &filters[0];

        if !is_fts_filter(filter) {
            return Err(NitriteError::new(
                "FTS filter must be the first filter for index scan",
                ErrorKind::FilterError,
            ));
        }

        index.find_nitrite_ids(find_plan)
    }
}

impl NitritePluginProvider for FtsIndexer {
    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        // Set base path from config if available
        if let Some(path) = config.db_path() {
            self.set_base_path(std::path::PathBuf::from(&path));
            self.inner
                .in_memory
                .store(false, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        // Close all indexes
        let registry = self
            .inner
            .index_registry
            .read()
            .map_err(|_| NitriteError::new("Lock poisoned", ErrorKind::InternalError))?;

        for index in registry.values() {
            index.close()?;
        }

        Ok(())
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(FtsIndexer::with_config(self.inner.config.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nitrite::common::Value;

    #[test]
    fn test_fts_indexer_new() {
        let indexer = FtsIndexer::new();
        assert_eq!(indexer.index_type(), FTS_INDEX);
    }

    #[test]
    fn test_fts_indexer_default() {
        let indexer = FtsIndexer::default();
        assert_eq!(indexer.index_type(), FTS_INDEX);
    }

    #[test]
    fn test_fts_indexer_with_config() {
        let config = FtsConfig::new()
            .with_index_writer_heap_size(100 * 1024 * 1024)
            .with_num_threads(4);
        let indexer = FtsIndexer::with_config(config);
        assert_eq!(indexer.config().index_writer_heap_size(), 100 * 1024 * 1024);
        assert_eq!(indexer.config().num_threads(), 4);
    }

    #[test]
    fn test_fts_indexer_index_type() {
        let indexer = FtsIndexer::new();
        assert_eq!(indexer.index_type(), "TantivyFullText");
    }

    #[test]
    fn test_fts_indexer_is_not_unique() {
        let indexer = FtsIndexer::new();
        assert!(!indexer.is_unique());
    }

    #[test]
    fn test_fts_indexer_clone() {
        let indexer = FtsIndexer::new();
        let cloned = indexer.clone();
        assert_eq!(cloned.index_type(), indexer.index_type());
    }

    // ===== Validation Tests =====

    #[test]
    fn test_fts_indexer_validate_single_field() {
        let indexer = FtsIndexer::new();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        assert!(indexer.validate_index(&fields).is_ok());
    }

    #[test]
    fn test_fts_indexer_validate_empty_field() {
        let indexer = FtsIndexer::new();
        let fields = Fields::with_names(vec!["title"]).unwrap();
        assert!(indexer.validate_index(&fields).is_ok());
    }

    #[test]
    fn test_fts_indexer_validate_multi_field_fails() {
        let indexer = FtsIndexer::new();
        let fields = Fields::with_names(vec!["title", "content"]).unwrap();
        let result = indexer.validate_index(&fields);
        assert!(result.is_err());
    }

    #[test]
    fn test_fts_indexer_validate_three_fields_fails() {
        let indexer = FtsIndexer::new();
        let fields = Fields::with_names(vec!["title", "content", "body"]).unwrap();
        let result = indexer.validate_index(&fields);
        assert!(result.is_err());
    }

    // ===== Base Path Tests =====

    #[test]
    fn test_fts_indexer_set_base_path() {
        let indexer = FtsIndexer::new();
        let temp_dir = tempfile::tempdir().unwrap();
        indexer.set_base_path(temp_dir.path().to_path_buf());
        // No assertion needed - just verify it doesn't panic
    }

    #[test]
    fn test_fts_indexer_set_base_path_multiple_times() {
        let indexer = FtsIndexer::new();
        let temp_dir1 = tempfile::tempdir().unwrap();
        let temp_dir2 = tempfile::tempdir().unwrap();

        indexer.set_base_path(temp_dir1.path().to_path_buf());
        indexer.set_base_path(temp_dir2.path().to_path_buf());
        // Should succeed without issues
    }

    // ===== Plugin Provider Tests =====

    #[test]
    fn test_fts_indexer_close() {
        let indexer = FtsIndexer::new();
        assert!(indexer.close().is_ok());
    }

    #[test]
    fn test_fts_indexer_as_plugin() {
        let indexer = FtsIndexer::new();
        let _plugin = indexer.as_plugin();
        // Just verify it doesn't panic
    }

    // ===== Index Registry Tests =====

    #[test]
    fn test_fts_indexer_get_or_create_index() {
        let indexer = FtsIndexer::new();
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor = IndexDescriptor::new("TantivyFullText", fields, &format!("test_{}", uuid));

        // First call creates the index
        let index1 = indexer.get_or_create_index(&descriptor);
        assert!(index1.is_ok());

        // Second call returns the same index
        let index2 = indexer.get_or_create_index(&descriptor);
        assert!(index2.is_ok());
    }

    #[test]
    fn test_fts_indexer_multiple_indexes() {
        let indexer = FtsIndexer::new();

        let fields1 = Fields::with_names(vec!["content"]).unwrap();
        let desc1 = IndexDescriptor::new("TantivyFullText", fields1, "collection1");

        let fields2 = Fields::with_names(vec!["body"]).unwrap();
        let desc2 = IndexDescriptor::new("TantivyFullText", fields2, "collection2");

        let index1 = indexer.get_or_create_index(&desc1);
        let index2 = indexer.get_or_create_index(&desc2);

        assert!(index1.is_ok());
        assert!(index2.is_ok());
    }

    // ===== Write and Remove Tests =====

    #[test]
    fn test_fts_indexer_write_index_entry() {
        let indexer = FtsIndexer::new();
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor =
            IndexDescriptor::new("TantivyFullText", fields.clone(), &format!("test_{}", uuid));
        let config = NitriteConfig::default();

        let nitrite_id = NitriteId::new();
        let field_values = FieldValues::new(
            vec![(
                "content".to_string(),
                Value::String("hello world".to_string()),
            )],
            nitrite_id,
            fields,
        );

        let result = indexer.write_index_entry(&field_values, &descriptor, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fts_indexer_remove_index_entry() {
        let indexer = FtsIndexer::new();
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor =
            IndexDescriptor::new("TantivyFullText", fields.clone(), &format!("test_{}", uuid));
        let config = NitriteConfig::default();

        let nitrite_id = NitriteId::new();
        let field_values = FieldValues::new(
            vec![(
                "content".to_string(),
                Value::String("hello world".to_string()),
            )],
            nitrite_id,
            fields,
        );

        // First write, then remove
        indexer
            .write_index_entry(&field_values, &descriptor, &config)
            .unwrap();
        let result = indexer.remove_index_entry(&field_values, &descriptor, &config);
        assert!(result.is_ok());
    }

    // ===== Drop Index Tests =====

    #[test]
    fn test_fts_indexer_drop_index() {
        let indexer = FtsIndexer::new();
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor = IndexDescriptor::new("TantivyFullText", fields, &format!("test_{}", uuid));
        let config = NitriteConfig::default();

        // Create the index first
        indexer.get_or_create_index(&descriptor).unwrap();

        // Drop it
        let result = indexer.drop_index(&descriptor, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fts_indexer_drop_nonexistent_index() {
        let indexer = FtsIndexer::new();
        let uuid = uuid::Uuid::new_v4();
        let fields = Fields::with_names(vec!["content"]).unwrap();
        let descriptor = IndexDescriptor::new("TantivyFullText", fields, &format!("test_{}", uuid));
        let config = NitriteConfig::default();

        // Dropping a nonexistent index should succeed (no-op)
        let result = indexer.drop_index(&descriptor, &config);
        assert!(result.is_ok());
    }
}
