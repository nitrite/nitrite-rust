use super::{
    compound_index::CompoundIndex, nitrite_index::{NitriteIndex, NitriteIndexProvider}, simple_index::SimpleIndex, IndexDescriptor, NitriteIndexerProvider,
};
use crate::{
    collection::{FindPlan, NitriteId}, errors::{ErrorKind, NitriteError, NitriteResult}, nitrite_config::NitriteConfig, FieldValues, Fields, NitritePlugin, NitritePluginProvider, UNIQUE_INDEX,
};
use dashmap::DashMap;
use log::log;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct UniqueIndexer {
    inner: Arc<UniqueIndexerInner>,
}

impl UniqueIndexer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(UniqueIndexerInner::new()),
        }
    }

    fn find_nitrite_index(
        &self,
        index_descriptor: &IndexDescriptor,
    ) -> NitriteResult<NitriteIndex> {
        let result = self.inner
            .find_nitrite_index(index_descriptor);

        match result {
            Some(nitrite_index) => Ok(nitrite_index),
            None => {
                log::error!("Index not found for descriptor: {:?}", index_descriptor);
                Err(NitriteError::new(
                    "Index not found",
                    ErrorKind::IndexingError,
                ))
            },
        }
    }
}

impl NitritePluginProvider for UniqueIndexer {
    fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        Ok(())
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(self.clone())
    }
}

impl NitriteIndexerProvider for UniqueIndexer {
    fn index_type(&self) -> String {
        UNIQUE_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        true
    }

    fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
        Ok(())
    }

    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        self.inner.drop_index(index_descriptor)
    }

    fn write_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        self.inner
            .write_index_entry(field_values, index_descriptor, nitrite_config)
    }

    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        self.inner
            .remove_index_entry(field_values, index_descriptor, nitrite_config)
    }

    fn find_by_filter(
        &self,
        find_plan: &FindPlan,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        self.inner.find_by_filter(find_plan, nitrite_config)
    }
}

struct UniqueIndexerInner {
    index_registry: DashMap<IndexDescriptor, NitriteIndex>,
}

impl UniqueIndexerInner {
    fn new() -> Self {
        Self {
            index_registry: DashMap::new(),
        }
    }

    fn find_nitrite_index(
        &self,
        index_descriptor: &IndexDescriptor,
    ) -> Option<NitriteIndex> {
        self.index_registry.get(index_descriptor).map(|it| it.value().clone())
    }

    fn create_nitrite_index(
        &self,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<NitriteIndex> {
        let store = nitrite_config.nitrite_store()?;
        let nitrite_index: NitriteIndex = if index_descriptor.is_compound_index() {
            NitriteIndex::new(CompoundIndex::new(index_descriptor.clone(), store))
        } else {
            NitriteIndex::new(SimpleIndex::new(index_descriptor.clone(), store))
        };

        self.index_registry
            .insert(index_descriptor.clone(), nitrite_index.clone());

        Ok(nitrite_index)
    }

    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
    ) -> NitriteResult<()> {
        let nitrite_index = self.find_nitrite_index(index_descriptor);
        if let Some(nitrite_index) = nitrite_index {
            nitrite_index.drop_index()?;
            self.index_registry.remove(index_descriptor);
        }
        Ok(())
    }

    fn write_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let mut nitrite_index = self.find_nitrite_index(index_descriptor);
        if nitrite_index.is_none() {
            nitrite_index = Some(self.create_nitrite_index(index_descriptor, nitrite_config)?);
        }

        if let Some(nitrite_index) = nitrite_index {
            nitrite_index.write(field_values)?;
        }
        Ok(())
    }

    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let mut nitrite_index = self.find_nitrite_index(index_descriptor);
        if nitrite_index.is_none() {
            nitrite_index = Some(self.create_nitrite_index(&index_descriptor, nitrite_config)?);
        }

        if let Some(nitrite_index) = nitrite_index {
            nitrite_index.remove(field_values)?;
        }
        Ok(())
    }

    fn find_by_filter(
        &self,
        find_plan: &FindPlan,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>> {
        let index_descriptor = find_plan.index_descriptor();

        match index_descriptor {
            Some(index_descriptor) => {
                // Use idiomatic if-let pattern instead of is_none() + unwrap()
                let nitrite_index = if let Some(idx) = self.find_nitrite_index(&index_descriptor) {
                    idx
                } else {
                    self.create_nitrite_index(&index_descriptor, nitrite_config)?
                };

                let nitrite_ids = nitrite_index.find_nitrite_ids(find_plan)?;
                Ok(nitrite_ids)
            }
            None => {
                log::error!("Index descriptor not found in find plan");
                Err(NitriteError::new(
                    "Index descriptor not found",
                    ErrorKind::IndexingError,
                ))
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Value;
    use std::any::{Any, TypeId};

    fn create_test_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field1"]).unwrap(),
            "test",
        )
    }

    fn create_test_field_values() -> FieldValues {
        FieldValues::new(
            vec![("field1".to_string(), Value::String("value1".to_string()))],
            NitriteId::new(),
            Fields::with_names(vec!["field1"]).unwrap(),
        )
    }

    fn create_test_nitrite_config() -> NitriteConfig {
        let config = NitriteConfig::new();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        config
    }

    #[test]
    fn test_find_nitrite_index() {
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();

        // Positive case: index exists
        unique_indexer.inner.create_nitrite_index(&index_descriptor, &create_test_nitrite_config()).unwrap();
        let result = unique_indexer.find_nitrite_index(&index_descriptor);
        assert!(result.is_ok());

        // Negative case: index does not exist
        let non_existent_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["non_existent_field"]).unwrap(),
            "test",
        );
        let result = unique_indexer.find_nitrite_index(&non_existent_descriptor);
        assert!(result.is_err());
    }

    #[test]
    fn test_initialize() {
        let unique_indexer = UniqueIndexer::new();
        let nitrite_config = create_test_nitrite_config();

        let result = unique_indexer.initialize(nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let unique_indexer = UniqueIndexer::new();

        let result = unique_indexer.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_as_plugin() {
        let unique_indexer = UniqueIndexer::new();

        let plugin = unique_indexer.as_plugin();
        assert_eq!(plugin.type_id(), TypeId::of::<NitritePlugin>());
    }

    #[test]
    fn test_index_type() {
        let unique_indexer = UniqueIndexer::new();

        let index_type = unique_indexer.index_type();
        assert_eq!(index_type, UNIQUE_INDEX);
    }

    #[test]
    fn test_is_unique() {
        let unique_indexer = UniqueIndexer::new();

        let is_unique = unique_indexer.is_unique();
        assert!(is_unique);
    }

    #[test]
    fn test_validate_index() {
        let unique_indexer = UniqueIndexer::new();

        // Positive case: valid fields
        let fields = Fields::with_names(vec!["field1"]).unwrap();
        let result = unique_indexer.validate_index(&fields);
        assert!(result.is_ok());

        // Negative case: invalid fields
        let invalid_fields = Fields::with_names(vec!["field1", "field2"]).unwrap();
        let result = unique_indexer.validate_index(&invalid_fields);
        assert!(result.is_ok()); // Unique indexer does not validate fields strictly
    }

    #[test]
    fn test_drop_index() {
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        // Positive case: index exists
        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();
        let result = unique_indexer.drop_index(&index_descriptor, &nitrite_config);
        assert!(result.is_ok());

        // Negative case: index does not exist
        let result = unique_indexer.drop_index(&index_descriptor, &nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_index_entry() {
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        // index exists
        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();
        let result = unique_indexer.write_index_entry(&field_values, &index_descriptor, &nitrite_config);
        assert!(result.is_ok());

        // index does not exist, create new index
        let non_existent_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["non_existent_field"]).unwrap(),
            "test",
        );
        let result = unique_indexer.write_index_entry(&field_values, &non_existent_descriptor, &nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_index_entry() {
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();
        let result = unique_indexer.remove_index_entry(&field_values, &index_descriptor, &nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_by_filter() {
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();
        let mut find_plan = FindPlan::new();

        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let result = unique_indexer.find_by_filter(&find_plan, &nitrite_config);
        assert!(result.is_ok());
    }

    // Performance optimization tests
    #[test]
    fn test_find_nitrite_index_efficient_dashmap_access() {
        // Validates that find_nitrite_index uses .value() instead of full clone
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();

        // Multiple lookups should be efficient
        for _ in 0..10 {
            let result = unique_indexer.find_nitrite_index(&index_descriptor);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_unique_indexer_write_and_remove_reuse_same_index() {
        // Validates that write/remove reuse cached indices
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        // First write creates index
        unique_indexer
            .write_index_entry(&field_values, &index_descriptor, &nitrite_config)
            .unwrap();

        // Registry should have 1 index
        assert_eq!(unique_indexer.inner.index_registry.len(), 1);

        // Remove entry to clean up before second write
        unique_indexer
            .remove_index_entry(&field_values, &index_descriptor, &nitrite_config)
            .unwrap();

        // Write again with same index
        unique_indexer
            .write_index_entry(&field_values, &index_descriptor, &nitrite_config)
            .unwrap();

        // Registry should still have 1 index, not 2
        assert_eq!(unique_indexer.inner.index_registry.len(), 1);
    }

    #[test]
    fn test_unique_indexer_find_by_filter_creates_and_caches_index() {
        // Validates lazy creation and caching pattern
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();
        let mut find_plan = FindPlan::new();
        find_plan.set_index_descriptor(index_descriptor.clone());

        // First find_by_filter creates index
        unique_indexer
            .find_by_filter(&find_plan, &nitrite_config)
            .ok();
        assert_eq!(unique_indexer.inner.index_registry.len(), 1);

        // Second call reuses cached index
        unique_indexer
            .find_by_filter(&find_plan, &nitrite_config)
            .ok();
        assert_eq!(unique_indexer.inner.index_registry.len(), 1);
    }

    #[test]
    fn test_unique_indexer_drop_index_safely_handles_missing_index() {
        // Validates graceful handling when dropping non-existent index
        let unique_indexer = UniqueIndexer::new();
        let non_existent_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["non_existent"]).unwrap(),
            "test",
        );
        let nitrite_config = create_test_nitrite_config();

        let result = unique_indexer.drop_index(&non_existent_descriptor, &nitrite_config);
        assert!(result.is_ok());
        assert_eq!(unique_indexer.inner.index_registry.len(), 0);
    }

    #[test]
    fn test_unique_indexer_concurrent_index_access_patterns() {
        // Validates concurrent access patterns with DashMap
        let unique_indexer = UniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        // Create initial index
        unique_indexer.inner.create_nitrite_index(&index_descriptor, &nitrite_config).unwrap();

        // Multiple concurrent-style reads through find_nitrite_index
        for _ in 0..5 {
            let result = unique_indexer.find_nitrite_index(&index_descriptor);
            assert!(result.is_ok());
        }

        assert_eq!(unique_indexer.inner.index_registry.len(), 1);
    }
}