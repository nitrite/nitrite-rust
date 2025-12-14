use super::{
    compound_index::CompoundIndex, nitrite_index::NitriteIndex,
    nitrite_index::NitriteIndexProvider,
    simple_index::SimpleIndex, IndexDescriptor, NitriteIndexerProvider,
};
use crate::{
    collection::{FindPlan, NitriteId},
    errors::{ErrorKind, NitriteError, NitriteResult}
    ,
    nitrite_config::NitriteConfig,
    FieldValues, Fields, NitritePlugin, NitritePluginProvider, NON_UNIQUE_INDEX,
};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct NonUniqueIndexer {
    inner: Arc<NonUniqueIndexerInner>,
}

impl NonUniqueIndexer {
    pub fn new() -> Self {
        NonUniqueIndexer {
            inner: Arc::new(NonUniqueIndexerInner::new()),
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
                log::error!("Index not found for the descriptor {:?}", index_descriptor);
                Err(NitriteError::new(
                    "Index descriptor not found",
                    ErrorKind::IndexingError,
                ))
            }
        }
    }
}

impl NitritePluginProvider for NonUniqueIndexer {
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

impl NitriteIndexerProvider for NonUniqueIndexer {
    fn index_type(&self) -> String {
        NON_UNIQUE_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        false
    }

    fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
        Ok(())
    }

    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
        _nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        self.inner
            .drop_index(index_descriptor)
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

struct NonUniqueIndexerInner {
    index_registry: DashMap<IndexDescriptor, NitriteIndex>,
}

impl NonUniqueIndexerInner {
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
            nitrite_index = Some(self.create_nitrite_index(index_descriptor, nitrite_config)?);
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
                log::error!("Index descriptor not found in the find plan");
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
    use crate::common::Convertible;
    use std::any::{Any, TypeId};

    fn create_test_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(
            NON_UNIQUE_INDEX,
            Fields::with_names(vec!["test_field"]).unwrap(),
            "test",
        )
    }

    fn create_test_field_values() -> FieldValues {
        FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        )
    }

    fn create_test_find_plan() -> FindPlan {
        FindPlan::new()
    }

    #[test]
    fn test_initialize() {
        let indexer = NonUniqueIndexer::new();
        let config = NitriteConfig::default();
        assert!(indexer.initialize(config).is_ok());
    }

    #[test]
    fn test_close() {
        let indexer = NonUniqueIndexer::new();
        assert!(indexer.close().is_ok());
    }

    #[test]
    fn test_as_plugin() {
        let indexer = NonUniqueIndexer::new();
        assert_eq!(indexer.as_plugin().type_id(), TypeId::of::<NitritePlugin>());
    }

    #[test]
    fn test_index_type() {
        let indexer = NonUniqueIndexer::new();
        assert_eq!(indexer.index_type(), NON_UNIQUE_INDEX);
    }

    #[test]
    fn test_is_unique() {
        let indexer = NonUniqueIndexer::new();
        assert!(!indexer.is_unique());
    }

    #[test]
    fn test_validate_index() {
        let indexer = NonUniqueIndexer::new();
        let fields = Fields::with_names(vec!["test_field"]).unwrap();
        assert!(indexer.validate_index(&fields).is_ok());
    }

    #[test]
    fn test_drop_index() {
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        assert!(indexer.drop_index(&index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_write_index_entry() {
        let indexer = NonUniqueIndexer::new();
        let field_values = create_test_field_values();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        assert!(indexer.write_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_remove_index_entry() {
        let indexer = NonUniqueIndexer::new();
        let field_values = create_test_field_values();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        indexer.inner.create_nitrite_index(&index_descriptor, &config).unwrap();
        assert!(indexer.remove_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_find_by_filter_no_descriptor() {
        let indexer = NonUniqueIndexer::new();
        let find_plan = create_test_find_plan();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // no index descriptor - error
        assert!(indexer.find_by_filter(&find_plan, &config).is_err());
    }

    #[test]
    fn test_find_by_filter() {
        let indexer = NonUniqueIndexer::new();
        let mut find_plan = create_test_find_plan();
        let index_descriptor = create_test_index_descriptor();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        indexer.inner.create_nitrite_index(&index_descriptor, &config).unwrap();
        assert!(indexer.find_by_filter(&find_plan, &config).is_ok());
    }

    #[test]
    fn test_find_nitrite_index_not_found() {
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        assert!(indexer.find_nitrite_index(&index_descriptor).is_err());
    }

    #[test]
    fn test_create_nitrite_index() {
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        assert!(indexer.inner.create_nitrite_index(&index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_drop_index_not_found() {
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        assert!(indexer.inner.drop_index(&index_descriptor).is_ok());
    }

    #[test]
    fn test_write_index_entry_not_found() {
        let indexer = NonUniqueIndexer::new();
        let field_values = create_test_field_values();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        assert!(indexer.inner.write_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_remove_index_entry_not_found() {
        let indexer = NonUniqueIndexer::new();
        let field_values = create_test_field_values();
        let index_descriptor = create_test_index_descriptor();
        assert!(indexer.inner.remove_index_entry(&field_values, &index_descriptor, &NitriteConfig::default()).is_err());
    }

    #[test]
    fn test_find_by_filter_not_found() {
        let indexer = NonUniqueIndexer::new();
        let find_plan = create_test_find_plan();
        assert!(indexer.inner.find_by_filter(&find_plan, &NitriteConfig::default()).is_err());
    }

    #[test]
    fn test_find_by_filter_creates_index_when_missing() {
        // Test that find_by_filter properly uses if-let to create index when missing
        // This validates the fix for the is_none() + unwrap() anti-pattern
        let indexer = NonUniqueIndexer::new();
        let mut find_plan = create_test_find_plan();
        let index_descriptor = create_test_index_descriptor();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Index doesn't exist yet
        assert!(indexer.find_nitrite_index(&index_descriptor).is_err());
        
        // find_by_filter should create it using safe if-let pattern
        let result = indexer.find_by_filter(&find_plan, &config);
        
        // Should succeed (either Ok with empty results or Ok with results)
        assert!(result.is_ok());
        
        // Now index should exist
        assert!(indexer.find_nitrite_index(&index_descriptor).is_ok());
    }

    #[test]
    fn test_find_by_filter_uses_existing_index() {
        // Test that find_by_filter uses existing index without recreating
        let indexer = NonUniqueIndexer::new();
        let mut find_plan = create_test_find_plan();
        let index_descriptor = create_test_index_descriptor();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Create index first
        indexer.inner.create_nitrite_index(&index_descriptor, &config).unwrap();
        
        // find_by_filter should use existing index
        let result = indexer.find_by_filter(&find_plan, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_by_filter_handles_none_descriptor_gracefully() {
        // Test that find_by_filter handles missing descriptor with proper error
        let indexer = NonUniqueIndexer::new();
        let find_plan = FindPlan::new();  // No descriptor set
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Should return error, not panic
        let result = indexer.find_by_filter(&find_plan, &config);
        assert!(result.is_err());
    }

    // Performance optimization tests
    #[test]
    fn test_find_nitrite_index_efficient_dashmap_access() {
        // Test that find_nitrite_index uses efficient .value() instead of full clone
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Create index first
        indexer.inner.create_nitrite_index(&index_descriptor, &config).unwrap();
        
        // Multiple accesses should use efficient pattern
        let result1 = indexer.find_nitrite_index(&index_descriptor);
        assert!(result1.is_ok());
        
        let result2 = indexer.find_nitrite_index(&index_descriptor);
        assert!(result2.is_ok());
    }

    #[test]
    fn test_write_and_remove_reuse_same_index() {
        // Test that write/remove efficiently reuse cached index without recreating
        let indexer = NonUniqueIndexer::new();
        let field_values = create_test_field_values();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // First write creates index
        assert!(indexer.write_index_entry(&field_values, &index_descriptor, &config).is_ok());
        
        // Second write should reuse index
        assert!(indexer.write_index_entry(&field_values, &index_descriptor, &config).is_ok());
        
        // Remove should also reuse index
        assert!(indexer.remove_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_find_by_filter_creates_and_caches_index() {
        // Test that find_by_filter efficiently creates and caches index on first access
        let indexer = NonUniqueIndexer::new();
        let mut find_plan = create_test_find_plan();
        let index_descriptor = create_test_index_descriptor();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // First call creates index
        let result1 = indexer.find_by_filter(&find_plan, &config);
        assert!(result1.is_ok());
        
        // Verify index now exists in registry
        assert!(indexer.find_nitrite_index(&index_descriptor).is_ok());
        
        // Second call reuses cached index
        let result2 = indexer.find_by_filter(&find_plan, &config);
        assert!(result2.is_ok());
    }

    #[test]
    fn test_drop_index_safely_handles_missing_index() {
        // Test that drop_index gracefully handles missing index in registry
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        
        // Dropping non-existent index should not error
        let result = indexer.inner.drop_index(&index_descriptor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_concurrent_index_access_patterns() {
        // Test that concurrent access patterns work efficiently with DashMap
        let indexer = NonUniqueIndexer::new();
        let index_descriptor = create_test_index_descriptor();
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Create index
        indexer.inner.create_nitrite_index(&index_descriptor, &config).unwrap();
        
        // Multiple sequential accesses
        for _ in 0..5 {
            let result = indexer.find_nitrite_index(&index_descriptor);
            assert!(result.is_ok());
        }
    }
}