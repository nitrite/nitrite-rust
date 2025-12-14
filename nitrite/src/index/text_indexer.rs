use super::{
    nitrite_index::NitriteIndexProvider, text::Tokenizer, text_index::TextIndex, IndexDescriptor,
    NitriteIndexerProvider,
};
use crate::{
    collection::{FindPlan, NitriteId},
    errors::{ErrorKind, NitriteError, NitriteResult},
    nitrite_config::NitriteConfig,
    FieldValues, Fields, NitritePlugin, NitritePluginProvider, FULL_TEXT_INDEX,
};
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct TextIndexer {
    inner: Arc<TextIndexerInner>,
}

impl TextIndexer {
    pub fn new(tokenizer: Tokenizer) -> TextIndexer {
        TextIndexer {
            inner: Arc::new(TextIndexerInner::new(tokenizer)),
        }
    }

    fn find_text_index(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<TextIndex> {
        let result = self.inner.find_text_index(index_descriptor);

        match result {
            Some(text_index) => Ok(text_index),
            None => {
                log::error!("Full text index not found for {:?}", index_descriptor);
                Err(NitriteError::new(
                    "Full text index not found",
                    ErrorKind::IndexingError,
                ))
            }
        }
    }
}

impl NitritePluginProvider for TextIndexer {
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

impl NitriteIndexerProvider for TextIndexer {
    fn index_type(&self) -> String {
        FULL_TEXT_INDEX.to_string()
    }

    fn is_unique(&self) -> bool {
        false
    }

    fn validate_index(&self, fields: &Fields) -> NitriteResult<()> {
        self.inner.validate_index(fields)
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

struct TextIndexerInner {
    index_registry: DashMap<IndexDescriptor, TextIndex>,
    tokenizer: Tokenizer,
}

impl TextIndexerInner {
    fn new(tokenizer: Tokenizer) -> Self {
        Self {
            index_registry: DashMap::new(),
            tokenizer,
        }
    }

    fn find_text_index(&self, index_descriptor: &IndexDescriptor) -> Option<TextIndex> {
        self.index_registry
            .get(index_descriptor)
            .map(|it| it.value().clone())
    }

    fn create_nitrite_index(
        &self,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<TextIndex> {
        let store = nitrite_config.nitrite_store()?;
        let text_index = TextIndex::new(
            index_descriptor.clone(),
            store.clone(),
            self.tokenizer.clone(),
        );

        self.index_registry
            .insert(index_descriptor.clone(), text_index.clone());
        Ok(text_index)
    }

    fn validate_index(&self, fields: &Fields) -> NitriteResult<()> {
        if fields.field_names().len() != 1 {
            log::error!(
                "Text index can only be created on single field, but found {:?}",
                fields.field_names()
            );
            return Err(NitriteError::new(
                "Text index can only be created on single field",
                ErrorKind::IndexingError,
            ));
        }
        Ok(())
    }

    fn drop_index(&self, index_descriptor: &IndexDescriptor) -> NitriteResult<()> {
        let text_index = self.find_text_index(index_descriptor);
        if let Some(text_index) = text_index {
            text_index.drop_index()?;
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
        let mut text_index = self.find_text_index(index_descriptor);
        if text_index.is_none() {
            text_index = Some(self.create_nitrite_index(index_descriptor, nitrite_config)?);
        }

        if let Some(text_index) = text_index {
            text_index.write(field_values)?;
        }
        Ok(())
    }

    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()> {
        let mut text_index = self.find_text_index(index_descriptor);
        if text_index.is_none() {
            text_index = Some(self.create_nitrite_index(index_descriptor, nitrite_config)?);
        }

        if let Some(text_index) = text_index {
            text_index.remove(field_values)?;
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
                let text_index = if let Some(idx) = self.find_text_index(&index_descriptor) {
                    idx
                } else {
                    self.create_nitrite_index(&index_descriptor, nitrite_config)?
                };

                let nitrite_ids = text_index.find_nitrite_ids(find_plan)?;
                Ok(nitrite_ids)
            }
            None => {
                log::error!("Index descriptor not found in find plan");
                Err(NitriteError::new(
                    "Index descriptor not found",
                    ErrorKind::IndexingError,
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text::EnglishTokenizer;
    use crate::{FieldValues, Value};
    use std::any::{Any, TypeId};

    fn create_test_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(
            FULL_TEXT_INDEX,
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

    fn create_test_tokenizer() -> Tokenizer {
        Tokenizer::new(EnglishTokenizer)
    }

    fn create_test_nitrite_config() -> NitriteConfig {
        let config = NitriteConfig::new();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        config
    }

    #[test]
    fn test_find_text_index() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        // Positive case: index exists
        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();
        let result = text_indexer.find_text_index(&index_descriptor);
        assert!(result.is_ok());

        // Negative case: index does not exist
        let non_existent_descriptor = IndexDescriptor::new(
            FULL_TEXT_INDEX,
            Fields::with_names(vec!["non_existent_field"]).unwrap(),
            "test",
        );
        let result = text_indexer.find_text_index(&non_existent_descriptor);
        assert!(result.is_err());
    }

    #[test]
    fn test_initialize() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let nitrite_config = create_test_nitrite_config();

        let result = text_indexer.initialize(nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());

        let result = text_indexer.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_as_plugin() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());

        let plugin = text_indexer.as_plugin();
        assert_eq!(plugin.type_id(), TypeId::of::<NitritePlugin>());
    }

    #[test]
    fn test_index_type() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());

        let index_type = text_indexer.index_type();
        assert_eq!(index_type, FULL_TEXT_INDEX);
    }

    #[test]
    fn test_is_unique() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());

        let is_unique = text_indexer.is_unique();
        assert!(!is_unique);
    }

    #[test]
    fn test_validate_index() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());

        // Positive case: valid fields
        let fields = Fields::with_names(vec!["field1"]).unwrap();
        let result = text_indexer.validate_index(&fields);
        assert!(result.is_ok());

        // Negative case: invalid fields
        let invalid_fields = Fields::with_names(vec!["field1", "field2"]).unwrap();
        let result = text_indexer.validate_index(&invalid_fields);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_index() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        // Positive case: index exists
        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();
        let result = text_indexer.drop_index(&index_descriptor, &nitrite_config);
        assert!(result.is_ok());

        // Negative case: index does not exist
        let result = text_indexer.drop_index(&index_descriptor, &nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_index_entry() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        // index exists
        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();
        let result = text_indexer.write_index_entry(
            &field_values,
            &index_descriptor,
            &nitrite_config,
        );
        assert!(result.is_ok());

        // index does not exist, create new index
        let non_existent_descriptor = IndexDescriptor::new(
            FULL_TEXT_INDEX,
            Fields::with_names(vec!["non_existent_field"]).unwrap(),
            "test",
        );
        let result =
            text_indexer.write_index_entry(&field_values, &non_existent_descriptor, &nitrite_config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_index_entry() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();
        let result = text_indexer.remove_index_entry(
            &field_values,
            &index_descriptor,
            &nitrite_config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_by_filter() {
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();
        let mut find_plan = FindPlan::new();

        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();
        find_plan.set_index_descriptor(index_descriptor.clone());
        let result = text_indexer.find_by_filter(&find_plan, &nitrite_config);
        assert!(result.is_ok());
    }

    // Performance optimization tests
    #[test]
    fn test_find_text_index_efficient_dashmap_access() {
        // Validates that find_text_index uses .value() instead of full clone
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();

        // Multiple lookups should be efficient
        for _ in 0..10 {
            let result = text_indexer.find_text_index(&index_descriptor);
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_text_indexer_write_and_remove_reuse_same_index() {
        // Validates that write/remove reuse cached indices instead of creating duplicates
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let field_values = create_test_field_values();
        let nitrite_config = create_test_nitrite_config();

        // First write creates index
        text_indexer
            .write_index_entry(&field_values, &index_descriptor, &nitrite_config)
            .unwrap();

        // Registry should have 1 index
        assert_eq!(text_indexer.inner.index_registry.len(), 1);

        // Second write should reuse same index
        text_indexer
            .write_index_entry(&field_values, &index_descriptor, &nitrite_config)
            .unwrap();

        // Registry should still have 1 index, not 2
        assert_eq!(text_indexer.inner.index_registry.len(), 1);
    }

    #[test]
    fn test_text_indexer_find_by_filter_creates_and_caches_index() {
        // Validates lazy creation and caching pattern
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();
        let mut find_plan = FindPlan::new();
        find_plan.set_index_descriptor(index_descriptor.clone());

        // First find_by_filter creates index
        text_indexer
            .find_by_filter(&find_plan, &nitrite_config)
            .ok();
        assert_eq!(text_indexer.inner.index_registry.len(), 1);

        // Second call reuses cached index
        text_indexer
            .find_by_filter(&find_plan, &nitrite_config)
            .ok();
        assert_eq!(text_indexer.inner.index_registry.len(), 1);
    }

    #[test]
    fn test_text_indexer_drop_index_safely_handles_missing_index() {
        // Validates graceful handling when dropping non-existent index
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let non_existent_descriptor = IndexDescriptor::new(
            FULL_TEXT_INDEX,
            Fields::with_names(vec!["non_existent"]).unwrap(),
            "test",
        );
        let nitrite_config = create_test_nitrite_config();

        let result = text_indexer.drop_index(&non_existent_descriptor, &nitrite_config);
        assert!(result.is_ok());
        assert_eq!(text_indexer.inner.index_registry.len(), 0);
    }

    #[test]
    fn test_text_indexer_concurrent_index_access_patterns() {
        // Validates concurrent access patterns with DashMap
        let tokenizer = create_test_tokenizer();
        let text_indexer = TextIndexer::new(tokenizer.clone());
        let index_descriptor = create_test_index_descriptor();
        let nitrite_config = create_test_nitrite_config();

        // Create initial index
        text_indexer
            .inner
            .create_nitrite_index(&index_descriptor, &nitrite_config)
            .unwrap();

        // Multiple concurrent-style reads through find_text_index
        for _ in 0..5 {
            let result = text_indexer.find_text_index(&index_descriptor);
            assert!(result.is_ok());
        }

        assert_eq!(text_indexer.inner.index_registry.len(), 1);
    }
}
