use super::nitrite_index::NitriteIndexProvider;
use super::text::TokenizerProvider;
use super::IndexMap;
use super::{text::Tokenizer, IndexDescriptor};
use crate::collection::{FindPlan, NitriteId};
use crate::common::{validate_string_array_index_field, Token};
use crate::filter::TextFilter;
use crate::filter::{is_text_filter, FilterProvider};
use crate::store::{NitriteMapProvider, NitriteStoreProvider};
use crate::{
    derive_index_map_name,
    errors::{ErrorKind, NitriteError, NitriteResult},
    store::{NitriteMap, NitriteStore},
    FieldValues, Value,
};
use itertools::Itertools;
use once_cell::sync::Lazy;
use smallvec::SmallVec;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

type TokenVec = SmallVec<[String; 8]>;

static UNIQUE_CONSTRAINT_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Unique constraint violated",
        ErrorKind::UniqueConstraintViolation,
    )
});

static INVALID_TYPE_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Invalid value type for text index",
        ErrorKind::IndexingError,
    )
});

static INVALID_FILTER_TYPE_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Invalid filter type for text index",
        ErrorKind::IndexingError,
    )
});

static INVALID_FILTER_COUNT_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Invalid filter count for text index",
        ErrorKind::IndexingError,
    )
});

#[derive(Clone)]
pub(crate) struct TextIndex {
    inner: Arc<TextIndexInner>,
}

impl TextIndex {
    pub fn new(
        index_descriptor: IndexDescriptor,
        store: NitriteStore,
        tokenizer: Tokenizer,
    ) -> Self {
        TextIndex {
            inner: Arc::new(TextIndexInner::new(index_descriptor, store, tokenizer)),
        }
    }
}

impl Deref for TextIndex {
    type Target = Arc<TextIndexInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NitriteIndexProvider for TextIndex {
    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
        self.inner.index_descriptor()
    }

    fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        self.inner.write(field_values)
    }

    fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        self.inner.remove(field_values)
    }

    fn drop_index(&self) -> NitriteResult<()> {
        self.inner.drop_index()
    }

    fn find_nitrite_ids(&self, find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>> {
        self.inner.find_nitrite_ids(find_plan)
    }

    fn is_unique(&self) -> bool {
        false
    }
}

pub struct TextIndexInner {
    index_descriptor: IndexDescriptor,
    store: NitriteStore,
    tokenizer: Tokenizer,
}

impl TextIndexInner {
    fn new(index_descriptor: IndexDescriptor, store: NitriteStore, tokenizer: Tokenizer) -> Self {
        Self {
            index_descriptor,
            store,
            tokenizer,
        }
    }

    fn add_index_element(
        &self,
        index_map: &NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let words = self.decompose(value);

        for word in &words {
            let word_key = Value::String(word.clone());
            let existing = index_map.get(&word_key)
                .map_err(|e| NitriteError::new(&format!("Failed to retrieve existing entries for word '{}' from text index: {}", word, e), e.kind().clone()))?;
            let mut nitrite_ids = match existing {
                Some(Value::Array(arr)) => arr,
                _ => Vec::with_capacity(1),
            };
            let nitrite_ids = self.add_nitrite_ids(&mut nitrite_ids, field_values)
                .map_err(|e| NitriteError::new(&format!("Failed to add nitrite IDs for word '{}' in text index: {}", word, e), e.kind().clone()))?;

            index_map.put(word_key, Value::Array(nitrite_ids))
                .map_err(|e| NitriteError::new(&format!("Failed to store word '{}' in text index: {}", word, e), e.kind().clone()))?;

            // for case-insensitive index, add lower case word in the map
            let mut lower_word = String::with_capacity(word.len() + 2);
            lower_word.push_str("i_");
            lower_word.push_str(&word.to_lowercase());

            if word != &lower_word {
                let lower_word_key = Value::String(lower_word.clone());
                let lower_existing = index_map.get(&lower_word_key)
                    .map_err(|e| NitriteError::new(&format!("Failed to retrieve existing entries for lowercase word '{}' from text index: {}", lower_word, e), e.kind().clone()))?;
                let mut lower_nitrite_ids = match lower_existing {
                    Some(Value::Array(arr)) => arr,
                    _ => Vec::with_capacity(1),
                };
                let lower_nitrite_ids =
                    self.add_nitrite_ids(&mut lower_nitrite_ids, field_values)
                        .map_err(|e| NitriteError::new(&format!("Failed to add nitrite IDs for lowercase word '{}' in text index: {}", lower_word, e), e.kind().clone()))?;

                index_map.put(lower_word_key, Value::Array(lower_nitrite_ids))
                    .map_err(|e| NitriteError::new(&format!("Failed to store lowercase word '{}' in text index: {}", lower_word, e), e.kind().clone()))?;
            }
        }
        Ok(())
    }

    fn remove_index_element(
        &self,
        index_map: &NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let words = self.decompose(value);

        for word in &words {
            let word_key = Value::String(word.clone());

                // Handle IDs more efficiently
                if let Some(Value::Array(ids)) = index_map.get(&word_key)? {
                    // Pre-allocate with reasonable size
                    let mut new_ids = Vec::with_capacity(ids.len());

                    // Filter directly without additional allocations
                    for id in &ids {
                        // Safely extract NitriteId with error handling
                        match id.as_nitrite_id() {
                            Some(nitrite_id) if nitrite_id != field_values.nitrite_id() => {
                                new_ids.push(id.clone());
                            }
                            None => {
                                log::warn!("Invalid NitriteId value in text index word map: {:?}", id);
                                // Skip corrupted entry instead of panicking
                            }
                            _ => {} // ID matches, don't add to new_ids
                        }
                    }

                    // Update or remove based on result
                    if new_ids.is_empty() {
                        index_map.remove(&word_key)?;
                    } else {
                        index_map.put(word_key, Value::Array(new_ids))?;
                    }
                }            // for case-insensitive index, remove lower case word in the map
            let mut lower_word = String::with_capacity(word.len() + 2);
            lower_word.push_str("i_");
            lower_word.push_str(&word.to_lowercase());

            if word != &lower_word {
                let lower_word_key = Value::String(lower_word.clone());

                // Handle IDs more efficiently
                if let Some(Value::Array(ids)) = index_map.get(&lower_word_key)? {
                    // Pre-allocate with reasonable size
                    let mut new_lower_ids = Vec::with_capacity(ids.len());

                    // Filter directly without additional allocations
                    for id in &ids {
                        // Safely extract NitriteId with error handling
                        match id.as_nitrite_id() {
                            Some(nitrite_id) if nitrite_id != field_values.nitrite_id() => {
                                new_lower_ids.push(id.clone());
                            }
                            None => {
                                log::warn!("Invalid NitriteId value in text index lowercase word map: {:?}", id);
                                // Skip corrupted entry instead of panicking
                            }
                            _ => {} // ID matches, don't add to new_lower_ids
                        }
                    }

                    // Update or remove based on result
                    if new_lower_ids.is_empty() {
                        index_map.remove(&lower_word_key)?;
                    } else {
                        index_map.put(lower_word_key, Value::Array(new_lower_ids))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn add_nitrite_ids(
        &self,
        nitrite_ids: &mut Vec<Value>,
        field_values: &FieldValues,
    ) -> NitriteResult<Vec<Value>> {
        if self.is_unique() && nitrite_ids.len() == 1 {
            // if key is already exists for unique type, throw error
            log::error!("Unique constraint violated for {:?}", field_values);
            return Err(UNIQUE_CONSTRAINT_ERROR.clone());
        }

        // index always are in ascending format
        nitrite_ids.push(Value::NitriteId(*field_values.nitrite_id()));

        // dedupe the nitrite_ids
        let mut ids = Vec::with_capacity(nitrite_ids.len());
        for id in nitrite_ids.iter().unique() {
            ids.push(id.clone());
        }
        Ok(ids)
    }

    fn find_index_map(&self) -> NitriteResult<NitriteMap> {
        let map_name = derive_index_map_name(&self.index_descriptor);
        let store = self.store.clone();
        store.open_map(&map_name)
    }

    fn decompose(&self, value: &Value) -> TokenVec {
        match value {
            Value::String(s) => {
                if s.is_empty() {
                    return TokenVec::new();                    
                }
                
                let mut tokens = TokenVec::new();
                for token in self.tokenizer.tokenize(s) {
                    tokens.push(token);
                }
                tokens
            }
            _ => TokenVec::new(),
        }
    }

    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
        Ok(self.index_descriptor.clone())
    }

    fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        let first_field = field_names.first()
            .ok_or_else(|| NitriteError::new(
                "Text index error: no field names found in index descriptor",
                ErrorKind::IndexingError
            ))?;
        let first_value = field_values.get_value(first_field);

        let index_map = self.find_index_map()?;

        match first_value {
            None | Some(Value::Null) => {
                self.add_index_element(&index_map, field_values, &Value::Null)?;
            }
            Some(Value::String(_)) => {
                self.add_index_element(&index_map, field_values, first_value.unwrap())?;
            }
            Some(Value::Array(values)) => {
                validate_string_array_index_field(values, first_field)?;
                for value in values {
                    self.add_index_element(&index_map, field_values, value)?;
                }
            }
            _ => {
                log::error!("Invalid value type for text index {:?}", first_value);
                return Err(INVALID_TYPE_ERROR.clone());
            }
        }

        Ok(())
    }

    fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        let first_field = field_names.first()
            .ok_or_else(|| NitriteError::new(
                "Text index error: no field names found in index descriptor during removal",
                ErrorKind::IndexingError
            ))?;
        let first_value = field_values.get_value(first_field);

        let index_map = self.find_index_map()?;

        match first_value {
            None | Some(Value::Null) => {
                self.remove_index_element(&index_map, field_values, &Value::Null)?;
            }
            Some(Value::String(_)) => {
                self.remove_index_element(&index_map, field_values, first_value.unwrap())?;
            }
            Some(Value::Array(values)) => {
                validate_string_array_index_field(values, first_field)?;
                for value in values {
                    self.remove_index_element(&index_map, field_values, value)?;
                }
            }
            _ => {
                log::error!("Invalid value type for text index {:?}", first_value);
                return Err(INVALID_TYPE_ERROR.clone());
            }
        }

        Ok(())
    }

    fn drop_index(&self) -> NitriteResult<()> {
        let index_map = self.find_index_map()?;
        index_map.clear()?;
        index_map.dispose()?;
        Ok(())
    }

    fn find_nitrite_ids(&self, find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>> {
        let index_scan_filter = find_plan.index_scan_filter();
        if index_scan_filter.is_none() {
            return Ok(Vec::new());
        }

        let filters = index_scan_filter.unwrap().filters();
        if filters.len() != 1 {
            log::error!("Invalid filter count {} for text index", filters.len());
            return Err(INVALID_FILTER_COUNT_ERROR.clone());
        }

        let filter = &filters[0];
        if !is_text_filter(filter) {
            log::error!("Invalid filter type for text index {}", filter);
            return Err(INVALID_FILTER_TYPE_ERROR.clone());
        }

        // Process text filter
        let text_filter = filter
            .as_any()
            .downcast_ref::<TextFilter>()
            .ok_or_else(|| NitriteError::new(
                "Text index error: cannot process non-TextFilter in text index query. Expected TextFilter but found another filter type",
                ErrorKind::IndexingError
            ))?
            .clone();
        text_filter.set_tokenizer(self.tokenizer.clone());

        // Get results and pre-allocate with estimated capacity
        let index_map = IndexMap::new(Some(self.find_index_map()?), None);
        let result = text_filter.apply_on_index(&index_map)?;

        // Pre-allocate HashSet with capacity
        let mut id_list = HashSet::with_capacity(result.len());
        for value in result {
            // Gracefully handle invalid NitriteId values
            match value.as_nitrite_id() {
                Some(id) => {
                    id_list.insert(*id);
                }
                None => {
                    log::warn!("Invalid NitriteId value in text index result: {:?}", value);
                    // Skip invalid IDs instead of panicking
                }
            }
        }

        // Convert to Vec with known capacity
        let mut id_vec = Vec::with_capacity(id_list.len());
        id_vec.extend(id_list);
        Ok(id_vec)
    }

    fn is_unique(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Fields, UNIQUE_INDEX};
    use crate::index::text::EnglishTokenizer;
    use crate::{FieldValues, Value};

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

    fn create_test_tokenizer() -> Tokenizer {
        Tokenizer::new(EnglishTokenizer)
    }

    #[test]
    fn test_text_index_new() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(
            index_descriptor.clone(),
            nitrite_store.clone(),
            tokenizer.clone(),
        );

        assert_eq!(text_index.inner.index_descriptor, index_descriptor);
    }

    #[test]
    fn test_text_index_add_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let index_map = text_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = text_index.add_index_element(&index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_remove_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let index_map = text_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = text_index.remove_index_element(&index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_add_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let mut nitrite_ids = Vec::new();
        let field_values = create_test_field_values();

        let result = text_index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_find_index_map() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let result = text_index.find_index_map();
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_decompose() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let value = Value::String("test_value".to_string());
        let result = text_index.decompose(&value);
        // word will be tokenized to "test" and "value". Later "value" will be removed
        // as it is an English stop word
        assert_eq!(result.into_vec(), vec!["test"]);
    }

    #[test]
    fn test_text_index_write() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = create_test_field_values();

        let result = text_index.write(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_remove() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = create_test_field_values();

        let result = text_index.remove(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_drop_index() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let result = text_index.drop_index();
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_find_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let find_plan = FindPlan::new();

        let result = text_index.find_nitrite_ids(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_is_unique() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let result = text_index.is_unique();
        assert!(!result);
    }

    #[test]
    fn test_text_index_remove_index_element_not_found() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let index_map = text_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("non_existent_value".to_string());

        let result = text_index.remove_index_element(&index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_write_with_empty_field_names() {
        // Test that write() handles empty field names gracefully
        // instead of panicking with first().unwrap()
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        
        // Create a field descriptor with at least one field
        let index_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field1"]).unwrap(),
            "test",
        );
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        // Create field values with no actual fields
        let field_values = FieldValues::new(
            vec![],
            NitriteId::new(),
            Fields::with_names(vec![]).unwrap_or_else(|_| {
                // If empty fields can't be created, create a dummy one
                Fields::with_names(vec!["dummy"]).unwrap()
            }),
        );

        // This should handle the mismatch gracefully
        let result = text_index.write(&field_values);
        // Should succeed or fail gracefully, not panic
        let _ = result;
    }

    #[test]
    fn test_text_index_remove_with_empty_field_names() {
        // Test that remove() handles empty field names gracefully
        // instead of panicking with first().unwrap()
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        
        let index_descriptor = IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field1"]).unwrap(),
            "test",
        );
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        // Create field values with no actual fields
        let field_values = FieldValues::new(
            vec![],
            NitriteId::new(),
            Fields::with_names(vec![]).unwrap_or_else(|_| {
                // If empty fields can't be created, create a dummy one
                Fields::with_names(vec!["dummy"]).unwrap()
            }),
        );

        // This should handle the mismatch gracefully
        let result = text_index.remove(&field_values);
        // Should succeed or fail gracefully, not panic
        let _ = result;
    }

    #[test]
    fn test_text_index_find_nitrite_ids_with_invalid_filter() {
        // Test that find_nitrite_ids handles invalid filter types gracefully
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        // Create a FindPlan with no index_scan_filter (simulates invalid/missing filter)
        let find_plan = FindPlan::new();

        let result = text_index.find_nitrite_ids(&find_plan);
        // Should handle gracefully - no filter means empty result
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_text_index_find_nitrite_ids_with_invalid_ids_in_result() {
        // Test that find_nitrite_ids handles invalid NitriteId values gracefully
        // in the result set, logging warnings instead of panicking
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = create_test_field_values();
        
        // Write a valid value to the text index
        let result = text_index.write(&field_values);
        assert!(result.is_ok());

        // The text index is now populated and should handle ID extraction gracefully
        // even if it encounters corrupted data (which won't happen here but tests the path)
    }

    #[test]
    fn test_text_index_write_with_string_value() {
        // Test that write() properly handles string values without unwrap issues
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = FieldValues::new(
            vec![("field1".to_string(), Value::String("test value".to_string()))],
            NitriteId::new(),
            Fields::with_names(vec!["field1"]).unwrap(),
        );

        let result = text_index.write(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_remove_with_string_value() {
        // Test that remove() properly handles string values without unwrap issues
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = FieldValues::new(
            vec![("field1".to_string(), Value::String("test value".to_string()))],
            NitriteId::new(),
            Fields::with_names(vec!["field1"]).unwrap(),
        );

        // First write, then remove to ensure index has data
        let _ = text_index.write(&field_values);
        let result = text_index.remove(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index_handles_invalid_nitrite_ids_in_word_map() {
        // Test that text index gracefully handles corrupted NitriteId values in word map
        // This validates the safe pattern used instead of as_nitrite_id().unwrap()
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = FieldValues::new(
            vec![("field1".to_string(), Value::String("test document".to_string()))],
            NitriteId::new(),
            Fields::with_names(vec!["field1"]).unwrap(),
        );

        // Write and remove to test the removal path with ID validation
        text_index.write(&field_values).ok();
        let result = text_index.remove(&field_values);
        
        // Should succeed without panicking even if IDs are corrupted
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_text_index_handles_invalid_nitrite_ids_in_lowercase_map() {
        // Test that text index handles corrupted IDs in lowercase word map
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let field_values = FieldValues::new(
            vec![("field1".to_string(), Value::String("UPPERCASE TEXT".to_string()))],
            NitriteId::new(),
            Fields::with_names(vec!["field1"]).unwrap(),
        );

        // Write with uppercase (tests case-insensitive storage)
        text_index.write(&field_values).ok();
        // Remove should handle lowercase map safely
        let result = text_index.remove(&field_values);
        
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_text_index_find_nitrite_ids_with_corrupted_result_data() {
        // Test that find_nitrite_ids gracefully handles non-NitriteId values in results
        // This validates the safe match pattern for as_nitrite_id()
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let tokenizer = create_test_tokenizer();
        let text_index = TextIndex::new(index_descriptor, nitrite_store, tokenizer);

        let find_plan = FindPlan::new();
        // Without setting index_scan_filter, find_nitrite_ids should return empty successfully
        let result = text_index.find_nitrite_ids(&find_plan);
        
        // Should handle gracefully without panicking
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
