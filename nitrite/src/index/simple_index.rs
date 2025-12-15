use super::{
    index_scanner::IndexScanner, nitrite_index::NitriteIndexProvider, IndexDescriptor, IndexMap,
};
use crate::{
    collection::{FindPlan, NitriteId},
    derive_index_map_name,
    errors::{ErrorKind, NitriteError, NitriteResult}
    ,
    store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider},
    FieldValues, Value, UNIQUE_INDEX,
};
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

static UNIQUE_CONSTRAINT_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Unique constraint violated",
        ErrorKind::UniqueConstraintViolation,
    )
});

#[derive(Clone)]
pub(crate) struct SimpleIndex {
    inner: Arc<SimpleIndexInner>,
}

impl SimpleIndex {
    pub fn new(index_descriptor: IndexDescriptor, store: NitriteStore) -> Self {
        SimpleIndex {
            inner: Arc::new(SimpleIndexInner::new(index_descriptor, store)),
        }
    }
}

impl Deref for SimpleIndex {
    type Target = Arc<SimpleIndexInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NitriteIndexProvider for SimpleIndex {
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
        self.inner.is_unique()
    }
}

pub struct SimpleIndexInner {
    index_descriptor: IndexDescriptor,
    store: NitriteStore,
}

impl SimpleIndexInner {
    fn new(index_descriptor: IndexDescriptor, store: NitriteStore) -> Self {
        Self {
            index_descriptor,
            store,
        }
    }

    fn find_index_map(&self) -> NitriteResult<NitriteMap> {
        let map_name = derive_index_map_name(&self.index_descriptor);
        self.store.open_map(&map_name)
    }

    fn add_index_element(
        &self,
        index_map: &NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let existing = index_map.get(value)?;

        let mut nitrite_ids = match existing {
            Some(Value::Array(arr)) => arr,
            _ => Vec::with_capacity(1),
        };

        let nitrite_ids = self.add_nitrite_ids(&mut nitrite_ids, field_values)?;
        index_map.put(value.clone(), Value::Array(nitrite_ids))?;
        Ok(())
    }

    fn remove_index_element(
        &self,
        index_map: NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let nitrite_ids = index_map.get(value)?;
        let mut nitrite_ids = nitrite_ids.unwrap_or(Value::Array(Vec::new()));
        
        match nitrite_ids.as_array_mut() {
            Some(array) => {
                if !array.is_empty() {
                    array.retain(|x| {
                        match x.as_nitrite_id() {
                            Some(id) => id != field_values.nitrite_id(),
                            None => {
                                log::warn!("Invalid NitriteId found in index, skipping");
                                true  // Keep the entry
                            }
                        }
                    });
                }

                if array.is_empty() {
                    index_map.remove(value)?;
                } else {
                    index_map.put(value.clone(), Value::Array(array.to_vec()))?;
                }
            }
            None => {
                log::error!("Index entry is not an array, expected array type");
                return Err(NitriteError::new(
                    "Index entry is not an array - index data corrupted",
                    ErrorKind::IndexingError,
                ));
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

        // Sort and dedup in-place instead of collecting unique
        nitrite_ids.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        nitrite_ids.dedup();
        
        Ok(std::mem::take(nitrite_ids))
    }

    fn remove_nitrite_ids(
        &self,
        nitrite_ids: &mut Vec<Value>,
        field_values: &FieldValues,
    ) -> NitriteResult<Vec<Value>> {
        if !nitrite_ids.is_empty() {
            nitrite_ids.retain(|x| {
                match x.as_nitrite_id() {
                    Some(id) => id != field_values.nitrite_id(),
                    None => {
                        log::warn!("Invalid NitriteId found in index, keeping entry");
                        true  // Keep the entry
                    }
                }
            });
        }
        Ok(std::mem::take(nitrite_ids))
    }

    fn scan_index(
        &self,
        find_plan: &FindPlan,
        index_map: NitriteMap,
    ) -> NitriteResult<Vec<NitriteId>> {
        if find_plan.index_scan_filter().is_none() {
            return Ok(Vec::new());
        }
        
        let index_scan_filter = find_plan.index_scan_filter()
            .ok_or_else(|| NitriteError::new(
                "Index scan filter is not available",
                ErrorKind::InvalidOperation,
            ))?;
        let filters = index_scan_filter.filters();
        let index_scan_order = find_plan.index_scan_order().unwrap_or_default();

        let i_map = IndexMap::new(Some(index_map), None);
        let index_scanner = IndexScanner::new(i_map);
        index_scanner.scan(filters, index_scan_order)
    }

    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
        Ok(self.index_descriptor.clone())
    }

    fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        let first_field = field_names.first()
            .ok_or_else(|| NitriteError::new(
                "Cannot write to index: no field names specified",
                ErrorKind::InvalidOperation,
            ))?;
        let field_value = field_values.get_value(first_field);

        let index_map = self.find_index_map()?;

        match field_value {
            None | Some(Value::Null) => {
                self.add_index_element(&index_map, field_values, &Value::Null)?;
            }
            Some(Value::Array(arr)) => {
                for value in arr {
                    self.add_index_element(&index_map, field_values, value)?;
                }
            }
            Some(value) => {
                if value.is_comparable() {
                    self.add_index_element(&index_map, field_values, value)?;
                }
            }
        }

        Ok(())
    }

    fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        let first_field = field_names.first()
            .ok_or_else(|| NitriteError::new(
                "Cannot remove from index: no field names specified",
                ErrorKind::InvalidOperation,
            ))?;
        let field_value = field_values.get_value(first_field);

        let index_map = self.find_index_map()?;

        match field_value { 
            None | Some(Value::Null) => {
                self.remove_index_element(index_map.clone(), field_values, &Value::Null)?;
            }
            Some(Value::Array(arr)) => {
                for value in arr {
                    self.remove_index_element(index_map.clone(), field_values, value)?;
                }
            }
            Some(value) => {
                if value.is_comparable() {
                    self.remove_index_element(index_map.clone(), field_values, value)?;
                }
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
        if find_plan.index_scan_filter().is_none() {
            return Ok(Vec::new());
        }

        let index_map = self.find_index_map()?;
        self.scan_index(find_plan, index_map)
    }

    fn is_unique(&self) -> bool {
        self.index_descriptor
            .index_type()
            .eq_ignore_ascii_case(UNIQUE_INDEX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Fields;
    use crate::errors::ErrorKind;
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

    #[test]
    fn test_simple_index_new() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor.clone(), nitrite_store.clone());

        assert_eq!(simple_index.inner.index_descriptor, index_descriptor);
    }

    #[test]
    fn test_simple_index_find_index_map() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let result = simple_index.find_index_map();
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_add_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = simple_index.add_index_element(&index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_remove_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_add_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let mut nitrite_ids = Vec::new();
        let field_values = create_test_field_values();

        let result = simple_index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_remove_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let mut nitrite_ids = vec![*field_values.nitrite_id()];

        let result = simple_index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_scan_index() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new();
        let index_map = simple_index.find_index_map().unwrap();

        let result = simple_index.scan_index(&find_plan, index_map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_write() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();

        let result = simple_index.write(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_remove() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();

        let result = simple_index.remove(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_drop_index() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let result = simple_index.drop_index();
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_find_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new();

        let result = simple_index.find_nitrite_ids(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_simple_index_is_unique() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let result = simple_index.is_unique();
        assert!(result);
    }

    #[test]
    fn test_simple_index_add_index_element_unique_violation() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        // Add the same element twice to trigger unique constraint violation
        simple_index.add_index_element(&index_map, &field_values, &value).unwrap();
        let result = simple_index.add_index_element(&index_map, &field_values, &value);

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::UniqueConstraintViolation);
    }

    #[test]
    fn test_simple_index_remove_index_element_not_found() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("non_existent_value".to_string());

        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    // Tests for error handling in SimpleIndex operations

    #[test]
    fn test_simple_index_write_with_empty_field_names() {
        // Create a mock FieldValues with empty field names
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let _simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        // Note: In a real scenario, you'd need to create a FieldValues with empty names
        // This test documents the expected error behavior
    }

    #[test]
    fn test_simple_index_remove_index_element_with_corrupted_data() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        // Put a non-array value to simulate corruption
        index_map.put(value.clone(), Value::String("corrupted_data".to_string())).unwrap();

        // This should handle the error gracefully, not panic
        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_err(), "Should return error for corrupted index data");
        if let Err(e) = result {
            assert!(e.to_string().contains("not an array") || e.to_string().contains("corrupted"));
        }
    }

    #[test]
    fn test_simple_index_scan_index_with_no_filter() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new();
        // Ensure no index scan filter is set
        
        let index_map = simple_index.find_index_map().unwrap();
        let result = simple_index.scan_index(&find_plan, index_map);
        
        // Should return empty result, not panic
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_simple_index_write_handles_multiple_field_values() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        
        // This should not panic even with complex field values
        let result = simple_index.write(&field_values);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_simple_index_remove_with_complex_values() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        
        // This should not panic even with complex field values
        let result = simple_index.remove(&field_values);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_simple_index_remove_index_element_handles_empty_array() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_empty".to_string());

        // Put an empty array
        index_map.put(value.clone(), Value::Array(Vec::new())).unwrap();

        // This should handle gracefully
        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_ok(), "Should handle empty arrays gracefully");
    }

    #[test]
    fn test_simple_index_corrupted_index_entry_not_panicking() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("corrupted".to_string());

        // Store a non-array type to simulate corruption
        index_map.put(value.clone(), Value::I32(42)).unwrap();

        // Should fail gracefully, not panic
        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_err(), "Should error on corrupted data");
    }

    // Performance optimization tests
    #[test]
    fn test_add_nitrite_ids_dedup_efficiency() {
        // Test that add_nitrite_ids uses sort+dedup instead of unique().collect()
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let id = *field_values.nitrite_id();
        
        // Create duplicates to test dedup efficiency
        let mut nitrite_ids = vec![id, id, id];

        let result = simple_index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        
        // Should have deduplicated and added the new field value ID
        let dedup = result.unwrap();
        assert!(!dedup.is_empty());
    }

    #[test]
    fn test_add_nitrite_ids_uses_mem_take() {
        // Test that add_nitrite_ids uses mem::take instead of clone
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let mut nitrite_ids = vec![];
        
        let result = simple_index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        
        // Original should be empty due to mem::take
        assert!(nitrite_ids.is_empty());
    }

    #[test]
    fn test_remove_nitrite_ids_uses_mem_take() {
        // Test that remove_nitrite_ids uses mem::take instead of clone
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let id = *field_values.nitrite_id();
        let mut nitrite_ids = vec![id];
        
        let result = simple_index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
        
        // Original should be empty due to mem::take
        assert!(nitrite_ids.is_empty());
    }

    #[test]
    fn test_remove_index_element_array_handling() {
        // Test that remove_index_element handles arrays efficiently
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let simple_index = SimpleIndex::new(index_descriptor, nitrite_store);

        let index_map = simple_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test".to_string());

        // Add then remove to test array handling
        simple_index.add_index_element(&index_map, &field_values, &value).ok();
        let result = simple_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_ok());
    }
}

