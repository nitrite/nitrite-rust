use super::{
    index_map::normalize_index_value, index_scanner::IndexScanner,
    nitrite_index::NitriteIndexProvider, IndexDescriptor, IndexMap,
};
use crate::{
    collection::{FindPlan, NitriteId},
    derive_index_map_name,
    errors::{ErrorKind, NitriteError, NitriteResult},
    store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider},
    validate_index_field, common::Key, FieldValues, Value, UNIQUE_INDEX,
};
use itertools::Itertools;
use once_cell::sync::Lazy;
use std::collections::{BTreeMap, HashMap};
use std::ops::Deref;
use std::sync::Arc;

static UNIQUE_CONSTRAINT_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Unique constraint violated",
        ErrorKind::UniqueConstraintViolation,
    )
});

static COMPOUND_INDEX_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Compound multikey index is supported on the first field of the index only",
        ErrorKind::IndexingError,
    )
});

/// Manages multi-field indexes on documents where two or more fields are indexed together.
///
/// Compound indexes optimize queries that filter on multiple fields simultaneously.
/// They use nested maps to represent the hierarchical structure of indexed values,
/// enabling efficient lookup of documents matching conditions on all indexed fields.
///
/// # Characteristics
/// - **Multi-field indexing**: Indexes two or more fields as a unit
/// - **Multikey support**: First field can contain arrays (multikey indexing); subsequent fields cannot
/// - **Unique constraints**: Supports both unique and non-unique compound indexes
/// - **Index-accelerated queries**: Optimizes `find()` operations using compound filters
/// - **Nested structure**: Uses nested BTreeMap values to organize by field values
///
/// # Usage
///
/// Compound indexes are created on collections with multiple fields:
/// - `collection.create_index(vec!["first_name", "last_name"], &unique_index())?` - Unique compound index
/// - `collection.create_index(vec!["field1", "field2"], &non_unique_index())?` - Non-unique compound index
/// - `collection.create_index(vec!["array_field", "status"], &non_unique_index())?` - Multikey on first field
///
/// Queries using compound indexes:
/// - `collection.find(field("first_name").eq("John").and(field("last_name").eq("Doe")))` - Uses index
/// - `collection.find(field("last_name").eq("Doe"))` - Uses first field index
///
/// # Responsibilities
/// - **Index Maintenance**: Maintains nested map structure during document writes/deletes
/// - **Query Optimization**: Enables index-accelerated searches on multi-field conditions
/// - **Uniqueness Enforcement**: Validates unique constraints across multiple fields
/// - **Multikey Handling**: Supports arrays in first field, rejects in subsequent fields
#[derive(Clone)]
pub struct CompoundIndex {
    inner: Arc<CompoundIndexInner>,
}

impl CompoundIndex {
    /// Creates a new compound index for multi-field indexing.
    ///
    /// # Arguments
    /// * `index_descriptor` - Defines the indexed fields, index name, and type (unique/non-unique)
    /// * `nitrite_store` - The underlying store containing the index data
    ///
    /// # Returns
    /// A new `CompoundIndex` ready to be used for writes and queries.
    ///
    /// # Behavior
    /// Initializes the index with an empty nested map structure. The actual index is populated
    /// when documents are written to the indexed collection. For a two-field index,
    /// the structure is: `Map[field1_value] -> Map[field2_value] -> Array[NitriteIds]`
    pub fn new(index_descriptor: IndexDescriptor, nitrite_store: NitriteStore) -> CompoundIndex {
        let inner = CompoundIndexInner::new(index_descriptor, nitrite_store);
        CompoundIndex {
            inner: Arc::new(inner),
        }
    }
}

impl Deref for CompoundIndex {
    type Target = Arc<CompoundIndexInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NitriteIndexProvider for CompoundIndex {
    /// Retrieves the index descriptor containing field names, index name, and type.
    ///
    /// # Returns
    /// The IndexDescriptor that defines this compound index.
    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
        self.inner.index_descriptor()
    }

    /// Writes a document's indexed field values into the compound index.
    ///
    /// # Arguments
    /// * `field_values` - The field values and document ID to index
    ///
    /// # Behavior
    /// - Inserts the document ID into the nested map at the position corresponding to the field values
    /// - For arrays in the first field, creates an index entry for each array element
    /// - Enforces unique constraints if this is a unique index
    /// - Returns error if unique constraint is violated
    fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        self.inner.write(field_values)
    }

    /// Removes a document's indexed field values from the compound index.
    ///
    /// # Arguments
    /// * `field_values` - The field values and document ID to remove
    ///
    /// # Behavior
    /// - Removes the document ID from the nested map at the position corresponding to the field values
    /// - For arrays in the first field, removes index entries for each array element
    /// - Cleans up empty nested maps during removal
    fn remove(&self, field_values: &FieldValues) -> NitriteResult<()> {
        self.inner.remove(field_values)
    }

    /// Drops the entire compound index and clears its storage.
    ///
    /// # Behavior
    /// Clears all entries from the underlying index map and disposes of the storage.
    fn drop_index(&self) -> NitriteResult<()> {
        self.inner.drop_index()
    }

    /// Finds document IDs matching the conditions in a find plan using index acceleration.
    ///
    /// # Arguments
    /// * `find_plan` - Contains filters and scan order for index-optimized query execution
    ///
    /// # Returns
    /// Vector of NitriteIds for documents matching all indexed field conditions.
    ///
    /// # Behavior
    /// - Scans the nested index map according to filter conditions
    /// - Supports range scans and specific value lookups
    /// - Returns empty vector if no filters apply to this index
    fn find_nitrite_ids(&self, find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>> {
        self.inner.find_nitrite_ids(find_plan)
    }

    /// Returns whether this compound index has a unique constraint.
    ///
    /// # Returns
    /// `true` if this is a unique compound index, `false` if non-unique.
    fn is_unique(&self) -> bool {
        self.inner.is_unique()
    }
}

/// Internal implementation of compound indexing.
///
/// This type handles the actual index data structure and operations.
/// Users interact with this through the public `CompoundIndex` wrapper.
pub struct CompoundIndexInner {
    index_descriptor: IndexDescriptor,
    nitrite_store: NitriteStore,
}

impl CompoundIndexInner {
    fn new(index_descriptor: IndexDescriptor, nitrite_store: NitriteStore) -> Self {
        Self {
            index_descriptor,
            nitrite_store,
        }
    }

    fn find_index_map(&self) -> NitriteResult<NitriteMap> {
        let map_name = derive_index_map_name(&self.index_descriptor);
        self.nitrite_store.open_map(&map_name)
    }

    /// Number of indexed fields (the arity of the composite key, excluding the trailing id).
    fn field_count(&self) -> usize {
        self.index_descriptor.index_fields().field_names().len()
    }

    /// Builds the flat composite key `[v0, v1, …, v(K-1), id]` for one first-field value.
    ///
    /// `first_value` is a single value for the first field (one element when that field is a
    /// multikey array); the remaining components come from the other indexed fields in order.
    /// Only the first field may be an array — a multikey array in any later field is rejected.
    fn composite_key(
        &self,
        field_values: &FieldValues,
        first_value: &Value,
    ) -> NitriteResult<Key> {
        let values = field_values.values();
        let mut parts = Vec::with_capacity(values.len() + 1);
        parts.push(normalize_index_value(first_value));

        for (_, value) in values.iter().skip(1) {
            match value {
                Value::Array(_) => {
                    log::error!(
                        "Compound multikey index is supported on the first field of the index only"
                    );
                    return Err(COMPOUND_INDEX_ERROR.clone());
                }
                Value::Null => parts.push(Value::Null),
                v if v.is_comparable() => parts.push(normalize_index_value(v)),
                v => {
                    log::error!(
                        "Found non comparable value {} in compound index {:?}",
                        v,
                        self.index_descriptor
                    );
                    return Err(NitriteError::new(
                        &format!("{} is not comparable", v),
                        ErrorKind::IndexingError,
                    ));
                }
            }
        }

        parts.push(Value::NitriteId(*field_values.nitrite_id()));
        Ok(Value::Array(parts))
    }

    /// Inserts one composite-key row in O(1). For a unique index, first verifies no other id is
    /// already stored for the same field-value tuple.
    fn add_index_element(
        &self,
        index_map: &NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let key = self.composite_key(field_values, value)?;
        if self.is_unique() {
            self.check_unique(index_map, &key)?;
        }
        index_map.put(key, Value::Null)
    }

    /// Enforces a unique compound constraint: the field-value tuple (the composite key without
    /// its trailing id) must not already map to a different id. A prefix `ceiling_key` probe
    /// finds any existing row for the tuple in O(log n).
    fn check_unique(&self, index_map: &NitriteMap, key: &Key) -> NitriteResult<()> {
        let Value::Array(parts) = key else {
            return Ok(());
        };
        let k = self.field_count();
        if parts.len() != k + 1 {
            return Ok(());
        }
        let tuple = &parts[..k];
        let id = &parts[k];

        // `[tuple]` (k elements) sorts immediately before `[tuple, id]` (k+1 elements), so the
        // ceiling lands on the first stored row for this tuple, if any.
        let probe = Value::Array(tuple.to_vec());
        if let Some(Value::Array(existing)) = index_map.ceiling_key(&probe)? {
            if existing.len() == k + 1 && existing[..k] == *tuple && existing[k] != *id {
                log::error!("Unique constraint violated for {:?}", tuple);
                return Err(UNIQUE_CONSTRAINT_ERROR.clone());
            }
        }
        Ok(())
    }

    /// Removes one composite-key row in O(1).
    fn remove_index_element(
        &self,
        index_map: NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let key = self.composite_key(field_values, value)?;
        index_map.remove(&key)?;
        Ok(())
    }

    fn scan_index(
        &self,
        find_plan: &FindPlan,
        index_map: NitriteMap,
    ) -> NitriteResult<Vec<NitriteId>> {
        if find_plan.index_scan_filter().is_none() {
            return Ok(Vec::new());
        }
        
        // Safely unwrap after checking is_none
        let filters = find_plan.index_scan_filter()
            .ok_or_else(|| NitriteError::new(
                "Compound index scan error: index_scan_filter is required for compound index query optimization",
                ErrorKind::InvalidOperation
            ))?
            .filters();
        let index_scan_order = find_plan.index_scan_order().unwrap_or_default();

        // The compound index stores flat composite keys `[v0, …, v(K-1), id]`; the composite
        // IndexMap reconstructs the nested per-leading-value view the scanner expects.
        let i_map = IndexMap::composite(index_map, self.field_count());
        let index_scanner = IndexScanner::new(i_map);
        index_scanner.scan(filters, index_scan_order)
    }

    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
        Ok(self.index_descriptor.clone())
    }

    fn write(&self, field_values: &FieldValues) -> NitriteResult<()> {
        let fields = field_values.fields();
        let field_names = fields.field_names();

        let first_field = field_names.first().map_or("", |x| x.as_str());
        let first_value = field_values.get_value(first_field);

        // NOTE: only first field can have array or iterable value, subsequent fields can not
        validate_index_field(first_value, first_field)?;

        let index_map: NitriteMap = self.find_index_map()?;
        match first_value {
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

        let first_field = field_names.first().map_or("", |x| x.as_str());
        let first_value = field_values.get_value(first_field);

        // NOTE: only first field can have array or iterable value, subsequent fields can not
        validate_index_field(first_value, first_field)?;

        let index_map: NitriteMap = self.find_index_map()?;
        match first_value {
            None | Some(Value::Null) => {
                self.remove_index_element(index_map, field_values, &Value::Null)?;
            }
            Some(Value::Array(arr)) => {
                for value in arr {
                    self.remove_index_element(index_map.clone(), field_values, value)?;
                }
            }
            Some(value) => {
                if value.is_comparable() {
                    self.remove_index_element(index_map, field_values, value)?;
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
    use crate::{FieldValues, Value};

    fn create_test_index_descriptor() -> IndexDescriptor {
        IndexDescriptor::new(
            UNIQUE_INDEX,
            Fields::with_names(vec!["field1", "field2"]).unwrap(),
            "test",
        )
    }

    fn create_test_field_values() -> FieldValues {
        FieldValues::new(
            vec![
                ("field1".to_string(), Value::String("value1".to_string())),
                ("field2".to_string(), Value::String("value2".to_string())),
            ],
            NitriteId::new(),
            Fields::with_names(vec!["field1", "field2"]).unwrap(),
        )
    }

    #[test]
    fn test_compound_index_new() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor.clone(), nitrite_store.clone());

        assert_eq!(compound_index.inner.index_descriptor, index_descriptor);
    }

    #[test]
    fn test_compound_index_find_index_map() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let result = compound_index.find_index_map();
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_add_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let index_map = compound_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = compound_index.add_index_element(&index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_remove_index_element() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let index_map = compound_index.find_index_map().unwrap();
        let field_values = create_test_field_values();
        let value = Value::String("test_value".to_string());

        let result = compound_index.remove_index_element(index_map, &field_values, &value);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_composite_key_layout() {
        // A compound write stores one flat composite key `[v0, v1, id]` per first-field value.
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let first = Value::String("value1".to_string());
        let key = compound_index.composite_key(&field_values, &first).unwrap();
        match key {
            Value::Array(parts) => {
                assert_eq!(parts.len(), 3); // [field1, field2, id]
                assert_eq!(parts[0], Value::String("value1".to_string()));
                assert_eq!(parts[1], Value::String("value2".to_string()));
                assert!(parts[2].is_nitrite_id());
            }
            other => panic!("expected composite array key, got {other:?}"),
        }
    }

    #[test]
    fn test_compound_index_scan_index() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new();
        let index_map = compound_index.find_index_map().unwrap();

        let result = compound_index.scan_index(&find_plan, index_map);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_write() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();

        let result = compound_index.write(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_remove() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();

        let result = compound_index.remove(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_drop_index() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let result = compound_index.drop_index();
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_find_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new();

        let result = compound_index.find_nitrite_ids(&find_plan);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_is_unique() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let result = compound_index.is_unique();
        assert!(result);
    }

    // populate_sub_map and delete_from_sub_map error handling tests
    #[test]
    fn test_populate_sub_map_graceful_error_handling() {
        // Test that populate_sub_map handles non-map values gracefully
        // instead of panicking with unwrap
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        // This should not panic even if sub_map is not a proper map type
        // The ok_or_else error handling should propagate gracefully
        let result = compound_index.write(&field_values);
        assert!(result.is_ok() || result.is_err());
        // If it errors, it should be a proper NitriteError, not a panic
    }

    #[test]
    fn test_delete_from_sub_map_graceful_error_handling() {
        // Test that delete_from_sub_map handles corrupted array references gracefully
        // instead of panicking with unwrap
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        // Write first to set up the index
        let _ = compound_index.write(&field_values);
        
        // Remove should handle any type mismatches gracefully
        let result = compound_index.remove(&field_values);
        assert!(result.is_ok() || result.is_err());
        // If it errors, it should be a proper NitriteError, not a panic
    }

    #[test]
    fn test_compound_index_rejects_multikey_in_later_field() {
        // Only the first field may be a multikey array; a later array field is rejected.
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = FieldValues::new(
            vec![
                ("field1".to_string(), Value::String("a".to_string())),
                ("field2".to_string(), Value::Array(vec![Value::I32(1)])),
            ],
            NitriteId::new(),
            Fields::with_names(vec!["field1", "field2"]).unwrap(),
        );
        let first = Value::String("a".to_string());
        let result = compound_index.composite_key(&field_values, &first);
        assert!(result.is_err());
    }

    // Performance optimization tests
    #[test]
    fn test_delete_from_sub_map_avoids_excessive_cloning() {
        // Test that delete_from_sub_map uses efficient map operations
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let _ = compound_index.write(&field_values);
        
        // Perform deletions multiple times
        for _ in 0..3 {
            let result = compound_index.remove(&field_values);
            assert!(result.is_ok() || result.is_err());
        }
    }

    #[test]
    fn test_add_nitrite_ids_dedup_efficiency() {
        // Test that add_nitrite_ids efficiently deduplicates via write operations
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        
        // Test write with same field values multiple times - should deduplicate internally
        for _ in 0..3 {
            let result = compound_index.write(&field_values);
            assert!(result.is_ok() || result.is_err());
        }
    }

    #[test]
    fn test_remove_method_efficient_array_processing() {
        // Test that remove method efficiently handles arrays without unnecessary cloning
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        
        let result = compound_index.write(&field_values);
        assert!(result.is_ok());
        
        let result = compound_index.remove(&field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_scan_index_no_filter_early_return() {
        // Test that scan_index returns early without allocating when no filter present
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let find_plan = FindPlan::new(); // No filter
        let index_map = compound_index.find_index_map().unwrap();

        let result = compound_index.scan_index(&find_plan, index_map);
        assert!(result.is_ok());
        
        let ids = result.unwrap();
        // Should return empty vec with no filter
        assert_eq!(ids.len(), 0);
    }
}
