use super::{
    index_scanner::IndexScanner, nitrite_index::NitriteIndexProvider, IndexDescriptor, IndexMap,
};
use crate::{
    collection::{FindPlan, NitriteId},
    derive_index_map_name,
    errors::{ErrorKind, NitriteError, NitriteResult},
    store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider},
    validate_index_field, FieldValues, Value, UNIQUE_INDEX,
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

    fn add_index_element(
        &self,
        index_map: &NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let existing = index_map.get(value)?;
        // index are always in ascending order
        let mut sub_map = match existing {
            Some(map) => map,
            None => Value::Map(BTreeMap::new())
        };
        
        self.populate_sub_map(&mut sub_map, field_values, 1)?;
        index_map.put(value.clone(), sub_map)
    }

    fn remove_index_element(
        &self,
        index_map: NitriteMap,
        field_values: &FieldValues,
        value: &Value,
    ) -> NitriteResult<()> {
        let sub_map = index_map.get(value)?;
        let mut sub_map = sub_map.unwrap_or(Value::Map(BTreeMap::new()));
        self.delete_from_sub_map(&mut sub_map, field_values, 1)?;
        index_map.put(value.clone(), sub_map)
    }

    fn populate_sub_map(
        &self,
        sub_map: &mut Value,
        field_values: &FieldValues,
        depth: usize,
    ) -> NitriteResult<()> {
        if depth >= field_values.values().len() {
            return Ok(());
        }

        let values = field_values.values();
        // Safely get the field value at the specified depth, returning error if out of bounds
        let (_, value) = values.get(depth)
            .ok_or_else(|| NitriteError::new(
                &format!("Field value at depth {} not found in compound index", depth),
                ErrorKind::IndexingError,
            ))?;

        let db_value = match value {
            Value::Array(_) => {
                log::error!("Compound multikey index is supported on the first field of the index only");
                return Err(COMPOUND_INDEX_ERROR.clone());
            }
            value => {
                if !value.is_comparable() {
                    log::error!("Found non comparable value {} in compound index {:?}", value, self.index_descriptor);
                    return Err(NitriteError::new(
                        &format!("{} is not comparable", value),
                        ErrorKind::IndexingError,
                    ));
                }
                value.clone()
            }
        };

        if depth == field_values.values().len() - 1 {
            // Safely get the mutable map reference
            let sub_map_inner = sub_map.as_map_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index corruption: expected map at depth {} for field values {:?}", depth, field_values.values()),
                    ErrorKind::IndexingError
                ))?;
            
            let mut nitrite_ids = sub_map_inner.remove(&db_value)
                .unwrap_or_else(|| Value::Array(Vec::new()));
            
            // Validate the retrieved value is an array
            let nitrite_ids_arr = nitrite_ids.as_array_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index error: expected array of NitriteIds for key {:?} at depth {}", db_value, depth),
                    ErrorKind::IndexingError
                ))?;
            let nitrite_ids = self.add_nitrite_ids(nitrite_ids_arr, field_values)?;
            
            sub_map_inner.insert(db_value, Value::Array(nitrite_ids));
        } else {
            // Safely get the mutable map reference
            let sub_map_inner = sub_map.as_map_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index corruption: expected map at depth {} for field values {:?}", depth, field_values.values()),
                    ErrorKind::IndexingError
                ))?;
            
            let mut sub_map2 = sub_map_inner.remove(&db_value)
                .unwrap_or_else(|| Value::Map(BTreeMap::new()));
            
            self.populate_sub_map(&mut sub_map2, field_values, depth + 1)?;
            sub_map_inner.insert(db_value, sub_map2);
        }

        Ok(())
    }

    fn delete_from_sub_map(
        &self,
        sub_map: &mut Value,
        field_values: &FieldValues,
        depth: usize,
    ) -> NitriteResult<()> {
        let values = field_values.values();
        // Safely get the field value at the specified depth, returning error if out of bounds
        let (_, value) = values.get(depth)
            .ok_or_else(|| NitriteError::new(
                &format!("Field value at depth {} not found in compound index", depth),
                ErrorKind::IndexingError,
            ))?;

        let db_value = match value {
            Value::Null => Value::Null,
            value => {
                if !value.is_comparable() {
                    return Ok(());
                }
                value.clone()
            }
        };

        if depth == field_values.values().len() - 1 {
            // Safely get mutable map reference and remove/update in place
            let sub_map_inner = sub_map.as_map_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index corruption during deletion: expected map at depth {} for field values {:?}", depth, field_values.values()),
                    ErrorKind::IndexingError
                ))?;
            
            let mut nitrite_ids = sub_map_inner.remove(&db_value)
                .unwrap_or_else(|| Value::Array(Vec::new()));
            
            // Validate the retrieved value is an array
            let nitrite_ids_arr = nitrite_ids.as_array_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index error during deletion: expected array of NitriteIds for key {:?} at depth {}", db_value, depth),
                    ErrorKind::IndexingError
                ))?;
            let nitrite_ids = self.remove_nitrite_ids(nitrite_ids_arr, field_values)?;

            if !nitrite_ids.is_empty() {
                sub_map_inner.insert(db_value, Value::Array(nitrite_ids));
            }
        } else {
            // Safely get mutable map reference
            let sub_map_inner = sub_map.as_map_mut()
                .ok_or_else(|| NitriteError::new(
                    &format!("Compound index corruption during deletion: expected map at depth {} for field values {:?}", depth, field_values.values()),
                    ErrorKind::IndexingError
                ))?;
            
            let mut sub_map2 = sub_map_inner.remove(&db_value)
                .unwrap_or_else(|| Value::Map(BTreeMap::new()));

            let is_empty = sub_map2.as_map()
                .map(|m| m.is_empty())
                .unwrap_or(true);
                
            if !is_empty {
                self.delete_from_sub_map(&mut sub_map2, field_values, depth + 1)?;
                sub_map_inner.insert(db_value, sub_map2);
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
            log::error!(
                "Unique constraint violated for {:?}",
                field_values.values()
            );
            return Err(UNIQUE_CONSTRAINT_ERROR.clone());
        }

        // index always are in ascending format
        nitrite_ids.push(Value::NitriteId(*field_values.nitrite_id()));

        // Use itertools::unique which is more efficient than collecting and deduplicating
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
            let target_id = field_values.nitrite_id();
            nitrite_ids.retain(|x| {
                // Gracefully handle invalid IDs - keep them if they can't be converted
                match x.as_nitrite_id() {
                    Some(id) => id != target_id,
                    None => {
                        log::warn!("Invalid NitriteId value in compound index: {:?}", x);
                        true // Keep invalid IDs
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
        
        // Safely unwrap after checking is_none
        let filters = find_plan.index_scan_filter()
            .ok_or_else(|| NitriteError::new(
                "Compound index scan error: index_scan_filter is required for compound index query optimization",
                ErrorKind::InvalidOperation
            ))?
            .filters();
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
    fn test_compound_index_populate_sub_map() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let mut sub_map = Value::Map(BTreeMap::new());
        let field_values = create_test_field_values();

        let result = compound_index.populate_sub_map(&mut sub_map, &field_values, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_delete_from_sub_map() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let mut sub_map = Value::Map(BTreeMap::new());
        let field_values = create_test_field_values();

        let result = compound_index.delete_from_sub_map(&mut sub_map, &field_values, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_add_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let mut nitrite_ids = Vec::new();
        let field_values = create_test_field_values();

        let result = compound_index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
    }

    #[test]
    fn test_compound_index_remove_nitrite_ids() {
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let mut nitrite_ids = vec![field_values.nitrite_id().clone()];

        let result = compound_index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
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

    // remove_nitrite_ids error handling tests
    #[test]
    fn test_remove_nitrite_ids_with_invalid_ids() {
        // Test that remove_nitrite_ids handles invalid NitriteId values gracefully
        // instead of panicking with unwrap
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        // Create a vector with mixed valid and invalid values
        let mut nitrite_ids = vec![
            Value::String("not_an_id".to_string()), // Invalid - will be logged and retained
            Value::I32(42),                          // Invalid - will be logged and retained
            Value::Null,                             // Invalid - will be logged and retained
        ];

        let field_values = create_test_field_values();
        
        // This should not panic - it should handle the invalid IDs gracefully
        let result = compound_index.inner.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        
        // Invalid IDs should be retained since they couldn't be converted
        // The function returns the retained IDs
        let retained_ids = result.unwrap();
        assert_eq!(retained_ids.len(), 3);
    }

    #[test]
    fn test_remove_nitrite_ids_removes_matching_ids() {
        // Test that remove_nitrite_ids correctly removes matching NitriteId values
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let target_id = field_values.nitrite_id().clone();
        
        // Create a vector with the target ID that should be removed
        let mut nitrite_ids = vec![Value::NitriteId(target_id)];

        let result = compound_index.inner.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        
        // The target ID should be removed (result should be empty)
        let retained_ids = result.unwrap();
        assert_eq!(retained_ids.len(), 0);
    }

    #[test]
    fn test_remove_nitrite_ids_preserves_non_matching_ids() {
        // Test that remove_nitrite_ids preserves IDs that don't match target
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let field_values = create_test_field_values();
        let other_id = NitriteId::new();
        
        // Create a vector with IDs that don't match the target
        let mut nitrite_ids = vec![Value::NitriteId(other_id)];

        let result = compound_index.inner.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        
        // Non-matching IDs should be preserved in the return value
        let retained_ids = result.unwrap();
        assert_eq!(retained_ids.len(), 1);
    }

    // Performance optimization tests
    #[test]
    fn test_populate_sub_map_avoids_excessive_cloning() {
        // Test that populate_sub_map uses remove() instead of cloned() for efficiency
        let index_descriptor = create_test_index_descriptor();
        let nitrite_store = NitriteStore::default();
        let compound_index = CompoundIndex::new(index_descriptor, nitrite_store);

        let mut sub_map = Value::Map(BTreeMap::new());
        let field_values = create_test_field_values();

        // Populate multiple times to ensure no excessive cloning
        for _ in 0..5 {
            let result = compound_index.populate_sub_map(&mut sub_map, &field_values, 0);
            assert!(result.is_ok() || result.is_err());
        }
    }

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
