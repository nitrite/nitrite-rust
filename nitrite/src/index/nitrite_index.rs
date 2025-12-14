use super::IndexDescriptor;
use crate::{
    collection::{FindPlan, NitriteId},
    errors::{ErrorKind, NitriteError, NitriteResult},
    FieldValues,
};
use std::ops::Deref;
use std::sync::Arc;

/// Provider trait for concrete index implementations.
///
/// NitriteIndexProvider defines the core contract for index storage and retrieval.
/// Each index type (simple B-Tree, compound nested, spatial) implements this trait
/// to manage the mapping between field values and document identifiers.
///
/// # Responsibilities
/// - Store and retrieve mappings from indexed field values to NitriteIds
/// - Enforce uniqueness constraints if applicable
/// - Support write/remove operations for document changes
/// - Execute filter-based queries to find matching documents
/// - Lifecycle management for index resources
pub trait NitriteIndexProvider: Send + Sync {
    /// Retrieves the metadata descriptor for this index.
    ///
    /// # Returns
    /// IndexDescriptor containing index type, field names, and collection info.
    ///
    /// # Errors
    /// Returns IndexingError if descriptor metadata is inaccessible.
    fn index_descriptor(&self) -> NitriteResult<IndexDescriptor>;

    /// Records a field value mapping in the index.
    ///
    /// # Arguments
    /// * `field_values` - The indexed field value(s) and associated NitriteId
    ///
    /// # Returns
    /// Ok(()) on success, Error on failure.
    ///
    /// # Behavior
    /// Stores the mapping from field_values to its NitriteId in the index.
    /// For unique indexes, enforces that duplicate field values are rejected.
    ///
    /// # Errors
    /// Returns IndexingError if write fails or unique constraint is violated.
    fn write(&self, field_values: &FieldValues) -> NitriteResult<()>;

    /// Removes a field value mapping from the index.
    ///
    /// # Arguments
    /// * `field_values` - The indexed field value(s) and NitriteId to remove
    ///
    /// # Returns
    /// Ok(()) on success, Error on failure.
    ///
    /// # Errors
    /// Returns IndexingError if removal fails.
    fn remove(&self, field_values: &FieldValues) -> NitriteResult<()>;

    /// Drops the entire index, releasing all associated resources.
    ///
    /// # Returns
    /// Ok(()) if successful, Error on failure.
    ///
    /// # Behavior
    /// Removes all data associated with this index. After this call,
    /// the index is no longer usable.
    ///
    /// # Errors
    /// Returns IndexingError if cleanup fails.
    fn drop_index(&self) -> NitriteResult<()>;

    /// Finds all NitriteIds matching criteria specified in the find plan.
    ///
    /// # Arguments
    /// * `find_plan` - Query plan with filters to evaluate
    ///
    /// # Returns
    /// Vector of NitriteIds matching all filter criteria, empty if none match.
    ///
    /// # Behavior
    /// Evaluates filters against indexed data and returns matching document IDs.
    /// Results are deduplicated and consistently ordered.
    ///
    /// # Errors
    /// Returns IndexingError if query execution fails.
    fn find_nitrite_ids(&self, find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>>;

    /// Returns whether this index enforces uniqueness on indexed field values.
    ///
    /// # Returns
    /// `true` for unique indexes, `false` for non-unique indexes.
    ///
    /// # Behavior
    /// Used to determine if duplicate field values should be rejected.
    /// Unique indexes will return error from write() if value exists.
    fn is_unique(&self) -> bool;

    /// Adds a NitriteId to the given list if uniqueness constraints allow it.
    ///
    /// # Arguments
    /// * `nitrite_ids` - Mutable vector to add the ID to
    /// * `field_values` - The field values context for uniqueness checking
    ///
    /// # Returns
    /// The modified nitrite_ids vector with the new ID added, or error.
    ///
    /// # Behavior
    /// For unique indexes with existing values, returns IndexingError.
    /// For non-unique or non-conflicting unique indexes, appends ID using
    /// mem::take for efficiency (moves vector ownership).
    ///
    /// # Errors
    /// Returns IndexingError if unique constraint is violated.
    fn add_nitrite_ids(
        &self,
        nitrite_ids: &mut Vec<NitriteId>,
        field_values: &FieldValues,
    ) -> NitriteResult<Vec<NitriteId>> {
        if self.is_unique() && nitrite_ids.len() == 1 {
            // if key is already exists for unique type, throw error
            log::error!("Unique constraint violated for {:?}", field_values);
            return Err(NitriteError::new(
                &format!("Unique constraint violated for {:?}", field_values),
                ErrorKind::IndexingError,
            ));
        }

        // index always are in ascending format
        nitrite_ids.push(field_values.nitrite_id().clone());
        Ok(std::mem::take(nitrite_ids))
    }

    /// Removes a NitriteId from the given list if it matches field values.
    ///
    /// # Arguments
    /// * `nitrite_ids` - Mutable vector to remove ID from
    /// * `field_values` - The field values and NitriteId to match and remove
    ///
    /// # Returns
    /// The modified nitrite_ids vector with matching ID removed, or error.
    ///
    /// # Behavior
    /// Filters out the matching NitriteId using retain. If entry doesn't exist,
    /// silently succeeds. Uses mem::take for efficient vector ownership transfer.
    ///
    /// # Errors
    /// Returns IndexingError only if an unexpected error occurs during filtering.
    fn remove_nitrite_ids(
        &self,
        nitrite_ids: &mut Vec<NitriteId>,
        field_values: &FieldValues,
    ) -> NitriteResult<Vec<NitriteId>> {
        if !nitrite_ids.is_empty() {
            nitrite_ids.retain(|x| x != field_values.nitrite_id());
        }
        Ok(std::mem::take(nitrite_ids))
    }
}

/// Wrapper for concrete index implementations with thread-safe reference counting.
///
/// NitriteIndex provides a unified interface for low-level index operations by
/// wrapping concrete index implementations. It enables different index types
/// (simple B-Tree, compound nested, spatial) to be used interchangeably through
/// the NitriteIndexProvider trait.
///
/// # Characteristics
/// - **Type-agnostic**: Handles any index type implementing NitriteIndexProvider
/// - **Thread-safe**: Safely shared across threads via Arc
/// - **Cloneable**: Cheap cloning via shared Arc reference
/// - **Direct mapping**: Maps field values directly to NitriteIds
///
/// # Relationship to NitriteIndexer
///
/// NitriteIndex provides lower-level storage operations (write, remove, find),
/// while NitriteIndexer wraps indexer plugins that handle higher-level concerns
/// like validation and lifecycle management.
///
/// # Deref Behavior
///
/// NitriteIndex implements Deref to transparently access NitriteIndexProvider
/// methods through the wrapper, allowing seamless method calls on the underlying
/// implementation.
#[derive(Clone)]
pub struct NitriteIndex {
    inner: Arc<dyn NitriteIndexProvider>,
}

impl NitriteIndex {
    /// Creates a new NitriteIndex wrapping a concrete implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete type implementing NitriteIndexProvider
    ///
    /// # Returns
    /// A new NitriteIndex wrapping the provided implementation in an Arc
    /// for thread-safe reference counting.
    ///
    /// # Behavior
    /// Wraps the implementation in Arc<dyn NitriteIndexProvider> to enable
    /// polymorphic index usage. The implementation remains accessible through
    /// Deref without explicit unwrapping.
    pub fn new<T: NitriteIndexProvider + 'static>(inner: T) -> Self {
        NitriteIndex { inner: Arc::new(inner) }
    }
}

impl Deref for NitriteIndex {
    type Target = Arc<dyn NitriteIndexProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Convertible, Fields, UNIQUE_INDEX};
    use crate::errors::ErrorKind;

    struct MockNitriteIndex;

    impl NitriteIndexProvider for MockNitriteIndex {
        fn index_descriptor(&self) -> NitriteResult<IndexDescriptor> {
            Ok(IndexDescriptor::new(
                UNIQUE_INDEX,
                Fields::with_names(vec!["test_field"])?,
                "test",
            ))
        }

        fn write(&self, _field_values: &FieldValues) -> NitriteResult<()> {
            Ok(())
        }

        fn remove(&self, _field_values: &FieldValues) -> NitriteResult<()> {
            Ok(())
        }

        fn drop_index(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn find_nitrite_ids(&self, _find_plan: &FindPlan) -> NitriteResult<Vec<NitriteId>> {
            Ok(vec![NitriteId::new()])
        }

        fn is_unique(&self) -> bool {
            true
        }
    }

    #[test]
    fn test_index_descriptor() {
        let index = NitriteIndex::new(MockNitriteIndex);
        assert!(index.index_descriptor().is_ok());
    }

    #[test]
    fn test_write() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        assert!(index.write(&field_values).is_ok());
    }

    #[test]
    fn test_remove() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        assert!(index.remove(&field_values).is_ok());
    }

    #[test]
    fn test_drop_index() {
        let index = NitriteIndex::new(MockNitriteIndex);
        assert!(index.drop_index().is_ok());
    }

    #[test]
    fn test_find_nitrite_ids() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let find_plan = FindPlan::new();
        assert!(index.find_nitrite_ids(&find_plan).is_ok());
    }

    #[test]
    fn test_is_unique() {
        let index = NitriteIndex::new(MockNitriteIndex);
        assert!(index.is_unique());
    }

    #[test]
    fn test_add_nitrite_ids_unique_violation() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![NitriteId::new()];
        let result = index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::IndexingError);
    }

    #[test]
    fn test_add_nitrite_ids() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![];
        let result = index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_remove_nitrite_ids() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![field_values.nitrite_id().clone()];
        let result = index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_remove_nitrite_ids_not_found() {
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![NitriteId::new()];
        let result = index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    // Performance optimization tests
    #[test]
    fn test_add_nitrite_ids_uses_mem_take() {
        // Test that add_nitrite_ids uses mem::take instead of clone
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![];
        let result = index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
        // Original should be empty due to mem::take
        assert!(nitrite_ids.is_empty());
    }

    #[test]
    fn test_remove_nitrite_ids_uses_mem_take() {
        // Test that remove_nitrite_ids uses mem::take instead of clone
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids = vec![field_values.nitrite_id().clone()];
        let result = index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
        // Original should be empty due to mem::take
        assert!(nitrite_ids.is_empty());
    }

    #[test]
    fn test_add_nitrite_ids_multiple_entries() {
        // Test that add_nitrite_ids efficiently handles multiple IDs
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let mut nitrite_ids: Vec<NitriteId> = (0..10)
            .map(|_| NitriteId::new())
            .collect();
        
        let original_len = nitrite_ids.len();
        let result = index.add_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), original_len + 1);
    }

    #[test]
    fn test_remove_nitrite_ids_batch_retention() {
        // Test that remove_nitrite_ids efficiently filters multiple entries
        let index = NitriteIndex::new(MockNitriteIndex);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        
        let mut nitrite_ids = vec![
            field_values.nitrite_id().clone(),
            NitriteId::new(),
            NitriteId::new(),
        ];
        
        let result = index.remove_nitrite_ids(&mut nitrite_ids, &field_values);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 2);
    }
}
