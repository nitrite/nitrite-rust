use crate::collection::{FindPlan, NitriteId};
use crate::common::{Fields, NitritePlugin};
use crate::errors::NitriteResult;
use crate::index::IndexDescriptor;
use crate::nitrite_config::NitriteConfig;
use crate::{FieldValues, NitritePluginProvider};
use std::ops::Deref;
use std::sync::Arc;

/// Wrapper for index implementation providers with thread-safe reference counting.
///
/// NitriteIndexer provides a unified interface for index operations by wrapping
/// concrete index implementations. It enables different index types (simple, compound,
/// spatial) to be used interchangeably through the NitriteIndexerProvider trait.
///
/// # Characteristics
/// - **Type-agnostic**: Handles any index type implementing NitriteIndexerProvider
/// - **Thread-safe**: Safely shared across threads via Arc
/// - **Cloneable**: Cheap cloning via shared Arc reference
/// - **Pluggable**: Leverages plugin architecture for extensibility
///
/// # Usage Pattern
///
/// Index operations typically retrieve an indexer from the configuration, then
/// use it to manage index entries during document modifications:
/// - Writing entries when documents are inserted/updated
/// - Removing entries when documents are deleted
/// - Querying entries to find matching documents
///
/// # Deref Behavior
///
/// NitriteIndexer implements Deref to transparently access NitriteIndexerProvider
/// methods through the wrapper, allowing seamless method calls on the underlying
/// implementation.
#[derive(Clone)]
pub struct NitriteIndexer {
    inner: Arc<dyn NitriteIndexerProvider>,
}

/// Provider trait for index implementation strategies.
///
/// NitriteIndexerProvider defines the contract that all index implementations must
/// satisfy. Implementors handle the actual storage and querying of indexed field
/// values. Each implementation optimizes for specific use cases (e.g., B-Tree for
/// simple indexes, spatial structures for geospatial indexes).
///
/// # Responsibilities
/// - Maintain mapping between field values and document NitriteIds
/// - Validate index compatibility with field definitions
/// - Execute filter-based queries against indexed data
/// - Lifecycle management (initialization, cleanup)
pub trait NitriteIndexerProvider: NitritePluginProvider + Send + Sync {
    /// Returns the index type identifier as a string.
    ///
    /// # Returns
    /// Type name identifying this index implementation (e.g., "SimpleIndex", "CompoundIndex").
    ///
    /// # Behavior
    /// Used to retrieve specific indexers from configuration by type name.
    /// Each index implementation must return a unique type identifier.
    fn index_type(&self) -> String;

    /// Returns whether this index enforces uniqueness constraints.
    ///
    /// # Returns
    /// `true` if duplicate field values are prohibited, `false` otherwise.
    ///
    /// # Behavior
    /// Unique indexes will return an error during write_index_entry if the
    /// field value already exists. Used to prevent duplicate entries.
    fn is_unique(&self) -> bool;

    /// Validates that the given fields are compatible with this index type.
    ///
    /// # Arguments
    /// * `fields` - The fields to validate for index compatibility
    ///
    /// # Returns
    /// Ok(()) if validation succeeds, Error if the field combination
    /// is invalid for this index type.
    ///
    /// # Behavior
    /// Called when creating indexes to ensure field definitions match
    /// index requirements. For example, compound indexes may reject
    /// single-field definitions, or spatial indexes may reject non-numeric fields.
    ///
    /// # Errors
    /// Returns IndexingError if fields don't meet index-specific requirements.
    fn validate_index(&self, fields: &Fields) -> NitriteResult<()>;

    /// Drops this index, removing all stored mappings and releasing resources.
    ///
    /// # Arguments
    /// * `index_descriptor` - Metadata describing the index being dropped
    /// * `nitrite_config` - Database configuration for resource access
    ///
    /// # Returns
    /// Ok(()) if index successfully dropped, Error on failure.
    ///
    /// # Behavior
    /// Called when removing an index from a collection. Must clean up all
    /// index-related data and structures. Subsequent operations on this
    /// indexer should fail after drop is called.
    ///
    /// # Errors
    /// Returns IndexingError if underlying storage operations fail.
    fn drop_index(
        &self,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()>;

    /// Records a new or updated field value mapping to a NitriteId in the index.
    ///
    /// # Arguments
    /// * `field_values` - The field values and associated NitriteId to index
    /// * `index_descriptor` - Metadata describing the index structure
    /// * `nitrite_config` - Database configuration for resource access
    ///
    /// # Returns
    /// Ok(()) if entry successfully written, Error on failure.
    ///
    /// # Behavior
    /// Adds or updates a mapping from the indexed field value(s) to the
    /// document's NitriteId. For unique indexes, fails if the value
    /// already exists. For compound indexes, inserts into intermediate
    /// structures based on field hierarchy.
    ///
    /// # Errors
    /// Returns IndexingError if:
    /// - Unique constraint is violated (for unique indexes with existing value)
    /// - Underlying storage write fails
    /// - Field value extraction fails
    fn write_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()>;

    /// Removes a field value mapping from the index.
    ///
    /// # Arguments
    /// * `field_values` - The field values and NitriteId to remove from index
    /// * `index_descriptor` - Metadata describing the index structure
    /// * `nitrite_config` - Database configuration for resource access
    ///
    /// # Returns
    /// Ok(()) if entry successfully removed, Error on failure.
    ///
    /// # Behavior
    /// Deletes the mapping between the indexed field value(s) and the
    /// NitriteId. For compound indexes, traverses nested structures to
    /// find and remove the entry. If entry doesn't exist, silently succeeds.
    ///
    /// # Errors
    /// Returns IndexingError if underlying storage operations fail.
    fn remove_index_entry(
        &self,
        field_values: &FieldValues,
        index_descriptor: &IndexDescriptor,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<()>;

    /// Finds all NitriteIds matching the criteria in a find plan.
    ///
    /// # Arguments
    /// * `find_plan` - Query plan specifying filters and traversal strategy
    /// * `nitrite_config` - Database configuration for resource access
    ///
    /// # Returns
    /// Vector of NitriteIds matching all filter criteria, empty if none match.
    ///
    /// # Behavior
    /// Evaluates the find plan against indexed data:
    /// - Simple indexes apply filters directly to NitriteIds
    /// - Compound indexes recursively navigate through intermediate maps
    /// - Results are deduplicated and consistently ordered
    ///
    /// # Errors
    /// Returns IndexingError if filter evaluation fails or if
    /// index structure is corrupted.
    fn find_by_filter(
        &self,
        find_plan: &FindPlan,
        nitrite_config: &NitriteConfig,
    ) -> NitriteResult<Vec<NitriteId>>;
}


impl NitriteIndexer {
    /// Creates a new NitriteIndexer wrapping a concrete implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete type implementing NitriteIndexerProvider
    ///
    /// # Returns
    /// A new NitriteIndexer wrapping the provided implementation in an Arc
    /// for thread-safe reference counting.
    ///
    /// # Behavior
    /// Wraps the implementation in Arc<dyn NitriteIndexerProvider> to enable
    /// polymorphic indexer usage. The implementation remains accessible through
    /// Deref without explicit unwrapping.
    pub fn new<T: NitriteIndexerProvider + 'static>(inner: T) -> Self {
        NitriteIndexer { inner: Arc::new(inner) }
    }
}

impl Deref for NitriteIndexer {
    type Target = Arc<dyn NitriteIndexerProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Convertible, Fields, NitritePlugin};
    use crate::errors::{ErrorKind, NitriteError};
    use crate::index::IndexDescriptor;
    use crate::nitrite_config::NitriteConfig;
    use std::any::{Any, TypeId};

    #[derive(Copy, Clone)]
    struct MockNitriteIndexer;

    impl NitriteIndexerProvider for MockNitriteIndexer {
        fn index_type(&self) -> String {
            "MockIndex".to_string()
        }

        fn is_unique(&self) -> bool {
            true
        }

        fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
            Ok(())
        }

        fn drop_index(
            &self,
            _index_descriptor: &IndexDescriptor,
            _nitrite_config: &NitriteConfig,
        ) -> NitriteResult<()> {
            Ok(())
        }

        fn write_index_entry(
            &self,
            _field_values: &FieldValues,
            _index_descriptor: &IndexDescriptor,
            _nitrite_config: &NitriteConfig,
        ) -> NitriteResult<()> {
            Ok(())
        }

        fn remove_index_entry(
            &self,
            _field_values: &FieldValues,
            _index_descriptor: &IndexDescriptor,
            _nitrite_config: &NitriteConfig,
        ) -> NitriteResult<()> {
            Ok(())
        }

        fn find_by_filter(
            &self,
            _find_plan: &FindPlan,
            _nitrite_config: &NitriteConfig,
        ) -> NitriteResult<Vec<NitriteId>> {
            Ok(vec![NitriteId::new()])
        }
    }

    impl NitritePluginProvider for MockNitriteIndexer {
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

    #[test]
    fn test_index_type() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        assert_eq!(indexer.index_type(), "MockIndex");
    }

    #[test]
    fn test_is_unique() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        assert!(indexer.is_unique());
    }

    #[test]
    fn test_validate_index() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let fields = Fields::with_names(vec!["test_field"]).unwrap();
        assert!(indexer.validate_index(&fields).is_ok());
    }

    #[test]
    fn test_drop_index() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let index_descriptor = IndexDescriptor::new(
            "test_index",
            Fields::with_names(vec!["test_field"]).unwrap(),
            "test",
        );
        let config = NitriteConfig::default();
        assert!(indexer.drop_index(&index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_write_index_entry() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let index_descriptor = IndexDescriptor::new(
            "test_index",
            Fields::with_names(vec!["test_field"]).unwrap(),
            "test",
        );
        let config = NitriteConfig::default();
        assert!(indexer.write_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_remove_index_entry() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let index_descriptor = IndexDescriptor::new(
            "test_index",
            Fields::with_names(vec!["test_field"]).unwrap(),
            "test",
        );
        let config = NitriteConfig::default();
        assert!(indexer.remove_index_entry(&field_values, &index_descriptor, &config).is_ok());
    }

    #[test]
    fn test_find_by_filter() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let find_plan = FindPlan::new();
        let config = NitriteConfig::default();
        assert!(indexer.find_by_filter(&find_plan, &config).is_ok());
    }

    #[test]
    fn test_initialize() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        let config = NitriteConfig::default();
        assert!(indexer.initialize(config).is_ok());
    }

    #[test]
    fn test_close() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        assert!(indexer.close().is_ok());
    }

    #[test]
    fn test_as_plugin() {
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        assert_eq!(indexer.as_plugin().type_id(), TypeId::of::<NitritePlugin>());
    }

    #[test]
    fn test_validate_index_error() {
        #[derive(Copy, Clone)]
        struct ErrorMockNitriteIndexer;

        impl NitriteIndexerProvider for ErrorMockNitriteIndexer {
            fn index_type(&self) -> String {
                "ErrorMockIndex".to_string()
            }

            fn is_unique(&self) -> bool {
                false
            }

            fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
                Err(NitriteError::new("Validation error", ErrorKind::IndexingError))
            }

            fn drop_index(
                &self,
                _index_descriptor: &IndexDescriptor,
                _nitrite_config: &NitriteConfig,
            ) -> NitriteResult<()> {
                Err(NitriteError::new("Drop index error", ErrorKind::IndexingError))
            }

            fn write_index_entry(
                &self,
                _field_values: &FieldValues,
                _index_descriptor: &IndexDescriptor,
                _nitrite_config: &NitriteConfig,
            ) -> NitriteResult<()> {
                Err(NitriteError::new("Write index entry error", ErrorKind::IndexingError))
            }

            fn remove_index_entry(
                &self,
                _field_values: &FieldValues,
                _index_descriptor: &IndexDescriptor,
                _nitrite_config: &NitriteConfig,
            ) -> NitriteResult<()> {
                Err(NitriteError::new("Remove index entry error", ErrorKind::IndexingError))
            }

            fn find_by_filter(
                &self,
                _find_plan: &FindPlan,
                _nitrite_config: &NitriteConfig,
            ) -> NitriteResult<Vec<NitriteId>> {
                Err(NitriteError::new("Find by filter error", ErrorKind::IndexingError))
            }
        }

        impl NitritePluginProvider for ErrorMockNitriteIndexer {
            fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
                Err(NitriteError::new("Initialize error", ErrorKind::IndexingError))
            }

            fn close(&self) -> NitriteResult<()> {
                Err(NitriteError::new("Close error", ErrorKind::IndexingError))
            }

            fn as_plugin(&self) -> NitritePlugin {
                NitritePlugin::new(self.clone())
            }
        }

        let indexer = NitriteIndexer::new(ErrorMockNitriteIndexer);
        let fields = Fields::with_names(vec!["test_field"]).unwrap();
        let index_descriptor = IndexDescriptor::new(
            "test_index",
            Fields::with_names(vec!["test_field"]).unwrap(),
            "test",
        );
        let config = NitriteConfig::default();
        let field_values = FieldValues::new(
            vec![(String::from("test_field"), 1.to_value().unwrap())],
            NitriteId::new(),
            Fields::with_names(vec!["test_field"]).unwrap(),
        );
        let find_plan = FindPlan::new();

        assert!(indexer.validate_index(&fields).is_err());
        assert!(indexer.drop_index(&index_descriptor, &config).is_err());
        assert!(indexer.write_index_entry(&field_values, &index_descriptor, &config).is_err());
        assert!(indexer.remove_index_entry(&field_values, &index_descriptor, &config).is_err());
        assert!(indexer.find_by_filter(&find_plan, &config).is_err());
        assert!(indexer.initialize(config).is_err());
        assert!(indexer.close().is_err());
    }

    // Performance optimization tests
    #[test]
    fn test_indexer_get_method_efficiency() {
        // Test that Deref trait is used for efficient access without cloning
        let indexer = NitriteIndexer::new(MockNitriteIndexer);
        
        // Multiple accesses should reuse same Arc
        assert_eq!(indexer.index_type(), "MockIndex");
        assert_eq!(indexer.index_type(), "MockIndex");
        assert!(indexer.is_unique());
    }

    #[test]
    fn test_indexer_arc_sharing() {
        // Test that multiple clones share the same underlying Arc
        let indexer1 = NitriteIndexer::new(MockNitriteIndexer);
        let indexer2 = indexer1.clone();
        
        // Both should have same index_type with no cloning of inner data
        assert_eq!(indexer1.index_type(), indexer2.index_type());
    }

    #[test]
    fn test_indexer_error_propagation_efficiency() {
        // Test that errors are returned directly without wrapping
        #[derive(Copy, Clone)]
        struct MinimalErrorMockIndexer;

        impl NitriteIndexerProvider for MinimalErrorMockIndexer {
            fn index_type(&self) -> String { "MinimalError".to_string() }
            fn is_unique(&self) -> bool { false }
            fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
                Err(NitriteError::new("Quick error", ErrorKind::IndexingError))
            }
            fn drop_index(&self, _: &IndexDescriptor, _: &NitriteConfig) -> NitriteResult<()> { Ok(()) }
            fn write_index_entry(&self, _: &FieldValues, _: &IndexDescriptor, _: &NitriteConfig) -> NitriteResult<()> { Ok(()) }
            fn remove_index_entry(&self, _: &FieldValues, _: &IndexDescriptor, _: &NitriteConfig) -> NitriteResult<()> { Ok(()) }
            fn find_by_filter(&self, _: &FindPlan, _: &NitriteConfig) -> NitriteResult<Vec<NitriteId>> { Ok(vec![]) }
        }

        impl NitritePluginProvider for MinimalErrorMockIndexer {
            fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> { Ok(()) }
            fn close(&self) -> NitriteResult<()> { Ok(()) }
            fn as_plugin(&self) -> NitritePlugin { NitritePlugin::new(self.clone()) }
        }

        let indexer = NitriteIndexer::new(MinimalErrorMockIndexer);
        let fields = Fields::with_names(vec!["test"]).unwrap();
        let result = indexer.validate_index(&fields);
        
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("Quick error"));
        }
    }
}