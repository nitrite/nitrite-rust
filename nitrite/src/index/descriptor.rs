use std::sync::Arc;

use crate::collection::Document;
use crate::common::Fields;
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::repository::{EntityId, EntityIndex, NitriteEntity};
use crate::{Convertible, Value};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// Describes the configuration of an index on a collection.
///
/// An index descriptor defines which fields are indexed, the index type (unique or non-unique),
/// and the collection the index belongs to. Index descriptors are used to create, manage,
/// and identify indexes throughout the database lifecycle.
///
/// # Characteristics
/// - **Immutable**: Once created, descriptor properties cannot be changed
/// - **Serializable**: Can be converted to/from documents for persistence
/// - **Uniqueness**: Descriptors are compared based on their fields and collection
/// - **Compound support**: Can describe single-field or multi-field (compound) indexes
///
/// # Usage
///
/// Index descriptors are created internally when indexes are created on collections:
/// - `collection.create_index(vec!["field1"], &unique_index())?` - Creates UNIQUE index
/// - `collection.create_index(vec!["field1", "field2"], &non_unique_index())?` - Creates compound index
///
/// # Responsibilities
/// - **Index Configuration**: Stores index type, fields, and collection name
/// - **Index Identification**: Uniquely identifies an index within the database
/// - **Compound Detection**: Determines if this is a compound (multi-field) index
/// - **Serialization**: Converts to/from documents for persistent storage
pub struct IndexDescriptor {
    inner: Arc<IndexDescriptorInner>,
}

impl IndexDescriptor {
    /// Creates a new index descriptor for a set of fields.
    ///
    /// # Arguments
    /// * `index_type` - The type of index: "UNIQUE" for unique constraint or "NON_UNIQUE" for standard index
    /// * `index_fields` - The fields to be indexed (can be single field or multiple fields for compound index)
    /// * `collection_name` - The name of the collection this index applies to
    ///
    /// # Returns
    /// A new `IndexDescriptor` that can be used to configure the index.
    ///
    /// # Behavior
    /// Creates an immutable descriptor with the provided configuration. The descriptor is cloneable
    /// and can be compared for equality based on its configuration.
    pub fn new(
        index_type: &str,
        index_fields: Fields,
        collection_name: &str,
    ) -> Self {
        Self {
            inner: Arc::new(IndexDescriptorInner {
                index_type: index_type.to_string(),
                index_fields,
                collection_name: collection_name.to_string(),
            }),
        }
    }

    /// Returns the index type string.
    ///
    /// # Returns
    /// Index type such as "UNIQUE" or "NON_UNIQUE".
    pub fn index_type(&self) -> String {
        self.inner.index_type.clone()
    }

    /// Returns the fields included in this index.
    ///
    /// # Returns
    /// A `Fields` object containing all indexed field names.
    pub fn index_fields(&self) -> Fields {
        self.inner.index_fields.clone()
    }

    /// Returns the name of the collection this index applies to.
    ///
    /// # Returns
    /// Collection name as a string.
    pub fn collection_name(&self) -> String {
        self.inner.collection_name.clone()
    }

    /// Determines whether this is a compound (multi-field) index.
    ///
    /// # Returns
    /// `true` if the index contains multiple fields (compound index), `false` for single-field indexes.
    pub fn is_compound_index(&self) -> bool {
        self.inner.index_fields.field_names().len() > 1
    }
}

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
/// Internal representation of index descriptor configuration.
///
/// This struct holds the actual index metadata. Users access this through
/// the public `IndexDescriptor` wrapper via its public methods.
pub struct IndexDescriptorInner {
    index_type: String,
    index_fields: Fields,
    collection_name: String,
}

impl IndexDescriptorInner {
    pub(crate) fn new(
        index_type: String,
        index_fields: Fields,
        collection_name: String,
    ) -> Self {
        Self {
            index_type,
            index_fields,
            collection_name,
        }
    }
}

impl Convertible for IndexDescriptor {
    type Output = Self;

    /// Converts this index descriptor to a document value for serialization.
    ///
    /// # Returns
    /// A `Value::Document` containing index_type, index_fields, and collection_name.
    ///
    /// # Behavior
    /// Creates a structured document representation suitable for storing in the database
    /// or transmitting over the network.
    fn to_value(&self) -> NitriteResult<Value> {
        let mut doc = Document::new();
        doc.put("index_type", Value::String(self.index_type()))?;
        doc.put("index_fields", self.index_fields().to_value()?)?;
        doc.put("collection_name", Value::String(self.collection_name()))?;
        Ok(Value::Document(doc))
    }

    /// Constructs an index descriptor from a document value.
    ///
    /// # Arguments
    /// * `value` - A `Value::Document` containing index descriptor fields
    ///
    /// # Returns
    /// A reconstructed `IndexDescriptor` if deserialization succeeds.
    ///
    /// # Errors
    /// Returns error if:
    /// - `value` is not a document
    /// - Required fields (index_type, index_fields, collection_name) are missing
    /// - index_type or collection_name fields are not strings
    ///
    /// # Behavior
    /// Safely extracts each field with validation, returning descriptive errors
    /// if type mismatches occur.
    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Document(doc) => {
                let index_type = doc.get("index_type")?
                    .as_string()
                    .ok_or_else(|| NitriteError::new(
                        "Index descriptor deserialization error: 'index_type' field is missing or not a string",
                        ErrorKind::ObjectMappingError
                    ))?
                    .clone();
                let index_fields = doc.get("index_fields")?;
                let index_fields = Fields::from_value(&index_fields)?;
                let collection_name = doc.get("collection_name")?
                    .as_string()
                    .ok_or_else(|| NitriteError::new(
                        "Index descriptor deserialization error: 'collection_name' field is missing or not a string",
                        ErrorKind::ObjectMappingError
                    ))?
                    .clone();
                Ok(IndexDescriptor::new(&index_type, index_fields, &collection_name))
            }
            _ => {
                log::error!("Failed to create IndexDescriptor from Value {:?}", value);
                Err(NitriteError::new(
                    "Index descriptor deserialization error: expected document value but found another type",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;

    #[test]
    fn test_index_descriptor_new() {
        let fields = Fields::with_names(vec!["field1", "field2"]).expect("Failed to create fields");
        let descriptor = IndexDescriptor::new("type1", fields.clone(), "collection1");

        assert_eq!(descriptor.index_type(), "type1");
        assert_eq!(descriptor.index_fields(), fields);
        assert_eq!(descriptor.collection_name(), "collection1");
    }

    #[test]
    fn test_is_compound_index() {
        let single_field = Fields::with_names(vec!["field1"]).expect("Failed to create fields");
        let compound_field = Fields::with_names(vec!["field1", "field2"]).expect("Failed to create fields");

        let single_descriptor = IndexDescriptor::new("type1", single_field, "collection1");
        let compound_descriptor = IndexDescriptor::new("type1", compound_field, "collection1");

        assert!(!single_descriptor.is_compound_index());
        assert!(compound_descriptor.is_compound_index());
    }

    #[test]
    fn test_to_value() {
        let fields = Fields::with_names(vec!["field1"]).expect("Failed to create fields");
        let descriptor = IndexDescriptor::new("type1", fields.clone(), "collection1");

        let value = descriptor.to_value().unwrap();
        if let Value::Document(doc) = value {
            assert_eq!(doc.get("index_type").unwrap().as_string().unwrap(), "type1");
            assert_eq!(doc.get("collection_name").unwrap().as_string().unwrap(), "collection1");
            assert_eq!(Fields::from_value(&doc.get("index_fields").unwrap()).unwrap(), fields);
        } else {
            panic!("Expected Value::Document");
        }
    }

    #[test]
    fn test_from_value() {
        let fields = Fields::with_names(vec!["field1"]).expect("Failed to create fields");
        let mut doc = Document::new();
        doc.put("index_type", Value::String("type1".to_string())).unwrap();
        doc.put("index_fields", fields.to_value().unwrap()).unwrap();
        doc.put("collection_name", Value::String("collection1".to_string())).unwrap();

        let value = Value::Document(doc);
        let descriptor = IndexDescriptor::from_value(&value).unwrap();

        assert_eq!(descriptor.index_type(), "type1");
        assert_eq!(descriptor.index_fields(), fields);
        assert_eq!(descriptor.collection_name(), "collection1");
    }

    #[test]
    fn test_from_value_invalid() {
        let value = Value::String("invalid".to_string());
        let result = IndexDescriptor::from_value(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_value_missing_fields() {
        let mut doc = Document::new();
        doc.put("index_type", Value::String("type1".to_string())).unwrap();
        // Missing index_fields and collection_name

        let value = Value::Document(doc);
        let result = IndexDescriptor::from_value(&value);
        assert!(result.is_err());
    }

    // as_string().unwrap() error handling tests
    #[test]
    fn test_from_value_non_string_index_type() {
        // Test that from_value() handles non-string index_type gracefully
        // instead of panicking with as_string().unwrap()
        let fields = Fields::with_names(vec!["field1"]).unwrap();
        let mut doc = Document::new();
        doc.put("index_type", Value::I32(123)).unwrap();  // Non-string!
        doc.put("index_fields", fields.to_value().unwrap()).unwrap();
        doc.put("collection_name", Value::String("collection1".to_string())).unwrap();

        let value = Value::Document(doc);
        let result = IndexDescriptor::from_value(&value);
        
        // Should return error, not panic
        assert!(result.is_err());
    }

    #[test]
    fn test_from_value_non_string_collection_name() {
        // Test that from_value() handles non-string collection_name gracefully
        // instead of panicking with as_string().unwrap()
        let fields = Fields::with_names(vec!["field1"]).unwrap();
        let mut doc = Document::new();
        doc.put("index_type", Value::String("type1".to_string())).unwrap();
        doc.put("index_fields", fields.to_value().unwrap()).unwrap();
        doc.put("collection_name", Value::Array(vec![])).unwrap();  // Non-string!

        let value = Value::Document(doc);
        let result = IndexDescriptor::from_value(&value);
        
        // Should return error, not panic
        assert!(result.is_err());
    }

    #[test]
    fn test_from_value_with_correct_types() {
        // Test that from_value() properly handles string values without unwrap issues
        let fields = Fields::with_names(vec!["field1"]).unwrap();
        let mut doc = Document::new();
        doc.put("index_type", Value::String("UNIQUE".to_string())).unwrap();
        doc.put("index_fields", fields.to_value().unwrap()).unwrap();
        doc.put("collection_name", Value::String("test_collection".to_string())).unwrap();

        let value = Value::Document(doc);
        let result = IndexDescriptor::from_value(&value);
        
        // Should succeed with proper string types
        assert!(result.is_ok());
        let descriptor = result.unwrap();
        assert_eq!(descriptor.index_type(), "UNIQUE");
        assert_eq!(descriptor.collection_name(), "test_collection");
    }

    #[test]
    fn test_round_trip_to_from_value() {
        // Test round-trip conversion: descriptor -> value -> descriptor
        let fields = Fields::with_names(vec!["field1", "field2"]).unwrap();
        let original = IndexDescriptor::new("COMPOUND", fields.clone(), "my_collection");

        // Convert to value
        let value = original.to_value().unwrap();
        
        // Convert back from value
        let recovered = IndexDescriptor::from_value(&value).unwrap();

        // Should match original
        assert_eq!(recovered.index_type(), original.index_type());
        assert_eq!(recovered.index_fields(), original.index_fields());
        assert_eq!(recovered.collection_name(), original.collection_name());
    }
}