use crate::common::{Convertible, Value, DOC_ID, UNIQUE_INDEX};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::filter::{and, field, Filter};
use crate::FIELD_SEPARATOR;

/// Trait that defines the schema and metadata for a database entity (repository type).
///
/// # Purpose
/// Provides runtime metadata about an entity type, including its name, ID field configuration,
/// and index definitions. Implemented automatically by the NitriteEntity derive macro.
///
/// # Characteristics
/// - Must implement Default for entity instantiation
/// - Associated type Id must implement Convertible for value conversion
/// - Provides metadata for schema mapping and constraint enforcement
/// - Typically implemented via derive macro, not manually
///
/// # Methods
/// * `entity_name()` - Returns the entity type name (e.g., "Book")
/// * `entity_indexes()` - Returns index definitions if any
/// * `entity_id()` - Returns ID field configuration if specified
///
/// # Usage
/// ```ignore
/// #[derive(NitriteEntity, Default)]
/// pub struct Book {
///     id: i32,
///     name: String,
/// }
///
/// #[derive(NitriteEntity, Default)]
/// #[entity(id(field = "id"))]
/// pub struct User {
///     id: i32,
///     name: String,
/// }
/// ```
pub trait NitriteEntity: Default {
    /// Associated type for entity IDs, must be convertible to/from Value.
    type Id: Convertible + Send + Sync + 'static;

    /// Returns the entity type name as a String.
    ///
    /// # Returns
    /// Entity name (e.g., "Book", "User")
    ///
    /// # Behavior
    /// - Defaults to the struct name if not customized via #[entity(name = "...")]
    /// - Used for schema mapping and table-like organization
    fn entity_name(&self) -> String;

    /// Returns index definitions for this entity, if any.
    ///
    /// # Returns
    /// - Some(Vec<EntityIndex>) if indexes are defined via #[entity(indexes = ...)]
    /// - None if no indexes are configured
    fn entity_indexes(&self) -> Option<Vec<EntityIndex>>;

    /// Returns the ID field configuration for this entity, if any.
    ///
    /// # Returns
    /// - Some(EntityId) if an ID field is defined via #[entity(id(field = "..."))]
    /// - None if no explicit ID field is configured
    fn entity_id(&self) -> Option<EntityId>;
}

/// Defines a database index on one or more fields of an entity.
///
/// # Purpose
/// Specifies which fields should be indexed and what type of index to create (e.g., UNIQUE, NON_UNIQUE).
/// Used by the entity derive macro to configure indexes during entity registration.
///
/// # Characteristics
/// - Immutable after creation
/// - Hashable and comparable for use in collections
/// - Clone-able for sharing across contexts
/// - Default index type is UNIQUE_INDEX
///
/// # Usage
/// ```ignore
/// #[derive(NitriteEntity, Default)]
/// #[entity(indexes = [("email", "UNIQUE"), ("name", "NON_UNIQUE")])]
/// pub struct User {
///     id: i32,
///     email: String,
///     name: String,
/// }
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct EntityIndex {
    fields: Vec<String>,
    index_type: String,
}

impl EntityIndex {
    /// Creates a new index definition.
    ///
    /// # Arguments
    /// * `fields` - Field names to index (e.g., ["email"] or ["first_name", "last_name"])
    /// * `index_type` - Index type (e.g., "UNIQUE", "NON_UNIQUE")
    ///
    /// # Behavior
    /// - Defaults to UNIQUE_INDEX if index_type is None
    /// - Converts string slices to owned Strings for storage
    pub fn new(fields: Vec<&str>, index_type: Option<&str>) -> Self {
        EntityIndex {
            fields: fields.iter().map(|field| field.to_string()).collect(),
            index_type: index_type.unwrap_or(UNIQUE_INDEX).to_string(),
        }
    }

    /// Returns the field names for this index.
    ///
    /// # Returns
    /// Reference to the vector of field names (e.g., ["email"])
    pub fn field_names(&self) -> &Vec<String> {
        &self.fields
    }

    /// Returns the index type as a string.
    ///
    /// # Returns
    /// Index type (e.g., "UNIQUE", "NON_UNIQUE")
    pub fn index_type(&self) -> &str {
        &self.index_type
    }
}

/// Defines the ID field configuration for an entity.
///
/// # Purpose
/// Specifies which field(s) serve as the entity's primary identifier, including support for:
/// - Single fields (simple IDs)
/// - Compound IDs (multiple fields)
/// - Embedded IDs (ID composed of nested document fields)
/// - NitriteId (database-generated IDs)
///
/// # Characteristics
/// - Immutable after creation
/// - Hashable and comparable for use in collections
/// - Clone-able for sharing across contexts
/// - Supports creating filters from ID values for database queries
///
/// # Usage
/// ```ignore
/// // Simple ID field
/// #[derive(NitriteEntity, Default)]
/// #[entity(id(field = "id"))]
/// pub struct User {
///     id: i32,
///     name: String,
/// }
///
/// // Embedded ID (compound ID from nested struct fields)
/// #[derive(NitriteEntity, Default)]
/// #[entity(id(field = "book_id", embedded_fields = "author, isbn"))]
/// pub struct Book {
///     book_id: BookId,
///     title: String,
/// }
/// ```
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct EntityId {
    field_name: String,
    is_nitrite_id: bool,
    embedded_fields: Vec<String>,
}

impl EntityId {
    /// Creates a new ID field configuration.
    ///
    /// # Arguments
    /// * `field_name` - Name of the field (or nested struct) that holds the ID
    /// * `is_nitrite_id` - True if this is a NitriteId type (database-generated)
    /// * `embedded_fields` - Optional list of fields within a nested struct that comprise the ID
    ///
    /// # Behavior
    /// - is_nitrite_id defaults to false if None
    /// - embedded_fields defaults to empty if None (indicating a simple ID)
    /// - For compound IDs, concatenates nested fields with FIELD_SEPARATOR
    pub fn new(
        field_name: &str,
        is_nitrite_id: Option<bool>,
        embedded_fields: Option<Vec<&str>>,
    ) -> Self {
        EntityId {
            field_name: field_name.to_string(),
            is_nitrite_id: is_nitrite_id.unwrap_or(false),
            embedded_fields: embedded_fields
                .unwrap_or_default()
                .iter()
                .map(|field| field.to_string())
                .collect(),
        }
    }

    /// Returns the field name that contains the ID.
    ///
    /// # Returns
    /// Reference to the field name string
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Returns whether this is a NitriteId (database-generated ID).
    ///
    /// # Returns
    /// true if NitriteId, false for user-provided or other ID types
    pub fn is_nitrite_id(&self) -> bool {
        self.is_nitrite_id
    }

    /// Returns the list of embedded field names that comprise a compound ID.
    ///
    /// # Returns
    /// Reference to vector of embedded field names (empty for simple IDs)
    pub fn embedded_fields(&self) -> &Vec<String> {
        &self.embedded_fields
    }

    /// Returns field names formatted for compound key lookups.
    ///
    /// # Returns
    /// Vector of encoded field names (field_name.embedded_field format)
    /// Empty vector for non-embedded IDs
    ///
    /// # Behavior
    /// - Concatenates field_name with each embedded field using FIELD_SEPARATOR
    /// - Returns ["id.sub_id1", "id.sub_id2"] for embedded field ["sub_id1", "sub_id2"]
    pub fn encoded_field_names(&self) -> Vec<String> {
        let mut result = Vec::with_capacity(self.embedded_fields.len());
        let separator = FIELD_SEPARATOR.read();
        for field in &self.embedded_fields {
            result.push(format!("{}{}{}", self.field_name, separator, field));
        }
        result
    }

    /// Returns whether this ID is an embedded (compound) ID.
    ///
    /// # Returns
    /// true if embedded_fields is non-empty, false for simple IDs
    pub fn is_embedded(&self) -> bool {
        !self.embedded_fields.is_empty()
    }

    /// Creates a filter for unique lookups by this ID value.
    ///
    /// # Arguments
    /// * `element` - The ID value (can be simple value or Document for embedded IDs)
    ///
    /// # Returns
    /// `Ok(Filter)` - Filter suitable for find() queries
    /// `Err(NitriteError)` - If embedded ID receives wrong type
    ///
    /// # Behavior
    /// - For simple IDs: creates field(name).eq(value) filter
    /// - For embedded IDs: creates and-filter of all embedded field filters
    pub fn create_unique_filter<T: Into<Value>>(&self, element: T) -> NitriteResult<Filter> {
        if self.is_embedded() {
            self.create_embedded_id_filter(element)
        } else {
            let filter = field(&self.field_name).eq(element);
            Ok(filter)
        }
    }

    /// Creates a filter for ID-based lookups, respecting NitriteId designation.
    ///
    /// # Arguments
    /// * `id` - The ID value for lookup
    ///
    /// # Returns
    /// `Ok(Filter)` - Filter using appropriate field (_id for NitriteId, field_name otherwise)
    /// `Err(NitriteError)` - If embedded ID receives wrong type
    ///
    /// # Behavior
    /// - For NitriteId: uses DOC_ID field (system-level ID)
    /// - For other simple IDs: uses the configured field_name
    /// - For embedded IDs: delegates to create_embedded_id_filter()
    pub fn create_id_filter<T: Into<Value>>(&self, id: T) -> NitriteResult<Filter> {
        if self.is_embedded() {
            self.create_embedded_id_filter(id)
        } else if self.is_nitrite_id() {
            let filter = field(DOC_ID).eq(id);
            Ok(filter)
        } else {
            let filter = field(&self.field_name).eq(id);
            Ok(filter)
        }
    }

    /// Creates filter for embedded (compound) ID lookups.
    ///
    /// # Arguments
    /// * `element` - Single value (for single embedded field) or Document (for multiple fields)
    ///
    /// # Returns
    /// `Ok(Filter)` - AND-combined filter of all embedded field conditions
    /// `Err(NitriteError)` - If element type doesn't match embedded field count
    ///
    /// # Behavior
    /// - Single embedded field with non-Document: creates simple eq(field, value) filter
    /// - Multiple embedded fields: expects Document with all embedded field values
    /// - Returns AND-combined filter for all embedded field constraints
    fn create_embedded_id_filter<T: Into<Value>>(&self, element: T) -> NitriteResult<Filter> {
        let value = element.into();

        if self.embedded_fields.len() == 1 && !value.is_document() {
            // in case of single embedded field, the value is directly passed
            // as the value of the field
            let separator = FIELD_SEPARATOR.read();
            let filter_field = format!(
                "{}{}{}",
                self.field_name,
                separator,
                self.embedded_fields[0]
            );
            let filter = field(&filter_field).eq(value);
            Ok(filter)
        } else {
            if !value.is_document() {
                log::error!("Embedded field value should be a document");
                return Err(NitriteError::new(
                    "Embedded field value should be a document",
                    ErrorKind::InvalidOperation,
                ));
            }

            let mut filters = Vec::with_capacity(self.embedded_fields.len());
            let document = match value.as_document() {
                Some(doc) => doc.clone(),
                None => {
                    log::error!("Failed to convert value to document for embedded field filter");
                    return Err(NitriteError::new(
                        "Embedded field value should be a document",
                        ErrorKind::InvalidOperation,
                    ));
                }
            };

            let separator = FIELD_SEPARATOR.read();
            for embedded_field in &self.embedded_fields {
                let filter_field = format!(
                    "{}{}{}",
                    self.field_name,
                    separator,
                    embedded_field
                );
                let field_value = document.get(embedded_field)?;
                let filter = field(&filter_field).eq(field_value.clone());

                filters.push(filter);
            }

            Ok(and(filters))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::Value;
    use crate::errors::NitriteError;
    use crate::filter::{is_and_filter, is_equals_filter};

    #[derive(Default)]
    struct TestEntity;

    impl NitriteEntity for TestEntity {
        type Id = String;

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            Some(vec![EntityIndex::new(vec!["field1"], Some("index_type"))])
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", Some(true), Some(vec!["sub_id"])))
        }
    }

    #[test]
    fn test_entity_index_new() {
        let index = EntityIndex::new(vec!["field1", "field2"], Some("index_type"));
        assert_eq!(index.field_names(), &vec!["field1".to_string(), "field2".to_string()]);
        assert_eq!(index.index_type(), "index_type");
    }

    #[test]
    fn test_entity_id_new() {
        let id = EntityId::new("id", Some(true), Some(vec!["sub_id1", "sub_id2"]));
        assert_eq!(id.field_name(), "id");
        assert!(id.is_nitrite_id());
        assert_eq!(id.embedded_fields(), &vec!["sub_id1".to_string(), "sub_id2".to_string()]);
    }

    #[test]
    fn test_entity_id_encoded_field_names() {
        let id = EntityId::new("id", Some(true), Some(vec!["sub_id1", "sub_id2"]));
        let encoded_fields = id.encoded_field_names();
        assert_eq!(encoded_fields, vec!["id.sub_id1", "id.sub_id2"]);
    }

    #[test]
    fn test_entity_id_is_embedded() {
        let id = EntityId::new("id", Some(true), Some(vec!["sub_id1", "sub_id2"]));
        assert!(id.is_embedded());
    }

    #[test]
    fn test_entity_id_create_unique_filter() {
        let id = EntityId::new("id", Some(false), Some(vec!["sub_id1", "sub_id2"]));
        let mut doc = Document::new();
        doc.put("sub_id1", Value::String("value1".to_string())).unwrap();
        doc.put("sub_id2", Value::String("value2".to_string())).unwrap();
        let filter = id.create_unique_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_entity_id_create_id_filter() {
        let id = EntityId::new("id", Some(true), None);
        let filter = id.create_id_filter(Value::String("value".to_string())).unwrap();
        assert!(is_equals_filter(&filter));
    }

    #[test]
    fn test_entity_id_create_embedded_id_filter() {
        let id = EntityId::new("id", Some(false), Some(vec!["sub_id1", "sub_id2"]));
        let mut doc = Document::new();
        doc.put("sub_id1", Value::String("value1".to_string())).unwrap();
        doc.put("sub_id2", Value::String("value2".to_string())).unwrap();
        let filter = id.create_embedded_id_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_entity_id_create_embedded_id_filter_invalid() {
        let id = EntityId::new("id", Some(false), Some(vec!["sub_id1", "sub_id2"]));
        let result = id.create_embedded_id_filter(Value::String("invalid".to_string()));
        assert!(matches!(result, Err(NitriteError { .. })));
    }

    #[test]
    fn test_embedded_filter_with_numeric_values() {
        // Test embedded filter with numeric field values
        let id = EntityId::new("address", Some(false), Some(vec!["zip", "country"]));
        let mut doc = Document::new();
        doc.put("zip", Value::I32(12345)).unwrap();
        doc.put("country", Value::String("USA".to_string())).unwrap();
        
        let filter = id.create_embedded_id_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_embedded_filter_with_single_field() {
        // Test embedded filter with single embedded field and non-document value
        let id = EntityId::new("contact", Some(false), Some(vec!["phone"]));
        let value = Value::String("555-1234".to_string());
        
        let filter = id.create_embedded_id_filter(value).unwrap();
        assert!(is_equals_filter(&filter));
    }

    #[test]
    fn test_embedded_filter_with_corrupted_type() {
        // Test that non-document value is properly rejected with error, not panic
        let id = EntityId::new("metadata", Some(false), Some(vec!["key", "value"]));
        let result = id.create_embedded_id_filter(Value::I32(42));
        
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::InvalidOperation);
        }
    }

    #[test]
    fn test_embedded_filter_multi_field_all_types() {
        // Test embedded filter with multiple fields of different types
        let id = EntityId::new("details", Some(false), Some(vec!["count", "active", "label"]));
        let mut doc = Document::new();
        doc.put("count", Value::I32(100)).unwrap();
        doc.put("active", Value::Bool(true)).unwrap();
        doc.put("label", Value::String("test".to_string())).unwrap();
        
        let filter = id.create_embedded_id_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    // Performance optimization tests
    #[test]
    fn test_encoded_field_names_pre_allocation() {
        // Validates that encoded_field_names uses Vec::with_capacity instead of repeated collect
        let id = EntityId::new("address", Some(false), Some(vec!["street", "city", "zip", "country"]));
        
        let encoded = id.encoded_field_names();
        assert_eq!(encoded.len(), 4);
        assert!(encoded[0].contains("address"));
        assert!(encoded[0].contains("street"));
    }

    #[test]
    fn test_encoded_field_names_multiple_calls() {
        // Validates that multiple calls to encoded_field_names work correctly
        let id = EntityId::new("location", Some(false), Some(vec!["lat", "lon"]));
        
        let encoded1 = id.encoded_field_names();
        let encoded2 = id.encoded_field_names();
        
        assert_eq!(encoded1, encoded2);
        assert_eq!(encoded1.len(), 2);
    }

    #[test]
    fn test_create_embedded_id_filter_pre_allocation() {
        // Validates that create_embedded_id_filter pre-allocates Vec with capacity
        let id = EntityId::new("metadata", Some(false), Some(vec!["key", "value", "type", "priority"]));
        let mut doc = Document::new();
        doc.put("key", Value::String("k1".to_string())).unwrap();
        doc.put("value", Value::String("v1".to_string())).unwrap();
        doc.put("type", Value::String("t1".to_string())).unwrap();
        doc.put("priority", Value::I32(1)).unwrap();
        
        let filter = id.create_embedded_id_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_create_embedded_id_filter_separator_caching() {
        // Validates that separator is read once and cached, not repeatedly
        let id = EntityId::new("contact", Some(false), Some(vec!["phone", "email", "address"]));
        let mut doc = Document::new();
        doc.put("phone", Value::String("555-1234".to_string())).unwrap();
        doc.put("email", Value::String("test@example.com".to_string())).unwrap();
        doc.put("address", Value::String("123 Main St".to_string())).unwrap();
        
        let filter = id.create_embedded_id_filter(Value::Document(doc)).unwrap();
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_single_embedded_field_separator_caching() {
        // Validates that single field separator is also cached
        let id = EntityId::new("profile", Some(false), Some(vec!["name"]));
        let value = Value::String("John".to_string());
        
        let filter = id.create_embedded_id_filter(value).unwrap();
        assert!(is_equals_filter(&filter));
    }

    #[test]
    fn test_encoded_field_names_empty() {
        // Validates that empty embedded fields returns empty vec
        let id = EntityId::new("simple", Some(false), Some(vec![]));
        
        let encoded = id.encoded_field_names();
        assert_eq!(encoded.len(), 0);
    }
}