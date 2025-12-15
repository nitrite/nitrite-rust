use crate::collection::NitriteId;
use crate::common::{SortOrder, Value, NAME_SEPARATOR};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use std::cmp::min;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;

use super::Convertible;

/// Represents an ordered collection of field names for indexing and querying.
///
/// This struct manages a set of field names with support for prefix matching,
/// sorting, and serialization. It provides an efficient way to track which fields
/// are involved in indexes and query operations.
///
/// # Responsibilities
///
/// * **Field Management**: Stores and maintains ordered field names
/// * **Serialization**: Converts field names to/from value representations
/// * **Comparison**: Supports ordering and equality checks based on encoded names
/// * **Prefix Matching**: Checks if fields start with a specific prefix sequence
/// * **Hashing**: Implements hashing for use in collections
/// * **Display Formatting**: Provides string representation with field separator
///
/// # Example
///
/// ```ignore
/// let fields = Fields::with_names(vec!["name", "age"])?;
/// assert_eq!(fields.encoded_names(), "name|age");
/// 
/// let prefix = Fields::with_names(vec!["name"])?;
/// assert!(fields.starts_with(&prefix));
/// ```
#[derive(Clone, Debug, Eq)]
pub struct Fields {
    inner: Arc<FieldsInner>,
}

impl Fields {
    /// Creates a new Fields instance with the provided field names.
    pub fn with_names(field_names: Vec<&str>) -> NitriteResult<Fields> {
        if field_names.is_empty() {
            log::error!("Field names cannot be empty");
            return Err(NitriteError::new(
                "Field names cannot be empty",
                ErrorKind::ValidationError,
            ));
        }

        Ok(Fields {
            inner: Arc::new(FieldsInner {
                field_names: field_names.iter().map(|s| s.to_string()).collect(),
            }),
        })
    }

    /// Returns the list of field names.
    pub fn field_names(&self) -> Vec<String> {
        self.inner.field_names()
    }

    /// Returns the encoded representation of field names separated by NAME_SEPARATOR.
    pub fn encoded_names(&self) -> String {
        self.inner.encoded_names()
    }

    /// Checks if these fields start with the given prefix fields.
    pub fn starts_with(&self, prefix: &Fields) -> bool {
        self.inner.starts_with(prefix)
    }
}

impl Display for Fields {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.encoded_names())
    }
}

impl Ord for Fields {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.encoded_names().cmp(&other.encoded_names())
    }
}

impl PartialOrd for Fields {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Fields {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_names() == other.encoded_names()
    }
}

impl Hash for Fields {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.encoded_names().hash(state);
    }
}

impl Convertible for Fields {
    type Output = Self;

    fn to_value(&self) -> NitriteResult<Value> {
        Ok(Value::from_vec(self.field_names()))
    }

    fn from_value(value: &Value) -> NitriteResult<Self::Output> {
        match value {
            Value::Array(ref array) => {
                let mut field_names = Vec::new();
                for v in array {
                    match v.as_string() {
                        Some(name) => field_names.push(name.as_str()),
                        None => {
                            log::error!("Field name must be a string, got: {:?}", v);
                            return Err(NitriteError::new(
                                "Field name must be a string",
                                ErrorKind::ObjectMappingError,
                            ));
                        }
                    }
                }
                Fields::with_names(field_names)
            }
            _ => {
                log::error!("Value {} is not an array", value);
                Err(NitriteError::new(
                    "Value is not an array",
                    ErrorKind::ObjectMappingError,
                ))
            },
        }
    }
}

/// Inner implementation of field name storage and operations.
#[derive(Debug, Eq)]
struct FieldsInner {
    field_names: Vec<String>,
}

impl FieldsInner {
    #[inline]
    pub fn field_names(&self) -> Vec<String> {
        self.field_names.clone()
    }

    #[inline]
    pub fn encoded_names(&self) -> String {
        self.field_names.join(NAME_SEPARATOR)
    }

    #[inline]
    pub fn starts_with(&self, prefix: &Fields) -> bool {
        let prefix_names = prefix.field_names();
        let length = min(self.field_names.len(), prefix_names.len());

        if prefix_names.len() > length {
            return false;
        }

        for i in 0..length {
            if self.field_names[i] != prefix_names[i] {
                return false;
            }
        }

        true
    }
}

impl PartialEq for FieldsInner {
    fn eq(&self, other: &Self) -> bool {
        self.encoded_names() == other.encoded_names()
    }
}

pub struct SortableFields {
    field_names: Vec<String>,
    sorting_order: Vec<(String, SortOrder)>,
}

impl Default for SortableFields {
    fn default() -> Self {
        Self::new()
    }
}

impl SortableFields {
    pub fn new() -> SortableFields {
        SortableFields {
            field_names: Vec::new(),
            sorting_order: Vec::new(),
        }
    }

    pub fn with_names(field_names: Vec<String>) -> NitriteResult<SortableFields> {
        if field_names.is_empty() {
            log::error!("Field names cannot be empty");
            return Err(NitriteError::new(
                "Field names cannot be empty",
                ErrorKind::ValidationError,
            ));
        }

        let sorting_order = field_names
            .iter()
            .map(|field_name| (field_name.clone(), SortOrder::Ascending))
            .collect();

        Ok(SortableFields {
            field_names,
            sorting_order,
        })
    }

    pub fn with_names_and_order(
        field_names: Vec<String>,
        sorting_order: Vec<(String, SortOrder)>,
    ) -> NitriteResult<SortableFields> {
        if field_names.is_empty() {
            log::error!("Field names cannot be empty");
            return Err(NitriteError::new(
                "Field names cannot be empty",
                ErrorKind::ValidationError,
            ));
        }

        Ok(SortableFields {
            field_names,
            sorting_order,
        })
    }

    pub fn field_names(&self) -> Vec<String> {
        self.field_names.clone()
    }

    #[inline]
    pub fn encoded_names(&self) -> String {
        self.field_names.join(NAME_SEPARATOR)
    }

    #[inline]
    pub fn add_field(self, field_name: String) -> SortableFields {
        self.add_sorted_field(field_name, SortOrder::Ascending)
    }

    #[inline]
    pub fn add_sorted_field(mut self, field_name: String, sort_order: SortOrder) -> SortableFields {
        self.field_names.push(field_name.clone());
        self.sorting_order.push((field_name, sort_order));
        self
    }

    #[inline]
    pub fn sorting_order(&self) -> Vec<(String, SortOrder)> {
        // Pre-allocate with known capacity
        let mut sorting_order = Vec::with_capacity(self.sorting_order.len());
        for (field_name, sort_order) in &self.sorting_order {
            sorting_order.push((field_name.clone(), *sort_order));
        }
        sorting_order
    }
}

#[derive(Debug)]
pub struct FieldValues {
    field_values: Vec<(String, Value)>,
    nitrite_id: NitriteId,
    fields: Fields,
}

impl FieldValues {
    #[inline]
    pub fn new(
        field_values: Vec<(String, Value)>,
        nitrite_id: NitriteId,
        fields: Fields,
    ) -> FieldValues {
        FieldValues {
            field_values,
            nitrite_id,
            fields,
        }
    }

    #[inline]
    pub fn get_value(&self, field_name: &str) -> Option<&Value> {
        self.field_values
            .iter()
            .find(|(name, _)| name == field_name).and_then(|(_, value)| {
                if value.is_null() {
                    return None;
                }
                Some(value)   
            })
    }

    #[inline]
    pub fn fields(&self) -> &Fields {
        &self.fields
    }

    #[inline]
    pub fn nitrite_id(&self) -> &NitriteId {
        &self.nitrite_id
    }

    #[inline]
    pub fn values(&self) -> Vec<(String, Value)> {
        self.field_values.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SortOrder;
    use crate::errors::ErrorKind;

    #[test]
    fn test_fields_with_names() {
        let field_names = vec!["name", "age"];
        let fields = Fields::with_names(field_names.clone()).unwrap();
        assert_eq!(fields.field_names(), field_names);
    }

    #[test]
    fn test_fields_with_empty_names() {
        let result = Fields::with_names(vec![]);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ValidationError);
        }
    }

    #[test]
    fn test_fields_encoded_names() {
        let field_names = vec!["name", "age"];
        let fields = Fields::with_names(field_names).unwrap();
        assert_eq!(fields.encoded_names(), "name|age");
    }

    #[test]
    fn test_fields_starts_with() {
        let fields = Fields::with_names(vec!["name", "age"]).unwrap();
        let prefix = Fields::with_names(vec!["name"]).unwrap();
        assert!(fields.starts_with(&prefix));
    }

    #[test]
    fn test_fields_starts_with_false() {
        let fields = Fields::with_names(vec!["name", "age"]).unwrap();
        let prefix = Fields::with_names(vec!["age"]).unwrap();
        assert!(!fields.starts_with(&prefix));
    }

    #[test]
    fn test_fields_display() {
        let fields = Fields::with_names(vec!["name", "age"]).unwrap();
        assert_eq!(format!("{}", fields), "name|age");
    }

    #[test]
    fn test_fields_ord() {
        let fields1 = Fields::with_names(vec!["name"]).unwrap();
        let fields2 = Fields::with_names(vec!["age"]).unwrap();
        assert!(fields1 > fields2);
    }

    #[test]
    fn test_fields_partial_eq() {
        let fields1 = Fields::with_names(vec!["name"]).unwrap();
        let fields2 = Fields::with_names(vec!["name"]).unwrap();
        assert_eq!(fields1, fields2);
    }

    #[test]
    fn test_fields_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let fields = Fields::with_names(vec!["name"]).unwrap();
        let mut hasher = DefaultHasher::new();
        fields.hash(&mut hasher);
        let hash = hasher.finish();
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_fields_to_value() {
        let fields = Fields::with_names(vec!["name", "age"]).unwrap();
        let value = fields.to_value().unwrap();
        assert_eq!(value.as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_fields_from_value() {
        let value = Value::from_vec(vec!["name".to_string(), "age".to_string()]);
        let fields = Fields::from_value(&value).unwrap();
        assert_eq!(fields.field_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_fields_from_value_error() {
        let value = Value::String("not an array".to_string());
        let result = Fields::from_value(&value);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_sortable_fields_with_names() {
        let field_names = vec!["name".to_string(), "age".to_string()];
        let sortable_fields = SortableFields::with_names(field_names.clone()).unwrap();
        assert_eq!(sortable_fields.field_names(), field_names);
    }

    #[test]
    fn test_sortable_fields_with_empty_names() {
        let result = SortableFields::with_names(vec![]);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ValidationError);
        }
    }

    #[test]
    fn test_sortable_fields_with_names_and_order() {
        let field_names = vec!["name".to_string(), "age".to_string()];
        let sorting_order = vec![("name".to_string(), SortOrder::Ascending), ("age".to_string(), SortOrder::Descending)];
        let sortable_fields = SortableFields::with_names_and_order(field_names.clone(), sorting_order.clone()).unwrap();
        assert_eq!(sortable_fields.field_names(), field_names);
        assert_eq!(sortable_fields.sorting_order(), sorting_order);
    }

    #[test]
    fn test_sortable_fields_add_field() {
        let sortable_fields = SortableFields::new().add_field("name".to_string());
        assert_eq!(sortable_fields.field_names(), vec!["name"]);
    }

    #[test]
    fn test_sortable_fields_add_sorted_field() {
        let sortable_fields = SortableFields::new().add_sorted_field("name".to_string(), SortOrder::Descending);
        assert_eq!(sortable_fields.field_names(), vec!["name"]);
        assert_eq!(sortable_fields.sorting_order(), vec![("name".to_string(), SortOrder::Descending)]);
    }

    #[test]
    fn test_field_values_new() {
        let field_values_vec = vec![("name".to_string(), Value::String("John".to_string()))];
        let nitrite_id = NitriteId::new();
        let fields = Fields::with_names(vec!["name"]).unwrap();
        let field_values = FieldValues::new(field_values_vec.clone(), nitrite_id, fields.clone());
        assert_eq!(field_values.values(), field_values_vec);
        assert_eq!(field_values.nitrite_id(), &nitrite_id);
        assert_eq!(field_values.fields(), &fields);
    }

    #[test]
    fn test_field_values_get_value() {
        let field_values = vec![("name".to_string(), Value::String("John".to_string()))];
        let nitrite_id = NitriteId::new();
        let fields = Fields::with_names(vec!["name"]).unwrap();
        let field_values = FieldValues::new(field_values, nitrite_id, fields);
        assert_eq!(field_values.get_value("name").unwrap(), &Value::String("John".to_string()));
    }

    #[test]
    fn test_field_values_get_value_none() {
        let field_values = vec![("name".to_string(), Value::String("John".to_string()))];
        let nitrite_id = NitriteId::new();
        let fields = Fields::with_names(vec!["name"]).unwrap();
        let field_values = FieldValues::new(field_values, nitrite_id, fields);
        assert!(field_values.get_value("age").is_none());
    }

    #[test]
    fn test_fields_from_value_with_mixed_array_types() {
        // Test that from_value properly handles corrupted metadata with mixed types
        let mixed_array = Value::Array(vec![
            Value::String("field1".to_string()),
            Value::I32(100),  // Non-string element - should cause error
            Value::String("field2".to_string()),
        ]);
        let result = Fields::from_value(&mixed_array);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_fields_from_value_with_first_non_string_element() {
        // Test that from_value catches non-string at start of array
        let array = Value::Array(vec![
            Value::I32(42),  // Non-string at start
            Value::String("field1".to_string()),
        ]);
        let result = Fields::from_value(&array);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_fields_from_value_with_last_non_string_element() {
        // Test that from_value catches non-string at end of array
        let array = Value::Array(vec![
            Value::String("field1".to_string()),
            Value::Null,  // Non-string at end
        ]);
        let result = Fields::from_value(&array);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_fields_from_value_with_all_non_string_elements() {
        // Test that from_value rejects array with all non-string elements
        let array = Value::Array(vec![
            Value::I32(1),
            Value::F64(2.5),
            Value::Null,
        ]);
        let result = Fields::from_value(&array);
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_fields_from_value_valid_multi_field_names() {
        // Test that from_value correctly processes valid string arrays
        let value = Value::Array(vec![
            Value::String("firstName".to_string()),
            Value::String("lastName".to_string()),
            Value::String("email".to_string()),
        ]);
        let fields = Fields::from_value(&value).unwrap();
        assert_eq!(fields.field_names(), vec!["firstName", "lastName", "email"]);
    }

    #[test]
    fn test_fields_from_value_roundtrip() {
        // Test that to_value and from_value are symmetric
        let original_fields = Fields::with_names(vec!["field1", "field2", "field3"]).unwrap();
        let value = original_fields.to_value().unwrap();
        let restored_fields = Fields::from_value(&value).unwrap();
        assert_eq!(original_fields, restored_fields);
        assert_eq!(original_fields.field_names(), restored_fields.field_names());
    }

    #[test]
    fn test_fields_from_value_empty_array_name_validation() {
        // Test that from_value properly validates empty field names
        let array_with_empty_string = Value::Array(vec![
            Value::String("field1".to_string()),
            Value::String("".to_string()),  // Empty string field name
        ]);
        // Empty string is still a valid string, so this should succeed
        let fields = Fields::from_value(&array_with_empty_string).unwrap();
        assert_eq!(fields.field_names().len(), 2);
    }

    #[test]
    fn bench_fields_encoded_names() {
        let fields = Fields::with_names(vec!["field1", "field2", "field3", "field4"]).unwrap();
        for _ in 0..1000 {
            let _ = fields.encoded_names();
        }
    }

    #[test]
    fn bench_sortable_fields_operations() {
        for _ in 0..100 {
            let sortable = SortableFields::with_names(vec![
                "field1".to_string(),
                "field2".to_string(),
                "field3".to_string(),
            ]).unwrap();
            let _ = sortable.sorting_order();
        }
    }

    #[test]
    fn bench_field_values_access() {
        let fields = Fields::with_names(vec!["name", "age", "email"]).unwrap();
        let field_values = vec![
            ("name".to_string(), Value::String("John".to_string())),
            ("age".to_string(), Value::I32(30)),
            ("email".to_string(), Value::String("john@example.com".to_string())),
        ];
        let fv = FieldValues::new(field_values, NitriteId::new(), fields);
        
        for _ in 0..1000 {
            let _ = fv.get_value("name");
            let _ = fv.get_value("age");
            let _ = fv.get_value("email");
        }
    }
}
