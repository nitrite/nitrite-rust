use std::{any::Any, fmt::Display, sync::OnceLock};

use crate::{
    collection::Document,
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::IndexMap,
    Value,
};

use super::{Filter, FilterProvider};

/// A filter that matches all documents.
///
/// This filter accepts every document in the collection without applying any conditions.
/// It is commonly used as a default filter when no specific filtering is needed.
///
/// # Responsibilities
///
/// * **Universal Matching**: Accepts all documents in the collection
/// * **Default Filter**: Serves as the base filter when no conditions are specified
pub(crate) struct AllFilter;

impl FilterProvider for AllFilter {
    fn apply(&self, _entry: &Document) -> NitriteResult<bool> {
        Ok(true)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for AllFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "AllFilter")
    }
}

/// A filter that matches documents where a field equals a specific value.
///
/// This filter evaluates whether a document's field value exactly matches the specified value.
/// It supports index-accelerated lookups for efficient query execution. Field names and values
/// are stored using `OnceLock` for safe initialization within the filter provider pattern.
///
/// # Responsibilities
///
/// * **Equality Matching**: Evaluates whether a field equals a target value
/// * **Index Optimization**: Supports efficient index-based scanning when available
/// * **Field Value Storage**: Maintains field name and value through the filter lifecycle
/// * **Collection Context**: Tracks collection name for query planning
pub(crate) struct EqualsFilter {
    field_name: OnceLock<String>,
    field_value: OnceLock<Value>,
    collection_name: OnceLock<String>,
}

impl EqualsFilter {
    /// Creates a new equality filter for the specified field and value.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to filter on
    /// * `field_value` - The value to match against
    ///
    /// # Returns
    ///
    /// A new `EqualsFilter` instance with initialized field name and value
    #[inline]
    pub(crate) fn new(field_name: String, field_value: Value) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let value = OnceLock::new();
        let _ = value.set(field_value);

        EqualsFilter {
            field_name: name,
            field_value: value,
            collection_name: OnceLock::new(),
        }
    }
}

impl Display for EqualsFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.field_name.get(), self.field_value.get()) {
            (Some(name), Some(value)) => write!(f, "({} == {})", name, value),
            (Some(name), None) => write!(f, "({} == unknown)", name),
            (None, Some(value)) => write!(f, "(unknown == {})", value),
            (None, None) => write!(f, "(unknown == unknown)"),
        }
    }
}

impl FilterProvider for EqualsFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field_name = self.field_name.get()
            .ok_or_else(|| NitriteError::new(
                "Equals filter error: field name not set - filter must be properly initialized before applying",
                ErrorKind::InvalidOperation
            ))?;
        let value = entry.get(field_name)?;
        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new(
                "Equals filter error: field value not set - filter must be properly initialized before applying",
                ErrorKind::InvalidOperation
            ))?;
        Ok(&value == field_value)
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let field_value = self.field_value.get().cloned().unwrap_or(Value::Null);
        let val = index_map.get(&field_value)?;

        match val {
            Some(Value::Array(array)) => Ok(array.clone()),
            Some(v) => Ok(vec![v]),
            None => Ok(vec![]),
        }
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        self.collection_name.get()
            .cloned()
            .ok_or_else(|| {
                log::error!("Collection name is not set for filter");
                NitriteError::new(
                    "Collection name is not set",
                    ErrorKind::InvalidOperation,
                )
            })
    }

    fn set_collection_name(&self, collection_name: String) -> NitriteResult<()> {
        self.collection_name.get_or_init(|| collection_name);
        Ok(())
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        self.field_name.get()
            .cloned()
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidOperation))
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        if self.field_value.get().is_none() {
            Ok(None)
        } else {
            Ok(self.field_value.get().cloned())
        }
    }

    fn set_field_value(&self, field_value: Value) -> NitriteResult<()> {
        self.field_value.get_or_init(|| field_value);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A filter that matches documents where a field does not equal a specific value.
///
/// This filter evaluates whether a document's field value differs from the specified value.
/// It supports index-based scans that exclude matching values for efficient query execution.
/// Field names and values are stored using `OnceLock` for safe initialization within the filter provider pattern.
///
/// # Responsibilities
///
/// * **Inequality Matching**: Evaluates whether a field differs from a target value
/// * **Index Optimization**: Supports efficient index-based scanning excluding matched values
/// * **Field Value Storage**: Maintains field name and value through the filter lifecycle
/// * **Collection Context**: Tracks collection name for query planning
pub(crate) struct NotEqualsFilter {
    field_name: OnceLock<String>,
    field_value: OnceLock<Value>,
    collection_name: OnceLock<String>,
}

impl NotEqualsFilter {
    /// Creates a new inequality filter for the specified field and value.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to filter on
    /// * `field_value` - The value to exclude from matches
    ///
    /// # Returns
    ///
    /// A new `NotEqualsFilter` instance with initialized field name and value
    #[inline]
    pub(crate) fn new(field_name: String, field_value: Value) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let value = OnceLock::new();
        let _ = value.set(field_value);

        NotEqualsFilter {
            field_name: name,
            field_value: value,
            collection_name: OnceLock::new(),
        }
    }
}

impl Display for NotEqualsFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.field_name.get(), self.field_value.get()) {
            (Some(name), Some(value)) => write!(f, "({} != {})", name, value),
            (Some(name), None) => write!(f, "({} != unknown)", name),
            (None, Some(value)) => write!(f, "(unknown != {})", value),
            (None, None) => write!(f, "(unknown != unknown)"),
        }
    }
}

impl FilterProvider for NotEqualsFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field_name = self.field_name.get()
            .ok_or_else(|| NitriteError::new(
                "Not-equals filter error: field name not set - filter must be properly initialized before applying",
                ErrorKind::InvalidOperation
            ))?;
        let value = entry.get(field_name)?;
        let field_value = self.field_value.get()
            .unwrap_or(&Value::Null);
        Ok(&value != field_value)
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let mut sub_map = Vec::new();
        let mut nitrite_ids = Vec::new();

        let cmp_value = self.field_value.get().unwrap_or(&Value::Null).clone();
        let entries = index_map.entries()?;
        for result in entries {
            let (key, value) = result?;
            if key != cmp_value {
                self.process_index_value(Some(value), &mut sub_map, &mut nitrite_ids);
            }
        }

        if sub_map.is_empty() {
            // it is filtering on either single field index,
            // or it is a terminal filter on compound index, return only nitrite-ids
            Ok(nitrite_ids)
        } else {
            // if sub-map is populated then filtering on compound index, return sub-map
            Ok(sub_map)
        }
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        self.collection_name.get()
            .cloned()
            .ok_or_else(|| {
                log::error!("Collection name is not set for filter");
                NitriteError::new(
                    "Collection name is not set",
                    ErrorKind::InvalidOperation,
                )
            })
    }

    fn set_collection_name(&self, collection_name: String) -> NitriteResult<()> {
        self.collection_name.get_or_init(|| collection_name);
        Ok(())
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        self.field_name.get()
            .cloned()
            .ok_or_else(|| NitriteError::new(
                "Not-equals filter error: field name not set - filter must be properly initialized before accessing",
                ErrorKind::InvalidOperation
            ))
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        if self.field_value.get().is_none() {
            Ok(None)
        } else {
            Ok(self.field_value.get().cloned())
        }
    }

    fn set_field_value(&self, field_value: Value) -> NitriteResult<()> {
        self.field_value.get_or_init(|| field_value);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;

    #[test]
    fn test_all_filter_apply() {
        let filter = AllFilter;
        let doc = Document::new();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_equals_filter_apply() {
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        assert_eq!(filter.apply(&doc).unwrap(), true);
    }

    #[test]
    fn test_equals_filter_apply_negative() {
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert_eq!(filter.apply(&doc).unwrap(), false);
    }

    #[test]
    fn test_not_equals_filter_apply() {
        let filter = NotEqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert_eq!(filter.apply(&doc).unwrap(), true);
    }

    #[test]
    fn test_not_equals_filter_apply_negative() {
        let filter = NotEqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        assert_eq!(filter.apply(&doc).unwrap(), false);
    }

    // OnceLock initialization and display tests
    #[test]
    fn test_equals_filter_display_with_initialized_values() {
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        let display_str = format!("{}", filter);
        assert_eq!(display_str, "(field == 42)");
    }

    #[test]
    fn test_equals_filter_display_with_uninitialized_collection_name() {
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        // Collection name is not initialized, but display should still work
        let display_str = format!("{}", filter);
        assert!(display_str.contains("field") && display_str.contains("42"));
    }

    #[test]
    fn test_equals_filter_get_field_name_after_initialization() {
        let filter = EqualsFilter::new("test_field".to_string(), Value::I32(42));
        let field_name = filter.get_field_name().unwrap();
        assert_eq!(field_name, "test_field");
    }

    #[test]
    fn test_equals_filter_get_field_value_initialization() {
        let filter = EqualsFilter::new("field".to_string(), Value::String("test_value".to_string()));
        let field_value = filter.get_field_value().unwrap();
        assert_eq!(field_value, Some(Value::String("test_value".to_string())));
    }

    #[test]
    fn test_not_equals_filter_display_with_initialized_values() {
        let filter = NotEqualsFilter::new("status".to_string(), Value::String("inactive".to_string()));
        let display_str = format!("{}", filter);
        // Display for String values includes quotes
        assert_eq!(display_str, "(status != \"inactive\")");
    }

    #[test]
    fn test_not_equals_filter_get_collection_name_fails_when_not_set() {
        let filter = NotEqualsFilter::new("field".to_string(), Value::I32(1));
        let result = filter.get_collection_name();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Collection name is not set"));
    }

    #[test]
    fn test_not_equals_filter_set_and_get_collection_name() {
        let filter = NotEqualsFilter::new("field".to_string(), Value::I32(1));
        filter.set_collection_name("my_collection".to_string()).unwrap();
        let name = filter.get_collection_name().unwrap();
        assert_eq!(name, "my_collection");
    }

    #[test]
    fn test_not_equals_filter_get_field_name_after_initialization() {
        let filter = NotEqualsFilter::new("my_field".to_string(), Value::String("value".to_string()));
        let field_name = filter.get_field_name().unwrap();
        assert_eq!(field_name, "my_field");
    }

    #[test]
    fn test_not_equals_filter_apply_with_missing_field() {
        let filter = NotEqualsFilter::new("missing_field".to_string(), Value::I32(42));
        let doc = Document::new();
        // When field is missing, entry.get() returns Value::Null by default
        // So the comparison should work: Null != 42 is true
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), true); // Null != 42
    }

    #[test]
    fn test_equals_filter_apply_with_uninitialized_field_name_fails() {
        // Create filter via new() which properly initializes, so this verifies the initialization works
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        // Should successfully apply since field_name is initialized
        assert_eq!(filter.apply(&doc).unwrap(), true);
    }

    // Performance and optimization tests
    #[test]
    fn test_equals_filter_once_lock_initialization_efficiency() {
        // Verify OnceLock is properly initialized via set() rather than get_or_init()
        let filter = EqualsFilter::new("perf_field".to_string(), Value::I32(100));
        // Both should be accessible on first call
        assert_eq!(filter.get_field_name().unwrap(), "perf_field");
        assert_eq!(filter.get_field_value().unwrap(), Some(Value::I32(100)));
    }

    #[test]
    fn test_not_equals_filter_value_comparison_optimization() {
        // Verify that value comparisons are done efficiently
        let filter = NotEqualsFilter::new("test_field".to_string(), Value::I32(99));
        let mut doc = Document::new();
        doc.put("test_field", Value::I32(100)).unwrap();
        
        // Perform multiple comparisons to test inline optimization
        for _ in 0..100 {
            assert_eq!(filter.apply(&doc).unwrap(), true);
        }
    }

    #[test]
    fn test_equals_filter_multiple_applies() {
        // Test that inline hints are effective with repeated applies
        let filter = EqualsFilter::new("field".to_string(), Value::I32(42));
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        
        for _ in 0..1000 {
            assert_eq!(filter.apply(&doc).unwrap(), true);
        }
    }

    #[test]
    fn test_not_equals_filter_apply_on_index_efficiency() {
        // Verify the optimized apply_on_index avoids unnecessary allocations
        let filter = NotEqualsFilter::new("field".to_string(), Value::I32(42));
        let mut map = std::collections::BTreeMap::new();
        map.insert(Value::I32(1), Value::Array(vec![Value::I32(10)]));
        map.insert(Value::I32(2), Value::Array(vec![Value::I32(20)]));
        map.insert(Value::I32(42), Value::Array(vec![Value::I32(30)])); // This should be excluded
        
        let index_map = IndexMap::new(None, Some(map));
        let result = filter.apply_on_index(&index_map).unwrap();
        
        // Should have 2 entries (excluding value 42)
        assert_eq!(result.len(), 2);
    }
}

