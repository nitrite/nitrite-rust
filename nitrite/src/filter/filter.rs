use crate::collection::Document;
use crate::collection::NitriteId;
use crate::errors::ErrorKind;
use crate::errors::NitriteError;
use crate::errors::NitriteResult;
use crate::index::IndexMap;
use crate::Value;
use crate::DOC_ID;
use std::any::Any;
use std::fmt::Display;
use std::ops::Deref;
use std::sync::Arc;

use super::AllFilter;
use super::AndFilter;
use super::ElementMatchFilter;
use super::EqualsFilter;
use super::NotFilter;
use super::OrFilter;
use super::TextFilter;

/// Trait for implementing custom filters.
///
/// A `FilterProvider` defines how to evaluate filter conditions on documents.
/// Implementations can provide optimizations through index scanning and support
/// for various filter operations.
pub trait FilterProvider: Any + Send + Sync + Display {
    /// Applies the filter to a document and returns whether it matches.
    ///
    /// # Arguments
    ///
    /// * `entry` - The document to evaluate
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the document matches the filter, `Ok(false)` otherwise
    fn apply(&self, entry: &Document) -> NitriteResult<bool>;

    /// Applies the filter using an index map for optimized scanning.
    ///
    /// This method can be overridden to provide index-accelerated query execution.
    /// The default implementation returns an error.
    ///
    /// # Arguments
    ///
    /// * `index_map` - The index map to scan
    ///
    /// # Returns
    ///
    /// A list of matching keys from the index, or an error if index scanning is not supported
    fn apply_on_index(&self, _index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        log::error!("Filter {} does not support index scan", self);
        Err(NitriteError::new(
            "Filter does not support index scan",
            ErrorKind::FilterError,
        ))
    }

    /// Gets the associated collection name for this filter.
    ///
    /// # Returns
    ///
    /// The collection name, or an error if not set
    fn get_collection_name(&self) -> NitriteResult<String> {
        log::error!("Filter {} does not have collection name", self);
        Err(NitriteError::new(
            "Filter does not have collection name",
            ErrorKind::FilterError,
        ))
    }

    /// Sets the collection name for this filter.
    ///
    /// # Arguments
    ///
    /// * `collection_name` - The name of the collection
    fn set_collection_name(&self, _collection_name: String) -> NitriteResult<()> {
        Ok(())
    }
    
    /// Checks if this filter operates on a specific field.
    #[inline]
    fn has_field(&self) -> bool {
        false
    }

    /// Gets the field name this filter operates on.
    ///
    /// # Returns
    ///
    /// The field name, or an error if the filter doesn't operate on a specific field
    fn get_field_name(&self) -> NitriteResult<String> {
        log::error!("Filter {} does not have field name", self);
        Err(NitriteError::new(
            "Filter does not have field name",
            ErrorKind::FilterError,
        ))
    }

    /// Sets the field name for this filter.
    fn set_field_name(&self, _field_name: String) -> NitriteResult<()> {
        Ok(())
    }

    /// Gets the field value this filter operates on.
    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        log::debug!("Filter {} does not have field value", self);
        Err(NitriteError::new(
            "Filter does not have field value",
            ErrorKind::FilterError,
        ))
    }

    /// Sets the field value for this filter.
    fn set_field_value(&self, _field_value: Value) -> NitriteResult<()> {
        Ok(())
    }

    /// Gets whether reverse scanning is enabled.
    fn get_reverse_scan(&self) -> NitriteResult<bool> {
        log::error!("Filter {} does not have reverse scan", self);
        Err(NitriteError::new(
            "Filter does not have reverse scan",
            ErrorKind::FilterError,
        ))
    }

    /// Sets whether to use reverse scanning.
    fn set_reverse_scan(&self, _reverse_scan: bool) -> NitriteResult<()> {
        Ok(())
    }

    /// Checks if this filter supports reverse scanning through indexes.
    #[inline]
    fn is_reverse_scan_supported(&self) -> bool {
        false
    }

    /// Checks if this filter can be satisfied by index lookup alone.
    #[inline]
    fn is_index_only_filter(&self) -> bool {
        false
    }

    /// Gets the index type that this filter supports.
    fn supported_index_type(&self) -> NitriteResult<String> {
        log::error!("Filter {} does not specify supported index type", self);
        Err(NitriteError::new(
            "Filter does not specify supported index type",
            ErrorKind::FilterError,
        ))
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        let _ = other;
        Ok(false)
    }

    #[inline]
    fn process_index_value(
        &self,
        value: Option<Value>,
        sub_map: &mut Vec<Value>,
        nitrite_ids: &mut Vec<Value>,
    ) {
        match value {
            Some(Value::Array(array)) => {
                // if it is list then add it directly to nitrite ids
                nitrite_ids.extend(array.iter().cloned());
            }
            Some(Value::Map(map)) => {
                // if it is map then add it to sub map
                sub_map.push(Value::Map(map));
            }
            Some(_) | None => {}
        }
    }

    fn validate_array_search_term(&self, field: String, value: &Value) -> NitriteResult<()> {
        match value {
            Value::Array(array) => {
                for v in array {
                    match v {
                        Value::Null => continue,
                        Value::Array(_) => {
                            log::error!("Nested array field {} is not supported", field);
                            return Err(NitriteError::new(
                                "Nested array is not supported",
                                ErrorKind::FilterError,
                            ))
                        }
                        _ => {
                            if !v.is_comparable() {
                                log::error!("Cannot filter using non comparable values {}", field);
                                return Err(NitriteError::new(
                                    &format!("Cannot filter using non comparable values {}", field),
                                    ErrorKind::FilterError,
                                ));
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn logical_filters(&self) -> NitriteResult<Vec<Filter>> {
        Err(NitriteError::new(
            "Filter is not a logical filters",
            ErrorKind::FilterError,
        ))
    }

    fn as_any(&self) -> &dyn Any;
}

/// A query filter for selecting documents from a collection.
///
/// `Filter` encapsulates filter logic through a provider pattern that supports
/// custom filtering implementations. Filters are used with collection `find()` and
/// similar methods to query documents with various conditions.
///
/// # Filter Composition
///
/// Filters can be composed using logical operators:
/// - `and(other)` - Combines with another filter using logical AND
/// - `or(other)` - Combines with another filter using logical OR
/// - `not()` - Negates the filter using logical NOT
///
/// # Responsibilities
///
/// * **Document Matching**: Evaluates whether documents match filter conditions
/// * **Index Support**: Provides index-accelerated query evaluation
/// * **Field Operations**: Supports filtering by specific field names and values
/// * **Logical Composition**: Enables combining multiple filters with AND/OR/NOT
/// * **Query Optimization**: Supports reverse scanning and index-only filtering
#[derive(Clone)]
pub struct Filter {
    inner: Arc<dyn FilterProvider>,
}

impl Filter {
    /// Creates a new filter from a filter provider implementation.
    ///
    /// # Arguments
    ///
    /// * `inner` - A type implementing `FilterProvider`
    ///
    /// # Returns
    ///
    /// A new `Filter` instance wrapping the provider
    pub fn new<T: FilterProvider + 'static>(inner: T) -> Self {
        Filter { inner: Arc::new(inner) }
    }

    /// Combines this filter with another using logical AND.
    ///
    /// # Arguments
    ///
    /// * `filter` - The other filter to combine
    ///
    /// # Returns
    ///
    /// A new `Filter` representing `self AND filter`
    pub fn and(&self, filter: Filter) -> Self {
        Filter::new(AndFilter::new(vec![filter, self.clone()]))
    }

    /// Combines this filter with another using logical OR.
    ///
    /// # Arguments
    ///
    /// * `filter` - The other filter to combine
    ///
    /// # Returns
    ///
    /// A new `Filter` representing `self OR filter`
    pub fn or(&self, filter: Filter) -> Self {
        Filter::new(OrFilter::new(vec![filter, self.clone()]))
    }

    /// Negates this filter using logical NOT.
    ///
    /// # Returns
    ///
    /// A new `Filter` representing `NOT self`
    pub fn not(&self) -> Self {
        Filter::new(NotFilter::new(self.clone()))
    }
}

impl Display for Filter {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Deref for Filter {
    type Target = Arc<dyn FilterProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Creates a filter that matches all documents.
///
/// This filter accepts every document in the collection without applying
/// any filtering conditions.
///
/// # Returns
///
/// A `Filter` that matches all documents
pub fn all() -> Filter {
    Filter::new(AllFilter {})
}

/// Creates a filter that matches a document by its ID.
///
/// Matches documents that have the specified `NitriteId` as their internal document ID.
///
/// # Arguments
///
/// * `id` - The `NitriteId` to match
///
/// # Returns
///
/// A `Filter` that matches the document with the specified ID
pub fn by_id(id: NitriteId) -> Filter {
    Filter::new(EqualsFilter::new(
        DOC_ID.to_string(),
        Value::NitriteId(id),
    ))
}

/// Combines multiple filters using logical AND.
///
/// Creates a filter that matches documents satisfying all of the provided filters.
///
/// # Arguments
///
/// * `filters` - A vector of filters to combine
///
/// # Returns
///
/// A `Filter` representing the AND of all filters
pub fn and(filters: Vec<Filter>) -> Filter {
    Filter::new(AndFilter::new(filters))
}

/// Combines multiple filters using logical OR.
///
/// Creates a filter that matches documents satisfying at least one of the provided filters.
///
/// # Arguments
///
/// * `filters` - A vector of filters to combine
///
/// # Returns
///
/// A `Filter` representing the OR of all filters
pub fn or(filters: Vec<Filter>) -> Filter {
    Filter::new(OrFilter::new(filters))
}

/// Negates a filter using logical NOT.
///
/// Creates a filter that matches documents not matching the provided filter.
///
/// # Arguments
///
/// * `filter` - The filter to negate
///
/// # Returns
///
/// A `Filter` representing `NOT filter`
pub fn not(filter: Filter) -> Filter {
    Filter::new(NotFilter::new(filter))
}

/// Internal filter for optimized index scans.
///
/// This struct groups multiple filters for coordinated index-accelerated query execution.
/// It represents a collection of filters that can be evaluated using index lookups
/// rather than full collection scans, enabling efficient query optimization.
///
/// # Responsibilities
///
/// * **Filter Aggregation**: Collects multiple filters for joint evaluation
/// * **Index Optimization**: Enables index-based query planning
/// * **Filter Preservation**: Maintains filter list for execution
#[derive(Clone)]
pub struct IndexScanFilter {
    inner: Arc<IndexScanFilterInner>,
}

pub(crate) struct IndexScanFilterInner {
    filters: Vec<Filter>,
}

impl IndexScanFilter {
    pub(crate) fn new(filters: Vec<Filter>) -> Self {
        IndexScanFilter {
            inner: Arc::new(IndexScanFilterInner { filters }),
        }
    }

    /// Gets the filters in this index scan.
    pub fn filters(&self) -> Vec<Filter> {
        self.inner.filters.clone()
    }
}

pub(crate) fn is_all_filter(filter: &Filter) -> bool {
    filter.as_any().is::<AllFilter>()
}

pub(crate) fn is_and_filter(filter: &Filter) -> bool {
    filter.as_any().is::<AndFilter>()
}

pub(crate) fn is_or_filter(filter: &Filter) -> bool {
    filter.as_any().is::<OrFilter>()
}

pub(crate) fn is_text_filter(filter: &Filter) -> bool {
    filter.as_any().is::<TextFilter>()
}

pub(crate) fn is_equals_filter(filter: &Filter) -> bool {
    filter.as_any().is::<EqualsFilter>()
}

pub(crate) fn is_element_match_filter(filter: &Filter) -> bool {
    filter.as_any().is::<ElementMatchFilter>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{Document, NitriteId};
    use crate::common::Convertible;
    use crate::filter::field;
    use crate::index::IndexMap;
    use crate::Value;
    use std::collections::BTreeMap;
    use std::fmt::Formatter;

    struct MockFilter;

    impl Display for MockFilter {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "MockFilter")
        }
    }

    impl FilterProvider for MockFilter {
        fn apply(&self, _entry: &Document) -> NitriteResult<bool> {
            Ok(true)
        }

        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_filter_apply() {
        let filter = Filter::new(MockFilter);
        let doc = Document::new();
        assert_eq!(filter.apply(&doc).unwrap(), true);
    }

    #[test]
    fn test_filter_apply_on_index() {
        let filter = Filter::new(MockFilter);
        let index_map = IndexMap::new(None, Some(BTreeMap::new()));
        assert!(filter.apply_on_index(&index_map).is_err());
    }

    #[test]
    fn test_filter_get_collection_name() {
        let filter = Filter::new(MockFilter);
        assert!(filter.get_collection_name().is_err());
    }

    #[test]
    fn test_filter_set_collection_name() {
        let filter = Filter::new(MockFilter);
        assert!(filter.set_collection_name("test".to_string()).is_ok());
    }

    #[test]
    fn test_filter_get_field_name() {
        let filter = Filter::new(MockFilter);
        assert!(filter.get_field_name().is_err());
    }

    #[test]
    fn test_filter_set_field_name() {
        let filter = Filter::new(MockFilter);
        assert!(filter.set_field_name("test".to_string()).is_ok());
    }

    #[test]
    fn test_filter_get_field_value() {
        let filter = Filter::new(MockFilter);
        assert!(filter.get_field_value().is_err());
    }

    #[test]
    fn test_filter_set_field_value() {
        let filter = Filter::new(MockFilter);
        assert!(filter.set_field_value(Value::I32(42)).is_ok());
    }

    #[test]
    fn test_filter_get_reverse_scan() {
        let filter = Filter::new(MockFilter);
        assert!(filter.get_reverse_scan().is_err());
    }

    #[test]
    fn test_filter_set_reverse_scan() {
        let filter = Filter::new(MockFilter);
        assert!(filter.set_reverse_scan(true).is_ok());
    }

    #[test]
    fn test_filter_is_reverse_scan_supported() {
        let filter = Filter::new(MockFilter);
        assert!(!filter.is_reverse_scan_supported());
    }

    #[test]
    fn test_filter_is_index_only_filter() {
        let filter = Filter::new(MockFilter);
        assert!(!filter.is_index_only_filter());
    }

    #[test]
    fn test_filter_supported_index_type() {
        let filter = Filter::new(MockFilter);
        assert!(filter.supported_index_type().is_err());
    }

    #[test]
    fn test_filter_can_be_grouped() {
        let filter1 = Filter::new(MockFilter);
        let filter2 = Filter::new(MockFilter);
        assert!(!filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_filter_process_index_value() {
        let filter = Filter::new(MockFilter);
        let mut sub_map = Vec::new();
        let mut nitrite_ids = Vec::new();
        filter.process_index_value(Some(Value::Array(vec![Value::I32(42)])), &mut sub_map, &mut nitrite_ids);
        assert_eq!(nitrite_ids, vec![Value::I32(42)]);
    }

    #[test]
    fn test_filter_validate_array_search_term() {
        let filter = Filter::new(MockFilter);
        let value = Value::Array(vec![Value::I32(42)]);
        assert!(filter.validate_array_search_term("field".to_string(), &value).is_ok());
    }

    #[test]
    fn test_filter_logical_filters() {
        let filter = Filter::new(MockFilter);
        assert!(filter.logical_filters().is_err());
    }

    #[test]
    fn test_all_filter() {
        let filter = all();
        let doc = Document::new();
        assert!(filter.apply(&doc).is_ok());
    }

    #[test]
    fn test_by_id_filter() {
        let id = NitriteId::new();
        let filter = by_id(id.clone());
        let mut doc = Document::new();
        doc.put(DOC_ID, Value::NitriteId(id)).expect("Failed to put value");
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_and_filter() {
        let filter = and(vec![all(), all()]);
        let doc = Document::new();
        assert!(filter.apply(&doc).is_ok());
    }

    #[test]
    fn test_or_filter() {
        let filter = or(vec![all(), all()]);
        let doc = Document::new();
        assert!(filter.apply(&doc).is_ok());
    }

    #[test]
    fn test_not_filter() {
        let filter = not(all());
        let doc = Document::new();
        assert!(filter.apply(&doc).is_ok());
    }

    #[test]
    fn test_index_scan_filter() {
        let filter = IndexScanFilter::new(vec![all()]);
        assert_eq!(filter.filters().len(), 1);
    }

    #[test]
    fn test_is_all_filter() {
        let filter = all();
        assert!(is_all_filter(&filter));
    }

    #[test]
    fn test_is_and_filter() {
        let filter = and(vec![all(), all()]);
        assert!(is_and_filter(&filter));
    }

    #[test]
    fn test_is_or_filter() {
        let filter = or(vec![all(), all()]);
        assert!(is_or_filter(&filter));
    }

    #[test]
    fn test_is_text_filter() {
        let filter = field("field").text("value");
        assert!(is_text_filter(&filter));
    }

    #[test]
    fn test_is_equals_filter() {
        let filter = field("field").eq("value".to_value().unwrap());
        assert!(is_equals_filter(&filter));
    }

    #[test]
    fn test_is_element_match_filter() {
        let filter = field("field").elem_match(all());
        assert!(is_element_match_filter(&filter));
    }
}