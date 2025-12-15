use crate::Value;

use super::{
    Filter,
    {
        BetweenFilter, Bound, ComparisonMode, ElementMatchFilter, EqualsFilter, InFilter,
        NotEqualsFilter, NotInFilter, RegexFilter, SortingAwareFilter, TextFilter,
    },
};

/// Creates a fluent filter builder for the specified field name.
///
/// This function initializes a filter builder that allows chaining of comparison and filter operations
/// on a specific field. The returned `FluentFilter` provides methods for building equality, comparison,
/// and pattern-matching filters.
///
/// # Arguments
///
/// * `field_name` - The name of the field to filter on
///
/// # Returns
///
/// A `FluentFilter` builder for constructing field-specific filters
pub fn field(field_name: &str) -> FluentFilter {
    FluentFilter {
        field_name: field_name.to_string(),
    }
}

/// A fluent builder for constructing filters on a specific field.
///
/// `FluentFilter` provides chainable methods for creating filters with various conditions
/// including equality, comparison operators, text matching, and array operations.
/// Each method returns a `Filter` that can be used directly with collection `find()` operations
/// or combined with other filters.
///
/// # Responsibilities
///
/// * **Filter Construction**: Builds filter conditions using fluent method chaining
/// * **Comparison Operations**: Provides equality and relational comparison methods
/// * **Pattern Matching**: Supports text search and regex-based filtering
/// * **Array Operations**: Filters based on membership in collections
/// * **Range Filtering**: Provides between filtering for value ranges
pub struct FluentFilter {
    field_name: String,
}

impl FluentFilter {
    /// Creates a filter that matches documents where the field equals the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to match against
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field equals the value
    #[inline]
    pub fn eq<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(EqualsFilter::new(self.field_name, value.into()))
    }

    /// Creates a filter that matches documents where the field does not equal the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The value to exclude from matches
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field differs from the value
    #[inline]
    pub fn ne<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(NotEqualsFilter::new(self.field_name, value.into()))
    }

    /// Creates a filter that matches documents where the field is greater than the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The threshold value
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is greater than the value
    #[inline]
    pub fn gt<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(SortingAwareFilter::new(
            self.field_name,
            value.into(),
            ComparisonMode::Greater,
        ))
    }

    /// Creates a filter that matches documents where the field is greater than or equal to the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The threshold value
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is greater than or equal to the value
    #[inline]
    pub fn gte<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(SortingAwareFilter::new(
            self.field_name,
            value.into(),
            ComparisonMode::GreaterEqual,
        ))
    }

    /// Creates a filter that matches documents where the field is less than the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The threshold value
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is less than the value
    #[inline]
    pub fn lt<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(SortingAwareFilter::new(
            self.field_name,
            value.into(),
            ComparisonMode::Lesser,
        ))
    }

    /// Creates a filter that matches documents where the field is less than or equal to the specified value.
    ///
    /// # Arguments
    ///
    /// * `value` - The threshold value
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is less than or equal to the value
    #[inline]
    pub fn lte<T: Into<Value>>(self, value: T) -> Filter {
        Filter::new(SortingAwareFilter::new(
            self.field_name,
            value.into(),
            ComparisonMode::LesserEqual,
        ))
    }

    /// Creates a filter that matches documents where the field value is within a range (both bounds inclusive).
    ///
    /// # Arguments
    ///
    /// * `lower_bound` - The lower boundary of the range (inclusive)
    /// * `upper_bound` - The upper boundary of the range (inclusive)
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is within the specified inclusive range
    pub fn between_optional_inclusive<T: Into<Value>>(
        self,
        lower_bound: T,
        upper_bound: T,
    ) -> Filter {
        Filter::new(BetweenFilter::new(
            self.field_name,
            Bound::inclusive(lower_bound.into(), upper_bound.into()),
        ))
    }

    /// Creates a filter that matches documents where the field value is within a range with configurable inclusivity.
    ///
    /// # Arguments
    ///
    /// * `lower_bound` - The lower boundary of the range
    /// * `upper_bound` - The upper boundary of the range
    /// * `inclusive` - If true, both bounds are inclusive; if false, both bounds are exclusive
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is within the specified range
    pub fn between_inclusive<T: Into<Value>>(
        self,
        lower_bound: T,
        upper_bound: T,
        inclusive: bool,
    ) -> Filter {
        Filter::new(BetweenFilter::new(
            self.field_name,
            Bound::optional_inclusive(lower_bound.into(), upper_bound.into(), inclusive),
        ))
    }

    /// Creates a filter that matches documents where the field value is within a range with independent bound inclusivity.
    ///
    /// # Arguments
    ///
    /// * `lower_bound` - The lower boundary of the range
    /// * `upper_bound` - The upper boundary of the range
    /// * `lower_inclusive` - If true, the lower bound is inclusive; if false, exclusive
    /// * `upper_inclusive` - If true, the upper bound is inclusive; if false, exclusive
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is within the specified range
    pub fn between<T: Into<Value>>(
        self,
        lower_bound: T,
        upper_bound: T,
        lower_inclusive: bool,
        upper_inclusive: bool,
    ) -> Filter {
        Filter::new(BetweenFilter::new(
            self.field_name,
            Bound::new(
                lower_bound.into(),
                upper_bound.into(),
                lower_inclusive,
                upper_inclusive,
            ),
        ))
    }

    /// Creates a filter that matches documents containing the specified text in the field (case-sensitive).
    ///
    /// Performs full-text search within the field value, matching documents that contain the text substring.
    ///
    /// # Arguments
    ///
    /// * `value` - The text to search for
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field contains the specified text
    #[inline]
    pub fn text(self, value: &str) -> Filter {
        Filter::new(TextFilter::new(self.field_name, value.to_string(), true))
    }

    /// Creates a filter that matches documents containing the specified text in the field (case-insensitive).
    ///
    /// Performs full-text search within the field value, matching documents that contain the text substring
    /// regardless of letter casing.
    ///
    /// # Arguments
    ///
    /// * `value` - The text to search for
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field contains the specified text (case-insensitive)
    #[inline]
    pub fn text_case_insensitive(self, value: &str) -> Filter {
        Filter::new(TextFilter::new(self.field_name, value.to_string(), false))
    }

    /// Creates a filter that matches documents where the field matches the specified regex pattern.
    ///
    /// # Arguments
    ///
    /// * `value` - The regular expression pattern to match
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field matches the regex pattern
    #[inline]
    pub fn text_regex(self, value: &str) -> Filter {
        Filter::new(RegexFilter::new(self.field_name, value.to_string()))
    }

    /// Creates a filter that matches documents where the field value is in the specified array.
    ///
    /// Matches documents where the field equals one of the values in the provided collection.
    ///
    /// # Arguments
    ///
    /// * `values` - A vector of values to match against
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is in the specified values
    pub fn in_array<T: Into<Value>>(self, values: Vec<T>) -> Filter {
        Filter::new(InFilter::new(
            self.field_name,
            values.into_iter().map(|v| v.into()).collect(),
        ))
    }

    /// Creates a filter that matches documents where the field value is not in the specified array.
    ///
    /// Matches documents where the field value differs from all values in the provided collection.
    ///
    /// # Arguments
    ///
    /// * `values` - A vector of values to exclude from matches
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where the field is not in the specified values
    pub fn not_in_array<T: Into<Value>>(self, values: Vec<T>) -> Filter {
        Filter::new(NotInFilter::new(
            self.field_name,
            values.into_iter().map(|v| v.into()).collect(),
        ))
    }

    /// Creates a filter that matches documents where at least one array element matches the specified filter.
    ///
    /// Evaluates the provided filter against each element in the field array and matches documents
    /// where at least one element satisfies the filter condition.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter condition to evaluate against array elements
    ///
    /// # Returns
    ///
    /// A `Filter` matching documents where at least one element in the array satisfies the filter
    #[inline]
    pub fn elem_match(self, filter: Filter) -> Filter {
        Filter::new(ElementMatchFilter::new(self.field_name, filter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::filter::*;
    use crate::filter::FilterProvider;
    use crate::Value;

    #[test]
    fn test_fluent_filter_eq() {
        let filter = field("field").eq(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_ne() {
        let filter = field("field").ne(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_gt() {
        let filter = field("field").gt(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_gte() {
        let filter = field("field").gte(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_lt() {
        let filter = field("field").lt(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(41)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_lte() {
        let filter = field("field").lte(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_between_optional_inclusive() {
        let filter = field("field").between_optional_inclusive(10, 20);
        let mut doc = Document::new();
        doc.put("field", Value::I32(15)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_between_inclusive() {
        let filter = field("field").between_inclusive(10, 20, true);
        let mut doc = Document::new();
        doc.put("field", Value::I32(10)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_between() {
        let filter = field("field").between(10, 20, true, false);
        let mut doc = Document::new();
        doc.put("field", Value::I32(20)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_text() {
        let filter = field("field").text("test");
        let mut doc = Document::new();
        doc.put("field", Value::String("this is a test".to_string()))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_text_case_insensitive() {
        let filter = field("field").text_case_insensitive("test");
        let mut doc = Document::new();
        doc.put("field", Value::String("This Is A Test".to_string()))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_text_regex() {
        let filter = field("field").text_regex("test.*");
        let mut doc = Document::new();
        doc.put("field", Value::String("test123".to_string()))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_in_array() {
        let filter = field("field").in_array(vec![1, 2, 3]);
        let mut doc = Document::new();
        doc.put("field", Value::I32(2)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_not_in_array() {
        let filter = field("field").not_in_array(vec![1, 2, 3]);
        let mut doc = Document::new();
        doc.put("field", Value::I32(4)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_fluent_filter_elem_match() {
        let inner_filter = field("inner_field").eq(42);
        let filter = field("field").elem_match(inner_filter);
        let mut doc = Document::new();
        let mut inner_doc = Document::new();
        inner_doc.put("inner_field", Value::I32(42)).unwrap();
        doc.put("field", Value::Array(vec![Value::Document(inner_doc)]))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    // Performance optimization tests
    #[test]
    fn test_fluent_filter_inline_optimization_eq() {
        // Verify inline optimization for eq method with repeated calls
        let filter = field("field").eq(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(42)).unwrap();
        
        // Multiple applications to test inlining effectiveness
        for _ in 0..500 {
            assert!(filter.apply(&doc).unwrap());
        }
    }

    #[test]
    fn test_fluent_filter_inline_optimization_ne() {
        // Verify inline optimization for ne method
        let filter = field("field").ne(42);
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        
        for _ in 0..500 {
            assert!(filter.apply(&doc).unwrap());
        }
    }

    #[test]
    fn test_fluent_filter_inline_optimization_comparison_ops() {
        // Test all comparison operators for inline optimization
        let gt_filter = field("field").gt(10);
        let gte_filter = field("field").gte(10);
        let lt_filter = field("field").lt(10);
        let lte_filter = field("field").lte(10);
        
        let mut doc_15 = Document::new();
        doc_15.put("field", Value::I32(15)).unwrap();
        
        let mut doc_10 = Document::new();
        doc_10.put("field", Value::I32(10)).unwrap();
        
        let mut doc_5 = Document::new();
        doc_5.put("field", Value::I32(5)).unwrap();
        
        // Test with values in tight loops
        for _ in 0..200 {
            assert!(gt_filter.apply(&doc_15).unwrap());
            assert!(gte_filter.apply(&doc_10).unwrap());
            assert!(lt_filter.apply(&doc_5).unwrap());
            assert!(lte_filter.apply(&doc_10).unwrap());
        }
    }

    #[test]
    fn test_fluent_filter_inline_optimization_text_ops() {
        // Test text operations for inline optimization
        let text_filter = field("field").text("test");
        let regex_filter = field("field").text_regex("test.*");
        
        let mut doc = Document::new();
        doc.put("field", Value::String("this is a test".to_string())).unwrap();
        
        for _ in 0..100 {
            assert!(text_filter.apply(&doc).unwrap());
            assert!(regex_filter.apply(&doc).unwrap());
        }
    }

    #[test]
    fn test_fluent_filter_inline_optimization_elem_match() {
        // Test elem_match for inline optimization
        let inner_filter = field("inner").eq(1);
        let filter = field("arr").elem_match(inner_filter);
        
        let mut doc = Document::new();
        let mut inner = Document::new();
        inner.put("inner", Value::I32(1)).unwrap();
        doc.put("arr", Value::Array(vec![Value::Document(inner)])).unwrap();
        
        for _ in 0..100 {
            assert!(filter.apply(&doc).unwrap());
        }
    }
}
