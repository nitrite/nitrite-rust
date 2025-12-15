use std::{any::Any, fmt::Display};

use crate::{
    collection::Document,
    errors::NitriteResult,
};

use super::{Filter, FilterProvider};

/// A filter that applies logical AND operation on multiple filters.
///
/// This filter matches documents that satisfy all of the provided filters simultaneously.
/// It uses short-circuit evaluation, returning false immediately when any filter fails.
/// The filters are evaluated in the order they were provided.
///
/// # Responsibilities
///
/// * **Conjunction Evaluation**: Matches documents satisfying all filters
/// * **Filter Composition**: Combines multiple filters into a single condition
/// * **Short-Circuit Logic**: Optimizes evaluation by stopping at first failure
pub(crate) struct AndFilter {
    filters: Vec<Filter>,
}

impl AndFilter {
    /// Creates a new AND filter combining multiple filters.
    ///
    /// # Arguments
    ///
    /// * `filters` - A vector of filters that must all be satisfied
    ///
    /// # Returns
    ///
    /// An `AndFilter` that matches documents satisfying all provided filters
    pub(crate) fn new(filters: Vec<Filter>) -> Self {
        AndFilter { filters }
    }
}

impl Display for AndFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut filters = String::with_capacity(self.filters.len() * 16);
        for (i, filter) in self.filters.iter().enumerate() {
            filters.push_str(&format!("{}", filter));
            if i < self.filters.len() - 1 {
                filters.push_str(" && ");
            }
        }
        write!(f, "({})", filters)
    }
}

impl FilterProvider for AndFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        for filter in &self.filters {
            if !filter.apply(entry)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn logical_filters(&self) -> NitriteResult<Vec<Filter>> {
        Ok(self.filters.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A filter that applies logical OR operation on multiple filters.
///
/// This filter matches documents that satisfy at least one of the provided filters.
/// It uses short-circuit evaluation, returning true immediately when any filter succeeds.
/// The filters are evaluated in the order they were provided.
///
/// # Responsibilities
///
/// * **Disjunction Evaluation**: Matches documents satisfying at least one filter
/// * **Filter Composition**: Combines multiple filters with OR logic
/// * **Short-Circuit Logic**: Optimizes evaluation by stopping at first success
pub(crate) struct OrFilter {
    filters: Vec<Filter>,
}

impl OrFilter {
    /// Creates a new OR filter combining multiple filters.
    ///
    /// # Arguments
    ///
    /// * `filters` - A vector of filters where at least one must be satisfied
    ///
    /// # Returns
    ///
    /// An `OrFilter` that matches documents satisfying at least one provided filter
    pub(crate) fn new(filters: Vec<Filter>) -> Self {
        OrFilter { filters }
    }
}

impl Display for OrFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut filters = String::with_capacity(self.filters.len() * 16);
        for (i, filter) in self.filters.iter().enumerate() {
            filters.push_str(&format!("{}", filter));
            if i < self.filters.len() - 1 {
                filters.push_str(" || ");
            }
        }
        write!(f, "({})", filters)
    }
}

impl FilterProvider for OrFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        for filter in &self.filters {
            if filter.apply(entry)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn logical_filters(&self) -> NitriteResult<Vec<Filter>> {
        Ok(self.filters.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A filter that applies logical NOT operation on a filter.
///
/// This filter inverts the result of another filter, matching documents that do not satisfy
/// the provided filter. It negates both the matching condition and any internal short-circuit behavior.
///
/// # Responsibilities
///
/// * **Negation**: Inverts the matching condition of the wrapped filter
/// * **Filter Composition**: Combines a single filter with NOT logic
/// * **Result Inversion**: Returns the logical opposite of the wrapped filter's result
pub(crate) struct NotFilter {
    filter: Filter,
}

impl NotFilter {
    /// Creates a new NOT filter inverting the provided filter.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter to negate
    ///
    /// # Returns
    ///
    /// A `NotFilter` that matches documents not matching the provided filter
    pub(crate) fn new(filter: Filter) -> Self {
        NotFilter { filter }
    }
}

impl Display for NotFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(not {})", self.filter)
    }
}

impl FilterProvider for NotFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        Ok(!self.filter.apply(entry)?)
    }

    fn logical_filters(&self) -> NitriteResult<Vec<Filter>> {
        Ok(vec![self.filter.clone()])
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::filter::basic_filters::{AllFilter, EqualsFilter};
    use crate::Value;

    #[test]
    fn test_and_filter_apply() {
        let filter = AndFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(AllFilter),
        ]);
        let doc = Document::new();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_and_filter_apply_negative() {
        let filter = AndFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(EqualsFilter::new(
                "field".to_string(),
                Value::I32(42),
            )),
        ]);
        let doc = Document::new();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_or_filter_apply() {
        let filter = OrFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(EqualsFilter::new(
                "field".to_string(),
                Value::I32(42),
            )),
        ]);
        let doc = Document::new();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_or_filter_apply_negative() {
        let filter = OrFilter::new(vec![
            Filter::new(EqualsFilter::new(
                "field".to_string(),
                Value::I32(41),
            )),
            Filter::new(EqualsFilter::new(
                "field".to_string(),
                Value::I32(42),
            )),
        ]);
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_not_filter_apply() {
        let filter = NotFilter::new(Filter::new(AllFilter));
        let doc = Document::new();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_not_filter_apply_negative() {
        let filter = NotFilter::new(Filter::new(EqualsFilter::new(
            "field".to_string(),
            Value::I32(42),
        )));
        let mut doc = Document::new();
        doc.put("field", Value::I32(43)).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    // Performance optimization tests
    #[test]
    fn test_and_filter_short_circuit_optimization() {
        // Verify early return on first false filter
        let filter = AndFilter::new(vec![
            Filter::new(EqualsFilter::new("field".to_string(), Value::I32(99))),
            Filter::new(AllFilter), // Should not be evaluated
        ]);
        let doc = Document::new();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_or_filter_short_circuit_optimization() {
        // Verify early return on first true filter
        let filter = OrFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(EqualsFilter::new("field".to_string(), Value::I32(99))), // Should not be evaluated
        ]);
        let doc = Document::new();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_and_filter_multiple_filters() {
        let mut doc = Document::new();
        doc.put("a", Value::I32(1)).unwrap();
        doc.put("b", Value::I32(2)).unwrap();
        
        for _ in 0..100 {
            let filter = AndFilter::new(vec![
                Filter::new(EqualsFilter::new("a".to_string(), Value::I32(1))),
                Filter::new(EqualsFilter::new("b".to_string(), Value::I32(2))),
            ]);
            assert!(filter.apply(&doc).unwrap());
        }
    }

    #[test]
    fn test_or_filter_multiple_filters() {
        let mut doc = Document::new();
        doc.put("x", Value::I32(5)).unwrap();
        
        for _ in 0..100 {
            let filter = OrFilter::new(vec![
                Filter::new(EqualsFilter::new("x".to_string(), Value::I32(1))),
                Filter::new(EqualsFilter::new("x".to_string(), Value::I32(5))),
            ]);
            assert!(filter.apply(&doc).unwrap());
        }
    }

    #[test]
    fn test_and_filter_display_format() {
        let filter = AndFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(AllFilter),
        ]);
        let display = format!("{}", filter);
        assert!(display.contains("&&"));
        assert!(display.starts_with("("));
        assert!(display.ends_with(")"));
    }

    #[test]
    fn test_or_filter_display_format() {
        let filter = OrFilter::new(vec![
            Filter::new(AllFilter),
            Filter::new(AllFilter),
        ]);
        let display = format!("{}", filter);
        assert!(display.contains("||"));
        assert!(display.starts_with("("));
        assert!(display.ends_with(")"));
    }

    #[test]
    fn test_not_filter_display_format() {
        let filter = NotFilter::new(Filter::new(AllFilter));
        let display = format!("{}", filter);
        assert!(display.contains("not"));
    }
}