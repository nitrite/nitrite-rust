//! Fluent API for FTS queries.
//!
//! This module provides a builder-pattern API for constructing FTS filters
//! in a readable and intuitive way.
//!
//! ## Example
//!
//! ```rust,ignore
//! use nitrite_tantivy_fts::fts_field;
//!
//! // Simple text search
//! let filter = fts_field("content").matches("search terms");
//!
//! // Phrase search
//! let filter = fts_field("content").phrase("exact phrase");
//! ```

use nitrite::filter::Filter;

use crate::filter::{PhraseFilter, TextSearchFilter};

/// Entry point for building FTS queries on a field.
///
/// ## Example
///
/// ```rust,ignore
/// use nitrite_tantivy_fts::fts_field;
///
/// let filter = fts_field("content").matches("hello world");
/// ```
pub fn fts_field(field: &str) -> FtsFluentFilter {
    FtsFluentFilter::new(field)
}

/// Fluent builder for FTS filters.
pub struct FtsFluentFilter {
    field: String,
}

impl FtsFluentFilter {
    /// Creates a new fluent filter for the given field.
    pub fn new(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
        }
    }

    /// Creates a text search filter that matches documents containing
    /// any of the specified search terms.
    ///
    /// The query is parsed by Tantivy's query parser, which supports:
    /// - Multiple terms (OR by default)
    /// - Required terms with `+` prefix
    /// - Excluded terms with `-` prefix
    /// - Boolean operators (AND, OR, NOT)
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use nitrite_tantivy_fts::fts_field;
    ///
    /// // Match documents containing "hello" OR "world"
    /// let filter = fts_field("content").matches("hello world");
    ///
    /// // Match documents containing "hello" AND "world"
    /// let filter = fts_field("content").matches("+hello +world");
    /// ```
    pub fn matches(self, query: impl Into<String>) -> Filter {
        Filter::new(TextSearchFilter::new(self.field, query))
    }

    /// Creates a phrase filter that matches documents containing
    /// the exact phrase specified.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use nitrite_tantivy_fts::fts_field;
    ///
    /// // Match documents containing the exact phrase "hello world"
    /// let filter = fts_field("content").phrase("hello world");
    /// ```
    pub fn phrase(self, phrase: impl Into<String>) -> Filter {
        Filter::new(PhraseFilter::new(self.field, phrase))
    }

    /// Alias for `matches` - creates a text search filter.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use nitrite_tantivy_fts::fts_field;
    ///
    /// let filter = fts_field("content").contains("search terms");
    /// ```
    pub fn contains(self, query: impl Into<String>) -> Filter {
        self.matches(query)
    }

    /// Alias for `matches` - creates a text search filter.
    ///
    /// ## Example
    ///
    /// ```rust,ignore
    /// use nitrite_tantivy_fts::fts_field;
    ///
    /// let filter = fts_field("content").text("search terms");
    /// ```
    pub fn text(self, query: impl Into<String>) -> Filter {
        self.matches(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter::{is_fts_filter, FtsFilter, FTS_INDEX};

    #[test]
    fn test_fts_field_creates_fluent_filter() {
        let fluent = fts_field("content");
        assert_eq!(fluent.field, "content");
    }

    #[test]
    fn test_fts_field_with_different_field_names() {
        let f1 = fts_field("title");
        let f2 = fts_field("body");
        let f3 = fts_field("description");

        assert_eq!(f1.field, "title");
        assert_eq!(f2.field, "body");
        assert_eq!(f3.field, "description");
    }

    #[test]
    fn test_fts_field_with_nested_field() {
        let fluent = fts_field("metadata.content");
        assert_eq!(fluent.field, "metadata.content");
    }

    // ===== FtsFluentFilter::new Tests =====

    #[test]
    fn test_fts_fluent_filter_new() {
        let fluent = FtsFluentFilter::new("content");
        assert_eq!(fluent.field, "content");
    }

    #[test]
    fn test_fts_fluent_filter_new_with_string() {
        let field_name = String::from("content");
        let fluent = FtsFluentFilter::new(field_name);
        assert_eq!(fluent.field, "content");
    }

    // ===== matches() Method Tests =====

    #[test]
    fn test_matches_creates_text_search_filter() {
        let filter = fts_field("content").matches("hello world");
        assert!(is_fts_filter(&filter));

        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>();
        assert!(text_filter.is_some());
        assert_eq!(text_filter.unwrap().query_string(), "hello world");
    }

    #[test]
    fn test_matches_single_term() {
        let filter = fts_field("content").matches("hello");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.query_string(), "hello");
    }

    #[test]
    fn test_matches_multiple_terms() {
        let filter = fts_field("content").matches("hello world test");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.query_string(), "hello world test");
    }

    #[test]
    fn test_matches_preserves_field_name() {
        let filter = fts_field("my_field").matches("test");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.field_name(), "my_field");
    }

    #[test]
    fn test_matches_returns_index_only_filter() {
        let filter = fts_field("content").matches("test");
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_matches_supported_index_type() {
        let filter = fts_field("content").matches("test");
        assert_eq!(filter.supported_index_type().unwrap(), FTS_INDEX);
    }

    // ===== phrase() Method Tests =====

    #[test]
    fn test_phrase_creates_phrase_filter() {
        let filter = fts_field("content").phrase("hello world");
        assert!(is_fts_filter(&filter));

        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>();
        assert!(phrase_filter.is_some());
        assert_eq!(phrase_filter.unwrap().query_string(), "\"hello world\"");
    }

    #[test]
    fn test_phrase_single_word() {
        let filter = fts_field("content").phrase("hello");
        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>().unwrap();
        assert_eq!(phrase_filter.query_string(), "\"hello\"");
    }

    #[test]
    fn test_phrase_preserves_field_name() {
        let filter = fts_field("title").phrase("exact phrase");
        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>().unwrap();
        assert_eq!(phrase_filter.field_name(), "title");
    }

    #[test]
    fn test_phrase_returns_index_only_filter() {
        let filter = fts_field("content").phrase("test");
        assert!(filter.is_index_only_filter());
    }

    // ===== contains() Alias Tests =====

    #[test]
    fn test_contains_alias() {
        let filter = fts_field("content").contains("test");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>();
        assert!(text_filter.is_some());
    }

    #[test]
    fn test_contains_equivalent_to_matches() {
        let filter1 = fts_field("content").matches("hello");
        let filter2 = fts_field("content").contains("hello");

        let tf1 = filter1.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        let tf2 = filter2.as_any().downcast_ref::<TextSearchFilter>().unwrap();

        assert_eq!(tf1.query_string(), tf2.query_string());
    }

    // ===== text() Alias Tests =====

    #[test]
    fn test_text_alias() {
        let filter = fts_field("content").text("test");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>();
        assert!(text_filter.is_some());
    }

    #[test]
    fn test_text_equivalent_to_matches() {
        let filter1 = fts_field("content").matches("world");
        let filter2 = fts_field("content").text("world");

        let tf1 = filter1.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        let tf2 = filter2.as_any().downcast_ref::<TextSearchFilter>().unwrap();

        assert_eq!(tf1.query_string(), tf2.query_string());
    }

    // ===== Edge Cases =====

    #[test]
    fn test_empty_query() {
        let filter = fts_field("content").matches("");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.query_string(), "");
    }

    #[test]
    fn test_empty_phrase() {
        let filter = fts_field("content").phrase("");
        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>().unwrap();
        assert_eq!(phrase_filter.query_string(), "\"\"");
    }

    #[test]
    fn test_query_with_special_characters() {
        let filter = fts_field("content").matches("hello+world -excluded");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.query_string(), "hello+world -excluded");
    }

    #[test]
    fn test_phrase_with_quotes() {
        let filter = fts_field("content").phrase("say \"hello\" world");
        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>().unwrap();
        assert!(phrase_filter.query_string().contains("hello"));
    }

    #[test]
    fn test_unicode_query() {
        let filter = fts_field("content").matches("日本語テスト");
        let text_filter = filter.as_any().downcast_ref::<TextSearchFilter>().unwrap();
        assert_eq!(text_filter.query_string(), "日本語テスト");
    }

    #[test]
    fn test_unicode_phrase() {
        let filter = fts_field("content").phrase("こんにちは世界");
        let phrase_filter = filter.as_any().downcast_ref::<PhraseFilter>().unwrap();
        assert!(phrase_filter.query_string().contains("こんにちは世界"));
    }
}
