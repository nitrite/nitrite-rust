//! FTS filters for querying text in Nitrite collections.
//!
//! This module provides filter types for full-text search queries:
//! - `TextSearchFilter` - finds documents matching a text query
//! - `PhraseFilter` - finds documents containing an exact phrase

use std::any::Any;
use std::fmt::{self, Display};
use std::sync::Arc;

use nitrite::collection::Document;
use nitrite::common::Value;
use nitrite::errors::NitriteResult;
use nitrite::filter::{Filter, FilterProvider};

use parking_lot::RwLock;

/// The index type name for FTS indexes.
pub const FTS_INDEX: &str = "tantivy-fts";

/// Base trait for FTS filters.
pub trait FtsFilter: Send + Sync {
    /// Returns the query string for this filter.
    fn query_string(&self) -> String;

    /// Returns the field name this filter applies to.
    fn field_name(&self) -> String;
}

/// Filter that finds documents matching a text query.
///
/// Uses Tantivy's query parser to match documents containing
/// the specified search terms.
#[derive(Clone)]
pub struct TextSearchFilter {
    inner: Arc<TextSearchFilterInner>,
}

struct TextSearchFilterInner {
    field: RwLock<String>,
    query: String,
}

impl TextSearchFilter {
    /// Creates a new text search filter.
    pub fn new(field: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(TextSearchFilterInner {
                field: RwLock::new(field.into()),
                query: query.into(),
            }),
        }
    }

    /// Returns the search query.
    pub fn query(&self) -> &str {
        &self.inner.query
    }
}

impl FtsFilter for TextSearchFilter {
    fn query_string(&self) -> String {
        self.inner.query.clone()
    }

    fn field_name(&self) -> String {
        self.inner.field.read().clone()
    }
}

impl FilterProvider for TextSearchFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.read();
        let value = entry.get(&field)?;

        match value {
            Value::String(s) => {
                // Simple contains check for document-level filtering
                // The actual FTS search is done via the index
                let query_lower = self.inner.query.to_lowercase();
                let text_lower = s.to_lowercase();
                
                // Check if any query term is in the text
                for term in query_lower.split_whitespace() {
                    if text_lower.contains(term) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Value::Null => Ok(false),
            _ => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        Ok(self.inner.field.read().clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        *self.inner.field.write() = field_name;
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(FTS_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        // Can be grouped with other FTS filters on the same field
        if let Some(other_fts) = other.as_any().downcast_ref::<TextSearchFilter>() {
            let self_field = self.inner.field.read();
            let other_field = other_fts.inner.field.read();
            return Ok(*self_field == *other_field);
        }
        if let Some(other_phrase) = other.as_any().downcast_ref::<PhraseFilter>() {
            let self_field = self.inner.field.read();
            let other_field = other_phrase.inner.field.read();
            return Ok(*self_field == *other_field);
        }
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for TextSearchFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.read();
        write!(f, "TextSearchFilter({}: '{}')", field, self.inner.query)
    }
}

/// Filter that finds documents containing an exact phrase.
#[derive(Clone)]
pub struct PhraseFilter {
    inner: Arc<PhraseFilterInner>,
}

struct PhraseFilterInner {
    field: RwLock<String>,
    phrase: String,
}

impl PhraseFilter {
    /// Creates a new phrase filter.
    pub fn new(field: impl Into<String>, phrase: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(PhraseFilterInner {
                field: RwLock::new(field.into()),
                phrase: phrase.into(),
            }),
        }
    }

    /// Returns the phrase to match.
    pub fn phrase(&self) -> &str {
        &self.inner.phrase
    }
}

impl FtsFilter for PhraseFilter {
    fn query_string(&self) -> String {
        // Wrap phrase in quotes for Tantivy phrase query
        format!("\"{}\"", self.inner.phrase)
    }

    fn field_name(&self) -> String {
        self.inner.field.read().clone()
    }
}

impl FilterProvider for PhraseFilter {
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field = self.inner.field.read();
        let value = entry.get(&field)?;

        match value {
            Value::String(s) => {
                let text_lower = s.to_lowercase();
                let phrase_lower = self.inner.phrase.to_lowercase();
                Ok(text_lower.contains(&phrase_lower))
            }
            Value::Null => Ok(false),
            _ => Ok(false),
        }
    }

    fn has_field(&self) -> bool {
        true
    }

    fn get_field_name(&self) -> NitriteResult<String> {
        Ok(self.inner.field.read().clone())
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        *self.inner.field.write() = field_name;
        Ok(())
    }

    fn is_index_only_filter(&self) -> bool {
        true
    }

    fn supported_index_type(&self) -> NitriteResult<String> {
        Ok(FTS_INDEX.to_string())
    }

    fn can_be_grouped(&self, other: Filter) -> NitriteResult<bool> {
        if let Some(other_fts) = other.as_any().downcast_ref::<TextSearchFilter>() {
            let self_field = self.inner.field.read();
            let other_field = other_fts.inner.field.read();
            return Ok(*self_field == *other_field);
        }
        if let Some(other_phrase) = other.as_any().downcast_ref::<PhraseFilter>() {
            let self_field = self.inner.field.read();
            let other_field = other_phrase.inner.field.read();
            return Ok(*self_field == *other_field);
        }
        Ok(false)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for PhraseFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let field = self.inner.field.read();
        write!(f, "PhraseFilter({}: \"{}\")", field, self.inner.phrase)
    }
}

/// Checks if a filter is an FTS filter.
pub fn is_fts_filter(filter: &Filter) -> bool {
    filter.as_any().is::<TextSearchFilter>() || filter.as_any().is::<PhraseFilter>()
}

/// Attempts to cast a filter to an FtsFilter trait object.
pub fn as_fts_filter(filter: &Filter) -> Option<&dyn FtsFilter> {
    if let Some(text_filter) = filter.as_any().downcast_ref::<TextSearchFilter>() {
        return Some(text_filter);
    }
    if let Some(phrase_filter) = filter.as_any().downcast_ref::<PhraseFilter>() {
        return Some(phrase_filter);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use nitrite::doc;

    #[test]
    fn test_text_search_filter_index_type() {
        let filter = TextSearchFilter::new("content", "hello world");
        assert_eq!(filter.supported_index_type().unwrap(), FTS_INDEX);
    }

    #[test]
    fn test_text_search_filter_is_index_only() {
        let filter = TextSearchFilter::new("content", "test");
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_text_search_filter_has_field() {
        let filter = TextSearchFilter::new("content", "test");
        assert!(filter.has_field());
    }

    #[test]
    fn test_text_search_filter_query() {
        let filter = TextSearchFilter::new("content", "hello world");
        assert_eq!(filter.query(), "hello world");
        assert_eq!(filter.query_string(), "hello world");
    }

    #[test]
    fn test_text_search_filter_field_name() {
        let filter = TextSearchFilter::new("content", "test");
        assert_eq!(filter.field_name(), "content");
        assert_eq!(filter.get_field_name().unwrap(), "content");
    }

    #[test]
    fn test_text_search_filter_set_field_name() {
        let filter = TextSearchFilter::new("content", "test");
        filter.set_field_name("new_field".to_string()).unwrap();
        assert_eq!(filter.get_field_name().unwrap(), "new_field");
    }

    #[test]
    fn test_text_search_filter_apply_match() {
        let filter = TextSearchFilter::new("content", "hello");
        let doc = doc! { content: "hello world" };
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_search_filter_apply_no_match() {
        let filter = TextSearchFilter::new("content", "goodbye");
        let doc = doc! { content: "hello world" };
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_search_filter_apply_case_insensitive() {
        let filter = TextSearchFilter::new("content", "HELLO");
        let doc = doc! { content: "hello world" };
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_search_filter_apply_multiple_terms() {
        let filter = TextSearchFilter::new("content", "foo bar");
        let doc = doc! { content: "this has bar in it" };
        assert!(filter.apply(&doc).unwrap()); // Matches "bar"
    }

    #[test]
    fn test_text_search_filter_apply_null_value() {
        let filter = TextSearchFilter::new("content", "test");
        let doc = doc! { other: "value" };
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_search_filter_clone() {
        let filter = TextSearchFilter::new("content", "test");
        let cloned = filter.clone();
        assert_eq!(cloned.query(), filter.query());
        assert_eq!(cloned.field_name(), filter.field_name());
    }

    #[test]
    fn test_text_search_filter_display() {
        let filter = TextSearchFilter::new("content", "hello");
        let display = format!("{}", filter);
        assert!(display.contains("TextSearchFilter"));
        assert!(display.contains("content"));
        assert!(display.contains("hello"));
    }

    #[test]
    fn test_text_search_filter_empty_query() {
        let filter = TextSearchFilter::new("content", "");
        let doc = doc! { content: "hello world" };
        assert!(!filter.apply(&doc).unwrap()); // Empty query matches nothing
    }

    #[test]
    fn test_phrase_filter_query_string() {
        let filter = PhraseFilter::new("content", "hello world");
        assert_eq!(filter.query_string(), "\"hello world\"");
    }

    #[test]
    fn test_phrase_filter_phrase() {
        let filter = PhraseFilter::new("content", "hello world");
        assert_eq!(filter.phrase(), "hello world");
    }

    #[test]
    fn test_phrase_filter_is_index_only() {
        let filter = PhraseFilter::new("content", "test phrase");
        assert!(filter.is_index_only_filter());
    }

    #[test]
    fn test_phrase_filter_has_field() {
        let filter = PhraseFilter::new("content", "test");
        assert!(filter.has_field());
    }

    #[test]
    fn test_phrase_filter_field_name() {
        let filter = PhraseFilter::new("title", "test");
        assert_eq!(filter.field_name(), "title");
        assert_eq!(filter.get_field_name().unwrap(), "title");
    }

    #[test]
    fn test_phrase_filter_set_field_name() {
        let filter = PhraseFilter::new("content", "test");
        filter.set_field_name("body".to_string()).unwrap();
        assert_eq!(filter.get_field_name().unwrap(), "body");
    }

    #[test]
    fn test_phrase_filter_apply_match() {
        let filter = PhraseFilter::new("content", "hello world");
        let doc = doc! { content: "say hello world today" };
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_phrase_filter_apply_no_match() {
        let filter = PhraseFilter::new("content", "hello world");
        let doc = doc! { content: "hello there world" };
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_phrase_filter_apply_case_insensitive() {
        let filter = PhraseFilter::new("content", "HELLO WORLD");
        let doc = doc! { content: "say hello world today" };
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_phrase_filter_apply_null_value() {
        let filter = PhraseFilter::new("content", "test");
        let doc = doc! { other: "value" };
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_phrase_filter_clone() {
        let filter = PhraseFilter::new("content", "test phrase");
        let cloned = filter.clone();
        assert_eq!(cloned.phrase(), filter.phrase());
        assert_eq!(cloned.field_name(), filter.field_name());
    }

    #[test]
    fn test_phrase_filter_display() {
        let filter = PhraseFilter::new("title", "exact match");
        let display = format!("{}", filter);
        assert!(display.contains("PhraseFilter"));
        assert!(display.contains("title"));
        assert!(display.contains("exact match"));
    }

    #[test]
    fn test_phrase_filter_supported_index_type() {
        let filter = PhraseFilter::new("content", "test");
        assert_eq!(filter.supported_index_type().unwrap(), FTS_INDEX);
    }

    #[test]
    fn test_is_fts_filter_text_search() {
        let filter = Filter::new(TextSearchFilter::new("content", "test"));
        assert!(is_fts_filter(&filter));
    }

    #[test]
    fn test_is_fts_filter_phrase() {
        let filter = Filter::new(PhraseFilter::new("content", "test"));
        assert!(is_fts_filter(&filter));
    }

    #[test]
    fn test_as_fts_filter_text_search() {
        let filter = Filter::new(TextSearchFilter::new("content", "test"));
        let fts_filter = as_fts_filter(&filter);
        assert!(fts_filter.is_some());
        assert_eq!(fts_filter.unwrap().query_string(), "test");
    }

    #[test]
    fn test_as_fts_filter_phrase() {
        let filter = Filter::new(PhraseFilter::new("content", "test phrase"));
        let fts_filter = as_fts_filter(&filter);
        assert!(fts_filter.is_some());
        assert_eq!(fts_filter.unwrap().query_string(), "\"test phrase\"");
    }

    #[test]
    fn test_text_search_can_be_grouped_same_field() {
        let filter1 = TextSearchFilter::new("content", "test1");
        let filter2 = Filter::new(TextSearchFilter::new("content", "test2"));
        assert!(filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_text_search_cannot_be_grouped_different_field() {
        let filter1 = TextSearchFilter::new("content", "test1");
        let filter2 = Filter::new(TextSearchFilter::new("title", "test2"));
        assert!(!filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_text_search_can_be_grouped_with_phrase() {
        let filter1 = TextSearchFilter::new("content", "test");
        let filter2 = Filter::new(PhraseFilter::new("content", "phrase"));
        assert!(filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_phrase_can_be_grouped_same_field() {
        let filter1 = PhraseFilter::new("content", "phrase1");
        let filter2 = Filter::new(PhraseFilter::new("content", "phrase2"));
        assert!(filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_phrase_cannot_be_grouped_different_field() {
        let filter1 = PhraseFilter::new("content", "phrase1");
        let filter2 = Filter::new(PhraseFilter::new("body", "phrase2"));
        assert!(!filter1.can_be_grouped(filter2).unwrap());
    }

    #[test]
    fn test_text_search_filter_special_chars_in_query() {
        let filter = TextSearchFilter::new("content", "hello+world");
        assert_eq!(filter.query(), "hello+world");
    }

    #[test]
    fn test_phrase_filter_special_chars_in_phrase() {
        let filter = PhraseFilter::new("content", "hello \"quoted\" world");
        assert_eq!(filter.phrase(), "hello \"quoted\" world");
    }

    #[test]
    fn test_fts_index_constant() {
        assert_eq!(FTS_INDEX, "tantivy-fts");
    }

    #[test]
    fn test_text_search_filter_unicode_query() {
        let filter = TextSearchFilter::new("content", "日本語");
        let doc = doc! { content: "これは日本語のテストです" };
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_phrase_filter_unicode_phrase() {
        let filter = PhraseFilter::new("content", "こんにちは世界");
        let doc = doc! { content: "挨拶：こんにちは世界！" };
        assert!(filter.apply(&doc).unwrap());
    }
}
