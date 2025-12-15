use regex::Regex;
use std::{any::Any, collections::HashMap, fmt::Display, sync::OnceLock};

use crate::{
    collection::Document,
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::{
        text::{Tokenizer, TokenizerProvider},
        IndexMap,
    },
    DefaultFilter, StringTokenizer, Value,
};

use super::{is_element_match_filter, is_text_filter, Filter, FilterProvider};

/// A filter that matches documents using regular expressions.
///
/// This filter evaluates field values against a compiled regex pattern. The regex pattern
/// is compiled during initialization and cached for efficient evaluation. Invalid regex patterns
/// are logged but do not cause panics, allowing graceful error handling during filter application.
///
/// # Responsibilities
///
/// * **Pattern Compilation**: Compiles and validates regex patterns at initialization
/// * **Regex Matching**: Evaluates field values against the compiled pattern
/// * **String Extraction**: Converts field values to strings for pattern matching
/// * **Error Handling**: Safely handles invalid patterns and non-string field values
pub(crate) struct RegexFilter {
    field_name: OnceLock<String>,
    field_value: OnceLock<String>,
    pattern: OnceLock<Regex>,
    collection_name: OnceLock<String>,
}

impl RegexFilter {
    /// Creates a new regex filter with the specified field name and pattern.
    ///
    /// The regex pattern is compiled immediately. If the pattern is invalid, it is logged
    /// and stored as uninitialized, causing apply() to return an error when invoked.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to match against
    /// * `field_value` - The regular expression pattern to compile and use for matching
    ///
    /// # Returns
    ///
    /// A new `RegexFilter` instance with the specified field and pattern
    #[inline]
    pub(crate) fn new(field_name: String, field_value: String) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let value = OnceLock::new();
        let _ = value.set(field_value.clone());

        let pattern = OnceLock::new();
        // Try to compile the regex, but don't panic - store None if compilation fails
        match Regex::new(&field_value) {
            Ok(regex) => {
                let _ = pattern.set(regex);
            }
            Err(e) => {
                log::error!("Invalid regex pattern '{}': {}", field_value, e);
                // Don't initialize pattern - get() will return None for invalid regex
            }
        }

        RegexFilter {
            field_name: name,
            field_value: value,
            pattern,
            collection_name: OnceLock::new(),
        }
    }
}

impl Display for RegexFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.field_name.get(), self.field_value.get()) {
            (Some(name), Some(value)) => write!(f, "({} =~ {})", name, value),
            (Some(name), None) => write!(f, "({} =~ unknown)", name),
            (None, Some(value)) => write!(f, "(unknown =~ {})", value),
            (None, None) => write!(f, "(unknown =~ unknown)"),
        }
    }
}

impl FilterProvider for RegexFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field_name = self.field_name.get()
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))?;
        let value = entry.get(field_name)?;
        let value = value.as_string();
        if value.is_none() {
            return Ok(false);
        }

        match self.pattern.get() {
            Some(p) => {
                let val = value.as_ref().ok_or_else(|| NitriteError::new(
                    "Field value is null or not a string",
                    ErrorKind::InvalidOperation,
                ))?;
                Ok(p.is_match(val))
            }
            None => {
                log::error!("Invalid regex pattern for filter {}", self);
                Err(NitriteError::new(
                    "Invalid regex pattern",
                    ErrorKind::InvalidOperation,
                ))
            },
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
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        Ok(self.field_value.get()
            .map(|v| Value::String(v.clone())))
    }

    fn set_field_value(&self, field_value: Value) -> NitriteResult<()> {
        if let Value::String(string_value) = field_value {
            self.field_value.get_or_init(|| string_value.clone());
            // Try to compile the regex - if it fails, pattern remains uninitialized
            match Regex::new(&string_value) {
                Ok(regex) => {
                    self.pattern.get_or_init(|| regex);
                }
                Err(e) => {
                    log::error!("Invalid regex pattern '{}': {}", string_value, e);
                    return Err(NitriteError::new(
                        &format!("Invalid regex pattern: {}", e),
                        ErrorKind::InvalidOperation,
                    ));
                }
            }
            Ok(())
        } else {
            log::error!("Field value is not a string for filter {}", self);
            Err(NitriteError::new(
                "Field value is not a string",
                ErrorKind::InvalidOperation,
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}


#[derive(Clone)]
pub(crate) struct TextFilter {
    field_name: OnceLock<String>,
    field_value: OnceLock<String>,
    collection_name: OnceLock<String>,
    case_sensitive: OnceLock<bool>,
    tokenizer: OnceLock<Tokenizer>,
}

impl TextFilter {
    /// Creates a new text filter for the specified field with case sensitivity option.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the field to search in
    /// * `field_value` - The text to search for
    /// * `case_sensitive` - If true, search is case-sensitive; if false, case-insensitive
    ///
    /// # Returns
    ///
    /// A new `TextFilter` instance configured for text search
    #[inline]
    pub(crate) fn new(field_name: String, field_value: String, case_sensitive: bool) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        let value = OnceLock::new();
        let _ = value.set(field_value.clone());

        let case = OnceLock::new();
        let _ = case.set(case_sensitive);

        TextFilter {
            field_name: name,
            field_value: value,
            collection_name: OnceLock::new(),
            case_sensitive: case,
            tokenizer: OnceLock::new(),
        }
    }

    /// Sets the tokenizer for this text filter.
    ///
    /// The tokenizer is used for word-based text search and index optimization.
    /// It should be set before using index-accelerated search features.
    ///
    /// # Arguments
    ///
    /// * `tokenizer` - The tokenizer to use for text processing
    pub(crate) fn set_tokenizer(&self, tokenizer: Tokenizer) {
        self.tokenizer.get_or_init(|| tokenizer);
    }

    fn search_exact_by_index(
        &self,
        index_map: &IndexMap,
        search_string: String,
    ) -> NitriteResult<Vec<Value>> {
        let mut score_map = HashMap::new();
        
        // Get tokenizer with proper error handling
        let tokenizer = self.tokenizer.get()
            .ok_or_else(|| {
                log::error!("Tokenizer not initialized for text filter");
                NitriteError::new("Tokenizer not initialized", ErrorKind::InvalidOperation)
            })?;
        
        let words = tokenizer.tokenize(&search_string);

        for word in words {
            let value = index_map.get(&Value::String(word.clone()))?;
            
            if let Some(val) = value {
                if let Some(nitrite_ids) = val.as_array() {
                    for nitrite_id in nitrite_ids {
                        let count = score_map.entry(nitrite_id.clone()).or_insert(0);
                        *count += 1;
                    }
                }
            }

            // Handle case-insensitive search with proper error handling
            let case_sensitive = self.case_sensitive.get()
                .ok_or_else(|| {
                    log::error!("Case sensitive flag not initialized for text filter");
                    NitriteError::new("Case sensitive flag not initialized", ErrorKind::InvalidOperation)
                })?;
            
            if !*case_sensitive {
                // case-insensitive search
                let search_string = format!("i_{}", word.to_lowercase());
                let value = index_map.get(&Value::String(search_string))?;
                if let Some(val) = value {
                    if let Some(nitrite_ids) = val.as_array() {
                        for nitrite_id in nitrite_ids {
                            let count = score_map.entry(nitrite_id.clone()).or_insert(0);
                            *count += 1;
                        }
                    }
                }
            }
        }

        self.sorted_ids_by_score(score_map)
    }

    fn sorted_ids_by_score(&self, score_map: HashMap<Value, i32>) -> NitriteResult<Vec<Value>> {
        let mut sorted_map: Vec<_> = score_map.into_iter().collect();
        sorted_map.sort_by(|a, b| b.1.cmp(&a.1));

        let mut sorted_ids = Vec::new();
        for (key, _) in sorted_map {
            sorted_ids.push(key);
        }

        Ok(sorted_ids)
    }

    fn search_by_wildcard(
        &self,
        index_map: &IndexMap,
        search_string: String,
    ) -> NitriteResult<Vec<Value>> {
        if search_string.eq("*") {
            let field_name = self.field_name.get().map(|s| s.as_str()).unwrap_or("unknown");
            log::error!("'*' alone is not a valid search string for wildcard filter on field '{}'", field_name);
            return Err(NitriteError::new(
                &format!("Invalid wildcard search pattern '*' on field '{}'. Use '*text' (ends with), 'text*' (starts with), or '*text*' (contains)", field_name),
                ErrorKind::FilterError,
            ));
        }

        let string_tokenizer = StringTokenizer::new(DefaultFilter, &search_string);
        let tokens = string_tokenizer.collect::<Vec<_>>();
        if tokens.len() > 1 {
            let field_name = self.field_name.get().map(|s| s.as_str()).unwrap_or("unknown");
            log::error!("Wildcard search with multiple words '{}' cannot be applied on field '{}' - use phrase search instead", search_string, field_name);
            return Err(NitriteError::new(
                &format!("Wildcard search on field '{}' failed: '{}' contains multiple words. Use phrase search or split into multiple filters", field_name, search_string),
                ErrorKind::FilterError,
            ));
        }

        if search_string.starts_with("*") && !search_string.ends_with("*") {
            self.search_end_with(index_map, search_string)
        } else if !search_string.starts_with("*") && search_string.ends_with("*") {
            self.search_start_with(index_map, search_string)
        } else if search_string.starts_with("*") && search_string.ends_with("*") {
            self.search_contains(index_map, search_string)
        } else {
            self.search_exact_by_index(index_map, search_string)
        }
    }

    fn search_start_with(
        &self,
        index_map: &IndexMap,
        search_string: String,
    ) -> NitriteResult<Vec<Value>> {
        let mut nitrite_ids = Vec::new();
        let search_term = search_string.trim_end_matches('*');

        let entries = index_map.entries()?;
        for result in entries {
            let (key, value) = result?;
            if let Value::String(key_str) = &key {
                if key_str.starts_with(search_term) {
                    if let Value::Array(array) = value {
                        nitrite_ids.extend(array);
                    }
                }
            }
        }

        Ok(nitrite_ids)
    }

    fn search_end_with(
        &self,
        index_map: &IndexMap,
        search_string: String,
    ) -> NitriteResult<Vec<Value>> {
        let mut nitrite_ids = Vec::new();
        let search_term = search_string.trim_start_matches('*');

        let entries = index_map.entries()?;
        for result in entries {
            let (key, value) = result?;
            if let Value::String(key_str) = &key {
                if key_str.ends_with(search_term) {
                    if let Value::Array(array) = value {
                        nitrite_ids.extend(array);
                    }
                }
            }
        }

        Ok(nitrite_ids)
    }

    fn search_contains(
        &self,
        index_map: &IndexMap,
        search_string: String,
    ) -> NitriteResult<Vec<Value>> {
        let mut nitrite_ids = Vec::new();
        let search_term = search_string.trim_start_matches('*').trim_end_matches('*');

        let entries = index_map.entries()?;
        for result in entries {
            let (key, value) = result?;
            if let Value::String(key_str) = &key {
                if key_str.contains(search_term) {
                    if let Value::Array(array) = value {
                        nitrite_ids.extend(array);
                    }
                }
            }
        }

        Ok(nitrite_ids)
    }
}

impl Display for TextFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let case_sensitive = self.case_sensitive.get().unwrap_or(&false);
        let field_name = self.field_name.get().map(|s| s.as_str()).unwrap_or("unknown");
        let field_value = self.field_value.get().map(|s| s.as_str()).unwrap_or("unknown");

        if *case_sensitive {
            write!(
                f,
                "({} text {} case_sensitive)",
                field_name, field_value
            )
        } else {
            write!(
                f,
                "({} text_case_insensitive {})",
                field_name, field_value
            )
        }
    }
}

impl FilterProvider for TextFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        let field_name = self.field_name.get()
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))?;
        let value = entry.get(field_name)?;
        let value = value.as_string();
        if value.is_none() {
            return Ok(false);
        }

        let field_value = self.field_value.get()
            .ok_or_else(|| NitriteError::new("Field value not initialized", ErrorKind::InvalidOperation))?;
        let case_sensitive = *self.case_sensitive.get()
            .ok_or_else(|| NitriteError::new("Case sensitive flag not initialized", ErrorKind::InvalidOperation))?;

        if case_sensitive {
            Ok(value.as_ref().ok_or_else(|| NitriteError::new(
                "Field value is null or not a string",
                ErrorKind::InvalidOperation,
            ))?.contains(field_value))
        } else {
            Ok(value.as_ref().ok_or_else(|| NitriteError::new(
                "Field value is null or not a string",
                ErrorKind::InvalidOperation,
            ))?.to_lowercase().contains(&field_value.to_lowercase()))
        }
    }

    fn apply_on_index(&self, index_map: &IndexMap) -> NitriteResult<Vec<Value>> {
        let search_string = self.field_value.get()
            .ok_or_else(|| NitriteError::new("Field value not initialized", ErrorKind::InvalidOperation))?
            .clone();
        self.search_by_wildcard(index_map, search_string)
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
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
        Ok(())
    }

    fn get_field_value(&self) -> NitriteResult<Option<Value>> {
        Ok(self.field_value.get()
            .map(|v| Value::String(v.clone())))
    }

    fn set_field_value(&self, field_value: Value) -> NitriteResult<()> {
        if let Value::String(string_value) = field_value {
            self.field_value.get_or_init(|| string_value);
            Ok(())
        } else {
            log::error!("Field value is not a string for filter {}", self);
            Err(NitriteError::new(
                "Field value is not a string",
                ErrorKind::InvalidOperation,
            ))
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A filter that matches elements within arrays.
///
/// This filter evaluates a condition against each element in an array field and matches
/// documents where at least one element satisfies the condition. Elements can be documents
/// or scalar values (matched using a special `$` field name). The filter validates that
/// the wrapped filter is not itself an ElementMatchFilter or TextFilter to prevent invalid nesting.
///
/// # Responsibilities
///
/// * **Array Element Matching**: Evaluates filters against array elements
/// * **Document Handling**: Applies filters to document elements
/// * **Scalar Matching**: Matches scalar values using synthetic documents with `$` field
/// * **Filter Validation**: Prevents invalid filter nesting
/// * **Short-Circuit Evaluation**: Returns true on first matching element
pub(crate) struct ElementMatchFilter {
    field_name: OnceLock<String>,
    filter: Filter,
    collection_name: OnceLock<String>,
}

impl ElementMatchFilter {
    /// Creates a new element match filter for the specified array field and condition.
    ///
    /// The filter evaluates the provided condition against each element in the array field.
    /// For document elements, the condition is applied directly. For scalar elements,
    /// they are wrapped in a synthetic document with field name `$`.
    ///
    /// # Arguments
    ///
    /// * `field_name` - The name of the array field to filter
    /// * `filter` - The filter condition to evaluate against array elements
    ///
    /// # Returns
    ///
    /// A new `ElementMatchFilter` that matches arrays containing at least one matching element
    #[inline]
    pub(crate) fn new(field_name: String, filter: Filter) -> Self {
        let name = OnceLock::new();
        let _ = name.set(field_name);

        ElementMatchFilter {
            field_name: name,
            filter,
            collection_name: OnceLock::new(),
        }
    }

    fn has_element_match_filter(&self) -> bool {
        if is_element_match_filter(&self.filter) {
            return true;
        }
        false
    }

    fn has_text_filter(&self) -> bool {
        if is_text_filter(&self.filter) {
            return true;
        }
        false
    }

    fn matches(&self, value: Vec<Value>) -> NitriteResult<bool> {
        for v in value {
            if self.match_element(&v)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn match_element(&self, value: &Value) -> NitriteResult<bool> {
        match value {
            Value::Document(doc) => self.filter.apply(doc),
            _ => {
                let mut doc = Document::new();
                doc.put("$", value.clone())?;
                self.filter.apply(&doc)
            }
        }
    }
}

impl Display for ElementMatchFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(elemMatch {})", self.filter)
    }
}

impl FilterProvider for ElementMatchFilter {
    #[inline]
    fn apply(&self, entry: &Document) -> NitriteResult<bool> {
        if self.has_element_match_filter() {
            log::error!("ElementMatchFilter {} cannot have another ElementMatchFilter {}", self, self.filter);
            return Err(NitriteError::new(
                "ElementMatchFilter cannot have another ElementMatchFilter",
                ErrorKind::FilterError,
            ));
        }

        if self.has_text_filter() {
            log::error!("ElementMatchFilter {} cannot have TextFilter {}", self, self.filter);
            return Err(NitriteError::new(
                "ElementMatchFilter cannot have TextFilter",
                ErrorKind::FilterError,
            ));
        }

        let field_name = self.field_name.get()
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))?;
        let value = entry.get(field_name)?;
        if value.is_null() {
            return Ok(false);
        }

        if let Value::Array(array) = value {
            return self.matches(array);
        }

        log::error!(
            "ElementMatchFilter can only be applied on array field, found {}",
            value
        );
        Err(NitriteError::new(
            "ElementMatchFilter can only be applied on array field",
            ErrorKind::FilterError,
        ))
    }

    fn get_collection_name(&self) -> NitriteResult<String> {
        self.collection_name.get()
            .cloned()
            .ok_or_else(|| {
                log::error!("Collection name is not set for filter {}", self);
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
            .ok_or_else(|| NitriteError::new("Field name not initialized", ErrorKind::InvalidFieldName))
    }

    fn set_field_name(&self, field_name: String) -> NitriteResult<()> {
        self.field_name.get_or_init(|| field_name);
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
    use crate::filter::basic_filters::EqualsFilter;

    #[test]
    fn test_regex_filter_apply() {
        let filter = RegexFilter::new("field".to_string(), "test.*".to_string());
        let mut doc = Document::new();
        doc.put("field", Value::String("test123".to_string()))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_regex_filter_apply_negative() {
        let filter = RegexFilter::new("field".to_string(), "test.*".to_string());
        let mut doc = Document::new();
        doc.put("field", Value::String("no_match".to_string()))
            .unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_filter_apply() {
        let filter = TextFilter::new("field".to_string(), "test".to_string(), false);
        let mut doc = Document::new();
        doc.put("field", Value::String("this is a test".to_string()))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_filter_apply_negative() {
        let filter = TextFilter::new("field".to_string(), "test".to_string(), false);
        let mut doc = Document::new();
        doc.put("field", Value::String("no match".to_string()))
            .unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_element_match_filter_apply() {
        let inner_filter = EqualsFilter::new("inner_field".to_string(), Value::I32(42));
        let filter =
            ElementMatchFilter::new("field".to_string(), Filter::new(inner_filter));
        let mut doc = Document::new();
        let mut inner_doc = Document::new();
        inner_doc.put("inner_field", Value::I32(42)).unwrap();
        doc.put("field", Value::Array(vec![Value::Document(inner_doc)]))
            .unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_element_match_filter_apply_negative() {
        let inner_filter = EqualsFilter::new("inner_field".to_string(), Value::I32(42));
        let filter =
            ElementMatchFilter::new("field".to_string(), Filter::new(inner_filter));
        let mut doc = Document::new();
        let mut inner_doc = Document::new();
        inner_doc.put("inner_field", Value::I32(43)).unwrap();
        doc.put("field", Value::Array(vec![Value::Document(inner_doc)]))
            .unwrap();
        assert!(!filter.apply(&doc).unwrap());
    }

    // Invalid regex pattern handling tests
    #[test]
    fn test_regex_filter_handles_invalid_pattern_in_constructor() {
        // Invalid regex pattern - should not panic, but pattern will be uninitialized
        let filter = RegexFilter::new("field".to_string(), "(?P<invalid>".to_string());
        // Creation should not panic
        assert!(filter.field_name.get().is_some());
        assert!(filter.field_value.get().is_some());
        // Pattern should not be initialized due to invalid regex
        assert!(filter.pattern.get().is_none());
    }

    #[test]
    fn test_regex_filter_valid_pattern_initializes() {
        let filter = RegexFilter::new("field".to_string(), "test.*".to_string());
        // Valid pattern should be initialized
        assert!(filter.pattern.get().is_some());
        let mut doc = Document::new();
        doc.put("field", Value::String("test123".to_string())).unwrap();
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_regex_filter_set_invalid_field_value_returns_error() {
        let filter = RegexFilter::new("field".to_string(), "valid.*".to_string());
        let result = filter.set_field_value(Value::String("(?P<invalid>".to_string()));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_regex_filter_display_with_initialized_values() {
        let filter = RegexFilter::new("email".to_string(), r"^[a-z]+@.*\.com$".to_string());
        let display_str = format!("{}", filter);
        assert!(display_str.contains("email"));
        assert!(display_str.contains("=~"));
    }

    #[test]
    fn test_regex_filter_apply_with_invalid_pattern_returns_error() {
        // Create filter with invalid pattern through constructor
        let filter = RegexFilter::new("field".to_string(), "(?P<invalid>".to_string());
        let mut doc = Document::new();
        doc.put("field", Value::String("test".to_string())).unwrap();
        let result = filter.apply(&doc);
        // Should return error due to invalid pattern
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid regex"));
    }

    #[test]
    fn test_regex_filter_get_field_name_error_handling() {
        let filter = RegexFilter::new("test_field".to_string(), "test.*".to_string());
        let field_name = filter.get_field_name().unwrap();
        assert_eq!(field_name, "test_field");
    }

    // TextFilter tokenizer initialization and handling tests
    #[test]
    fn test_text_filter_basic_apply() {
        let filter = TextFilter::new("content".to_string(), "hello".to_string(), false);
        
        let mut doc = Document::new();
        doc.put("content", Value::String("hello world example".to_string())).unwrap();
        // This tests basic text matching without needing a tokenizer
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_filter_case_insensitive_apply() {
        let filter = TextFilter::new("field".to_string(), "TEST".to_string(), false);
        let mut doc = Document::new();
        doc.put("field", Value::String("this is a test".to_string())).unwrap();
        // Case-insensitive match should work
        assert!(filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_filter_case_sensitive_apply() {
        let filter = TextFilter::new("field".to_string(), "Test".to_string(), true);
        let mut doc = Document::new();
        doc.put("field", Value::String("this is a test".to_string())).unwrap();
        // Case-sensitive match should fail
        assert!(!filter.apply(&doc).unwrap());
    }

    #[test]
    fn test_text_filter_display_case_sensitive() {
        let filter = TextFilter::new("body".to_string(), "search_term".to_string(), true);
        let display_str = format!("{}", filter);
        assert!(display_str.contains("case_sensitive"));
        assert!(display_str.contains("body"));
    }

    #[test]
    fn test_text_filter_display_case_insensitive() {
        let filter = TextFilter::new("body".to_string(), "search_term".to_string(), false);
        let display_str = format!("{}", filter);
        assert!(display_str.contains("case_insensitive"));
        assert!(display_str.contains("body"));
    }

    #[test]
    fn test_text_filter_set_and_get_collection_name() {
        let filter = TextFilter::new("field".to_string(), "value".to_string(), false);
        filter.set_collection_name("my_collection".to_string()).unwrap();
        let name = filter.get_collection_name().unwrap();
        assert_eq!(name, "my_collection");
    }

    #[test]
    fn test_text_filter_get_field_name() {
        let filter = TextFilter::new("search_field".to_string(), "value".to_string(), false);
        let field_name = filter.get_field_name().unwrap();
        assert_eq!(field_name, "search_field");
    }

    #[test]
    fn test_text_filter_set_invalid_field_value_type() {
        let filter = TextFilter::new("field".to_string(), "value".to_string(), false);
        let result = filter.set_field_value(Value::I32(42));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not a string"));
    }
    
    #[test]
    fn test_element_match_filter_safe_field_name_access() {
        // Verify ElementMatchFilter::get_field_name uses safe access
        let inner_filter = EqualsFilter::new("inner".to_string(), Value::I32(1));
        let filter = ElementMatchFilter::new("items".to_string(), Filter::new(inner_filter));
        
        let result = filter.get_field_name();
        assert!(result.is_ok());
        let field_name = result.unwrap();
        assert_eq!(field_name, "items");
    }

    #[test]
    fn test_element_match_filter_safe_collection_name_access() {
        // Verify ElementMatchFilter::get_collection_name uses safe access
        let inner_filter = EqualsFilter::new("inner".to_string(), Value::I32(1));
        let filter = ElementMatchFilter::new("field".to_string(), Filter::new(inner_filter));
        
        filter.set_collection_name("test_collection".to_string()).unwrap();
        let result = filter.get_collection_name();
        assert!(result.is_ok());
        let name = result.unwrap();
        assert_eq!(name, "test_collection");
    }

    #[test]
    fn test_element_match_filter_collection_name_not_set_error() {
        // Verify safe error handling when collection name not initialized
        let inner_filter = EqualsFilter::new("inner".to_string(), Value::I32(1));
        let filter = ElementMatchFilter::new("field".to_string(), Filter::new(inner_filter));
        
        let result = filter.get_collection_name();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Collection name is not set"));
    }

    #[test]
    fn test_regex_filter_safe_string_extraction_in_apply() {
        // Verify RegexFilter::apply safely handles string extraction
        let filter = RegexFilter::new("email".to_string(), r"^[a-z]+@.*\.com$".to_string());
        
        let mut doc = Document::new();
        doc.put("email", Value::String("user@example.com".to_string())).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_regex_filter_safe_handle_non_string_field() {
        // Verify RegexFilter::apply returns false for non-string fields
        let filter = RegexFilter::new("count".to_string(), r"\d+".to_string());
        
        let mut doc = Document::new();
        doc.put("count", Value::I32(42)).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_text_filter_safe_string_unwrapping_case_sensitive() {
        // Verify TextFilter::apply safely unwraps string with case sensitivity
        let filter = TextFilter::new("content".to_string(), "SEARCH".to_string(), true);
        
        let mut doc = Document::new();
        doc.put("content", Value::String("this is SEARCH term".to_string())).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_text_filter_safe_string_unwrapping_case_insensitive() {
        // Verify TextFilter::apply safely unwraps string case-insensitive
        let filter = TextFilter::new("content".to_string(), "search".to_string(), false);
        
        let mut doc = Document::new();
        doc.put("content", Value::String("this is SEARCH term".to_string())).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_element_match_filter_apply_with_empty_array() {
        // Verify ElementMatchFilter::apply safely handles empty arrays
        let inner_filter = EqualsFilter::new("value".to_string(), Value::I32(42));
        let filter = ElementMatchFilter::new("items".to_string(), Filter::new(inner_filter));
        
        let mut doc = Document::new();
        doc.put("items", Value::Array(vec![])).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_regex_filter_complex_pattern_matches() {
        // Verify regex patterns work correctly with safe extraction
        let pattern = r"^[A-Z][a-z]*$";
        let filter = RegexFilter::new("name".to_string(), pattern.to_string());
        
        let mut doc = Document::new();
        doc.put("name", Value::String("Alice".to_string())).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }

    #[test]
    fn test_element_match_filter_with_multiple_elements() {
        // Verify ElementMatchFilter correctly matches first matching element
        let inner_filter = EqualsFilter::new("status".to_string(), Value::String("active".to_string()));
        let filter = ElementMatchFilter::new("users".to_string(), Filter::new(inner_filter));
        
        let mut doc = Document::new();
        let mut user1 = Document::new();
        user1.put("status", Value::String("inactive".to_string())).unwrap();
        let mut user2 = Document::new();
        user2.put("status", Value::String("active".to_string())).unwrap();
        
        doc.put("users", Value::Array(vec![
            Value::Document(user1),
            Value::Document(user2),
        ])).unwrap();
        
        let result = filter.apply(&doc);
        assert!(result.is_ok());
        assert!(result.unwrap());
    }
}