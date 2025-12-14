use crate::common::{DefaultFilter, StringTokenizer};
use crate::index::text::Languages;
use std::ops::Deref;
use std::sync::Arc;

/// Provides text tokenization and stop word filtering for full-text indexing.
///
/// TokenizerProvider is a trait for implementing language-specific tokenization
/// strategies. It breaks text into tokens (words) and filters out common stop words
/// that don't contribute meaningful search value.
///
/// # Purpose
/// TokenizerProvider enables pluggable tokenization for different languages and
/// text processing strategies. Different language implementations can customize
/// tokenization rules and stop word lists.
///
/// # Characteristics
/// - **Thread-safe**: Requires Send + Sync for concurrent access
/// - **Language-aware**: Supports multiple languages with language-specific stop words
/// - **Customizable**: Implementations can override tokenization behavior
/// - **Stateless**: Default tokenization uses immutable operations
///
/// # Implementations
/// - `EnglishTokenizer` - English-specific tokenization
/// - `UniversalTokenizer` - Multi-language support with configurable languages
///
/// # Usage in Full-Text Indexing
/// TokenizerProvider is used when creating full-text indexes on text fields:
/// - Text is tokenized into individual words
/// - Stop words (common words like "the", "a", "is") are filtered out
/// - Remaining tokens are indexed for full-text search queries
pub trait TokenizerProvider: Send + Sync {
    /// Returns the languages supported by this tokenizer.
    ///
    /// # Returns
    /// The Languages enum value indicating supported language(s).
    ///
    /// # Behavior
    /// Indicates which language(s) the tokenizer is optimized for.
    /// This determines the set of stop words and tokenization rules used.
    ///
    /// # Usage
    /// Check tokenizer language support:
    /// ```ignore
    /// let english_tok = EnglishTokenizer;
    /// assert_eq!(english_tok.supported_languages(), Languages::English);
    /// ```
    fn supported_languages(&self) -> Languages;

    /// Tokenizes text into a vector of word tokens with stop word filtering.
    ///
    /// # Arguments
    /// * `text` - The text to tokenize
    ///
    /// # Returns
    /// Vector of tokenized strings with stop words removed.
    ///
    /// # Behavior
    /// Default implementation:
    /// 1. Creates StringTokenizer with DefaultFilter and input text
    /// 2. Filters out tokens matching stop_words() list
    /// 3. Converts remaining tokens to owned Strings
    /// 4. Collects into Vec
    ///
    /// This can be overridden for custom tokenization logic.
    ///
    /// # Usage
    /// Tokenize text for indexing:
    /// ```ignore
    /// let tokenizer = Tokenizer::new(EnglishTokenizer);
    /// let text = "This is a sample document";
    /// let tokens = tokenizer.tokenize(text);
    /// // tokens: ["This", "is", "sample", "document"]
    /// // "a" is filtered as stop word
    /// ```
    fn tokenize(&self, text: &str) -> Vec<String> {
        StringTokenizer::new(DefaultFilter, text)
            .filter(|token| !self.stop_words().contains(&token.term()))
            .map(|token| token.term().to_string())
            .collect()
    }

    /// Returns the list of stop words to filter during tokenization.
    ///
    /// # Returns
    /// Vector of static string references representing stop words.
    ///
    /// # Behavior
    /// Stop words are common words that typically don't contribute to search meaning
    /// (e.g., "the", "a", "is", "and"). Different languages have different stop word lists.
    ///
    /// # Usage
    /// Stop words are automatically used by tokenize() to filter tokens:
    /// ```ignore
    /// let english_tok = EnglishTokenizer;
    /// let stop_words = english_tok.stop_words();
    /// // Contains common English stop words
    /// ```
    fn stop_words(&self) -> Vec<&'static str>;
}

#[derive(Clone)]
/// Type-erased, polymorphic wrapper for TokenizerProvider implementations.
///
/// Tokenizer provides a unified interface for accessing tokenizer functionality
/// regardless of the underlying implementation (English, Universal, etc.).
/// It uses Arc for shared ownership, enabling efficient cloning and concurrent access.
///
/// # Purpose
/// Tokenizer abstracts away concrete TokenizerProvider implementations,
/// allowing tokenizers to be passed around as trait objects while maintaining
/// thread safety and ergonomic ownership semantics.
///
/// # Characteristics
/// - **Type-erased**: Wraps any TokenizerProvider implementation
/// - **Thread-safe**: Arc provides shared concurrent access
/// - **Cloneable**: Inexpensive cloning via Arc sharing
/// - **Transparent**: Deref implementation provides direct method access
/// - **Polymorphic**: Trait object enables runtime dispatch
///
/// # Usage
///
/// Create tokenizer from concrete implementation:
/// ```ignore
/// let tokenizer = Tokenizer::new(EnglishTokenizer);
/// ```
///
/// Use transparent Deref for trait methods:
/// ```ignore
/// let tokenizer = Tokenizer::new(EnglishTokenizer);
/// let languages = tokenizer.supported_languages();
/// let tokens = tokenizer.tokenize("hello world");
/// ```
///
/// Pass to functions expecting trait methods:
/// ```ignore
/// fn process_text(tokenizer: &Tokenizer, text: &str) {
///     let tokens = tokenizer.tokenize(text);
/// }
/// ```
pub struct Tokenizer {
    inner: Arc<dyn TokenizerProvider>,
}

impl Tokenizer {
    /// Creates a new Tokenizer wrapping the given TokenizerProvider implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete implementation of TokenizerProvider
    ///
    /// # Returns
    /// A new Tokenizer instance wrapping the provided implementation in Arc.
    ///
    /// # Behavior
    /// Wraps the concrete implementation in Arc<dyn TokenizerProvider> for:
    /// - Type erasure (concrete type is hidden behind trait object)
    /// - Shared ownership (Arc enables cloning without duplicating)
    /// - Thread-safe access (underlying TokenizerProvider is Send + Sync)
    ///
    /// The implementation is stored once and shared across all clones.
    ///
    /// # Usage
    /// Create tokenizer from concrete implementation:
    /// ```ignore
    /// // English tokenizer for full-text indexing
    /// let tokenizer = Tokenizer::new(EnglishTokenizer);
    ///
    /// // Universal tokenizer with specific languages
    /// let universal = UniversalTokenizer::new(Some(vec![Languages::English, Languages::Spanish]));
    /// let tokenizer = Tokenizer::new(universal);
    /// ```
    pub fn new<T: TokenizerProvider + 'static>(inner: T) -> Self {
        Tokenizer { inner: Arc::new(inner) }
    }
}

impl Deref for Tokenizer {
    type Target = Arc<dyn TokenizerProvider>;

    /// Provides transparent access to the underlying TokenizerProvider trait methods.
    ///
    /// # Returns
    /// A reference to Arc<dyn TokenizerProvider> for calling trait methods.
    ///
    /// # Behavior
    /// Implements Deref to enable calling TokenizerProvider methods directly on Tokenizer
    /// without explicit dereferencing. This provides ergonomic transparent access.
    ///
    /// # Usage
    /// Call trait methods directly on Tokenizer instance:
    /// ```ignore
    /// let tokenizer = Tokenizer::new(EnglishTokenizer);
    ///
    /// // No explicit deref needed - automatic via Deref trait
    /// let languages = tokenizer.supported_languages();
    /// let tokens = tokenizer.tokenize("hello world");
    /// let stops = tokenizer.stop_words();
    /// ```
    ///
    /// This works because Deref coercion automatically dereferences Tokenizer
    /// to Arc<dyn TokenizerProvider>, which then coerces to &dyn TokenizerProvider
    /// for method calls.
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text::Languages;

    struct MockTokenizer;

    impl TokenizerProvider for MockTokenizer {
        fn supported_languages(&self) -> Languages {
            Languages::English
        }

        fn stop_words(&self) -> Vec<&'static str> {
            vec!["a", "an", "the"]
        }
    }

    #[test]
    fn test_tokenizer_supported_languages() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        assert_eq!(tokenizer.supported_languages(), Languages::English);
    }

    #[test]
    fn test_tokenizer_tokenize() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        let text = "This is a test.";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(tokens, vec!["This", "is", "test"]);
    }

    #[test]
    fn test_tokenizer_stop_words() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        assert_eq!(tokenizer.stop_words(), vec!["a", "an", "the"]);
    }

    #[test]
    fn test_tokenizer_empty_text() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        let text = "";
        let tokens = tokenizer.tokenize(text);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenizer_whitespace_text() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        let text = "   ";
        let tokens = tokenizer.tokenize(text);
        assert!(tokens.is_empty());
    }

    #[test]
    fn test_tokenizer_special_characters() {
        let tokenizer = Tokenizer::new(MockTokenizer);
        let text = "!@#$%^&*()";
        let tokens = tokenizer.tokenize(text);
        assert_eq!(tokens, vec!["$"]);
    }
}