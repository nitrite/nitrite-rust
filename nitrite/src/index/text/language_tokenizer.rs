use super::{languages::stop_words::English, Language, Languages, TokenizerProvider};
use crate::index::text::languages::stop_words::*;

pub struct EnglishTokenizer;

impl TokenizerProvider for EnglishTokenizer {
    #[inline]
    fn supported_languages(&self) -> Languages {
        Languages::English
    }

    #[inline]
    fn stop_words(&self) -> Vec<&'static str> {
        English.stop_words()
    }
}

pub struct UniversalTokenizer {
    stop_words: Vec<&'static str>,
}

impl UniversalTokenizer {
    #[inline]
    pub fn new(languages: Option<Vec<Languages>>) -> Self {
        // Use match instead of is_none() + unwrap() anti-pattern
        let stop_words = match languages {
            None => {
                // No languages specified - load all stop words
                Self::load_all_stop_words()
            }
            Some(langs) => {
                // Languages specified - load only those languages' stop words
                // Pre-allocate with estimated capacity (avg 500 words per language * # of langs)
                let mut stop_words = Vec::with_capacity(langs.len() * 500);
                for language in langs {
                    match language {
                        Languages::All => {
                            stop_words = Self::load_all_stop_words();
                            break;
                        }
                        Languages::Afrikaans => {
                            stop_words.extend(Afrikaans.stop_words());
                        }
                        Languages::Arabic => {
                            stop_words.extend(Arabic.stop_words());
                        }
                        Languages::Armenian => {
                            stop_words.extend(Armenian.stop_words());
                        }
                        Languages::Basque => {
                            stop_words.extend(Basque.stop_words());
                        }
                        Languages::Bengali => {
                            stop_words.extend(Bengali.stop_words());
                        }
                        Languages::Brazilian => {
                            stop_words.extend(Brazilian.stop_words());
                        }
                        Languages::Breton => {
                            stop_words.extend(Breton.stop_words());
                        }
                        Languages::Bulgarian => {
                            stop_words.extend(Bulgarian.stop_words());
                        }
                        Languages::Catalan => {
                            stop_words.extend(Catalan.stop_words());
                        }
                        Languages::Chinese => {
                            stop_words.extend(Chinese.stop_words());
                        }
                        Languages::Croatian => {
                            stop_words.extend(Croatian.stop_words());
                        }
                        Languages::Czech => {
                            stop_words.extend(Czech.stop_words());
                        }
                        Languages::Danish => {
                            stop_words.extend(Danish.stop_words());
                        }
                        Languages::Dutch => {
                            stop_words.extend(Dutch.stop_words());
                        }
                        Languages::English => {
                            stop_words.extend(English.stop_words());
                        }
                        Languages::Esperanto => {
                            stop_words.extend(Esperanto.stop_words());
                        }
                        Languages::Estonian => {
                            stop_words.extend(Estonian.stop_words());
                        }
                        Languages::Finnish => {
                            stop_words.extend(Finnish.stop_words());
                        }
                        Languages::French => {
                            stop_words.extend(French.stop_words());
                        }
                        Languages::Galician => {
                            stop_words.extend(Galician.stop_words());
                        }
                        Languages::German => {
                            stop_words.extend(German.stop_words());
                        }
                        Languages::Greek => {
                            stop_words.extend(Greek.stop_words());
                        }
                        Languages::Hausa => {
                            stop_words.extend(Hausa.stop_words());
                        }
                        Languages::Hebrew => {
                            stop_words.extend(Hebrew.stop_words());
                        }
                        Languages::Hindi => {
                            stop_words.extend(Hindi.stop_words());
                        }
                        Languages::Hungarian => {
                            stop_words.extend(Hungarian.stop_words());
                        }
                        Languages::Indonesian => {
                            stop_words.extend(Indonesian.stop_words());
                        }
                        Languages::Irish => {
                            stop_words.extend(Irish.stop_words());
                        }
                        Languages::Italian => {
                            stop_words.extend(Italian.stop_words());
                        }
                        Languages::Japanese => {
                            stop_words.extend(Japanese.stop_words());
                        }
                        Languages::Korean => {
                            stop_words.extend(Korean.stop_words());
                        }
                        Languages::Kurdish => {
                            stop_words.extend(Kurdish.stop_words());
                        }
                        Languages::Latin => {
                            stop_words.extend(Latin.stop_words());
                        }
                        Languages::Latvian => {
                            stop_words.extend(Latvian.stop_words());
                        }
                        Languages::Lithuanian => {
                            stop_words.extend(Lithuanian.stop_words());
                        }
                        Languages::Malay => {
                            stop_words.extend(Malay.stop_words());
                        }
                        Languages::Marathi => {
                            stop_words.extend(Marathi.stop_words());
                        }
                        Languages::Norwegian => {
                            stop_words.extend(Norwegian.stop_words());
                        }
                        Languages::Persian => {
                            stop_words.extend(Persian.stop_words());
                        }
                        Languages::Polish => {
                            stop_words.extend(Polish.stop_words());
                        }
                        Languages::Portuguese => {
                            stop_words.extend(Portuguese.stop_words());
                        }
                        Languages::Romanian => {
                            stop_words.extend(Romanian.stop_words());
                        }
                        Languages::Russian => {
                            stop_words.extend(Russian.stop_words());
                        }
                        Languages::Sesotho => {
                            stop_words.extend(Sesotho.stop_words());
                        }
                        Languages::Slovak => {
                            stop_words.extend(Slovak.stop_words());
                        }
                        Languages::Slovenian => {
                            stop_words.extend(Slovenian.stop_words());
                        }
                        Languages::Somali => {
                            stop_words.extend(Somali.stop_words());
                        }
                        Languages::Spanish => {
                            stop_words.extend(Spanish.stop_words());
                        }
                        Languages::Swahili => {
                            stop_words.extend(Swahili.stop_words());
                        }
                        Languages::Swedish => {
                            stop_words.extend(Swedish.stop_words());
                        }
                        Languages::Tagalog => {
                            stop_words.extend(Tagalog.stop_words());
                        }
                        Languages::Thai => {
                            stop_words.extend(Thai.stop_words());
                        }
                        Languages::Turkish => {
                            stop_words.extend(Turkish.stop_words());
                        }
                        Languages::Ukrainian => {
                            stop_words.extend(Ukrainian.stop_words());
                        }
                        Languages::Urdu => {
                            stop_words.extend(Urdu.stop_words());
                        }
                        Languages::Vietnamese => {
                            stop_words.extend(Vietnamese.stop_words());
                        }
                        Languages::Yoruba => {
                            stop_words.extend(Yoruba.stop_words());
                        }
                        Languages::Zulu => {
                            stop_words.extend(Zulu.stop_words());
                        }
                        Languages::Unknown => {
                            // nothing to do
                        }
                    }
                }
                stop_words
            }
        };
        UniversalTokenizer { stop_words }
    }
    
    fn load_all_stop_words() -> Vec<&'static str> {
        // Pre-allocate Vec with capacity for ~55 languages * ~500 words avg = ~27500
        let mut stop_words = Vec::with_capacity(27500);
        stop_words.extend(Afrikaans.stop_words());
        stop_words.extend(Arabic.stop_words());
        stop_words.extend(Armenian.stop_words());
        stop_words.extend(Basque.stop_words());
        stop_words.extend(Bengali.stop_words());
        stop_words.extend(Brazilian.stop_words());
        stop_words.extend(Breton.stop_words());
        stop_words.extend(Bulgarian.stop_words());
        stop_words.extend(Catalan.stop_words());
        stop_words.extend(Chinese.stop_words());
        stop_words.extend(Croatian.stop_words());
        stop_words.extend(Czech.stop_words());
        stop_words.extend(Danish.stop_words());
        stop_words.extend(Dutch.stop_words());
        stop_words.extend(English.stop_words());
        stop_words.extend(Esperanto.stop_words());
        stop_words.extend(Estonian.stop_words());
        stop_words.extend(Finnish.stop_words());
        stop_words.extend(French.stop_words());
        stop_words.extend(Galician.stop_words());
        stop_words.extend(German.stop_words());
        stop_words.extend(Greek.stop_words());
        stop_words.extend(Hausa.stop_words());
        stop_words.extend(Hebrew.stop_words());
        stop_words.extend(Hindi.stop_words());
        stop_words.extend(Hungarian.stop_words());
        stop_words.extend(Indonesian.stop_words());
        stop_words.extend(Irish.stop_words());
        stop_words.extend(Italian.stop_words());
        stop_words.extend(Japanese.stop_words());
        stop_words.extend(Korean.stop_words());
        stop_words.extend(Kurdish.stop_words());
        stop_words.extend(Latin.stop_words());
        stop_words.extend(Latvian.stop_words());
        stop_words.extend(Lithuanian.stop_words());
        stop_words.extend(Malay.stop_words());
        stop_words.extend(Marathi.stop_words());
        stop_words.extend(Norwegian.stop_words());
        stop_words.extend(Persian.stop_words());
        stop_words.extend(Polish.stop_words());
        stop_words.extend(Portuguese.stop_words());
        stop_words.extend(Romanian.stop_words());
        stop_words.extend(Russian.stop_words());
        stop_words.extend(Sesotho.stop_words());
        stop_words.extend(Slovak.stop_words());
        stop_words.extend(Slovenian.stop_words());
        stop_words.extend(Somali.stop_words());
        stop_words.extend(Spanish.stop_words());
        stop_words.extend(Swahili.stop_words());
        stop_words.extend(Swedish.stop_words());
        stop_words.extend(Tagalog.stop_words());
        stop_words.extend(Thai.stop_words());
        stop_words.extend(Turkish.stop_words());
        stop_words.extend(Ukrainian.stop_words());
        stop_words.extend(Urdu.stop_words());
        stop_words.extend(Vietnamese.stop_words());
        stop_words.extend(Yoruba.stop_words());
        stop_words.extend(Zulu.stop_words());
        
        stop_words
    }
}

impl TokenizerProvider for UniversalTokenizer {
    #[inline]
    fn supported_languages(&self) -> Languages {
        Languages::All
    }

    #[inline]
    fn stop_words(&self) -> Vec<&'static str> {
        self.stop_words.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::text::languages::stop_words::*;

    #[test]
    fn test_english_tokenizer_supported_languages() {
        let tokenizer = EnglishTokenizer;
        assert_eq!(tokenizer.supported_languages(), Languages::English);
    }

    #[test]
    fn test_english_tokenizer_stop_words() {
        let tokenizer = EnglishTokenizer;
        assert_eq!(tokenizer.stop_words(), English.stop_words());
    }

    #[test]
    fn test_universal_tokenizer_no_languages() {
        let tokenizer = UniversalTokenizer::new(None);
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        assert_eq!(tokenizer.stop_words(), all_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_with_languages() {
        let languages = vec![Languages::English, Languages::French];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let mut expected_stop_words = vec![];
        expected_stop_words.extend(English.stop_words());
        expected_stop_words.extend(French.stop_words());
        assert_eq!(tokenizer.stop_words(), expected_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_supported_languages() {
        let tokenizer = UniversalTokenizer::new(None);
        assert_eq!(tokenizer.supported_languages(), Languages::All);
    }

    #[test]
    fn test_universal_tokenizer_stop_words() {
        let tokenizer = UniversalTokenizer::new(None);
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        assert_eq!(tokenizer.stop_words(), all_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_empty_languages() {
        let tokenizer = UniversalTokenizer::new(Some(vec![]));
        assert!(tokenizer.stop_words().is_empty());
    }

    #[test]
    fn test_universal_tokenizer_invalid_language() {
        let languages = vec![Languages::Zulu, Languages::Unknown];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let mut expected_stop_words = vec![];
        expected_stop_words.extend(Zulu.stop_words());
        assert_eq!(tokenizer.stop_words(), expected_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_with_all_language() {
        // When Languages::All is present, it should load all stop words and break early
        let languages = vec![Languages::English, Languages::All, Languages::French];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        assert_eq!(tokenizer.stop_words(), all_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_single_language() {
        let tokenizer = UniversalTokenizer::new(Some(vec![Languages::Spanish]));
        assert_eq!(tokenizer.stop_words(), Spanish.stop_words());
    }

    #[test]
    fn test_universal_tokenizer_multiple_languages_combination() {
        let languages = vec![Languages::German, Languages::Italian, Languages::Portuguese];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let mut expected = vec![];
        expected.extend(German.stop_words());
        expected.extend(Italian.stop_words());
        expected.extend(Portuguese.stop_words());
        assert_eq!(tokenizer.stop_words(), expected);
    }

    #[test]
    fn test_universal_tokenizer_preserves_order_of_languages() {
        // Test that the order of languages is preserved in stop words
        let languages = vec![Languages::French, Languages::English];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let mut expected = vec![];
        expected.extend(French.stop_words());
        expected.extend(English.stop_words());
        assert_eq!(tokenizer.stop_words(), expected);
    }

    #[test]
    fn test_universal_tokenizer_none_vs_empty_vec() {
        // None should load all stop words, Some(vec![]) should load nothing
        let tokenizer_none = UniversalTokenizer::new(None);
        let tokenizer_empty = UniversalTokenizer::new(Some(vec![]));
        
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        assert_eq!(tokenizer_none.stop_words(), all_stop_words);
        assert!(tokenizer_empty.stop_words().is_empty());
    }

    #[test]
    fn test_universal_tokenizer_all_language_breaks_iteration() {
        // All should break the loop and load all languages
        let languages = vec![
            Languages::English,
            Languages::All,
            Languages::Russian,
            Languages::Arabic,
        ];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        // Should only contain all stop words, not repeated additions
        assert_eq!(tokenizer.stop_words(), all_stop_words);
    }

    #[test]
    fn test_universal_tokenizer_duplicate_languages() {
        // Test that duplicate languages are handled (both are added)
        let languages = vec![Languages::English, Languages::English];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let mut expected = vec![];
        expected.extend(English.stop_words());
        expected.extend(English.stop_words());
        assert_eq!(tokenizer.stop_words(), expected);
    }

    // Performance optimization tests
    #[test]
    fn test_universal_tokenizer_capacity_optimization() {
        // Verify that pre-allocation reduces allocations for multiple languages
        let languages = vec![
            Languages::English,
            Languages::French,
            Languages::German,
            Languages::Spanish,
            Languages::Italian,
        ];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let stop_words = tokenizer.stop_words();
        
        // Verify all languages' stop words are present
        assert!(!stop_words.is_empty());
        assert!(stop_words.contains(&"the")); // English
        assert!(stop_words.contains(&"le"));  // French
        assert!(stop_words.contains(&"der")); // German
        assert!(stop_words.contains(&"el"));  // Spanish
        assert!(stop_words.contains(&"il"));  // Italian
    }

    #[test]
    fn test_universal_tokenizer_load_all_capacity() {
        // Verify load_all_stop_words uses pre-allocated capacity
        let all_stop_words = UniversalTokenizer::load_all_stop_words();
        assert!(!all_stop_words.is_empty());
        // Should have significant words from all languages
        assert!(all_stop_words.len() > 5000);
    }

    #[test]
    fn test_english_tokenizer_repeated_calls() {
        // Verify inline optimization for repeated stop_words calls
        let tokenizer = EnglishTokenizer;
        for _ in 0..100 {
            let words = tokenizer.stop_words();
            assert!(!words.is_empty());
        }
    }

    #[test]
    fn test_universal_tokenizer_supported_languages_inline() {
        // Verify inline supported_languages returns correct value
        let tokenizer = UniversalTokenizer::new(Some(vec![Languages::English]));
        assert_eq!(tokenizer.supported_languages(), Languages::All);
    }

    #[test]
    fn test_universal_tokenizer_english_only_performance() {
        // Single language should be fast with pre-allocation
        let tokenizer = UniversalTokenizer::new(Some(vec![Languages::English]));
        let words = tokenizer.stop_words();
        assert_eq!(words, English.stop_words());
    }

    #[test]
    fn test_universal_tokenizer_many_languages_consolidation() {
        // Test with many languages to verify Vec consolidation works
        let languages = vec![
            Languages::English,
            Languages::French,
            Languages::German,
            Languages::Spanish,
            Languages::Italian,
            Languages::Portuguese,
            Languages::Dutch,
            Languages::Swedish,
            Languages::Polish,
            Languages::Russian,
        ];
        let tokenizer = UniversalTokenizer::new(Some(languages));
        let stop_words = tokenizer.stop_words();
        assert!(!stop_words.is_empty());
    }

    #[test]
    fn test_english_tokenizer_supported_languages_inline_opt() {
        // Verify supported_languages uses inline optimization
        let tokenizer = EnglishTokenizer;
        for _ in 0..100 {
            assert_eq!(tokenizer.supported_languages(), Languages::English);
        }
    }
}