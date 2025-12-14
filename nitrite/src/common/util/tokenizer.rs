// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//
// No crate is available. Source - https://github.com/Jeffail/tokesies

use std::{borrow::Cow, fmt};

pub struct Token<'a> {
    /// The content of the extracted token.
    pub term: Cow<'a, str>,

    /// The absolute offset of the token in chars.
    pub start_offset: usize,

    /// The token position.
    pub position: usize,
}

impl<'a> Token<'a> {
    #[inline]
    pub fn from_str(term: &'a str, start_offset: usize, position: usize) -> Self {
        Token {
            term: Cow::Borrowed(term),
            start_offset,
            position,
        }
    }

    #[inline]
    pub fn term(&self) -> &str {
        self.term.as_ref()
    }
}

impl<'a> fmt::Debug for Token<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.term())
    }
}

/// Implementation of Tokenizer that extracts based on a provided Filter
/// implementation.
pub struct StringTokenizer<'a, T: StringFilter> {
    filter: T,
    input: &'a str,
    byte_offset: usize,
    char_offset: usize,
    position: usize,
}

impl<'a, T: StringFilter> StringTokenizer<'a, T> {
    pub fn new(filter: T, input: &'a str) -> Self {
        StringTokenizer {
            filter,
            input,
            byte_offset: 0,
            char_offset: 0,
            position: 0,
        }
    }
}

impl<'a, T: StringFilter> Iterator for StringTokenizer<'a, T> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        let mut skipped_bytes = 0;
        let mut skipped_chars = 0;

        let filter = &self.filter;

        // cidx, bidx is the char and byte index from the last found separator
        for (cidx, bidx, c, is_keep) in
            self.input[self.byte_offset..]
                .char_indices()
                .enumerate()
                // Remove any drop codes entirely
                .filter_map(|(ci, (bi, c))| {
                    let (is_filtered, is_keep) = filter.on_char(&c);
                    if is_filtered {
                        Some((ci, bi, c, is_keep))
                    } else {
                        None
                    }
                })
        {
            let char_len = c.len_utf8();

            // If we found a separator but had no text beforehand simply move
            // our counters to the new position.
            if cidx == skipped_chars {
                self.char_offset += 1;
                self.byte_offset += char_len;
                skipped_bytes += char_len;
                skipped_chars += 1;
                if is_keep {
                    let slice = &self.input[self.byte_offset - char_len..
                        self.byte_offset + bidx + char_len - skipped_bytes];
                    let token = Token::from_str(slice, self.char_offset - 1, self.position);
                    self.position += 1;
                    return Some(token);
                }
                continue;
            }

            let slice = &self.input[self.byte_offset..self.byte_offset + bidx - skipped_bytes];
            let token = Token::from_str(slice, self.char_offset, self.position);

            self.char_offset += slice.chars().count();
            self.position += 1;
            self.byte_offset += bidx - skipped_bytes;
            if !is_keep {
                self.char_offset += 1;
                self.byte_offset += char_len;
            }
            return Some(token);
        }

        if self.byte_offset < self.input.len() {
            let slice = &self.input[self.byte_offset..];
            let token = Token::from_str(slice, self.char_offset, self.position);
            self.byte_offset = self.input.len();
            Some(token)
        } else {
            None
        }
    }
}

/// A type for filtering chars during tokenization.
pub trait StringFilter {
    /// Returns a tuple of bool, bool indicating whether the character marks the
    /// end of a token, and whether it should also be collected as a token in
    /// itself, respectively.
    ///
    /// (false, false) - part of a token
    /// (true,  false) - not part of a token and should be discarded
    /// (true,   true) - not part of token but is one in its own right
    fn on_char(&self, c: &char) -> (bool, bool);
}

/// A filter for selecting whitespace characters only.
pub struct WhitespaceFilter;

impl StringFilter for WhitespaceFilter {
    #[inline]
    fn on_char(&self, c: &char) -> (bool, bool) {
        (c.is_whitespace(), false)
    }
}

/// A filter that uses a pre-chosen set of default tokenization characters.
pub struct DefaultFilter;

impl StringFilter for DefaultFilter {
    #[inline]
    fn on_char(&self, c: &char) -> (bool, bool) {
        match *c {
            ' ' | '\t' | '\n' | '\r' | '\u{C}' | '\u{B}' | '\u{A0}' | '\u{FEFF}' |
            '#' | '!' | '\\' | '"' | '%' | '&' | '\'' | '(' | ')' | '+' | '-' | '.' | ',' | '*' |
            '/' | ':' | ';' | '<' | '=' | '>' | '?' | '@' | '[' | ']' | '^' | '_' | '`' | '{' |
            '|' | '}' | '~' | '\u{201C}' | '\u{201D}' | '\u{2033}' => (true, false),
            _ => (false, false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_from_str() {
        let token = Token::from_str("test", 0, 1);
        assert_eq!(token.term(), "test");
        assert_eq!(token.start_offset, 0);
        assert_eq!(token.position, 1);
    }

    #[test]
    fn test_token_debug() {
        let token = Token::from_str("test", 0, 1);
        assert_eq!(format!("{:?}", token), "test");
    }

    #[test]
    fn test_string_tokenizer_whitespace_filter() {
        let input = "hello world";
        let filter = WhitespaceFilter;
        let mut tokenizer = StringTokenizer::new(filter, input);

        let token1 = tokenizer.next().unwrap();
        assert_eq!(token1.term(), "hello");
        assert_eq!(token1.start_offset, 0);
        assert_eq!(token1.position, 0);

        let token2 = tokenizer.next().unwrap();
        assert_eq!(token2.term(), "world");
        assert_eq!(token2.start_offset, 6);
        assert_eq!(token2.position, 1);

        assert!(tokenizer.next().is_none());
    }

    #[test]
    fn test_string_tokenizer_default_filter() {
        let input = "hello, world!";
        let filter = DefaultFilter;
        let mut tokenizer = StringTokenizer::new(filter, input);
        
        let token1 = tokenizer.next().unwrap();
        assert_eq!(token1.term(), "hello");
        assert_eq!(token1.start_offset, 0);
        assert_eq!(token1.position, 0);
        
        let token2 = tokenizer.next().unwrap();
        assert_eq!(token2.term(), "world");
        assert_eq!(token2.start_offset, 7);
        assert_eq!(token2.position, 1);
        
        assert!(tokenizer.next().is_none());
    }

    #[test]
    fn test_whitespace_filter() {
        let filter = WhitespaceFilter;
        assert_eq!(filter.on_char(&' '), (true, false));
        assert_eq!(filter.on_char(&'a'), (false, false));
    }

    #[test]
    fn test_default_filter() {
        let filter = DefaultFilter;
        assert_eq!(filter.on_char(&' '), (true, false));
        assert_eq!(filter.on_char(&'!'), (true, false));
        assert_eq!(filter.on_char(&'a'), (false, false));
    }

    #[test]
    fn bench_whitespace_tokenizer() {
        let input = "The quick brown fox jumps over the lazy dog";
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let filter = WhitespaceFilter;
            let tokenizer = StringTokenizer::new(filter, input);
            let count = tokenizer.count();
            assert!(count > 0);
        }
        let elapsed = start.elapsed();
        println!(
            "Whitespace tokenizer (1000x): {:?} ({:.3}µs per tokenize)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }

    #[test]
    fn bench_default_tokenizer() {
        let input = "hello, world! this-is-a-test";
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let filter = DefaultFilter;
            let tokenizer = StringTokenizer::new(filter, input);
            let count = tokenizer.count();
            assert!(count > 0);
        }
        let elapsed = start.elapsed();
        println!(
            "Default tokenizer (1000x): {:?} ({:.3}µs per tokenize)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }

    #[test]
    fn bench_token_creation() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _token = Token::from_str("test_token", 0, 0);
        }
        let elapsed = start.elapsed();
        println!(
            "Token creation (10,000x): {:?} ({:.3}µs per token)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }
}