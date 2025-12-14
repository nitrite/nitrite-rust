//! FTS configuration module.
//!
//! This module provides configuration options for the Tantivy full-text search indexer.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Default index writer heap size: 50 MB
pub const DEFAULT_INDEX_WRITER_HEAP_MB: usize = 50;

/// Default number of threads: 0 (auto-detect based on CPU cores)
pub const DEFAULT_NUM_THREADS: usize = 0;

/// Default search result limit: 10,000 documents
pub const DEFAULT_SEARCH_RESULT_LIMIT: usize = 10_000;

/// FTS configuration wrapper.
///
/// A cloneable, thread-safe configuration holder for Tantivy FTS parameters.
///
/// # Example
///
/// ```rust,ignore
/// use nitrite_tantivy_fts::FtsConfig;
///
/// let config = FtsConfig::new()
///     .with_index_writer_heap_size(100 * 1024 * 1024)  // 100 MB
///     .with_num_threads(4)
///     .with_search_result_limit(5000);
/// ```
#[derive(Clone)]
pub struct FtsConfig {
    inner: Arc<FtsConfigInner>,
}

/// Internal configuration storage.
struct FtsConfigInner {
    /// Memory budget for index writer in bytes.
    index_writer_heap_size: AtomicUsize,

    /// Number of indexing threads (0 = auto-detect).
    num_threads: AtomicUsize,

    /// Maximum results returned per search.
    search_result_limit: AtomicUsize,
}

impl FtsConfig {
    /// Creates a new FTS configuration with default values.
    ///
    /// Defaults:
    /// - Index writer heap: 50 MB
    /// - Num threads: 0 (auto-detect)
    /// - Search result limit: 10,000
    #[inline]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(FtsConfigInner::new()),
        }
    }

    /// Returns the index writer heap size in bytes.
    #[inline]
    pub fn index_writer_heap_size(&self) -> usize {
        self.inner.index_writer_heap_size.load(Ordering::Relaxed)
    }

    /// Sets the index writer heap size in bytes.
    #[inline]
    pub fn set_index_writer_heap_size(&self, size: usize) {
        self.inner
            .index_writer_heap_size
            .store(size, Ordering::Relaxed);
    }

    /// Returns the index writer heap size in bytes.
    /// Builder-style method for chaining.
    #[inline]
    pub fn with_index_writer_heap_size(self, size: usize) -> Self {
        self.set_index_writer_heap_size(size);
        self
    }

    /// Returns the number of indexing threads.
    #[inline]
    pub fn num_threads(&self) -> usize {
        self.inner.num_threads.load(Ordering::Relaxed)
    }

    /// Sets the number of indexing threads.
    #[inline]
    pub fn set_num_threads(&self, n: usize) {
        self.inner.num_threads.store(n, Ordering::Relaxed);
    }

    /// Sets the number of indexing threads.
    /// Builder-style method for chaining.
    #[inline]
    pub fn with_num_threads(self, n: usize) -> Self {
        self.set_num_threads(n);
        self
    }

    /// Returns the search result limit.
    #[inline]
    pub fn search_result_limit(&self) -> usize {
        self.inner.search_result_limit.load(Ordering::Relaxed)
    }

    /// Sets the search result limit.
    #[inline]
    pub fn set_search_result_limit(&self, limit: usize) {
        self.inner
            .search_result_limit
            .store(limit, Ordering::Relaxed);
    }

    /// Sets the search result limit.
    /// Builder-style method for chaining.
    #[inline]
    pub fn with_search_result_limit(self, limit: usize) -> Self {
        self.set_search_result_limit(limit);
        self
    }
}

impl Default for FtsConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl FtsConfigInner {
    fn new() -> Self {
        Self {
            index_writer_heap_size: AtomicUsize::new(DEFAULT_INDEX_WRITER_HEAP_MB * 1024 * 1024),
            num_threads: AtomicUsize::new(DEFAULT_NUM_THREADS),
            search_result_limit: AtomicUsize::new(DEFAULT_SEARCH_RESULT_LIMIT),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fts_config_new() {
        let config = FtsConfig::new();
        assert_eq!(config.index_writer_heap_size(), 50 * 1024 * 1024);
        assert_eq!(config.num_threads(), 0);
        assert_eq!(config.search_result_limit(), 10_000);
    }

    #[test]
    fn test_fts_config_default() {
        let config = FtsConfig::default();
        assert_eq!(config.index_writer_heap_size(), 50 * 1024 * 1024);
    }

    #[test]
    fn test_fts_config_builder() {
        let config = FtsConfig::new()
            .with_index_writer_heap_size(100 * 1024 * 1024)
            .with_num_threads(4)
            .with_search_result_limit(5000);

        assert_eq!(config.index_writer_heap_size(), 100 * 1024 * 1024);
        assert_eq!(config.num_threads(), 4);
        assert_eq!(config.search_result_limit(), 5000);
    }

    #[test]
    fn test_fts_config_setters() {
        let config = FtsConfig::new();
        config.set_index_writer_heap_size(200 * 1024 * 1024);
        config.set_num_threads(8);
        config.set_search_result_limit(20000);

        assert_eq!(config.index_writer_heap_size(), 200 * 1024 * 1024);
        assert_eq!(config.num_threads(), 8);
        assert_eq!(config.search_result_limit(), 20000);
    }

    #[test]
    fn test_fts_config_clone() {
        let config1 = FtsConfig::new().with_num_threads(4);
        let config2 = config1.clone();

        assert_eq!(config2.num_threads(), 4);

        // Changes to config1 affect config2 (Arc-based sharing)
        config1.set_num_threads(8);
        assert_eq!(config2.num_threads(), 8);
    }
}
