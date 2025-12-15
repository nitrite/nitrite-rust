//! Nitrite module for full-text search.
//!
//! This module provides the `TantivyFtsModule` that integrates the FTS indexer
//! with Nitrite's plugin system.

use nitrite::{
    common::{NitriteModule, NitritePlugin, PluginRegistrar},
    errors::NitriteResult,
    index::NitriteIndexer,
};

use crate::config::FtsConfig;
use crate::indexer::FtsIndexer;

/// Nitrite module for loading the FTS indexer.
///
/// Use this module to enable full-text search indexing in your Nitrite database.
///
/// ## Example
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
/// use nitrite_tantivy_fts::TantivyFtsModule;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Default configuration
/// let db = Nitrite::builder()
///     .load_module(TantivyFtsModule::default())
///     .open_or_create(None, None)?;
///
/// // Custom configuration
/// let db = Nitrite::builder()
///     .load_module(
///         TantivyFtsModule::with_config()
///             .index_writer_heap_size(100 * 1024 * 1024)  // 100 MB
///             .num_threads(4)
///             .build()
///     )
///     .open_or_create(None, None)?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct TantivyFtsModule {
    config: FtsConfig,
}

impl TantivyFtsModule {
    /// Creates a new builder for configuring the FTS module.
    ///
    /// # Returns
    ///
    /// A `TantivyFtsModuleBuilder` for fluent configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let module = TantivyFtsModule::with_config()
    ///     .index_writer_heap_size(100 * 1024 * 1024)
    ///     .build();
    /// ```
    #[inline]
    pub fn with_config() -> TantivyFtsModuleBuilder {
        TantivyFtsModuleBuilder::new()
    }

    /// Returns the configuration for this module.
    #[inline]
    pub fn config(&self) -> &FtsConfig {
        &self.config
    }
}


impl NitriteModule for TantivyFtsModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        Ok(vec![NitritePlugin::new(FtsIndexer::with_config(
            self.config.clone(),
        ))])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        plugin_registrar.register_indexer_plugin(NitriteIndexer::new(FtsIndexer::with_config(
            self.config.clone(),
        )))
    }
}

/// Builder for configuring a TantivyFtsModule.
///
/// Provides a fluent API for customizing FTS indexer parameters.
pub struct TantivyFtsModuleBuilder {
    config: FtsConfig,
}

impl TantivyFtsModuleBuilder {
    /// Creates a new builder with default configuration.
    #[inline]
    pub fn new() -> Self {
        Self {
            config: FtsConfig::default(),
        }
    }

    /// Sets the index writer heap size in bytes.
    ///
    /// Default: 50 MB
    #[inline]
    pub fn index_writer_heap_size(self, bytes: usize) -> Self {
        self.config.set_index_writer_heap_size(bytes);
        self
    }

    /// Sets the number of indexing threads.
    ///
    /// Default: 0 (auto-detect based on CPU cores)
    #[inline]
    pub fn num_threads(self, n: usize) -> Self {
        self.config.set_num_threads(n);
        self
    }

    /// Sets the maximum search result limit.
    ///
    /// Default: 10,000 documents
    #[inline]
    pub fn search_result_limit(self, limit: usize) -> Self {
        self.config.set_search_result_limit(limit);
        self
    }

    /// Builds the TantivyFtsModule with the configured settings.
    #[inline]
    pub fn build(self) -> TantivyFtsModule {
        TantivyFtsModule {
            config: self.config,
        }
    }
}

impl Default for TantivyFtsModuleBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fts_module_plugins() {
        let module = TantivyFtsModule::default();
        let plugins = module.plugins().unwrap();
        assert_eq!(plugins.len(), 1);
    }

    #[test]
    fn test_fts_module_default() {
        let module = TantivyFtsModule::default();
        assert_eq!(module.config().index_writer_heap_size(), 50 * 1024 * 1024);
    }

    #[test]
    fn test_fts_module_with_config() {
        let module = TantivyFtsModule::with_config()
            .index_writer_heap_size(100 * 1024 * 1024)
            .num_threads(4)
            .search_result_limit(5000)
            .build();

        assert_eq!(module.config().index_writer_heap_size(), 100 * 1024 * 1024);
        assert_eq!(module.config().num_threads(), 4);
        assert_eq!(module.config().search_result_limit(), 5000);
    }

    #[test]
    fn test_fts_module_builder_default() {
        let builder = TantivyFtsModuleBuilder::default();
        let module = builder.build();
        assert_eq!(module.config().index_writer_heap_size(), 50 * 1024 * 1024);
    }
}
