use nitrite::{common::{NitriteModule, NitritePlugin, PluginRegistrar}, errors::NitriteResult, index::NitriteIndexer};

use crate::SpatialIndexer;

/// Nitrite module for loading the spatial indexer.
///
/// Use this module to enable spatial indexing in your Nitrite database.
///
/// ## Example
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
/// use nitrite_spatial::SpatialModule;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Nitrite::builder()
///     .load_module(SpatialModule)
///     .open_or_create(None, None)?;
/// # Ok(())
/// # }
/// ```
pub struct SpatialModule;

impl Default for SpatialModule {
    fn default() -> Self {
        Self
    }
}

impl NitriteModule for SpatialModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        Ok(vec![NitritePlugin::new(SpatialIndexer::new())])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        plugin_registrar.register_indexer_plugin(NitriteIndexer::new(SpatialIndexer::new()))
    }
}