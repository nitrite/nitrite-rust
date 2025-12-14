use super::plugin_manager::PluginRegistrar;
use crate::errors::NitriteResult;
use crate::nitrite_config::NitriteConfig;
use std::ops::Deref;
use std::sync::Arc;

/// Contract for implementing Nitrite plugins.
///
/// # Purpose
/// Defines the interface for plugins that integrate with Nitrite database. Plugins are
/// responsible for providing functionality such as storage backends, indexing, transactions,
/// and other extensions to the core database.
///
/// # Trait Methods
/// - `initialize()`: Initializes the plugin with database configuration
/// - `close()`: Cleanly closes the plugin and releases resources
/// - `as_plugin()`: Returns a polymorphic wrapper of this plugin
///
/// # Thread Safety
/// Implementations must be `Send + Sync` for safe concurrent use.
///
/// # Usage
/// Implemented by storage adapters (FjallStore, InMemoryStore), indexers (SpatialIndexer),
/// and other database extensions. The plugin is initialized when the database starts
/// and closed when the database shuts down.
///
/// # Example Implementations
/// - FjallStore: Persistent storage backend using Fjall
/// - InMemoryStore: Ephemeral in-memory storage backend
/// - SpatialIndexer: Spatial indexing capabilities
/// - TransactionStore: Transaction support
pub trait NitritePluginProvider: Send + Sync {
    /// Initializes the plugin with the provided configuration.
    ///
    /// # Arguments
    /// * `config` - Database configuration containing initialization parameters.
    ///
    /// # Returns
    /// `Ok(())` on successful initialization, or an error if initialization fails.
    ///
    /// # Behavior
    /// Called once during database startup. Plugin should use config to initialize
    /// internal state, open connections, load resources, and validate preconditions.
    /// If this method returns an error, database startup fails.
    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()>;

    /// Closes the plugin and releases all resources.
    ///
    /// # Returns
    /// `Ok(())` on successful closure, or an error if closing fails.
    ///
    /// # Behavior
    /// Called during database shutdown. Plugin should gracefully close connections,
    /// flush pending operations, release locks, and clean up resources. This method
    /// should be idempotent (safe to call multiple times).
    fn close(&self) -> NitriteResult<()>;

    /// Returns a polymorphic wrapper of this plugin.
    ///
    /// # Returns
    /// A `NitritePlugin` containing this implementation wrapped in Arc.
    ///
    /// # Behavior
    /// Used internally for plugin registration and management. Implementations typically
    /// call `NitritePlugin::new(self.clone())`.
    fn as_plugin(&self) -> NitritePlugin;
}

/// Contract for modules that provide plugins to Nitrite.
///
/// # Purpose
/// Defines the interface for modules that bundle and provide plugins to extend
/// Nitrite's functionality. Modules are responsible for discovering plugins,
/// registering them with the plugin system, and managing their lifecycle.
///
/// # Trait Methods
/// - `plugins()`: Returns the list of plugins provided by this module
/// - `load()`: Registers plugins with the plugin registrar
///
/// # Thread Safety
/// Implementations must be `Send + Sync` for safe concurrent use.
///
/// # Responsibilities
/// * **Plugin Discovery**: Enumerate all plugins provided by the module
/// * **Plugin Registration**: Register plugins through the PluginRegistrar
/// * **Initialization Coordination**: Ensure plugins are initialized in correct order
///
/// # Usage
/// Modules are loaded during database initialization to register their plugins.
/// Each module provides one or more related plugins grouped by functionality.
pub trait NitriteModule: Send + Sync {
    /// Returns the list of plugins provided by this module.
    ///
    /// # Returns
    /// `Ok(Vec<NitritePlugin>)` containing all plugins in this module, or an error.
    ///
    /// # Behavior
    /// Discovers and returns all plugins provided by this module. Plugins are returned
    /// as polymorphic wrappers (NitritePlugin). The returned plugins should be in
    /// dependency order (dependencies first).
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>>;

    /// Registers plugins with the plugin registrar.
    ///
    /// # Arguments
    /// * `plugin_registrar` - Registrar to register plugins with.
    ///
    /// # Returns
    /// `Ok(())` on successful registration, or an error if registration fails.
    ///
    /// # Behavior
    /// Called during database initialization. Module should use the PluginRegistrar
    /// to register all its plugins. The registrar manages plugin availability and
    /// dependency resolution.
    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()>;
}

/// Polymorphic wrapper around a Nitrite plugin implementation.
///
/// # Purpose
/// Provides type-erased, polymorphic access to any `NitritePluginProvider` implementation.
/// Uses `Arc` for thread-safe sharing and efficient reference-counted ownership.
///
/// # Characteristics
/// - **Type-erased**: Works with any `NitritePluginProvider` implementation
/// - **Thread-safe**: Arc enables safe concurrent access via Send + Sync
/// - **Cloneable**: Can be cloned to share across multiple parts of the system
/// - **Transparent**: Deref trait provides transparent access to plugin methods
///
/// # Usage
/// Created from a concrete plugin implementation via `NitritePlugin::new()`.
/// Used internally by the database for plugin management and lifecycle control.
/// Typically cloned and shared across multiple subsystems.
#[derive(Clone)]
pub struct NitritePlugin {
    inner: Arc<dyn NitritePluginProvider>,
}

impl NitritePlugin {
    /// Creates a new plugin wrapper from an implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete `NitritePluginProvider` implementation.
    ///
    /// # Returns
    /// A new `NitritePlugin` wrapping the implementation in an `Arc`.
    ///
    /// # Behavior
    /// Wraps the provided plugin implementation for polymorphic use. The inner
    /// implementation is shared via Arc, allowing efficient cloning and concurrent access.
    /// Commonly called from `as_plugin()` implementations.
    pub fn new<T: NitritePluginProvider + 'static>(inner: T) -> Self {
        NitritePlugin { inner: Arc::new(inner) }
    }
}

/// Transparent deref implementation for accessing plugin methods.
///
/// # Behavior
/// Allows `NitritePlugin` to be used transparently as `Arc<dyn NitritePluginProvider>`,
/// enabling direct access to plugin methods (initialize, close, as_plugin) without
/// explicit dereferencing.
impl Deref for NitritePlugin {
    type Target = Arc<dyn NitritePluginProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PluginManager;
    use crate::nitrite_config::NitriteConfig;
    use std::sync::Arc;

    struct MockPlugin;

    impl NitritePluginProvider for MockPlugin {
        fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn as_plugin(&self) -> NitritePlugin {
            NitritePlugin::new(MockPlugin)
        }
    }

    struct MockModule;

    impl NitriteModule for MockModule {
        fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
            Ok(vec![NitritePlugin::new(MockPlugin)])
        }

        fn load(&self, _plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
            Ok(())
        }
    }

    #[test]
    fn test_nitrite_plugin_initialize() {
        let plugin = NitritePlugin::new(MockPlugin);
        let config = NitriteConfig::default();
        assert!(plugin.initialize(config).is_ok());
    }

    #[test]
    fn test_nitrite_plugin_close() {
        let plugin = NitritePlugin::new(MockPlugin);
        assert!(plugin.close().is_ok());
    }

    #[test]
    fn test_nitrite_plugin_as_plugin() {
        let plugin = NitritePlugin::new(MockPlugin);
        let plugin_clone = plugin.as_plugin();
        assert_eq!(Arc::strong_count(&plugin.inner), Arc::strong_count(&plugin_clone.inner));
    }

    #[test]
    fn test_nitrite_module_plugins() {
        let module = MockModule;
        let plugins = module.plugins().unwrap();
        assert_eq!(plugins.len(), 1);
    }

    #[test]
    fn test_nitrite_module_load() {
        let module = MockModule;
        let plugin_registrar = PluginRegistrar::new(PluginManager::new());
        assert!(module.load(&plugin_registrar).is_ok());
    }
}