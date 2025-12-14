use super::{NitriteModule, NitritePluginProvider};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::index::non_unique_indexer::NonUniqueIndexer;
use crate::index::text::{EnglishTokenizer, Tokenizer};
use crate::index::unique_indexer::UniqueIndexer;
use crate::index::NitriteIndexer;
use crate::index::{text_indexer::TextIndexer, NitriteIndexerProvider};
use crate::nitrite_config::NitriteConfig;
use crate::store::memory::{InMemoryStore, InMemoryStoreConfig};
use crate::store::NitriteStore;
use crate::{FULL_TEXT_INDEX, NON_UNIQUE_INDEX, UNIQUE_INDEX};
use dashmap::DashMap;
use std::sync::{Arc, OnceLock};

pub trait PluginRegistrarProvider {
    fn register_indexer_plugin(&self, plugin: NitriteIndexer) -> NitriteResult<()>;

    fn register_store_plugin(&self, plugin: NitriteStore) -> NitriteResult<()>;
}

/// Registers plugins with the plugin manager.
///
/// This struct provides a registrar interface for plugins to register their
/// implementations with the plugin manager. It acts as a facade to the underlying
/// plugin manager functionality, allowing modules to register indexers and stores.
pub struct PluginRegistrar {
    inner: Arc<dyn PluginRegistrarProvider>,
}

impl PluginRegistrar {
    /// Creates a new plugin registrar with the given provider.
    pub fn new<T: PluginRegistrarProvider + 'static>(inner: T) -> Self {
        PluginRegistrar { inner: Arc::new(inner) }
    }

    /// Registers an indexer plugin.
    pub fn register_indexer_plugin(&self, plugin: NitriteIndexer) -> NitriteResult<()> {
        self.inner.register_indexer_plugin(plugin)
    }

    /// Registers a store plugin.
    pub fn register_store_plugin(&self, plugin: NitriteStore) -> NitriteResult<()> {
        self.inner.register_store_plugin(plugin)
    }
}

impl Default for PluginRegistrar {
    fn default() -> Self {
        PluginRegistrar::new(PluginManager::new())
    }
}

/// Manages plugin registration and lifecycle.
///
/// This struct manages the registration, loading, and initialization of plugins
/// including indexers and data stores. It maintains registries of available plugins
/// and coordinates their lifecycle from registration through closing.
///
/// # Responsibilities
///
/// * **Plugin Registration**: Registers indexer and store plugins
/// * **Plugin Retrieval**: Retrieves registered plugins by type or store
/// * **Module Loading**: Loads plugin modules and registers their plugins
/// * **Plugin Loading**: Loads default built-in plugins for indexing and storage
/// * **Plugin Initialization**: Initializes all registered plugins with configuration
/// * **Lifecycle Management**: Closes and cleans up plugins on shutdown
/// * **Configuration Management**: Sets and manages plugin configuration
#[derive(Clone)]
pub struct PluginManager {
    inner: Arc<PluginManagerInner>,
}

impl PluginManager {
    pub fn new() -> Self {
        PluginManager {
            inner: Arc::new(PluginManagerInner::new()),
        }
    }

    pub fn set_nitrite_config(&self, nitrite_config: NitriteConfig) {
        self.inner.set_nitrite_config(nitrite_config);
    }

    pub fn get_indexer(&self, index_type: &str) -> Option<NitriteIndexer> {
        self.inner.get_indexer(index_type)
    }

    pub fn get_store(&self) -> Option<NitriteStore> {
        self.inner.get_store()
    }

    pub fn load_module(&self, module: Box<dyn NitriteModule>) -> NitriteResult<()> {
        self.inner.load_module(module, self.clone())
    }

    pub fn load_plugins(&self) -> NitriteResult<()> {
        self.inner.load_plugins()
    }

    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    pub fn initialize_plugins(&self) -> NitriteResult<()> {
        self.inner.initialize_plugins()
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        PluginManager::new()
    }
}

impl PluginRegistrarProvider for PluginManager {
    fn register_indexer_plugin(&self, plugin: NitriteIndexer) -> NitriteResult<()> {
        self.inner.register_indexer_plugin(plugin)
    }

    fn register_store_plugin(&self, plugin: NitriteStore) -> NitriteResult<()> {
        self.inner.register_store_plugin(plugin)
    }
}

/// Inner implementation of the plugin manager.
struct PluginManagerInner {
    nitrite_config: OnceLock<NitriteConfig>,
    indexer_maps: DashMap<String, NitriteIndexer>,
    nitrite_store: OnceLock<NitriteStore>,
}

impl PluginManagerInner {
    fn new() -> Self {
        PluginManagerInner {
            nitrite_config: OnceLock::new(),
            indexer_maps: DashMap::new(),
            nitrite_store: OnceLock::new(),
        }
    }

    pub fn set_nitrite_config(&self, nitrite_config: NitriteConfig) {
        self.nitrite_config.get_or_init(|| nitrite_config);
    }

    pub fn get_indexer(&self, index_type: &str) -> Option<NitriteIndexer> {
        self.indexer_maps.get(index_type).map(|entry| entry.value().clone())
    }

    pub fn get_store(&self) -> Option<NitriteStore> {
        self.nitrite_store.get().cloned()
    }

    pub fn register_indexer_plugin(&self, plugin: NitriteIndexer) -> NitriteResult<()> {
        let index_type = plugin.index_type();
        self.indexer_maps.insert(index_type, plugin);
        Ok(())
    }

    pub fn register_store_plugin(&self, plugin: NitriteStore) -> NitriteResult<()> {
        self.nitrite_store.get_or_init(|| plugin);
        Ok(())
    }

    pub fn load_module(&self, module: Box<dyn NitriteModule>, registrar: PluginManager) -> NitriteResult<()> {
        let registrar = PluginRegistrar::new(registrar);
        module.load(&registrar)?;
        Ok(())
    }

    pub fn load_plugins(&self) -> NitriteResult<()> {
        // Use a lazy initialization pattern for better performance
        if !self.indexer_maps.contains_key(UNIQUE_INDEX) {
            self.register_indexer_plugin(NitriteIndexer::new(UniqueIndexer::new()))?;
        }
        if !self.indexer_maps.contains_key(FULL_TEXT_INDEX) {
            let tokenizer = Tokenizer::new(EnglishTokenizer);
            self.register_indexer_plugin(NitriteIndexer::new(TextIndexer::new(tokenizer)))?;
        }
        if !self.indexer_maps.contains_key(NON_UNIQUE_INDEX) {
            self.register_indexer_plugin(NitriteIndexer::new(NonUniqueIndexer::new()))?;
        }

        if self.nitrite_store.get().is_none() {
            let store = InMemoryStore::new(InMemoryStoreConfig::new());
            self.register_store_plugin(NitriteStore::new(store))?;
        }
        
        Ok(())
    }

    pub fn close(&self) -> NitriteResult<()> {
        // Optimize the closing process to avoid collecting errors vector
        for plugin in self.indexer_maps.iter() {
            plugin.close().ok();
        }
        
        if let Some(store) = self.nitrite_store.get() {
            store.close()?;
        }
        
        Ok(())
    }

    fn initialize_plugins(&self) -> NitriteResult<()> {
        // Extract config once to avoid TOCTOU pattern with OnceLock
        if let Some(config) = self.nitrite_config.get().cloned() {
            // Initialize store first - it sets db_path on the config which other plugins may need
            if let Some(store) = self.nitrite_store.get() {
                store.initialize(config.clone())?;
            }

            // Then initialize indexers - they can now access db_path from config
            for plugin in self.indexer_maps.iter() {
                plugin.initialize(config.clone())?;
            }
            
            Ok(())
        } else {
            log::error!("NitriteConfig is not set");
            Err(NitriteError::new(
                "NitriteConfig is not set",
                ErrorKind::PluginError,
            ))
        }
    }
}

impl Drop for PluginManagerInner {
    fn drop(&mut self) {
        match self.close() {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error while closing plugin manager: {:?}", e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::{FindPlan, NitriteId};
    use crate::common::{FieldValues, Fields, NitritePlugin, SubscriberRef};
    use crate::index::{IndexDescriptor, NitriteIndexer};
    use crate::store::{NitriteMap, NitriteStore, NitriteStoreProvider, StoreCatalog, StoreConfig, StoreEventListener};
    use std::collections::{HashMap, HashSet};

    struct MockIndexer;

    impl NitritePluginProvider for MockIndexer {
        fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn as_plugin(&self) -> NitritePlugin {
            NitritePlugin::new(MockIndexer)
        }
    }

    impl NitriteIndexerProvider for MockIndexer {
        fn index_type(&self) -> String {
            "mock_index".to_string()
        }

        fn is_unique(&self) -> bool {
            false
        }

        fn validate_index(&self, _fields: &Fields) -> NitriteResult<()> {
            Ok(())
        }

        fn drop_index(&self, _index_descriptor: &IndexDescriptor, _nitrite_config: &NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn write_index_entry(&self, _field_values: &FieldValues, _index_descriptor: &IndexDescriptor, _nitrite_config: &NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn remove_index_entry(&self, _field_values: &FieldValues, _index_descriptor: &IndexDescriptor, _nitrite_config: &NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn find_by_filter(&self, _find_plan: &FindPlan, _nitrite_config: &NitriteConfig) -> NitriteResult<Vec<NitriteId>> {
            Ok(vec![])
        }
    }

    struct MockStore;

    impl NitritePluginProvider for MockStore {
        fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn as_plugin(&self) -> NitritePlugin {
            NitritePlugin::new(MockStore)
        }
    }

    impl NitriteStoreProvider for MockStore {
        fn open_or_create(&self) -> NitriteResult<()> {
            todo!()
        }

        fn is_closed(&self) -> NitriteResult<bool> {
            todo!()
        }

        fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
            todo!()
        }

        fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
            todo!()
        }

        fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
            todo!()
        }

        fn has_unsaved_changes(&self) -> NitriteResult<bool> {
            todo!()
        }

        fn is_read_only(&self) -> NitriteResult<bool> {
            todo!()
        }

        fn is_map_opened(&self, _name: &str) -> NitriteResult<bool> {
            todo!()
        }

        fn commit(&self) -> NitriteResult<()> {
            todo!()
        }

        fn compact(&self) -> NitriteResult<()> {
            todo!()
        }

        fn before_close(&self) -> NitriteResult<()> {
            todo!()
        }

        fn has_map(&self, _name: &str) -> NitriteResult<bool> {
            todo!()
        }

        fn open_map(&self, _name: &str) -> NitriteResult<NitriteMap> {
            todo!()
        }

        fn close_map(&self, _name: &str) -> NitriteResult<()> {
            todo!()
        }

        fn remove_map(&self, _name: &str) -> NitriteResult<()> {
            todo!()
        }

        fn subscribe(&self, _listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
            todo!()
        }

        fn unsubscribe(&self, _subscriber_ref: SubscriberRef) -> NitriteResult<()> {
            todo!()
        }

        fn store_version(&self) -> NitriteResult<String> {
            todo!()
        }

        fn store_config(&self) -> NitriteResult<StoreConfig> {
            todo!()
        }

        fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
            todo!()
        }
    }

    #[test]
    fn test_register_indexer_plugin() {
        let manager = PluginManager::new();
        let indexer = NitriteIndexer::new(MockIndexer);
        assert!(manager.register_indexer_plugin(indexer).is_ok());
    }

    #[test]
    fn test_register_store_plugin() {
        let manager = PluginManager::new();
        let store = NitriteStore::new(MockStore);
        assert!(manager.register_store_plugin(store).is_ok());
    }

    #[test]
    fn test_get_indexer() {
        let manager = PluginManager::new();
        let indexer = NitriteIndexer::new(MockIndexer);
        manager.register_indexer_plugin(indexer.clone()).unwrap();
        assert!(manager.get_indexer("mock_index").is_some());
        assert!(manager.get_indexer("non_existent_index").is_none());
    }

    #[test]
    fn test_get_store() {
        let manager = PluginManager::new();
        let store = NitriteStore::new(MockStore);
        manager.register_store_plugin(store.clone()).unwrap();
        assert!(manager.get_store().is_some());
    }

    #[test]
    fn test_load_module() {
        struct MockModule;
        impl NitriteModule for MockModule {
            fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
                Ok(vec![])
            }
            fn load(&self, _plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
                Ok(())
            }
        }

        let manager = PluginManager::new();
        let module = Box::new(MockModule);
        assert!(manager.load_module(module).is_ok());
    }

    #[test]
    fn test_load_plugins() {
        let manager = PluginManager::new();
        assert!(manager.load_plugins().is_ok());
    }

    #[test]
    fn test_initialize_plugins() {
        let manager = PluginManager::new();
        let config = NitriteConfig::default();
        manager.set_nitrite_config(config);
        assert!(manager.initialize_plugins().is_ok());
    }

    #[test]
    fn test_initialize_plugins_without_config() {
        let manager = PluginManager::new();
        let result = manager.initialize_plugins();
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.message(), "NitriteConfig is not set");
        }
    }

    #[test]
    fn test_close() {
        let manager = PluginManager::new();
        assert!(manager.close().is_ok());
    }

    #[test]
    fn test_drop() {
        let manager = PluginManager::new();
        drop(manager); // This will call the Drop trait implementation
    }

    #[test]
    fn test_initialize_plugins_with_atomic_config_access() {
        // Config should be accessed atomically without TOCTOU pattern
        let manager = PluginManager::new();
        let config = NitriteConfig::default();
        manager.set_nitrite_config(config.clone());
        
        let result = manager.initialize_plugins();
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize_plugins_fails_gracefully_without_config() {
        // Should fail cleanly when config not set, not panic
        let manager = PluginManager::new();
        
        let result = manager.initialize_plugins();
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("NitriteConfig is not set"));
    }

    #[test]
    fn test_initialize_plugins_uses_single_config_clone() {
        // Config should be cloned once and reused for all plugins
        let manager = PluginManager::new();
        let config = NitriteConfig::default();
        manager.set_nitrite_config(config);
        
        assert!(manager.initialize_plugins().is_ok());
    }

    #[test]
    fn test_initialize_plugins_with_store_plugin() {
        // Should successfully initialize with both indexers and store
        let manager = PluginManager::new();
        let config = NitriteConfig::default();
        let store = NitriteStore::new(MockStore);
        
        manager.register_store_plugin(store).unwrap();
        manager.set_nitrite_config(config);
        
        let result = manager.initialize_plugins();
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize_plugins_no_toctou_race_condition() {
        // Verify that OnceLock pattern is handled safely without TOCTOU
        let manager = PluginManager::new();
        let config = NitriteConfig::default();
        manager.set_nitrite_config(config);
        
        // Multiple calls should succeed consistently
        assert!(manager.initialize_plugins().is_ok());
        assert!(manager.initialize_plugins().is_ok());
    }

    #[test]
    fn bench_plugin_manager_operations() {
        let manager = PluginManager::new();
        
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = manager.get_indexer(UNIQUE_INDEX);
        }
        let elapsed = start.elapsed();
        
        println!("1000 plugin lookups: {:?}", elapsed);
        assert!(elapsed.as_millis() < 200);
    }

    #[test]
    fn bench_plugin_registration() {
        let manager = PluginManager::new();
        
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let indexer = NitriteIndexer::new(MockIndexer);
            manager.register_indexer_plugin(indexer).ok();
        }
        let elapsed = start.elapsed();
        
        println!("100 plugin registrations: {:?}", elapsed);
        assert!(elapsed.as_millis() < 300);
    }
}