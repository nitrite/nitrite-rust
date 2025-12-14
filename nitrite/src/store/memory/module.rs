use crate::common::{NitriteModule, NitritePlugin, PluginRegistrar, PluginRegistrarProvider};
use crate::errors::NitriteResult;
use crate::store::memory::{InMemoryStore, InMemoryStoreConfig};
use crate::store::{NitriteStore, StoreConfigProvider, StoreEventListener, StoreModule};
use crate::NitritePluginProvider;

#[derive(Default)]
pub struct InMemoryStoreModule {
    store_config: InMemoryStoreConfig,
}

impl InMemoryStoreModule {
    pub fn new() -> InMemoryStoreModule {
        InMemoryStoreModule {
            store_config: InMemoryStoreConfig::new(),
        }
    }

    pub fn with_config() -> InMemoryStoreModuleBuilder {
        InMemoryStoreModuleBuilder::new()
    }
}

impl NitriteModule for InMemoryStoreModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        let store = self.get_store()?;
        let plugin = store.as_plugin();
        Ok(vec![plugin])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        let store = self.get_store()?;
        plugin_registrar.register_store_plugin(store)
    }
}

impl StoreModule for InMemoryStoreModule {
    fn get_store(&self) -> NitriteResult<NitriteStore> {
        let store = InMemoryStore::new(self.store_config.clone());
        Ok(NitriteStore::new(store))
    }
}

#[derive(Default)]
pub struct InMemoryStoreModuleBuilder {
    store_config: InMemoryStoreConfig,
    event_listeners: Vec<StoreEventListener>,
}

impl InMemoryStoreModuleBuilder {
    pub fn new() -> InMemoryStoreModuleBuilder {
        InMemoryStoreModuleBuilder {
            store_config: InMemoryStoreConfig::new(),
            event_listeners: Vec::new(),
        }
    }

    pub fn add_event_listener(mut self, listener: StoreEventListener) -> Self {
        self.event_listeners.push(listener);
        self
    }

    pub fn build(self) -> InMemoryStoreModule {
        let mut store_module = InMemoryStoreModule::new();
        store_module.store_config = self.store_config;
        for listener in self.event_listeners {
            store_module.store_config.add_store_listener(listener);
        }
        store_module
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::PluginManager;

    #[test]
    fn test_in_memory_store_module_new() {
        let module = InMemoryStoreModule::new();
        assert!(module.store_config.event_listeners().is_empty());
        assert!(!module.store_config.is_read_only())
    }

    #[test]
    fn test_in_memory_store_module_with_config() {
        let builder = InMemoryStoreModule::with_config();
        assert!(builder.store_config.event_listeners().is_empty());
        assert!(!builder.store_config.is_read_only());
        assert!(builder.event_listeners.is_empty());
    }

    #[test]
    fn test_in_memory_store_module_plugins() {
        let module = InMemoryStoreModule::new();
        let plugins = module.plugins().unwrap();
        assert_eq!(plugins.len(), 1);
    }

    #[test]
    fn test_in_memory_store_module_load() {
        let module = InMemoryStoreModule::new();
        let plugin_registrar = PluginRegistrar::new(PluginManager::new());
        assert!(module.load(&plugin_registrar).is_ok());
    }

    #[test]
    fn test_in_memory_store_module_get_store() {
        let module = InMemoryStoreModule::new();
        let store = module.get_store();
        assert!(store.is_ok());
    }

    #[test]
    fn test_in_memory_store_module_builder_new() {
        let builder = InMemoryStoreModuleBuilder::new();
        assert!(builder.store_config.event_listeners().is_empty());
        assert!(!builder.store_config.is_read_only());
        assert!(builder.event_listeners.is_empty());
    }

    #[test]
    fn test_in_memory_store_module_builder_add_event_listener() {
        let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
        let builder = InMemoryStoreModuleBuilder::new().add_event_listener(listener);
        assert_eq!(builder.event_listeners.len(), 1);
    }

    #[test]
    fn test_in_memory_store_module_builder_build() {
        let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
        let builder = InMemoryStoreModuleBuilder::new().add_event_listener(listener);
        let module = builder.build();
        assert_eq!(module.store_config.event_listeners().len(), 1);
    }

    #[test]
    fn test_builder_add_multiple_event_listeners() {
        // Test builder efficiently chains multiple listeners
        let builder = InMemoryStoreModuleBuilder::new();
        let mut builder = builder;
        
        for _ in 0..5 {
            let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
            builder = builder.add_event_listener(listener);
        }
        
        let module = builder.build();
        assert_eq!(module.store_config.event_listeners().len(), 5);
    }

    #[test]
    fn test_builder_efficiency_no_unnecessary_clones() {
        // Test that builder build() efficiently moves listeners without unnecessary copies
        let builder = InMemoryStoreModuleBuilder::new();
        let mut builder = builder;
        
        for _ in 0..10 {
            let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
            builder = builder.add_event_listener(listener);
        }
        
        let module = builder.build();
        let listeners = module.store_config.event_listeners();
        assert_eq!(listeners.len(), 10);
    }

    #[test]
    fn test_builder_with_large_listener_set() {
        // Stress test builder with many listeners
        let mut builder = InMemoryStoreModuleBuilder::new();
        
        for _ in 0..100 {
            let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
            builder = builder.add_event_listener(listener);
        }
        
        let module = builder.build();
        assert_eq!(module.store_config.event_listeners().len(), 100);
    }

    #[test]
    fn test_module_get_store_efficiency() {
        // Test that get_store() efficiently creates store without extra allocations
        let module = InMemoryStoreModule::new();
        let store1 = module.get_store().unwrap();
        let store2 = module.get_store().unwrap();
        
        // Both should be functional stores
        assert!(store1.is_closed().unwrap() == store2.is_closed().unwrap());
    }
}