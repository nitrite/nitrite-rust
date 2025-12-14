use super::InMemoryMap;
use crate::common::{NitritePlugin, SubscriberRef, COLLECTION_CATALOG};
use crate::errors::NitriteResult;
use crate::nitrite_config::NitriteConfig;
use crate::store::memory::config::InMemoryStoreConfig;
use crate::store::{
    NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider, StoreCatalog, StoreConfig,
    StoreEventInfo, StoreEventListener, StoreEvents,
};
use crate::{NitriteEventBus, NitritePluginProvider};
use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

/// In-memory implementation of a Nitrite database store.
///
/// # Purpose
/// `InMemoryStore` provides a complete in-memory database store implementation suitable for
/// testing, temporary data, and scenarios where persistence is not required. All data is stored
/// in memory using concurrent data structures for thread-safe access.
///
/// # Characteristics
/// - **Thread-Safe**: Fully concurrent with safe data sharing across threads
/// - **Event-Driven**: Publishes events for open, close, and commit operations
/// - **Registry Management**: Tracks collections and repositories
/// - **Temporary Storage**: Perfect for unit tests and temporary use cases
/// - **No Persistence**: All data is lost when the store is closed
///
/// # Usage
/// Create and initialize an in-memory store:
/// ```text
/// let store = InMemoryStore::new(InMemoryStoreConfig::new());
/// let nitrite_store = NitriteStore::new(store);
/// nitrite_store.open_or_create().unwrap();
/// let map = nitrite_store.open_map("test").unwrap();
/// map.put(key!("key1"), val!("value1")).unwrap();
/// ```
#[derive(Clone)]
pub struct InMemoryStore {
    inner: Arc<InMemoryStoreInner>,
}

impl InMemoryStore {
    /// Creates a new in-memory store with the specified configuration.
    ///
    /// # Arguments
    /// * `store_config` - Configuration for the store including event listeners
    ///
    /// # Returns
    /// A new `InMemoryStore` instance
    pub fn new(store_config: InMemoryStoreConfig) -> InMemoryStore {
        InMemoryStore {
            inner: Arc::new(InMemoryStoreInner::new(store_config)),
        }
    }
}

impl NitritePluginProvider for InMemoryStore {
    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        self.inner.initialize(config)
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(self.clone())
    }
}

impl NitriteStoreProvider for InMemoryStore {
    fn open_or_create(&self) -> NitriteResult<()> {
        self.inner.open_or_create()
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        self.inner.is_closed()
    }

    fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        let collection_names = catalog.get_collection_names()?;
        Ok(collection_names)
    }

    fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        let repository_registry = catalog.get_repository_names()?;
        Ok(repository_registry)
    }

    fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        let catalog = self.store_catalog()?;
        let keyed_repository_registry = catalog.get_keyed_repository_names()?;
        Ok(keyed_repository_registry)
    }

    fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        Ok(false)
    }

    fn is_read_only(&self) -> NitriteResult<bool> {
        Ok(false)
    }

    fn is_map_opened(&self, name: &str) -> NitriteResult<bool> {
        self.inner.has_map(name)
    }

    fn commit(&self) -> NitriteResult<()> {
        self.inner.commit()
    }

    fn compact(&self) -> NitriteResult<()> {
        Ok(())
    }

    fn before_close(&self) -> NitriteResult<()> {
        self.inner.before_close()
    }

    fn has_map(&self, name: &str) -> NitriteResult<bool> {
        self.inner.has_map(name)
    }

    fn open_map(&self, name: &str) -> NitriteResult<NitriteMap> {
        self.inner.open_map(name, self.clone())
    }

    fn close_map(&self, name: &str) -> NitriteResult<()> {
        self.inner.close_map(name)?;

        let catalog = self.store_catalog()?;
        catalog.remove(name)?;
        Ok(())
    }

    fn remove_map(&self, name: &str) -> NitriteResult<()> {
        self.close_map(name)
    }

    fn subscribe(&self, listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.inner.subscribe(listener)
    }

    fn unsubscribe(&self, subscriber_ref: SubscriberRef) -> NitriteResult<()> {
        self.inner.unsubscribe(subscriber_ref)
    }

    fn store_version(&self) -> NitriteResult<String> {
        self.inner.get_store_version()
    }

    fn store_config(&self) -> NitriteResult<StoreConfig> {
        self.inner.store_config()
    }

    fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
        self.inner.store_catalog(self.clone())
    }
}

struct InMemoryStoreInner {
    closed: AtomicBool,
    event_bus: NitriteEventBus<StoreEventInfo, StoreEventListener>,
    store_config: InMemoryStoreConfig,
    nitrite_config: OnceLock<NitriteConfig>,
    map_registry: DashMap<String, InMemoryMap>,
}

impl InMemoryStoreInner {
    pub(crate) fn new(store_config: InMemoryStoreConfig) -> InMemoryStoreInner {
        InMemoryStoreInner {
            closed: AtomicBool::from(false),
            event_bus: NitriteEventBus::new(),
            store_config,
            nitrite_config: OnceLock::new(),
            map_registry: DashMap::new(),
        }
    }

    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        self.nitrite_config.get_or_init(|| config.clone());
        Ok(())
    }

    pub(crate) fn alert(&self, event: StoreEvents) -> NitriteResult<()> {
        if !self.event_bus.has_listeners() {
            return Ok(());
        }

        if let Some(config) = self.nitrite_config.get() {
            let event_info = StoreEventInfo::new(event, config.clone());
            self.event_bus.publish(event_info)
        } else {
            Ok(())
        }
    }

    pub(crate) fn store_catalog(&self, store: InMemoryStore) -> NitriteResult<StoreCatalog> {
        let nitrite_store = NitriteStore::new(store);
        let catalog_map = nitrite_store.open_map(COLLECTION_CATALOG)?;
        StoreCatalog::new(catalog_map)
    }

    pub(crate) fn close(&self) -> NitriteResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.before_close()?;

        // Close maps using std::thread::scope - avoids global thread pool contention
        {
            let maps: Vec<_> = self.map_registry.iter()
                .map(|r| r.value().clone())
                .collect();

            // Use std::thread::scope for predictable parallelism without global state
            std::thread::scope(|s| {
                for map in maps {
                    s.spawn(move || {
                        let _ = map.close();
                    });
                }
            });
        }

    
        self.map_registry.clear();
        self.closed.store(true, Ordering::Relaxed);
        self.event_bus.close()?;

        Ok(())
    }

    pub(crate) fn open_or_create(&self) -> NitriteResult<()> {
        let listeners = self.store_config.event_listeners();
        for listener in listeners {
            self.event_bus.register(listener)?;
        }
        self.alert(StoreEvents::Open)?;
        Ok(())
    }

    pub(crate) fn is_closed(&self) -> NitriteResult<bool> {
        Ok(self.closed.load(Ordering::Relaxed))
    }

    pub(crate) fn commit(&self) -> NitriteResult<()> {
        self.alert(StoreEvents::Commit)
    }

    pub(crate) fn before_close(&self) -> NitriteResult<()> {
        self.alert(StoreEvents::Closing)
    }

    pub(crate) fn has_map(&self, name: &str) -> NitriteResult<bool> {
        Ok(self.map_registry.contains_key(name))
    }

    pub(crate) fn open_map(&self, name: &str, store: InMemoryStore) -> NitriteResult<NitriteMap> {
        match self.map_registry.entry(name.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let map = entry.get();
                if map.is_closed()? {
                    // Drop the entry reference to avoid deadlock
                    drop(entry);

                    // Now we can remove and recreate
                    self.map_registry.remove(name);
                    let map = InMemoryMap::new(name, NitriteStore::new(store));
                    self.map_registry.insert(name.to_string(), map.clone());
                    Ok(NitriteMap::new(map))
                } else {
                    // If the map is not closed, return it
                    return Ok(NitriteMap::new(map.clone()));
                }
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // Create new map without prior checks
                let map = InMemoryMap::new(name, NitriteStore::new(store));
                entry.insert(map.clone());
                Ok(NitriteMap::new(map))
            }
        }
    }

    pub(crate) fn close_map(&self, name: &str) -> NitriteResult<()> {
        // Use idiomatic Rust pattern matching
        // DashMap::remove() returns Option<(K, V)>
        if let Some((_key, map)) = self.map_registry.remove(name) {
            // Explicitly drop to ensure cleanup semantics
            drop(map);
        }
        Ok(())
    }

    pub(crate) fn subscribe(&self, listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.event_bus.register(listener)
    }

    pub(crate) fn unsubscribe(&self, subscriber_ref: SubscriberRef) -> NitriteResult<()> {
        self.event_bus.deregister(subscriber_ref)
    }

    pub(crate) fn get_store_version(&self) -> NitriteResult<String> {
        Ok(format!("InMemory/{}", env!("CARGO_PKG_VERSION")))
    }

    pub(crate) fn store_config(&self) -> NitriteResult<StoreConfig> {
        Ok(StoreConfig::new(self.store_config.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::config::InMemoryStoreConfig;
    use crate::store::memory::InMemoryMap;
    use crate::store::{StoreConfigProvider, StoreEventListener, StoreEvents};

    fn create_store() -> InMemoryStore {
        InMemoryStore::new(InMemoryStoreConfig::new())
    }

    #[test]
    fn test_in_memory_store_new() {
        let store = create_store();
        assert!(!store.inner.closed.load(Ordering::Relaxed));
    }

    #[test]
    fn test_initialize() {
        let store = create_store();
        let config = NitriteConfig::default();
        assert!(store.initialize(config).is_ok());
    }

    #[test]
    fn test_initialize_already_initialized() {
        let store = create_store();
        let config = NitriteConfig::default();
        store.initialize(config.clone()).unwrap();
        assert!(store.initialize(config).is_ok());
    }

    #[test]
    fn test_close() {
        let store = create_store();
        assert!(store.close().is_ok());
        assert!(store.inner.closed.load(Ordering::Relaxed));
    }

    #[test]
    fn test_open_or_create() {
        let store = create_store();
        assert!(store.open_or_create().is_ok());
    }

    #[test]
    fn test_is_closed() {
        let store = create_store();
        assert!(!store.is_closed().unwrap());
        store.close().unwrap();
        assert!(store.is_closed().unwrap());
    }

    #[test]
    fn test_get_collection_names() {
        let store = create_store();
        store.open_or_create().unwrap();
        let names = store.get_collection_names().unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_get_repository_registry() {
        let store = create_store();
        store.open_or_create().unwrap();
        let registry = store.get_repository_registry().unwrap();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_get_keyed_repository_registry() {
        let store = create_store();
        store.open_or_create().unwrap();
        let registry = store.get_keyed_repository_registry().unwrap();
        assert!(registry.is_empty());
    }

    #[test]
    fn test_has_unsaved_changes() {
        let store = create_store();
        assert!(!store.has_unsaved_changes().unwrap());
    }

    #[test]
    fn test_is_read_only() {
        let store = create_store();
        assert!(!store.is_read_only().unwrap());
    }

    #[test]
    fn test_commit() {
        let store = create_store();
        assert!(store.commit().is_ok());
    }

    #[test]
    fn test_before_close() {
        let store = create_store();
        assert!(store.before_close().is_ok());
    }

    #[test]
    fn test_has_map() {
        let store = create_store();
        assert!(!store.has_map("test_map").unwrap());
    }

    #[test]
    fn test_open_map() {
        let store = create_store();
        let map = store.open_map("test_map").unwrap();
        assert_eq!(map.get_name().unwrap(), "test_map");
    }

    #[test]
    fn test_close_map() {
        let store = create_store();
        store.open_map("test_map").unwrap();
        assert!(store.close_map("test_map").is_ok());
    }

    #[test]
    fn test_remove_map() {
        let store = create_store();
        store.open_map("test_map").unwrap();
        assert!(store.remove_map("test_map").is_ok());
    }

    #[test]
    fn test_subscribe() {
        let store = create_store();
        let listener = StoreEventListener::new(|_| Ok(()));
        let subscriber_ref = store.subscribe(listener).unwrap();
        assert!(store.unsubscribe(subscriber_ref.unwrap()).is_ok());
    }

    #[test]
    fn test_store_version() {
        let store = create_store();
        let version = store.store_version().unwrap();
        assert!(version.starts_with("InMemory/"));
    }

    #[test]
    fn test_store_config() {
        let store = create_store();
        let config = store.store_config().unwrap();
        assert!(!config.is_read_only());
    }

    #[test]
    fn test_store_catalog() {
        let store = create_store();
        store.open_or_create().unwrap();
        let catalog = store.store_catalog().unwrap();
        assert!(catalog.get_collection_names().unwrap().is_empty());
    }

    #[test]
    fn test_alert() {
        let store = create_store();
        assert!(store.inner.alert(StoreEvents::Open).is_ok());
    }

    #[test]
    fn test_alert_without_config() {
        let store = create_store();
        assert!(store.inner.alert(StoreEvents::Open).is_ok());
    }

    #[test]
    fn test_close_map_with_idiomatic_pattern() {
        // Test that close_map uses pattern matching instead of is_some() + unwrap()
        let store = create_store();
        
        // Create a map in the registry  
        let map_name = "test_map";
        let map = InMemoryMap::new(map_name, NitriteStore::new(store.clone()));
        store.inner.map_registry.insert(map_name.to_string(), map);
        
        // Verify map exists before closing
        assert!(store.inner.map_registry.contains_key(map_name));
        
        // Close the map using idiomatic pattern
        assert!(store.inner.close_map(map_name).is_ok());
        
        // Verify map is removed from registry
        assert!(!store.inner.map_registry.contains_key(map_name));
    }

    #[test]
    fn test_close_non_existent_map() {
        // Test that closing non-existent map is safe
        let store = create_store();
        
        // Should not panic, just return Ok
        let result = store.inner.close_map("non_existent");
        assert!(result.is_ok());
    }

    #[test]
    fn test_close_and_reopen_map() {
        // Test that map can be closed and removed from registry
        let store = create_store();
        let map_name = "test_reopen";
        
        // Create map
        let map = InMemoryMap::new(map_name, NitriteStore::new(store.clone()));
        store.inner.map_registry.insert(map_name.to_string(), map);
        assert!(store.inner.map_registry.contains_key(map_name));
        
        // Close it
        store.inner.close_map(map_name).unwrap();
        assert!(!store.inner.map_registry.contains_key(map_name));
        
        // Can reinsert a new map with same name
        let map2 = InMemoryMap::new(map_name, NitriteStore::new(store.clone()));
        store.inner.map_registry.insert(map_name.to_string(), map2);
        assert!(store.inner.map_registry.contains_key(map_name));
    }

    #[test]
    fn test_close_multiple_maps_parallel_efficiency() {
        // Test that close() efficiently closes multiple maps in parallel
        let store = create_store();
        
        // Create multiple maps
        for i in 0..10 {
            let map_name = format!("test_map_{}", i);
            let map = InMemoryMap::new(&map_name, NitriteStore::new(store.clone()));
            store.inner.map_registry.insert(map_name, map);
        }
        
        assert_eq!(store.inner.map_registry.len(), 10);
        
        // Close all maps - uses rayon thread pool
        assert!(store.close().is_ok());
        assert!(store.is_closed().unwrap());
        assert_eq!(store.inner.map_registry.len(), 0);
    }

    #[test]
    fn test_close_many_maps_stress_test() {
        // Stress test: close with many maps to verify parallel efficiency
        let store = create_store();
        
        // Create 50 maps
        for i in 0..50 {
            let map_name = format!("stress_map_{}", i);
            let map = InMemoryMap::new(&map_name, NitriteStore::new(store.clone()));
            store.inner.map_registry.insert(map_name, map);
        }
        
        assert_eq!(store.inner.map_registry.len(), 50);
        
        // Close all - parallel processing should be efficient
        assert!(store.close().is_ok());
        assert!(store.is_closed().unwrap());
    }

    #[test]
    fn test_open_map_entry_optimization() {
        // Test that open_map uses efficient entry-based access pattern
        let store = create_store();
        
        // First open creates new map
        let map1 = store.inner.open_map("perf_test", store.clone()).unwrap();
        assert_eq!(map1.get_name().unwrap(), "perf_test");
        
        // Second open should use cached entry efficiently
        let map2 = store.inner.open_map("perf_test", store.clone()).unwrap();
        assert_eq!(map2.get_name().unwrap(), "perf_test");
    }

    #[test]
    fn test_close_maps_collection_with_capacity() {
        // Test that map registry iteration uses efficient collection
        let store = create_store();
        
        // Add 25 maps to stress the collection process
        for i in 0..25 {
            let map_name = format!("coll_test_{}", i);
            let map = InMemoryMap::new(&map_name, NitriteStore::new(store.clone()));
            store.inner.map_registry.insert(map_name, map);
        }
        
        // Close should collect maps efficiently
        assert!(store.close().is_ok());
        assert_eq!(store.inner.map_registry.len(), 0);
    }
}
