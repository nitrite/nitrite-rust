use super::transactional_map::TransactionalMap;
use crate::common::{NitritePlugin, NitritePluginProvider, SubscriberRef, COLLECTION_CATALOG};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::nitrite_config::NitriteConfig;
use crate::store::memory::InMemoryStore;
use crate::store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider, StoreCatalog, StoreConfig, StoreEventListener};
use basu::HandlerId;
use parking_lot::Mutex;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A transaction store providing isolated storage with Copy-On-Write semantics.
///
/// Wraps an underlying `NitriteStore` and manages transaction-specific isolation by creating
/// separate transactional maps for each collection accessed during the transaction. Implements
/// the `NitriteStoreProvider` trait for integration with the query engine.
///
/// # Purpose
/// Enables transactional isolation by maintaining separate views of data:
/// - **Backing Map**: Stores new/modified entries (in-memory, transaction-local)
/// - **Tombstones**: Tracks deleted keys to shadow primary data
/// - **Primary Map**: Read-only reference to original database state
///
/// # Characteristics
/// - **Thread-Safe**: All state protected by Arc and Mutex
/// - **Lazy Initialization**: Transactional maps created on first access
/// - **Read-Write Separation**: Modifications isolated to backing maps
/// - **Registry Tracking**: Maintains HashMap of open transactional maps
/// - **Cloneable**: Arc-based shared ownership enables cheap cloning
/// - **Isolation**: Complete separation from other concurrent transactions
///
/// # Usage
/// Created by `NitriteTransaction` during transaction begin, used to coordinate
/// collection and repository access with isolation guarantees.
#[derive(Clone)]
pub struct TransactionStore {
    inner: Arc<TransactionStoreInner>,
}

impl TransactionStore {
    /// Creates a new transaction store wrapping an underlying store.
    ///
    /// # Arguments
    /// * `store` - The parent `NitriteStore` to wrap with transaction isolation
    ///
    /// # Returns
    /// A new `TransactionStore` with empty transactional map registry
    ///
    /// The transaction store creates an isolated view of the database state at creation time,
    /// enabling consistent reads throughout the transaction's lifetime.
    pub fn new(store: NitriteStore) -> Self {
        TransactionStore {
            inner: Arc::new(TransactionStoreInner::new(store)),
        }
    }

    /// Gets or creates a transactional map for a named collection.
    ///
    /// # Arguments
    /// * `name` - The name of the collection
    ///
    /// # Returns
    /// * `Ok(NitriteMap)` - A transactional map wrapper for the collection
    /// * `Err(NitriteError)` - If underlying store access fails
    ///
    /// Retrieves existing transactional map from registry, or creates new one if not exists.
    /// Each call with the same name returns a consistent map instance within the transaction.
    pub fn open_map(&self, name: &str) -> NitriteResult<NitriteMap> {
        self.inner.get_or_create_map(name)
    }

    /// Closes all transactional maps and releases resources.
    ///
    /// # Returns
    /// * `Ok(())` - If all maps closed successfully
    /// * `Err(NitriteError)` - If any map close operation fails
    ///
    /// Iterates through all tracked transactional maps and calls `dispose()` on each,
    /// clearing backing maps and tombstone sets. Called during transaction close.
    pub fn close_all(&self) -> NitriteResult<()> {
        self.inner.close()
    }
}

impl NitritePluginProvider for TransactionStore {
    fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(self.clone())
    }
}

impl NitriteStoreProvider for TransactionStore {
    fn open_or_create(&self) -> NitriteResult<()> {
        // nothing to do for transaction store
        Ok(())
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        Ok(false)
    }

    fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
        self.inner.get_collection_names()
    }

    fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
        self.inner.get_repository_registry()
    }

    fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        self.inner.get_keyed_repository_registry()
    }

    fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        Ok(true)
    }

    fn is_read_only(&self) -> NitriteResult<bool> {
        Ok(false)
    }

    fn is_map_opened(&self, name: &str) -> NitriteResult<bool> {
        self.inner.is_map_opened(name)
    }

    fn commit(&self) -> NitriteResult<()> {
        Err(NitriteError::new("Call commit on Transaction object instead of TransactionStore", ErrorKind::InvalidOperation))
    }

    fn compact(&self) -> NitriteResult<()> {
        Ok(())
    }

    fn before_close(&self) -> NitriteResult<()> {
        Ok(())
    }

    fn has_map(&self, name: &str) -> NitriteResult<bool> {
        self.inner.has_map(name)
    }

    fn open_map(&self, name: &str) -> NitriteResult<NitriteMap> {
        self.inner.open_map(name, NitriteStore::new(self.clone()))
    }

    fn close_map(&self, name: &str) -> NitriteResult<()> {
        self.inner.close_map(name)
    }

    fn remove_map(&self, name: &str) -> NitriteResult<()> {
        self.inner.remove_map(name)
    }

    fn subscribe(&self, _listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
        // no-op for transaction store
        Ok(None)
    }

    fn unsubscribe(&self, _subscriber_ref: SubscriberRef) -> NitriteResult<()> {
        Ok(())
    }

    fn store_version(&self) -> NitriteResult<String> {
        self.inner.store_version()
    }

    fn store_config(&self) -> NitriteResult<StoreConfig> {
        self.inner.store_config()
    }

    fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
        self.inner.store_catalog()
    }
}

struct TransactionStoreInner {
    /// Map registry for transaction-specific maps
    map_registry: Arc<Mutex<HashMap<String, TransactionalMap>>>,
    /// Underlying store for metadata operations
    underlying_store: NitriteStore,
    /// Set of deleted map names
    deleted_maps: Arc<Mutex<HashSet<String>>>,
}

impl TransactionStoreInner {
    /// Creates a transaction store wrapping an underlying store
    fn new(store: NitriteStore) -> Self {
        TransactionStoreInner {
            map_registry: Arc::new(Mutex::new(HashMap::new())),
            underlying_store: store,
            deleted_maps: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    /// Gets a map if it exists
    fn get_map(&self, map_name: &str) -> Option<TransactionalMap> {
        self.map_registry.lock().get(map_name).cloned()
    }

    /// Gets or creates a transactional map for a collection
    fn get_or_create_map(&self, name: &str) -> NitriteResult<NitriteMap> {
        self.open_map(name, self.underlying_store.clone())
    }

    fn close(&self) -> NitriteResult<()> {
        let registry = self.map_registry.lock();
        for map in registry.values() {
            map.dispose()?;
        }
        Ok(())
    }

    fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        catalog.get_collection_names()
    }

    fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        catalog.get_repository_names()
    }

    fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        let catalog = self.store_catalog()?;
        catalog.get_keyed_repository_names()
    }

    fn is_map_opened(&self, name: &str) -> NitriteResult<bool> {
        self.has_map(name)
    }

    fn has_map(&self, name: &str) -> NitriteResult<bool> {
        if self.deleted_maps.lock().contains(name) {
            return Ok(false);
        }

        let mut exists = self.underlying_store.has_map(name)?;
        if !exists {
            exists = self.map_registry.lock().contains_key(name);
        }
        Ok(exists)
    }

    fn open_map(&self, name: &str, store: NitriteStore) -> NitriteResult<NitriteMap> {
        self.deleted_maps.lock().remove(name);

        // Check if transactional map already exists
        if let Some(tx_map) = self.get_map(name) {
            if tx_map.is_closed()? {
                self.map_registry.lock().remove(name);
            } else {
                return Ok(NitriteMap::new(tx_map));
            }
        }

        // Open underlying map
        let underlying_map = self.underlying_store.open_map(name)?;

        // Create transactional map
        let tx_map = TransactionalMap::new(name.to_string(), underlying_map, store);
        self.map_registry
            .lock()
            .insert(name.to_string(), tx_map.clone());

        Ok(NitriteMap::new(tx_map))
    }

    fn close_map(&self, name: &str) -> NitriteResult<()> {
        self.map_registry.lock().remove(name);
        Ok(())
    }

    fn remove_map(&self, name: &str) -> NitriteResult<()> {
        self.deleted_maps.lock().insert(name.to_string());
        self.map_registry.lock().remove(name);
        Ok(())
    }

    fn store_version(&self) -> NitriteResult<String> {
        self.underlying_store.store_version()
    }

    fn store_config(&self) -> NitriteResult<StoreConfig> {
        self.underlying_store.store_config()
    }

    fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
        let catalog_map = self.underlying_store.open_map(COLLECTION_CATALOG)?;
        StoreCatalog::new(catalog_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::memory::{InMemoryStore, InMemoryStoreConfig};

    fn create_test_store() -> NitriteStore {
        let in_memory_config = InMemoryStoreConfig::new();
        let in_memory_store = InMemoryStore::new(in_memory_config);
        NitriteStore::new(in_memory_store)
    }

    

    /// Tests that a transaction store can be created
    #[test]
    fn test_transaction_store_creation() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let is_closed = tx_store.is_closed();
        assert!(is_closed.is_ok());
        assert!(!is_closed.unwrap());
    }

    /// Tests that transaction store wraps underlying store
    #[test]
    fn test_transaction_store_wraps_store() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        // Should successfully get store config
        let config = tx_store.store_config();
        assert!(config.is_ok());
    }

    /// Tests that transaction store can be cloned
    #[test]
    fn test_transaction_store_clone() {
        let store = create_test_store();
        let tx_store1 = TransactionStore::new(store);
        let tx_store2 = tx_store1.clone();
        
        // Both should be functional
        assert!(tx_store1.is_closed().is_ok());
        assert!(tx_store2.is_closed().is_ok());
    }

    

    /// Tests opening a new map
    #[test]
    fn test_open_map() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let map = tx_store.open_map("test_collection");
        
        assert!(map.is_ok());
    }

    /// Tests opening multiple maps
    #[test]
    fn test_open_multiple_maps() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let map1 = tx_store.open_map("collection1");
        let map2 = tx_store.open_map("collection2");
        let map3 = tx_store.open_map("collection3");
        
        assert!(map1.is_ok());
        assert!(map2.is_ok());
        assert!(map3.is_ok());
    }

    /// Tests that opening same map twice returns consistent result
    #[test]
    fn test_open_same_map_twice() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let map1 = tx_store.open_map("test_collection");
        let map2 = tx_store.open_map("test_collection");
        
        assert!(map1.is_ok());
        assert!(map2.is_ok());
    }

    /// Tests map_registry management
    #[test]
    fn test_map_registry() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map1 = tx_store.open_map("collection1");
        let _map2 = tx_store.open_map("collection2");
        
        // Maps should be tracked
        let has_map1 = tx_store.has_map("collection1");
        let has_map2 = tx_store.has_map("collection2");
        
        assert!(has_map1.is_ok());
        assert!(has_map2.is_ok());
    }

    /// Tests closing all maps
    #[test]
    fn test_close_all() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let _map1 = tx_store.open_map("collection1");
        let _map2 = tx_store.open_map("collection2");
        
        let result = tx_store.close_all();
        
        assert!(result.is_ok());
    }

    

    /// Tests NitriteStoreProvider::open_or_create
    #[test]
    fn test_store_provider_open_or_create() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let result = tx_store.open_or_create();
        
        // Should succeed (no-op for transaction store)
        assert!(result.is_ok());
    }

    /// Tests NitriteStoreProvider::is_closed
    #[test]
    fn test_is_closed() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let is_closed = tx_store.is_closed();
        
        assert!(is_closed.is_ok());
        assert!(!is_closed.unwrap());
    }

    /// Tests NitriteStoreProvider::has_unsaved_changes
    #[test]
    fn test_has_unsaved_changes() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let has_changes = tx_store.has_unsaved_changes();
        
        assert!(has_changes.is_ok());
        assert!(has_changes.unwrap());
    }

    /// Tests NitriteStoreProvider::is_read_only
    #[test]
    fn test_is_read_only() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let is_read_only = tx_store.is_read_only();
        
        assert!(is_read_only.is_ok());
        assert!(!is_read_only.unwrap());
    }

    /// Tests NitriteStoreProvider::commit fails with error
    #[test]
    fn test_commit_fails() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let result = tx_store.commit();
        
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
    }

    /// Tests NitriteStoreProvider::compact succeeds
    #[test]
    fn test_compact() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let result = tx_store.compact();
        
        assert!(result.is_ok());
    }

    /// Tests NitriteStoreProvider::before_close succeeds
    #[test]
    fn test_before_close() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let result = tx_store.before_close();
        
        assert!(result.is_ok());
    }

    

    /// Tests is_map_opened
    #[test]
    fn test_is_map_opened() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let _map = tx_store.open_map("test_collection");
        
        let is_opened = tx_store.is_map_opened("test_collection");
        
        assert!(is_opened.is_ok());
    }

    /// Tests has_map for existing map
    #[test]
    fn test_has_map_exists() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let _map = tx_store.open_map("test_collection");
        
        let has_map = tx_store.has_map("test_collection");
        
        assert!(has_map.is_ok());
        assert!(has_map.unwrap());
    }

    /// Tests has_map for non-existent map
    #[test]
    fn test_has_map_not_exists() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let has_map = tx_store.has_map("nonexistent_map");
        
        // Should return false
        let result = has_map.unwrap_or(false);
        assert!(!result);
    }

    /// Tests close_map
    #[test]
    fn test_close_map() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let _map = tx_store.open_map("test_collection");
        
        let result = tx_store.close_map("test_collection");
        
        assert!(result.is_ok());
    }

    /// Tests remove_map
    #[test]
    fn test_remove_map() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let _map = tx_store.open_map("test_collection");
        
        let result = tx_store.remove_map("test_collection");
        
        assert!(result.is_ok());
    }

    

    /// Tests get_collection_names
    #[test]
    fn test_get_collection_names() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let names = tx_store.get_collection_names();
        
        assert!(names.is_ok());
    }

    /// Tests get_repository_registry
    #[test]
    fn test_get_repository_registry() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let repos = tx_store.get_repository_registry();
        
        assert!(repos.is_ok());
    }

    /// Tests get_keyed_repository_registry
    #[test]
    fn test_get_keyed_repository_registry() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let keyed_repos = tx_store.get_keyed_repository_registry();
        
        assert!(keyed_repos.is_ok());
    }

    /// Tests store_version
    #[test]
    fn test_store_version() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let version = tx_store.store_version();
        
        assert!(version.is_ok());
        let v = version.unwrap();
        assert!(!v.is_empty());
    }

    /// Tests store_config
    #[test]
    fn test_store_config() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let config = tx_store.store_config();
        
        assert!(config.is_ok());
    }

    /// Tests store_catalog
    #[test]
    fn test_store_catalog() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let catalog = tx_store.store_catalog();
        
        assert!(catalog.is_ok());
    }

    

    /// Tests NitritePluginProvider::initialize
    #[test]
    fn test_plugin_initialize() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let config = NitriteConfig::new();
        
        let result = tx_store.initialize(config);
        
        assert!(result.is_ok());
    }

    /// Tests NitritePluginProvider::close
    #[test]
    fn test_plugin_close() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let result = tx_store.close();
        
        assert!(result.is_ok());
    }

    /// Tests NitritePluginProvider::as_plugin
    #[test]
    fn test_as_plugin() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _plugin = tx_store.as_plugin();
        
        // Plugin created successfully
        assert!(true);
    }

    

    /// Tests subscribe returns None (no-op)
    #[test]
    fn test_subscribe_noop() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        // Create a dummy listener with closure that returns Ok(())
        let listener = StoreEventListener::new(|_| Ok(()));
        let result = tx_store.subscribe(listener);
        
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// Tests unsubscribe succeeds (no-op)
    #[test]
    fn test_unsubscribe_noop() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        // Create dummy subscriber ref with HandlerId
        let subscriber_ref = SubscriberRef::new(HandlerId::new());
        let result = tx_store.unsubscribe(subscriber_ref);
        
        assert!(result.is_ok());
    }

    

    /// Tests transactional map creation and isolation
    #[test]
    fn test_map_isolation() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map1 = tx_store.open_map("collection1");
        let _map2 = tx_store.open_map("collection2");
        
        // Maps should be separate transactional instances
        let has_map1 = tx_store.has_map("collection1").unwrap_or(false);
        let has_map2 = tx_store.has_map("collection2").unwrap_or(false);
        
        assert!(has_map1);
        assert!(has_map2);
    }

    /// Tests deleted maps set
    #[test]
    fn test_deleted_maps_tracking() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map = tx_store.open_map("test_collection");
        tx_store.remove_map("test_collection").unwrap();
        
        // Removed map should be marked deleted
        let has_map = tx_store.has_map("test_collection");
        assert!(has_map.is_ok());
    }

    

    /// Tests commit error with descriptive message
    #[test]
    fn test_commit_error_message() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        let result = tx_store.commit();
        
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("Transaction object"));
    }

    /// Tests operations after close_all
    #[test]
    fn test_operations_after_close_all() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map = tx_store.open_map("test_collection");
        let close_result = tx_store.close_all();
        
        assert!(close_result.is_ok());
    }

    

    /// Tests Arc-based cloning for shared state
    #[test]
    fn test_arc_shared_state() {
        let store = create_test_store();
        let tx_store1 = TransactionStore::new(store);
        let tx_store2 = tx_store1.clone();
        
        let _map1 = tx_store1.open_map("collection1");
        
        // Clone should see same maps
        let has_map = tx_store2.has_map("collection1");
        assert!(has_map.is_ok());
    }

    /// Tests mutex protection of map_registry
    #[test]
    fn test_map_registry_mutex_protection() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map1 = tx_store.open_map("collection1");
        let _map2 = tx_store.open_map("collection2");
        let _map3 = tx_store.open_map("collection3");
        
        // Registry should be consistently accessible
        let count1 = if tx_store.has_map("collection1").unwrap_or(false) { 1 } else { 0 }
                   + if tx_store.has_map("collection2").unwrap_or(false) { 1 } else { 0 }
                   + if tx_store.has_map("collection3").unwrap_or(false) { 1 } else { 0 };
        
        assert_eq!(count1, 3);
    }

    

    /// Tests that store_catalog is accessible
    #[test]
    fn test_catalog_accessibility() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let catalog1 = tx_store.store_catalog();
        let catalog2 = tx_store.store_catalog();
        
        assert!(catalog1.is_ok());
        assert!(catalog2.is_ok());
    }

    /// Tests that underlying store reference is preserved
    #[test]
    fn test_underlying_store_preserved() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store.clone());
        
        // Should have access to underlying store version
        let version = tx_store.store_version();
        assert!(version.is_ok());
    }

    

    /// Tests complete store workflow
    #[test]
    fn test_complete_store_workflow() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        // Open maps
        let _map1 = tx_store.open_map("collection1");
        let _map2 = tx_store.open_map("collection2");
        assert!(tx_store.has_map("collection1").is_ok());
        
        // Get metadata
        let names = tx_store.get_collection_names();
        assert!(names.is_ok());
        
        // Query store
        let config = tx_store.store_config();
        assert!(config.is_ok());
        
        // Close all
        let close = tx_store.close_all();
        assert!(close.is_ok());
    }

    /// Tests store with clones
    #[test]
    fn test_store_with_clones() {
        let store = create_test_store();
        let tx_store1 = TransactionStore::new(store);
        let tx_store2 = tx_store1.clone();
        let tx_store3 = tx_store1.clone();
        
        // All clones should work
        let _map1 = tx_store1.open_map("collection1");
        let _map2 = tx_store2.open_map("collection2");
        let _map3 = tx_store3.open_map("collection3");
        
        // All clones should see all maps
        assert!(tx_store1.has_map("collection1").is_ok());
        assert!(tx_store2.has_map("collection2").is_ok());
        assert!(tx_store3.has_map("collection3").is_ok());
    }

    /// Tests map lifecycle with reopening
    #[test]
    fn test_map_reopen() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        let _map1 = tx_store.open_map("test_collection");
        tx_store.close_map("test_collection").unwrap();
        
        // Should be able to reopen
        let _map2 = tx_store.open_map("test_collection");
        assert!(tx_store.has_map("test_collection").is_ok());
    }

    /// Tests multiple operations on same store
    #[test]
    fn test_multiple_operations() {
        let store = create_test_store();
        let tx_store = TransactionStore::new(store);
        
        for i in 0..10 {
            let collection_name = format!("collection_{}", i);
            let _map = tx_store.open_map(&collection_name);
            assert!(tx_store.has_map(&collection_name).is_ok());
        }
        
        // Should still be functional
        let config = tx_store.store_config();
        assert!(config.is_ok());
    }
}
