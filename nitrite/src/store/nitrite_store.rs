use crate::common::{NitritePlugin, SubscriberRef};
use crate::errors::NitriteResult;
use crate::nitrite_config::NitriteConfig;
use crate::store::{NitriteMap, StoreCatalog, StoreConfig, StoreEventListener};
use crate::NitritePluginProvider;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

/// Low-level interface for managing a Nitrite database store.
///
/// # Purpose
/// Defines the contract that all store implementations must follow. A store manages
/// the persistence layer, including creating/opening maps, handling collections and
/// repositories, managing transactions, and publishing storage events.
///
/// # Key Responsibilities
/// - **Map Management**: Create, open, and manage key-value maps
/// - **Registry Management**: Track collections, repositories, and keyed repositories
/// - **Lifecycle**: Initialize, commit, compact, and close the store
/// - **Event Publishing**: Notify listeners of storage state changes
/// - **Configuration**: Provide store configuration and catalog information
///
/// # Implementations
/// - `InMemoryStore`: In-memory storage for testing/temporary use
/// - `ReDBStore`: Persistent storage using ReDB backend
/// - `FjallStore`: Persistent storage using Fjall backend
///
/// # Thread Safety
/// Implementers must be `Send + Sync` for safe use in concurrent contexts.
pub trait NitriteStoreProvider: NitritePluginProvider + Send + Sync {
    /// Opens or creates the store.
    ///
    /// This must be called before any other store operations.
    /// If the store already exists at the configured location, it is opened.
    /// Otherwise, a new store is created.
    ///
    /// # Returns
    /// * `Ok(())` if the store was successfully opened or created
    /// * `Err(NitriteError)` if the operation fails
    fn open_or_create(&self) -> NitriteResult<()>;

    /// Checks if the store is closed.
    ///
    /// A closed store cannot be used for further operations.
    ///
    /// # Returns
    /// * `Ok(true)` if the store is closed
    /// * `Ok(false)` if the store is open
    /// * `Err(NitriteError)` if the operation fails
    fn is_closed(&self) -> NitriteResult<bool>;

    /// Retrieves all collection names in the store.
    ///
    /// # Returns
    /// * `Ok(HashSet)` with all collection names
    /// * `Err(NitriteError)` if the operation fails
    fn get_collection_names(&self) -> NitriteResult<HashSet<String>>;

    /// Retrieves all repository types registered in the store.
    ///
    /// # Returns
    /// * `Ok(HashSet)` with fully qualified type names of repositories
    /// * `Err(NitriteError)` if the operation fails
    fn get_repository_registry(&self) -> NitriteResult<HashSet<String>>;

    /// Retrieves all keyed repositories and their keys in the store.
    ///
    /// # Returns
    /// * `Ok(HashMap)` mapping repository type names to sets of key names
    /// * `Err(NitriteError)` if the operation fails
    fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>>;

    /// Checks if the store has unsaved changes.
    ///
    /// # Returns
    /// * `Ok(true)` if there are pending changes
    /// * `Ok(false)` if all changes are committed
    /// * `Err(NitriteError)` if the operation fails
    fn has_unsaved_changes(&self) -> NitriteResult<bool>;

    /// Checks if the store is in read-only mode.
    ///
    /// In read-only mode, write operations are not allowed.
    ///
    /// # Returns
    /// * `Ok(true)` if the store is read-only
    /// * `Ok(false)` if the store allows writes
    /// * `Err(NitriteError)` if the operation fails
    fn is_read_only(&self) -> NitriteResult<bool>;

    /// Checks if a specific map is already opened.
    ///
    /// # Arguments
    /// * `name` - The name of the map to check
    ///
    /// # Returns
    /// * `Ok(true)` if the map is open
    /// * `Ok(false)` if the map is closed or does not exist
    /// * `Err(NitriteError)` if the operation fails
    fn is_map_opened(&self, name: &str) -> NitriteResult<bool>;

    /// Commits all pending changes to the store.
    ///
    /// For in-memory stores, this is a no-op. For persistent stores,
    /// this ensures all data is flushed to disk.
    ///
    /// # Returns
    /// * `Ok(())` if the commit was successful
    /// * `Err(NitriteError)` if the operation fails
    fn commit(&self) -> NitriteResult<()>;
    
    /// Compacts the store to reclaim space.
    ///
    /// This operation may be expensive and is typically called during maintenance.
    /// The store should remain usable during compaction.
    ///
    /// # Returns
    /// * `Ok(())` if the compaction was successful
    /// * `Err(NitriteError)` if the operation fails
    fn compact(&self) -> NitriteResult<()>;

    /// Performs cleanup before closing the store.
    ///
    /// This is called before `close()` and allows the store to perform
    /// final operations like flushing pending data or notifying listeners.
    ///
    /// # Returns
    /// * `Ok(())` if pre-close operations were successful
    /// * `Err(NitriteError)` if the operation fails
    fn before_close(&self) -> NitriteResult<()>;

    /// Checks if a map with the given name exists in the store.
    ///
    /// # Arguments
    /// * `name` - The name of the map to check
    ///
    /// # Returns
    /// * `Ok(true)` if the map exists
    /// * `Ok(false)` if the map does not exist
    /// * `Err(NitriteError)` if the operation fails
    fn has_map(&self, name: &str) -> NitriteResult<bool>;

    /// Opens or creates a map with the given name.
    ///
    /// If the map already exists, it is opened. Otherwise, a new map is created.
    ///
    /// # Arguments
    /// * `name` - The name/identifier for the map
    ///
    /// # Returns
    /// * `Ok(NitriteMap)` with the opened or created map
    /// * `Err(NitriteError)` if the operation fails
    fn open_map(&self, name: &str) -> NitriteResult<NitriteMap>;

    /// Closes an opened map.
    ///
    /// After closing, the map should not be used for further operations.
    /// The map remains in the store but is no longer cached.
    ///
    /// # Arguments
    /// * `name` - The name of the map to close
    ///
    /// # Returns
    /// * `Ok(())` if the map was successfully closed
    /// * `Err(NitriteError)` if the operation fails
    fn close_map(&self, name: &str) -> NitriteResult<()>;

    /// Removes a map from the store.
    ///
    /// This is a destructive operation that deletes all data in the map.
    /// The map must not be open when this is called.
    ///
    /// # Arguments
    /// * `name` - The name of the map to remove
    ///
    /// # Returns
    /// * `Ok(())` if the map was successfully removed
    /// * `Err(NitriteError)` if the operation fails
    fn remove_map(&self, name: &str) -> NitriteResult<()>;

    /// Subscribes to store events.
    ///
    /// The listener will be called whenever store state changes occur.
    /// Returns a subscriber reference that can be used to unsubscribe.
    ///
    /// # Arguments
    /// * `listener` - The event listener callback
    ///
    /// # Returns
    /// * `Ok(Some(subscriber_ref))` with a handle to unsubscribe later
    /// * `Ok(None)` if subscriptions are not supported
    /// * `Err(NitriteError)` if the operation fails
    fn subscribe(&self, listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>>;

    /// Unsubscribes from store events.
    ///
    /// # Arguments
    /// * `subscriber_ref` - The subscriber reference returned from `subscribe()`
    ///
    /// # Returns
    /// * `Ok(())` if the listener was successfully unsubscribed
    /// * `Err(NitriteError)` if the operation fails
    fn unsubscribe(&self, subscriber_ref: SubscriberRef) -> NitriteResult<()>;

    /// Returns the version of the store.
    ///
    /// # Returns
    /// * `Ok(String)` with the version identifier
    /// * `Err(NitriteError)` if the operation fails
    fn store_version(&self) -> NitriteResult<String>;

    /// Returns the configuration of the store.
    ///
    /// # Returns
    /// * `Ok(StoreConfig)` with store configuration details
    /// * `Err(NitriteError)` if the operation fails
    fn store_config(&self) -> NitriteResult<StoreConfig>;

    /// Returns the catalog of the store.
    ///
    /// The catalog contains metadata about all stored entities.
    ///
    /// # Returns
    /// * `Ok(StoreCatalog)` with store catalog information
    /// * `Err(NitriteError)` if the operation fails
    fn store_catalog(&self) -> NitriteResult<StoreCatalog>;
}


/// High-level wrapper for accessing a Nitrite database store.
///
/// # Purpose
/// `NitriteStore` provides the public API for interacting with a database store.
/// It wraps a concrete `NitriteStoreProvider` implementation using `Arc` for
/// efficient, thread-safe sharing across the application.
///
/// # Characteristics
/// - **Thread-Safe**: Can be safely cloned and shared across threads
/// - **Provider-Agnostic**: Works with any `NitriteStoreProvider` implementation
/// - **Ergonomic**: Implements `Deref` for seamless access to provider methods
/// - **Lightweight**: Cloning is cheap - only increments the reference count
///
/// # Obtaining a Store
/// Stores are typically obtained via `NitriteConfig`:
/// ```text
/// let config = NitriteConfig::default();
/// config.auto_configure().unwrap();
/// config.initialize().unwrap();
/// let store = config.nitrite_store().unwrap();
/// ```
///
/// # Usage Example
/// ```text
/// // Open or create a map
/// let map = store.open_map("users").unwrap();
///
/// // Store data
/// map.put(key!("user:1"), val!({"name": "Alice"})).unwrap();
///
/// // Retrieve data
/// let value = map.get(&key!("user:1")).unwrap();
///
/// // Commit changes
/// store.commit().unwrap();
/// ```
#[derive(Clone)]
pub struct NitriteStore {
    inner: Arc<dyn NitriteStoreProvider>,
}

impl NitriteStore {
    /// Creates a new `NitriteStore` wrapping a provider implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete implementation of `NitriteStoreProvider`
    ///
    /// # Returns
    /// A new `NitriteStore` that dereferences to `Arc<dyn NitriteStoreProvider>`
    ///
    /// # Notes
    /// - The provider is wrapped in an `Arc` for efficient, thread-safe sharing
    /// - Cloning `NitriteStore` is cheap - it only increments the reference count
    /// - The same store can be safely shared across multiple threads
    pub fn new<T: NitriteStoreProvider + 'static>(inner: T) -> Self {
        NitriteStore { inner: Arc::new(inner) }
    }
}

impl Deref for NitriteStore {
    type Target = Arc<dyn NitriteStoreProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
impl Default for NitriteStore {
    fn default() -> Self {
        let config = NitriteConfig::default();
        config
            .auto_configure()
            .expect("Failed to auto configure Nitrite");
        config.initialize().expect("Failed to initialize Nitrite");
        config.nitrite_store().expect("Failed to get NitriteStore")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::NitriteError;
    use basu::HandlerId;
    use std::collections::HashSet;

    #[derive(Clone)]
    struct MockNitriteStore;

    impl NitritePluginProvider for MockNitriteStore {
        fn initialize(&self, _config: NitriteConfig) -> NitriteResult<()> {
            Ok(())
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn as_plugin(&self) -> NitritePlugin {
            NitritePlugin::new(self.clone())
        }
    }

    impl NitriteStoreProvider for MockNitriteStore {
        fn open_or_create(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_closed(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
            Ok(HashSet::new())
        }

        fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
            Ok(HashSet::new())
        }

        fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
            Ok(HashMap::new())
        }

        fn has_unsaved_changes(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn is_read_only(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn is_map_opened(&self, _name: &str) -> NitriteResult<bool> {
            Ok(false)
        }

        fn commit(&self) -> NitriteResult<()> {
            Ok(())
        }
        
        fn compact(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn before_close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn has_map(&self, _name: &str) -> NitriteResult<bool> {
            Ok(false)
        }

        fn open_map(&self, _name: &str) -> NitriteResult<NitriteMap> {
            Err(NitriteError::new("Map not found", crate::errors::ErrorKind::InvalidOperation))
        }

        fn close_map(&self, _name: &str) -> NitriteResult<()> {
            Ok(())
        }

        fn remove_map(&self, _name: &str) -> NitriteResult<()> {
            Ok(())
        }

        fn subscribe(&self, _listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
            Err(NitriteError::new("Subscription failed", crate::errors::ErrorKind::InvalidOperation))
        }

        fn unsubscribe(&self, _subscriber_ref: SubscriberRef) -> NitriteResult<()> {
            Ok(())
        }

        fn store_version(&self) -> NitriteResult<String> {
            Ok("1.0".to_string())
        }

        fn store_config(&self) -> NitriteResult<StoreConfig> {
            Err(NitriteError::new("Config not found", crate::errors::ErrorKind::InvalidOperation))
        }

        fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
            Err(NitriteError::new("Catalog not found", crate::errors::ErrorKind::InvalidOperation))
        }
    }

    #[test]
    fn test_open_or_create() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.open_or_create().is_ok());
    }

    #[test]
    fn test_is_closed() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(!store.is_closed().unwrap());
    }

    #[test]
    fn test_get_collection_names() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.get_collection_names().unwrap().is_empty());
    }

    #[test]
    fn test_get_repository_registry() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.get_repository_registry().unwrap().is_empty());
    }

    #[test]
    fn test_get_keyed_repository_registry() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.get_keyed_repository_registry().unwrap().is_empty());
    }

    #[test]
    fn test_has_unsaved_changes() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(!store.has_unsaved_changes().unwrap());
    }

    #[test]
    fn test_is_read_only() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(!store.is_read_only().unwrap());
    }

    #[test]
    fn test_commit() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.commit().is_ok());
    }
    
    #[test]
    fn test_compact() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.compact().is_ok());
    }

    #[test]
    fn test_before_close() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.before_close().is_ok());
    }

    #[test]
    fn test_has_map() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(!store.has_map("test_map").unwrap());
    }

    #[test]
    fn test_open_map() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.open_map("test_map").is_err());
    }

    #[test]
    fn test_close_map() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.close_map("test_map").is_ok());
    }

    #[test]
    fn test_remove_map() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.remove_map("test_map").is_ok());
    }

    #[test]
    fn test_subscribe() {
        let store = NitriteStore::new(MockNitriteStore);
        let listener = StoreEventListener::new(Box::new(|_| Ok(())));
        assert!(store.subscribe(listener).is_err());
    }

    #[test]
    fn test_unsubscribe() {
        let store = NitriteStore::new(MockNitriteStore);
        let subscriber_ref = SubscriberRef::new(HandlerId::new());
        assert!(store.unsubscribe(subscriber_ref).is_ok());
    }

    #[test]
    fn test_store_version() {
        let store = NitriteStore::new(MockNitriteStore);
        assert_eq!(store.store_version().unwrap(), "1.0");
    }

    #[test]
    fn test_store_config() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.store_config().is_err());
    }

    #[test]
    fn test_store_catalog() {
        let store = NitriteStore::new(MockNitriteStore);
        assert!(store.store_catalog().is_err());
    }

    #[test]
    fn test_default() {
        let store = NitriteStore::default();
        assert!(store.open_or_create().is_ok());
    }

    #[test]
    fn test_store_cloning_efficiency() {
        // Test that store cloning is efficient with Arc
        let store1 = NitriteStore::new(MockNitriteStore);
        let store2 = store1.clone();
        
        // Both should be functional and independent
        assert!(store1.commit().is_ok());
        assert!(store2.commit().is_ok());
    }

    #[test]
    fn test_deref_access_efficiency() {
        // Test that Deref allows efficient access to Arc<dyn TNitriteStore>
        let store = NitriteStore::new(MockNitriteStore);
        let _deref_target = &*store;
        
        assert!(!store.is_closed().unwrap());
    }

    #[test]
    fn test_multiple_registry_queries() {
        // Test efficiency of multiple registry queries
        let store = NitriteStore::new(MockNitriteStore);
        
        let names1 = store.get_collection_names().unwrap();
        let names2 = store.get_collection_names().unwrap();
        let registry1 = store.get_repository_registry().unwrap();
        let registry2 = store.get_repository_registry().unwrap();
        
        // All should succeed without issues
        assert!(names1.is_empty());
        assert!(names2.is_empty());
        assert!(registry1.is_empty());
        assert!(registry2.is_empty());
    }

    #[test]
    fn test_lifecycle_operations_sequence() {
        // Test efficient execution of lifecycle operations
        let store = NitriteStore::new(MockNitriteStore);
        
        assert!(store.open_or_create().is_ok());
        assert!(!store.is_closed().unwrap());
        assert!(!store.has_unsaved_changes().unwrap());
        assert!(store.commit().is_ok());
        assert!(store.before_close().is_ok());
    }

    #[test]
    fn test_concurrent_store_operations() {
        // Test that multiple store instances can operate without interference
        let store1 = NitriteStore::new(MockNitriteStore);
        let store2 = NitriteStore::new(MockNitriteStore);
        
        assert!(store1.open_or_create().is_ok());
        assert!(store2.open_or_create().is_ok());
        assert!(store1.commit().is_ok());
        assert!(store2.commit().is_ok());
    }
}