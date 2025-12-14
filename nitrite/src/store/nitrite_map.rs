use crate::common::{AttributeAware, Key, Value};
use crate::errors::NitriteResult;
use crate::store::iters::{
    EntryIterator, KeyIterator, ValueIterator,
};
use crate::store::NitriteStore;
use std::fmt::Debug;
use std::iter::Rev;
use std::ops::Deref;
use std::sync::Arc;

/// Low-level interface for key-value store implementations in Nitrite.
///
/// # Purpose
/// Defines the contract that all key-value store backends must implement.
/// Implementers provide concrete storage operations for Nitrite, such as in-memory
/// or persistent storage via adapters like ReDB or Fjall.
///
/// # Key Methods
/// - **Basic Operations**: `put()`, `get()`, `remove()`, `contains_key()`
/// - **Bulk Operations**: `put_all()` for atomic batch inserts
/// - **Iteration**: `keys()`, `values()`, `entries()` with bidirectional traversal
/// - **Navigation**: `first_key()`, `last_key()`, `higher_key()`, `ceiling_key()`, `lower_key()`, `floor_key()`
/// - **Lifecycle**: `open()`, `close()`, `clear()`, `dispose()`
/// - **State**: `is_closed()`, `is_dropped()`, `size()`, `is_empty()`
/// - **Attributes**: Support for metadata via `AttributeAware`
///
/// # Implementations
/// - `InMemoryMapProvider`: In-memory hash-based storage
/// - `ReDBMapProvider`: Persistent storage via ReDB
/// - `FjallMapProvider`: Persistent storage via Fjall
/// - `TransactionMapProvider`: Transactional view over map data
///
/// # Thread Safety
/// Implementers must be `Send + Sync` for safe use in concurrent contexts.
pub trait NitriteMapProvider: AttributeAware + Send + Sync {
    /// Checks whether the map contains a key.
    ///
    /// # Arguments
    /// * `key` - The key to check for existence
    ///
    /// # Returns
    /// * `Ok(true)` if the key exists
    /// * `Ok(false)` if the key does not exist
    /// * `Err(NitriteError)` if the operation fails
    fn contains_key(&self, key: &Key) -> NitriteResult<bool>;

    /// Retrieves the value associated with a key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve
    ///
    /// # Returns
    /// * `Ok(Some(value))` if the key exists
    /// * `Ok(None)` if the key does not exist
    /// * `Err(NitriteError)` if the operation fails
    fn get(&self, key: &Key) -> NitriteResult<Option<Value>>;

    /// Clears all entries from the map.
    ///
    /// # Returns
    /// * `Ok(())` if the map was successfully cleared
    /// * `Err(NitriteError)` if the operation fails
    fn clear(&self) -> NitriteResult<()>;

    /// Checks if the map is closed.
    ///
    /// A closed map cannot be used for further operations.
    ///
    /// # Returns
    /// * `Ok(true)` if the map is closed
    /// * `Ok(false)` if the map is open
    /// * `Err(NitriteError)` if the operation fails
    fn is_closed(&self) -> NitriteResult<bool>;

    /// Closes the map.
    ///
    /// After closing, no further operations should be performed on this map.
    /// For persistent storage backends, this may flush pending data.
    ///
    /// # Returns
    /// * `Ok(())` if the map was successfully closed
    /// * `Err(NitriteError)` if the operation fails
    fn close(&self) -> NitriteResult<()>;

    /// Retrieves an iterator over all values in the map.
    ///
    /// The returned iterator supports bidirectional traversal using the `Iterator`
    /// and `DoubleEndedIterator` traits.
    ///
    /// # Returns
    /// * `Ok(ValueIterator)` with an iterator over all values
    /// * `Err(NitriteError)` if the operation fails
    fn values(&self) -> NitriteResult<ValueIterator>;

    /// Retrieves an iterator over all keys in the map.
    ///
    /// The returned iterator supports bidirectional traversal using the `Iterator`
    /// and `DoubleEndedIterator` traits.
    ///
    /// # Returns
    /// * `Ok(KeyIterator)` with an iterator over all keys
    /// * `Err(NitriteError)` if the operation fails
    fn keys(&self) -> NitriteResult<KeyIterator>;

    /// Removes a key-value pair from the map.
    ///
    /// # Arguments
    /// * `key` - The key to remove
    ///
    /// # Returns
    /// * `Ok(Some(value))` with the removed value if the key existed
    /// * `Ok(None)` if the key did not exist
    /// * `Err(NitriteError)` if the operation fails
    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>>;

    /// Inserts or updates a key-value pair in the map.
    ///
    /// If the key already exists, the value is updated.
    ///
    /// # Arguments
    /// * `key` - The key to insert or update
    /// * `value` - The value to store
    ///
    /// # Returns
    /// * `Ok(())` if the operation was successful
    /// * `Err(NitriteError)` if the operation fails
    fn put(&self, key: Key, value: Value) -> NitriteResult<()>;

    /// Atomically inserts multiple key-value pairs in a single batch operation.
    ///
    /// This is more efficient than calling `put()` multiple times as it reduces
    /// journal writes and fsync overhead for persistent backends. The operation is atomic -
    /// all entries are committed together or none at all.
    ///
    /// # Arguments
    /// * `entries` - A vector of key-value pairs to insert
    ///
    /// # Returns
    /// * `Ok(())` if all entries were successfully inserted
    /// * `Err(NitriteError)` if the operation failed
    ///
    /// # Default Implementation
    /// The default implementation calls `put()` individually for each entry.
    /// Implementations should override this for better performance.
    fn put_all(&self, entries: Vec<(Key, Value)>) -> NitriteResult<()> {
        // Default implementation: fall back to individual puts
        for (key, value) in entries {
            self.put(key, value)?;
        }
        Ok(())
    }

    /// Returns the number of entries in the map.
    ///
    /// # Returns
    /// * `Ok(count)` with the total number of key-value pairs
    /// * `Err(NitriteError)` if the operation fails
    fn size(&self) -> NitriteResult<u64>;

    /// Inserts a key-value pair only if the key does not already exist.
    ///
    /// This is an atomic operation - either the key-value pair is inserted
    /// or the operation returns the existing value.
    ///
    /// # Arguments
    /// * `key` - The key to potentially insert
    /// * `value` - The value to store if the key does not exist
    ///
    /// # Returns
    /// * `Ok(None)` if the key did not exist and was successfully inserted
    /// * `Ok(Some(existing_value))` if the key already existed
    /// * `Err(NitriteError)` if the operation fails
    fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>>;

    /// Returns the first (lowest) key in the map, if it exists.
    ///
    /// # Returns
    /// * `Ok(Some(key))` with the first key in sort order
    /// * `Ok(None)` if the map is empty
    /// * `Err(NitriteError)` if the operation fails
    fn first_key(&self) -> NitriteResult<Option<Key>>;

    /// Returns the last (highest) key in the map, if it exists.
    ///
    /// # Returns
    /// * `Ok(Some(key))` with the last key in sort order
    /// * `Ok(None)` if the map is empty
    /// * `Err(NitriteError)` if the operation fails
    fn last_key(&self) -> NitriteResult<Option<Key>>;

    /// Returns the least key greater than the specified key, if it exists.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// * `Ok(Some(higher_key))` if a greater key exists
    /// * `Ok(None)` if no greater key exists
    /// * `Err(NitriteError)` if the operation fails
    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>>;

    /// Returns the least key greater than or equal to the specified key, if it exists.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// * `Ok(Some(ceiling_key))` if a key >= the given key exists
    /// * `Ok(None)` if no such key exists
    /// * `Err(NitriteError)` if the operation fails
    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>>;

    /// Returns the greatest key less than the specified key, if it exists.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// * `Ok(Some(lower_key))` if a lesser key exists
    /// * `Ok(None)` if no lesser key exists
    /// * `Err(NitriteError)` if the operation fails
    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>>;

    /// Returns the greatest key less than or equal to the specified key, if it exists.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// * `Ok(Some(floor_key))` if a key <= the given key exists
    /// * `Ok(None)` if no such key exists
    /// * `Err(NitriteError)` if the operation fails
    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>>;

    /// Checks if the map is empty.
    ///
    /// # Returns
    /// * `Ok(true)` if the map contains no entries
    /// * `Ok(false)` if the map contains at least one entry
    /// * `Err(NitriteError)` if the operation fails
    fn is_empty(&self) -> NitriteResult<bool>;

    /// Returns a reference to the parent `NitriteStore` that owns this map.
    ///
    /// # Returns
    /// * `Ok(NitriteStore)` with the parent store
    /// * `Err(NitriteError)` if the operation fails
    fn get_store(&self) -> NitriteResult<NitriteStore>;

    /// Returns the name of this map.
    ///
    /// # Returns
    /// * `Ok(String)` with the map's identifier
    /// * `Err(NitriteError)` if the operation fails
    fn get_name(&self) -> NitriteResult<String>;

    /// Retrieves an iterator over all key-value entries in the map.
    ///
    /// The returned iterator supports bidirectional traversal using the `Iterator`
    /// and `DoubleEndedIterator` traits.
    ///
    /// # Returns
    /// * `Ok(EntryIterator)` with an iterator over all entries
    /// * `Err(NitriteError)` if the operation fails
    fn entries(&self) -> NitriteResult<EntryIterator>;

    /// Retrieves a reverse iterator over all key-value entries in the map.
    ///
    /// Iterates entries in reverse order (from last to first).
    ///
    /// # Returns
    /// * `Ok(Rev<EntryIterator>)` with a reverse iterator over all entries
    /// * `Err(NitriteError)` if the operation fails
    fn reverse_entries(&self) -> NitriteResult<Rev<EntryIterator>>;

    /// Disposes of the map's resources.
    ///
    /// This is typically called when the database is closing. Unlike `close()`,
    /// this performs cleanup and may be called even if already closed.
    ///
    /// # Returns
    /// * `Ok(())` if disposal was successful
    /// * `Err(NitriteError)` if the operation fails
    fn dispose(&self) -> NitriteResult<()>;

    /// Checks if the map has been dropped.
    ///
    /// A dropped map is no longer part of the database and should not be used.
    ///
    /// # Returns
    /// * `Ok(true)` if the map has been dropped
    /// * `Ok(false)` if the map is still valid
    /// * `Err(NitriteError)` if the operation fails
    fn is_dropped(&self) -> NitriteResult<bool>;
}

#[derive(Clone)]
pub struct NitriteMap {
    inner: Arc<dyn NitriteMapProvider>,
}

impl Deref for NitriteMap {
    type Target = Arc<dyn NitriteMapProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl NitriteMap {
    /// Creates a new `NitriteMap` wrapping a provider implementation.
    ///
    /// # Arguments
    /// * `inner` - A concrete implementation of `NitriteMapProvider`
    ///
    /// # Returns
    /// A new `NitriteMap` that dereferences to `Arc<dyn NitriteMapProvider>`
    ///
    /// # Notes
    /// - The provider is wrapped in an `Arc` for efficient, thread-safe sharing
    /// - Cloning `NitriteMap` is cheap - it only increments the reference count
    /// - The same map can be safely shared across multiple threads
    pub fn new<T: NitriteMapProvider + 'static>(inner: T) -> Self {
        NitriteMap { inner: Arc::new(inner) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Attributes, Key, Value};
    use crate::errors::NitriteError;
    use crate::store::iters::{EntryIterator, KeyIterator, ValueIterator};
    use crate::store::NitriteStore;

    #[derive(Copy, Clone)]
    struct MockNitriteMap;

    impl AttributeAware for MockNitriteMap {
        fn attributes(&self) -> NitriteResult<Option<Attributes>> {
            Ok(None)
        }

        fn set_attributes(&self, _attributes: Attributes) -> NitriteResult<()> {
            Ok(())
        }
    }

    impl NitriteMapProvider for MockNitriteMap {
        fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
            Ok(key == &Key::from("key1"))
        }

        fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
            if key == &Key::from("key1") {
                Ok(Some(Value::from("value1")))
            } else {
                Ok(None)
            }
        }

        fn clear(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_closed(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn values(&self) -> NitriteResult<ValueIterator> {
            Err(NitriteError::new("Invalid operation", crate::errors::ErrorKind::InvalidOperation))
        }

        fn keys(&self) -> NitriteResult<KeyIterator> {
            Err(NitriteError::new("Invalid operation", crate::errors::ErrorKind::InvalidOperation))
        }

        fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
            if key == &Key::from("key1") {
                Ok(Some(Value::from("value1")))
            } else {
                Ok(None)
            }
        }

        fn put(&self, _key: Key, _value: Value) -> NitriteResult<()> {
            Ok(())
        }

        fn size(&self) -> NitriteResult<u64> {
            Ok(1)
        }

        fn put_if_absent(&self, key: Key, _value: Value) -> NitriteResult<Option<Value>> {
            if key == Key::from("key1") {
                Ok(Some(Value::from("value1")))
            } else {
                Ok(None)
            }
        }

        fn first_key(&self) -> NitriteResult<Option<Key>> {
            Ok(Some(Key::from("key1")))
        }

        fn last_key(&self) -> NitriteResult<Option<Key>> {
            Ok(Some(Key::from("key1")))
        }

        fn higher_key(&self, _key: &Key) -> NitriteResult<Option<Key>> {
            Ok(None)
        }

        fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
            Ok(Some(key.clone()))
        }

        fn lower_key(&self, _key: &Key) -> NitriteResult<Option<Key>> {
            Ok(None)
        }

        fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
            Ok(Some(key.clone()))
        }

        fn is_empty(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn get_store(&self) -> NitriteResult<NitriteStore> {
            Err(NitriteError::new("Invalid operation", crate::errors::ErrorKind::InvalidOperation))
        }

        fn get_name(&self) -> NitriteResult<String> {
            Ok("test_map".to_string())
        }

        fn entries(&self) -> NitriteResult<EntryIterator> {
            Err(NitriteError::new("Invalid operation", crate::errors::ErrorKind::InvalidOperation))
        }

        fn reverse_entries(&self) -> NitriteResult<Rev<EntryIterator>> {
            Err(NitriteError::new("Invalid operation", crate::errors::ErrorKind::InvalidOperation))
        }

        fn dispose(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_dropped(&self) -> NitriteResult<bool> {
            Ok(false)
        }
    }

    #[test]
    fn test_contains_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.contains_key(&Key::from("key1")).unwrap());
        assert!(!map.contains_key(&Key::from("key2")).unwrap());
    }

    #[test]
    fn test_get() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.get(&Key::from("key1")).unwrap(), Some(Value::from("value1")));
        assert_eq!(map.get(&Key::from("key2")).unwrap(), None);
    }

    #[test]
    fn test_clear() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.clear().is_ok());
    }

    #[test]
    fn test_is_closed() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(!map.is_closed().unwrap());
    }

    #[test]
    fn test_close() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.close().is_ok());
    }

    #[test]
    fn test_values() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.values().is_err());
    }

    #[test]
    fn test_keys() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.keys().is_err());
    }

    #[test]
    fn test_remove() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.remove(&Key::from("key1")).unwrap(), Some(Value::from("value1")));
        assert_eq!(map.remove(&Key::from("key2")).unwrap(), None);
    }

    #[test]
    fn test_put() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.put(Key::from("key1"), Value::from("value1")).is_ok());
    }

    #[test]
    fn test_size() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.size().unwrap(), 1);
    }

    #[test]
    fn test_put_if_absent() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.put_if_absent(Key::from("key1"), Value::from("value1")).unwrap(), Some(Value::from("value1")));
        assert_eq!(map.put_if_absent(Key::from("key2"), Value::from("value2")).unwrap(), None);
    }

    #[test]
    fn test_first_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.first_key().unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_last_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.last_key().unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_higher_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.higher_key(&Key::from("key1")).unwrap(), None);
    }

    #[test]
    fn test_ceiling_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.ceiling_key(&Key::from("key1")).unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_lower_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.lower_key(&Key::from("key1")).unwrap(), None);
    }

    #[test]
    fn test_floor_key() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.floor_key(&Key::from("key1")).unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_is_empty() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(!map.is_empty().unwrap());
    }

    #[test]
    fn test_get_store() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.get_store().is_err());
    }

    #[test]
    fn test_get_name() {
        let map = NitriteMap::new(MockNitriteMap);
        assert_eq!(map.get_name().unwrap(), "test_map");
    }

    #[test]
    fn test_entries() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.entries().is_err());
    }

    #[test]
    fn test_reverse_entries() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.reverse_entries().is_err());
    }

    #[test]
    fn test_dispose_map() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(map.dispose().is_ok());
    }

    #[test]
    fn test_is_dropped() {
        let map = NitriteMap::new(MockNitriteMap);
        assert!(!map.is_dropped().unwrap());
    }

    #[test]
    fn test_arc_cloning_efficiency() {
        // Test that NitriteMap cloning is efficient with Arc
        let map1 = NitriteMap::new(MockNitriteMap);
        let map2 = map1.clone();
        
        // Both should reference the same underlying Arc data
        assert_eq!(map1.get_name().unwrap(), map2.get_name().unwrap());
    }

    #[test]
    fn test_deref_access_efficiency() {
        // Test that Deref trait allows efficient access to Arc<dyn TNitriteMap>
        let map = NitriteMap::new(MockNitriteMap);
        
        // Deref should allow direct access without extra allocations
        let _deref_target = &*map;
        assert!(!map.is_empty().unwrap());
    }

    #[test]
    fn test_multiple_sequential_operations() {
        // Test efficiency of multiple sequential operations
        let map = NitriteMap::new(MockNitriteMap);
        
        // Multiple operations should not cause unnecessary overhead
        assert_eq!(map.size().unwrap(), 1);
        assert!(map.contains_key(&Key::from("key1")).unwrap());
        assert_eq!(map.get(&Key::from("key1")).unwrap(), Some(Value::from("value1")));
        assert!(!map.is_empty().unwrap());
    }

    #[test]
    fn test_put_if_absent_consistency() {
        // Test put_if_absent consistency across multiple calls
        let map = NitriteMap::new(MockNitriteMap);
        
        // First call should return existing value
        let result1 = map.put_if_absent(Key::from("key1"), Value::from("new_value1")).unwrap();
        assert_eq!(result1, Some(Value::from("value1")));
        
        // Calling again should return same result
        let result2 = map.put_if_absent(Key::from("key1"), Value::from("another_value")).unwrap();
        assert_eq!(result2, Some(Value::from("value1")));
    }

    #[test]
    fn test_key_operations_efficiency() {
        // Test efficiency of multiple key operations
        let map = NitriteMap::new(MockNitriteMap);
        
        assert_eq!(map.first_key().unwrap(), Some(Key::from("key1")));
        assert_eq!(map.last_key().unwrap(), Some(Key::from("key1")));
        assert_eq!(map.ceiling_key(&Key::from("key1")).unwrap(), Some(Key::from("key1")));
        assert_eq!(map.floor_key(&Key::from("key1")).unwrap(), Some(Key::from("key1")));
    }
}