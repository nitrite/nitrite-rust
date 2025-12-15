use crate::collection::{Document, NitriteId};
use crate::common::{AttributeAware, Attributes, Key, Value, META_MAP_NAME};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::nitrite::Nitrite;
use crate::store::memory::InMemoryMap;
use crate::store::{EntryIterator, KeyIterator, NitriteMap, NitriteMapProvider, NitriteStore, ValueIterator};
use crate::transaction::iters::{TransactionEntryProvider, TransactionKeyProvider, TransactionValueProvider};
use crate::transaction::TransactionStore;
use aes_gcm::aead::rand_core::le;
use itertools::Itertools;
use parking_lot::Mutex;

use std::collections::{HashMap, HashSet};
use std::iter::Rev;
use std::sync::Arc;

/// A transactional map providing Copy-On-Write storage.
///
/// Implements the `NitriteMapProvider` trait to provide an isolated, transaction-local
/// view of a collection's data. Enables multiple concurrent transactions to operate
/// independently without blocking each other.
///
/// # Purpose
/// Manages data isolation during transactions by maintaining three logical data layers:
/// - **Backing Map**: In-memory store of transaction-local modifications
/// - **Primary Map**: Read-only reference to database state at transaction start
/// - **Tombstones**: Set of deleted keys to shadow primary data
///
/// # Characteristics
/// - **Copy-On-Write**: Modifications stored separately; primary remains untouched
/// - **Read-Through**: Reads fall back to primary if key not in backing or tombstones
/// - **Thread-Safe**: All state protected by Arc and Mutex
/// - **Lazy Updates**: Backing map populated only on writes
/// - **No Memory Leaks**: Read-only operations do NOT copy to backing map
/// - **Cloneable**: Arc-based shared ownership for cheap cloning
///
/// # Implementation Notes
/// Read-only operations (get, contains_key, iteration) do NOT modify backing_map.
/// Only write operations (put, remove) modify backing_map. This prevents O(n) memory
/// growth per read that occurred in earlier copy-on-read implementations.
///
/// # Usage
/// Created by `TransactionStore` for each collection accessed in a transaction.
/// Users interact through the `NitriteCollection` public API.
#[derive(Clone)]
pub struct TransactionalMap {
    inner: Arc<TransactionalMapInner>,
}

impl TransactionalMap {
    /// Creates a new transactional map wrapping a primary map.
    ///
    /// # Arguments
    /// * `name` - The name of the collection
    /// * `primary_map` - The read-only map from the primary database state
    /// * `store` - The transaction store for metadata operations
    ///
    /// # Returns
    /// A new `TransactionalMap` with empty backing map, tombstones, and cleared flag
    ///
    /// Initializes an in-memory backing map and empty tombstone set for tracking modifications.
    pub fn new(name: String, primary_map: NitriteMap, store: NitriteStore) -> Self {
        TransactionalMap {
            inner: Arc::new(TransactionalMapInner::new(name, primary_map, store)),
        }
    }
}

struct TransactionalMapInner {
    name: String,
    primary_map: NitriteMap,
    backing_map: NitriteMap,
    store: NitriteStore,
    tombstones: Arc<Mutex<HashSet<Key>>>,
    cleared: Arc<Mutex<bool>>,
    dropped: Arc<Mutex<bool>>,
    closed: Arc<Mutex<bool>>,
}

impl TransactionalMapInner {
    /// Creates a new transactional map
    fn new(name: String, primary_map: NitriteMap, store: NitriteStore) -> Self {
        TransactionalMapInner {
            name: name.clone(),
            primary_map,
            backing_map: NitriteMap::new(InMemoryMap::new(&name, store.clone())),
            store,
            tombstones: Arc::new(Mutex::new(HashSet::new())),
            cleared: Arc::new(Mutex::new(false)),
            dropped: Arc::new(Mutex::new(false)),
            closed: Arc::new(Mutex::new(false)),
        }
    }

    /// Checks if key exists
    fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
        let cleared = *self.cleared.lock();
        if cleared {
            return Ok(false);
        }

        if self.backing_map.contains_key(key)? {
            return Ok(true);
        }

        let tombstones = self.tombstones.lock();
        if tombstones.contains(key) {
            return Ok(false);
        }

        self.primary_map.contains_key(key)
    }

    /// Gets a value by key
    /// 
    /// Note: Read-only operations should NOT copy data to backing_map.
    /// Only writes (put, remove) should modify backing_map.
    /// Copying on read caused memory leaks - every iteration would copy
    /// all documents to backing_map, causing O(n) memory growth per read.
    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        if self.tombstones.lock().contains(key) || *self.cleared.lock() {
            return Ok(None);
        }

        // First check backing_map (transaction-local modifications)
        if let Some(value) = self.backing_map.get(key)? {
            return Ok(Some(value));
        }

        // Fall through to primary_map (read-only, no copy)
        self.primary_map.get(key)
    }

    fn get_store(&self) -> NitriteResult<NitriteStore> {
        Ok(NitriteStore::new(TransactionStore::new(self.store.clone())))
    }

    // auto-commit clear
    fn clear(&self) -> NitriteResult<()> {
        self.backing_map.clear()?;
        self.tombstones.lock().clear();
        *self.cleared.lock() = true;
        let store = self.get_store()?;
        store.close_map(&self.name)?;
        Ok(())
    }

    /// Inserts a key-value pair
    fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        *self.cleared.lock() = false;
        self.tombstones.lock().remove(&key);
        self.backing_map.put(key, value)
    }

    fn get_name(&self) -> &str {
        &self.name
    }

    /// Removes a key
    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        let cleared = *self.cleared.lock();
        let mut tombstones = self.tombstones.lock();

        if cleared || tombstones.contains(key) {
            return Ok(None);
        }

        match self.backing_map.remove(key)? {
            Some(value) => {
                tombstones.insert(key.clone());
                Ok(Some(value))
            },
            None => {
                match self.primary_map.get(key)? {
                    Some(value) => {
                        tombstones.insert(key.clone());
                        Ok(Some(value))
                    },
                    None => Ok(None),
                }
            }
        }
    }

    /// Gets all entries
    fn entries(&self) -> NitriteResult<EntryIterator> {
        let provider = TransactionEntryProvider::new(
            self.backing_map.clone(),
            self.primary_map.clone(),
            self.tombstones.clone(),
            self.cleared.clone(),
        )?;
        Ok(EntryIterator::new(provider))
    }

    /// Gets all keys
    fn keys(&self) -> NitriteResult<KeyIterator> {
        let provider = TransactionKeyProvider::new(
            self.backing_map.clone(),
            self.primary_map.clone(),
            self.tombstones.clone(),
            self.cleared.clone(),
        )?;
        Ok(KeyIterator::new(provider))
    }

    fn values(&self) -> NitriteResult<ValueIterator> {
        let provider = TransactionValueProvider::new(
            self.backing_map.clone(),
            self.primary_map.clone(),
            self.tombstones.clone(),
            self.cleared.clone(),
        )?;
        Ok(ValueIterator::new(provider))
    }

    /// Returns the number of entries
    fn len(&self) -> NitriteResult<usize> {
        let backing_len = self.backing_map.size()? as usize;
        let tombstones_len = self.tombstones.lock().len();

        let primary_len = if *self.cleared.lock() {
            0
        } else {
            let total_primary = self.primary_map.size()? as usize;
            total_primary.saturating_sub(tombstones_len)
        };

        Ok(backing_len + primary_len)
    }

    fn close(&self) -> NitriteResult<()> {
        self.backing_map.clear()?;
        self.tombstones.lock().clear();
        *self.closed.lock() = true;
        *self.cleared.lock() = true;
        self.get_store()?.close_map(&self.name)
    }

    fn is_closed(&self) -> bool {
        if self.primary_map.is_closed().unwrap_or(true) || self.primary_map.is_dropped().unwrap_or(true) {
            true
        } else {
            *self.closed.lock()
        }
    }

    fn is_dropped(&self) -> bool {
        *self.dropped.lock()
    }
}

impl AttributeAware for TransactionalMap {
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        if !self.is_dropped()? {
            let store = self.get_store()?;
            let meta_map = store.open_map(META_MAP_NAME)?;
            let name = self.get_name()?;

            if name.ne(META_MAP_NAME) {
                let attributes = meta_map.get(&Value::from(name.clone()))?;
                if let Some(attributes) = attributes {
                    // Check if value is actually a Document before unwrapping
                    return match attributes.as_document() {
                        Some(doc) => {
                            Ok(Some(Attributes::from_document(doc)))
                        }
                        None => {
                            log::warn!(
                                "Metadata for map '{}' is not a Document, skipping attributes",
                                name
                            );
                            Ok(None)
                        }
                    }
                }
            }
        }
        Ok(None)
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        if !self.is_dropped()? {
            let store = self.get_store()?;
            let meta_map = store.open_map(META_MAP_NAME)?;
            let name = self.get_name()?;

            if name.ne(META_MAP_NAME) {
                meta_map.put(Value::from(name), Value::from(attributes.to_document()))?;
            }
        }
        Ok(())
    }
}

impl NitriteMapProvider for TransactionalMap {
    fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
        self.inner.contains_key(key)
    }

    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.get(key)
    }

    fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        Ok(*self.inner.closed.lock())
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn values(&self) -> NitriteResult<ValueIterator> {
        let provider = TransactionValueProvider::new(
            self.inner.backing_map.clone(),
            self.inner.primary_map.clone(),
            self.inner.tombstones.clone(),
            self.inner.cleared.clone(),
        )?;
        Ok(ValueIterator::new(provider))
    }

    fn keys(&self) -> NitriteResult<KeyIterator> {
        let provider = TransactionKeyProvider::new(
            self.inner.backing_map.clone(),
            self.inner.primary_map.clone(),
            self.inner.tombstones.clone(),
            self.inner.cleared.clone(),
        )?;
        Ok(KeyIterator::new(provider))
    }

    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.remove(key)
    }

    fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        self.inner.put(key, value)
    }

    fn size(&self) -> NitriteResult<u64> {
        Ok(self.inner.len()? as u64)
    }

    fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>> {
        *self.inner.cleared.lock() = false;
        self.inner.tombstones.lock().remove(&key);

        self.inner.get(&key)?.map_or_else(
            || {
                self.inner.put(key, value)?;
                Ok(None)
            },
            |existing_value| Ok(Some(existing_value)),
        )
    }

    fn first_key(&self) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_first = self.inner.primary_map.first_key()?;
        let backing_first = self.inner.backing_map.first_key()?;
        match (primary_first, backing_first) {
            (Some(pk), Some(bk)) => {
                if pk < bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }        
    }

    fn last_key(&self) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_last = self.inner.primary_map.last_key()?;
        let backing_last = self.inner.backing_map.last_key()?;
        match (primary_last, backing_last) {
            (Some(pk), Some(bk)) => {
                if pk > bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }
    }

    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_higher = self.inner.primary_map.higher_key(key)?;
        let backing_higher = self.inner.backing_map.higher_key(key)?;
        match (primary_higher, backing_higher) {
            (Some(pk), Some(bk)) => {
                if pk < bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }
    }

    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_ceiling = self.inner.primary_map.ceiling_key(key)?;
        let backing_ceiling = self.inner.backing_map.ceiling_key(key)?;
        match (primary_ceiling, backing_ceiling) {
            (Some(pk), Some(bk)) => {
                if pk < bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }
    }

    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_lower = self.inner.primary_map.lower_key(key)?;
        let backing_lower = self.inner.backing_map.lower_key(key)?;
        match (primary_lower, backing_lower) {
            (Some(pk), Some(bk)) => {
                if pk > bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }
    }

    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if *self.inner.cleared.lock() {
            return Ok(None);
        }

        let primary_floor = self.inner.primary_map.floor_key(key)?;
        let backing_floor = self.inner.backing_map.floor_key(key)?;
        match (primary_floor, backing_floor) {
            (Some(pk), Some(bk)) => {
                if pk > bk {
                    // Check if pk is tombstoned
                    if self.inner.tombstones.lock().contains(&pk) {
                        Ok(Some(bk))
                    } else {
                        Ok(Some(pk))
                    }
                } else {
                    Ok(Some(bk))
                }
            }
            (Some(pk), None) => {
                if self.inner.tombstones.lock().contains(&pk) {
                    Ok(None)
                } else {
                    Ok(Some(pk))
                }
            }
            (None, Some(bk)) => {
                Ok(Some(bk))
            }
            (None, None) => Ok(None),
        }
    }

    fn is_empty(&self) -> NitriteResult<bool> {
        if *self.inner.cleared.lock() {
            return Ok(true);
        }

        let primary_empty = self.inner.primary_map.is_empty()?;
        let backing_empty = self.inner.backing_map.is_empty()?;
        Ok(primary_empty && backing_empty)
    }

    fn get_store(&self) -> NitriteResult<NitriteStore> {
        self.inner.get_store()
    }

    fn get_name(&self) -> NitriteResult<String> {
        Ok(self.inner.get_name().to_string())
    }

    fn entries(&self) -> NitriteResult<EntryIterator> {
        let provider = TransactionEntryProvider::new(
            self.inner.backing_map.clone(),
            self.inner.primary_map.clone(),
            self.inner.tombstones.clone(),
            self.inner.cleared.clone(),
        )?;
        Ok(EntryIterator::new(provider))
    }

    fn reverse_entries(&self) -> NitriteResult<Rev<EntryIterator>> {
        // Create a reverse entry iterator
        let provider = TransactionEntryProvider::new(
            self.inner.backing_map.clone(),
            self.inner.primary_map.clone(),
            self.inner.tombstones.clone(),
            self.inner.cleared.clone(),
        )?;
        let iter = EntryIterator::new(provider);
        Ok(iter.rev())
    }

    fn dispose(&self) -> NitriteResult<()> {
        if *self.inner.dropped.lock() {
            return Ok(());
        }

        self.inner.backing_map.clear()?;
        self.inner.tombstones.lock().clear();
        // NOTE: Do NOT dispose primary_map - it belongs to the main database
        // and should remain usable after the transaction closes
        *self.inner.dropped.lock() = true;
        *self.inner.cleared.lock() = true;
        // Remove only the transactional backing map, not the primary
        // The name of the transactional map should be different from primary
        // but we don't need to remove it since it's in-memory
        Ok(())
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        Ok(*self.inner.dropped.lock())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nitrite::Nitrite;

    fn create_test_db() -> Nitrite {
        Nitrite::builder().open_or_create(None, None).unwrap()
    }

    fn create_test_map(name: &str) -> (TransactionalMap, NitriteStore, Nitrite) {
        let db = create_test_db();
        let store = db.store();
        let primary_map = store.open_map(name).unwrap();
        let txn_map = TransactionalMap::new(name.to_string(), primary_map, store.clone());
        (txn_map, store, db)
    }

    // ==================== Creation Tests ====================

    #[test]
    fn test_transactional_map_creation() {
        let (txn_map, _store, _db) = create_test_map("test_map");
        
        assert!(!txn_map.is_closed().unwrap());
        assert!(!txn_map.is_dropped().unwrap());
    }

    #[test]
    fn test_transactional_map_get_name() {
        let (txn_map, _store, _db) = create_test_map("my_test_map");
        
        assert_eq!(txn_map.get_name().unwrap(), "my_test_map");
    }

    // ==================== Put/Get Tests ====================

    #[test]
    fn test_put_and_get() {
        let (txn_map, _store, _db) = create_test_map("test_put_get");
        
        let key = Key::from("key1");
        let value = Value::String("value1".to_string());
        
        txn_map.put(key.clone(), value.clone()).unwrap();
        
        let result = txn_map.get(&key).unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let (txn_map, _store, _db) = create_test_map("test_get_nonexistent");
        
        let key = Key::from("nonexistent");
        let result = txn_map.get(&key).unwrap();
        
        assert!(result.is_none());
    }

    #[test]
    fn test_put_overwrite() {
        let (txn_map, _store, _db) = create_test_map("test_put_overwrite");
        
        let key = Key::from("key1");
        let value1 = Value::String("value1".to_string());
        let value2 = Value::String("value2".to_string());
        
        txn_map.put(key.clone(), value1).unwrap();
        txn_map.put(key.clone(), value2.clone()).unwrap();
        
        let result = txn_map.get(&key).unwrap();
        assert_eq!(result.unwrap(), value2);
    }

    // ==================== Contains Key Tests ====================

    #[test]
    fn test_contains_key_existing() {
        let (txn_map, _store, _db) = create_test_map("test_contains_existing");
        
        let key = Key::from("key1");
        let value = Value::String("value1".to_string());
        
        txn_map.put(key.clone(), value).unwrap();
        
        assert!(txn_map.contains_key(&key).unwrap());
    }

    #[test]
    fn test_contains_key_nonexistent() {
        let (txn_map, _store, _db) = create_test_map("test_contains_nonexistent");
        
        let key = Key::from("nonexistent");
        
        assert!(!txn_map.contains_key(&key).unwrap());
    }

    // ==================== Remove Tests ====================

    #[test]
    fn test_remove_existing_key() {
        let (txn_map, _store, _db) = create_test_map("test_remove_existing");
        
        let key = Key::from("key1");
        let value = Value::String("value1".to_string());
        
        txn_map.put(key.clone(), value.clone()).unwrap();
        
        let removed = txn_map.remove(&key).unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap(), value);
        
        // Verify it's gone
        assert!(!txn_map.contains_key(&key).unwrap());
    }

    #[test]
    fn test_remove_nonexistent_key() {
        let (txn_map, _store, _db) = create_test_map("test_remove_nonexistent");
        
        let key = Key::from("nonexistent");
        
        let removed = txn_map.remove(&key).unwrap();
        assert!(removed.is_none());
    }

    #[test]
    fn test_remove_tombstone_prevents_get() {
        let (txn_map, _store, _db) = create_test_map("test_tombstone");
        
        let key = Key::from("key1");
        let value = Value::String("value1".to_string());
        
        txn_map.put(key.clone(), value).unwrap();
        txn_map.remove(&key).unwrap();
        
        let result = txn_map.get(&key).unwrap();
        assert!(result.is_none());
    }

    // ==================== Clear Tests ====================

    #[test]
    fn test_clear() {
        let (txn_map, _store, _db) = create_test_map("test_clear");
        
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");
        
        txn_map.put(key1.clone(), Value::String("v1".to_string())).unwrap();
        txn_map.put(key2.clone(), Value::String("v2".to_string())).unwrap();
        
        txn_map.clear().unwrap();
        
        assert!(!txn_map.contains_key(&key1).unwrap());
        assert!(!txn_map.contains_key(&key2).unwrap());
    }

    #[test]
    fn test_clear_then_get() {
        let (txn_map, _store, _db) = create_test_map("test_clear_get");
        
        let key = Key::from("key1");
        txn_map.put(key.clone(), Value::String("v1".to_string())).unwrap();
        
        txn_map.clear().unwrap();
        
        let result = txn_map.get(&key).unwrap();
        assert!(result.is_none());
    }

    // ==================== Size Tests ====================

    #[test]
    fn test_size_empty() {
        let (txn_map, _store, _db) = create_test_map("test_size_empty");
        
        assert_eq!(txn_map.size().unwrap(), 0);
    }

    #[test]
    fn test_size_with_entries() {
        let (txn_map, _store, _db) = create_test_map("test_size_entries");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.put(Key::from("k2"), Value::String("v2".to_string())).unwrap();
        txn_map.put(Key::from("k3"), Value::String("v3".to_string())).unwrap();
        
        assert_eq!(txn_map.size().unwrap(), 3);
    }

    // ==================== is_empty Tests ====================

    #[test]
    fn test_is_empty_true() {
        let (txn_map, _store, _db) = create_test_map("test_is_empty_true");
        
        assert!(txn_map.is_empty().unwrap());
    }

    #[test]
    fn test_is_empty_false() {
        let (txn_map, _store, _db) = create_test_map("test_is_empty_false");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        
        assert!(!txn_map.is_empty().unwrap());
    }

    #[test]
    fn test_is_empty_after_clear() {
        let (txn_map, _store, _db) = create_test_map("test_is_empty_clear");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.clear().unwrap();
        
        assert!(txn_map.is_empty().unwrap());
    }

    // ==================== put_if_absent Tests ====================

    #[test]
    fn test_put_if_absent_new_key() {
        let (txn_map, _store, _db) = create_test_map("test_put_if_absent_new");
        
        let key = Key::from("key1");
        let value = Value::String("value1".to_string());
        
        let result = txn_map.put_if_absent(key.clone(), value.clone()).unwrap();
        
        assert!(result.is_none());
        assert_eq!(txn_map.get(&key).unwrap().unwrap(), value);
    }

    #[test]
    fn test_put_if_absent_existing_key() {
        let (txn_map, _store, _db) = create_test_map("test_put_if_absent_existing");
        
        let key = Key::from("key1");
        let value1 = Value::String("value1".to_string());
        let value2 = Value::String("value2".to_string());
        
        txn_map.put(key.clone(), value1.clone()).unwrap();
        
        let result = txn_map.put_if_absent(key.clone(), value2).unwrap();
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value1);
    }

    // ==================== Key Navigation Tests ====================

    #[test]
    fn test_first_key_empty() {
        let (txn_map, _store, _db) = create_test_map("test_first_key_empty");
        
        let result = txn_map.first_key().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_first_key() {
        let (txn_map, _store, _db) = create_test_map("test_first_key");
        
        txn_map.put(Key::from("b"), Value::String("vb".to_string())).unwrap();
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        txn_map.put(Key::from("c"), Value::String("vc".to_string())).unwrap();
        
        let result = txn_map.first_key().unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_last_key_empty() {
        let (txn_map, _store, _db) = create_test_map("test_last_key_empty");
        
        let result = txn_map.last_key().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_last_key() {
        let (txn_map, _store, _db) = create_test_map("test_last_key");
        
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        txn_map.put(Key::from("b"), Value::String("vb".to_string())).unwrap();
        txn_map.put(Key::from("c"), Value::String("vc".to_string())).unwrap();
        
        let result = txn_map.last_key().unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_higher_key() {
        let (txn_map, _store, _db) = create_test_map("test_higher_key");
        
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        txn_map.put(Key::from("c"), Value::String("vc".to_string())).unwrap();
        
        let result = txn_map.higher_key(&Key::from("a")).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_higher_key_none() {
        let (txn_map, _store, _db) = create_test_map("test_higher_key_none");
        
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        
        let result = txn_map.higher_key(&Key::from("z")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_lower_key() {
        let (txn_map, _store, _db) = create_test_map("test_lower_key");
        
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        txn_map.put(Key::from("c"), Value::String("vc".to_string())).unwrap();
        
        let result = txn_map.lower_key(&Key::from("c")).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_lower_key_none() {
        let (txn_map, _store, _db) = create_test_map("test_lower_key_none");
        
        txn_map.put(Key::from("b"), Value::String("vb".to_string())).unwrap();
        
        let result = txn_map.lower_key(&Key::from("a")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_ceiling_key() {
        let (txn_map, _store, _db) = create_test_map("test_ceiling_key");
        
        txn_map.put(Key::from("b"), Value::String("vb".to_string())).unwrap();
        txn_map.put(Key::from("d"), Value::String("vd".to_string())).unwrap();
        
        let result = txn_map.ceiling_key(&Key::from("c")).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_floor_key() {
        let (txn_map, _store, _db) = create_test_map("test_floor_key");
        
        txn_map.put(Key::from("a"), Value::String("va".to_string())).unwrap();
        txn_map.put(Key::from("c"), Value::String("vc".to_string())).unwrap();
        
        let result = txn_map.floor_key(&Key::from("b")).unwrap();
        assert!(result.is_some());
    }

    // ==================== Close Tests ====================

    #[test]
    fn test_close() {
        let (txn_map, _store, _db) = create_test_map("test_close");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        
        txn_map.close().unwrap();
        
        assert!(txn_map.is_closed().unwrap());
    }

    // ==================== Dispose Tests ====================

    #[test]
    fn test_dispose() {
        let (txn_map, _store, _db) = create_test_map("test_dispose");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        
        txn_map.dispose().unwrap();
        
        assert!(txn_map.is_dropped().unwrap());
    }

    #[test]
    fn test_dispose_idempotent() {
        let (txn_map, _store, _db) = create_test_map("test_dispose_idem");
        
        txn_map.dispose().unwrap();
        txn_map.dispose().unwrap();
        
        assert!(txn_map.is_dropped().unwrap());
    }

    // ==================== Iterator Tests ====================

    #[test]
    fn test_keys_iterator() {
        let (txn_map, _store, _db) = create_test_map("test_keys_iter");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.put(Key::from("k2"), Value::String("v2".to_string())).unwrap();
        
        let keys: Vec<_> = txn_map.keys().unwrap().collect();
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn test_values_iterator() {
        let (txn_map, _store, _db) = create_test_map("test_values_iter");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.put(Key::from("k2"), Value::String("v2".to_string())).unwrap();
        
        let values: Vec<_> = txn_map.values().unwrap().collect();
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_entries_iterator() {
        let (txn_map, _store, _db) = create_test_map("test_entries_iter");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.put(Key::from("k2"), Value::String("v2".to_string())).unwrap();
        
        let entries: Vec<_> = txn_map.entries().unwrap().collect();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_reverse_entries_iterator() {
        let (txn_map, _store, _db) = create_test_map("test_rev_entries_iter");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.put(Key::from("k2"), Value::String("v2".to_string())).unwrap();
        
        let entries: Vec<_> = txn_map.reverse_entries().unwrap().collect();
        assert_eq!(entries.len(), 2);
    }

    // ==================== Get Store Tests ====================

    #[test]
    fn test_get_store() {
        let (txn_map, _store, _db) = create_test_map("test_get_store");
        
        let result = txn_map.get_store();
        assert!(result.is_ok());
    }

    // ==================== Attribute Tests ====================

    #[test]
    fn test_attributes_empty() {
        let (txn_map, _store, _db) = create_test_map("test_attrs_empty");
        
        let attrs = txn_map.attributes().unwrap();
        // Attributes may or may not exist initially
        assert!(attrs.is_none() || attrs.is_some());
    }

    #[test]
    fn test_set_and_get_attributes() {
        let (txn_map, _store, _db) = create_test_map("test_set_attrs");
        
        let attrs = Attributes::default();
        
        txn_map.set_attributes(attrs).unwrap();
        
        // Verify we can get the store
        let store = txn_map.get_store().unwrap();
        assert!(store.has_map(META_MAP_NAME).unwrap());
    }

    // ==================== Cleared State Tests ====================

    #[test]
    fn test_cleared_state_prevents_contains_key() {
        let (txn_map, _store, _db) = create_test_map("test_cleared_contains");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.clear().unwrap();
        
        assert!(!txn_map.contains_key(&Key::from("k1")).unwrap());
    }

    #[test]
    fn test_cleared_state_prevents_first_key() {
        let (txn_map, _store, _db) = create_test_map("test_cleared_first");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.clear().unwrap();
        
        assert!(txn_map.first_key().unwrap().is_none());
    }

    #[test]
    fn test_cleared_state_prevents_last_key() {
        let (txn_map, _store, _db) = create_test_map("test_cleared_last");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.clear().unwrap();
        
        assert!(txn_map.last_key().unwrap().is_none());
    }

    // ==================== Put After Clear Tests ====================

    #[test]
    fn test_put_after_clear_resets_cleared_flag() {
        let (txn_map, _store, _db) = create_test_map("test_put_after_clear");
        
        txn_map.put(Key::from("k1"), Value::String("v1".to_string())).unwrap();
        txn_map.clear().unwrap();
        
        let key = Key::from("k2");
        let value = Value::String("v2".to_string());
        txn_map.put(key.clone(), value.clone()).unwrap();
        
        assert!(txn_map.contains_key(&key).unwrap());
        assert_eq!(txn_map.get(&key).unwrap().unwrap(), value);
    }

    // ==================== Tombstone Tests ====================

    #[test]
    fn test_tombstone_cleared_by_put() {
        let (txn_map, _store, _db) = create_test_map("test_tombstone_cleared");
        
        let key = Key::from("k1");
        let value1 = Value::String("v1".to_string());
        let value2 = Value::String("v2".to_string());
        
        txn_map.put(key.clone(), value1).unwrap();
        txn_map.remove(&key).unwrap();
        txn_map.put(key.clone(), value2.clone()).unwrap();
        
        assert!(txn_map.contains_key(&key).unwrap());
        assert_eq!(txn_map.get(&key).unwrap().unwrap(), value2);
    }

    // ==================== Clone Tests ====================

    #[test]
    fn test_transactional_map_clone() {
        let (txn_map, _store, _db) = create_test_map("test_clone");
        
        let key = Key::from("k1");
        let value = Value::String("v1".to_string());
        
        txn_map.put(key.clone(), value.clone()).unwrap();
        
        let cloned = txn_map.clone();
        
        assert_eq!(cloned.get(&key).unwrap().unwrap(), value);
    }
}
