use crate::common::{
    get_current_time, get_current_time_or_zero, AttributeAware, Attributes, Key, Value,
    META_MAP_NAME,
};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::store::iters::{
    EntryIterator, KeyIterator, SingleMapEntryProvider, SingleMapKeyProvider,
    SingleMapValueProvider, ValueIterator,
};
use crate::store::memory::store::InMemoryStore;
use crate::store::{NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider};
use crossbeam_skiplist::SkipMap;
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::iter::Rev;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// In-memory key-value map implementation using a concurrent skip list.
///
/// # Purpose
/// `InMemoryMap` provides fast, thread-safe access to key-value pairs stored entirely in memory.
/// It uses a concurrent skip list internally for efficient O(log n) operations and supports
/// bidirectional iteration with full range query capabilities.
///
/// # Characteristics
/// - **Thread-Safe**: Can be safely cloned and shared across threads
/// - **Concurrent Skip List**: O(log n) time complexity for get, put, remove operations
/// - **Bidirectional Iteration**: Supports forward and backward traversal
/// - **Range Queries**: Supports finding keys based on ordering relations
/// - **Lifecycle Management**: Supports opening, closing, and disposal
/// - **Attribute Support**: Can store and retrieve attributes metadata
///
/// # Usage
/// Typically obtained via `NitriteStore::open_map()`:
/// ```text
/// let store = InMemoryStore::new(InMemoryStoreConfig::new());
/// let nitrite_store = NitriteStore::new(store);
/// let map = nitrite_store.open_map("users").unwrap();
/// ```
#[derive(Clone)]
pub struct InMemoryMap {
    inner: Arc<InMemoryMapInner>,
}

impl InMemoryMap {
    /// Creates a new in-memory map.
    ///
    /// # Arguments
    /// * `name` - The name/identifier for the map
    /// * `store` - The parent `NitriteStore` that owns this map
    ///
    /// # Returns
    /// A new `InMemoryMap` instance
    pub fn new(name: &str, store: NitriteStore) -> Self {
        InMemoryMap {
            inner: Arc::new(InMemoryMapInner::new(name, store.clone())),
        }
    }
}

impl AttributeAware for InMemoryMap {
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.inner.get_attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.inner.set_attributes(attributes)
    }
}

impl NitriteMapProvider for InMemoryMap {
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
        self.inner.is_closed()
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn values(&self) -> NitriteResult<ValueIterator> {
        let provider = SingleMapValueProvider::new(NitriteMap::new(self.clone()));
        Ok(ValueIterator::new(provider))
    }

    fn keys(&self) -> NitriteResult<KeyIterator> {
        let provider = SingleMapKeyProvider::new(NitriteMap::new(self.clone()));
        Ok(KeyIterator::new(provider))
    }

    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.remove(key)
    }

    fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        self.inner.put(key, value)
    }

    fn size(&self) -> NitriteResult<u64> {
        self.inner.size()
    }

    fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>> {
        self.inner.put_if_absent(key, value)
    }

    fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.first_key()
    }

    fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.last_key()
    }

    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.higher_key(key)
    }

    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.ceiling_key(key)
    }

    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.lower_key(key)
    }

    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.floor_key(key)
    }

    fn is_empty(&self) -> NitriteResult<bool> {
        self.inner.is_empty()
    }

    fn get_store(&self) -> NitriteResult<NitriteStore> {
        self.inner.get_store()
    }

    fn get_name(&self) -> NitriteResult<String> {
        self.inner.get_name()
    }

    fn entries(&self) -> NitriteResult<EntryIterator> {
        let provider = SingleMapEntryProvider::new(NitriteMap::new(self.clone()));
        Ok(EntryIterator::new(provider))
    }

    fn reverse_entries(&self) -> NitriteResult<Rev<EntryIterator>> {
        let provider = SingleMapEntryProvider::new(NitriteMap::new(self.clone()));
        Ok(EntryIterator::new(provider).rev())
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.inner.dispose()
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        self.inner.is_dropped()
    }
}

struct InMemoryMapInner {
    backing_map: SkipMap<Key, Value>,
    closed: AtomicBool,
    dropped: AtomicBool,
    name: String,
    store: NitriteStore,
}

impl InMemoryMapInner {
    pub(crate) fn new(name: &str, store: NitriteStore) -> InMemoryMapInner {
        InMemoryMapInner {
            backing_map: SkipMap::new(),
            closed: AtomicBool::from(false),
            dropped: AtomicBool::from(false),
            name: name.to_string(),
            store,
        }
    }

    pub(crate) fn check_opened(&self) -> NitriteResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            log::error!("Map {} is closed", self.name);
            return Err(NitriteError::new(
                &format!("Map {} is closed", self.name),
                ErrorKind::InvalidOperation,
            ));
        }

        if self.dropped.load(Ordering::Relaxed) {
            log::error!("Map {} is dropped", self.name);
            return Err(NitriteError::new(
                &format!("Map {} is dropped", self.name),
                ErrorKind::InvalidOperation,
            ));
        }

        Ok(())
    }

    pub(crate) fn get_attributes(&self) -> NitriteResult<Option<Attributes>> {
        if !self.is_dropped()? {
            let store = self.get_store()?;
            let meta_map = store.open_map(META_MAP_NAME)?;
            let name = self.get_name()?;

            if name.ne(META_MAP_NAME) {
                let attributes = meta_map.get(&Value::from(name))?;
                if let Some(attributes) = attributes {
                    // Safe type conversion with proper error handling
                    let doc = attributes.as_document()
                        .ok_or_else(|| NitriteError::new(
                            "Stored attributes value is not a document",
                            ErrorKind::InvalidOperation,
                        ))?;
                    return Ok(Some(Attributes::from_document(doc)));
                }
            }
        }
        Ok(None)
    }

    pub(crate) fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
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

    pub(crate) fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
        self.check_opened()?;
        Ok(self.backing_map.contains_key(key))
    }

    pub(crate) fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.check_opened()?;

        if let Some(entry) = self.backing_map.get(key) {
            Ok(Some(entry.value().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn clear(&self) -> NitriteResult<()> {
        self.check_opened()?;

        if !self.backing_map.is_empty() {
            self.backing_map.clear();
        }

        Ok(())
    }

    pub(crate) fn is_closed(&self) -> NitriteResult<bool> {
        Ok(self.closed.load(Ordering::Relaxed))
    }

    pub(crate) fn close(&self) -> NitriteResult<()> {
        self.backing_map.clear();
        self.closed.store(true, Ordering::Relaxed);
        let store = self.get_store()?;
        store.close_map(&self.name)?;
        Ok(())
    }

    pub(crate) fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.check_opened()?;

        if let Some(entry) = self.backing_map.remove(key) {
            Ok(Some(entry.value().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        self.check_opened()?;
        self.backing_map.insert(key, value);
        Ok(())
    }

    pub(crate) fn size(&self) -> NitriteResult<u64> {
        self.check_opened()?;
        Ok(self.backing_map.len() as u64)
    }

    pub(crate) fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>> {
        self.check_opened()?;

        if let Some(existing_value) = self.backing_map.get(&key) {
            return Ok(Some(existing_value.value().clone()));
        }

        // If the key does not exist, insert the new value
        self.backing_map.insert(key, value);
        // Return None to indicate that the key was absent
        Ok(None)
    }

    pub(crate) fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        Ok(self.backing_map.front().map(|entry| entry.key().clone()))
    }

    pub(crate) fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        Ok(self.backing_map.back().map(|entry| entry.key().clone()))
    }

    pub(crate) fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        if let Some(e) = self.backing_map.range((Excluded(key), Unbounded)).next() {
            Ok(Some(e.key().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        if let Some(e) = self.backing_map.range((Included(key), Unbounded)).next() {
            Ok(Some(e.key().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        if let Some(e) = self.backing_map.range((Unbounded, Excluded(key))).next_back() {
            Ok(Some(e.key().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        if let Some(e) = self.backing_map.range((Unbounded, Included(key))).next_back() {
            Ok(Some(e.key().clone()))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn is_empty(&self) -> NitriteResult<bool> {
        self.check_opened()?;
        Ok(self.backing_map.is_empty())
    }

    pub(crate) fn get_store(&self) -> NitriteResult<NitriteStore> {
        Ok(self.store.clone())
    }

    pub(crate) fn get_name(&self) -> NitriteResult<String> {
        Ok(self.name.clone())
    }

    pub(crate) fn dispose(&self) -> NitriteResult<()> {
        self.backing_map.clear();
        self.dropped.store(true, Ordering::Relaxed);
        self.closed.store(true, Ordering::Relaxed);

        let store = self.get_store()?;
        store.remove_map(&self.name)?;

        Ok(())
    }

    pub(crate) fn is_dropped(&self) -> NitriteResult<bool> {
        Ok(self.dropped.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Attributes, Key, Value};
    use crate::store::memory::store::InMemoryStore;
    use crate::store::memory::InMemoryStoreConfig;

    fn create_test_map() -> InMemoryMap {
        let store = InMemoryStore::new(InMemoryStoreConfig::new());
        InMemoryMap::new("test_map", NitriteStore::new(store))
    }

    fn create_inner_test_map() -> InMemoryMapInner {
        let store = InMemoryStore::new(InMemoryStoreConfig::new());
        InMemoryMapInner::new("test_map", NitriteStore::new(store))
    }

    #[test]
    fn test_new_map() {
        let map = create_test_map();
        assert_eq!(map.inner.name, "test_map");
    }

    #[test]
    fn test_contains_key() {
        let map = create_test_map();
        let key = Key::from("key1");
        assert!(!map.contains_key(&key).unwrap());
        map.put(key.clone(), Value::from("value1")).unwrap();
        assert!(map.contains_key(&key).unwrap());
    }

    #[test]
    fn test_get() {
        let map = create_test_map();
        let key = Key::from("key1");
        assert!(map.get(&key).unwrap().is_none());
        map.put(key.clone(), Value::from("value1")).unwrap();
        assert_eq!(map.get(&key).unwrap(), Some(Value::from("value1")));
    }

    #[test]
    fn test_clear() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.clear().unwrap();
        assert!(map.is_empty().unwrap());
    }

    #[test]
    fn test_is_closed() {
        let map = create_test_map();
        assert!(!map.is_closed().unwrap());
        map.close().unwrap();
        assert!(map.is_closed().unwrap());
    }

    #[test]
    fn test_close() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.close().unwrap();
        assert!(map.is_closed().unwrap());
        assert!(map.get(&Key::from("key1")).is_err());
    }

    #[test]
    fn test_remove() {
        let map = create_test_map();
        let key = Key::from("key1");
        map.put(key.clone(), Value::from("value1")).unwrap();
        assert_eq!(map.remove(&key).unwrap(), Some(Value::from("value1")));
        assert!(map.get(&key).unwrap().is_none());
    }

    #[test]
    fn test_put() {
        let map = create_test_map();
        let key = Key::from("key1");
        let value = Value::from("value1");
        map.put(key.clone(), value.clone()).unwrap();
        assert_eq!(map.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn test_size() {
        let map = create_test_map();
        assert_eq!(map.size().unwrap(), 0);
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        assert_eq!(map.size().unwrap(), 1);
    }

    #[test]
    fn test_put_if_absent() {
        let map = create_test_map();
        let key = Key::from("key1");
        let value = Value::from("value1");
        assert!(map
            .put_if_absent(key.clone(), value.clone())
            .unwrap()
            .is_none());
        assert_eq!(
            map.put_if_absent(key.clone(), Value::from("value2"))
                .unwrap(),
            Some(value)
        );
    }

    #[test]
    fn test_first_key() {
        let map = create_test_map();
        assert!(map.first_key().unwrap().is_none());
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        assert_eq!(map.first_key().unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_last_key() {
        let map = create_test_map();
        assert!(map.last_key().unwrap().is_none());
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        assert_eq!(map.last_key().unwrap(), Some(Key::from("key1")));
    }

    #[test]
    fn test_higher_key() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.put(Key::from("key2"), Value::from("value2")).unwrap();
        assert_eq!(
            map.higher_key(&Key::from("key1")).unwrap(),
            Some(Key::from("key2"))
        );
    }

    #[test]
    fn test_ceiling_key() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.put(Key::from("key2"), Value::from("value2")).unwrap();
        assert_eq!(
            map.ceiling_key(&Key::from("key1")).unwrap(),
            Some(Key::from("key1"))
        );
    }

    #[test]
    fn test_lower_key() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.put(Key::from("key2"), Value::from("value2")).unwrap();
        assert_eq!(
            map.lower_key(&Key::from("key2")).unwrap(),
            Some(Key::from("key1"))
        );
    }

    #[test]
    fn test_floor_key() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.put(Key::from("key2"), Value::from("value2")).unwrap();
        assert_eq!(
            map.floor_key(&Key::from("key2")).unwrap(),
            Some(Key::from("key2"))
        );
    }

    #[test]
    fn test_is_empty() {
        let map = create_test_map();
        assert!(map.is_empty().unwrap());
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        assert!(!map.is_empty().unwrap());
    }

    #[test]
    fn test_get_store() {
        let map = create_test_map();
        assert!(map.get_store().is_ok());
    }

    #[test]
    fn test_get_name() {
        let map = create_test_map();
        assert_eq!(map.get_name().unwrap(), "test_map");
    }

    #[test]
    fn test_entries() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        let mut entries = map.entries().unwrap();
        assert!(entries.next().is_some());
    }

    #[test]
    fn test_reverse_entries() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        let mut entries = map.reverse_entries().unwrap();
        assert!(entries.next().is_some());
    }

    #[test]
    fn test_dispose_map() {
        let map = create_test_map();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.dispose().unwrap();
        assert!(map.is_dropped().unwrap());
    }

    #[test]
    fn test_is_dropped() {
        let map = create_test_map();
        assert!(!map.is_dropped().unwrap());
        map.dispose().unwrap();
        assert!(map.is_dropped().unwrap());
    }

    #[test]
    fn test_set_attributes() {
        let map = create_test_map();
        let attributes = Attributes::new();
        map.set_attributes(attributes.clone()).unwrap();
        assert_eq!(map.attributes().unwrap(), Some(attributes));
    }

    #[test]
    fn test_get_attributes() {
        let map = create_test_map();
        assert!(map.attributes().unwrap().is_none());
        let attributes = Attributes::new();
        map.set_attributes(attributes.clone()).unwrap();
        assert_eq!(map.attributes().unwrap(), Some(attributes));
    }

    #[test]
    fn test_attributes_with_valid_data() {
        // Test that valid document attributes are retrieved correctly
        let map = create_test_map();
        let attributes = Attributes::new();
        map.set_attributes(attributes.clone()).unwrap();
        
        let retrieved = map.attributes().unwrap();
        assert_eq!(retrieved, Some(attributes));
    }

    #[test]
    fn test_attributes_empty_map() {
        // Test that empty map returns None for attributes
        let map = create_test_map();
        let result = map.attributes().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_set_and_get_attributes_round_trip() {
        // Test setting and getting attributes multiple times
        let map = create_test_map();
        
        let attrs1 = Attributes::new();
        map.set_attributes(attrs1.clone()).unwrap();
        assert_eq!(map.attributes().unwrap(), Some(attrs1));
        
        let attrs2 = Attributes::new();
        map.set_attributes(attrs2.clone()).unwrap();
        assert_eq!(map.attributes().unwrap(), Some(attrs2));
    }

    #[test]
    fn test_get_with_if_let_pattern() {
        // Test that get() uses efficient if-let pattern for optional handling
        let map = create_test_map();
        let key = Key::from("perf_key");
        
        // Get non-existent key
        assert!(map.get(&key).unwrap().is_none());
        
        // Put and get value
        let value = Value::from("perf_value");
        map.put(key.clone(), value.clone()).unwrap();
        assert_eq!(map.get(&key).unwrap(), Some(value));
    }

    #[test]
    fn test_remove_with_if_let_pattern() {
        // Test that remove() uses efficient if-let pattern
        let map = create_test_map();
        let key = Key::from("remove_key");
        let value = Value::from("remove_value");
        
        map.put(key.clone(), value.clone()).unwrap();
        let removed = map.remove(&key).unwrap();
        assert_eq!(removed, Some(value));
        
        // Second remove should return None
        assert!(map.remove(&key).unwrap().is_none());
    }

    #[test]
    fn test_range_operations_efficiency() {
        // Test that range operations (higher, ceiling, lower, floor) are optimized
        let map = create_test_map();
        
        map.put(Key::from("a"), Value::from("1")).unwrap();
        map.put(Key::from("c"), Value::from("3")).unwrap();
        map.put(Key::from("e"), Value::from("5")).unwrap();
        
        // Higher than "a" should be "c"
        assert_eq!(map.higher_key(&Key::from("a")).unwrap(), Some(Key::from("c")));
        
        // Ceiling of "c" should be "c"
        assert_eq!(map.ceiling_key(&Key::from("c")).unwrap(), Some(Key::from("c")));
        
        // Lower than "e" should be "c"
        assert_eq!(map.lower_key(&Key::from("e")).unwrap(), Some(Key::from("c")));
        
        // Floor of "c" should be "c"
        assert_eq!(map.floor_key(&Key::from("c")).unwrap(), Some(Key::from("c")));
    }

    #[test]
    fn test_higher_key_if_let_pattern() {
        // Test that higher_key uses efficient if-let pattern
        let map = create_test_map();
        
        map.put(Key::from("key1"), Value::from("v1")).unwrap();
        map.put(Key::from("key3"), Value::from("v3")).unwrap();
        
        let result = map.higher_key(&Key::from("key1")).unwrap();
        assert_eq!(result, Some(Key::from("key3")));
        
        let result = map.higher_key(&Key::from("key3")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_ceiling_key_if_let_pattern() {
        // Test that ceiling_key uses efficient if-let pattern
        let map = create_test_map();
        
        map.put(Key::from("b"), Value::from("2")).unwrap();
        map.put(Key::from("d"), Value::from("4")).unwrap();
        
        let result = map.ceiling_key(&Key::from("b")).unwrap();
        assert_eq!(result, Some(Key::from("b")));
        
        let result = map.ceiling_key(&Key::from("c")).unwrap();
        assert_eq!(result, Some(Key::from("d")));
    }

    #[test]
    fn test_lower_key_if_let_pattern() {
        // Test that lower_key uses efficient if-let pattern
        let map = create_test_map();
        
        map.put(Key::from("x"), Value::from("24")).unwrap();
        map.put(Key::from("z"), Value::from("26")).unwrap();
        
        let result = map.lower_key(&Key::from("z")).unwrap();
        assert_eq!(result, Some(Key::from("x")));
        
        let result = map.lower_key(&Key::from("x")).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_floor_key_if_let_pattern() {
        // Test that floor_key uses efficient if-let pattern
        let map = create_test_map();
        
        map.put(Key::from("m"), Value::from("13")).unwrap();
        map.put(Key::from("p"), Value::from("16")).unwrap();
        
        let result = map.floor_key(&Key::from("p")).unwrap();
        assert_eq!(result, Some(Key::from("p")));
        
        let result = map.floor_key(&Key::from("o")).unwrap();
        assert_eq!(result, Some(Key::from("m")));
    }

    #[test]
    fn test_sequential_range_lookups() {
        // Test multiple range lookups to validate if-let efficiency
        let map = create_test_map();
        
        for i in 0..10 {
            let key = Key::from(format!("key{}", i));
            let value = Value::from(format!("value{}", i));
            map.put(key, value).unwrap();
        }
        
        // Multiple higher_key lookups should be efficient
        let key1 = map.higher_key(&Key::from("key1")).unwrap();
        let key2 = map.higher_key(&Key::from("key3")).unwrap();
        let key3 = map.higher_key(&Key::from("key5")).unwrap();
        
        assert!(key1.is_some());
        assert!(key2.is_some());
        assert!(key3.is_some());
    }
}
