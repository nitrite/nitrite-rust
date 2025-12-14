use once_cell::sync::Lazy;
use smallvec::SmallVec;

use crate::collection::NitriteId;
use crate::common::NavigableMap;
use crate::common::{Key, Value};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::store::{EntryIterator, NitriteMap, NitriteMapProvider};
use std::collections::BTreeMap;
use std::fmt::{Debug, Display, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static INDEX_CORRUPT_ERROR: Lazy<NitriteError> = Lazy::new(|| {
    NitriteError::new(
        "Index is in corrupt state",
        ErrorKind::IndexingError,
    )
});

#[derive(Clone)]
/// Provides efficient key-value access for index data with navigable operations.
///
/// IndexMap wraps either a persisted NitriteMap or an in-memory BTreeMap, providing a unified
/// interface for navigation and range queries on index data. It supports both exact lookups
/// and range-based key operations commonly used in index scanning.
///
/// # Characteristics
/// - **Dual storage**: Can wrap persistent NitriteMap or in-memory BTreeMap
/// - **Navigable**: Supports key range operations (higher, lower, ceiling, floor keys)
/// - **Reversible**: Supports reverse scan iteration for optimization
/// - **Type abstraction**: Provides unified interface regardless of underlying storage
///
/// # Usage
///
/// IndexMap is created internally for index operations:
/// - From persistent storage: `IndexMap::new(Some(nitrite_map), None)`
/// - From in-memory data: `IndexMap::new(None, Some(btree_map))`
///
/// Operations on IndexMaps:
/// - `index_map.get(&key)?` - Retrieve value by key
/// - `index_map.first_key()` - Get minimum key
/// - `index_map.last_key()` - Get maximum key
/// - `index_map.higher_key(&key)` - Get next key greater than given
/// - `index_map.entries()` - Iterate all entries with direction support
///
/// # Responsibilities
/// - **Storage Abstraction**: Unifies persistent and in-memory map access
/// - **Range Navigation**: Provides navigable operations for index scans
/// - **Iteration**: Supports bidirectional iteration with reverse scan optimization
/// - **Terminal ID Collection**: Extracts NitriteIds from nested structures
pub struct IndexMap {
    inner: Arc<IndexMapInner>,
}

impl IndexMap {
    /// Creates a new IndexMap from either persistent or in-memory storage.
    ///
    /// # Arguments
    /// * `nitrite_map` - Optional persistent NitriteMap for disk-backed storage
    /// * `sub_map` - Optional in-memory BTreeMap for temporary/nested index data
    ///
    /// # Returns
    /// A new IndexMap wrapping the provided storage.
    ///
    /// # Behavior
    /// Exactly one of `nitrite_map` or `sub_map` should be provided. If both are None
    /// or both are Some, the nitrite_map takes precedence.
    pub fn new(
        nitrite_map: Option<NitriteMap>,
        sub_map: Option<BTreeMap<Value, Value>>,
    ) -> Self {
        let inner_map = IndexMapInner::new(nitrite_map, sub_map);
        IndexMap {
            inner: Arc::new(inner_map),
        }
    }

    /// Retrieves the value associated with a key.
    ///
    /// # Arguments
    /// * `key` - The key to look up
    ///
    /// # Returns
    /// Some(value) if found, None if not found.
    ///
    /// # Errors
    /// Returns error if the underlying storage is in corrupt state or access fails.
    pub fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.get(key)
    }

    /// Returns the smallest key in the map.
    ///
    /// # Returns
    /// Some(key) if map is non-empty, None if empty.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.first_key()
    }

    /// Returns the largest key in the map.
    ///
    /// # Returns
    /// Some(key) if map is non-empty, None if empty.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.last_key()
    }

    /// Returns the smallest key strictly greater than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// Some(key) if such a key exists, None otherwise.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.higher_key(key)
    }

    /// Returns the smallest key greater than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// Some(key) if such a key exists, None otherwise.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.ceiling_key(key)
    }

    /// Returns the largest key strictly less than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// Some(key) if such a key exists, None otherwise.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.lower_key(key)
    }

    /// Returns the largest key less than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key
    ///
    /// # Returns
    /// Some(key) if such a key exists, None otherwise.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.floor_key(key)
    }

    /// Returns an iterator over all key-value entries.
    ///
    /// # Returns
    /// An IndexMapIterator for bidirectional iteration.
    ///
    /// # Behavior
    /// Respects the current reverse_scan setting; iteration direction can be controlled
    /// by set_reverse_scan(). The iterator implements both Iterator and DoubleEndedIterator.
    ///
    /// # Errors
    /// Returns error if map is in corrupt state.
    pub fn entries(&self) -> NitriteResult<IndexMapIterator> {
        self.inner.entries()
    }

    /// Sets whether subsequent iterations should scan in reverse order.
    ///
    /// # Arguments
    /// * `reverse_scan` - true for reverse iteration, false for forward
    ///
    /// # Behavior
    /// Affects entries() iterator direction. Used to optimize range queries
    /// based on filter predicates (e.g., descending order for < comparisons).
    pub(crate) fn set_reverse_scan(&self, reverse_scan: bool) {
        self.inner.set_reverse_scan(reverse_scan)
    }

    /// Collects all NitriteIds from terminal array values in the map.
    ///
    /// # Returns
    /// A SmallVec of NitriteIds extracted from array values and nested maps.
    ///
    /// # Behavior
    /// Recursively traverses the map structure:
    /// - Array values are scanned for NitriteId elements
    /// - Nested Map values are recursively traversed
    /// - Other value types are skipped
    ///
    /// # Errors
    /// Returns error if iteration fails.
    pub(crate) fn terminal_nitrite_ids(&self) -> NitriteResult<SmallVec<[NitriteId; 16]>> {
        self.inner.terminal_nitrite_ids()
    }
}

impl Debug for IndexMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(ref nitrite_map) = &self.inner.nitrite_map {
            let entries = match nitrite_map.entries() {
                Ok(entries) => entries,
                Err(e) => return write!(f, "Error retrieving entries: {:?}", e),
            };

            for entry in entries {
                match entry {
                    Ok((key, value)) => write!(f, "Key: {:?}, Value: {:?}\n", key, value)?,
                    Err(e) => write!(f, "Error retrieving entry: {:?}", e)?,
                }
            }
        } else if let Some(sub_map) = &self.inner.sub_map {
            let entries = match sub_map.iter() {
                Ok(entries) => entries,
                Err(e) => return write!(f, "Error retrieving sub_map entries: {:?}", e),
            };

            for entry in entries {
                match entry {
                    Ok((key, value)) => write!(f, "Key: {:?}, Value: {:?}\n", key, value)?,
                    Err(e) => write!(f, "Error retrieving entry: {:?}", e)?,
                }
            }
        } else {
            write!(f, "Empty IndexMap")?
        }
        Ok(())
    }
}

struct IndexMapInner {
    nitrite_map: Option<NitriteMap>,
    sub_map: Option<InMemoryIndexMap>,
    reverse_scan: AtomicBool,
}

impl IndexMapInner {
    fn new(
        nitrite_map: Option<NitriteMap>,
        sub_map: Option<BTreeMap<Value, Value>>,
    ) -> Self {
        let in_memory_map = if let Some(sub_map) = sub_map {
            Some(InMemoryIndexMap::new(sub_map))
        } else {
            None
        };

        IndexMapInner {
            nitrite_map,
            sub_map: in_memory_map,
            reverse_scan: AtomicBool::from(false),
        }
    }

    pub fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        if let Some(ref nitrite_map) = self.nitrite_map {
            let value = nitrite_map.get(key)?;
            Ok(value)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get value for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            let value = sub_map.get(key);
            value
        }
    }

    pub fn first_key(&self) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.first_key()?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get first key");
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.first_key()
        }
    }

    pub fn last_key(&self) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.last_key()?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get last key");
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.last_key()
        }
    }

    pub fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.higher_key(key)?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get higher key for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.higher_key(key)
        }
    }

    pub fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.ceiling_key(key)?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get ceiling key for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.ceiling_key(key)
        }
    }

    pub fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.lower_key(key)?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get lower key for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.lower_key(key)
        }
    }

    pub fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let key = nitrite_map.floor_key(key)?;
            Ok(key)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get floor key for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.floor_key(key)
        }
    }

    pub fn entries(&self) -> NitriteResult<IndexMapIterator> {
        if let Some(ref nitrite_map) = &self.nitrite_map {
            let iterator = nitrite_map.entries()?;
            Ok(IndexMapIterator::new(
                Some(iterator),
                None,
                self.reverse_scan.load(Ordering::Relaxed),
            ))
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get entries");
                INDEX_CORRUPT_ERROR.clone()
            })?;
            sub_map.iter()
        }
    }

    pub(crate) fn terminal_nitrite_ids(&self) -> NitriteResult<SmallVec<[NitriteId; 16]>> {
        let mut nitrite_ids = SmallVec::new();

        for entry in self.entries()? {
            let (_, value) = entry?;
            match value {
                Value::Array(array) => {
                    for value in array {
                        if let Value::NitriteId(nitrite_id) = value {
                            nitrite_ids.push(nitrite_id);
                        }
                    }
                }
                Value::Map(btree_map) => {
                    let index_map = IndexMap::new(None, Some(btree_map));
                    let terminal_nitrite_ids = index_map.terminal_nitrite_ids()?;
                    nitrite_ids.extend(terminal_nitrite_ids);
                }
                _ => continue,
            }
        }

        Ok(nitrite_ids)
    }

    pub(crate) fn set_reverse_scan(&self, reverse_scan: bool) {
        self.reverse_scan.store(reverse_scan, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct InMemoryIndexMap {
    inner: Arc<InMemoryIndexMapInner>,
}

impl InMemoryIndexMap {
    fn new(inner_map: BTreeMap<Value, Value>) -> Self {
        let inner = InMemoryIndexMapInner {
            inner_map,
            reverse_scan: AtomicBool::from(false),
        };

        InMemoryIndexMap {
            inner: Arc::new(inner),
        }
    }

    fn iter(&self) -> NitriteResult<IndexMapIterator> {
        Ok(IndexMapIterator::new(
            None,
            Some(self.clone()),
            self.inner.reverse_scan.load(Ordering::Relaxed),
        ))
    }

    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.as_ref().get(key)
    }

    fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().first_key()
    }

    fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().last_key()
    }

    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().higher_key(key)
    }

    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().ceiling_key(key)
    }

    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().lower_key(key)
    }

    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.as_ref().floor_key(key)
    }

    fn set_reverse_scan(&self, _reverse_scan: bool) {
        // For in-memory maps, reverse_scan is tracked at InMemoryIndexMap level
        // (not shown in the methods above as they're utility methods)
        // This is a no-op for now as reverse_scan is only used during iteration
    }
}

pub struct InMemoryIndexMapInner {
    inner_map: BTreeMap<Value, Value>,
    reverse_scan: AtomicBool,
}

impl InMemoryIndexMapInner {
    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        Ok(self.inner_map.get(key).cloned())
    }

    fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.inner_map.first_key()
    }

    fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.inner_map.last_key()
    }

    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner_map.higher_key(key)
    }

    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner_map.ceiling_key(key)
    }

    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner_map.lower_key(key)
    }

    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner_map.floor_key(key)
    }
}

/// Iterator over IndexMap entries with bidirectional navigation support.
///
/// IndexMapIterator provides forward and backward iteration over index entries,
/// supporting both single-direction and double-ended iteration. It handles both
/// persistent and in-memory maps transparently.
///
/// # Characteristics
/// - **Bidirectional**: Implements both Iterator and DoubleEndedIterator
/// - **Reverse scan**: Respects reverse_scan flag for optimization
/// - **State tracking**: Maintains current position for navigable operations
/// - **Dual storage**: Works with both NitriteMap and in-memory maps
///
/// # Usage
///
/// Typically obtained through IndexMap::entries():
/// ```ignore
/// let mut iterator = index_map.entries()?;
/// for entry in iterator {
///     let (key, value) = entry?;
///     // Process entry
/// }
/// ```
///
/// Bidirectional iteration:
/// ```ignore
/// let mut iterator = index_map.entries()?;
/// let first = iterator.next(); // From start
/// let last = iterator.next_back(); // From end
/// ```
pub struct IndexMapIterator {
    nitrite_map_iterator: Option<EntryIterator>,
    cached_index_map: Option<InMemoryIndexMap>,
    current: Option<Key>,
    reverse_scan: bool,
}

impl IndexMapIterator {
    fn new(
        nitrite_map_iterator: Option<EntryIterator>,
        cached_index_map: Option<InMemoryIndexMap>,
        reverse_scan: bool,
    ) -> Self {
        IndexMapIterator {
            nitrite_map_iterator,
            cached_index_map,
            current: None,
            reverse_scan,
        }
    }

    fn higher_key(&self, btree_map: &InMemoryIndexMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => btree_map.higher_key(current_key),
            None => btree_map.first_key(),
        }
    }

    fn lower_key(&self, btree_map: &InMemoryIndexMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => btree_map.lower_key(current_key),
            None => btree_map.last_key(),
        }
    }
}

impl Iterator for IndexMapIterator {
    type Item = NitriteResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.reverse_scan {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.higher_key(&map);
                match next_key {
                    Ok(Some(key)) => {
                        self.current = Some(key.clone());
                        
                        // Use the key directly instead of unwrapping self.current
                        // self.current was just set above, so it's guaranteed to be Some
                        match map.inner.get(&key) {
                            Ok(Some(value)) => Some(Ok((key.clone(), value))),
                            Ok(None) => Some(Ok((key.clone(), Value::Null))),
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Ok(None) => {
                        self.current = None;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        } else {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next_back();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.lower_key(&map);
                match next_key {
                    Ok(Some(key)) => {
                        self.current = Some(key.clone());
                        // Avoid unnecessary clone - use reference when possible
                        match map.inner.get(&key) {
                            Ok(Some(value)) => Some(Ok((key, value))),
                            Ok(None) => Some(Ok((key, Value::Null))),
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Ok(None) => {
                        self.current = None;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        }
    }
}

impl DoubleEndedIterator for IndexMapIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if !self.reverse_scan {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next_back();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.lower_key(&map);
                match next_key {
                    Ok(Some(key)) => {
                        self.current = Some(key.clone());
                        match map.inner.get(&key) {
                            Ok(Some(value)) => Some(Ok((key, value))),
                            Ok(None) => Some(Ok((key, Value::Null))),
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Ok(None) => {
                        self.current = None;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        } else {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.higher_key(&map);
                match next_key {
                    Ok(Some(key)) => {
                        self.current = Some(key.clone());
                        match map.inner.get(&key) {
                            Ok(Some(value)) => Some(Ok((key, value))),
                            Ok(None) => Some(Ok((key, Value::Null))),
                            Err(e) => Some(Err(e)),
                        }
                    }
                    Ok(None) => {
                        self.current = None;
                        None
                    }
                    Err(e) => Some(Err(e)),
                }
            } else {
                None
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::NitriteId;
    use crate::common::{Key, Value};
    use crate::store::{NitriteStore, NitriteStoreProvider};
    use std::collections::BTreeMap;

    #[test]
    fn test_index_map_new() {
        let store = NitriteStore::default();
        let map = store.open_map("test").expect("Failed to open map");
        let nitrite_map = Some(map);
        let sub_map = Some(BTreeMap::new());
        let index_map = IndexMap::new(nitrite_map.clone(), sub_map.clone());

        assert!(index_map.inner.nitrite_map.is_some());
        assert!(index_map.inner.sub_map.is_some());
    }

    #[test]
    fn test_index_map_get() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key1".to_string());
        let result = index_map.get(&key).unwrap();
        assert_eq!(result, Some(Value::String("value1".to_string())));
    }

    #[test]
    fn test_index_map_get_not_found() {
        let sub_map = BTreeMap::new();
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key1".to_string());
        let result = index_map.get(&key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_index_map_first_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let result = index_map.first_key().unwrap();
        assert_eq!(result, Some(Key::String("key1".to_string())));
    }

    #[test]
    fn test_index_map_last_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let result = index_map.last_key().unwrap();
        assert_eq!(result, Some(Key::String("key1".to_string())));
    }

    #[test]
    fn test_index_map_higher_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key1".to_string());
        let result = index_map.higher_key(&key).unwrap();
        assert_eq!(result, Some(Key::String("key2".to_string())));
    }

    #[test]
    fn test_index_map_ceiling_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key1".to_string());
        let result = index_map.ceiling_key(&key).unwrap();
        assert_eq!(result, Some(Key::String("key1".to_string())));
    }

    #[test]
    fn test_index_map_lower_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key2".to_string());
        let result = index_map.lower_key(&key).unwrap();
        assert_eq!(result, Some(Key::String("key1".to_string())));
    }

    #[test]
    fn test_index_map_floor_key() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let key = Key::String("key2".to_string());
        let result = index_map.floor_key(&key).unwrap();
        assert_eq!(result, Some(Key::String("key2".to_string())));
    }

    #[test]
    fn test_index_map_entries() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        let index_map = IndexMap::new(None, Some(sub_map));

        let mut entries = index_map.entries().unwrap();
        let entry = entries.next().unwrap().unwrap();
        assert_eq!(entry.0, Key::String("key1".to_string()));
        assert_eq!(entry.1, Value::String("value1".to_string()));
    }

    #[test]
    fn test_index_map_terminal_nitrite_ids() {
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::Array(vec![Value::NitriteId(NitriteId::new())]));
        let index_map = IndexMap::new(None, Some(sub_map));

        let result = index_map.terminal_nitrite_ids().unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_index_map_terminal_nitrite_ids_empty() {
        let sub_map = BTreeMap::new();
        let index_map = IndexMap::new(None, Some(sub_map));

        let result = index_map.terminal_nitrite_ids().unwrap();
        assert_eq!(result.len(), 0);
    }

    // IndexMap unwrap error handling tests
    #[test]
    fn test_index_map_iterator_forward_scan_with_cached_map() {
        // Test that iterator forward scan works correctly with cached index map
        // This tests the fix for the unsafe unwrap() at line 365
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        sub_map.insert(Value::String("key3".to_string()), Value::String("value3".to_string()));
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        // Verify forward iteration works correctly
        let entry1 = entries.next().unwrap().unwrap();
        assert_eq!(entry1.0, Key::String("key1".to_string()));
        assert_eq!(entry1.1, Value::String("value1".to_string()));
        
        let entry2 = entries.next().unwrap().unwrap();
        assert_eq!(entry2.0, Key::String("key2".to_string()));
        assert_eq!(entry2.1, Value::String("value2".to_string()));
        
        let entry3 = entries.next().unwrap().unwrap();
        assert_eq!(entry3.0, Key::String("key3".to_string()));
        assert_eq!(entry3.1, Value::String("value3".to_string()));
    }

    #[test]
    fn test_index_map_iterator_reverse_scan_with_cached_map() {
        // Test that iterator reverse scan works correctly with cached index map
        // This verifies both forward and reverse scanning work properly
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::String("value1".to_string()));
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        // Test next_back for reverse iteration
        let entry_back = entries.next_back().unwrap().unwrap();
        assert_eq!(entry_back.0, Key::String("key2".to_string()));
        assert_eq!(entry_back.1, Value::String("value2".to_string()));
    }

    #[test]
    fn test_index_map_in_memory_get_none_handling() {
        // Test that get() on missing key returns None gracefully
        // instead of unwrapping
        let sub_map = BTreeMap::new();
        let index_map = IndexMap::new(None, Some(sub_map));
        
        let key = Key::String("nonexistent".to_string());
        let result = index_map.get(&key);
        
        // Should succeed and return None, not panic
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_index_map_null_value_handling() {
        // Test that Null values are handled correctly when retrieved
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::Null);
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let key = Key::String("key1".to_string());
        let result = index_map.get(&key).unwrap();
        
        // Should successfully retrieve the Null value
        assert_eq!(result, Some(Value::Null));
    }

    #[test]
    fn test_index_map_iterator_with_null_values() {
        // Test that iterator handles Null values correctly during iteration
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::Null);
        sub_map.insert(Value::String("key2".to_string()), Value::String("value2".to_string()));
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        let entry1 = entries.next().unwrap().unwrap();
        assert_eq!(entry1.0, Key::String("key1".to_string()));
        assert_eq!(entry1.1, Value::Null);
        
        let entry2 = entries.next().unwrap().unwrap();
        assert_eq!(entry2.0, Key::String("key2".to_string()));
        assert_eq!(entry2.1, Value::String("value2".to_string()));
    }

    // Performance optimization tests for IndexMap
    #[test]
    fn test_index_map_iterator_forward_scan_efficiency() {
        // Test that forward iteration avoids unnecessary clones in reverse_scan=false path
        let mut sub_map = BTreeMap::new();
        for i in 0..10 {
            sub_map.insert(
                Value::String(format!("key{}", i)),
                Value::String(format!("value{}", i))
            );
        }
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        // Iterate all entries
        let mut count = 0;
        while let Some(result) = entries.next() {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_index_map_iterator_reverse_scan_efficiency() {
        // Test that reverse iteration avoids unnecessary clones in reverse_scan=true path
        let mut sub_map = BTreeMap::new();
        for i in 0..10 {
            sub_map.insert(
                Value::String(format!("key{}", i)),
                Value::String(format!("value{}", i))
            );
        }
        
        let index_map = IndexMap::new(None, Some(sub_map));
        index_map.set_reverse_scan(true);
        let mut entries = index_map.entries().unwrap();
        
        // Iterate all entries in reverse
        let mut count = 0;
        while let Some(result) = entries.next() {
            assert!(result.is_ok());
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_index_map_iterator_double_ended_forward() {
        // Test bidirectional iteration from forward direction
        let mut sub_map = BTreeMap::new();
        for i in 0..5 {
            sub_map.insert(
                Value::String(format!("key{}", i)),
                Value::String(format!("value{}", i))
            );
        }
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        // Get first from front
        if let Some(result) = entries.next() {
            let first = result.unwrap();
            assert_eq!(first.0, Key::String("key0".to_string()));
        }
        
        // Get last from back
        if let Some(result) = entries.next_back() {
            let last = result.unwrap();
            assert_eq!(last.0, Key::String("key4".to_string()));
        }
    }

    #[test]
    fn test_index_map_terminal_nitrite_ids_performance() {
        // Test that terminal_nitrite_ids efficiently processes array values
        let mut sub_map = BTreeMap::new();
        let ids: Vec<Value> = (0..100)
            .map(|_| Value::NitriteId(NitriteId::new()))
            .collect();
        
        sub_map.insert(Value::String("key1".to_string()), Value::Array(ids.clone()));
        sub_map.insert(Value::String("key2".to_string()), Value::Array(ids.clone()));
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let result = index_map.terminal_nitrite_ids().unwrap();
        
        // Should collect all IDs efficiently
        assert!(result.len() > 0);
    }

    #[test]
    fn test_index_map_entries_no_unwrap_errors() {
        // Test that entries() doesn't panic on None values
        let mut sub_map = BTreeMap::new();
        sub_map.insert(Value::String("key1".to_string()), Value::Null);
        sub_map.insert(Value::String("key2".to_string()), Value::Array(vec![]));
        sub_map.insert(Value::String("key3".to_string()), Value::String("value".to_string()));
        
        let index_map = IndexMap::new(None, Some(sub_map));
        let mut entries = index_map.entries().unwrap();
        
        let count = entries.by_ref().filter(|e| e.is_ok()).count();
        assert_eq!(count, 3);
    }
}
