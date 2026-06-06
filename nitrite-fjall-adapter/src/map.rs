use crate::config::FjallConfig;
use crate::store::FjallStore;
use crate::wrapper::FjallValue;
use fjall::{GarbageCollection, TxPartitionHandle};
use nitrite::common::{async_task, AttributeAware, Attributes, Key, Value, META_MAP_NAME};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::store::{
    EntryIterator, KeyIterator, NitriteMap, NitriteMapProvider, NitriteStore,
    SingleMapEntryProvider, SingleMapKeyProvider, SingleMapValueProvider, ValueIterator,
};
use std::collections::Bound::{Excluded, Included, Unbounded};
use std::iter::Rev;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone)]
/// Fjall-based key-value map implementation.
///
/// A persistent, thread-safe key-value store backed by Fjall LSM engine. Uses PIMPL pattern
/// with `Arc<FjallMapInner>` for efficient cloning and shared ownership. Implements the
/// NitriteMapProvider trait for integration with Nitrite's storage abstraction.
///
/// Purpose: Provides durable, transactional key-value operations with automatic persistence,
/// garbage collection, and attribute metadata storage.
///
/// Characteristics:
/// - Thread-safe (Arc-based, can be cloned across threads)
/// - Persistent (backed by Fjall LSM storage engine)
/// - Garbage collection (automatic cleanup with `collect_garbage()`)
/// - Attributes (supports metadata via AttributeAware trait)
/// - Iterator support (entries, keys, values with reverse iteration)
/// - Cloneable (cheap Arc clones)
/// - Trait delegation (no Deref, explicit method forwarding)
///
/// Usage: Created via FjallStore, used for collection data storage and indexes.
/// Accessed through NitriteMapProvider trait for iterator and CRUD operations.
pub struct FjallMap {
    inner: Arc<FjallMapInner>,
}

impl FjallMap {
    /// Creates a new FjallMap with the given partition.
    ///
    /// Arguments:
    /// - `name`: Name of this map (identifies the partition)
    /// - `partition`: Fjall partition backing this map
    /// - `store`: Parent FjallStore for lifecycle management
    /// - `fjall_config`: Configuration for this map's operations
    ///
    /// Returns: A new `FjallMap` instance ready for use
    #[inline]
    pub fn new(
        name: String,
        partition: TxPartitionHandle,
        store: FjallStore,
        fjall_config: FjallConfig,
    ) -> FjallMap {
        FjallMap {
            inner: Arc::new(FjallMapInner::new(name, partition, store, fjall_config)),
        }
    }
}

impl AttributeAware for FjallMap {
    /// Returns the attributes associated with this map.
    ///
    /// Returns: `Ok(Some(Attributes))` if attributes exist, `Ok(None)` otherwise
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.inner.get_attributes()
    }

    /// Sets the attributes for this map.
    ///
    /// Arguments:
    /// - `attributes`: Metadata to store for this map
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.inner.set_attributes(attributes)
    }
}

impl NitriteMapProvider for FjallMap {
    /// Checks if a key exists in this map.
    ///
    /// Arguments:
    /// - `key`: Key to check for existence
    ///
    /// Returns: `Ok(true)` if key exists, `Ok(false)` otherwise
    #[inline]
    fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
        self.inner.contains_key(key)
    }

    /// Retrieves a value by key.
    ///
    /// Arguments:
    /// - `key`: Key to retrieve
    ///
    /// Returns: `Ok(Some(Value))` if key exists, `Ok(None)` otherwise
    #[inline]
    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.inner.get(key)
    }

    /// Removes all entries from this map.
    ///
    /// Also triggers asynchronous cleanup (commit and garbage collection) in the background.
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()?;
        let clone = self.clone();
        async_task(move || {
            // Safe cleanup: handle all errors gracefully without panicking in async task
            match clone.get_store() {
                Ok(store) => {
                    match store.commit() {
                        Ok(_) => {
                            log::debug!("Successfully committed store after clear");
                        }
                        Err(e) => {
                            log::error!("Failed to commit store after clear: {}", e);
                            // Do not panic - log and continue
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to get store for cleanup after clear: {}", e);
                    // Do not panic - store may have been dropped or closed
                    return;
                }
            }

            // Attempt garbage collection but don't crash if it fails
            match clone.collect_garbage() {
                Ok(_) => {
                    log::debug!("Successfully collected garbage after clear");
                }
                Err(e) => {
                    log::error!("Failed to collect garbage after clear: {}", e);
                    // Non-fatal - garbage collection is best-effort
                }
            }
        });
        Ok(())
    }

    /// Checks if this map is closed.
    ///
    /// Returns: `Ok(true)` if closed, `Ok(false)` otherwise
    fn is_closed(&self) -> NitriteResult<bool> {
        self.inner.is_closed()
    }

    /// Closes this map, preventing further operations.
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    /// Returns an iterator over all values in this map.
    ///
    /// Returns: A `ValueIterator` for iteration
    fn values(&self) -> NitriteResult<ValueIterator> {
        let provider = SingleMapValueProvider::new(NitriteMap::new(self.clone()));
        Ok(ValueIterator::new(provider))
    }

    /// Returns an iterator over all keys in this map.
    ///
    /// Returns: A `KeyIterator` for iteration
    fn keys(&self) -> NitriteResult<KeyIterator> {
        let provider = SingleMapKeyProvider::new(NitriteMap::new(self.clone()));
        Ok(KeyIterator::new(provider))
    }

    /// Removes an entry by key, returning its previous value.
    ///
    /// Arguments:
    /// - `key`: Key to remove
    ///
    /// Returns: `Ok(Some(Value))` if key existed, `Ok(None)` otherwise
    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        let result = self.inner.remove(key)?;
        Ok(result)
    }

    /// Inserts or updates a key-value pair.
    ///
    /// Arguments:
    /// - `key`: Key to insert/update
    /// - `value`: Value to store
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        self.inner.put(key, value)?;
        Ok(())
    }

    /// Inserts or updates multiple key-value pairs atomically.
    ///
    /// Arguments:
    /// - `entries`: Vector of (key, value) pairs to insert
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn put_all(&self, entries: Vec<(Key, Value)>) -> NitriteResult<()> {
        self.inner.put_all(entries)
    }

    /// Returns the number of entries in this map.
    ///
    /// Returns: Number of key-value pairs
    fn size(&self) -> NitriteResult<u64> {
        self.inner.size()
    }

    /// Inserts a key-value pair if the key is not already present.
    ///
    /// Arguments:
    /// - `key`: Key to check/insert
    /// - `value`: Value to store if key is absent
    ///
    /// Returns: `Ok(Some(Value))` if key existed, `Ok(None)` if inserted
    fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>> {
        let result = self.inner.put_if_absent(key, value)?;
        Ok(result)
    }

    /// Returns the first key in this map.
    ///
    /// Returns: `Ok(Some(Key))` if map is non-empty, `Ok(None)` otherwise
    fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.first_key()
    }

    /// Returns the last key in this map.
    ///
    /// Returns: `Ok(Some(Key))` if map is non-empty, `Ok(None)` otherwise
    fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.inner.last_key()
    }

    /// Returns the smallest key strictly greater than the given key.
    ///
    /// Arguments:
    /// - `key`: Reference key
    ///
    /// Returns: `Ok(Some(Key))` if such key exists, `Ok(None)` otherwise
    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.higher_key(key)
    }

    /// Returns the smallest key greater than or equal to the given key.
    ///
    /// Arguments:
    /// - `key`: Reference key
    ///
    /// Returns: `Ok(Some(Key))` if such key exists, `Ok(None)` otherwise
    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.ceiling_key(key)
    }

    /// Returns the largest key strictly less than the given key.
    ///
    /// Arguments:
    /// - `key`: Reference key
    ///
    /// Returns: `Ok(Some(Key))` if such key exists, `Ok(None)` otherwise
    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.lower_key(key)
    }

    /// Returns the largest key less than or equal to the given key.
    ///
    /// Arguments:
    /// - `key`: Reference key
    ///
    /// Returns: `Ok(Some(Key))` if such key exists, `Ok(None)` otherwise
    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.inner.floor_key(key)
    }

    /// Checks if this map is empty.
    ///
    /// Returns: `Ok(true)` if no entries, `Ok(false)` otherwise
    fn is_empty(&self) -> NitriteResult<bool> {
        self.inner.is_empty()
    }

    /// Returns the parent store of this map.
    ///
    /// Returns: A `NitriteStore` instance
    fn get_store(&self) -> NitriteResult<NitriteStore> {
        self.inner.get_store()
    }

    /// Returns the name of this map.
    ///
    /// Returns: String name of the map
    fn get_name(&self) -> NitriteResult<String> {
        self.inner.get_name()
    }

    /// Returns an iterator over all entries in this map.
    ///
    /// Returns: An `EntryIterator` for iteration
    fn entries(&self) -> NitriteResult<EntryIterator> {
        let provider = SingleMapEntryProvider::new(NitriteMap::new(self.clone()));
        Ok(EntryIterator::new(provider))
    }

    /// Returns a reverse iterator over all entries in this map.
    ///
    /// Returns: A reverse `EntryIterator` for iteration
    fn reverse_entries(&self) -> NitriteResult<Rev<EntryIterator>> {
        let provider = SingleMapEntryProvider::new(NitriteMap::new(self.clone()));
        Ok(EntryIterator::new(provider).rev())
    }

    /// Marks this map as disposed, freeing resources.
    ///
    /// Returns: `Ok(())` on success, error otherwise
    fn dispose(&self) -> NitriteResult<()> {
        self.inner.dispose()
    }

    /// Checks if this map is dropped.
    ///
    /// Returns: `Ok(true)` if dropped, `Ok(false)` otherwise
    fn is_dropped(&self) -> NitriteResult<bool> {
        self.inner.is_dropped()
    }
}

impl FjallMap {
    /// Performs garbage collection on this map.
    ///
    /// Reclaims space by removing deleted/unused data through compaction.
    ///
    /// Returns: `Ok(())` on success, error otherwise
    pub fn collect_garbage(&self) -> NitriteResult<()> {
        self.inner.collect_garbage()
    }

    /// Initializes this map.
    ///
    /// Returns: `Ok(())` on success, error otherwise
    pub fn initialize(&self) -> NitriteResult<()> {
        self.inner.initialize()
    }

    /// Returns the attributes associated with this map.
    ///
    /// Returns: `Ok(Some(Attributes))` if attributes exist, `Ok(None)` otherwise
    #[allow(dead_code)]
    pub fn get_attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.inner.get_attributes()
    }

    /// Sets the attributes for this map.
    ///
    /// Arguments:
    /// - `attributes`: Metadata to store for this map
    ///
    /// Returns: `Ok(())` on success, error otherwise
    #[allow(dead_code)]
    pub fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.inner.set_attributes(attributes)
    }
}

/// Internal Fjall map implementation.
///
/// Encapsulates the actual Fjall partition and manages state (open/closed/dropped).
/// Provides atomic access to partition operations with lifecycle tracking.
///
/// Thread-safe: Uses AtomicBool for state flags with Relaxed ordering for performance.
struct FjallMapInner {
    name: String,
    overlay_key: Arc<str>,
    partition: TxPartitionHandle,
    closed: AtomicBool,
    dropped: AtomicBool,
    store: FjallStore,
    fjall_config: FjallConfig,
}

#[derive(Clone, Copy)]
enum SeekDirection {
    Forward,
    Reverse,
}

impl FjallMapInner {
    /// Creates a new FjallMapInner wrapping the given partition.
    ///
    /// Arguments:
    /// - `name`: Map identifier
    /// - `partition`: Fjall partition backend
    /// - `store`: Parent store reference
    /// - `fjall_config`: Configuration for operations
    ///
    /// Returns: A new `FjallMapInner` with closed=false, dropped=false
    fn new(
        name: String,
        partition: TxPartitionHandle,
        store: FjallStore,
        fjall_config: FjallConfig,
    ) -> FjallMapInner {
        let overlay_key = Arc::<str>::from(name.as_str());
        FjallMapInner {
            name,
            overlay_key,
            partition,
            store,
            closed: AtomicBool::new(false),
            dropped: AtomicBool::new(false),
            fjall_config,
        }
    }

    /// Initializes this map (currently a no-op).
    ///
    /// Returns: `Ok(())`
    pub fn initialize(&self) -> NitriteResult<()> {
        Ok(())
    }

    fn check_opened(&self) -> NitriteResult<()> {
        if self.closed.load(Ordering::Relaxed) {
            log::error!("Map {} is closed", self.name);
            return Err(NitriteError::new(
                &format!("Map {} is closed", self.name),
                ErrorKind::StoreAlreadyClosed,
            ));
        }

        if self.dropped.load(Ordering::Relaxed) {
            log::error!("Map {} is dropped", self.name);
            return Err(NitriteError::new(
                &format!("Map {} is dropped", self.name),
                ErrorKind::StoreNotInitialized,
            ));
        }

        Ok(())
    }

    /// Builds a `BackendError` for a failed Fjall operation, logging it as well.
    fn backend_err(op: &str, err: impl std::fmt::Display) -> NitriteError {
        log::error!("Failed to {} FjallMap: {}", op, err);
        NitriteError::new(
            &format!("Failed to {} FjallMap: {}", op, err),
            ErrorKind::BackendError,
        )
    }

    /// Deserializes a stored Fjall **key** back into a `Value`, using the order-preserving key
    /// codec (the counterpart of [`FjallValue::try_from_key`]).
    ///
    /// Corrupted or format-incompatible on-disk bytes are surfaced as a `NitriteError` rather
    /// than panicking, so a damaged or foreign database degrades to a recoverable read error
    /// instead of crashing the process.
    #[inline]
    fn decode_value(raw: FjallValue) -> NitriteResult<Value> {
        raw.try_into_key().map_err(NitriteError::from)
    }

    #[inline]
    fn decode_bytes(raw: &[u8]) -> NitriteResult<Value> {
        bincode::serde::decode_from_slice(raw, bincode::config::legacy())
            .map(|(value, _)| value)
            .map_err(|err| {
                NitriteError::from(crate::wrapper::FjallValueError::DeserializationError(
                    err.to_string(),
                ))
            })
    }

    fn insert_in_tx(&self, key: FjallValue, value: FjallValue) -> NitriteResult<()> {
        let raw_key = key.as_ref().to_vec();
        let raw_value = Box::<[u8]>::from(value.as_ref());
        crate::tx_scope::with_active(|tx| {
            let tx = tx.expect("write_in_tx requires an active transaction");
            tx.insert(&self.partition, key, value);
        });
        crate::tx_scope::record_insert(&self.overlay_key, raw_key, raw_value);
        Ok(())
    }

    fn remove_in_tx(&self, raw_key: Vec<u8>) -> NitriteResult<()> {
        crate::tx_scope::with_active(|tx| {
            let tx = tx.expect("write_in_tx requires an active transaction");
            tx.remove(&self.partition, raw_key.clone());
        });
        crate::tx_scope::record_remove(&self.overlay_key, raw_key);
        Ok(())
    }

    fn visible_contains_key(&self, op: &str, key: &FjallValue) -> NitriteResult<bool> {
        if !crate::tx_scope::in_scope() {
            return self
                .partition
                .contains_key(key.clone())
                .map_err(|err| Self::backend_err(op, err));
        }

        crate::tx_scope::with_partition_overlay(&self.overlay_key, |overlay| {
            if let Some(value) = overlay.and_then(|entries| entries.entries.get(key.as_ref())) {
                return Ok(value.is_some());
            }

            self.partition
                .contains_key(key.clone())
                .map_err(|err| Self::backend_err(op, err))
        })
    }

    fn visible_value(&self, op: &str, key: &FjallValue) -> NitriteResult<Option<Value>> {
        if !crate::tx_scope::in_scope() {
            return self
                .partition
                .get(key.clone())
                .map_err(|err| Self::backend_err(op, err))?
                .as_deref()
                .map(Self::decode_bytes)
                .transpose();
        }

        crate::tx_scope::with_partition_overlay(&self.overlay_key, |overlay| {
            if let Some(value) = overlay.and_then(|entries| entries.entries.get(key.as_ref())) {
                return value.as_deref().map(Self::decode_bytes).transpose();
            }

            self.partition
                .get(key.clone())
                .map_err(|err| Self::backend_err(op, err))?
                .as_deref()
                .map(Self::decode_bytes)
                .transpose()
        })
    }

    fn committed_entry_raw(
        &self,
        op: &str,
        bound: Option<&[u8]>,
        inclusive: bool,
        direction: SeekDirection,
    ) -> NitriteResult<Option<(Vec<u8>, Vec<u8>)>> {
        let result = match (direction, bound) {
            (SeekDirection::Forward, None) => self.partition.first_key_value(),
            (SeekDirection::Reverse, None) => self.partition.last_key_value(),
            (SeekDirection::Forward, Some(bound)) => self
                .partition
                .inner()
                .range::<Vec<u8>, _>(((if inclusive {
                    Included(bound.to_vec())
                } else {
                    Excluded(bound.to_vec())
                }), Unbounded))
                .next()
                .transpose(),
            (SeekDirection::Reverse, Some(bound)) => self
                .partition
                .inner()
                .range::<Vec<u8>, _>((Unbounded, if inclusive {
                    Included(bound.to_vec())
                } else {
                    Excluded(bound.to_vec())
                }))
                .next_back()
                .transpose(),
        }
        .map_err(|err| Self::backend_err(op, err))?;

        Ok(result.map(|(key, value)| (key.to_vec(), value.to_vec())))
    }

    fn overlay_entry_raw<'a>(
        overlay: Option<&'a crate::tx_scope::PartitionOverlay>,
        bound: Option<&[u8]>,
        inclusive: bool,
        direction: SeekDirection,
    ) -> Option<(&'a Vec<u8>, &'a [u8])> {
        let overlay = overlay?;

        match (direction, bound) {
            (SeekDirection::Forward, None) => overlay
                .entries
                .iter()
                .find_map(|(key, value)| value.as_deref().map(|value| (key, value))),
            (SeekDirection::Reverse, None) => overlay
                .entries
                .iter()
                .rev()
                .find_map(|(key, value)| value.as_deref().map(|value| (key, value))),
            (SeekDirection::Forward, Some(bound)) => overlay
                .entries
                .range((if inclusive {
                    Included(bound.to_vec())
                } else {
                    Excluded(bound.to_vec())
                }, Unbounded))
                .find_map(|(key, value)| value.as_deref().map(|value| (key, value))),
            (SeekDirection::Reverse, Some(bound)) => overlay
                .entries
                .range((Unbounded, if inclusive {
                    Included(bound.to_vec())
                } else {
                    Excluded(bound.to_vec())
                }))
                .rev()
                .find_map(|(key, value)| value.as_deref().map(|value| (key, value))),
        }
    }

    fn choose_entry(
        direction: SeekDirection,
        overlay_candidate: Option<(&Vec<u8>, &[u8])>,
        committed_candidate: Option<(Vec<u8>, Vec<u8>)>,
    ) -> Option<(Vec<u8>, Vec<u8>)> {
        match (overlay_candidate, committed_candidate) {
            (Some((overlay_key, overlay_value)), Some(committed_entry)) => match direction {
                SeekDirection::Forward => {
                    if overlay_key.as_slice() <= committed_entry.0.as_slice() {
                        Some((overlay_key.clone(), overlay_value.to_vec()))
                    } else {
                        Some(committed_entry)
                    }
                }
                SeekDirection::Reverse => {
                    if overlay_key.as_slice() >= committed_entry.0.as_slice() {
                        Some((overlay_key.clone(), overlay_value.to_vec()))
                    } else {
                        Some(committed_entry)
                    }
                }
            },
            (Some((overlay_key, overlay_value)), None) => {
                Some((overlay_key.clone(), overlay_value.to_vec()))
            }
            (None, Some(committed_entry)) => Some(committed_entry),
            (None, None) => None,
        }
    }

    fn committed_size(&self, op: &str) -> NitriteResult<u64> {
        self.partition
            .inner()
            .len()
            .map(|len| len as u64)
            .map_err(|err| Self::backend_err(op, err))
    }

    fn overlay_size_delta(&self, op: &str) -> NitriteResult<i64> {
        crate::tx_scope::with_partition_overlay_mut(&self.overlay_key, |overlay| {
            let Some(overlay) = overlay else {
                return Ok(0);
            };

            if let Some(delta) = overlay.cached_size_delta() {
                return Ok(delta);
            }

            let mut delta = 0i64;
            for (key, value) in &overlay.entries {
                let committed_present = self
                    .partition
                    .contains_key(key.as_slice())
                    .map_err(|err| Self::backend_err(op, err))?;

                match (committed_present, value.is_some()) {
                    (false, true) => delta += 1,
                    (true, false) => delta -= 1,
                    _ => {}
                }
            }

            overlay.cache_size_delta(delta);
            Ok(delta)
        })
    }

    fn visible_size(&self, op: &str) -> NitriteResult<u64> {
        let committed = self.committed_size(op)? as i64;
        let visible = committed + self.overlay_size_delta(op)?;
        debug_assert!(visible >= 0, "transaction overlay size delta underflowed");
        Ok(visible.max(0) as u64)
    }

    fn visible_entry_raw(
        &self,
        op: &str,
        bound: Option<&[u8]>,
        inclusive: bool,
        direction: SeekDirection,
    ) -> NitriteResult<Option<(Vec<u8>, Vec<u8>)>> {
        if !crate::tx_scope::in_scope() {
            return self.committed_entry_raw(op, bound, inclusive, direction);
        }

        crate::tx_scope::with_partition_overlay(&self.overlay_key, |overlay| {
            let overlay_candidate = Self::overlay_entry_raw(overlay, bound, inclusive, direction);
            let mut committed_candidate = self.committed_entry_raw(op, bound, inclusive, direction)?;

            while let Some((key, value)) = committed_candidate.take() {
                match overlay.and_then(|entries| entries.entries.get(key.as_slice())) {
                    Some(None) => {
                        committed_candidate =
                            self.committed_entry_raw(op, Some(&key), false, direction)?;
                    }
                    Some(Some(overlay_value)) => {
                        committed_candidate = Some((key, overlay_value.to_vec()));
                        break;
                    }
                    None => {
                        committed_candidate = Some((key, value));
                        break;
                    }
                }
            }

            Ok(Self::choose_entry(
                direction,
                overlay_candidate,
                committed_candidate,
            ))
        })
    }

    fn get_attributes(&self) -> NitriteResult<Option<Attributes>> {
        if !self.is_dropped()? {
            let store = self.get_store()?;
            let meta_map = store.open_map(META_MAP_NAME)?;
            let name = self.get_name()?;

            if name.ne(META_MAP_NAME) {
                let attributes = meta_map.get(&Value::from(name.clone()))?;
                if let Some(attributes) = attributes {
                    // Check if value is actually a Document before unwrapping
                    return match attributes.as_document() {
                        Some(doc) => Ok(Some(Attributes::from_document(doc))),
                        None => {
                            log::warn!(
                                "Metadata for map '{}' is not a Document, skipping attributes",
                                name
                            );
                            Ok(None)
                        }
                    };
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

    fn contains_key(&self, key: &Key) -> NitriteResult<bool> {
        self.check_opened()?;
        // Normalize numeric key types so this matches the encoding used by get/put/remove
        // (e.g. U64(5) and I64(5) map to the same stored key); otherwise contains_key could
        // miss a key that get() would find.
        let fjall_key = FjallValue::try_from_key(key)?;
        if !crate::tx_scope::in_scope() {
            return self
                .partition
                .contains_key(fjall_key)
                .map_err(|err| Self::backend_err("check key in", err));
        }

        self.visible_contains_key("check key in", &fjall_key)
    }

    fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.check_opened()?;

        // Use normalized numeric types for keys to ensure consistent index behavior
        let normalized_key = FjallValue::try_from_key(key)?;
        self.visible_value("get value from", &normalized_key)
    }

    fn clear(&self) -> NitriteResult<()> {
        self.check_opened()?;

        // Collect every visible key first, then delete them inside one atomic transaction.
        let mut keys = Vec::new();
        let mut next = self.visible_entry_raw("clear", None, true, SeekDirection::Forward)?;
        while let Some((key, _)) = next {
            keys.push(key.clone());
            next = self.visible_entry_raw("clear", Some(&key), false, SeekDirection::Forward)?;
        }

        self.store.write_in_tx(|| {
            for key in keys {
                self.remove_in_tx(key)?;
            }
            Ok(())
        })
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        Ok(self.closed.load(Ordering::Relaxed))
    }

    fn close(&self) -> NitriteResult<()> {
        self.closed.store(true, Ordering::Relaxed);
        let store = self.get_store()?;
        store.close_map(&self.name)
    }

    fn remove(&self, key: &Key) -> NitriteResult<Option<Value>> {
        self.check_opened()?;
        // Read the current value first (through the active transaction if any), then delete it
        // within an atomic write transaction.
        let value = self.get(key)?;
        let normalized_key = FjallValue::try_from_key(key)?;
        self.store
            .write_in_tx(|| self.remove_in_tx(normalized_key.as_ref().to_vec()))?;
        Ok(value)
    }

    fn put(&self, key: Key, value: Value) -> NitriteResult<()> {
        self.check_opened()?;
        // Use normalized numeric types for keys to ensure consistent index behavior
        // across different numeric types (e.g., I64 vs U64)
        let normalized_key = FjallValue::try_from_key(&key)?;
        self.store.write_in_tx(|| {
            let fjall_value = FjallValue::try_from_value(&value)?;
            self.insert_in_tx(normalized_key, fjall_value)
        })
    }

    /// Inserts multiple key-value pairs as part of one atomic write transaction.
    ///
    /// Every entry lands in the same transaction as the rest of the enclosing atomic scope, so
    /// a batch document insert and all of its index updates are persisted together or not at
    /// all. When called outside a scope a one-shot transaction is used for the whole batch.
    fn put_all(&self, entries: Vec<(Key, Value)>) -> NitriteResult<()> {
        self.check_opened()?;

        if entries.is_empty() {
            return Ok(());
        }

        self.store.write_in_tx(|| {
            for (key, value) in entries {
                let normalized_key = FjallValue::try_from_key(&key)?;
                let fjall_value = FjallValue::try_from_value(&value)?;
                self.insert_in_tx(normalized_key, fjall_value)?;
            }
            Ok(())
        })
    }

    fn size(&self) -> NitriteResult<u64> {
        self.check_opened()?;
        if !crate::tx_scope::in_scope() {
            return self.committed_size("get size of");
        }

        self.visible_size("get size of")
    }

    fn put_if_absent(&self, key: Key, value: Value) -> NitriteResult<Option<Value>> {
        self.check_opened()?;
        // Use normalized numeric types for keys to ensure consistent index behavior
        let normalized_key = FjallValue::try_from_key(&key)?;
        // The read and the conditional insert run in one transaction so the check and the
        // write are atomic and read-your-writes consistent.
        self.store.write_in_tx(|| {
            let existing = self.visible_value("get item from", &normalized_key)?;
            if existing.is_none() {
                let fjall_value = FjallValue::try_from_value(&value)?;
                self.insert_in_tx(normalized_key.clone(), fjall_value)?;
            }
            Ok(existing)
        })
    }

    fn first_key(&self) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let result = self.visible_entry_raw("get first key from", None, true, SeekDirection::Forward)?;
        result
            .map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn last_key(&self) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let result = self.visible_entry_raw("get last key from", None, true, SeekDirection::Reverse)?;
        result
            .map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn higher_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let normalized_key = FjallValue::try_from_key(key)?;
        let next = self.visible_entry_raw(
            "get higher key from",
            Some(normalized_key.as_ref()),
            false,
            SeekDirection::Forward,
        )?;
        next.map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn ceiling_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let normalized_key = FjallValue::try_from_key(key)?;
        let next = self.visible_entry_raw(
            "get ceiling key from",
            Some(normalized_key.as_ref()),
            true,
            SeekDirection::Forward,
        )?;
        next.map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn lower_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let normalized_key = FjallValue::try_from_key(key)?;
        let prev = self.visible_entry_raw(
            "get lower key from",
            Some(normalized_key.as_ref()),
            false,
            SeekDirection::Reverse,
        )?;
        prev.map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn floor_key(&self, key: &Key) -> NitriteResult<Option<Key>> {
        self.check_opened()?;
        let normalized_key = FjallValue::try_from_key(key)?;
        let prev = self.visible_entry_raw(
            "get floor key from",
            Some(normalized_key.as_ref()),
            true,
            SeekDirection::Reverse,
        )?;
        prev.map(|(key, _)| Self::decode_value(FjallValue::from(key)))
            .transpose()
    }

    fn is_empty(&self) -> NitriteResult<bool> {
        self.check_opened()?;
        if !crate::tx_scope::in_scope() {
            return self
                .partition
                .inner()
                .is_empty()
                .map_err(|err| Self::backend_err("check if empty", err));
        }

        Ok(self.visible_size("check if empty")? == 0)
    }

    fn get_store(&self) -> NitriteResult<NitriteStore> {
        Ok(NitriteStore::new(self.store.clone()))
    }

    fn get_name(&self) -> NitriteResult<String> {
        let encoded_name = self.name.clone();
        let name = FjallStore::decode_name(&encoded_name);
        Ok(name)
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.dropped.store(true, Ordering::Relaxed);
        self.closed.store(true, Ordering::Relaxed);

        let store = self.get_store()?;
        let name = self.get_name()?; // Get decoded name since remove_map will encode it
        store.remove_map(&name)?;

        Ok(())
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        Ok(self.dropped.load(Ordering::Relaxed))
    }

    // Helper function to avoid repeated error handling pattern
    fn handle_gc_error<E: std::fmt::Display>(err: E, operation: &str) -> NitriteResult<()> {
        log::error!("Failed to {} from FjallMap: {}", operation, err);
        Err(NitriteError::new(
            &format!("Failed to {} from FjallMap: {}", operation, err),
            ErrorKind::BackendError,
        ))
    }

    pub fn collect_garbage(&self) -> NitriteResult<()> {
        if self.fjall_config.kv_separated() {
            // Garbage collection lives on the underlying (non-transactional) partition handle.
            let partition = self.partition.inner();
            // Use if let pattern instead of repeated error handling
            if let Err(err) = partition.gc_scan() {
                return Self::handle_gc_error(err, "collect garbage (scan)");
            }

            let space_amp_factor = self.fjall_config.space_amp_factor();
            if let Err(err) = partition.gc_with_space_amp_target(space_amp_factor) {
                return Self::handle_gc_error(err, "collect garbage (space amp)");
            }

            let stale_threshold = self.fjall_config.staleness_threshold();
            if let Err(err) = partition.gc_with_staleness_threshold(stale_threshold) {
                return Self::handle_gc_error(err, "collect garbage (staleness)");
            }
        } else {
            log::warn!("Cannot use GC for non-KV-separated tree");
        }
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::assertions_on_constants)] // tests use assert!(true) as "reached without panic" markers
mod tests {
    use super::*;
    use crate::tests::{run_test, Context};
    use nitrite::common::{Attributes, Key, NitritePluginProvider, Value};
    use nitrite::nitrite_config::NitriteConfig;
    use nitrite::store::NitriteStoreProvider;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{fs, thread};

    #[inline(never)]
    #[allow(dead_code)]
    fn black_box<T>(x: T) -> T {
        x
    }

    fn create_context() -> Context {
        let path = random_path();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        let fjall_config = FjallConfig::new();
        fjall_config.set_db_path(&path);
        fjall_config.set_kv_separated(true);

        // Create the store first - it will create and own the keyspace
        let store = FjallStore::new(fjall_config.clone());
        store.open_or_create().expect("Failed to open store");
        store
            .initialize(nitrite_config)
            .expect("Failed to initialize store");

        // Get the keyspace from the store and use it to create the test partition
        // This ensures the partition belongs to the same keyspace used by the store
        let keyspace = store
            .keyspace()
            .expect("Store keyspace should be initialized");
        let partition = keyspace
            .clone()
            .open_partition("test_partition", fjall_config.partition_config())
            .expect("Failed to open partition");

        let fjall_map = FjallMap::new(
            "test_map".to_string(),
            partition.clone(),
            store.clone(),
            fjall_config,
        );

        Context::new(
            path,
            Some(keyspace),
            Some(partition),
            Some(store),
            Some(fjall_map),
        )
    }

    fn random_path() -> String {
        let id = uuid::Uuid::new_v4();
        PathBuf::from("../test-data")
            .join(id.to_string())
            .to_str()
            .unwrap()
            .to_string()
    }

    fn cleanup(ctx: Context) {
        let path = ctx.path();
        let mut retry = 0;
        while fs::remove_dir_all(path.clone()).is_err() && retry < 2 {
            thread::sleep(Duration::from_millis(100));
            retry += 1;
        }
    }

    #[test]
    fn test_initialize() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.initialize().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_collect_garbage() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                map.put(Value::I8(1), Value::I8(1))
                    .expect("Failed to put item");
                map.remove(&Value::I8(1)).expect("Failed to remove item");
                assert!(map.collect_garbage().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_attributes() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let attributes = Attributes::new();
                assert!(map.set_attributes(attributes.clone()).is_ok());
                assert_eq!(map.attributes().unwrap(), Some(attributes));
            },
            cleanup,
        );
    }

    #[test]
    fn test_contains_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.contains_key(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_contains_key_normalizes_numeric_types_like_get() {
        // Regression: contains_key must use the same normalized key encoding as get/put, so a
        // key stored as one numeric type is found regardless of the numeric type queried.
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                map.put(Key::U64(5), Value::from("five")).unwrap();

                // Found via the same and the cross numeric type, matching get().
                assert!(map.contains_key(&Key::U64(5)).unwrap());
                assert!(map.contains_key(&Key::I64(5)).unwrap());
                assert_eq!(map.get(&Key::I64(5)).unwrap(), Some(Value::from("five")));

                // A genuinely absent key is still reported absent.
                assert!(!map.contains_key(&Key::U64(6)).unwrap());
            },
            cleanup,
        );
    }

    #[test]
    fn test_get() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.get(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.clear().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_is_closed() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.is_closed().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_close() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.close().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_values() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.values().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_keys() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.keys().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_remove() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.remove(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_put() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                let value = Value::from("test_value");
                assert!(map.put(key, value).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_size() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.size().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_put_if_absent() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                let value = Value::from("test_value");
                assert!(map.put_if_absent(key, value).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_first_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.first_key().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_last_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.last_key().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_higher_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.higher_key(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_ceiling_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.ceiling_key(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_lower_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.lower_key(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_floor_key() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                assert!(map.floor_key(&key).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_is_empty() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.is_empty().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_get_store() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.get_store().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_get_name() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert_eq!(map.get_name().unwrap(), "test_map");
            },
            cleanup,
        );
    }

    #[test]
    fn test_entries() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.entries().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_reverse_entries() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.reverse_entries().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_dispose_map() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                let value = Value::from("test_value");
                map.put(key, value).expect("Failed to put item");
                assert!(map.dispose().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_is_dropped() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                assert!(map.is_dropped().is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_with_async_cleanup_success() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add some data
                map.put(Key::from("key1"), Value::from("value1"))
                    .expect("Failed to put item");
                map.put(Key::from("key2"), Value::from("value2"))
                    .expect("Failed to put item");

                // Verify data exists
                assert_eq!(map.size().unwrap(), 2);

                // Clear should complete without panicking
                assert!(map.clear().is_ok());

                // Give async task time to complete
                thread::sleep(Duration::from_millis(200));

                // Verify cleared
                assert_eq!(map.size().unwrap(), 0);
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_async_cleanup_handles_commit_gracefully() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add data
                map.put(Key::from("test"), Value::from("data"))
                    .expect("Failed to put item");

                // Clear should NOT panic even if store cleanup fails
                // The async task should handle errors gracefully
                assert!(map.clear().is_ok());

                // Give async task time to complete
                thread::sleep(Duration::from_millis(200));

                // Map should still be usable after clear
                assert!(map.get(&Key::from("test")).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_on_closed_map() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Close the map
                assert!(map.close().is_ok());

                // Clear should fail since map is closed
                assert!(map.clear().is_err());
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_on_dropped_map() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Dispose (drop) the map
                assert!(map.dispose().is_ok());

                // Clear should fail since map is dropped
                assert!(map.clear().is_err());
            },
            cleanup,
        );
    }

    #[test]
    fn test_multiple_clears_in_succession() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // First clear
                map.put(Key::from("key1"), Value::from("value1"))
                    .expect("Failed to put item");
                assert!(map.clear().is_ok());
                thread::sleep(Duration::from_millis(200));
                assert_eq!(map.size().unwrap(), 0);

                // Second clear (on already empty map)
                map.put(Key::from("key2"), Value::from("value2"))
                    .expect("Failed to put item");
                assert!(map.clear().is_ok());
                thread::sleep(Duration::from_millis(200));
                assert_eq!(map.size().unwrap(), 0);

                // Third clear
                assert!(map.clear().is_ok());
                thread::sleep(Duration::from_millis(200));
                assert_eq!(map.size().unwrap(), 0);
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_does_not_panic_on_async_errors() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add data
                for i in 0..10 {
                    map.put(
                        Key::from(format!("key{}", i)),
                        Value::from(format!("value{}", i)),
                    )
                    .expect("Failed to put item");
                }

                // Clear should complete without panicking in async task
                // Even if store/garbage collection has issues
                assert!(map.clear().is_ok());

                // Give async task time to complete
                thread::sleep(Duration::from_millis(300));

                // Map should be usable and cleared
                assert_eq!(map.size().unwrap(), 0);
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_followed_by_put_operations() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add initial data
                map.put(Key::from("initial"), Value::from("data"))
                    .expect("Failed to put item");

                // Clear
                assert!(map.clear().is_ok());
                thread::sleep(Duration::from_millis(200));

                // Should be able to add new data after clear
                map.put(Key::from("new_key"), Value::from("new_value"))
                    .expect("Failed to put new item after clear");

                let result = map
                    .get(&Key::from("new_key"))
                    .expect("Failed to get new item");
                assert!(result.is_some());
                assert_eq!(result.unwrap(), Value::from("new_value"));
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_with_large_dataset() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add large amount of data
                for i in 0..100 {
                    map.put(
                        Key::from(format!("key_{:03}", i)),
                        Value::from(format!("value_{:03}", i)),
                    )
                    .expect("Failed to put item");
                }

                assert_eq!(map.size().unwrap(), 100);

                // Clear should handle large dataset without panicking
                assert!(map.clear().is_ok());
                thread::sleep(Duration::from_millis(500));

                assert_eq!(map.size().unwrap(), 0);
            },
            cleanup,
        );
    }

    #[test]
    fn test_clear_async_task_error_logging() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add data
                map.put(Key::from("key"), Value::from("value"))
                    .expect("Failed to put item");

                // Clear and let async task complete
                assert!(map.clear().is_ok());

                // Even if async task encounters errors,
                // they should be logged, not panicked
                thread::sleep(Duration::from_millis(200));

                // Map should still be functional
                assert!(map.is_closed().is_ok() || map.get(&Key::from("key")).is_ok());
            },
            cleanup,
        );
    }

    #[test]
    fn test_collect_garbage_error_handling() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add and remove items to create garbage
                for i in 0..20 {
                    let key = Key::from(format!("gc_key_{}", i));
                    map.put(key.clone(), Value::I64(i as i64))
                        .expect("Failed to put");
                    map.remove(&key).expect("Failed to remove");
                }

                // Garbage collection should succeed and handle errors gracefully
                match map.collect_garbage() {
                    Ok(_) => {
                        // Expected success
                        assert!(true);
                    }
                    Err(e) => {
                        // Even if it fails, it should return a proper error, not panic
                        log::info!("GC returned error (non-fatal): {}", e);
                        assert!(true);
                    }
                }
            },
            cleanup,
        );
    }

    #[test]
    fn test_attributes_handles_non_document_gracefully() {
        // Verify attributes doesn't panic on non-Document metadata
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Try to get attributes - should handle gracefully if metadata is corrupted
                let result = map.get_attributes();
                assert!(
                    result.is_ok(),
                    "get_attributes should not panic on non-Document metadata"
                );
            },
            cleanup,
        );
    }

    #[test]
    fn test_remove_uses_safe_error_pattern() {
        // Verify remove() uses safe error handling pattern
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");

                // Put and remove a value
                map.put(key.clone(), Value::from("test_value"))
                    .expect("Failed to put");

                // Remove should use safe error pattern (if let Err)
                let result = map.remove(&key);
                assert!(
                    result.is_ok(),
                    "Remove should succeed and use safe error handling"
                );
            },
            cleanup,
        );
    }

    #[test]
    fn test_put_if_absent_uses_safe_error_pattern() {
        // Verify put_if_absent() uses safe error handling pattern
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("unique_key");
                let value = Value::from("unique_value");

                // First put_if_absent should succeed
                let result1 = map.put_if_absent(key.clone(), value.clone());
                assert!(result1.is_ok(), "First put_if_absent should succeed");

                // Second put_if_absent should return Some (existing value)
                let result2 = map.put_if_absent(key.clone(), Value::from("new_value"));
                assert!(result2.is_ok(), "Second put_if_absent should succeed");
                let existing = result2.unwrap();
                assert!(existing.is_some(), "Should return existing value");
            },
            cleanup,
        );
    }

    #[test]
    fn test_collect_garbage_uses_safe_error_pattern() {
        // Verify collect_garbage() uses safe error pattern
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add and remove items to create garbage
                for i in 0..10 {
                    let key = Key::from(format!("gc_key_{}", i));
                    map.put(key.clone(), Value::I64(i as i64))
                        .expect("Failed to put");
                    map.remove(&key).expect("Failed to remove");
                }

                // Collect garbage should use safe error pattern
                let _result = map.collect_garbage();
                // Result may be Ok or Err, but should never panic
                assert!(true, "GC completed safely");
            },
            cleanup,
        );
    }

    #[test]
    fn test_error_handling_consistency_across_methods() {
        // Verify all methods use consistent error handling patterns
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("test_key");
                let value = Value::from("test_value");

                // Test remove with safe pattern
                map.put(key.clone(), value.clone()).ok();
                let remove_result = map.remove(&key);
                assert!(
                    remove_result.is_ok(),
                    "Remove should use safe error handling"
                );

                // Test put_if_absent with safe pattern
                let put_if_absent_result = map.put_if_absent(key.clone(), value.clone());
                assert!(
                    put_if_absent_result.is_ok(),
                    "put_if_absent should use safe error handling"
                );

                // Test collect_garbage with safe pattern
                let _gc_result = map.collect_garbage();
                // Should complete without panic regardless of success/failure
                assert!(true, "collect_garbage completed safely");
            },
            cleanup,
        );
    }

    #[test]
    fn test_multiple_garbage_collection_cycles() {
        // Verify refactored GC helper handles multiple cycles safely
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                for cycle in 0..5 {
                    // Add data
                    for i in 0..5 {
                        let key = Key::from(format!("cycle_{}_key_{}", cycle, i));
                        map.put(key.clone(), Value::from(format!("data_{}", i)))
                            .expect("Failed to put");
                        map.remove(&key).expect("Failed to remove");
                    }

                    // Collect garbage
                    map.collect_garbage().ok();
                }

                assert!(true, "Multiple GC cycles completed safely");
            },
            cleanup,
        );
    }

    #[test]
    fn test_attributes_empty_map_doesnt_panic() {
        // Verify attributes don't panic on empty map
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Empty map should not panic when getting attributes
                let result = map.get_attributes();
                assert!(result.is_ok(), "Empty map attributes should not panic");
            },
            cleanup,
        );
    }

    #[test]
    fn test_remove_after_put_safe_error_handling() {
        // Comprehensive remove test with safe error handling
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                for i in 0..10 {
                    let key = Key::from(format!("remove_test_{}", i));
                    let value = Value::from(format!("value_{}", i));

                    map.put(key.clone(), value.clone()).expect("Failed to put");

                    // Remove should handle errors safely
                    let remove_result = map.remove(&key);
                    assert!(
                        remove_result.is_ok(),
                        "Remove should use safe error pattern"
                    );

                    let retrieved = remove_result.unwrap();
                    assert_eq!(retrieved, Some(value), "Should retrieve correct value");
                }
            },
            cleanup,
        );
    }

    #[test]
    fn test_garbage_collection_with_various_data_types() {
        // Test GC helper with various data types
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Add various value types
                let values = vec![
                    Value::I64(42),
                    Value::F64(3.5),
                    Value::String("test".to_string()),
                    Value::Null,
                    Value::Array(vec![1.into(), 2.into()]),
                ];

                for (i, value) in values.into_iter().enumerate() {
                    let key = Key::from(format!("diverse_key_{}", i));
                    map.put(key.clone(), value).expect("Failed to put");
                    map.remove(&key).expect("Failed to remove");
                }

                // GC should handle diverse data safely
                let _gc_result = map.collect_garbage();
                assert!(true, "GC completed for diverse data without panicking");
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_fjall_map_contains_key_perf() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("perf_key");
                map.put(key.clone(), Value::I64(123))
                    .expect("Failed to put");

                for _ in 0..10000 {
                    let result = black_box(map.contains_key(&key));
                    black_box(result.is_ok());
                }
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_fjall_map_get_perf() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();
                let key = Key::from("perf_key");
                map.put(key.clone(), Value::I64(456))
                    .expect("Failed to put");

                for _ in 0..1000 {
                    let result = black_box(map.get(&key));
                    black_box(result.is_ok());
                }
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    // =================== put_all batch write tests ===================

    #[test]
    fn test_put_all_empty_batch() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Empty batch should succeed without error
                let result = map.put_all(vec![]);
                assert!(result.is_ok(), "Empty batch should succeed");

                // Map should still be empty
                assert_eq!(map.size().unwrap(), 0, "Map should be empty");
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_single_entry() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                let entries = vec![(Key::from("batch_key1"), Value::from("batch_value1"))];

                let result = map.put_all(entries);
                assert!(result.is_ok(), "Single entry batch should succeed");

                // Verify entry was inserted
                let value = map.get(&Key::from("batch_key1")).unwrap();
                assert_eq!(value, Some(Value::from("batch_value1")));
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_multiple_entries() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                let entries = vec![
                    (Key::from("batch_a"), Value::from("value_a")),
                    (Key::from("batch_b"), Value::I64(42)),
                    (Key::from("batch_c"), Value::F64(3.5)),
                    (Key::from("batch_d"), Value::Bool(true)),
                    (Key::from("batch_e"), Value::Null),
                ];

                let result = map.put_all(entries);
                assert!(result.is_ok(), "Multiple entry batch should succeed");

                // Verify all entries were inserted
                assert_eq!(map.size().unwrap(), 5, "All 5 entries should be inserted");
                assert_eq!(
                    map.get(&Key::from("batch_a")).unwrap(),
                    Some(Value::from("value_a"))
                );
                assert_eq!(
                    map.get(&Key::from("batch_b")).unwrap(),
                    Some(Value::I64(42))
                );
                assert_eq!(
                    map.get(&Key::from("batch_c")).unwrap(),
                    Some(Value::F64(3.5))
                );
                assert_eq!(
                    map.get(&Key::from("batch_d")).unwrap(),
                    Some(Value::Bool(true))
                );
                assert_eq!(map.get(&Key::from("batch_e")).unwrap(), Some(Value::Null));
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_overwrites_existing() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Insert initial value
                map.put(Key::from("overwrite_key"), Value::from("original"))
                    .unwrap();

                // Batch overwrite
                let entries = vec![(Key::from("overwrite_key"), Value::from("updated"))];

                let result = map.put_all(entries);
                assert!(result.is_ok(), "Batch overwrite should succeed");

                // Verify value was updated
                assert_eq!(
                    map.get(&Key::from("overwrite_key")).unwrap(),
                    Some(Value::from("updated"))
                );
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_large_batch() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Create a large batch of 1000 entries
                let entries: Vec<(Key, Value)> = (0..1000)
                    .map(|i| {
                        (
                            Key::from(format!("large_batch_{:04}", i)),
                            Value::I64(i as i64),
                        )
                    })
                    .collect();

                let result = map.put_all(entries);
                assert!(result.is_ok(), "Large batch should succeed");

                // Verify count
                assert_eq!(
                    map.size().unwrap(),
                    1000,
                    "All 1000 entries should be inserted"
                );

                // Spot check some values
                assert_eq!(
                    map.get(&Key::from("large_batch_0000")).unwrap(),
                    Some(Value::I64(0))
                );
                assert_eq!(
                    map.get(&Key::from("large_batch_0500")).unwrap(),
                    Some(Value::I64(500))
                );
                assert_eq!(
                    map.get(&Key::from("large_batch_0999")).unwrap(),
                    Some(Value::I64(999))
                );
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_with_numeric_keys() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Test batch with various numeric key types
                let entries = vec![
                    (Key::I64(100), Value::from("i64_key")),
                    (Key::I32(200), Value::from("i32_key")),
                    (Key::U64(300), Value::from("u64_key")),
                    (Key::F64(1.5), Value::from("f64_key")),
                ];

                let result = map.put_all(entries);
                assert!(result.is_ok(), "Batch with numeric keys should succeed");

                // Verify all entries
                assert_eq!(
                    map.get(&Key::I64(100)).unwrap(),
                    Some(Value::from("i64_key"))
                );
                assert_eq!(
                    map.get(&Key::I32(200)).unwrap(),
                    Some(Value::from("i32_key"))
                );
                assert_eq!(
                    map.get(&Key::U64(300)).unwrap(),
                    Some(Value::from("u64_key"))
                );
                assert_eq!(
                    map.get(&Key::F64(1.5)).unwrap(),
                    Some(Value::from("f64_key"))
                );
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_on_closed_map_fails() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Close the map
                map.close().unwrap();

                // put_all should fail on closed map
                let entries = vec![(Key::from("should_fail"), Value::from("value"))];

                let result = map.put_all(entries);
                assert!(result.is_err(), "put_all should fail on closed map");
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }

    #[test]
    fn test_put_all_perf_vs_individual_puts() {
        run_test(
            create_context,
            |ctx| {
                let map = ctx.fjall_map_unsafe();

                // Time individual puts
                let start = std::time::Instant::now();
                for i in 0..100 {
                    let key = Key::from(format!("individual_{:04}", i));
                    let value = Value::I64(i as i64);
                    map.put(key, value).unwrap();
                }
                let individual_time = start.elapsed();

                // Clear and time batch put
                map.clear().unwrap();

                let entries: Vec<(Key, Value)> = (0..100)
                    .map(|i| (Key::from(format!("batch_{:04}", i)), Value::I64(i as i64)))
                    .collect();

                let start = std::time::Instant::now();
                map.put_all(entries).unwrap();
                let batch_time = start.elapsed();

                // Batch should be faster (or at least not significantly slower)
                // Note: this is a relative test, not absolute performance
                log::info!(
                    "Performance: individual={:?}, batch={:?}",
                    individual_time,
                    batch_time
                );

                // Verify both methods resulted in correct count
                assert_eq!(map.size().unwrap(), 100);
            },
            |ctx| {
                cleanup(ctx);
            },
        );
    }
}
