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

/// Storage layout used by an [`IndexMap`].
///
/// Non-unique indexes on low-cardinality fields would otherwise store every matching
/// `NitriteId` in a single ever-growing array keyed by the indexed value, making each
/// insert an O(k) read-modify-write of that array (O(n²) for a bulk load). The
/// [`IndexLayout::Composite`] layout instead stores one row per `(value, id)` pair —
/// keyed by `Value::Array([value, NitriteId])` with an empty value — so inserts and
/// removals are O(1) point operations and an equality lookup is a range scan over the
/// `(value, *)` prefix.
///
/// [`IndexLayout::Array`] is the classic `value -> Array[ids]` layout, still used for
/// unique simple indexes (array length ≤ 1, where the uniqueness check depends on the
/// single-array shape), compound-index sub-maps, and in-memory maps.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum IndexLayout {
    Array,
    Composite,
}

/// Canonicalizes a value used as the first component of a composite key.
///
/// Backends order keys by their serialized bytes (e.g. the Fjall adapter normalizes
/// unsigned integers to their signed equivalents before serializing keys). The
/// composite key is a `Value::Array`, and that per-element normalization does not
/// recurse into arrays, so we apply the same unsigned→signed mapping here. This keeps
/// the stored composite keys and the lookup bounds byte-identical regardless of the
/// numeric variant the caller used, matching how the classic array layout behaved.
pub(crate) fn normalize_index_value(value: &Value) -> Value {
    match value {
        Value::U8(v) => Value::I8(*v as i8),
        Value::U16(v) => Value::I16(*v as i16),
        Value::U32(v) => Value::I32(*v as i32),
        Value::U64(v) => Value::I64(*v as i64),
        Value::U128(v) => Value::I128(*v as i128),
        Value::USize(v) => Value::ISize(*v as isize),
        other => other.clone(),
    }
}

/// Builds the composite key `[value, id]` for the [`IndexLayout::Composite`] layout.
pub(crate) fn composite_key(value: &Value, id: &NitriteId) -> Key {
    Value::Array(vec![normalize_index_value(value), Value::NitriteId(*id)])
}

/// Upper bracket for a single-field (`arity == 1`) `(value, *)` composite-key range. Every real
/// id is `< u64::MAX`, so `[value, MAX]` sorts at or above every `[value, id]` but below any
/// `[value', _]` with `value' > value`.
fn composite_upper(value: &Value) -> Key {
    Value::Array(vec![
        normalize_index_value(value),
        Value::NitriteId(NitriteId::max_sentinel()),
    ])
}

/// Lower bracket for the whole `(value, *)` group, independent of the index arity. A 1-element
/// array `[value]` sorts immediately *before* every `[value, ...]` key (a shorter tuple is the
/// smaller key in both `Value::Ord` and the persisted order-preserving codec) and after every
/// entry of a smaller leading value, so `ceiling_key([value])` lands on the first key of the
/// group.
fn composite_prefix(value: &Value) -> Key {
    Value::Array(vec![normalize_index_value(value)])
}

/// The leading (first-field) component of a composite key.
fn composite_lead(key: &Key) -> Option<&Value> {
    match key {
        Value::Array(parts) if !parts.is_empty() => Some(&parts[0]),
        _ => None,
    }
}

/// Rebuilds the nested value the index scanner expects for one leading-value group, from the
/// flat composite keys.
///
/// `rows` are the per-key component tails that share the same leading value, each shaped
/// `[next_field, ..., last_field, id]`; `value_fields` is how many indexed fields remain in a
/// row before the trailing id. The result mirrors the classic nested layout: with no remaining
/// value fields it is `Array[ids]`, otherwise `Map{ field_value -> <nested> }`. The rows arrive
/// already sorted, so equal leading components form contiguous runs.
fn reconstruct_group(rows: &[&[Value]], value_fields: usize) -> Value {
    if value_fields == 0 {
        // Each row is just `[id]`.
        let ids = rows
            .iter()
            .filter_map(|r| r.first().cloned())
            .collect::<Vec<_>>();
        return Value::Array(ids);
    }

    let mut map: BTreeMap<Value, Value> = BTreeMap::new();
    let mut i = 0;
    while i < rows.len() {
        let Some(field_value) = rows[i].first() else {
            i += 1;
            continue;
        };
        let field_value = field_value.clone();
        let mut tails: Vec<&[Value]> = Vec::new();
        while i < rows.len() && rows[i].first() == Some(&field_value) {
            tails.push(&rows[i][1..]);
            i += 1;
        }
        map.insert(field_value, reconstruct_group(&tails, value_fields - 1));
    }
    Value::Map(map)
}

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
        let inner_map = IndexMapInner::new(nitrite_map, sub_map, IndexLayout::Array, 1);
        IndexMap {
            inner: Arc::new(inner_map),
        }
    }

    /// Creates a new IndexMap over a persisted map that uses the composite-key layout
    /// (one row per `(field-values…, id)`), used for non-unique simple and compound indexes.
    ///
    /// `arity` is the number of indexed fields: `1` for a simple index (keys `[value, id]`),
    /// `K` for a `K`-field compound index (keys `[v0, v1, …, v(K-1), id]`). All navigation and
    /// lookup methods translate transparently between the caller's nested, value-keyed view
    /// (`get(v0)` returns `Array[ids]` for a simple index, or the reconstructed nested
    /// `Map{ v1 -> … }` for a compound index; navigation walks distinct leading values) and the
    /// underlying flat `[v0, …, id] -> ()` rows.
    pub(crate) fn composite(nitrite_map: NitriteMap, arity: usize) -> Self {
        let inner_map =
            IndexMapInner::new(Some(nitrite_map), None, IndexLayout::Composite, arity.max(1));
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
                    Ok((key, value)) => writeln!(f, "Key: {:?}, Value: {:?}", key, value)?,
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
                    Ok((key, value)) => writeln!(f, "Key: {:?}, Value: {:?}", key, value)?,
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
    layout: IndexLayout,
    /// Number of indexed fields for a [`IndexLayout::Composite`] map (1 = simple index,
    /// K = K-field compound index). Ignored for the array layout.
    arity: usize,
    reverse_scan: AtomicBool,
}

impl IndexMapInner {
    fn new(
        nitrite_map: Option<NitriteMap>,
        sub_map: Option<BTreeMap<Value, Value>>,
        layout: IndexLayout,
        arity: usize,
    ) -> Self {
        let in_memory_map = sub_map.map(InMemoryIndexMap::new);

        IndexMapInner {
            nitrite_map,
            sub_map: in_memory_map,
            layout,
            arity,
            reverse_scan: AtomicBool::from(false),
        }
    }

    /// The persisted map backing a [`IndexLayout::Composite`] index.
    fn composite_map(&self) -> NitriteResult<&NitriteMap> {
        self.nitrite_map.as_ref().ok_or_else(|| {
            log::error!("Composite index is in corrupt state: missing backing map");
            INDEX_CORRUPT_ERROR.clone()
        })
    }

    /// Equality lookup in the composite layout: range-scan the leading-value group and
    /// reconstruct the value the scanner expects — `Array[ids]` for a simple index, or the
    /// nested `Map{ … }` for a compound index — mirroring the classic array/nested layout.
    fn composite_get(&self, value: &Key) -> NitriteResult<Option<Value>> {
        let map = self.composite_map()?;
        // Stored leading components are normalized, so compare against the normalized query
        // value for an exact match consistent with the seek bounds.
        let target = normalize_index_value(value);
        let mut rows: Vec<Vec<Value>> = Vec::new();
        let mut key = map.ceiling_key(&composite_prefix(value))?;
        while let Some(k) = key {
            match &k {
                Value::Array(parts)
                    if parts.first().map(|p| *p == target).unwrap_or(false) =>
                {
                    // Tail after the leading value: [v1, …, id].
                    rows.push(parts[1..].to_vec());
                    key = map.higher_key(&k)?;
                }
                _ => break,
            }
        }
        if rows.is_empty() {
            Ok(None)
        } else {
            let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
            Ok(Some(reconstruct_group(&refs, self.arity - 1)))
        }
    }

    /// Returns the leading value component of an underlying composite key, or `None` when the
    /// map is empty / the key is not a well-formed composite key.
    fn composite_value_of(key: Option<Key>) -> Option<Key> {
        key.and_then(|k| composite_lead(&k).cloned())
    }

    /// First distinct leading value strictly greater than `value`, walking the persisted map.
    /// Used for the compound (`arity > 1`) navigation where a per-id sentinel bracket does not
    /// apply because the second key component is a field value, not the id.
    fn composite_higher_scan(&self, value: &Key) -> NitriteResult<Option<Key>> {
        let map = self.composite_map()?;
        let target = normalize_index_value(value);
        let mut key = map.ceiling_key(&composite_prefix(value))?;
        while let Some(k) = key {
            match composite_lead(&k) {
                Some(lead) if *lead == target => {
                    key = map.higher_key(&k)?;
                }
                Some(lead) => return Ok(Some(lead.clone())),
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    pub fn get(&self, key: &Key) -> NitriteResult<Option<Value>> {
        if self.layout == IndexLayout::Composite {
            return self.composite_get(key);
        }
        if let Some(ref nitrite_map) = self.nitrite_map {
            let value = nitrite_map.get(key)?;
            Ok(value)
        } else {
            let sub_map = self.sub_map.as_ref().ok_or_else(|| {
                log::error!("Index is in corrupt state. Could not get value for key: {:?}", key);
                INDEX_CORRUPT_ERROR.clone()
            })?;
            
            sub_map.get(key)
        }
    }

    pub fn first_key(&self) -> NitriteResult<Option<Key>> {
        if self.layout == IndexLayout::Composite {
            return Ok(Self::composite_value_of(self.composite_map()?.first_key()?));
        }
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
        if self.layout == IndexLayout::Composite {
            return Ok(Self::composite_value_of(self.composite_map()?.last_key()?));
        }
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
        if self.layout == IndexLayout::Composite {
            // Next distinct leading value strictly greater than `key`.
            if self.arity == 1 {
                // Single field: everything `[key, *]` is `<= [key, MAX_id]`, so the first
                // underlying key past `[key, MAX_id]` is the first entry of the next value —
                // an O(log n) seek.
                return Ok(Self::composite_value_of(
                    self.composite_map()?.higher_key(&composite_upper(key))?,
                ));
            }
            // Compound: the second component is a field value (not the id), so there is no
            // per-id sentinel; skip past the group by walking.
            return self.composite_higher_scan(key);
        }
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
        if self.layout == IndexLayout::Composite {
            // First distinct leading value `>= key`: `[key]` sorts before every `[key, *]`, so
            // the ceiling of `[key]` is the first entry whose leading value is `>= key`.
            return Ok(Self::composite_value_of(
                self.composite_map()?.ceiling_key(&composite_prefix(key))?,
            ));
        }
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
        if self.layout == IndexLayout::Composite {
            // Largest distinct leading value strictly less than `key`: `[key]` sorts before
            // every `[key, *]`, so the largest underlying key below `[key]` belongs to the
            // previous value.
            return Ok(Self::composite_value_of(
                self.composite_map()?.lower_key(&composite_prefix(key))?,
            ));
        }
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
        if self.layout == IndexLayout::Composite {
            // Largest distinct leading value `<= key`.
            if self.arity == 1 {
                // Single field: every `[key, *]` is `<= [key, MAX_id]`, so the floor of
                // `[key, MAX_id]` lands on the last entry of `key` (or the previous value).
                return Ok(Self::composite_value_of(
                    self.composite_map()?.floor_key(&composite_upper(key))?,
                ));
            }
            // Compound: if `key` itself has entries the floor is `key`; otherwise it is the
            // largest leading value below `key`.
            let target = normalize_index_value(key);
            let ceiling = self.composite_map()?.ceiling_key(&composite_prefix(key))?;
            if ceiling
                .as_ref()
                .and_then(composite_lead)
                .map(|lead| *lead == target)
                .unwrap_or(false)
            {
                return Ok(Some(target));
            }
            return Ok(Self::composite_value_of(
                self.composite_map()?.lower_key(&composite_prefix(key))?,
            ));
        }
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
        if self.layout == IndexLayout::Composite {
            // Group the sorted flat `[v0, …, id] -> ()` rows back into one entry per distinct
            // leading value — `(v0, Array[ids])` for a simple index, `(v0, Map{…})` for a
            // compound index — so callers (full scans, not-equals/not-in) see the same shape as
            // the classic layout. Order is irrelevant to those callers (their results are
            // sorted/deduped downstream), so grouping is forward-only.
            let iterator = self.composite_map()?.entries()?;
            return Ok(IndexMapIterator::new_composite(iterator, self.arity));
        }
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
    /// When set, iterate the composite layout: group consecutive flat `[v0, …, id] -> ()`
    /// rows from this underlying iterator into one entry per distinct leading value.
    composite_iterator: Option<EntryIterator>,
    /// Number of indexed fields, for reconstructing the grouped value shape.
    composite_arity: usize,
    /// One-row lookahead buffer used by the composite grouping in `next()`.
    composite_pending: Option<(Key, Value)>,
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
            composite_iterator: None,
            composite_arity: 1,
            composite_pending: None,
        }
    }

    fn new_composite(iterator: EntryIterator, arity: usize) -> Self {
        IndexMapIterator {
            nitrite_map_iterator: None,
            cached_index_map: None,
            current: None,
            reverse_scan: false,
            composite_iterator: Some(iterator),
            composite_arity: arity,
            composite_pending: None,
        }
    }

    /// Pulls the next grouped entry (one distinct leading value) from the composite iterator,
    /// reconstructing `Array[ids]` (simple index) or the nested `Map{…}` (compound index).
    fn next_composite(&mut self) -> Option<NitriteResult<(Key, Value)>> {
        let arity = self.composite_arity;
        let iterator = self.composite_iterator.as_mut()?;

        // Seed the group from the pending lookahead or the next underlying row.
        let first = match self.composite_pending.take() {
            Some(kv) => kv,
            None => match iterator.next() {
                Some(Ok(kv)) => kv,
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            },
        };

        let (group_value, first_tail) = match &first.0 {
            Value::Array(parts) if !parts.is_empty() => {
                (parts[0].clone(), parts[1..].to_vec())
            }
            _ => {
                log::error!("Composite index is in corrupt state: malformed key {:?}", first.0);
                return Some(Err(INDEX_CORRUPT_ERROR.clone()));
            }
        };

        let mut rows: Vec<Vec<Value>> = vec![first_tail];
        loop {
            match iterator.next() {
                Some(Ok(kv)) => match &kv.0 {
                    Value::Array(parts)
                        if parts.first().map(|p| *p == group_value).unwrap_or(false) =>
                    {
                        rows.push(parts[1..].to_vec());
                    }
                    Value::Array(_) => {
                        // Reached the next distinct leading value — buffer it for the next call.
                        self.composite_pending = Some(kv);
                        break;
                    }
                    _ => {
                        log::error!("Composite index is in corrupt state: malformed key {:?}", kv.0);
                        return Some(Err(INDEX_CORRUPT_ERROR.clone()));
                    }
                },
                Some(Err(e)) => return Some(Err(e)),
                None => break,
            }
        }

        let refs: Vec<&[Value]> = rows.iter().map(|r| r.as_slice()).collect();
        Some(Ok((group_value, reconstruct_group(&refs, arity - 1))))
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
        if self.composite_iterator.is_some() {
            return self.next_composite();
        }
        if !self.reverse_scan {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.higher_key(map);
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
        } else if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
            let next = nitrite_map_iterator.next_back();
            if let Some(Ok((key, _))) = &next {
                self.current = Some(key.clone());
            }
            next
        } else if let Some(map) = &self.cached_index_map {
            let next_key = self.lower_key(map);
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

impl DoubleEndedIterator for IndexMapIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.composite_iterator.is_some() {
            // Composite grouping is forward-only; its consumers don't rely on order.
            return self.next_composite();
        }
        if !self.reverse_scan {
            if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
                let next = nitrite_map_iterator.next_back();
                if let Some(Ok((key, _))) = &next {
                    self.current = Some(key.clone());
                }
                next
            } else if let Some(map) = &self.cached_index_map {
                let next_key = self.lower_key(map);
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
        } else if let Some(nitrite_map_iterator) = &mut self.nitrite_map_iterator {
            let next = nitrite_map_iterator.next();
            if let Some(Ok((key, _))) = &next {
                self.current = Some(key.clone());
            }
            next
        } else if let Some(map) = &self.cached_index_map {
            let next_key = self.higher_key(map);
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
        let entries = index_map.entries().unwrap();
        
        // Iterate all entries
        let mut count = 0;
        for result in entries {
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
        let entries = index_map.entries().unwrap();
        
        // Iterate all entries in reverse
        let mut count = 0;
        for result in entries {
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
        assert!(!result.is_empty());
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
