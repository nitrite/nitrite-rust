use crate::errors::NitriteResult;
use crossbeam_skiplist::SkipMap;
use std::collections::BTreeMap;
use std::collections::Bound::Included;
use std::ops::Bound::{Excluded, Unbounded};

/// Provides ordered key navigation operations for sorted map implementations.
///
/// This trait abstracts navigable map operations (finding keys by position and range),
/// enabling efficient range queries and key iteration in index structures. It is
/// implemented by both in-memory BTreeMap and concurrent SkipMap.
///
/// # Purpose
/// NavigableMap enables index scanning operations that require finding specific keys
/// by position or order. These operations are essential for implementing database
/// queries with range predicates (e.g., "find all keys between X and Y").
///
/// # Characteristics
/// - **Ordered**: Maps are assumed to maintain keys in sorted order
/// - **Cloneable keys**: Keys must be Clone to return owned values
/// - **Fallible operations**: All methods return NitriteResult for error handling
/// - **Type generic**: Works with any key/value types where K: Ord + Clone
///
/// # Implementations
/// - `BTreeMap<K, V>` - In-memory tree-based sorted map
/// - `SkipMap<K, V>` - Concurrent skip list-based sorted map
///
/// # Usage in Indexing
/// Navigable maps are used internally by IndexMap and index structures:
/// - `first_key()` / `last_key()` - Range boundary detection
/// - `higher_key()` / `ceiling_key()` - Forward range scanning
/// - `lower_key()` / `floor_key()` - Backward range scanning
///
/// Example usage pattern from index operations:
/// ```ignore
/// // Get keys for range query: keys >= 10 and < 20
/// if let Some(start_key) = map.ceiling_key(&10)? {
///     if let Some(end_key) = map.lower_key(&20)? {
///         // Scan keys between start_key and end_key
///     }
/// }
/// ```
pub trait NavigableMap<K, V> {
    /// Returns the smallest (minimum) key in the map.
    ///
    /// # Returns
    /// - `Some(key)` - The minimum key if map is non-empty
    /// - `None` - If map is empty
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Returns immediately without iteration cost for BTreeMap. For SkipMap,
    /// accesses the front entry. No side effects on map state.
    fn first_key(&self) -> NitriteResult<Option<K>>;

    /// Returns the largest (maximum) key in the map.
    ///
    /// # Returns
    /// - `Some(key)` - The maximum key if map is non-empty
    /// - `None` - If map is empty
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Returns immediately without iteration cost for BTreeMap. For SkipMap,
    /// accesses the back entry. No side effects on map state.
    fn last_key(&self) -> NitriteResult<Option<K>>;

    /// Returns the smallest key strictly greater than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Some(k)` - If a key k exists where k > key
    /// - `None` - If no key is greater than the given key
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Uses range queries excluding the given key (open lower bound).
    /// Does not include the reference key itself even if it exists.
    ///
    /// # Used in
    /// Forward range scanning, finding next key in sequence.
    fn higher_key(&self, key: &K) -> NitriteResult<Option<K>>;

    /// Returns the smallest key greater than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Some(k)` - If a key k exists where k >= key
    /// - `None` - If no key is >= the given key
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Uses range queries including the given key (closed lower bound).
    /// Returns the reference key itself if it exists in the map.
    ///
    /// # Used in
    /// Starting range queries from inclusive boundaries.
    fn ceiling_key(&self, key: &K) -> NitriteResult<Option<K>>;

    /// Returns the largest key strictly less than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Some(k)` - If a key k exists where k < key
    /// - `None` - If no key is less than the given key
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Uses range queries excluding the given key (open upper bound).
    /// Does not include the reference key itself even if it exists.
    ///
    /// # Used in
    /// Backward range scanning, finding previous key in sequence.
    fn lower_key(&self, key: &K) -> NitriteResult<Option<K>>;

    /// Returns the largest key less than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Some(k)` - If a key k exists where k <= key
    /// - `None` - If no key is <= the given key
    ///
    /// # Errors
    /// Returns NitriteResult error if map is in corrupt state or access fails.
    ///
    /// # Behavior
    /// Uses range queries including the given key (closed upper bound).
    /// Returns the reference key itself if it exists in the map.
    ///
    /// # Used in
    /// Ending range queries at inclusive boundaries, finding maximum key <= value.
    fn floor_key(&self, key: &K) -> NitriteResult<Option<K>>;
}

impl<K, V> NavigableMap<K, V> for BTreeMap<K, V>
where
    K: Ord + Clone,
{
    /// Returns the minimum key in the BTreeMap.
    ///
    /// # Returns
    /// - `Ok(Some(key))` - The smallest key if map is non-empty
    /// - `Ok(None)` - If map is empty
    ///
    /// # Implementation
    /// Uses BTreeMap::keys() iterator which is O(log n) to find first node,
    /// then O(1) to access the first element.
    ///
    /// # Usage
    /// Get the starting point for a forward range scan in index operations.
    #[inline]
    fn first_key(&self) -> NitriteResult<Option<K>> {
        Ok(self.keys().next().cloned())
    }

    /// Returns the maximum key in the BTreeMap.
    ///
    /// # Returns
    /// - `Ok(Some(key))` - The largest key if map is non-empty
    /// - `Ok(None)` - If map is empty
    ///
    /// # Implementation
    /// Uses BTreeMap::keys() with next_back() for reverse iteration.
    /// O(log n) to find last node, then O(1) to access it.
    ///
    /// # Usage
    /// Get the ending point for a backward range scan in index operations.
    #[inline]
    fn last_key(&self) -> NitriteResult<Option<K>> {
        Ok(self.keys().next_back().cloned())
    }

    /// Returns the smallest key strictly greater than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - First key > key, if exists
    /// - `Ok(None)` - If no key is greater
    ///
    /// # Implementation
    /// Uses BTreeMap::range() with exclusive lower bound and unbounded upper bound.
    /// O(log n) for range setup, O(1) to get first element.
    /// Does NOT include the reference key even if it exists.
    ///
    /// # Usage
    /// Navigate forward to next distinct key during range scans.
    #[inline]
    fn higher_key(&self, key: &K) -> NitriteResult<Option<K>> {
        // Use range with direct iterator to avoid double allocation
        Ok(self
            .range((Excluded(key), Unbounded))
            .next()
            .map(|(k, _)| k.clone()))
    }

    /// Returns the smallest key greater than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - First key >= key, if exists
    /// - `Ok(None)` - If no key is >= the reference key
    ///
    /// # Implementation
    /// Uses BTreeMap::range() with inclusive lower bound.
    /// O(log n) for range setup, O(1) to get first element.
    /// Includes the reference key if it exists in the map.
    ///
    /// # Usage
    /// Find starting key for inclusive range queries (key >= value).
    #[inline]
    fn ceiling_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Included(key), Unbounded))
            .next()
            .map(|(k, _)| k.clone()))
    }

    /// Returns the largest key strictly less than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - Last key < key, if exists
    /// - `Ok(None)` - If no key is less than the reference key
    ///
    /// # Implementation
    /// Uses BTreeMap::range() with unbounded lower bound and exclusive upper bound.
    /// O(log n) for range setup, O(1) to access last element via next_back().
    /// Does NOT include the reference key even if it exists.
    ///
    /// # Usage
    /// Navigate backward to previous distinct key during reverse scans.
    #[inline]
    fn lower_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Unbounded, Excluded(key)))
            .next_back()
            .map(|(k, _)| k.clone()))
    }

    /// Returns the largest key less than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - Last key <= key, if exists
    /// - `Ok(None)` - If no key is <= the reference key
    ///
    /// # Implementation
    /// Uses BTreeMap::range() with unbounded lower bound and inclusive upper bound.
    /// O(log n) for range setup, O(1) to access last element via next_back().
    /// Includes the reference key if it exists in the map.
    ///
    /// # Usage
    /// Find ending key for inclusive range queries (key <= value).
    #[inline]
    fn floor_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Unbounded, Included(key)))
            .next_back()
            .map(|(k, _)| k.clone()))
    }
}

impl<K, V> NavigableMap<K, V> for SkipMap<K, V>
where
    K: Ord + Clone,
{
    /// Returns the minimum key in the SkipMap.
    ///
    /// # Returns
    /// - `Ok(Some(key))` - The smallest key if map is non-empty
    /// - `Ok(None)` - If map is empty
    ///
    /// # Implementation
    /// Uses SkipMap::front() which accesses the first node in the skip list.
    /// O(1) to access the front pointer, O(1) to extract key.
    /// Thread-safe without blocking.
    ///
    /// # Usage
    /// Get the starting point for a forward range scan in concurrent index operations.
    #[inline]
    fn first_key(&self) -> NitriteResult<Option<K>> {
        Ok(self.front().map(|entry| entry.key().clone()))
    }

    /// Returns the maximum key in the SkipMap.
    ///
    /// # Returns
    /// - `Ok(Some(key))` - The largest key if map is non-empty
    /// - `Ok(None)` - If map is empty
    ///
    /// # Implementation
    /// Uses SkipMap::back() which accesses the last node in the skip list.
    /// O(1) to access the back pointer, O(1) to extract key.
    /// Thread-safe without blocking.
    ///
    /// # Usage
    /// Get the ending point for a backward range scan in concurrent index operations.
    #[inline]
    fn last_key(&self) -> NitriteResult<Option<K>> {
        Ok(self.back().map(|entry| entry.key().clone()))
    }

    /// Returns the smallest key strictly greater than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - First key > key, if exists
    /// - `Ok(None)` - If no key is greater
    ///
    /// # Implementation
    /// Uses SkipMap::range() with exclusive lower bound and unbounded upper bound.
    /// O(log n) for range navigation via skip list levels.
    /// Does NOT include the reference key even if it exists.
    /// Thread-safe with lock-free iteration.
    ///
    /// # Usage
    /// Navigate forward to next distinct key during concurrent range scans.
    #[inline]
    fn higher_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Excluded(key), Unbounded))
            .next()
            .map(|entry| entry.key().clone()))
    }

    /// Returns the smallest key greater than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - First key >= key, if exists
    /// - `Ok(None)` - If no key is >= the reference key
    ///
    /// # Implementation
    /// Uses SkipMap::range() with inclusive lower bound.
    /// O(log n) for range navigation via skip list levels.
    /// Includes the reference key if it exists in the map.
    /// Thread-safe with lock-free iteration.
    ///
    /// # Usage
    /// Find starting key for inclusive range queries in concurrent operations (key >= value).
    #[inline]
    fn ceiling_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Included(key), Unbounded))
            .next()
            .map(|entry| entry.key().clone()))
    }

    /// Returns the largest key strictly less than the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - Last key < key, if exists
    /// - `Ok(None)` - If no key is less than the reference key
    ///
    /// # Implementation
    /// Uses SkipMap::range() with unbounded lower bound and exclusive upper bound.
    /// O(log n) for range navigation, then reverse iteration to get last element.
    /// Does NOT include the reference key even if it exists.
    /// Thread-safe with lock-free iteration.
    ///
    /// # Usage
    /// Navigate backward to previous distinct key during concurrent reverse scans.
    #[inline]
    fn lower_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Unbounded, Excluded(key)))
            .next_back()
            .map(|entry| entry.key().clone()))
    }

    /// Returns the largest key less than or equal to the given key.
    ///
    /// # Arguments
    /// * `key` - The reference key to compare against
    ///
    /// # Returns
    /// - `Ok(Some(k))` - Last key <= key, if exists
    /// - `Ok(None)` - If no key is <= the reference key
    ///
    /// # Implementation
    /// Uses SkipMap::range() with unbounded lower bound and inclusive upper bound.
    /// O(log n) for range navigation, then reverse iteration to get last element.
    /// Includes the reference key if it exists in the map.
    /// Thread-safe with lock-free iteration.
    ///
    /// # Usage
    /// Find ending key for inclusive range queries in concurrent operations (key <= value).
    #[inline]
    fn floor_key(&self, key: &K) -> NitriteResult<Option<K>> {
        Ok(self
            .range((Unbounded, Included(key)))
            .next_back()
            .map(|entry| entry.key().clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Key, Value};
    use crossbeam_skiplist::SkipMap;

    #[test]
    fn test_btree_map_first_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.first_key().unwrap(), Some(1));
    }

    #[test]
    fn test_btree_map_last_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.last_key().unwrap(), Some(2));
    }

    #[test]
    fn test_btree_map_higher_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.higher_key(&1).unwrap(), Some(2));
    }

    #[test]
    fn test_btree_map_ceiling_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.ceiling_key(&1).unwrap(), Some(1));
    }

    #[test]
    fn test_btree_map_lower_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.lower_key(&2).unwrap(), Some(1));
    }

    #[test]
    fn test_btree_map_floor_key() {
        let mut map = BTreeMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.floor_key(&2).unwrap(), Some(2));
    }

    #[test]
    fn test_skip_map_first_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.first_key().unwrap(), Some(1));
    }

    #[test]
    fn test_skip_map_last_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.last_key().unwrap(), Some(2));
    }

    #[test]
    fn test_skip_map_higher_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.higher_key(&1).unwrap(), Some(2));
    }

    #[test]
    fn test_skip_map_ceiling_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.ceiling_key(&1).unwrap(), Some(1));
    }

    #[test]
    fn test_skip_map_lower_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.lower_key(&2).unwrap(), Some(1));
    }

    #[test]
    fn test_skip_map_floor_key() {
        let map = SkipMap::new();
        map.insert(1, "a");
        map.insert(2, "b");
        assert_eq!(map.floor_key(&2).unwrap(), Some(2));
    }
    
    #[test]
    fn test() {
        let mut map = BTreeMap::new();
        let key1 = Key::from("key1");
        let key2 = Key::from("key2");
        let value = Value::from("value1");
        map.insert(key1.clone(), value.clone());
        map.insert(key2.clone(), value.clone());
        assert_eq!(map.floor_key(&key2).unwrap(), Some(key2));
    }

    #[test]
    fn bench_btree_map_navigable_ops() {
        let mut map = BTreeMap::new();
        for i in 0..1000 {
            map.insert(i, format!("value{}", i));
        }
        
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            for i in 0..100 {
                let _ = map.first_key();
                let _ = map.last_key();
                let _ = map.higher_key(&i);
                let _ = map.ceiling_key(&i);
                let _ = map.lower_key(&i);
                let _ = map.floor_key(&i);
            }
        }
        let elapsed = start.elapsed();
        println!(
            "BTreeMap navigable ops (1000x 100 queries): {:?} ({:.3}µs per op set)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0 / 100.0
        );
    }

    #[test]
    fn bench_skip_map_navigable_ops() {
        let map = SkipMap::new();
        for i in 0..1000 {
            map.insert(i, format!("value{}", i));
        }
        
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            for i in 0..100 {
                let _ = map.first_key();
                let _ = map.last_key();
                let _ = map.higher_key(&i);
                let _ = map.ceiling_key(&i);
                let _ = map.lower_key(&i);
                let _ = map.floor_key(&i);
            }
        }
        let elapsed = start.elapsed();
        println!(
            "SkipMap navigable ops (1000x 100 queries): {:?} ({:.3}µs per op set)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0 / 100.0
        );
    }
}

