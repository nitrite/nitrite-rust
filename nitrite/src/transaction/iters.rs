use parking_lot::Mutex;
/// Transaction-specific iterators for multi-map orchestration
///
/// These providers handle iteration over the transaction's three logical maps:
/// - Backing map: New/modified entries
/// - Primary map: Original entries (read-through)
/// - Tombstones: Deleted entries (to exclude from results)

use std::collections::HashSet;
use std::sync::Arc;

use crate::common::{Key, Value};
use crate::errors::NitriteResult;
use crate::store::{EntryIteratorProvider, KeyIteratorProvider, NitriteMap, ValueIteratorProvider};

/// Orchestrates entry iteration over backing, primary, and tombstone maps
///
/// Iteration order:
/// 1. Backing map entries (modified/new)
/// 2. Primary map entries not in backing or tombstones
/// 
/// OPTIMIZED for performance:
/// - Uses map navigation methods (higher_key, lower_key) for stateless iteration
/// - Releases locks early to reduce contention
/// - Minimizes key cloning
/// - Supports bidirectional iteration with backward state tracking
pub struct TransactionEntryProvider {
    backing_map: NitriteMap,
    primary_map: NitriteMap,
    tombstones: Arc<Mutex<HashSet<Key>>>,
    cleared: Arc<Mutex<bool>>,
    current_key: Option<Key>,
    current_phase: IterationPhase,
    // Reverse iteration state
    has_started_reverse: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum IterationPhase {
    Backing,
    Primary,
    Done,
}

impl TransactionEntryProvider {
    /// Create a new transaction entry provider
    pub fn new(
        backing_map: NitriteMap,
        primary_map: NitriteMap,
        tombstones: Arc<Mutex<HashSet<Key>>>,
        cleared: Arc<Mutex<bool>>,
    ) -> NitriteResult<Self> {
        Ok(TransactionEntryProvider {
            backing_map,
            primary_map,
            tombstones,
            cleared,
            current_key: None,
            current_phase: IterationPhase::Backing,
            has_started_reverse: false,
        })
    }
}

impl EntryIteratorProvider for TransactionEntryProvider {
    fn next_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Backing => {
                    // Get next key from backing map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.backing_map.higher_key(key),
                        None => self.backing_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            match self.backing_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok((key, value))),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            // No more keys in backing, move to primary
                            self.current_phase = IterationPhase::Primary;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Primary => {
                    // Get next key from primary map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.primary_map.higher_key(key),
                        None => self.primary_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            match self.primary_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok((key, value))),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }

    fn prev_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        // Initialize reverse iteration state on first call
        if !self.has_started_reverse {
            self.has_started_reverse = true;
            self.current_phase = IterationPhase::Primary;
            self.current_key = None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Primary => {
                    // Get previous key from primary map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.primary_map.lower_key(key),
                        None => self.primary_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            match self.primary_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok((key, value))),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            // No more keys in primary, move to backing
                            self.current_phase = IterationPhase::Backing;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Backing => {
                    // Get previous key from backing map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.backing_map.lower_key(key),
                        None => self.backing_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            match self.backing_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok((key, value))),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }
}

/// Orchestrates key iteration over backing, primary, and tombstone maps
/// 
/// OPTIMIZED: Uses map navigation methods (higher_key, lower_key) for stateless iteration
pub struct TransactionKeyProvider {
    backing_map: NitriteMap,
    primary_map: NitriteMap,
    tombstones: Arc<Mutex<HashSet<Key>>>,
    cleared: Arc<Mutex<bool>>,
    current_key: Option<Key>,
    current_phase: IterationPhase,
    has_started_reverse: bool,
}

impl TransactionKeyProvider {
    /// Create a new transaction key provider
    pub fn new(
        backing_map: NitriteMap,
        primary_map: NitriteMap,
        tombstones: Arc<Mutex<HashSet<Key>>>,
        cleared: Arc<Mutex<bool>>,
    ) -> NitriteResult<Self> {
        Ok(TransactionKeyProvider {
            backing_map,
            primary_map,
            tombstones,
            cleared,
            current_key: None,
            current_phase: IterationPhase::Backing,
            has_started_reverse: false,
        })
    }
}

impl KeyIteratorProvider for TransactionKeyProvider {
    fn next_key(&mut self) -> Option<NitriteResult<Key>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Backing => {
                    // Get next key from backing map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.backing_map.higher_key(key),
                        None => self.backing_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            return Some(Ok(key));
                        }
                        Ok(None) => {
                            // No more keys in backing, move to primary
                            self.current_phase = IterationPhase::Primary;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Primary => {
                    // Get next key from primary map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.primary_map.higher_key(key),
                        None => self.primary_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            return Some(Ok(key));
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }

    fn prev_key(&mut self) -> Option<NitriteResult<Key>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        // Initialize reverse iteration state on first call
        if !self.has_started_reverse {
            self.has_started_reverse = true;
            self.current_phase = IterationPhase::Primary;
            self.current_key = None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Primary => {
                    // Get previous key from primary map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.primary_map.lower_key(key),
                        None => self.primary_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            return Some(Ok(key));
                        }
                        Ok(None) => {
                            // No more keys in primary, move to backing
                            self.current_phase = IterationPhase::Backing;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Backing => {
                    // Get previous key from backing map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.backing_map.lower_key(key),
                        None => self.backing_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            return Some(Ok(key));
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }
}

/// Orchestrates value iteration over backing, primary, and tombstone maps
/// 
/// OPTIMIZED: Uses map navigation methods (higher_key, lower_key) for stateless iteration
pub struct TransactionValueProvider {
    backing_map: NitriteMap,
    primary_map: NitriteMap,
    tombstones: Arc<Mutex<HashSet<Key>>>,
    cleared: Arc<Mutex<bool>>,
    current_key: Option<Key>,
    current_phase: IterationPhase,
    has_started_reverse: bool,
}

impl TransactionValueProvider {
    /// Create a new transaction value provider
    pub fn new(
        backing_map: NitriteMap,
        primary_map: NitriteMap,
        tombstones: Arc<Mutex<HashSet<Key>>>,
        cleared: Arc<Mutex<bool>>,
    ) -> NitriteResult<Self> {
        Ok(TransactionValueProvider {
            backing_map,
            primary_map,
            tombstones,
            cleared,
            current_key: None,
            current_phase: IterationPhase::Backing,
            has_started_reverse: false,
        })
    }
}

impl ValueIteratorProvider for TransactionValueProvider {
    fn next_value(&mut self) -> Option<NitriteResult<Value>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Backing => {
                    // Get next key from backing map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.backing_map.higher_key(key),
                        None => self.backing_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            match self.backing_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok(value)),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            // No more keys in backing, move to primary
                            self.current_phase = IterationPhase::Primary;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Primary => {
                    // Get next key from primary map
                    let next_key_result = match &self.current_key {
                        Some(key) => self.primary_map.higher_key(key),
                        None => self.primary_map.first_key(),
                    };

                    match next_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            match self.primary_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok(value)),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }

    fn prev_value(&mut self) -> Option<NitriteResult<Value>> {
        // OPTIMIZATION: Check cleared once and release lock immediately
        if *self.cleared.lock() {
            return None;
        }

        // Initialize reverse iteration state on first call
        if !self.has_started_reverse {
            self.has_started_reverse = true;
            self.current_phase = IterationPhase::Primary;
            self.current_key = None;
        }

        loop {
            match self.current_phase {
                IterationPhase::Primary => {
                    // Get previous key from primary map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.primary_map.lower_key(key),
                        None => self.primary_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // Check if key is in backing or tombstones
                            let in_backing = self.backing_map.contains_key(&key).unwrap_or(false);
                            let is_tombstoned = {
                                let tombstones = self.tombstones.lock();
                                tombstones.contains(&key)
                            };

                            if in_backing || is_tombstoned {
                                continue;
                            }

                            match self.primary_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok(value)),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            // No more keys in primary, move to backing
                            self.current_phase = IterationPhase::Backing;
                            self.current_key = None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Backing => {
                    // Get previous key from backing map
                    let prev_key_result = match &self.current_key {
                        Some(key) => self.backing_map.lower_key(key),
                        None => self.backing_map.last_key(),
                    };

                    match prev_key_result {
                        Ok(Some(key)) => {
                            self.current_key = Some(key.clone());
                            
                            // OPTIMIZATION: No tombstone check needed in backing phase
                            // Invariant: backing keys cannot be in tombstones
                            match self.backing_map.get(&key) {
                                Ok(Some(value)) => return Some(Ok(value)),
                                Ok(None) => continue,
                                Err(e) => return Some(Err(e)),
                            }
                        }
                        Ok(None) => {
                            self.current_phase = IterationPhase::Done;
                            return None;
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }

                IterationPhase::Done => {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{AttributeAware, Attributes, Key, Value};
    use crate::store::{EntryIterator, KeyIterator, NitriteMapProvider, NitriteStore, ValueIterator};
    use std::sync::Arc as StdArc;


    struct MockNitriteMapData {
        entries: std::collections::BTreeMap<String, String>,
    }

    #[derive(Clone)]
    struct MockNitriteMapImpl {
        data: StdArc<parking_lot::Mutex<MockNitriteMapData>>,
    }

    impl MockNitriteMapImpl {
        fn new(entries: Vec<(String, String)>) -> Self {
            let mut map = std::collections::BTreeMap::new();
            for (k, v) in entries {
                map.insert(k, v);
            }
            MockNitriteMapImpl {
                data: StdArc::new(parking_lot::Mutex::new(MockNitriteMapData { entries: map })),
            }
        }

        fn empty() -> Self {
            MockNitriteMapImpl {
                data: StdArc::new(parking_lot::Mutex::new(MockNitriteMapData {
                    entries: std::collections::BTreeMap::new(),
                })),
            }
        }
    }

    impl AttributeAware for MockNitriteMapImpl {
        fn attributes(&self) -> crate::errors::NitriteResult<Option<Attributes>> {
            Ok(None)
        }

        fn set_attributes(&self, _attributes: Attributes) -> crate::errors::NitriteResult<()> {
            Ok(())
        }
    }

    impl NitriteMapProvider for MockNitriteMapImpl {
        fn contains_key(&self, key: &Key) -> crate::errors::NitriteResult<bool> {
            let data = self.data.lock();
            Ok(data.entries.contains_key(key.as_string().unwrap()))
        }

        fn get(&self, key: &Key) -> crate::errors::NitriteResult<Option<Value>> {
            let data = self.data.lock();
            Ok(data
                .entries
                .get(key.as_string().unwrap())
                .map(|v| Value::from(v.as_str())))
        }

        fn clear(&self) -> crate::errors::NitriteResult<()> {
            let mut data = self.data.lock();
            data.entries.clear();
            Ok(())
        }

        fn is_closed(&self) -> crate::errors::NitriteResult<bool> {
            Ok(false)
        }

        fn close(&self) -> crate::errors::NitriteResult<()> {
            Ok(())
        }

        fn values(&self) -> crate::errors::NitriteResult<ValueIterator> {
            Err(crate::errors::NitriteError::new(
                "Not implemented",
                crate::errors::ErrorKind::InvalidOperation,
            ))
        }

        fn keys(&self) -> crate::errors::NitriteResult<KeyIterator> {
            Err(crate::errors::NitriteError::new(
                "Not implemented",
                crate::errors::ErrorKind::InvalidOperation,
            ))
        }

        fn remove(&self, key: &Key) -> crate::errors::NitriteResult<Option<Value>> {
            let mut data = self.data.lock();
            Ok(data
                .entries
                .remove(key.as_string().unwrap())
                .map(|v| Value::from(v.as_str())))
        }

        fn put(&self, key: Key, value: Value) -> crate::errors::NitriteResult<()> {
            let mut data = self.data.lock();
            data.entries
                .insert(key.as_string().unwrap().to_string(), value.as_string().unwrap().to_string());
            Ok(())
        }

        fn size(&self) -> crate::errors::NitriteResult<u64> {
            let data = self.data.lock();
            Ok(data.entries.len() as u64)
        }

        fn put_if_absent(
            &self,
            key: Key,
            value: Value,
        ) -> crate::errors::NitriteResult<Option<Value>> {
            let mut data = self.data.lock();
            let result = data
                .entries
                .entry(key.as_string().unwrap().to_string())
                .or_insert_with(|| value.as_string().unwrap().to_string())
                .clone();
            Ok(Some(Value::from(result.as_str())))
        }

        fn first_key(&self) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            Ok(data.entries.keys().next().map(|k| Key::from(k.as_str())))
        }

        fn last_key(&self) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            Ok(data.entries.keys().last().map(|k| Key::from(k.as_str())))
        }

        fn higher_key(&self, key: &Key) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            let key_name = key.as_string().unwrap().clone();
            Ok(data
                .entries
                .range((std::ops::Bound::Excluded(key_name), std::ops::Bound::Unbounded))
                .next()
                .map(|(k, _)| Key::from(k.as_str())))
        }

        fn ceiling_key(&self, key: &Key) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            let key_name = key.as_string().unwrap().clone();
            Ok(data
                .entries
                .range((std::ops::Bound::Included(key_name), std::ops::Bound::Unbounded))
                .next()
                .map(|(k, _)| Key::from(k.as_str())))
        }

        fn lower_key(&self, key: &Key) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            let key_name = key.as_string().unwrap().clone();
            Ok(data
                .entries
                .range((std::ops::Bound::Unbounded, std::ops::Bound::Excluded(key_name)))
                .last()
                .map(|(k, _)| Key::from(k.as_str())))
        }

        fn floor_key(&self, key: &Key) -> crate::errors::NitriteResult<Option<Key>> {
            let data = self.data.lock();
            let key_name = key.as_string().unwrap().clone();
            Ok(data
                .entries
                .range((std::ops::Bound::Unbounded, std::ops::Bound::Included(key_name)))
                .last()
                .map(|(k, _)| Key::from(k.as_str())))
        }

        fn is_empty(&self) -> crate::errors::NitriteResult<bool> {
            let data = self.data.lock();
            Ok(data.entries.is_empty())
        }

        fn get_store(&self) -> crate::errors::NitriteResult<NitriteStore> {
            Err(crate::errors::NitriteError::new(
                "Not implemented",
                crate::errors::ErrorKind::InvalidOperation,
            ))
        }

        fn get_name(&self) -> crate::errors::NitriteResult<String> {
            Ok("mock".to_string())
        }

        fn entries(&self) -> crate::errors::NitriteResult<EntryIterator> {
            Err(crate::errors::NitriteError::new(
                "Not implemented",
                crate::errors::ErrorKind::InvalidOperation,
            ))
        }

        fn reverse_entries(&self) -> crate::errors::NitriteResult<std::iter::Rev<EntryIterator>> {
            Err(crate::errors::NitriteError::new(
                "Not implemented",
                crate::errors::ErrorKind::InvalidOperation,
            ))
        }

        fn dispose(&self) -> crate::errors::NitriteResult<()> {
            Ok(())
        }

        fn is_dropped(&self) -> crate::errors::NitriteResult<bool> {
            Ok(false)
        }
    }

    

    #[test]
    fn test_entry_provider_creation() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared);
        assert!(provider.is_ok());
    }

    #[test]
    fn test_entry_provider_empty_maps() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_entry();
        assert!(result.is_none());
    }

    #[test]
    fn test_entry_provider_from_backing_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let entry = provider.next_entry();

        assert!(entry.is_some());
        let result = entry.unwrap();
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key.as_string().unwrap(), &"key1".to_string());
        assert_eq!(value.as_string().unwrap(), &"val1".to_string());
    }

    #[test]
    fn test_entry_provider_from_primary_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let entry = provider.next_entry();

        assert!(entry.is_some());
        let result = entry.unwrap();
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key.as_string().unwrap(), &"key1".to_string());
        assert_eq!(value.as_string().unwrap(), &"val1".to_string());
    }

    #[test]
    fn test_entry_provider_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key1"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        // Should skip key1 (tombstoned) and return key2
        let entry = provider.next_entry();
        assert!(entry.is_some());
        let (key, _) = entry.unwrap().unwrap();
        assert_eq!(key.as_string().unwrap(), "key2");
    }

    #[test]
    fn test_entry_provider_skips_keys_in_backing() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "newval1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        // Should return both entries from backing, then key2 from primary
        let e1 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e1.0.as_string().unwrap(), "key1");
        assert_eq!(e1.1.as_string().unwrap(), "newval1");

        let e2 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e2.0.as_string().unwrap(), "key2");
    }

    #[test]
    fn test_entry_provider_returns_none_when_cleared() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(true));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_entry();
        assert!(result.is_none());
    }

    #[test]
    fn test_entry_provider_prev_entry_from_backing() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let entry = provider.prev_entry();

        assert!(entry.is_some());
        let result = entry.unwrap();
        assert!(result.is_ok());
        let (key, value) = result.unwrap();
        assert_eq!(key.as_string().unwrap(), "key1");
        assert_eq!(value.as_string().unwrap(), "val1");
    }

    #[test]
    fn test_entry_provider_prev_entry_from_primary() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let entry = provider.prev_entry();

        assert!(entry.is_some());
        let (key, _) = entry.unwrap().unwrap();
        assert_eq!(key.as_string().unwrap(), "key1");
    }

    #[test]
    fn test_entry_provider_prev_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key2"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        // Should skip key2 (tombstoned) and return key1
        let entry = provider.prev_entry();
        assert!(entry.is_some());
        let (key, _) = entry.unwrap().unwrap();
        assert_eq!(key.as_string().unwrap(), "key1");
    }

    #[test]
    fn test_entry_provider_prev_returns_none_when_cleared() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(true));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.prev_entry();
        assert!(result.is_none());
    }

    

    #[test]
    fn test_key_provider_creation() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared);
        assert!(provider.is_ok());
    }

    #[test]
    fn test_key_provider_empty_maps() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_key();
        assert!(result.is_none());
    }

    #[test]
    fn test_key_provider_from_backing_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        let key = provider.next_key();

        assert!(key.is_some());
        let result = key.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_string().unwrap(), "key1");
    }

    #[test]
    fn test_key_provider_from_primary_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        let key = provider.next_key();

        assert!(key.is_some());
        assert_eq!(key.unwrap().unwrap().as_string().unwrap(), "key1");
    }

    #[test]
    fn test_key_provider_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key1"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        let key = provider.next_key();
        assert_eq!(key.unwrap().unwrap().as_string().unwrap(), "key2");
    }

    #[test]
    fn test_key_provider_returns_none_when_cleared() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(true));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_key();
        assert!(result.is_none());
    }

    #[test]
    fn test_key_provider_prev_key() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        let key = provider.prev_key();

        assert!(key.is_some());
        assert_eq!(key.unwrap().unwrap().as_string().unwrap(), "key1");
    }

    #[test]
    fn test_key_provider_prev_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key2"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        let key = provider.prev_key();
        assert_eq!(key.unwrap().unwrap().as_string().unwrap(), "key1");
    }

    

    #[test]
    fn test_value_provider_creation() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let provider = TransactionValueProvider::new(backing, primary, tombstones, cleared);
        assert!(provider.is_ok());
    }

    #[test]
    fn test_value_provider_empty_maps() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_value();
        assert!(result.is_none());
    }

    #[test]
    fn test_value_provider_from_backing_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        let value = provider.next_value();

        assert!(value.is_some());
        let result = value.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_string().unwrap(), "val1");
    }

    #[test]
    fn test_value_provider_from_primary_map() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        let value = provider.next_value();

        assert!(value.is_some());
        assert_eq!(value.unwrap().unwrap().as_string().unwrap(), "val1");
    }

    #[test]
    fn test_value_provider_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key1"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        let value = provider.next_value();
        assert_eq!(value.unwrap().unwrap().as_string().unwrap(), "val2");
    }

    #[test]
    fn test_value_provider_returns_none_when_cleared() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(true));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        let result = provider.next_value();
        assert!(result.is_none());
    }

    #[test]
    fn test_value_provider_prev_value() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        let value = provider.prev_value();

        assert!(value.is_some());
        assert_eq!(value.unwrap().unwrap().as_string().unwrap(), "val1");
    }

    #[test]
    fn test_value_provider_prev_skips_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key2"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        let value = provider.prev_value();
        assert_eq!(value.unwrap().unwrap().as_string().unwrap(), "val1");
    }

    

    #[test]
    fn test_entry_provider_multiple_entries_sequence() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("a".to_string(), "val_a".to_string()),
            ("b".to_string(), "val_b".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("c".to_string(), "val_c".to_string()),
            ("d".to_string(), "val_d".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();

        let e1 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e1.0.as_string().unwrap(), "a");

        let e2 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e2.0.as_string().unwrap(), "b");

        let e3 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e3.0.as_string().unwrap(), "c");

        let e4 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e4.0.as_string().unwrap(), "d");

        let e5 = provider.next_entry();
        assert!(e5.is_none());
    }

    #[test]
    fn test_key_provider_multiple_keys_sequence() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("a".to_string(), "val_a".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("b".to_string(), "val_b".to_string()),
            ("c".to_string(), "val_c".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionKeyProvider::new(backing, primary, tombstones, cleared).unwrap();

        let k1 = provider.next_key().unwrap().unwrap();
        assert_eq!(k1.as_string().unwrap(), "a");

        let k2 = provider.next_key().unwrap().unwrap();
        assert_eq!(k2.as_string().unwrap(), "b");

        let k3 = provider.next_key().unwrap().unwrap();
        assert_eq!(k3.as_string().unwrap(), "c");

        let k4 = provider.next_key();
        assert!(k4.is_none());
    }

    #[test]
    fn test_value_provider_multiple_values_sequence() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("a".to_string(), "val_a".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("b".to_string(), "val_b".to_string()),
            ("c".to_string(), "val_c".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionValueProvider::new(backing, primary, tombstones, cleared).unwrap();

        let v1 = provider.next_value().unwrap().unwrap();
        assert_eq!(v1.as_string().unwrap(), "val_a");

        let v2 = provider.next_value().unwrap().unwrap();
        assert_eq!(v2.as_string().unwrap(), "val_b");

        let v3 = provider.next_value().unwrap().unwrap();
        assert_eq!(v3.as_string().unwrap(), "val_c");

        let v4 = provider.next_value();
        assert!(v4.is_none());
    }

    #[test]
    fn test_iteration_phase_transitions() {
        // Use empty backing to test phase transition immediately
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("a".to_string(), "val_a".to_string()),
            ("b".to_string(), "val_b".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();

        // Should start in Backing phase
        assert_eq!(provider.current_phase, IterationPhase::Backing);

        // First call should transition to Primary (no entries in backing)
        let e1 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e1.0.as_string().unwrap(), &"a".to_string());
        assert_eq!(provider.current_phase, IterationPhase::Primary);

        // Second call should get next from primary
        let e2 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e2.0.as_string().unwrap(), &"b".to_string());
        assert_eq!(provider.current_phase, IterationPhase::Primary);

        // Third call should transition to Done
        let result = provider.next_entry();
        assert!(result.is_none());
        assert_eq!(provider.current_phase, IterationPhase::Done);
    }

    #[test]
    fn test_multiple_tombstoned_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::empty());
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "val1".to_string()),
            ("key2".to_string(), "val2".to_string()),
            ("key3".to_string(), "val3".to_string()),
        ]));
        let mut tombstones = HashSet::new();
        tombstones.insert(Key::from("key1"));
        tombstones.insert(Key::from("key3"));
        let tombstones = Arc::new(parking_lot::Mutex::new(tombstones));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();
        
        // Should only return key2
        let e1 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e1.0.as_string().unwrap(), "key2");

        let e2 = provider.next_entry();
        assert!(e2.is_none());
    }

    #[test]
    fn test_backing_and_primary_with_overlapping_keys() {
        let backing = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "new_val1".to_string()),
            ("key2".to_string(), "new_val2".to_string()),
        ]));
        let primary = crate::store::NitriteMap::new(MockNitriteMapImpl::new(vec![
            ("key1".to_string(), "old_val1".to_string()),
            ("key3".to_string(), "val3".to_string()),
        ]));
        let tombstones = Arc::new(parking_lot::Mutex::new(HashSet::new()));
        let cleared = Arc::new(parking_lot::Mutex::new(false));

        let mut provider = TransactionEntryProvider::new(backing, primary, tombstones, cleared).unwrap();

        let e1 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e1.0.as_string().unwrap(), "key1");
        assert_eq!(e1.1.as_string().unwrap(), "new_val1"); // From backing

        let e2 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e2.0.as_string().unwrap(), "key2");
        assert_eq!(e2.1.as_string().unwrap(), "new_val2"); // From backing

        let e3 = provider.next_entry().unwrap().unwrap();
        assert_eq!(e3.0.as_string().unwrap(), "key3");
        assert_eq!(e3.1.as_string().unwrap(), "val3"); // From primary

        assert!(provider.next_entry().is_none());
    }
}
