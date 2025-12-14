use crate::common::{Key, Value};
use crate::errors::NitriteResult;
use crate::store::NitriteMap;

use super::NitriteMapProvider;
use std::sync::Arc;

// These traits define the interface for any iterator implementation

/// Trait for implementing entry iteration over (Key, Value) pairs.
///
/// # Purpose
///
/// `EntryIteratorProvider` defines the contract for any iterator that can traverse
/// key-value pairs in both forward and backward directions. Implementations must support
/// bidirectional iteration to enable flexible navigation through stored entries.
///
/// # Characteristics
///
/// - **Stateful**: Maintains current position for navigating entries
/// - **Bidirectional**: Supports both forward and backward traversal
/// - **Thread-Safe**: Requires `Send + Sync` for safe concurrent access
/// - **Error Handling**: Returns `NitriteResult<T>` for all operations
/// - **Extensible**: Can be implemented by different backends (NitriteMap, transactions, etc.)
///
/// # Implementations
///
/// Built-in implementations:
/// - `SingleMapEntryProvider`: Iterates entries from a single NitriteMap
/// - `TransactionEntryProvider`: Iterates entries across transaction layers
pub trait EntryIteratorProvider: Send + Sync {
    /// Get the next entry
    fn next_entry(&mut self) -> Option<NitriteResult<(Key, Value)>>;

    /// Get the previous entry (for bidirectional iteration)
    fn prev_entry(&mut self) -> Option<NitriteResult<(Key, Value)>>;
}

/// Trait for implementing key iteration.
///
/// # Purpose
///
/// `KeyIteratorProvider` defines the contract for iterators that traverse only the keys
/// of a store without retrieving associated values. This is useful for operations that
/// only need key information.
///
/// # Characteristics
///
/// - **Stateful**: Maintains current position for navigating keys
/// - **Bidirectional**: Supports both forward and backward traversal
/// - **Thread-Safe**: Requires `Send + Sync` for safe concurrent access
/// - **Error Handling**: Returns `NitriteResult<Key>` for all operations
/// - **Efficient**: Avoids retrieving values, reducing memory usage
///
/// # Implementations
///
/// Built-in implementations:
/// - `SingleMapKeyProvider`: Iterates keys from a single NitriteMap
/// - `TransactionKeyProvider`: Iterates keys across transaction layers
pub trait KeyIteratorProvider: Send + Sync {
    /// Get the next key
    fn next_key(&mut self) -> Option<NitriteResult<Key>>;

    /// Get the previous key (for bidirectional iteration)
    fn prev_key(&mut self) -> Option<NitriteResult<Key>>;
}

/// Trait for implementing value iteration.
///
/// # Purpose
///
/// `ValueIteratorProvider` defines the contract for iterators that traverse only the values
/// of a store without retrieving associated keys. This is useful for operations that
/// only need value information.
///
/// # Characteristics
///
/// - **Stateful**: Maintains current position for navigating values
/// - **Bidirectional**: Supports both forward and backward traversal
/// - **Thread-Safe**: Requires `Send + Sync` for safe concurrent access
/// - **Error Handling**: Returns `NitriteResult<Value>` for all operations
/// - **Efficient**: Avoids retrieving keys, optimizing for value-only access
///
/// # Implementations
///
/// Built-in implementations:
/// - `SingleMapValueProvider`: Iterates values from a single NitriteMap
/// - `TransactionValueProvider`: Iterates values across transaction layers
pub trait ValueIteratorProvider: Send + Sync {
    /// Get the next value
    fn next_value(&mut self) -> Option<NitriteResult<Value>>;

    /// Get the previous value (for bidirectional iteration)
    fn prev_value(&mut self) -> Option<NitriteResult<Value>>;
}


/// A unified facade for bidirectional iteration over (Key, Value) entries.
///
/// # Purpose
///
/// `EntryIterator` wraps any `EntryIteratorProvider` implementation and provides a
/// standard `Iterator` and `DoubleEndedIterator` interface for traversing entries.
/// It enables both forward and backward iteration through key-value pairs in a store.
///
/// # Characteristics
///
/// - **Facade Pattern**: Abstracts away the underlying provider implementation
/// - **Bidirectional**: Implements `DoubleEndedIterator` for both forward and backward traversal
/// - **Thread-Safe**: Uses `Arc<Mutex<_>>` for safe concurrent access
/// - **Cloneable**: Can be cloned cheaply via Arc; clones share iteration state
/// - **Provider-Agnostic**: Works with any `EntryIteratorProvider` (NitriteMap, transactions, etc.)
pub struct EntryIterator {
    provider: Arc<parking_lot::Mutex<Box<dyn EntryIteratorProvider>>>,
}

impl EntryIterator {
    /// Creates a new entry iterator wrapping the given provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Any implementation of `EntryIteratorProvider`
    ///
    /// # Returns
    ///
    /// A new `EntryIterator` ready to use with standard `Iterator` and `DoubleEndedIterator` methods.
    ///
    /// # Type Constraints
    ///
    /// The provider must:
    /// - Implement `EntryIteratorProvider`
    /// - Be `'static` (own all captured data)
    ///
    /// # Behavior
    ///
    /// - Wraps the provider in `Arc<Mutex<_>>` for thread-safe shared access
    /// - Multiple clones of the iterator share the same provider state
    /// - Iteration position is shared across clones
    pub fn new<T: EntryIteratorProvider + 'static>(provider: T) -> Self {
        EntryIterator {
            provider: Arc::new(parking_lot::Mutex::new(Box::new(provider))),
        }
    }
}

impl Clone for EntryIterator {
    fn clone(&self) -> Self {
        EntryIterator {
            provider: Arc::clone(&self.provider),
        }
    }
}

impl Iterator for EntryIterator {
    type Item = NitriteResult<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.next_entry()
    }
}

impl DoubleEndedIterator for EntryIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.prev_entry()
    }
}

/// A unified facade for bidirectional iteration over keys.
///
/// # Purpose
///
/// `KeyIterator` wraps any `KeyIteratorProvider` implementation and provides a
/// standard `Iterator` and `DoubleEndedIterator` interface for traversing only keys.
/// This is more efficient than entry iteration when values are not needed.
///
/// # Characteristics
///
/// - **Facade Pattern**: Abstracts away the underlying provider implementation
/// - **Bidirectional**: Implements `DoubleEndedIterator` for both forward and backward traversal
/// - **Thread-Safe**: Uses `Arc<Mutex<_>>` for safe concurrent access
/// - **Cloneable**: Can be cloned cheaply via Arc; clones share iteration state
/// - **Provider-Agnostic**: Works with any `KeyIteratorProvider` (NitriteMap, transactions, etc.)
pub struct KeyIterator {
    provider: Arc<parking_lot::Mutex<Box<dyn KeyIteratorProvider>>>,
}

impl KeyIterator {
    /// Creates a new key iterator wrapping the given provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Any implementation of `KeyIteratorProvider`
    ///
    /// # Returns
    ///
    /// A new `KeyIterator` ready to use with standard `Iterator` and `DoubleEndedIterator` methods.
    ///
    /// # Type Constraints
    ///
    /// The provider must:
    /// - Implement `KeyIteratorProvider`
    /// - Be `'static` (own all captured data)
    ///
    /// # Behavior
    ///
    /// - Wraps the provider in `Arc<Mutex<_>>` for thread-safe shared access
    /// - Multiple clones of the iterator share the same provider state
    /// - Iteration position is shared across clones
    pub fn new<T: KeyIteratorProvider + 'static>(provider: T) -> Self {
        KeyIterator {
            provider: Arc::new(parking_lot::Mutex::new(Box::new(provider))),
        }
    }
}

impl Clone for KeyIterator {
    fn clone(&self) -> Self {
        KeyIterator {
            provider: Arc::clone(&self.provider),
        }
    }
}

impl Iterator for KeyIterator {
    type Item = NitriteResult<Key>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.next_key()
    }
}

impl DoubleEndedIterator for KeyIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.prev_key()
    }
}

/// A unified facade for bidirectional iteration over values.
///
/// # Purpose
///
/// `ValueIterator` wraps any `ValueIteratorProvider` implementation and provides a
/// standard `Iterator` and `DoubleEndedIterator` interface for traversing only values.
/// This is more efficient than entry iteration when keys are not needed.
///
/// # Characteristics
///
/// - **Facade Pattern**: Abstracts away the underlying provider implementation
/// - **Bidirectional**: Implements `DoubleEndedIterator` for both forward and backward traversal
/// - **Thread-Safe**: Uses `Arc<Mutex<_>>` for safe concurrent access
/// - **Cloneable**: Can be cloned cheaply via Arc; clones share iteration state
/// - **Provider-Agnostic**: Works with any `ValueIteratorProvider` (NitriteMap, transactions, etc.)
pub struct ValueIterator {
    provider: Arc<parking_lot::Mutex<Box<dyn ValueIteratorProvider>>>,
}

impl ValueIterator {
    /// Creates a new value iterator wrapping the given provider.
    ///
    /// # Arguments
    ///
    /// * `provider` - Any implementation of `ValueIteratorProvider`
    ///
    /// # Returns
    ///
    /// A new `ValueIterator` ready to use with standard `Iterator` and `DoubleEndedIterator` methods.
    ///
    /// # Type Constraints
    ///
    /// The provider must:
    /// - Implement `ValueIteratorProvider`
    /// - Be `'static` (own all captured data)
    ///
    /// # Behavior
    ///
    /// - Wraps the provider in `Arc<Mutex<_>>` for thread-safe shared access
    /// - Multiple clones of the iterator share the same provider state
    /// - Iteration position is shared across clones
    pub fn new<T: ValueIteratorProvider + 'static>(provider: T) -> Self {
        ValueIterator {
            provider: Arc::new(parking_lot::Mutex::new(Box::new(provider))),
        }
    }
}

impl Clone for ValueIterator {
    fn clone(&self) -> Self {
        ValueIterator {
            provider: Arc::clone(&self.provider),
        }
    }
}

impl Iterator for ValueIterator {
    type Item = NitriteResult<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.next_value()
    }
}

impl DoubleEndedIterator for ValueIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let mut provider = self.provider.lock();
        provider.prev_value()
    }
}

// These maintain backward compatibility during transition

/// Type alias for `EntryIterator` for backward compatibility.
///
/// Represents an iterator over (Key, Value) entries. Use `EntryIterator` directly
/// in new code; this alias is maintained for compatibility with existing code.
pub type NitriteMapEntryIterator = EntryIterator;

/// Type alias for `KeyIterator` for backward compatibility.
///
/// Represents an iterator over keys. Use `KeyIterator` directly
/// in new code; this alias is maintained for compatibility with existing code.
pub type NitriteMapKeyIterator = KeyIterator;

/// Type alias for `ValueIterator` for backward compatibility.
///
/// Represents an iterator over values. Use `ValueIterator` directly
/// in new code; this alias is maintained for compatibility with existing code.
pub type NitriteMapValueIterator = ValueIterator;

// Built-in Provider Implementations
// These implement the provider traits for single-map iteration

/// Built-in provider for iterating entries from a single NitriteMap.
///
/// # Purpose
///
/// `SingleMapEntryProvider` is the default implementation of `EntryIteratorProvider` for
/// iterating over all entries in a single NitriteMap. It uses the map's navigation methods
/// (first_key, last_key, higher_key, lower_key) to traverse entries in sorted order.
///
/// # Characteristics
///
/// - **Sorted Traversal**: Iterates entries in sorted order of their keys
/// - **Stateful**: Tracks the current position using the last visited key
/// - **Lazy Navigation**: Uses key-based navigation rather than preloading all entries
/// - **Bidirectional**: Supports both forward and backward iteration
/// - **Map-Based**: Works directly with a NitriteMap without materializing entries
pub struct SingleMapEntryProvider {
    inner_map: NitriteMap,
    current: Option<Key>,
}

impl SingleMapEntryProvider {
    /// Creates a new entry provider for a NitriteMap.
    ///
    /// # Arguments
    ///
    /// * `map` - The `NitriteMap` to iterate over
    ///
    /// # Returns
    ///
    /// A new `SingleMapEntryProvider` initialized at position before the first entry.
    ///
    /// # Behavior
    ///
    /// - Starts with `current=None`, indicating position before the first entry
    /// - First call to `next_entry()` will return the entry at `map.first_key()`
    /// - First call to `prev_entry()` will return the entry at `map.last_key()`
    /// - The map is cloned internally; changes to the original map are reflected
    pub fn new(map: NitriteMap) -> Self {
        SingleMapEntryProvider {
            inner_map: map,
            current: None,
        }
    }

    fn set_current(
        &mut self,
        map: NitriteMap,
        next_key: NitriteResult<Option<Key>>,
    ) -> Option<NitriteResult<(Key, Value)>> {
        match next_key {
            Ok(Some(key)) => {
                self.current = Some(key.clone());
                match map.get(&key) {
                    Ok(Some(value)) => Some(Ok((key, value))),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn higher_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.higher_key(current_key),
            None => map.first_key(),
        }
    }

    fn lower_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.lower_key(current_key),
            None => map.last_key(),
        }
    }
}

impl EntryIteratorProvider for SingleMapEntryProvider {
    fn next_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
        let map = self.inner_map.clone();
        let next_key = self.higher_key(map.clone());
        self.set_current(map, next_key)
    }

    fn prev_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
        let map = self.inner_map.clone();
        let next_key = self.lower_key(map.clone());
        self.set_current(map, next_key)
    }
}

/// Built-in provider for iterating keys from a single NitriteMap.
///
/// # Purpose
///
/// `SingleMapKeyProvider` is the default implementation of `KeyIteratorProvider` for
/// iterating over all keys in a single NitriteMap. It uses the map's navigation methods
/// to traverse keys in sorted order without retrieving values.
///
/// # Characteristics
///
/// - **Sorted Traversal**: Iterates keys in sorted order
/// - **Stateful**: Tracks the current position using the last visited key
/// - **Lazy Navigation**: Uses key-based navigation rather than preloading all keys
/// - **Bidirectional**: Supports both forward and backward iteration
/// - **Efficient**: Only accesses keys, avoiding value retrieval overhead
pub struct SingleMapKeyProvider {
    inner_map: NitriteMap,
    current: Option<Key>,
}

impl SingleMapKeyProvider {
    /// Creates a new key provider for a NitriteMap.
    ///
    /// # Arguments
    ///
    /// * `map` - The `NitriteMap` to iterate over
    ///
    /// # Returns
    ///
    /// A new `SingleMapKeyProvider` initialized at position before the first key.
    ///
    /// # Behavior
    ///
    /// - Starts with `current=None`, indicating position before the first key
    /// - First call to `next_key()` will return the key at `map.first_key()`
    /// - First call to `prev_key()` will return the key at `map.last_key()`
    /// - The map is cloned internally; changes to the original map are reflected
    pub fn new(map: NitriteMap) -> Self {
        SingleMapKeyProvider {
            inner_map: map,
            current: None,
        }
    }

    fn set_current(&mut self, next_key: NitriteResult<Option<Key>>) -> Option<NitriteResult<Key>> {
        match next_key {
            Ok(Some(key)) => {
                self.current = Some(key.clone());
                Some(Ok(key))
            }
            Ok(None) => {
                self.current = None;
                None
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn higher_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.higher_key(current_key),
            None => map.first_key(),
        }
    }

    fn lower_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.lower_key(current_key),
            None => map.last_key(),
        }
    }
}

impl KeyIteratorProvider for SingleMapKeyProvider {
    fn next_key(&mut self) -> Option<NitriteResult<Key>> {
        let map = self.inner_map.clone();
        let next_key = self.higher_key(map.clone());
        self.set_current(next_key)
    }

    fn prev_key(&mut self) -> Option<NitriteResult<Key>> {
        let map = self.inner_map.clone();
        let next_key = self.lower_key(map.clone());
        self.set_current(next_key)
    }
}

/// Built-in provider for iterating values from a single NitriteMap.
///
/// # Purpose
///
/// `SingleMapValueProvider` is the default implementation of `ValueIteratorProvider` for
/// iterating over all values in a single NitriteMap. It uses the map's navigation methods
/// to traverse values in sorted order of their keys (without exposing keys).
///
/// # Characteristics
///
/// - **Sorted Traversal**: Iterates values in sorted order by their keys
/// - **Stateful**: Tracks the current position using the last visited key
/// - **Lazy Navigation**: Uses key-based navigation rather than preloading all values
/// - **Bidirectional**: Supports both forward and backward iteration
/// - **Efficient**: Only accesses values, but requires key lookups for positioning
pub struct SingleMapValueProvider {
    inner_map: NitriteMap,
    current: Option<Key>,
}

impl SingleMapValueProvider {
    /// Creates a new value provider for a NitriteMap.
    ///
    /// # Arguments
    ///
    /// * `map` - The `NitriteMap` to iterate over
    ///
    /// # Returns
    ///
    /// A new `SingleMapValueProvider` initialized at position before the first value.
    ///
    /// # Behavior
    ///
    /// - Starts with `current=None`, indicating position before the first value
    /// - First call to `next_value()` will return the value at `map.first_key()`
    /// - First call to `prev_value()` will return the value at `map.last_key()`
    /// - The map is cloned internally; changes to the original map are reflected
    pub fn new(map: NitriteMap) -> Self {
        SingleMapValueProvider {
            inner_map: map,
            current: None,
        }
    }

    fn set_current(
        &mut self,
        map: NitriteMap,
        next_key: NitriteResult<Option<Key>>,
    ) -> Option<NitriteResult<Value>> {
        match next_key {
            Ok(Some(key)) => {
                self.current = Some(key.clone());
                match map.get(&key) {
                    Ok(Some(value)) => Some(Ok(value)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn higher_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.higher_key(current_key),
            None => map.first_key(),
        }
    }

    fn lower_key(&self, map: NitriteMap) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => map.lower_key(current_key),
            None => map.last_key(),
        }
    }
}

impl ValueIteratorProvider for SingleMapValueProvider {
    fn next_value(&mut self) -> Option<NitriteResult<Value>> {
        let map = self.inner_map.clone();
        let next_key = self.higher_key(map.clone());
        self.set_current(map, next_key)
    }

    fn prev_value(&mut self) -> Option<NitriteResult<Value>> {
        let map = self.inner_map.clone();
        let next_key = self.lower_key(map.clone());
        self.set_current(map, next_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Key, Value};
    use crate::nitrite_config::NitriteConfig;

    fn create_test_map() -> NitriteMap {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        let store = config.nitrite_store().unwrap();
        let map = store.open_map("test_map").unwrap();
        map.put(Key::from("key1"), Value::from("value1")).unwrap();
        map.put(Key::from("key2"), Value::from("value2")).unwrap();
        map.put(Key::from("key3"), Value::from("value3")).unwrap();
        map
    }

    #[test]
    fn test_entry_iterator_forward() {
        let map = create_test_map();
        let provider = SingleMapEntryProvider::new(map);
        let mut iter = EntryIterator::new(provider);

        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(key, Key::from("key1"));
        assert_eq!(value, Value::from("value1"));

        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(key, Key::from("key2"));
        assert_eq!(value, Value::from("value2"));

        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(key, Key::from("key3"));
        assert_eq!(value, Value::from("value3"));

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_entry_iterator_backward() {
        let map = create_test_map();
        let provider = SingleMapEntryProvider::new(map);
        let mut iter = EntryIterator::new(provider);

        let (key, value) = iter.next_back().unwrap().unwrap();
        assert_eq!(key, Key::from("key3"));
        assert_eq!(value, Value::from("value3"));

        let (key, value) = iter.next_back().unwrap().unwrap();
        assert_eq!(key, Key::from("key2"));
        assert_eq!(value, Value::from("value2"));

        let (key, value) = iter.next_back().unwrap().unwrap();
        assert_eq!(key, Key::from("key1"));
        assert_eq!(value, Value::from("value1"));

        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_key_iterator_forward() {
        let map = create_test_map();
        let provider = SingleMapKeyProvider::new(map);
        let mut iter = KeyIterator::new(provider);

        assert_eq!(iter.next().unwrap().unwrap(), Key::from("key1"));
        assert_eq!(iter.next().unwrap().unwrap(), Key::from("key2"));
        assert_eq!(iter.next().unwrap().unwrap(), Key::from("key3"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_key_iterator_backward() {
        let map = create_test_map();
        let provider = SingleMapKeyProvider::new(map);
        let mut iter = KeyIterator::new(provider);

        assert_eq!(iter.next_back().unwrap().unwrap(), Key::from("key3"));
        assert_eq!(iter.next_back().unwrap().unwrap(), Key::from("key2"));
        assert_eq!(iter.next_back().unwrap().unwrap(), Key::from("key1"));
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_value_iterator_forward() {
        let map = create_test_map();
        let provider = SingleMapValueProvider::new(map);
        let mut iter = ValueIterator::new(provider);

        assert_eq!(iter.next().unwrap().unwrap(), Value::from("value1"));
        assert_eq!(iter.next().unwrap().unwrap(), Value::from("value2"));
        assert_eq!(iter.next().unwrap().unwrap(), Value::from("value3"));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_value_iterator_backward() {
        let map = create_test_map();
        let provider = SingleMapValueProvider::new(map);
        let mut iter = ValueIterator::new(provider);

        assert_eq!(iter.next_back().unwrap().unwrap(), Value::from("value3"));
        assert_eq!(iter.next_back().unwrap().unwrap(), Value::from("value2"));
        assert_eq!(iter.next_back().unwrap().unwrap(), Value::from("value1"));
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_entry_iterator_bidirectional() {
        let map = create_test_map();
        let provider = SingleMapEntryProvider::new(map.clone());
        let mut iter = EntryIterator::new(provider);

        // Test forward iteration
        let (key1, _) = iter.next().unwrap().unwrap();
        assert_eq!(key1, Key::from("key1"));

        let (key2, _) = iter.next().unwrap().unwrap();
        assert_eq!(key2, Key::from("key2"));

        // Test backward iteration from the end
        let provider_back = SingleMapEntryProvider::new(map);
        let mut iter_back = EntryIterator::new(provider_back);

        let (key3, _) = iter_back.next_back().unwrap().unwrap();
        assert_eq!(key3, Key::from("key3"));

        let (key2_back, _) = iter_back.next_back().unwrap().unwrap();
        assert_eq!(key2_back, Key::from("key2"));

        let (key1_back, _) = iter_back.next_back().unwrap().unwrap();
        assert_eq!(key1_back, Key::from("key1"));
    }

    #[test]
    fn test_entry_iterator_cloning() {
        let map = create_test_map();
        let provider = SingleMapEntryProvider::new(map);
        let mut iter = EntryIterator::new(provider);
        let mut iter_cloned = iter.clone();

        // Both should share the same provider via Arc
        let (key1, value1) = iter.next().unwrap().unwrap();
        assert_eq!(key1, Key::from("key1"));
        assert_eq!(value1, Value::from("value1"));

        // Cloned iterator shares state due to Arc<Mutex<_>>
        let (key2, value2) = iter_cloned.next().unwrap().unwrap();
        assert_eq!(key2, Key::from("key2"));
        assert_eq!(value2, Value::from("value2"));
    }

    #[test]
    fn test_custom_provider_implementation() {
        struct SimpleEntryProvider {
            entries: Vec<(Key, Value)>,
            index: usize,
        }

        impl EntryIteratorProvider for SimpleEntryProvider {
            fn next_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
                if self.index < self.entries.len() {
                    let entry = self.entries[self.index].clone();
                    self.index += 1;
                    Some(Ok(entry))
                } else {
                    None
                }
            }

            fn prev_entry(&mut self) -> Option<NitriteResult<(Key, Value)>> {
                if self.index > 0 {
                    self.index -= 1;
                    Some(Ok(self.entries[self.index].clone()))
                } else {
                    None
                }
            }
        }

        let provider = SimpleEntryProvider {
            entries: vec![
                (Key::from("a"), Value::from("1")),
                (Key::from("b"), Value::from("2")),
            ],
            index: 0,
        };

        let mut iter = EntryIterator::new(provider);

        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(key, Key::from("a"));
        assert_eq!(value, Value::from("1"));

        let (key, value) = iter.next().unwrap().unwrap();
        assert_eq!(key, Key::from("b"));
        assert_eq!(value, Value::from("2"));

        assert!(iter.next().is_none());
    }
}
