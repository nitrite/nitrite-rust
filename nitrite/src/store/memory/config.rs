use crate::common::{ReadExecutor, WriteExecutor};
use crate::{atomic, store::{StoreConfigProvider, StoreEventListener}, Atomic};
use std::any::Any;
use std::sync::Arc;

/// Configuration for an in-memory store.
///
/// # Purpose
/// `InMemoryStoreConfig` provides configuration and event listener management
/// for an in-memory store implementation. It allows registering listeners that
/// will be notified of store events such as open, close, and commit operations.
///
/// # Characteristics
/// - **Thread-Safe**: Can be safely cloned and shared across threads
/// - **Event-Driven**: Supports registering event listeners for store operations
/// - **Lightweight Cloning**: Uses Arc internally for efficient sharing
/// - **Read-Write Access**: Provides atomic read-write operations for listener management
///
/// # Usage
/// Typically created and passed to `InMemoryStore::new()` to configure store behavior:
/// ```text
/// let config = InMemoryStoreConfig::new();
/// let store = InMemoryStore::new(config);
/// ```
#[derive(Default, Clone)]
pub struct InMemoryStoreConfig {
    inner: Arc<InMemoryStoreConfigInner>,
}

impl InMemoryStoreConfig {
    /// Creates a new `InMemoryStoreConfig` with default settings.
    ///
    /// # Returns
    /// A new `InMemoryStoreConfig` instance with no registered listeners
    pub fn new() -> InMemoryStoreConfig {
        InMemoryStoreConfig {
            inner: Arc::new(InMemoryStoreConfigInner::new()),
        }
    }

    /// Retrieves all registered event listeners.
    ///
    /// # Returns
    /// A vector containing all registered `StoreEventListener` instances.
    /// Returns an empty vector if no listeners are registered.
    pub fn event_listeners(&self) -> Vec<StoreEventListener> {
        self.inner.event_listeners()
    }
}

impl StoreConfigProvider for InMemoryStoreConfig {
    /// Returns the file path for the store.
    ///
    /// For in-memory stores, this is always an empty string as there is no persistent storage.
    ///
    /// # Returns
    /// An empty string
    fn file_path(&self) -> String {
        "".to_string()
    }

    /// Checks if the store is in read-only mode.
    ///
    /// In-memory stores are never read-only.
    ///
    /// # Returns
    /// Always `false` for in-memory stores
    fn is_read_only(&self) -> bool {
        false
    }

    /// Registers an event listener with the store configuration.
    ///
    /// # Arguments
    /// * `listener` - The event listener to register
    fn add_store_listener(&self, listener: StoreEventListener) {
        self.inner.add_store_listener(listener)
    }

    /// The function `as_any` returns a reference to the trait object `dyn Any`.
    /// 
    /// Returns:
    /// 
    /// The method `as_any` is returning a reference to the object itself as a trait object of type `dyn
    /// Any`.
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Default)]
struct InMemoryStoreConfigInner {
    event_listeners: Atomic<Vec<StoreEventListener>>,
}

impl InMemoryStoreConfigInner {
    fn new() -> InMemoryStoreConfigInner {
        InMemoryStoreConfigInner {
            event_listeners: atomic(Vec::new()),
        }
    }

    fn add_store_listener(&self, listener: StoreEventListener) {
        self.event_listeners.write_with(|it| it.push(listener))
    }

    fn event_listeners(&self) -> Vec<StoreEventListener> {
        self.event_listeners.read_with(|listeners| {
            let mut result = Vec::with_capacity(listeners.len());
            result.extend(listeners.iter().cloned());
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreEventListener;

    #[test]
    fn test_in_memory_store_config_new() {
        let config = InMemoryStoreConfig::new();
        assert!(Arc::strong_count(&config.inner) > 0);
    }

    #[test]
    fn test_in_memory_store_config_event_listeners() {
        let config = InMemoryStoreConfig::new();
        assert!(config.event_listeners().is_empty());

        let listener = StoreEventListener::new(|_| {Ok(())});
        config.add_store_listener(listener.clone());
        assert_eq!(config.event_listeners().len(), 1);
    }

    #[test]
    fn test_in_memory_store_config_file_path() {
        let config = InMemoryStoreConfig::new();
        assert_eq!(config.file_path(), "");
    }

    #[test]
    fn test_in_memory_store_config_is_read_only() {
        let config = InMemoryStoreConfig::new();
        assert!(!config.is_read_only());
    }

    #[test]
    fn test_in_memory_store_config_add_store_listener() {
        let config = InMemoryStoreConfig::new();
        let listener = StoreEventListener::new(|_| {Ok(())});
        config.add_store_listener(listener.clone());
        assert_eq!(config.event_listeners().len(), 1);
    }

    #[test]
    fn test_in_memory_store_config_inner_new() {
        let inner = InMemoryStoreConfigInner::new();
        assert!(inner.event_listeners().is_empty());
    }

    #[test]
    fn test_in_memory_store_config_inner_add_store_listener() {
        let inner = InMemoryStoreConfigInner::new();
        let listener = StoreEventListener::new(|_| {Ok(())});
        inner.add_store_listener(listener.clone());
        assert_eq!(inner.event_listeners().len(), 1);
    }

    #[test]
    fn test_in_memory_store_config_inner_event_listeners() {
        let inner = InMemoryStoreConfigInner::new();
        assert!(inner.event_listeners().is_empty());

        let listener = StoreEventListener::new(|_| {Ok(())});
        inner.add_store_listener(listener.clone());
        assert_eq!(inner.event_listeners().len(), 1);
    }

    #[test]
    fn test_event_listeners_pre_allocation_efficiency() {
        // Test that event_listeners() pre-allocates with capacity to avoid reallocation
        let inner = InMemoryStoreConfigInner::new();
        
        // Add multiple listeners
        for _ in 0..10 {
            let listener = StoreEventListener::new(|_| {Ok(())});
            inner.add_store_listener(listener);
        }
        
        // Retrieve all listeners - should use pre-allocated Vec
        let listeners = inner.event_listeners();
        assert_eq!(listeners.len(), 10);
        
        // Verify capacity is correct (no unnecessary over-allocation)
        assert_eq!(listeners.capacity(), 10);
    }

    #[test]
    fn test_event_listeners_empty_vector_capacity() {
        // Test that empty event listeners returns Vec with 0 capacity
        let inner = InMemoryStoreConfigInner::new();
        let listeners = inner.event_listeners();
        assert!(listeners.is_empty());
        assert_eq!(listeners.capacity(), 0);
    }

    #[test]
    fn test_event_listeners_single_listener_capacity() {
        // Test single listener is pre-allocated correctly
        let inner = InMemoryStoreConfigInner::new();
        let listener = StoreEventListener::new(|_| {Ok(())});
        inner.add_store_listener(listener);
        
        let listeners = inner.event_listeners();
        assert_eq!(listeners.len(), 1);
        assert_eq!(listeners.capacity(), 1);
    }

    #[test]
    fn test_event_listeners_large_batch_efficiency() {
        // Test efficient handling of large listener batches
        let inner = InMemoryStoreConfigInner::new();
        
        // Add 100 listeners
        for _ in 0..100 {
            let listener = StoreEventListener::new(|_| {Ok(())});
            inner.add_store_listener(listener);
        }
        
        let listeners = inner.event_listeners();
        assert_eq!(listeners.len(), 100);
        
        // Verify no over-allocation occurs
        assert_eq!(listeners.capacity(), 100);
    }
}