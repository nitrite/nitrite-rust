use crate::collection::CollectionEventCallback;
use crate::errors::NitriteResult;
use crate::nitrite_config::NitriteConfig;
use anyhow::Error;
use basu::error::BasuError;
use basu::event::Event;
use basu::Handle;
use std::fmt::Debug;
use std::sync::Arc;

/// Enumeration of lifecycle events that occur at the store level.
///
/// # Purpose
///
/// `StoreEvents` represents important lifecycle state transitions in a Nitrite database store.
/// These events allow applications to react to store-level operations like opening, committing data,
/// or closing the store.
///
/// # Variants
///
/// - **Open**: Fired when the store is successfully opened or created
/// - **Commit**: Fired when a transaction is committed to persistent storage
/// - **Closing**: Fired when the store is about to be closed (before close completes)
/// - **Closed**: Fired after the store has been fully closed
///
/// # Characteristics
///
/// - **Debug**: Can be formatted for logging
/// - **PartialEq**: Can be compared for equality
/// - **Clone**: Can be cloned cheaply (enum values are small)
/// - **Send + Sync**: Safe for concurrent access and thread-safe event handling
///
/// # Usage
///
/// Events are passed to registered `StoreEventListener` instances to enable applications
/// to respond to database lifecycle changes:
/// ```ignore
/// let listener = StoreEventListener::new(|info| {
///     match info.event() {
///         StoreEvents::Open => println!("Database opened"),
///         StoreEvents::Commit => println!("Data committed"),
///         StoreEvents::Closing => println!("Database closing"),
///         StoreEvents::Closed => println!("Database closed"),
///     }
///     Ok(())
/// });
/// store.subscribe(listener)?;
/// ```
#[derive(Debug, PartialEq, Clone)]
pub enum StoreEvents {
    Open,
    Commit,
    Closing,
    Closed,
}

/// Context information provided with each store event.
///
/// # Purpose
///
/// `StoreEventInfo` bundles the event type with the database configuration at the time the event
/// occurred. This allows event handlers to access both what event happened and the current state
/// of the database.
///
/// # Characteristics
///
/// - **Cloneable**: Can be shared and cloned efficiently
/// - **Debug**: Can be formatted for logging and debugging
/// - **Thread-Safe**: Safe to pass to event handlers in parallel processing
///
/// # Fields
///
/// - **event**: The store event that occurred (`StoreEvents` enum value)
/// - **nitrite_config**: The database configuration at the time of the event (internal only via `nitrite_config()`)
///
/// # Examples
///
/// From nitrite source:
/// ```ignore
/// let listener = StoreEventListener::new(|info| {
///     let event = info.event();
///     match event {
///         StoreEvents::Open => {
///             println!("Store opened!");
///             // Access configuration if needed (internal API)
///         },
///         _ => {}
///     }
///     Ok(())
/// });
/// ```
#[derive(Clone)]
pub struct StoreEventInfo {
    event: StoreEvents,
    nitrite_config: NitriteConfig,
}

impl StoreEventInfo {
    /// Creates a new store event context with the given event and configuration.
    ///
    /// # Arguments
    ///
    /// * `event` - The store event that occurred
    /// * `nitrite_config` - The database configuration at the time of the event
    ///
    /// # Returns
    ///
    /// A new `StoreEventInfo` instance bundling the event and configuration.
    ///
    /// # Behavior
    ///
    /// - Stores both the event type and configuration for access by event handlers
    /// - The configuration can be retrieved via `nitrite_config()` (internal API)
    /// - Cheap to clone due to Arc-based configuration
    ///
    /// # Examples
    ///
    /// From nitrite source:
    /// ```ignore
    /// let config = NitriteConfig::default();
    /// let info = StoreEventInfo::new(StoreEvents::Open, config);
    /// ```
    pub fn new(event: StoreEvents, nitrite_config: NitriteConfig) -> Self {
        StoreEventInfo {
            event,
            nitrite_config,
        }
    }

    /// Returns a clone of the event that occurred.
    ///
    /// # Returns
    ///
    /// A `StoreEvents` enum value indicating which event occurred.
    ///
    /// # Examples
    ///
    /// From nitrite tests:
    /// ```ignore
    /// let info = StoreEventInfo::new(StoreEvents::Commit, config);
    /// assert_eq!(info.event(), StoreEvents::Commit);
    /// ```
    pub fn event(&self) -> StoreEvents {
        self.event.clone()
    }
    
    /// Returns a reference to the database configuration (internal API).
    ///
    /// # Returns
    ///
    /// A reference to the `NitriteConfig` at the time of the event.
    ///
    /// # Behavior
    ///
    /// - This is an internal API; not typically used by application code
    /// - Provides access to database configuration if needed by event handlers
    pub(crate) fn nitrite_config(&self) -> &NitriteConfig {
        &self.nitrite_config
    }
}

impl Debug for StoreEventInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreEventInfo")
            .field("event", &self.event)
            .finish()
    }
}

/// A trait for closures that handle store events.
///
/// # Purpose
///
/// `StoreEventCallback` defines the interface for any callable that processes store events.
/// It requires the closure to be `Send + Sync` for safe usage in a parallel event processing
/// context, and to return `NitriteResult<()>` to indicate success or error.
///
/// # Characteristics
///
/// - **Callable**: Implements `Fn(StoreEventInfo) -> NitriteResult<()>`
/// - **Thread-Safe**: Requires `Send + Sync` bounds
/// - **Zero-Cost Abstraction**: Automatically implemented for functions and closures
/// - **Async-Friendly**: Processed in Rayon's parallel thread pool
///
/// # Implementations
///
/// Automatically implemented for any function or closure that:
/// - Takes a `StoreEventInfo` parameter
/// - Returns `NitriteResult<()>`
/// - Is `Send + Sync` (safe to share across threads)
///
/// # Examples
///
/// From nitrite source:
/// ```ignore
/// // Simple closure handler
/// StoreEventListener::new(|info| {
///     println!("Event: {:?}", info.event());
///     Ok(())
/// });
///
/// // Closure capturing variables
/// let counter = Arc::new(AtomicUsize::new(0));
/// let counter_clone = counter.clone();
/// StoreEventListener::new(move |_| {
///     counter_clone.fetch_add(1, Ordering::Relaxed);
///     Ok(())
/// });
/// ```
pub trait StoreEventCallback: Send + Sync + Fn(StoreEventInfo) -> NitriteResult<()> {}

impl<F> StoreEventCallback for F
where
    F: Send + Sync + Fn(StoreEventInfo) -> NitriteResult<()>,
{
}

/// A listener for store-level events that wraps a callback function.
///
/// # Purpose
///
/// `StoreEventListener` packages a closure/function into a reusable listener that can be
/// registered with a store to receive notifications when database lifecycle events occur.
/// The listener is thread-safe and can be cloned for concurrent sharing.
///
/// # Characteristics
///
/// - **Callback-Based**: Wraps any `StoreEventCallback` implementation (closure or function)
/// - **Thread-Safe**: Uses `Arc<dyn StoreEventCallback>` for safe concurrent access
/// - **Cloneable**: Cloning is cheap (only increments Arc reference count)
/// - **Async-Processing**: Events are handled in Rayon's parallel thread pool
/// - **Handle Trait**: Implements the `Handle<StoreEventInfo>` interface for event processing
///
/// # Relationship to Related Types
///
/// - `StoreEventCallback`: The trait that event handlers implement
/// - `StoreEventInfo`: The event context passed to handlers
/// - `StoreEvents`: The enum of actual event types
///
/// # Examples
///
/// From nitrite source and tests:
/// ```ignore
/// // Create a listener with a simple closure
/// let listener = StoreEventListener::new(|info| {
///     match info.event() {
///         StoreEvents::Open => println!("Database opened"),
///         StoreEvents::Commit => println!("Transaction committed"),
///         StoreEvents::Closing => println!("Database closing"),
///         StoreEvents::Closed => println!("Database closed"),
///     }
///     Ok(())
/// });
///
/// // Register with store
/// store.subscribe(listener)?;
///
/// // Listener can be cloned cheaply for sharing
/// let listener2 = listener.clone();
/// ```
#[derive(Clone)]
pub struct StoreEventListener {
    on_event: Arc<dyn StoreEventCallback>,
}

impl StoreEventListener {
    /// Creates a new store event listener with the given callback.
    ///
    /// # Arguments
    ///
    /// * `on_event` - A closure or function implementing `StoreEventCallback` that handles events
    ///
    /// # Returns
    ///
    /// A new `StoreEventListener` ready to be registered with a store.
    ///
    /// # Behavior
    ///
    /// - Wraps the callback in an `Arc` for thread-safe sharing
    /// - The callback will be invoked for each event when the listener is registered
    /// - Callbacks are processed in Rayon's parallel thread pool
    /// - Multiple listeners can be registered with the same store
    /// - The listener can be cloned cheaply and registered with multiple stores
    ///
    /// # Type Constraints
    ///
    /// The `on_event` parameter must:
    /// - Take a single `StoreEventInfo` parameter
    /// - Return `NitriteResult<()>`
    /// - Be `Send + Sync` for safe concurrent execution
    /// - Be `'static` (own all captured data)
    ///
    /// # Examples
    ///
    /// From nitrite source and tests:
    /// ```ignore
    /// // Simple listener
    /// let listener = StoreEventListener::new(|_| Ok(()));
    /// store.subscribe(listener)?;
    ///
    /// // Listener with pattern matching
    /// let listener = StoreEventListener::new(|info| {
    ///     match info.event() {
    ///         StoreEvents::Open => println!("Database opened"),
    ///         _ => {}
    ///     }
    ///     Ok(())
    /// });
    ///
    /// // Listener capturing variables
    /// let counter = Arc::new(AtomicUsize::new(0));
    /// let counter_clone = counter.clone();
    /// let listener = StoreEventListener::new(move |_| {
    ///     counter_clone.fetch_add(1, Ordering::Relaxed);
    ///     Ok(())
    /// });
    /// ```
    pub fn new(on_event: impl StoreEventCallback + 'static) -> Self {
        StoreEventListener {
            on_event: Arc::new(on_event),
        }
    }
}

impl Handle<StoreEventInfo> for StoreEventListener {
    fn handle(&self, event: &Event<StoreEventInfo>) -> Result<(), BasuError> {
        // below code will run in rayon's thread pool using parallel iterator
        match (self.on_event)(event.data.clone()) {
            Ok(_) => Ok(()),
            Err(e) => Err(BasuError::HandlerError(Error::from(e))),
        }
    }
}

impl Debug for StoreEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StoreEventListener")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::nitrite_config::NitriteConfig;
    use basu::event::Event;
    use std::sync::Arc;

    #[test]
    fn test_store_event_listener_new() {
        let listener = StoreEventListener::new(|_| Ok(()));

        assert!(Arc::strong_count(&listener.on_event) > 0);
    }

    #[test]
    fn test_store_event_listener_handle_success() {
        let listener = StoreEventListener::new(|_| Ok(()));

        let nitrite_config = NitriteConfig::default();
        let store_event_info = StoreEventInfo::new(StoreEvents::Open, nitrite_config);
        let event = Event::new(store_event_info);

        assert!(listener.handle(&event).is_ok());
    }

    #[test]
    fn test_store_event_listener_handle_failure() {
        let listener = StoreEventListener::new(|_| {
            Err(NitriteError::new("Test error", ErrorKind::InvalidOperation))
        });

        let nitrite_config = NitriteConfig::default();
        let store_event_info = StoreEventInfo::new(StoreEvents::Open, nitrite_config);
        let event = Event::new(store_event_info);

        assert!(listener.handle(&event).is_err());
    }

    #[test]
    fn test_store_event_info_new() {
        let nitrite_config = NitriteConfig::default();
        let store_event_info = StoreEventInfo::new(StoreEvents::Commit, nitrite_config.clone());

        assert_eq!(store_event_info.event, StoreEvents::Commit);
    }

    #[test]
    fn test_store_event_info_debug() {
        let nitrite_config = NitriteConfig::default();
        let store_event_info = StoreEventInfo::new(StoreEvents::Closing, nitrite_config);

        let debug_str = format!("{:?}", store_event_info);
        assert!(debug_str.contains("StoreEventInfo"));
    }

    #[test]
    fn test_store_event_listener_debug() {
        let listener = StoreEventListener::new(|_| Ok(()));

        let debug_str = format!("{:?}", listener);
        assert!(debug_str.contains("StoreEventListener"));
    }

    #[test]
    fn test_store_event_listener_clone_efficiency() {
        // Test that listener cloning is efficient with Arc
        let listener = StoreEventListener::new(|_| Ok(()));
        let initial_count = Arc::strong_count(&listener.on_event);
        
        let listener2 = listener.clone();
        let new_count = Arc::strong_count(&listener2.on_event);
        
        // Clone should increment Arc count, not copy the callback
        assert_eq!(new_count, initial_count + 1);
    }

    #[test]
    fn test_store_event_info_clone_efficiency() {
        // Test that event info cloning with config is efficient
        let config1 = NitriteConfig::default();
        let info1 = StoreEventInfo::new(StoreEvents::Commit, config1);
        
        let info2 = info1.clone();
        assert_eq!(info2.event, StoreEvents::Commit);
    }

    #[test]
    fn test_multiple_listeners_efficiency() {
        // Test that multiple listeners can be created efficiently
        let listeners: Vec<_> = (0..10)
            .map(|_| StoreEventListener::new(|_| Ok(())))
            .collect();
        
        assert_eq!(listeners.len(), 10);
    }

    #[test]
    fn test_store_event_info_events_immutable() {
        // Test that StoreEventInfo events are immutable and efficiently accessed
        let config = NitriteConfig::default();
        let events = vec![
            StoreEvents::Open,
            StoreEvents::Commit,
            StoreEvents::Closing,
            StoreEvents::Closed,
        ];
        
        for event in events {
            let info = StoreEventInfo::new(event.clone(), config.clone());
            assert_eq!(info.event(), event);
        }
    }

    #[test]
    fn test_handle_with_multiple_events() {
        // Test handling multiple events efficiently
        let listener = StoreEventListener::new(|_| Ok(()));
        
        let events = vec![
            StoreEvents::Open,
            StoreEvents::Commit,
            StoreEvents::Closing,
        ];
        
        let config = NitriteConfig::default();
        for event in events {
            let info = StoreEventInfo::new(event, config.clone());
            let event_wrapper = Event::new(info);
            assert!(listener.handle(&event_wrapper).is_ok());
        }
    }

    #[test]
    fn test_listener_callback_capture_efficiency() {
        // Test that listener callback is efficiently captured in Arc
        let counter = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter_clone = counter.clone();
        
        let listener = StoreEventListener::new(move |_| {
            counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        });
        
        let config = NitriteConfig::default();
        let info = StoreEventInfo::new(StoreEvents::Open, config);
        let event = Event::new(info);
        
        listener.handle(&event).unwrap();
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
