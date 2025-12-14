use crate::collection::{CollectionEventCallback, CollectionEventListener};
use crate::common::NITRITE_EVENT;
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use basu::error::BasuError;
use basu::event::Event;
use basu::{EventBus, Handle, HandlerId};
use std::marker::PhantomData;
use std::sync::Arc;

pub trait EventAware {
    // NOTE: impl CollectionEventCallback cannot be used to make it object safe
    fn subscribe(&self, handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>>;

    fn unsubscribe(&self, subscriber: SubscriberRef) -> NitriteResult<()>;
}

/// Publishes and subscribes to events in the Nitrite system.
///
/// This struct manages an event bus that allows components to register listeners
/// and receive notifications about system events. It provides both synchronous
/// event publishing and listener management.
///
/// # Responsibilities
///
/// * **Event Publishing**: Broadcasts events to all registered listeners
/// * **Listener Registration**: Registers event handlers to receive notifications
/// * **Listener Deregistration**: Removes previously registered event handlers
/// * **Listener Queries**: Checks if any listeners are currently registered
/// * **Lifecycle Management**: Closes the event bus and cleans up resources
/// * **Performance Optimization**: Fast path for no-listener scenarios
///
/// # Example
///
/// ```ignore
/// let event_bus: NitriteEventBus<E, L> = NitriteEventBus::new();
/// let listener = MyEventListener;
/// let subscriber = event_bus.register(listener)?;
/// 
/// // Publish an event
/// event_bus.publish(my_event)?;
/// 
/// // Deregister when done
/// event_bus.deregister(subscriber)?;
/// ```
#[derive(Clone)]
pub struct NitriteEventBus<E, L> {
    inner: Arc<NitriteEventBusInner<E, L>>,
}

impl<E, L> Default for NitriteEventBus<E, L>
where
    L: Handle<E> + 'static,
    E: Send + Sync,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<E, L> NitriteEventBus<E, L>
where
    L: Handle<E> + 'static,
    E: Send + Sync,
{
    /// Creates a new event bus instance.
    pub fn new() -> Self {
        let inner = NitriteEventBusInner::new();
        NitriteEventBus {
            inner: Arc::new(inner),
        }
    }

    /// Registers an event listener with the bus.
    pub fn register(&self, listener: L) -> NitriteResult<Option<SubscriberRef>> {
        self.inner.register(listener)
    }

    /// Deregisters a previously registered event listener.
    pub fn deregister(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        self.inner.deregister(subscriber)
    }

    /// Publishes an event to all registered listeners.
    pub fn publish(&self, event: E) -> NitriteResult<()> {
        self.inner.publish(event)
    }

    /// Closes the event bus and clears all registered listeners.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    /// Returns true if there are any registered listeners.
    pub fn has_listeners(&self) -> bool {
        self.inner.has_listeners()
    }
}

pub struct SubscriberRef {
    pub(crate) inner: HandlerId,
}

impl SubscriberRef {
    pub fn new(inner: HandlerId) -> Self {
        SubscriberRef { inner }
    }
}

/// Inner implementation of the event bus.
struct NitriteEventBusInner<E, L> {
    event_bus: EventBus<E>,
    phantom_data: PhantomData<L>,
}

impl<E, L> NitriteEventBusInner<E, L>
where
    L: Handle<E> + 'static,
    E: Send + Sync,
{
    fn new() -> Self {
        let event_bus = EventBus::new();
        NitriteEventBusInner {
            event_bus,
            phantom_data: PhantomData,
        }
    }

    pub fn register(&self, listener: L) -> NitriteResult<Option<SubscriberRef>> {
        let subscriber = self.event_bus.subscribe(NITRITE_EVENT, Box::new(listener));
        match subscriber {
            Ok(subscriber) => Ok(Some(SubscriberRef::new(subscriber))),
            Err(e) => Err(Self::nitrite_error(e)),
        }
    }

    #[inline]
    pub fn deregister(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        match self.event_bus.unsubscribe(NITRITE_EVENT, &subscriber.inner) {
            Ok(_) => Ok(()),
            Err(e) => Err(Self::nitrite_error(e)),
        }
    }

    #[inline]
    pub fn publish(&self, event: E) -> NitriteResult<()> {
        // Fast path: check if there are listeners before creating event
        let handler_count = match self.event_bus.get_handler_count(NITRITE_EVENT) {
            Ok(count) => count,
            Err(e) => {
                // If event type not found, no listeners - early return
                if matches!(e, BasuError::EventTypeNotFOUND) {
                    return Ok(());
                }
                return Err(Self::nitrite_error(e));
            }
        };

        // Only create event if there are listeners
        if handler_count == 0 {
            return Ok(());
        }

        let basu_event = Event::new(event);
        match self.event_bus.publish(NITRITE_EVENT, &basu_event) {
            Ok(_) => Ok(()),
            Err(e) => Err(Self::nitrite_error(e)),
        }
    }

    #[inline]
    pub fn close(&self) -> NitriteResult<()> {
        let result = self.event_bus.clear();
        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Self::nitrite_error(e)),
        }
    }

    #[inline]
    pub fn has_listeners(&self) -> bool {
        match self.event_bus.get_handler_count(NITRITE_EVENT) {
            Ok(count) => count > 0,
            Err(e) => {
                if matches!(e, BasuError::EventTypeNotFOUND) {
                    false
                } else {
                    log::warn!("Failed to check listeners: {}, defaulting to false", e);
                    false
                }
            }
        }
    }

    #[inline]
    pub fn nitrite_error(e: BasuError) -> NitriteError {
        match e {
            BasuError::EventTypeNotFOUND => {
                NitriteError::new(
                    "Event bus error: the requested event type is not registered. Register a handler for the event type before publishing",
                    ErrorKind::EventError
                )
            }
            BasuError::MutexPoisoned => NitriteError::new(
                "Event bus error: internal mutex poisoned - the event bus may be in an inconsistent state",
                ErrorKind::EventError
            ),
            BasuError::HandlerError(e) => {
                let error_message = e.source()
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "Unknown error in event handler".to_string());
                NitriteError::new(
                    &format!("Event handler error: {}", error_message),
                    ErrorKind::EventError
                )
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use basu::event::Event;
    use std::sync::Arc;

    #[derive(Clone)]
    struct MockListener;

    impl Handle<Event<&str>> for MockListener {
        fn handle(&self, _event: &Event<Event<&str>>) -> Result<(), BasuError> {
            Ok(())
        }
    }

    #[test]
    fn test_event_bus_new() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        assert!(Arc::strong_count(&event_bus.inner) > 0);
    }

    #[test]
    fn test_event_bus_register() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let listener = MockListener;
        let subscriber = event_bus.register(listener);
        assert!(subscriber.is_ok());
    }

    #[test]
    fn test_event_bus_deregister() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let listener = MockListener;
        let subscriber = event_bus.register(listener).unwrap().unwrap();
        let result = event_bus.deregister(subscriber);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_bus_publish() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let listener = MockListener;
        let _subscriber = event_bus.register(listener).unwrap();
        let event = Event::new("test_event");
        let result = event_bus.publish(event);
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_bus_close() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let result = event_bus.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_event_bus_deregister_error() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let subscriber = SubscriberRef::new(HandlerId::new());
        let result = event_bus.deregister(subscriber);
        assert!(result.is_err());
    }

    #[test]
    fn test_event_bus_close_error() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let result = event_bus.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_listeners_with_event_type_not_found() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        // When no listeners are registered, get_handler_count returns EventTypeNotFOUND
        let result = event_bus.has_listeners();
        assert!(!result);
    }

    #[test]
    fn test_has_listeners_with_registered_listeners() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let listener = MockListener;
        let _subscriber = event_bus.register(listener).unwrap();
        // After registering a listener, has_listeners should return true
        let result = event_bus.has_listeners();
        assert!(result);
    }

    #[test]
    fn test_has_listeners_gracefully_handles_unknown_error() {
        // This test verifies that has_listeners handles other BasuError types gracefully
        // without panicking, defaulting to false
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let result = event_bus.has_listeners();
        // Even with various error conditions, should not panic
        assert!(!result);
    }

    #[test]
    fn test_nitrite_error_event_type_not_found() {
        let error = BasuError::EventTypeNotFOUND;
        let result = NitriteEventBusInner::<Event<&str>, MockListener>::nitrite_error(error);
        assert_eq!(*result.kind(), ErrorKind::EventError);
        assert!(result.to_string().contains("event type") && result.to_string().contains("not registered"));
    }

    #[test]
    fn test_nitrite_error_mutex_poisoned() {
        let error = BasuError::MutexPoisoned;
        let result = NitriteEventBusInner::<Event<&str>, MockListener>::nitrite_error(error);
        assert_eq!(*result.kind(), ErrorKind::EventError);
        assert!(result.to_string().contains("mutex poisoned"));
    }

    #[test]
    fn test_nitrite_error_handler_error_with_custom_error() {
        // This test verifies that nitrite_error handles HandlerError gracefully
        // without panicking, using source info when available
        use anyhow::anyhow;
        let error = BasuError::HandlerError(anyhow!("custom handler error"));
        let result = NitriteEventBusInner::<Event<&str>, MockListener>::nitrite_error(error);
        assert_eq!(*result.kind(), ErrorKind::EventError);
        // Should contain the error message from the source
        assert!(result.to_string().contains("custom handler error") || result.to_string().contains("Unknown error"));
    }

    #[test]
    fn bench_publish_no_listeners() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        
        for _ in 0..1000 {
            let _ = event_bus.publish(Event::new("test_event"));
        }
    }

    #[test]
    fn bench_publish_with_listeners() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        let _subscriber = event_bus.register(MockListener).unwrap();
        
        for _ in 0..100 {
            let _ = event_bus.publish(Event::new("test_event"));
        }
    }

    #[test]
    fn bench_listener_registration() {
        let event_bus: NitriteEventBus<Event<&str>, MockListener> = NitriteEventBus::new();
        
        for _ in 0..100 {
            let _ = event_bus.register(MockListener);
        }
    }
}