use crate::common::{ReadExecutor, WriteExecutor};
use crate::errors::NitriteResult;
use crate::{atomic, get_current_time_or_zero, Atomic, Value};
use anyhow::Error;
use basu::error::BasuError;
use basu::event::Event;
use basu::Handle;
use std::fmt::Debug;
use std::sync::Arc;

/// Event types that can occur on a collection.
///
/// CollectionEvents enumerates all collection-level operations that can trigger
/// event listeners. Events are fired when documents are modified or when indexes
/// are being built.
///
/// # Variants
/// - `Insert`: A new document was added to the collection
/// - `Update`: An existing document was modified in the collection
/// - `Remove`: A document was deleted from the collection
/// - `IndexStart`: Index creation/rebuild has begun
/// - `IndexEnd`: Index creation/rebuild has completed
///
/// # Usage
///
/// Event listeners receive CollectionEventInfo with the event_type to determine
/// what operation occurred on the collection.
#[derive(Debug, Clone, PartialEq)]
pub enum CollectionEvents {
    Insert,
    Update,
    Remove,
    IndexStart,
    IndexEnd,
}

/// Information about a collection event that occurred.
///
/// CollectionEventInfo contains details about an operation that happened on a collection,
/// including what type of operation, the affected data, and metadata about when/where
/// the event originated.
///
/// # Characteristics
/// - **Cloneable**: Thread-safe sharing via Arc, suitable for concurrent event processing
/// - **Immutable item**: The document/value is captured at event time
/// - **Atomic originator**: The originator can be updated via lock-free atomic operations
/// - **Timestamped**: Each event records its creation time automatically
///
/// # Usage
///
/// Event listeners receive CollectionEventInfo instances when events are triggered:
/// ```ignore
/// collection.subscribe(CollectionEventListener::new(|event: CollectionEventInfo| {
///     match event.event_type() {
///         CollectionEvents::Insert => println!("Document inserted"),
///         CollectionEvents::Update => println!("Document updated"),
///         _ => {}
///     }
///     Ok(())
/// }))?;
/// ```
#[derive(Clone)]
pub struct CollectionEventInfo {
    /// Arc-wrapped implementation pointer (opaque to users)
    inner: Arc<CollectionEventInner>,
}

impl CollectionEventInfo {
    /// Creates a new collection event with the specified item, event type, and originator.
    ///
    /// # Arguments
    ///
    /// * `item` - The document or value associated with this event (None for index events)
    /// * `event_type` - The type of event (Insert, Update, Remove, IndexStart, IndexEnd)
    /// * `originator` - A string identifying the source/originator of this event
    ///
    /// # Behavior
    ///
    /// Creates a new event with the current timestamp captured automatically.
    /// The originator string is stored with atomic operations to allow thread-safe updates.
    /// For index events, the item is typically None.
    pub fn new(item: Option<Value>, event_type: CollectionEvents, originator: String) -> Self {
        CollectionEventInfo {
            inner: Arc::new(CollectionEventInner::new(item, event_type, originator)),
        }
    }

    /// Returns the type of event (Insert, Update, Remove, etc.)
    ///
    /// # Returns
    ///
    /// The CollectionEvents variant indicating what type of operation occurred.
    pub fn event_type(&self) -> CollectionEvents {
        self.inner.event_type.clone()
    }

    /// Returns the item (document/value) associated with this event, if any.
    ///
    /// # Returns
    ///
    /// Some(Value) if an item is associated with the event (e.g., the inserted/updated document),
    /// None for index-related events (IndexStart, IndexEnd).
    pub fn item(&self) -> Option<Value> {
        self.inner.item.clone()
    }

    /// Returns the originator/source of this event.
    ///
    /// # Returns
    ///
    /// A string identifying where this event originated from (e.g., application name,
    /// trigger name, or module identifier).
    pub fn originator(&self) -> String {
        self.inner.originator.read_with(|x| x.clone())
    }

    /// Returns the timestamp (milliseconds since epoch) when this event was created.
    ///
    /// # Returns
    ///
    /// The creation timestamp in milliseconds since Unix epoch.
    pub fn timestamp(&self) -> u128 {
        self.inner.timestamp
    }

    /// Updates the originator of this event.
    ///
    /// # Arguments
    ///
    /// * `originator` - The new originator string
    ///
    /// # Behavior
    ///
    /// Mutates the internal originator field using lock-free atomic operations,
    /// allowing safe concurrent updates without locks.
    pub(crate) fn set_originator(&self, originator: String) {
        self.inner.set_originator(originator);
    }
}

impl Debug for CollectionEventInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionEventInfo")
            .field("item", &self.item())
            .field("event_type", &self.event_type())
            .field("timestamp", &self.timestamp())
            .field("originator", &self.originator())
            .finish()
    }
}

/// Opaque implementation details of CollectionEventInfo.
/// This struct is part of the PIMPL pattern and should not be accessed directly.
pub(crate) struct CollectionEventInner {
    item: Option<Value>,
    event_type: CollectionEvents,
    timestamp: u128,
    originator: Atomic<String>,
}

impl CollectionEventInner {
    fn new(item: Option<Value>, event_type: CollectionEvents, originator: String) -> Self {
        CollectionEventInner {
            item,
            event_type,
            timestamp: get_current_time_or_zero(),
            originator: atomic(originator),
        }
    }

    fn set_originator(&self, originator: String) {
        self.originator.write_with(|o| *o = originator);
    }
}

/// Trait for closure-based event handlers.
///
/// CollectionEventCallback defines the signature for event handler functions.
/// Any closure capturing the signature `Fn(CollectionEventInfo) -> NitriteResult<()>`
/// automatically implements this trait.
///
/// # Requirements
///
/// - Must be Send + Sync for thread-safe execution in Rayon's parallel thread pool
/// - Must accept a CollectionEventInfo parameter
/// - Must return NitriteResult<()> to allow error propagation
pub trait CollectionEventCallback: Send + Sync + Fn(CollectionEventInfo) -> NitriteResult<()> {}

impl<F> CollectionEventCallback for F
where
    F: Send + Sync + Fn(CollectionEventInfo) -> NitriteResult<()>,
{
}

/// Listener for collection events.
///
/// CollectionEventListener wraps an event handler callback and can be registered with
/// a collection to receive notifications when collection operations occur.
///
/// # Characteristics
/// - **Cloneable**: Thread-safe sharing via Arc for use across threads
/// - **Closure-based**: Accepts any callable matching CollectionEventCallback signature
/// - **Asynchronous**: Events are processed in Rayon's parallel thread pool
///
/// # Usage
///
/// Register a listener with a collection via subscribe():
/// ```ignore
/// collection.subscribe(CollectionEventListener::new(|event| {
///     println!("Event: {:?}", event.event_type());
///     Ok(())
/// }))?;
/// ```
#[derive(Clone)]
pub struct CollectionEventListener {
    on_event: Arc<dyn CollectionEventCallback>,
}

impl CollectionEventListener {
    /// Creates a new event listener wrapping the provided callback.
    ///
    /// # Arguments
    ///
    /// * `on_event` - A closure matching the CollectionEventCallback signature
    ///
    /// # Returns
    ///
    /// A new CollectionEventListener ready to be subscribed to a collection.
    ///
    /// # Behavior
    ///
    /// Wraps the callback in Arc<dyn CollectionEventCallback> to enable polymorphic
    /// listener usage and thread-safe sharing.
    pub fn new(on_event: impl CollectionEventCallback + 'static) -> Self {
        CollectionEventListener {
            on_event: Arc::new(on_event),
        }
    }
}

impl Handle<CollectionEventInfo> for CollectionEventListener {
    fn handle(&self, event: &Event<CollectionEventInfo>) -> Result<(), BasuError> {
        // below code will run in rayon's thread pool using parallel iterator
        match (self.on_event)(event.data.clone()) {
            Ok(_) => Ok(()),
            Err(e) => Err(BasuError::HandlerError(Error::from(e))),
        }
    }
}

impl Debug for CollectionEventListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CollectionEventListener")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basu::event::Event;
    use std::sync::Arc;

    #[test]
    fn test_collection_event_new() {
        let item = Some(Value::String("test_item".to_string()));
        let event_type = CollectionEvents::Insert;
        let originator = "originator".to_string();
        let event = CollectionEventInfo::new(item.clone(), event_type.clone(), originator.clone());

        assert_eq!(event.item(), item);
        assert_eq!(event.event_type(), event_type);
        assert_eq!(event.originator(), originator);
    }

    #[test]
    fn test_collection_event_set_originator() {
        let item = Some(Value::String("test_item".to_string()));
        let event_type = CollectionEvents::Insert;
        let originator = "originator".to_string();
        let event = CollectionEventInfo::new(item, event_type, originator);

        let new_originator = "new_originator".to_string();
        event.set_originator(new_originator.clone());

        assert_eq!(event.originator(), new_originator);
    }

    #[test]
    fn test_collection_event_listener_new() {
        let callback = |_event| Ok(());
        let listener = CollectionEventListener::new(callback);

        assert!(Arc::strong_count(&listener.on_event) > 0);
    }

    #[test]
    fn test_collection_event_listener_handle() {
        let callback = |_event| Ok(());
        let listener = CollectionEventListener::new(callback);

        let item = Some(Value::String("test_item".to_string()));
        let event_type = CollectionEvents::Insert;
        let originator = "originator".to_string();
        let collection_event = CollectionEventInfo::new(item, event_type, originator);
        let event = Event::new(collection_event);

        assert!(listener.handle(&event).is_ok());
    }

    #[test]
    fn test_collection_event_debug() {
        let item = Some(Value::String("test_item".to_string()));
        let event_type = CollectionEvents::Insert;
        let originator = "originator".to_string();
        let event = CollectionEventInfo::new(item, event_type, originator);

        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("CollectionEvent"));
    }
}