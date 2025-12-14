use crate::collection::Document;
use crate::common::util::get_current_time_or_zero;
use crate::common::{Value, CREATED_TIME, OWNER, UNIQUE_ID};
use crate::errors::NitriteResult;
use indexmap::IndexMap;
use std::fmt::Display;
use uuid::Uuid;

/// Trait for accessing and managing metadata attributes of persistent collections.
///
/// # Purpose
/// Provides a contract for objects that need to store and retrieve metadata (attributes) such as
/// creation time, ownership, and unique identifiers. Implementations are typically used by storage
/// adapters to manage collection-level metadata persistence.
///
/// # Usage
/// Implemented by storage adapter types like `FjallMap`. Client code accesses attributes through
/// the collection or map interface rather than directly through this trait.
pub trait AttributeAware {
    /// Retrieves the attributes for this object.
    ///
    /// # Returns
    /// `Ok(Some(Attributes))` if attributes are available, `Ok(None)` if no attributes are stored,
    /// or an error if the operation fails.
    ///
    /// # Behavior
    /// Returns all metadata attributes associated with this object. If the underlying storage
    /// contains invalid attribute data (wrong type), implementations should return `Ok(None)`
    /// rather than failing.
    fn attributes(&self) -> NitriteResult<Option<Attributes>>;

    /// Sets the attributes for this object.
    ///
    /// # Arguments
    /// * `attributes` - The `Attributes` struct containing all metadata to store.
    ///
    /// # Returns
    /// `Ok(())` on success, or an error if the operation fails.
    ///
    /// # Behavior
    /// Persists the provided attributes to the underlying storage. Implementations convert
    /// attributes to a `Document` format for storage. This operation is typically performed
    /// during collection creation to store ownership and creation metadata.
    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()>;
}

#[derive(Debug, Clone, Default, PartialEq)]
/// Metadata attributes container for persistent collections.
///
/// # Purpose
/// Stores key-value metadata pairs associated with collections, such as creation timestamp,
/// owner identifier, and unique IDs. Attributes are persisted to storage and retrieved when
/// collections are accessed.
///
/// # Characteristics
/// - **Ordered**: Uses IndexMap to maintain insertion order
/// - **Cloneable**: Can be cloned via the `Clone` trait
/// - **Serializable**: Converts to/from `Document` format for persistence
/// - **Default Metadata**: Automatically includes creation time and unique identifier
/// - **Collection-aware**: Can include owner information for collection-scoped attributes
///
/// # Fields
/// - `attributes`: Map of attribute key-value pairs
pub struct Attributes {
    attributes: IndexMap<String, Value>,
}

impl Attributes {
    /// Creates a new `Attributes` instance with default metadata.
    ///
    /// # Returns
    /// A new `Attributes` struct containing:
    /// - `CREATED_TIME`: Current timestamp as an ISO 8601 string (or "0" if time unavailable)
    /// - `UNIQUE_ID`: A randomly generated UUID v4 string
    ///
    /// # Behavior
    /// Initializes a map with capacity for 2 entries and populates them with creation
    /// metadata. This constructor is used when creating attributes for non-collection
    /// contexts without owner information.
    pub fn new() -> Self {
        // Preallocate with capacity 2 (CREATED_TIME, UNIQUE_ID)
        let mut attributes = IndexMap::with_capacity(2);
        attributes.insert(
            CREATED_TIME.to_string(),
            Value::String(get_current_time_or_zero().to_string()),
        );
        attributes.insert(
            UNIQUE_ID.to_string(),
            Value::String(Uuid::new_v4().to_string()),
        );

        Attributes { attributes }
    }
    /// Creates `Attributes` for a specific collection with ownership information.
    ///
    /// # Arguments
    /// * `collection` - The name/identifier of the collection that owns these attributes.
    ///
    /// # Returns
    /// A new `Attributes` struct containing:
    /// - `OWNER`: Collection name/identifier for ownership tracking
    /// - `CREATED_TIME`: Current timestamp as an ISO 8601 string
    /// - `UNIQUE_ID`: A randomly generated UUID v4 string
    ///
    /// # Behavior
    /// Similar to `new()` but includes the collection name as an owner identifier.
    /// Used when creating attributes for named collections to track which collection
    /// owns the metadata.
    pub fn new_for_collection(collection: &str) -> Self {
        // Preallocate with capacity 3 (OWNER, CREATED_TIME, UNIQUE_ID)
        let mut attributes = IndexMap::with_capacity(3);
        attributes.insert(OWNER.to_string(), Value::String(collection.to_string()));
        attributes.insert(
            CREATED_TIME.to_string(),
            Value::String(get_current_time_or_zero().to_string()),
        );
        attributes.insert(
            UNIQUE_ID.to_string(),
            Value::String(Uuid::new_v4().to_string()),
        );

        Attributes { attributes }
    }

    /// Creates `Attributes` for a collection with owner and unique identifier.
    ///
    /// # Arguments
    /// * `collection` - The name/identifier of the collection that owns these attributes.
    ///
    /// # Returns
    /// A new `Attributes` struct containing:
    /// - `OWNER`: Collection name/identifier
    /// - `CREATED_TIME`: Current timestamp as an ISO 8601 string
    /// - `UNIQUE_ID`: A randomly generated UUID v4 string
    ///
    /// # Behavior
    /// Equivalent to `new_for_collection()`. Includes owner, creation time, and a
    /// unique identifier for the attributed entity.
    pub fn new_with_id(collection: &str) -> Self {
        // Preallocate with capacity 3 (OWNER, CREATED_TIME, UNIQUE_ID)
        let mut attributes = IndexMap::with_capacity(3);
        attributes.insert(OWNER.to_string(), Value::String(collection.to_string()));
        attributes.insert(
            CREATED_TIME.to_string(),
            Value::String(get_current_time_or_zero().to_string()),
        );
        attributes.insert(
            UNIQUE_ID.to_string(),
            Value::String(Uuid::new_v4().to_string()),
        );

        Attributes { attributes }
    }

    /// Creates `Attributes` from a `Document`.
    ///
    /// # Arguments
    /// * `document` - A `Document` containing key-value attribute pairs.
    ///
    /// # Returns
    /// A new `Attributes` struct with all key-value pairs copied from the document.
    ///
    /// # Behavior
    /// Used for deserialization when retrieving persisted metadata. Copies all entries
    /// from the document into the attributes map. This is the inverse of `to_document()`.
    /// For example, when loading collection metadata from storage, the stored Document
    /// is converted back to Attributes via this method.
    pub fn from_document(document: &Document) -> Self {
        // Estimate capacity with reasonable default of 10
        let mut attributes = IndexMap::with_capacity(10);
        for (key, value) in document.iter() {
            attributes.insert(key.to_string(), value.clone());
        }
        Attributes { attributes }
    }

    /// Retrieves the value associated with a key.
    ///
    /// # Arguments
    /// * `key` - The attribute key to look up.
    ///
    /// # Returns
    /// `Some(&Value)` if the key exists, `None` otherwise.
    ///
    /// # Behavior
    /// Performs a case-sensitive lookup in the attributes map.
    #[inline]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.attributes.get(key)
    }

    /// Inserts or updates an attribute key-value pair.
    ///
    /// # Arguments
    /// * `key` - The attribute key (will be converted to String).
    /// * `value` - The `Value` to associate with the key.
    ///
    /// # Behavior
    /// Inserts a new key-value pair or updates the value if the key already exists.
    /// Uses IndexMap which preserves insertion order for new keys.
    #[inline]
    pub fn put(&mut self, key: &str, value: Value) {
        self.attributes.insert(key.to_string(), value);
    }

    /// Checks if an attribute key exists.
    ///
    /// # Arguments
    /// * `key` - The attribute key to check.
    ///
    /// # Returns
    /// `true` if the key exists, `false` otherwise.
    ///
    /// # Behavior
    /// Performs a case-sensitive existence check.
    #[inline]
    pub fn has_key(&self, key: &str) -> bool {
        self.attributes.contains_key(key)
    }

    /// Converts this `Attributes` to a `Document`.
    ///
    /// # Returns
    /// A new `Document` containing all attribute key-value pairs.
    ///
    /// # Behavior
    /// Serializes the attributes map into a Document for persistence to storage.
    /// The inverse operation is `from_document()`. This enables round-trip conversion:
    /// attributes → Document → attributes. All `Value` types are cloned.
    #[inline]
    pub fn to_document(&self) -> Document {
        let mut document = Document::new();
        for (key, value) in self.attributes.iter() {
            document.put(key, value.clone()).unwrap();
        }
        document
    }
}

impl Display for Attributes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::new();
        for (key, value) in self.attributes.iter() {
            result.push_str(&format!("{}: {}, ", key, value));
        }
        write!(f, "{}", result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::{atomic, Atomic, ReadExecutor, Value, WriteExecutor};
    use crate::errors::NitriteResult;

    #[test]
    fn test_attributes_new() {
        let attributes = Attributes::new();
        assert!(attributes.has_key(CREATED_TIME));
        assert!(attributes.has_key(UNIQUE_ID));
    }

    #[test]
    fn test_attributes_new_with_id() {
        let collection = "test_collection";
        let attributes = Attributes::new_with_id(collection);
        assert!(attributes.has_key(OWNER));
        assert_eq!(attributes.get(OWNER).unwrap(), &Value::String(collection.to_string()));
    }

    #[test]
    fn test_attributes_from_document() {
        let mut document = Document::new();
        document.put("key1", Value::String("value1".to_string())).unwrap();
        document.put("key2", Value::String("value2".to_string())).unwrap();

        let attributes = Attributes::from_document(&document);
        assert_eq!(attributes.get("key1").unwrap(), &Value::String("value1".to_string()));
        assert_eq!(attributes.get("key2").unwrap(), &Value::String("value2".to_string()));
    }

    #[test]
    fn test_attributes_get() {
        let mut attributes = Attributes::new();
        attributes.put("key", Value::String("value".to_string()));
        assert_eq!(attributes.get("key").unwrap(), &Value::String("value".to_string()));
    }

    #[test]
    fn test_attributes_get_none() {
        let attributes = Attributes::new();
        assert!(attributes.get("nonexistent_key").is_none());
    }

    #[test]
    fn test_attributes_put() {
        let mut attributes = Attributes::new();
        attributes.put("key", Value::String("value".to_string()));
        assert_eq!(attributes.get("key").unwrap(), &Value::String("value".to_string()));
    }

    #[test]
    fn test_attributes_has_key() {
        let mut attributes = Attributes::new();
        attributes.put("key", Value::String("value".to_string()));
        assert!(attributes.has_key("key"));
    }

    #[test]
    fn test_attributes_has_key_false() {
        let attributes = Attributes::new();
        assert!(!attributes.has_key("nonexistent_key"));
    }

    #[test]
    fn test_attributes_to_document() {
        let mut attributes = Attributes::new();
        attributes.put("key", Value::String("value".to_string()));
        let document = attributes.to_document();
        assert_eq!(document.get("key").unwrap(), Value::String("value".to_string()));
    }

    #[test]
    fn test_attributes_display() {
        let mut attributes = Attributes::new();
        attributes.put("key", Value::String("value".to_string()));
        let display = format!("{}", attributes);
        assert!(display.contains("key: \"value\""));
    }

    #[test]
    fn test_attribute_aware_trait() {
        struct TestStruct {
            attributes: Atomic<Attributes>,
        }

        impl AttributeAware for TestStruct {
            fn attributes(&self) -> NitriteResult<Option<Attributes>> {
                self.attributes.read_with(|attributes| Ok(Some(attributes.clone())))
            }

            fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
                self.attributes.write_with(|current_attributes| {
                    *current_attributes = attributes;
                    Ok(())
                })
            }
        }

        let test_struct = TestStruct {
            attributes: atomic(Attributes::new()),
        };

        let new_attributes = Attributes::new_with_id("test_collection");
        test_struct.set_attributes(new_attributes.clone()).unwrap();
        assert_eq!(test_struct.attributes().unwrap().unwrap(), new_attributes);
    }

    #[test]
    fn bench_attributes_creation() {
        for _ in 0..1000 {
            let _ = Attributes::new();
        }
    }

    #[test]
    fn bench_attributes_for_collection() {
        for i in 0..500 {
            let _ = Attributes::new_for_collection(&format!("collection_{}", i));
        }
    }

    #[test]
    fn bench_attributes_access_and_put() {
        let mut attributes = Attributes::new_for_collection("test");
        for i in 0..100 {
            attributes.put(&format!("key_{}", i), Value::I32(i as i32));
            let _ = attributes.get(&format!("key_{}", i));
        }
    }
}