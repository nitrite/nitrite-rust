use smallvec::SmallVec;
use im::OrdMap;

use crate::collection::nitrite_id::NitriteId;
use crate::common::{ReadExecutor, Value, DOC_ID, DOC_MODIFIED, DOC_REVISION, DOC_SOURCE, RESERVED_FIELDS};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::FIELD_SEPARATOR;
use itertools::Itertools;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Display};

type FieldVec = SmallVec<[String; 8]>;

/// Represents a document in Nitrite database using lock-free persistent data structure.
///
/// Nitrite document are composed of key-value pairs. The key is always a
/// [String] and value is a [Value].
///
/// Nitrite document supports nested documents as well. The key of a nested
/// document is a [String] separated by the field separator (default: `.`).
/// The field separator can be configured using [`crate::nitrite_builder::NitriteBuilder::field_separator`].
///
/// For example, if a document has a nested document `{"a": {"b": 1}}`, then the
/// value inside the nested document can be retrieved by calling `document.get("a.b")`.
///
/// Below fields are reserved and cannot be used as key in a document.
///
/// * `_id` - The unique identifier of the document. If not provided, Nitrite
/// will generate a unique [NitriteId] for the document during insertion.
/// * `_revision` - The revision number of the document.
/// * `_source` - The source of the document.
/// * `_modified` - The last modified time of the document.
///
/// ## Lock-Free Design
///
/// This struct uses `im::OrdMap` (a persistent ordered map) for lock-free operation:
/// - O(1) cloning via internal Arc sharing
/// - Mutations create new maps via structural sharing (90% structure reused)
/// - Each mutated document is completely independent
/// - Zero locks, zero copy-on-write overhead
#[derive(Clone, Eq, PartialEq, Hash, Default, Ord, PartialOrd, serde::Deserialize, serde::Serialize)]
pub struct Document {
    /// Persistent ordered map: O(1) clone via internal Arc, O(log n) mutations
    data: OrdMap<String, Value>,
}

impl Document {
    /// Creates a new empty document.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let doc = Document::new();
    /// assert!(doc.is_empty());
    /// assert_eq!(doc.size(), 0);
    /// ```
    pub fn new() -> Self {
        Document {
            data: OrdMap::new(),
        }
    }

    /// Checks if the document is empty.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let empty_doc = Document::new();
    /// assert!(empty_doc.is_empty());
    ///
    /// let mut doc = doc!{ "key": "value" };
    /// assert!(!doc.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Associates the specified [Value] with the specified key in this document.
    ///
    /// This method inserts a key-value pair into the document. If the key already exists,
    /// its value is updated. The method supports both top-level and embedded keys
    /// (e.g., `"user.name"` or `"location.address.zip"`).
    ///
    /// # Arguments
    ///
    /// * `key` - The key as a string or string slice. Cannot be empty.
    /// * `value` - The value to associate with the key. Can be any type that implements
    ///   `Into<Value>` (primitives, strings, documents, arrays, etc.).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// * The key is empty
    /// * The key contains the reserved field `_id` with a non-NitriteId value
    ///
    /// # Examples
    ///
    /// Basic insertion:
    /// ```ignore
    /// let mut doc = Document::new();
    /// doc.put("name", "Alice")?;
    /// doc.put("age", 30)?;
    /// assert_eq!(doc.size(), 2);
    /// ```
    ///
    /// Nested document insertion:
    /// ```ignore
    /// let mut doc = Document::new();
    /// doc.put("user.name", "Alice")?;
    /// doc.put("user.email", "alice@example.com")?;
    /// assert_eq!(doc.get("user.name")?, Value::String("Alice".to_string()));
    /// ```
    ///
    /// Updating existing key:
    /// ```ignore
    /// let mut doc = doc!{ "status": "inactive" };
    /// doc.put("status", "active")?;
    /// assert_eq!(doc.get("status")?, Value::String("active".to_string()));
    /// ```
    pub fn put<'a, T: Into<Value>>(&mut self, key: impl Into<Cow<'a, str>>, value: T) -> NitriteResult<()> {
        let key = key.into();
        // key cannot be empty
        if key.is_empty() {
            log::error!("Document does not support empty key");
            return Err(NitriteError::new(
                "Document does not support empty key",
                ErrorKind::InvalidOperation,
            ));
        }
        
        let value = value.into();

        // validate the _id field
        if key == DOC_ID && !value.is_nitrite_id() {
            log::error!("Document id is an auto generated field and cannot be set manually");
            return Err(NitriteError::new(
                "Document id is an auto generated field and cannot be set manually",
                ErrorKind::InvalidOperation,
            ));
        }

        // if field name contains field separator, split the fields, and put the value
        // accordingly associated with the embedded field.
        if FIELD_SEPARATOR.read_with(|sep| key.contains(sep)) {
            let splits: Vec<&str> = FIELD_SEPARATOR.read_with(|it| key.split(it).collect());
            self.deep_put(&splits, value)
        } else {
            self.data = self.data.update(key.to_string(), value);
            Ok(())
        }
    }

    /// Returns the [Value] to which the specified key is associated, or [Value::Null]
    /// if this document contains no mapping for the key.
    ///
    /// This method retrieves the value associated with a key. If the key does not exist,
    /// it returns [Value::Null]. The method supports both top-level and embedded keys
    /// (e.g., `"location.address.zip"`).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up as a string slice.
    ///
    /// # Returns
    ///
    /// Returns the associated [Value], or [Value::Null] if the key does not exist.
    ///
    /// # Examples
    ///
    /// Retrieving a top-level key:
    /// ```ignore
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// assert_eq!(doc.get("name")?, Value::String("Alice".to_string()));
    /// assert_eq!(doc.get("age")?, Value::I32(30));
    /// ```
    ///
    /// Accessing nested documents:
    /// ```ignore
    /// let doc = doc!{
    ///     "location": {
    ///         "city": "New York",
    ///         "zip": 10001
    ///     }
    /// };
    /// assert_eq!(doc.get("location.city")?, Value::String("New York".to_string()));
    /// assert_eq!(doc.get("location.zip")?, Value::I32(10001));
    /// ```
    ///
    /// Accessing array elements:
    /// ```ignore
    /// let doc = doc!{ "items": [1, 2, 3] };
    /// assert_eq!(doc.get("items.0")?, Value::I32(1));
    /// assert_eq!(doc.get("items.1")?, Value::I32(2));
    /// ```
    ///
    /// Non-existent key returns Null:
    /// ```ignore
    /// let doc = doc!{ "name": "Alice" };
    /// assert_eq!(doc.get("missing")?, Value::Null);
    /// ```
    pub fn get(&self, key: &str) -> NitriteResult<Value> {
        match self.data.get(key) {
            Some(value) => Ok(value.clone()),
            None => {
                // Only check for embedded key if not found at top level
                if FIELD_SEPARATOR.read_with(|sep| key.contains(sep)) {
                    self.deep_get(key)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    /// Return the [NitriteId] associated with this document.
    ///
    /// If the document does not have an `_id` field, this method automatically generates
    /// a new [NitriteId] and assigns it to the document. This method mutates the document
    /// only if an ID needs to be generated.
    ///
    /// # Returns
    ///
    /// The [NitriteId] associated with this document.
    ///
    /// # Examples
    ///
    /// Getting the ID of an existing document:
    /// ```ignore
    /// let mut doc = doc!{ "name": "Alice" };
    /// let id = doc.id()?;
    /// assert!(!id.to_string().is_empty());
    /// // After calling id(), the document now has an _id field
    /// assert!(doc.has_id());
    /// ```
    ///
    /// Auto-generating ID when missing:
    /// ```ignore
    /// let mut doc1 = doc!{ "name": "Alice" };
    /// let mut doc2 = doc!{ "name": "Bob" };
    ///
    /// let id1 = doc1.id()?;
    /// let id2 = doc2.id()?;
    ///
    /// // Each document gets a unique ID
    /// assert_ne!(id1, id2);
    /// ```
    pub fn id(&mut self) -> NitriteResult<NitriteId> {
        if let Some(Value::NitriteId(id)) = self.data.get(DOC_ID) {
            Ok(id.clone())
        } else {
            // if _id field is not populated already, create a new id
            // and set it in the document
            let nitrite_id = NitriteId::new();
            self.data = self.data.update(
                DOC_ID.to_string(),
                Value::NitriteId(nitrite_id.clone()),
            );
            Ok(nitrite_id)
        }
    }

    /// Retrieves all fields (top level and embedded) associated with this document.
    ///
    /// This method returns a collection of all field paths in the document, including
    /// top-level fields and embedded fields from nested documents. Embedded fields are
    /// represented using the field separator (default: `.`). Reserved fields
    /// (`_id`, `_revision`, `_source`, `_modified`) are excluded from the result.
    ///
    /// # Returns
    ///
    /// A [FieldVec] containing all field paths in the document.
    ///
    /// # Examples
    ///
    /// Retrieving fields from a simple document:
    /// ```ignore
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// let fields = doc.fields();
    /// assert_eq!(fields.len(), 2);
    /// assert!(fields.contains(&"name".to_string()));
    /// assert!(fields.contains(&"age".to_string()));
    /// ```
    ///
    /// Retrieving fields from a nested document:
    /// ```ignore
    /// let doc = doc!{
    ///     "user": {
    ///         "name": "Alice",
    ///         "email": "alice@example.com"
    ///     },
    ///     "status": "active"
    /// };
    /// let fields = doc.fields();
    /// // Returns ["user.name", "user.email", "status"]
    /// assert!(fields.contains(&"user.name".to_string()));
    /// assert!(fields.contains(&"user.email".to_string()));
    /// assert!(fields.contains(&"status".to_string()));
    /// ```
    ///
    /// Empty document:
    /// ```ignore
    /// let doc = Document::new();
    /// let fields = doc.fields();
    /// assert!(fields.is_empty());
    /// ```
    pub fn fields(&self) -> FieldVec {
        self.get_fields_internal("")
    }

    /// Checks if this document has a nitrite id.
    ///
    /// # Returns
    ///
    /// `true` if the document has an `_id` field, `false` otherwise.
    ///
    /// # Examples
    ///
    /// Checking ID presence:
    /// ```ignore
    /// let mut doc = doc!{ "name": "Alice" };
    /// assert!(!doc.has_id());  // No ID yet
    ///
    /// let _id = doc.id()?;      // Generate ID
    /// assert!(doc.has_id());    // Now has ID
    /// ```
    pub fn has_id(&self) -> bool {
        self.data.contains_key(DOC_ID)
    }

    /// Removes the key and its value from the document.
    ///
    /// Deletes the key-value pair associated with the given key. If the key does not exist,
    /// the operation succeeds without error. The method supports both top-level and embedded keys.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to remove as a string slice.
    ///
    /// # Errors
    ///
    /// Returns an error if the key contains invalid embedded field separators.
    ///
    /// # Examples
    ///
    /// Removing a top-level key:
    /// ```ignore
    /// let mut doc = doc!{ "name": "Alice", "age": 30 };
    /// doc.remove("age")?;
    /// assert_eq!(doc.get("age")?, Value::Null);
    /// assert_eq!(doc.size(), 1);
    /// ```
    ///
    /// Removing a nested field:
    /// ```ignore
    /// let mut doc = doc!{
    ///     "user": {
    ///         "name": "Alice",
    ///         "email": "alice@example.com"
    ///     }
    /// };
    /// doc.remove("user.email")?;
    /// assert_eq!(doc.get("user.email")?, Value::Null);
    /// assert_eq!(doc.get("user.name")?, Value::String("Alice".to_string()));
    /// ```
    ///
    /// Removing non-existent key succeeds:
    /// ```ignore
    /// let mut doc = doc!{ "name": "Alice" };
    /// doc.remove("missing")?;  // No error
    /// assert_eq!(doc.size(), 1);
    /// ```
    pub fn remove(&mut self, key: &str) -> NitriteResult<()> {
        if FIELD_SEPARATOR.read_with(|sep| key.contains(sep)) {
            // if the field is an embedded field,
            // run a deep scan and remove the last field
            let splits: Vec<&str> = FIELD_SEPARATOR.read_with(|it| key.split(it).collect());
            self.deep_remove(&splits)
        } else {
            self.data = self.data.without(key);
            Ok(())
        }
    }

    /// Returns the number of entries in the document.
    ///
    /// # Returns
    ///
    /// The count of key-value pairs in this document (top-level only, not including nested entries).
    ///
    /// # Examples
    ///
    /// Counting entries:
    /// ```ignore
    /// let doc = Document::new();
    /// assert_eq!(doc.size(), 0);
    ///
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// assert_eq!(doc.size(), 2);
    ///
    /// // Nested documents count as one entry
    /// let doc = doc!{ "user": { "name": "Alice" }, "status": "active" };
    /// assert_eq!(doc.size(), 2);
    /// ```
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Merges a document in this document.
    ///
    /// Merges all key-value pairs from another document into this one. If a key already exists:
    /// - If both values are documents, they are merged recursively
    /// - Otherwise, the value from `other` overwrites the existing value
    ///
    /// # Arguments
    ///
    /// * `other` - The document to merge into this one.
    ///
    /// # Examples
    ///
    /// Basic merge:
    /// ```ignore
    /// let mut doc1 = doc!{ "name": "Alice", "age": 30 };
    /// let doc2 = doc!{ "email": "alice@example.com", "age": 31 };
    /// doc1.merge(&doc2)?;
    ///
    /// assert_eq!(doc1.get("name")?, Value::String("Alice".to_string()));
    /// assert_eq!(doc1.get("age")?, Value::I32(31));       // Overwritten
    /// assert_eq!(doc1.get("email")?, Value::String("alice@example.com".to_string()));
    /// ```
    ///
    /// Recursive merge of nested documents:
    /// ```ignore
    /// let mut doc1 = doc!{
    ///     "user": {
    ///         "name": "Alice",
    ///         "age": 30
    ///     }
    /// };
    /// let doc2 = doc!{
    ///     "user": {
    ///         "email": "alice@example.com"
    ///     }
    /// };
    /// doc1.merge(&doc2)?;
    ///
    /// assert_eq!(doc1.get("user.name")?, Value::String("Alice".to_string()));
    /// assert_eq!(doc1.get("user.age")?, Value::I32(30));
    /// assert_eq!(doc1.get("user.email")?, Value::String("alice@example.com".to_string()));
    /// ```
    pub fn merge(&mut self, other: &Document) -> NitriteResult<()> {
        for (key, value) in other.data.iter() {
            match value {
                Value::Document(obj) => {
                    // if the value is a document, merge it recursively
                    if let Some(Value::Document(mut nested_obj)) = self.data.get(key).cloned() {
                        nested_obj.merge(&obj)?;
                        self.data = self.data.update(key.clone(), Value::Document(nested_obj));
                    } else {
                        // Otherwise, just set the value
                        self.data = self.data.update(key.clone(), value.clone());
                    }
                }
                _ => {
                    // if there is no embedded document, put the field in the document
                    self.data = self.data.update(key.clone(), value.clone());
                }
            }
        }
        Ok(())
    }

    /// Checks if a top level key exists in the document.
    ///
    /// This method only checks for top-level keys, not embedded fields. Use [contains_field]
    /// to check for embedded fields.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check as a string slice.
    ///
    /// # Returns
    ///
    /// `true` if the key exists at the top level, `false` otherwise.
    ///
    /// # Examples
    ///
    /// Checking top-level keys:
    /// ```ignore
    /// let doc = doc!{
    ///     "name": "Alice",
    ///     "user": { "email": "alice@example.com" }
    /// };
    ///
    /// assert!(doc.contains_key("name"));
    /// assert!(doc.contains_key("user"));
    /// assert!(!doc.contains_key("email"));  // email is nested, not top-level
    /// assert!(!doc.contains_key("age"));    // doesn't exist
    /// ```
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Checks if a top level field or embedded field exists in the document.
    ///
    /// This method checks both top-level and embedded fields. It returns `true` if the
    /// field exists at any level in the document hierarchy.
    ///
    /// # Arguments
    ///
    /// * `field` - The field path to check as a string slice (e.g., `"user.email"`).
    ///
    /// # Returns
    ///
    /// `true` if the field exists at any level, `false` otherwise.
    ///
    /// # Examples
    ///
    /// Checking embedded fields:
    /// ```ignore
    /// let doc = doc!{
    ///     "name": "Alice",
    ///     "location": {
    ///         "city": "New York",
    ///         "address": {
    ///             "zip": 10001
    ///         }
    ///     }
    /// };
    ///
    /// assert!(doc.contains_field("name"));                     // Top-level
    /// assert!(doc.contains_field("location"));                 // Top-level document
    /// assert!(doc.contains_field("location.city"));            // Nested
    /// assert!(doc.contains_field("location.address.zip"));     // Deeply nested
    /// assert!(!doc.contains_field("location.country"));        // Doesn't exist
    /// ```
    pub fn contains_field(&self, field: &str) -> bool {
        if self.contains_key(field) {
            true
        } else {
            self.fields().contains(&field.to_string())
        }
    }

    /// Gets the document revision number.
    ///
    /// The revision number is an internal metadata field that tracks how many times
    /// the document has been modified. Returns 0 if the document has not been stored
    /// in a collection yet.
    ///
    /// # Returns
    ///
    /// The revision number as an `i32`, or 0 if not set.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let doc = Document::new();
    /// assert_eq!(doc.revision()?, 0);
    ///
    /// // After inserting and updating in a collection, revision increments
    /// let doc_from_collection = collection.find(all())?.next()?;
    /// let revision = doc_from_collection.revision()?;
    /// // revision will be > 0 depending on update history
    /// ```
    pub fn revision(&self) -> NitriteResult<i32> {
        if let Ok(Value::I32(revision)) = self.get(DOC_REVISION) {
            Ok(revision)
        } else {
            Ok(0)
        }
    }

    /// Gets the source of this document.
    ///
    /// The source is a metadata field that indicates where the document came from
    /// or what operation created it. Returns an empty string if not set.
    ///
    /// # Returns
    ///
    /// The source as a [String], or an empty string if not set.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let doc = Document::new();
    /// assert_eq!(doc.source()?, "");
    ///
    /// // Documents from collections may have source set by the database
    /// ```
    pub fn source(&self) -> NitriteResult<String> {
        if let Ok(Value::String(source)) = self.get(DOC_SOURCE) {
            Ok(source.clone())
        } else {
            Ok("".to_string())
        }
    }

    /// Gets last modified time of this document since epoch.
    ///
    /// Returns the timestamp (in milliseconds since Unix epoch) when the document
    /// was last modified. Returns 0 if the document has not been stored in a collection yet.
    ///
    /// # Returns
    ///
    /// The last modified time as an `i64` (milliseconds since epoch), or 0 if not set.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let doc = Document::new();
    /// assert_eq!(doc.last_modified_since_epoch()?, 0);
    ///
    /// // After inserting in a collection, timestamp is set
    /// let doc_from_collection = collection.find(all())?.next()?;
    /// let timestamp = doc_from_collection.last_modified_since_epoch()?;
    /// assert!(timestamp > 0);
    /// ```
    pub fn last_modified_since_epoch(&self) -> NitriteResult<i64> {
        if let Ok(Value::I64(modified)) = self.get(DOC_MODIFIED) {
            Ok(modified)
        } else {
            Ok(0)
        }
    }

    /// Converts this document to a [BTreeMap].
    ///
    /// Creates a new [BTreeMap] containing all the key-value pairs from this document.
    /// This is useful for interoperability with code expecting a standard map type.
    ///
    /// # Returns
    ///
    /// A new [BTreeMap] containing all entries from this document.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// let map = doc.to_map();
    /// assert_eq!(map.len(), 2);
    /// assert_eq!(map.get("name").unwrap(), &Value::String("Alice".to_string()));
    /// ```
    pub fn to_map(&self) -> BTreeMap<String, Value> {
        self.data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Gets an iterator over the key-value pairs of this document.
    ///
    /// Returns a [DocumentIter] that iterates over all top-level key-value pairs
    /// in the document. Each iteration yields a tuple of (key, value) where both
    /// are owned values.
    ///
    /// # Returns
    ///
    /// A [DocumentIter] for iterating over the document entries.
    ///
    /// # Examples
    ///
    /// Iterating over document entries:
    /// ```ignore
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// let mut count = 0;
    /// for (key, value) in doc.iter() {
    ///     count += 1;
    ///     println!("{}: {}", key, value);
    /// }
    /// assert_eq!(count, 2);
    /// ```
    ///
    /// Collecting entries into a vector:
    /// ```ignore
    /// let doc = doc!{ "name": "Alice", "age": 30 };
    /// let entries: Vec<_> = doc.iter().collect();
    /// assert_eq!(entries.len(), 2);
    /// ```
    pub fn iter(&self) -> DocumentIter {
        DocumentIter {
            keys: self.data.keys().cloned().collect(),
            data: self.clone(),
            index: 0,
        }
    }

    pub(crate) fn to_pretty_json(&self, indent: usize) -> String {
        if self.data.is_empty() {
            return "{}".to_string();
        }

        let estimated_size = self.data.len() * 30 + indent * 2;
        let mut json_string = String::with_capacity(estimated_size);

        json_string.push_str("{\n");
        let indent_str = " ".repeat(indent + 2);
        for (key, value) in self.data.iter() {
            json_string.push_str(&format!(
                "{}\"{}\": {},\n",
                indent_str,
                key,
                value.to_pretty_json(indent + 2)
            ));
        }

        json_string.pop();
        json_string.pop();
        json_string.push_str(&format!("\n{}}}", " ".repeat(indent)));
        json_string
    }

    pub(crate) fn to_debug_string(&self, indent: usize) -> String {
        if self.data.is_empty() {
            return "{}".to_string();
        }

        let mut debug_string = String::new();
        debug_string.push_str("{\n");
        let indent_str = " ".repeat(indent + 2);
        for (key, value) in self.data.iter() {
            debug_string.push_str(&format!(
                "{}\"{}\": {},\n",
                indent_str,
                key,
                value.to_debug_string(indent + 2)
            ));
        }

        debug_string.pop();
        debug_string.pop();
        debug_string.push_str(&format!("\n{}}}", " ".repeat(indent)));
        debug_string
    }

    fn is_embedded(&self, key: &str) -> bool {
        FIELD_SEPARATOR.read_with(|it| key.contains(it))
    }

    fn get_fields_internal(&self, prefix: &str) -> FieldVec {
        let mut fields = FieldVec::new();
        let separator = FIELD_SEPARATOR.read_with(|s| s.clone());

        // iterate top level keys
        for key in self.data.keys() {
            // ignore the reserved fields
            if RESERVED_FIELDS.contains(&key.as_str()) {
                continue;
            }

            if key.is_empty() {
                continue;
            }

            let field = if prefix.is_empty() {
                // level-1 fields
                key.clone()
            } else {
                // level-n fields, separated by field separator
                format!("{}{}{}", prefix, separator, key)
            };

            if let Some(Value::Document(doc)) = self.data.get(key) {
                // if the value is a document, traverse its fields recursively,
                // prefix would be the field name of the document
                fields.append(&mut doc.get_fields_internal(&field));
            } else {
                // if there is no more embedded document, add the field to the list
                fields.push(field);
            }
        }
        fields
    }

    fn deep_get(&self, key: &str) -> NitriteResult<Value> {
        if !self.is_embedded(key) {
            Ok(Value::Null)
        } else {
            self.get_by_embedded_key(key)
        }
    }

    fn deep_put(&mut self, splits: &[&str], value: Value) -> NitriteResult<()> {
        if splits.is_empty() {
            log::error!("Empty embedded key");
            return Err(NitriteError::new(
                "Empty embedded key",
                ErrorKind::ValidationError,
            ));
        }

        let key = splits[0];
        if key.is_empty() {
            log::error!("Document does not support empty key");
            return Err(NitriteError::new(
                "Document does not support empty key",
                ErrorKind::InvalidOperation,
            ));
        }

        if splits.len() == 1 {
            // if last key, simply put in the current document
            self.put(key, value)
        } else {
            let remaining_splits = &splits[1..];
            if let Some(Value::Document(mut obj)) = self.data.get(key).cloned() {
                // if the current level value is embedded doc, scan to the next level
                let result = obj.deep_put(remaining_splits, value);
                self.data = self.data.update(key.to_string(), Value::Document(obj));
                result
            } else {
                // if current level value is null, create a new document
                let mut nested_doc = Document::new();
                let result = nested_doc.deep_put(remaining_splits, value);
                self.data = self.data.update(key.to_string(), Value::Document(nested_doc));
                result
            }
        }
    }

    fn deep_remove(&mut self, splits: &[&str]) -> NitriteResult<()> {
        if splits.is_empty() {
            log::error!("Empty embedded key");
            return Err(NitriteError::new(
                "Empty embedded key",
                ErrorKind::ValidationError,
            ));
        }

        let key = splits[0];
        if key.is_empty() {
            log::error!("Document does not support empty key");
            return Err(NitriteError::new(
                "Document does not support empty key",
                ErrorKind::InvalidOperation,
            ));
        }

        if splits.len() == 1 {
            // if last key, simply remove from the current document
            self.remove(key)
        } else {
            let remaining_splits = &splits[1..];

            match self.data.get(key) {
                Some(Value::Document(obj)) => {
                    // if the current level value is embedded doc, scan to the next level
                    let mut nested_doc = obj.clone();
                    let result = nested_doc.deep_remove(remaining_splits);
                    if nested_doc.is_empty() {
                        // if the next level document is an empty one
                        // remove the current level document also
                        self.data = self.data.without(key);
                    } else {
                        self.data = self.data.update(key.to_string(), Value::Document(nested_doc));
                    }
                    result
                }
                Some(Value::Array(arr)) => {
                    let first = splits[1];
                    // if the current level value is an iterable,
                    // remove the element at the next level
                    if let Ok(index) = first.parse::<isize>() {
                        if index < 0 {
                            log::error!(
                                "Invalid array index {} to access array inside a document",
                                &index
                            );
                            return Err(NitriteError::new(
                                &format!(
                                    "Invalid array index {} to access array inside a document",
                                    &index
                                ),
                                ErrorKind::ValidationError,
                            ));
                        }

                        let index = index as usize;
                        if index >= arr.len() {
                            log::error!("Array index {} out of bound", &index);
                            return Err(NitriteError::new(
                                &format!("Array index {} out of bound", &index),
                                ErrorKind::ValidationError,
                            ));
                        }

                        let item = &arr[index];
                        if let (Value::Document(obj), true) = (item, splits.len() > 2) {
                            // if there are more splits, then this is an embedded document
                            let mut nested_doc = obj.clone();
                            let result = nested_doc.deep_remove(&remaining_splits[1..]);
                            if nested_doc.is_empty() {
                                // if the next level document is an empty one
                                // remove the element from array
                                let mut new_arr = arr.clone();
                                new_arr.remove(index);
                                self.data = self.data.update(key.to_string(), Value::Array(new_arr));
                            } else {
                                let mut new_arr = arr.clone();
                                new_arr[index] = Value::Document(nested_doc);
                                self.data = self.data.update(key.to_string(), Value::Array(new_arr));
                            }
                            result
                        } else {
                            // if there are no more splits, remove the element at the next level
                            let mut new_arr = arr.clone();
                            new_arr.remove(index);
                            self.data = self.data.update(key.to_string(), Value::Array(new_arr));
                            Ok(())
                        }
                    } else {
                        log::error!(
                            "Invalid array index {} to access array inside a document",
                            first
                        );
                        Err(NitriteError::new(
                            &format!(
                                "Invalid array index {} to access array inside a document",
                                first
                            ),
                            ErrorKind::ValidationError,
                        ))
                    }
                }
                _ => {
                    // if current level value is null, remove the key
                    self.data = self.data.without(key);
                    Ok(())
                }
            }
        }
    }

    fn get_by_embedded_key(&self, key: &str) -> NitriteResult<Value> {
        let separator = FIELD_SEPARATOR.read_with(|s| s.clone());
        let splits: Vec<&str> = key.split(&separator).collect();
        
        if splits.is_empty() {
            return Ok(Value::Null);
        }

        let first = splits[0];
        if first.is_empty() {
            log::error!("Document does not support empty key");
            return Err(NitriteError::new(
                "Document does not support empty key",
                ErrorKind::InvalidOperation,
            ));
        }

        // get current level value and scan to next level using remaining keys
        self.recursive_get(self.data.get(first), &splits[1..])
    }

    fn recursive_get(&self, value: Option<&Value>, splits: &[&str]) -> NitriteResult<Value> {
        let value = match value {
            None => return Ok(Value::Null),
            Some(v) => v,
        };

        if splits.is_empty() {
            return Ok(value.clone());
        }

        let key = splits[0];
        if key.is_empty() {
            log::error!("Document does not support empty key");
            return Err(NitriteError::new(
                "Document does not support empty key",
                ErrorKind::InvalidOperation,
            ));
        }

        match value {
            Value::Document(obj) => {
                // if the current level value is document, scan to the next level with remaining keys
                self.recursive_get(obj.data.get(key), &splits[1..])
            }
            Value::Array(arr) => {
                // if the current level value is an iterable
                let first = key;
                if let Ok(index) = first.parse::<isize>() {
                    // check index lower bound
                    if index < 0 {
                        log::error!(
                            "Invalid array index {} to access array inside a document",
                            &index
                        );
                        return Err(NitriteError::new(
                            &format!(
                                "Invalid array index {} to access array inside a document",
                                &index
                            ),
                            ErrorKind::ValidationError,
                        ));
                    }

                    // check index upper bound
                    let index = index as usize;
                    if index >= arr.len() {
                        log::error!("Array index {} out of bound", &index);
                        return Err(NitriteError::new(
                            &format!("Array index {} out of bound", &index),
                            ErrorKind::ValidationError,
                        ));
                    }

                    // get the value at the index from the list
                    let item = &arr[index];
                    self.recursive_get(Some(item), &splits[1..])
                } else {
                    // if the current key is not an integer, decompose the list
                    self.decompose(arr, &splits)
                }
            }
            _ => Ok(Value::Null), // if no match found return null
        }
    }

    fn decompose(&self, arr: &[Value], splits: &[&str]) -> NitriteResult<Value> {
        let mut items: Vec<Value> = Vec::with_capacity(arr.len());

        for item in arr {
            // scan the item using remaining keys and use ? for error propagation
            let result = self.recursive_get(Some(item), splits)?;

            match result {
                Value::Array(arr) => {
                    // if the result is an iterable, add all items to the list
                    for v in arr {
                        items.push(v);
                    }
                }
                value => {
                    // if the result is not an iterable, add the result to the list
                    items.push(value);
                }
            }
        }
        // remove duplicates from the list
        Ok(Value::Array(items.iter().unique().cloned().collect::<Vec<_>>()))
    }
}

impl Debug for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_debug_string(0))
    }
}

impl Display for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_pretty_json(0))
    }
}

pub struct DocumentIter {
    keys: Vec<String>,
    data: Document,
    index: usize,
}

impl Iterator for DocumentIter {
    type Item = (String, Value);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.keys.len() {
            let key = &self.keys[self.index];
            if let Some(value) = self.data.data.get(key) {
                let result = (key.clone(), value.clone());
                self.index += 1;
                return Some(result);
            }
            self.index += 1;
            self.next()
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.keys.len().saturating_sub(self.index);
        (remaining, Some(remaining))
    }
}

pub fn normalize(value: &str) -> String {
    value.trim_matches('"').to_string()
}

/// Creates a Nitrite Document with JSON-like syntax.
///
/// # Examples
///
/// ```rust
/// use nitrite::doc;
///
/// // Empty document
/// let empty = doc!{};
///
/// // Simple key-value pairs
/// let simple = doc!{
///     name: "Alice",
///     age: 30
/// };
///
/// // With expressions
/// let base = 100;
/// let with_expr = doc!{
///     name: "Bob",
///     score: (base * 2),
///     computed: (base + 50)
/// };
///
/// // Nested documents and arrays
/// let complex = doc!{
///     user: {
///         name: "Charlie",
///         tags: ["admin", "user"]
///     },
///     values: [1, 2, 3]
/// };
/// ```
#[macro_export]
macro_rules! doc {
    // match an empty document (with braces for backward compat)
    ({}) => {
        $crate::collection::Document::new()
    };

    // match an empty document (new syntax)
    () => {
        $crate::collection::Document::new()
    };

    // match a document with key value pairs (old syntax with outer braces - for backward compat)
    ({ $($key:tt : $value:tt),* $(,)? }) => {
        $crate::doc!($($key : $value),*)
    };

    // match a document with key value pairs (new syntax without outer braces)
    ($($key:tt : $value:tt),* $(,)?) => {
        {
            #[allow(unused_imports)]
            use $crate::doc_value;

            let mut doc = $crate::collection::Document::new();
            $(
                doc.put(&$crate::collection::normalize(stringify!($key)), $crate::doc_value!($value))
                .expect(&format!("Failed to put value {} in document", stringify!($value)));
            )*
            doc
        }
    };
}

/// Helper macro to convert values for the doc! macro.
/// Handles nested documents, arrays, and expressions.
#[macro_export]
macro_rules! doc_value {
    // match a nested document
    ({ $($key:tt : $value:tt),* $(,)? }) => {
        {
            $crate::common::Value::Document($crate::doc!{ $($key : $value),* })
        }
    };

    // match an array of values
    ([ $($value:tt),* $(,)? ]) => {
        $crate::common::Value::Array(vec![$($crate::doc_value!($value)),*])
    };

    // match an expression (variable, function call, arithmetic in parens, literals, etc.)
    ($value:expr) => {
        $crate::common::Value::from($value)
    };
}

#[cfg(test)]
mod tests {
    use std::string;

    use super::*;
    use crate::collection::Document;
    use crate::common::Value::Null;
    use crate::{create_document, document_from_map, empty_document};

    fn set_up() -> Document {
        doc!{
            score: 1034,
            location: {
                state: "NY",
                city: "New York",
                address: {
                    line1: "40",
                    line2: "ABC Street",
                    house: ["1", "2", "3"],
                    zip: 10001,
                },
            },
            category: ["food", "produce", "grocery"],
            obj_array: [
                {
                    value: 1,
                },
                {
                    value: 2,
                },
            ]
        }
    }

    #[test]
    fn test_normalize() {
        let mut value = "\"ABC\"".to_string();
        let result = normalize(&mut value);
        assert_eq!(result, "ABC");

        let mut value = "ABC".to_string();
        let result = normalize(&mut value);
        assert_eq!(result, "ABC");
    }

    #[test]
    fn test_empty_document() {
        let doc = empty_document();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_new() {
        let doc = Document::new();
        assert!(doc.is_empty());
    }

    #[test]
    fn test_document_from_map() {
        let mut map = BTreeMap::new();
        map.insert("key1".to_string(), Value::I32(1));
        map.insert("key2".to_string(), Value::String("value".to_string()));
        map.insert(
            "key3".to_string(),
            Value::Array(vec![Value::I32(1), Value::I32(2)]),
        );
        map.insert("key4".to_string(), Value::Document(Document::new()));

        let doc = document_from_map(&map).unwrap();
        assert_eq!(doc.size(), 4);
    }

    #[test]
    fn test_create_document() {
        let doc = create_document("key", Value::I32(1)).unwrap();
        assert_eq!(doc.size(), 1);
    }

    #[test]
    fn test_is_empty() {
        let doc = empty_document();
        assert!(doc.is_empty());

        let doc = set_up();
        assert!(!doc.is_empty());
    }

    #[test]
    fn test_get() {
        let doc = set_up();
        let mut value = doc.get("").unwrap();
        assert_eq!(value, Null);
        value = doc.get("score").unwrap();
        assert_eq!(value, Value::I32(1034));
        value = doc.get("location.state").unwrap();
        assert_eq!(value, Value::String("NY".to_string()));
        value = doc.get("location.address").unwrap();
        assert_eq!(
            value,
            Value::Document(doc!{
                line1: "40",
                line2: "ABC Street",
                house: ["1", "2", "3"],
                zip: 10001,
            })
        );
        value = doc.get("location.address.line1").unwrap();
        assert_eq!(value, Value::String("40".to_string()));
        value = doc.get("location.address.line2").unwrap();
        assert_eq!(value, Value::String("ABC Street".to_string()));
        value = doc.get("location.address.house").unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::String("1".to_string()),
                Value::String("2".to_string()),
                Value::String("3".to_string())
            ])
        );
        value = doc.get("location.address.house.0").unwrap();
        assert_eq!(value, Value::String("1".to_string()));
        value = doc.get("location.address.house.1").unwrap();
        assert_eq!(value, Value::String("2".to_string()));
        value = doc.get("location.address.house.2").unwrap();
        assert_eq!(value, Value::String("3".to_string()));
        value = doc.get("location.address.zip").unwrap();
        assert_eq!(value, Value::I32(10001));

        value = doc.get("category").unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::String("food".to_string()),
                Value::String("produce".to_string()),
                Value::String("grocery".to_string())
            ])
        );
        value = doc.get("category.0").unwrap();
        assert_eq!(value, Value::String("food".to_string()));
        value = doc.get("category.1").unwrap();
        assert_eq!(value, Value::String("produce".to_string()));
        value = doc.get("category.2").unwrap();
        assert_eq!(value, Value::String("grocery".to_string()));

        value = doc.get("obj_array").unwrap();
        assert_eq!(
            value,
            Value::Array(vec![
                Value::Document(doc!{ value: 1 }),
                Value::Document(doc!{ value: 2 }),
            ])
        );
        value = doc.get("obj_array.0").unwrap();
        assert_eq!(value, Value::Document(doc!{ value: 1 }));
        value = doc.get("obj_array.1").unwrap();
        assert_eq!(value, Value::Document(doc!{ value: 2 }));
        value = doc.get("obj_array.0.value").unwrap();
        assert_eq!(value, Value::I32(1));
        value = doc.get("obj_array.1.value").unwrap();
        assert_eq!(value, Value::I32(2));

        value = doc.get("location.address.test").unwrap();
        assert_eq!(value, Null);
        assert_eq!(doc.get("location.address.house.3").is_err(), true);
        assert_eq!(doc.get("location.address.house.-1").is_err(), true);
        assert_eq!(doc.get(".").is_err(), true);
        assert_eq!(doc.get("..").is_err(), true);
        assert_eq!(doc.get("score.test").unwrap(), Null);
    }

    #[test]
    fn test_put_null() {
        let mut doc = empty_document();
        doc.put("key", Null).unwrap();
        assert_eq!(doc.size(), 1);
        assert_eq!(doc.get("key").unwrap(), Null);
    }

    #[test]
    fn test_put_and_get() {
        let mut doc = Document::new();
        doc.put("key", Value::I32(1)).unwrap();
        assert_eq!(doc.get("key").unwrap(), Value::I32(1));
    }

    #[test]
    fn test_put_empty_key() {
        let mut doc = Document::new();
        let result = doc.put("", Value::I32(1));
        assert!(result.is_err());
    }

    #[test]
    fn test_put_reserved_id() {
        let mut doc = Document::new();
        let result = doc.put(DOC_ID, Value::String("id".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_put_id() {
        let mut doc = empty_document();
        let result = doc.put(DOC_ID, Value::String("id".to_string()));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_put_valid_nitrite_id() {
        let mut doc = empty_document();
        let result = doc.put(DOC_ID, Value::NitriteId(NitriteId::new()));
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_get_invalid_id() {
        // Test that document_from_map validates the _id field
        let mut map = BTreeMap::new();
        map.insert(DOC_ID.to_string(), Value::String("invalid_id".to_string()));
        
        let err = document_from_map(&map).is_err();
        assert_eq!(err, true);
    }

    #[test]
    fn test_get_non_existent_key() {
        let doc = Document::new();
        assert_eq!(doc.get("non_existent").unwrap(), Null);
    }

    #[test]
    fn test_invalid_get() {
        let key = "first.array.-1";
        let doc = doc!{
            first: {
                array: [1, 2, 3],
            },
        };
        let err = doc.get(key).is_err();
        assert_eq!(err, true);
    }

    #[test]
    fn test_id() {
        let mut doc = empty_document();
        let id = doc.id().unwrap();
        assert_eq!(NitriteId::valid_id(id.id_value()).unwrap(), true);
        assert_eq!(doc.has_id(), true);
    }

    #[test]
    fn test_fields() {
        let doc = doc!{
            key1: 1,
            key2: "value",
            key3: [1, 2, 3],
            key4: {
                key5: 5,
                key6: "value",
            },
        };
        let fields = doc.fields();
        assert_eq!(fields.len(), 5);
    }

    #[test]
    fn test_has_id() {
        let mut doc = empty_document();
        assert_eq!(doc.has_id(), false);
        doc.put(DOC_ID, Value::NitriteId(NitriteId::new())).unwrap();
        assert_eq!(doc.has_id(), true);
    }

    #[test]
    fn test_contains_key() {
        let doc = set_up();
        assert!(doc.contains_key("score"));
        assert!(!doc.contains_key("non_existent"));
    }

    #[test]
    fn test_contains_field() {
        let doc = set_up();
        assert!(doc.contains_field("location.state"));
        assert!(!doc.contains_field("location.country"));
    }

    #[test]
    fn test_remove() {
        let mut doc = empty_document();
        doc.put("key", Value::I32(1)).unwrap();
        assert_eq!(doc.size(), 1);
        doc.remove("key").unwrap();
        assert_eq!(doc.size(), 0);
    }

    #[test]
    fn test_size() {
        let doc = set_up();
        assert_eq!(doc.size(), 4);
    }

    #[test]
    fn test_revision() {
        let mut doc = Document::new();
        doc.put(DOC_REVISION, Value::I32(1)).unwrap();
        assert_eq!(doc.revision().unwrap(), 1);
    }

    #[test]
    fn test_source() {
        let mut doc = Document::new();
        doc.put(DOC_SOURCE, Value::String("source".to_string())).unwrap();
        assert_eq!(doc.source().unwrap(), "source");
    }

    #[test]
    fn test_last_modified_since_epoch() {
        let mut doc = Document::new();
        doc.put(DOC_MODIFIED, Value::I64(123456789)).unwrap();
        assert_eq!(doc.last_modified_since_epoch().unwrap(), 123456789);
    }

    #[test]
    fn test_to_map() {
        let doc = set_up();
        let map = doc.to_map();
        assert_eq!(map.len(), 4);
    }

    #[test]
    fn test_iter() {
        let doc = doc!{
            key1: "value1",
            key2: 2,
        };

        let mut iter = doc.iter();
        let (key, value) = iter.next().unwrap();
        assert_eq!(key, "key1");
        assert_eq!(value, Value::String("value1".to_string()));

        let (key, value) = iter.next().unwrap();
        assert_eq!(key, "key2");
        assert_eq!(value, Value::I32(2));
    }

    #[test]
    fn test_get_fields() {
        let doc = set_up();
        let fields = doc.fields();
        assert_eq!(fields.len(), 9);

        assert_eq!(fields.contains(&"score".to_string()), true);
        assert_eq!(fields.contains(&"location.state".to_string()), true);
        assert_eq!(fields.contains(&"location.city".to_string()), true);
        assert_eq!(fields.contains(&"location.address.line1".to_string()), true);
        assert_eq!(fields.contains(&"location.address.line2".to_string()), true);
        assert_eq!(fields.contains(&"location.address.house".to_string()), true);
        assert_eq!(fields.contains(&"location.address.zip".to_string()), true);
        assert_eq!(fields.contains(&"category".to_string()), true);
        assert_eq!(fields.contains(&"obj_array".to_string()), true);
    }

    #[test]
    fn test_get_embedded_array_fields() {
        let doc = doc!{
            first: "value",
            second: ["1", "2"],
            third: Null,
            fourth: {
                first: "value",
                second: ["1", "2"],
                third: {
                    first: [1, 2],
                    second: "other",
                },
            },
            fifth: [
                {
                    first: "value",
                    second: [1, 2, 3],
                    third: {
                        first: "value",
                        second: [1, 2],
                    },
                    fourth: [
                        {
                            first: "value",
                            second: [1, 2],
                        },
                        {
                            first: "value",
                            second: [1, 2],
                        },
                    ],
                },
                {
                    first: "value",
                    second: [3, 4, 5],
                    third: {
                        first: "value",
                        second: [1, 2],
                    },
                    fourth: [
                        {
                            first: "value",
                            second: [1, 2],
                        },
                        {
                            first: "value",
                            second: [1, 2],
                        },
                    ],
                },
                {
                    first: "value",
                    second: [5, 6, 7],
                    third: {
                        first: "value",
                        second: [1, 2],
                    },
                    fourth: [
                        {
                            first: "value",
                            second: [1, 2],
                        },
                        {
                            first: "value",
                            second: [3, 4],
                        },
                    ],
                },
            ]
        };

        let val = doc.get("fifth.second").unwrap();
        let list = val.as_array().unwrap();
        assert_eq!(list.len(), 7);

        let val = doc.get("fifth.fourth.second").unwrap();
        let list = val.as_array().unwrap();
        assert_eq!(list.len(), 4);

        let val = doc.get("fourth.third.second").unwrap();
        assert_eq!(val, Value::String("other".to_string()));

        let val = doc.get("fifth.0.second.0").unwrap();
        assert_eq!(val, Value::I32(1));

        let val = doc.get("fifth.1.fourth.0.second.1").unwrap();
        assert_eq!(val, Value::I32(2));
    }

    #[test]
    fn test_deep_put() {
        let mut doc = set_up();
        doc.put("location.address.pin", Value::I32(700037)).unwrap();
        assert_eq!(doc.get("location.address.pin").unwrap(), Value::I32(700037));

        doc.put("location.address.business.pin", Value::I32(700037))
            .unwrap();
        assert_eq!(
            doc.get("location.address.business.pin").unwrap(),
            Value::I32(700037)
        );
    }

    #[test]
    fn test_deep_remove() {
        let mut doc = set_up();
        doc.remove("location.address.zip").unwrap();
        assert_eq!(doc.get("location.address.zip").unwrap(), Null);

        doc.remove("location.address.line1").unwrap();
        assert_eq!(doc.get("location.address.line1").unwrap(), Null);

        doc.remove("location.address.line2").unwrap();
        assert_eq!(doc.get("location.address.line2").unwrap(), Null);

        doc.remove("location.address.house").unwrap();
        assert_eq!(doc.get("location.address.house").unwrap(), Null);

        doc.remove("location.address").unwrap();
        assert_eq!(doc.get("location.address").unwrap(), Null);
    }

    #[test]
    fn test_deep_get() {
        let doc = set_up();
        let value = doc.get("location.address.line1").unwrap();
        assert_eq!(value, Value::String("40".to_string()));
    }

    #[test]
    fn test_recursive_get() {
        let doc = set_up();
        let value = doc.get("location.address.house.0").unwrap();
        assert_eq!(value, Value::String("1".to_string()));
    }

    #[test]
    fn test_decompose() {
        let doc = doc!{
            some_key: [{key: 1}, {key: 2}, {key: 3}],
        };

        let value = doc.get("some_key").unwrap();
        let array = value.as_array().unwrap();
        let decomposed = doc.decompose(array, &["key"]).unwrap();
        assert_eq!(decomposed, Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]));
    }

    #[test]
    fn test_deep_put_invalid_field() {
        let mut doc = empty_document();
        let result = doc.put("..invalid..field", Value::I32(1));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_deep_remove_invalid_field() {
        let mut doc = empty_document();
        let result = doc.remove("..invalid..field");
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_merge_documents() {
        let mut doc1 = doc!{
            "key1": "value1",
            "nested": {
                "key2": "value2",
            },
        };

        let doc2 = doc!{
            "key3": "value3",
            "nested": {
                "key4": "value4",
            },
        };

        doc1.merge(&doc2).unwrap();
        assert_eq!(doc1.size(), 3);
        assert_eq!(
            doc1.get("key1").unwrap(),
            Value::String("value1".to_string())
        );
        assert_eq!(
            doc1.get("key3").unwrap(),
            Value::String("value3".to_string())
        );
        assert_eq!(
            doc1.get("nested.key2").unwrap(),
            Value::String("value2".to_string())
        );
        assert_eq!(
            doc1.get("nested.key4").unwrap(),
            Value::String("value4".to_string())
        );
    }

    #[test]
    fn test_display() {
        let doc = doc!{
            key1: "value1",
            key2: 2,
        };

        let display = format!("{}", doc);
        assert!(display.contains("\"key1\": \"value1\""));
        assert!(display.contains("\"key2\": 2"));
    }

    #[test]
    fn test_debug() {
        let doc = doc!{
            key1: "value1",
            key2: 2,
        };

        let debug = format!("{:?}", doc);
        assert!(debug.contains("\"key1\": string(\"value1\")"));
        assert!(debug.contains("\"key2\": i32(2)"));
    }

    #[test]
    fn test_put_invalid_id() {
        let mut doc = empty_document();
        let result = doc.put(DOC_ID, Value::String("invalid_id".to_string()));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_get_invalid_key() {
        let doc = empty_document();
        let result = doc.get("invalid.key");
        assert_eq!(result.is_ok(), true);
        assert_eq!(result.unwrap(), Null);
    }

    #[test]
    fn test_remove_invalid_key() {
        let mut doc = empty_document();
        let result = doc.remove("invalid.key");
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_merge_empty_document() {
        let mut doc1 = empty_document();
        let doc2 = empty_document();
        doc1.merge(&doc2).unwrap();
        assert_eq!(doc1.size(), 0);
    }

    #[test]
    fn test_get_invalid_array_index() {
        let doc = doc!{
            key: [1, 2, 3],
        };

        let result = doc.get("key.-1");
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_remove_invalid_array_index() {
        let mut doc = doc!{
            key: [1, 2, 3],
        };

        let result = doc.remove("key.-1");
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_merge_conflicting_keys() {
        let mut doc1 = doc!{
            key1: "value1",
            key2: "value2",
        };

        let doc2 = doc!{
            key2: "value3",
            key3: "value4",
        };

        doc1.merge(&doc2).unwrap();
        assert_eq!(doc1.size(), 3);
        assert_eq!(
            doc1.get("key1").unwrap(),
            Value::String("value1".to_string())
        );
        assert_eq!(
            doc1.get("key2").unwrap(),
            Value::String("value3".to_string())
        );
        assert_eq!(
            doc1.get("key3").unwrap(),
            Value::String("value4".to_string())
        );
    }

    #[test]
    fn test_deep_put_invalid_path() {
        let mut doc = empty_document();
        let result = doc.put("key..key", Value::I32(1));
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_deep_get_invalid_path() {
        let doc = set_up();
        let result = doc.get("location..key");
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn contains_field_with_existing_field() {
        let doc = set_up();
        assert_eq!(doc.contains_field("location.state"), true);
    }

    #[test]
    fn contains_field_with_non_existing_field() {
        let doc = set_up();
        assert_eq!(doc.contains_field("location.country"), false);
    }

    #[test]
    fn revision_with_existing_revision() {
        let mut doc = empty_document();
        doc.put(DOC_REVISION, Value::I32(1)).unwrap();
        assert_eq!(doc.revision().unwrap(), 1);
    }

    #[test]
    fn revision_with_non_existing_revision() {
        let doc = empty_document();
        assert_eq!(doc.revision().unwrap(), 0);
    }

    #[test]
    fn source_with_existing_source() {
        let mut doc = empty_document();
        doc.put(DOC_SOURCE, Value::String("source".to_string()))
            .unwrap();
        assert_eq!(doc.source().unwrap(), "source");
    }

    #[test]
    fn source_with_non_existing_source() {
        let doc = empty_document();
        assert_eq!(doc.source().unwrap(), "");
    }

    #[test]
    fn last_modified_since_epoch_with_existing_modified() {
        let mut doc = empty_document();
        doc.put(DOC_MODIFIED, Value::I64(123456789)).unwrap();
        assert_eq!(doc.last_modified_since_epoch().unwrap(), 123456789);
    }

    #[test]
    fn last_modified_since_epoch_with_non_existing_modified() {
        let doc = empty_document();
        assert_eq!(doc.last_modified_since_epoch().unwrap(), 0);
    }

    #[test]
    fn test_macro() {
        let string_key = "c".to_string();
        let document = doc!{
            s: string_key,
            a: 12,
            b: "c",
            "d": [1, 2, 3],
            e: ["f", "g"],
            h: {
                i: 10,
                j: [1, 2, 3],
            },
            k: [
                {
                    l: 15,
                    m: [10, 15],
                    "n": {
                        o: 1.25,
                    },
                },
                {
                    p: 15,
                    q: [
                        {
                            r: "45",
                            s: 1569,
                        },
                        {
                            t: 20,
                            u: 1.25,
                        },
                    ],
                }
            ],
            u: [
                ["v", "w"],
                ["x", "y", "z"],
            ],
            v: {
                w: {
                    x: {
                        y: {
                            z: true,
                        }
                    }
                }
            }
        };

        let mut doc2 = empty_document();
        doc2.put("s", Value::String("c".to_string())).unwrap();
        doc2.put("a", Value::I32(12)).unwrap();
        doc2.put("b", Value::String("c".to_string())).unwrap();
        doc2.put(
            "d",
            Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)]),
        )
            .unwrap();
        doc2.put(
            "e",
            Value::Array(vec![
                Value::String("f".to_string()),
                Value::String("g".to_string()),
            ]),
        )
            .unwrap();
        doc2.put("h", Value::Document(doc!{ i: 10, j: [1, 2, 3] }))
            .unwrap();
        doc2.put(
            "k",
            Value::Array(vec![
                Value::Document(doc!{ l: 15, m: [10, 15], "n": { o: 1.25 } }),
                Value::Document(doc!{ p: 15, q: [
                { r: "45", s: 1569 },
                { t: 20, u: 1.25 },
            ] }),
            ]),
        )
            .unwrap();

        doc2.put(
            "u",
            Value::Array(vec![
                Value::Array(vec![
                    Value::String("v".to_string()),
                    Value::String("w".to_string()),
                ]),
                Value::Array(vec![
                    Value::String("x".to_string()),
                    Value::String("y".to_string()),
                    Value::String("z".to_string()),
                ]),
            ]),
        )
            .unwrap();

        doc2.put("v", Value::Document(doc!{ w: { x: { y: { z: true } } } }))
            .unwrap();

        assert_eq!(document, doc2);
    }

    #[test]
    fn test_empty_document_macro() {
        let doc = doc!{};
        assert_eq!(doc.is_empty(), true);
    }

    #[test]
    fn test_document_with_key_value_string() {
        let doc = doc!{
            key: "value",
        };
        assert_eq!(doc.size(), 1);
        assert_eq!(doc.get("key").unwrap(), Value::String("value".to_string()));
    }

    #[test]
    fn test_nested_document_macro() {
        let doc = doc!{
            key1: "value1",
            key2: {
                key3: "value3",
            },
        };
        assert_eq!(doc.size(), 2);
        assert_eq!(
            doc.get("key1").unwrap(),
            Value::String("value1".to_string())
        );
        assert_eq!(
            doc.get("key2.key3").unwrap(),
            Value::String("value3".to_string())
        );
    }

    #[test]
    fn test_array_in_document_macro() {
        let doc = doc!{
            key1: "value1",
            key2: [1, 2, 3],
        };
        assert_eq!(doc.size(), 2);
        assert_eq!(
            doc.get("key1").unwrap(),
            Value::String("value1".to_string())
        );
        assert_eq!(
            doc.get("key2").unwrap(),
            Value::Array(vec![Value::I32(1), Value::I32(2), Value::I32(3)])
        );
    }

    #[test]
    fn test_complex_document_macro() {
        let document = doc!{
            key1: "value1",
            key2: {
                nested_key1: "nested_value",
                nested_key2: [10, 20, 30]
            },
            key3: [true, false, {
                "deep_nested_key": "deep_value"
            }]
        };

        assert_eq!(document.size(), 3);
        assert_eq!(
            document.get("key1").unwrap(),
            Value::String("value1".to_string())
        );

        let binding = document.get("key2").unwrap();
        let nested_doc = binding.as_document().unwrap();
        assert_eq!(nested_doc.size(), 2);
        assert_eq!(
            nested_doc.get("nested_key1").unwrap(),
            Value::String("nested_value".to_string())
        );

        let nested_array = nested_doc.get("nested_key2").unwrap();
        let array = nested_array.as_array().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array[0], Value::I32(10));
        assert_eq!(array[1], Value::I32(20));
        assert_eq!(array[2], Value::I32(30));

        let binding = document.get("key3").unwrap();
        let array = binding.as_array().unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array[0], Value::Bool(true));
        assert_eq!(array[1], Value::Bool(false));

        let binding = &array[2];
        let nested_doc = binding.as_document().unwrap();
        assert_eq!(nested_doc.size(), 1);
        assert_eq!(
            nested_doc.get("deep_nested_key").unwrap(),
            Value::String("deep_value".to_string())
        );
    }

    // Tests for recursive_get with idiomatic pattern matching
    #[test]
    fn test_recursive_get_null_value() {
        let doc = Document::new();
        let result = doc.recursive_get(None, &[]);
        assert_eq!(result.unwrap(), Null);
    }

    #[test]
    fn test_recursive_get_simple_value() {
        let doc = doc!{
            "name": "Alice",
            "age": 25
        };

        let value = doc.get("name").unwrap();
        assert_eq!(value, Value::String("Alice".to_string()));

        let value = doc.get("age").unwrap();
        assert_eq!(value, Value::I32(25));
    }

    #[test]
    fn test_recursive_get_nested_document() {
        let doc = doc!{
            "user": {
                "profile": {
                    "name": "Bob",
                    "age": 30
                }
            }
        };

        let value = doc.get("user").unwrap();
        assert!(matches!(value, Value::Document(_)));

        let value = doc.get("user.profile").unwrap();
        assert!(matches!(value, Value::Document(_)));

        let value = doc.get("user.profile.name").unwrap();
        assert_eq!(value, Value::String("Bob".to_string()));
    }

    #[test]
    fn test_recursive_get_array_index() {
        let doc = doc!{
            "items": [
                {"name": "Item 1"},
                {"name": "Item 2"},
                {"name": "Item 3"}
            ]
        };

        let value = doc.get("items.0").unwrap();
        assert!(matches!(value, Value::Document(_)));

        let value = doc.get("items.0.name").unwrap();
        assert_eq!(value, Value::String("Item 1".to_string()));

        let value = doc.get("items.2.name").unwrap();
        assert_eq!(value, Value::String("Item 3".to_string()));
    }

    #[test]
    fn test_recursive_get_nonexistent_path() {
        let doc = doc!{
            "name": "Charlie",
            "age": 35
        };

        let value = doc.get("nonexistent").unwrap();
        assert_eq!(value, Null);

        let value = doc.get("name.nested").unwrap();
        assert_eq!(value, Null);
    }

    #[test]
    fn test_recursive_get_deep_nesting() {
        let doc = doc!{
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        };

        let value = doc.get("level1.level2.level3.level4.value").unwrap();
        assert_eq!(value, Value::String("deep".to_string()));
    }

    #[test]
    fn test_recursive_get_mixed_types() {
        let doc = doc!{
            "name": "David",
            "age": 28,
            "tags": ["tag1", "tag2"],
            "meta": {
                "created": "2025-01-01"
            }
        };

        assert_eq!(doc.get("name").unwrap(), Value::String("David".to_string()));
        assert_eq!(doc.get("age").unwrap(), Value::I32(28));
        assert!(matches!(doc.get("tags").unwrap(), Value::Array(_)));
        assert!(matches!(doc.get("meta").unwrap(), Value::Document(_)));
    }

    #[test]
    fn bench_get_top_level_key() {
        let doc = doc!{
            key1: "value1",
            key2: "value2",
            key3: "value3",
            key4: "value4",
            key5: "value5",
        };

        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let _ = doc.get("key1");
        }
        let elapsed = start.elapsed();
        println!("10000 top-level get calls: {:?}", elapsed);
        assert!(elapsed.as_millis() < 500);
    }

    #[test]
    fn bench_get_embedded_key() {
        let doc = doc!{
            level1: {
                level2: {
                    level3: "value"
                }
            }
        };

        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = doc.get("level1.level2.level3");
        }
        let elapsed = start.elapsed();
        println!("1000 embedded get calls: {:?}", elapsed);
        assert!(elapsed.as_millis() < 200);
    }

    #[test]
    fn bench_put_operations() {
        let start = std::time::Instant::now();
        for i in 0..1000 {
            let mut doc = Document::new();
            doc.put(&format!("key{}", i), Value::I32(i as i32)).ok();
        }
        let elapsed = start.elapsed();
        println!("1000 put operations: {:?}", elapsed);
        assert!(elapsed.as_millis() < 300);
    }

    #[test]
    fn bench_merge_documents() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let mut doc1 = doc!{
                key1: "value1",
                nested: {
                    key2: "value2",
                }
            };
            let doc2 = doc!{
                key3: "value3",
                nested: {
                    key4: "value4",
                }
            };
            doc1.merge(&doc2).ok();
        }
        let elapsed = start.elapsed();
        println!("100 merge operations: {:?}", elapsed);
        assert!(elapsed.as_millis() < 200);
    }

    #[test]
    fn bench_fields_collection() {
        let doc = doc!{
            key1: "value1",
            key2: 2,
            nested: {
                key3: "value3",
                deep: {
                    key4: "value4"
                }
            },
            array: [1, 2, 3]
        };

        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _ = doc.fields();
        }
        let elapsed = start.elapsed();
        println!("1000 fields() calls: {:?}", elapsed);
        assert!(elapsed.as_millis() < 300);
    }

    // Tests for expression support in doc! macro
    #[test]
    fn test_doc_macro_with_variables() {
        let name = "Alice";
        let age = 30;
        let city = String::from("NYC");

        let doc = doc!{
            name: name,
            age: age,
            city: city
        };

        assert_eq!(doc.get("name").unwrap(), Value::String("Alice".to_string()));
        assert_eq!(doc.get("age").unwrap(), Value::I32(30));
        assert_eq!(doc.get("city").unwrap(), Value::String("NYC".to_string()));
    }

    #[test]
    fn test_doc_macro_with_expressions() {
        let base = 100;
        let multiplier = 2;

        let doc = doc!{
            computed: (base * multiplier),
            sum: (10 + 20 + 30),
            concat: (format!("Hello {}", "World"))
        };

        assert_eq!(doc.get("computed").unwrap(), Value::I32(200));
        assert_eq!(doc.get("sum").unwrap(), Value::I32(60));
        assert_eq!(doc.get("concat").unwrap(), Value::String("Hello World".to_string()));
    }

    #[test]
    fn test_doc_macro_with_function_calls() {
        fn get_name() -> &'static str {
            "Bob"
        }

        fn calculate_score() -> i32 {
            42
        }

        let doc = doc!{
            name: (get_name()),
            score: (calculate_score()),
            length: ("hello".len())
        };

        assert_eq!(doc.get("name").unwrap(), Value::String("Bob".to_string()));
        assert_eq!(doc.get("score").unwrap(), Value::I32(42));
        assert_eq!(doc.get("length").unwrap(), Value::U64(5));
    }

    #[test]
    fn test_doc_macro_with_mixed_expressions_and_nested() {
        let user_name = "Charlie";
        let base_score = 50;

        let doc = doc!{
            user: {
                name: user_name,
                score: (base_score * 2)
            },
            tags: ["admin", user_name],
            values: [1, (2 + 3), 6]
        };

        assert_eq!(doc.get("user.name").unwrap(), Value::String("Charlie".to_string()));
        assert_eq!(doc.get("user.score").unwrap(), Value::I32(100));
        
        let tags = doc.get("tags").unwrap();
        let tags_arr = tags.as_array().unwrap();
        assert_eq!(tags_arr[0], Value::String("admin".to_string()));
        assert_eq!(tags_arr[1], Value::String("Charlie".to_string()));

        let values = doc.get("values").unwrap();
        let values_arr = values.as_array().unwrap();
        assert_eq!(values_arr[0], Value::I32(1));
        assert_eq!(values_arr[1], Value::I32(5));
        assert_eq!(values_arr[2], Value::I32(6));
    }

    #[test]
    fn test_doc_macro_empty_new_syntax() {
        let doc = doc!{};
        assert!(doc.is_empty());
    }

    // Additional tests for coverage improvement
    
    #[test]
    fn test_merge_nested_document_in_place() {
        // Test the path where nested document merge happens in place
        let mut doc1 = doc!{
            nested: {
                key1: "value1",
                inner: {
                    a: 1
                }
            }
        };

        let doc2 = doc!{
            nested: {
                key2: "value2",
                inner: {
                    b: 2
                }
            }
        };

        doc1.merge(&doc2).unwrap();
        
        // Verify the merge happened correctly
        assert_eq!(doc1.get("nested.key1").unwrap(), Value::String("value1".to_string()));
        assert_eq!(doc1.get("nested.key2").unwrap(), Value::String("value2".to_string()));
        assert_eq!(doc1.get("nested.inner.a").unwrap(), Value::I32(1));
        assert_eq!(doc1.get("nested.inner.b").unwrap(), Value::I32(2));
    }

    #[test]
    fn test_merge_document_overwrites_non_document() {
        // Test when trying to merge a document value into a non-document field
        let mut doc1 = doc!{
            field: "string_value"
        };

        let doc2 = doc!{
            field: {
                nested: "value"
            }
        };

        doc1.merge(&doc2).unwrap();
        
        // The document should overwrite the string
        assert_eq!(doc1.get("field.nested").unwrap(), Value::String("value".to_string()));
    }

    #[test]
    fn test_fields_with_prefix() {
        // Test get_fields_internal with a prefix (nested document fields)
        let doc = doc!{
            level1: {
                level2: {
                    leaf: "value"
                }
            }
        };

        let fields = doc.fields();
        assert!(fields.contains(&"level1.level2.leaf".to_string()));
    }

    #[test]
    fn test_iterator_size_hint() {
        let doc = doc!{
            key1: "value1",
            key2: "value2",
            key3: "value3"
        };

        let iter = doc.iter();
        let (lower, upper) = iter.size_hint();
        
        assert_eq!(lower, 3);
        assert_eq!(upper, Some(3));
    }

    #[test]
    fn test_remove_array_element_by_index() {
        let mut doc = doc!{
            items: [1, 2, 3, 4, 5]
        };

        // Remove element at index 2
        doc.remove("items.2").unwrap();
        
        let items = doc.get("items").unwrap();
        let arr = items.as_array().unwrap();
        assert_eq!(arr.len(), 4);
        assert_eq!(arr[0], Value::I32(1));
        assert_eq!(arr[1], Value::I32(2));
        assert_eq!(arr[2], Value::I32(4)); // Previously index 3
        assert_eq!(arr[3], Value::I32(5)); // Previously index 4
    }

    #[test]
    fn test_remove_nested_document_in_array() {
        let mut doc = doc!{
            items: [
                { name: "item1", value: 1 },
                { name: "item2", value: 2 },
                { name: "item3", value: 3 }
            ]
        };

        // Remove a field from nested document in array
        doc.remove("items.1.value").unwrap();
        
        // Check the nested document still exists but without the removed field
        let item1 = doc.get("items.1").unwrap();
        let item_doc = item1.as_document().unwrap();
        assert_eq!(item_doc.size(), 1);
        assert_eq!(item_doc.get("name").unwrap(), Value::String("item2".to_string()));
        assert_eq!(item_doc.get("value").unwrap(), Null);
    }

    #[test]
    fn test_remove_entire_nested_document_in_array_when_empty() {
        let mut doc = doc!{
            items: [
                { only_field: "value" }
            ]
        };

        // Remove the only field, which should remove the document from array
        doc.remove("items.0.only_field").unwrap();
        
        // The array should now be empty since the only document became empty
        let items = doc.get("items").unwrap();
        let arr = items.as_array().unwrap();
        assert_eq!(arr.len(), 0);
    }

    #[test]
    fn test_remove_array_out_of_bounds() {
        let mut doc = doc!{
            items: [1, 2, 3]
        };

        // Try to remove at out of bounds index
        let result = doc.remove("items.10");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_array_negative_index() {
        let mut doc = doc!{
            items: [1, 2, 3]
        };

        // Try to remove at negative index
        let result = doc.remove("items.-1");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_array_invalid_index_string() {
        let mut doc = doc!{
            items: [1, 2, 3]
        };

        // Try to remove with non-numeric index
        let result = doc.remove("items.abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_non_existent_nested_path() {
        let mut doc = doc!{
            existing: "value"
        };

        // Removing a non-existent path should succeed silently
        let result = doc.remove("non_existent.nested.path");
        assert!(result.is_ok());
    }

    #[test]
    fn test_deep_put_creates_nested_structure() {
        let mut doc = doc!{};
        
        // Put a deeply nested value - should create intermediate documents
        doc.put("a.b.c.d", Value::String("deep".to_string())).unwrap();
        
        assert_eq!(doc.get("a.b.c.d").unwrap(), Value::String("deep".to_string()));
    }

    #[test]
    fn test_deep_put_into_existing_nested_document() {
        let mut doc = doc!{
            existing: {
                nested: {
                    key1: "value1"
                }
            }
        };
        
        // Put into existing nested structure
        doc.put("existing.nested.key2", Value::String("value2".to_string())).unwrap();
        
        assert_eq!(doc.get("existing.nested.key1").unwrap(), Value::String("value1".to_string()));
        assert_eq!(doc.get("existing.nested.key2").unwrap(), Value::String("value2".to_string()));
    }

    #[test]
    fn test_fields_skips_reserved_fields() {
        let mut doc = doc!{
            regular_field: "value"
        };
        
        // Manually add reserved fields using put
        doc.put(DOC_ID, Value::NitriteId(NitriteId::new())).unwrap();
        doc.put(DOC_REVISION, Value::I32(1)).unwrap();
        doc.put(DOC_SOURCE, Value::String("source".to_string())).unwrap();
        doc.put(DOC_MODIFIED, Value::I64(12345)).unwrap();
        
        let fields = doc.fields();
        
        // Only the regular field should be in the fields list
        assert_eq!(fields.len(), 1);
        assert!(fields.contains(&"regular_field".to_string()));
        assert!(!fields.contains(&DOC_ID.to_string()));
        assert!(!fields.contains(&DOC_REVISION.to_string()));
    }

    #[test]
    fn test_fields_skips_empty_keys() {
        let doc = doc!{
            valid_key: "value"
        };
        
        // Note: We cannot directly add empty keys through put() as it validates against empty keys
        // The empty key validation in put() prevents this, which is correct behavior
        
        let fields = doc.fields();
        
        // Only the valid key should be in the fields list
        assert_eq!(fields.len(), 1);
        assert!(fields.contains(&"valid_key".to_string()));
    }

    #[test]
    fn test_to_pretty_json_empty() {
        let doc = doc!{};
        let json = doc.to_pretty_json(0);
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_to_debug_string_empty() {
        let doc = doc!{};
        let debug = doc.to_debug_string(0);
        assert_eq!(debug, "{}");
    }

    #[test]
    fn test_to_pretty_json_with_indent() {
        let doc = doc!{
            key: "value"
        };
        let json = doc.to_pretty_json(2);
        assert!(json.contains("    \"key\"")); // 4 spaces (2 + 2)
    }

    #[test]
    fn test_to_debug_string_with_indent() {
        let doc = doc!{
            key: "value"
        };
        let debug = doc.to_debug_string(2);
        assert!(debug.contains("    \"key\"")); // 4 spaces (2 + 2)
    }

    #[test]
    fn test_contains_field_embedded_path() {
        // Test the else branch of contains_field where key is not a direct key
        // but exists as an embedded field path
        let mut doc = doc!{
            outer: "temp"
        };
        
        // Create nested document
        let inner = doc!{
            inner: "value"
        };
        doc.put("outer", inner).unwrap();
        
        // Direct key check
        assert!(doc.contains_field("outer"));
        
        // Embedded field check (this exercises the else branch)
        assert!(doc.contains_field("outer.inner"));
        
        // Non-existent embedded field
        assert!(!doc.contains_field("outer.nonexistent"));
        assert!(!doc.contains_field("nonexistent.path"));
    }

    #[test]
    fn test_deep_get_non_embedded_key() {
        // Test deep_get when key is not embedded (doesn't contain separator)
        // This should hit the !is_embedded() branch returning Ok(Value::Null)
        let doc = doc!{
            name: "test"
        };
        
        // When we call get_by_embedded_key on a non-embedded key,
        // deep_get checks is_embedded first
        // But we need to call the internal deep_get method
        // The deep_get returns Null for non-embedded keys
        
        // Get a key that doesn't exist and isn't embedded
        let result = doc.get("nonexistent");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::Null);
    }

    
    #[cfg(test)]
    mod custom_separator_test {
        use super::*;
        use crate::nitrite_config::NitriteConfig;

        #[test]
        #[cfg_attr(not(feature = "custom_separator"), ignore)]
        fn custom_separator_test_remove_array() {
            NitriteConfig::new().set_field_separator(":").expect("Failed to set separator");
            let mut doc = set_up();
            assert_eq!(
                doc.get("location:address:house")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .len(),
                3
            );

            doc.remove("location:address:house:0").unwrap();
            assert_eq!(
                doc.get("location:address:house")
                    .unwrap()
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );

            assert_eq!(doc.get("obj_array").unwrap().as_array().unwrap().len(), 2);
            doc.remove("obj_array:0:value").unwrap();
            assert_eq!(doc.get("obj_array").unwrap().as_array().unwrap().len(), 1);
            assert_eq!(
                doc.get("obj_array:0")
                    .unwrap()
                    .as_document()
                    .unwrap()
                    .size(),
                1
            );
        }

        #[test]
        #[cfg_attr(not(feature = "custom_separator"), ignore)]
        fn custom_separator_test_remove() {
            NitriteConfig::new().set_field_separator(":").expect("Failed to set separator");
            let mut doc = set_up();
            assert_eq!(
                doc.get("location:address")
                    .unwrap()
                    .as_document()
                    .unwrap()
                    .size(),
                4
            );
            doc.remove("location:address:line1").unwrap();
            assert_eq!(
                doc.get("location:address")
                    .unwrap()
                    .as_document()
                    .unwrap()
                    .size(),
                3
            );
        }

        #[test]
        #[cfg_attr(not(feature = "custom_separator"), ignore)]
        fn custom_separator_test_get() {
            NitriteConfig::new().set_field_separator(":").expect("Failed to set separator");

            let doc = set_up();
            let mut value = doc.get("").unwrap();
            assert_eq!(value, Null);
            value = doc.get("score").unwrap();
            assert_eq!(value, Value::I32(1034));
            value = doc.get("location:state").unwrap();
            assert_eq!(value, Value::String("NY".to_string()));
            value = doc.get("location:address").unwrap();
            assert_eq!(
                value,
                Value::Document(doc!{
                    line1: "40",
                    line2: "ABC Street",
                    house: ["1", "2", "3"],
                    zip: 10001,
                })
            );
            value = doc.get("location:address:line1").unwrap();
            assert_eq!(value, Value::String("40".to_string()));
            value = doc.get("location:address:line2").unwrap();
            assert_eq!(value, Value::String("ABC Street".to_string()));
            value = doc.get("location:address:house:0").unwrap();
            assert_eq!(value, Value::String("1".to_string()));
            value = doc.get("location:address:house:1").unwrap();
            assert_eq!(value, Value::String("2".to_string()));
            value = doc.get("location:address:house:2").unwrap();
            assert_eq!(value, Value::String("3".to_string()));
            value = doc.get("location:address:zip").unwrap();
            assert_eq!(value, Value::I32(10001));

            value = doc.get("category:0").unwrap();
            assert_eq!(value, Value::String("food".to_string()));
            value = doc.get("category:1").unwrap();
            assert_eq!(value, Value::String("produce".to_string()));
            value = doc.get("category:2").unwrap();
            assert_eq!(value, Value::String("grocery".to_string()));

            value = doc.get("obj_array:0").unwrap();
            assert_eq!(value, Value::Document(doc!{ value: 1 }));
            value = doc.get("obj_array:1").unwrap();
            assert_eq!(value, Value::Document(doc!{ value: 2 }));
            value = doc.get("obj_array:0:value").unwrap();
            assert_eq!(value, Value::I32(1));
            value = doc.get("obj_array:1:value").unwrap();
            assert_eq!(value, Value::I32(2));

            value = doc.get("location:address:test").unwrap();
            assert_eq!(value, Null);
            assert_eq!(doc.get("location:address:house:3").is_err(), true);
            assert_eq!(doc.get("location:address:house:-1").is_err(), true);
            assert_eq!(doc.get(":").is_err(), true);
            assert_eq!(doc.get("::").is_err(), true);
            assert_eq!(doc.get("score:test").unwrap(), Null);
        }

        #[test]
        #[cfg_attr(not(feature = "custom_separator"), ignore)]
        fn custom_separator_test_default_separator_fails() {
            NitriteConfig::new().set_field_separator(":").expect("Failed to set separator");
            let doc = set_up();
            let value = doc.get("location.address.house.0").unwrap();
            assert_eq!(value, Null);

            let value = doc.get("location:address:house:0").unwrap();
            assert_eq!(value, Value::String("1".to_string()));
        }
    }
}
