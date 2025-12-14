use super::{operation::WriteResult, Document, FindOptions, NitriteId, UpdateOptions};
use crate::{
    errors::NitriteResult, filter::Filter, DocumentCursor
    , PersistentCollection,
};
use std::ops::Deref;
use std::sync::Arc;

/// Trait defining the interface for a document collection.
///
/// A collection is a container for documents in Nitrite. It provides methods for
/// inserting, updating, removing, and querying documents. Implementations handle
/// document indexing, validation, and persistence.
pub trait NitriteCollectionProvider: PersistentCollection {
    /// Inserts a single document into the collection.
    ///
    /// If the document doesn't have an `_id` field, Nitrite will generate a unique
    /// NitriteId for it. Returns information about the insertion operation.
    fn insert(&self, document: Document) -> NitriteResult<WriteResult>;

    /// Inserts multiple documents into the collection.
    ///
    /// This is more efficient than calling `insert()` multiple times for batch operations.
    fn insert_many(&self, documents: Vec<Document>) -> NitriteResult<WriteResult>;
    
    /// Updates documents matching a filter with the specified update document.
    ///
    /// This method updates all matching documents using default options.
    /// Use `update_with_options()` for more control.
    fn update(
        &self,
        filter: Filter,
        update: &Document,
    ) -> NitriteResult<WriteResult> {
        self.update_with_options(filter, update, &UpdateOptions::default())
    }

    /// Updates documents matching a filter with the specified update document and options.
    ///
    /// The options control whether to insert if absent, update just once, etc.
    fn update_with_options(
        &self,
        filter: Filter,
        update: &Document,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult>;

    /// Updates a single document (by object identity, not ID lookup).
    ///
    /// The document's `_id` field is used to locate the document in the collection.
    fn update_one(&self, document: &Document, insert_if_absent: bool)
        -> NitriteResult<WriteResult>;

    /// Updates a document directly by its NitriteId without filter-based lookup.
    ///
    /// This is an O(1) operation as it directly accesses the document by its key.
    /// More efficient than using a filter on the document ID.
    fn update_by_id(
        &self,
        id: &NitriteId,
        update: &Document,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult>;

    /// Removes documents matching a filter.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter to match documents
    /// * `just_once` - If true, remove only the first matching document
    fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult>;

    /// Removes a single document by its identity (using its `_id` field).
    fn remove_one(&self, document: &Document) -> NitriteResult<WriteResult>;

    /// Finds documents matching a filter.
    ///
    /// Returns a `DocumentCursor` for iterating over results.
    fn find(&self, filter: Filter) -> NitriteResult<DocumentCursor>;

    /// Finds documents matching a filter with additional options.
    ///
    /// Options include sorting, pagination (skip/limit), and distinctness.
    fn find_with_options(
        &self,
        filter: Filter,
        find_options: &FindOptions,
    ) -> NitriteResult<DocumentCursor>;

    /// Retrieves a document by its NitriteId.
    ///
    /// This is an O(1) operation.
    fn get_by_id(&self, id: &NitriteId) -> NitriteResult<Option<Document>>;

    /// Returns the name of this collection.
    fn name(&self) -> String;
}

/// A document collection in a Nitrite database.
///
/// `NitriteCollection` provides access to document operations on a named collection.
/// Documents in a collection are uniquely identified by their `_id` field and can be
/// queried using filters and options.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
/// use nitrite::collection::Document;
/// use nitrite::filter::field;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let db = Nitrite::builder().open_or_create(None, None)?;
/// let mut users = db.collection("users")?;
///
/// // Insert a document
/// let mut doc = Document::new();
/// doc.put("name", "Alice")?;
/// doc.put("age", 30i64)?;
/// users.insert(doc)?;
///
/// // Find documents
/// let filter = field("age").eq(30);
/// let results = users.find(filter)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct NitriteCollection {
    inner: Arc<dyn NitriteCollectionProvider>,
}

impl NitriteCollection {
    /// Creates a new `NitriteCollection` from a provider implementation.
    ///
    /// # Arguments
    ///
    /// * `inner` - A type implementing `NitriteCollectionProvider`
    pub fn new<T: NitriteCollectionProvider + 'static>(inner: T) -> Self {
        NitriteCollection { inner: Arc::new(inner) }
    }
}

impl Deref for NitriteCollection {
    type Target = Arc<dyn NitriteCollectionProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}