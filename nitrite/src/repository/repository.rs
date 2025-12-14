use crate::collection::operation::WriteResult;
use crate::collection::{CollectionEventListener, Document, FindOptions, NitriteCollection, NitriteId, UpdateOptions};
use crate::common::{
    AttributeAware, Attributes, Convertible, EventAware, PersistentCollection, Processor,
    SubscriberRef,
};
use crate::errors::NitriteResult;
use crate::filter::Filter;
use crate::index::{IndexDescriptor, IndexOptions};
use crate::repository::cursor::ObjectCursor;
use crate::repository::NitriteEntity;
use crate::store::NitriteStore;
use std::ops::Deref;
use std::sync::Arc;

/// A trait for implementing typed repository operations on Nitrite entities.
///
/// # Purpose
///
/// `ObjectRepositoryProvider` defines the complete interface for persisting, querying, and managing
/// strongly-typed entities in a Nitrite database. It provides both write operations (insert, update, remove)
/// and read operations (find, get_by_id), along with collection management and indexing capabilities.
///
/// This trait is generic over `T`, the entity type, which must implement `Convertible` and `NitriteEntity`
/// to enable transparent serialization and schema-aware operations.
///
/// # Characteristics
///
/// - **Generic Over Entity Type**: Works with any type implementing `Convertible<Output = T> + NitriteEntity + Send + Sync`
/// - **Type-Safe Operations**: Automatically converts between entity objects and internal document representation
/// - **Thread-Safe**: Requires `Send + Sync` to enable safe concurrent access
/// - **Inherits Collection Methods**: Extends `PersistentCollection` for index management and lifecycle control
/// - **Filter-Based Queries**: Uses the `Filter` DSL for expressive document filtering
///
/// # Relationship to Related Types
///
/// - `ObjectRepository<T>`: The facade wrapper that implements `Deref` to this trait
/// - `DefaultObjectRepository<T>`: The default implementation in `default_object_repository` module
/// - `NitriteEntity`: Defines schema and identity configuration for the entity type
/// - `Convertible`: Enables bidirectional conversion between objects and documents
pub trait ObjectRepositoryProvider<T>: PersistentCollection
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync
{    
    /// Inserts a single entity into the repository.
    ///
    /// # Arguments
    ///
    /// * `object` - The entity instance to insert
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing the NitriteIds of all inserted documents, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Automatically assigns a unique NitriteId to the document
    /// - Converts the entity to a document using `Convertible::to_value`
    /// - Triggers collection event handlers if subscribed
    /// - Respects any unique index constraints; fails if constraint violated
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<Employee> = db.repository()?;
    /// let emp = Employee { emp_id: Some(1), address: Some("xyz".to_string()), .. };
    /// repository.insert(emp)?;
    /// ```
    fn insert(&self, object: T) -> NitriteResult<WriteResult>;

    /// Inserts multiple entities into the repository in a batch operation.
    ///
    /// # Arguments
    ///
    /// * `objects` - A vector of entity instances to insert
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing the NitriteIds of all inserted documents, or an error if the operation fails.
    ///
    /// # Behavior
    ///
    /// - Converts all entities to documents before insertion
    /// - More efficient than calling `insert()` multiple times due to batch processing
    /// - Automatically assigns unique NitriteIds to all documents
    /// - All insertions are atomic; either all succeed or all fail
    /// - Respects unique index constraints across the entire batch
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<Employee> = db.repository()?;
    /// let employees = vec![manager.clone(), developer.clone()];
    /// repository.insert_many(employees)?;
    /// ```
    fn insert_many(&self, objects: Vec<T>) -> NitriteResult<WriteResult>;
    
    /// Updates entities matching a filter with a new object state (convenience method).
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify entities to update
    /// * `object` - The entity instance with updated data
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about updated documents.
    ///
    /// # Behavior
    ///
    /// This is a convenience method that calls `update_with_options` with default `UpdateOptions`.
    /// It updates matching entities but does not insert if no match is found.
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TxData> = db.repository()?;
    /// let filter = field("name").eq("John");
    /// let updated = TxData::new(1, "Jane");
    /// repository.update(filter, updated)?;
    /// ```
    fn update(
        &self,
        filter: Filter,
        object: T,
    ) -> NitriteResult<WriteResult> {
        self.update_with_options(filter, object, &UpdateOptions::default())
    }

    /// Updates entities matching a filter with configurable update behavior.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify entities to update
    /// * `object` - The entity instance with updated data
    /// * `update_options` - Configuration controlling insertion behavior and update scope
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about updated documents.
    ///
    /// # Behavior
    ///
    /// - Converts the entity to a document representation
    /// - If `insert_if_absent=false` (default): updates only matching documents
    /// - If `insert_if_absent=true`: inserts if no documents match the filter
    /// - If `just_once=true`: updates only the first matching document
    /// - If `just_once=false`: updates all matching documents
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let filter = field("id").eq("test_id");
    /// let update_options = UpdateOptions::default();
    /// repository.update_with_options(filter, entity, &update_options)?;
    /// ```
    fn update_with_options(
        &self,
        filter: Filter,
        object: T,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult>;

    /// Updates a single entity by matching on its unique identifier field(s).
    ///
    /// # Arguments
    ///
    /// * `object` - The entity instance with updated data
    /// * `insert_if_absent` - If true, inserts the entity if no match is found; otherwise fails silently
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about the update operation.
    ///
    /// # Behavior
    ///
    /// - Uses the entity's configured unique identifier field(s) from `NitriteEntity::entity_id()`
    /// - Updates only the single document matching the entity's ID
    /// - If `insert_if_absent=true`: inserts the entity if no ID match is found
    /// - If `insert_if_absent=false`: returns success even if no document matched
    /// - Most efficient for ID-based updates as it uses a unique filter
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TxData> = db.repository()?;
    /// let updated = TxData::new(1, "Jane");
    /// repository.update_one(updated, true)?;
    /// ```
    fn update_one(&self, object: T, insert_if_absent: bool) -> NitriteResult<WriteResult>;

    /// Updates documents at the raw document level matching a filter.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify documents to update
    /// * `document` - The raw document with updated fields
    /// * `just_once` - If true, updates only the first matching document; if false, updates all matches
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about updated documents.
    ///
    /// # Behavior
    ///
    /// - Updates documents directly without entity conversion
    /// - Useful when you have partial document updates that don't map to full entity objects
    /// - Works with raw `Document` values rather than typed entities
    /// - All documents matching the filter are updated with the provided field values
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let filter = all();
    /// let document = Document::default();
    /// repository.update_document(filter, &document, true)?;
    /// ```
    fn update_document(
        &self,
        filter: Filter,
        document: &Document,
        just_once: bool,
    ) -> NitriteResult<WriteResult>;

    /// Updates an entity directly by its NitriteId without filter-based lookup.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique NitriteId of the document to update
    /// * `object` - The entity instance with updated data
    /// * `insert_if_absent` - If true, inserts if the id doesn't exist; otherwise fails silently
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about the update operation.
    ///
    /// # Behavior
    ///
    /// - **O(1) operation**: Directly accesses the document by its internal NitriteId key
    /// - No filter-based search required, making it extremely efficient
    /// - Particularly useful when iterating over retrieved entities and updating them
    /// - The most performant way to update a known document
    /// - Avoids expensive filter-based lookups on large collections
    ///
    /// # Examples
    ///
    /// From nitrite-int-test (derived from transaction patterns):
    /// ```ignore
    /// let repository: ObjectRepository<Employee> = db.repository()?;
    /// let cursor = repository.find(all())?;
    /// for emp_wrapper in cursor {
    ///     let emp = emp_wrapper.get_object()?;
    ///     // Update the entity directly using its NitriteId
    ///     repository.update_by_nitrite_id(&emp.id, modified_emp, false)?;
    /// }
    /// ```
    fn update_by_nitrite_id(
        &self,
        id: &NitriteId,
        object: T,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult>;

    /// Removes a single entity by matching on its unique identifier field(s).
    ///
    /// # Arguments
    ///
    /// * `object` - The entity instance whose ID is used to identify the document to remove
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about the removed document.
    ///
    /// # Behavior
    ///
    /// - Uses the entity's configured unique identifier field(s) from `NitriteEntity::entity_id()`
    /// - Removes exactly one document matching the entity's ID
    /// - Fails if no document with the matching ID exists
    /// - Most efficient for ID-based removal
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let entity = TestEntity::default();
    /// repository.remove_one(entity)?;
    /// ```
    fn remove_one(&self, object: T) -> NitriteResult<WriteResult>;

    /// Removes documents matching a filter with optional single-match limiting.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify documents to remove
    /// * `just_once` - If true, removes only the first matching document; if false, removes all matches
    ///
    /// # Returns
    ///
    /// A `WriteResult` containing information about removed documents.
    ///
    /// # Behavior
    ///
    /// - Removes all documents matching the filter unless `just_once=true`
    /// - If `just_once=true`: removes only the first matching document
    /// - If `just_once=false`: removes all matching documents
    /// - Deletes documents from all indexes before removing from storage
    /// - Triggers collection event handlers if subscribed
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TxData> = db.repository()?;
    /// repository.remove(field("name").eq("John"), false)?;
    /// ```
    fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult>;

    /// Retrieves a single entity by its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `id` - The entity's unique identifier value (type must match entity's ID type)
    ///
    /// # Returns
    ///
    /// An `Option<T>` containing the entity if found, or `None` if not found.
    ///
    /// # Behavior
    ///
    /// - Uses the entity's configured ID field(s) to construct a filter
    /// - Returns `None` if no document matches the ID
    /// - Converts the matching document back to the entity type using `Convertible::from_value`
    /// - Efficient lookup for single ID-based retrieval
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let id = "test_id".to_string();
    /// let result = repository.get_by_id(&id)?;
    /// assert!(result.is_none());
    /// ```
    fn get_by_id(&self, id: &T::Id) -> NitriteResult<Option<T>>;

    /// Finds all entities matching a filter and returns them as a cursor for iteration.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify matching entities
    ///
    /// # Returns
    ///
    /// An `ObjectCursor<T>` for iterating over matching entities with default options.
    ///
    /// # Behavior
    ///
    /// - Returns an `ObjectCursor` that lazily converts documents to entities
    /// - Cursor can be iterated to extract entities one at a time
    /// - Uses default pagination and sorting (no options)
    /// - All matching documents are included in the result set
    /// - Cursor is lazy; documents are converted only when iterated
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let filter = all();
    /// let cursor = repository.find(filter)?;
    /// for wrapper in cursor {
    ///     let entity = wrapper.get_object()?;
    ///     // Process entity
    /// }
    /// ```
    fn find(&self, filter: Filter) -> NitriteResult<ObjectCursor<T>>;

    /// Finds entities matching a filter with advanced query options like sorting and pagination.
    ///
    /// # Arguments
    ///
    /// * `filter` - The filter expression to identify matching entities
    /// * `find_options` - Configuration for pagination, sorting, and projection
    ///
    /// # Returns
    ///
    /// An `ObjectCursor<T>` for iterating over matching entities with applied options.
    ///
    /// # Behavior
    ///
    /// - Applies sorting, pagination, and field projection from `FindOptions`
    /// - Returns an `ObjectCursor` that lazily converts documents to entities
    /// - Cursor respects limit and offset from find options
    /// - Documents are projected to include only specified fields if configured
    /// - Lazy evaluation; processing happens during iteration
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<TestEntity> = db.repository()?;
    /// let filter = all();
    /// let find_options = FindOptions::default();
    /// let cursor = repository.find_with_options(filter, &find_options)?;
    /// ```
    fn find_with_options(
        &self,
        filter: Filter,
        find_options: &FindOptions,
    ) -> NitriteResult<ObjectCursor<T>>;
    
    /// Returns the underlying raw collection for this repository.
    ///
    /// # Returns
    ///
    /// A `NitriteCollection` that provides access to the raw document-level API.
    ///
    /// # Behavior
    ///
    /// - Provides access to the raw collection without entity type wrapping
    /// - Enables advanced operations not exposed through the typed repository interface
    /// - Can be used to perform document-level operations directly
    /// - Thread-safe; the collection can be safely shared
    ///
    /// # Examples
    ///
    /// From nitrite-int-test:
    /// ```ignore
    /// let repository: ObjectRepository<Employee> = db.repository()?;
    /// let collection = repository.document_collection();
    /// // Can use collection for raw operations
    /// ```
    fn document_collection(&self) -> NitriteCollection;
}

/// A typed facade for repository operations on a specific entity type.
///
/// # Purpose
///
/// `ObjectRepository<T>` provides a user-friendly interface for persisting and querying entities
/// in a Nitrite database. It wraps an implementation of `ObjectRepositoryProvider<T>` (typically
/// `DefaultObjectRepository<T>`) and exposes all repository operations through the `Deref` trait,
/// making it ergonomic to call provider methods directly.
///
/// # Characteristics
///
/// - **Generic Over Entity Type**: Works with any type implementing `Convertible + NitriteEntity`
/// - **Thread-Safe**: Uses `Arc` for shared ownership and safe concurrent access
/// - **Cloneable**: Can be cloned cheaply (cloning just increments Arc reference count)
/// - **Trait Object Wrapper**: Wraps `Arc<dyn ObjectRepositoryProvider<T>>` for runtime polymorphism
/// - **Transparent Delegation**: Implements `Deref` to expose all provider methods
///
/// # How It Works
///
/// The repository uses dynamic dispatch through a trait object (`dyn ObjectRepositoryProvider<T>`)
/// to allow different implementations (like `DefaultObjectRepository<T>` or transactional variants).
/// The `Deref` implementation enables transparent access to all trait methods as if they were
/// directly available on `ObjectRepository<T>`.
///
/// # Relationship to Related Types
///
/// - `ObjectRepositoryProvider<T>`: The trait this facade wraps
/// - `DefaultObjectRepository<T>`: The default implementation provided by the database
/// - `NitriteEntity`: Defines schema and identity for the entity type
///
/// # Examples
///
/// From nitrite-int-test:
/// ```ignore
/// let repository: ObjectRepository<Employee> = db.repository()?;
/// 
/// // Facade transparently provides all ObjectRepositoryProvider methods
/// repository.insert(emp)?;
/// let cursor = repository.find(all())?;
/// for wrapper in cursor {
///     let emp = wrapper.get_object()?;
///     // Process employee
/// }
/// ```
#[derive(Clone)]
pub struct ObjectRepository<T>
where
    T: Convertible + NitriteEntity
{
    inner: Arc<dyn ObjectRepositoryProvider<T>>,
}

impl<T> ObjectRepository<T>
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync
{
    /// Creates a new typed repository facade wrapping an implementation.
    ///
    /// # Arguments
    ///
    /// * `inner` - An implementation of `ObjectRepositoryProvider<T>` (e.g., `DefaultObjectRepository<T>`)
    ///
    /// # Returns
    ///
    /// A new `ObjectRepository<T>` that wraps the provided implementation.
    ///
    /// # Behavior
    ///
    /// - Wraps the implementation in an `Arc` for shared, thread-safe ownership
    /// - Stores it as a trait object of type `Arc<dyn ObjectRepositoryProvider<T>>`
    /// - All subsequent calls to repository methods delegate to the wrapped implementation
    /// - The wrapper is cheap to clone (only increments Arc reference count)
    ///
    /// # Type Constraints
    ///
    /// The generic parameter `I` must:
    /// - Implement `ObjectRepositoryProvider<T>` (which includes `PersistentCollection`)
    /// - Be `'static` (owned, no borrowed references)
    /// - This allows any implementation to be wrapped transparently
    ///
    /// # Examples
    ///
    /// From repository.rs module tests:
    /// ```ignore
    /// let repo = ObjectRepository::new(MockBaseObjectRepository);
    /// assert!(repo.insert(entity).is_ok());
    /// ```
    pub fn new<I: ObjectRepositoryProvider<T> + 'static>(inner: I) -> Self {
        ObjectRepository { inner: Arc::new(inner) }
    }
}

impl<T> Deref for ObjectRepository<T>
where
    T: Convertible + NitriteEntity
{
    type Target = Arc<dyn ObjectRepositoryProvider<T>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::operation::WriteResult;
    use crate::collection::{Document, FindOptions, UpdateOptions};
    use crate::common::{Attributes, DocumentCursor, Processor, ProcessorChain, ProcessorProvider, Value};
    use crate::errors::{ErrorKind, NitriteError, NitriteResult};
    use crate::filter::{all, field, Filter};
    use crate::index::{IndexDescriptor, IndexOptions};
    use crate::repository::cursor::ObjectCursor;
    use crate::repository::{EntityId, EntityIndex};
    use crate::store::NitriteStore;
    use basu::HandlerId;

    struct MockBaseObjectRepository;

    impl PersistentCollection for MockBaseObjectRepository {
        fn add_processor(&self, _processor: Processor) -> NitriteResult<()> {
            Ok(())
        }

        fn create_index(&self, _field_names: Vec<&str>, _index_options: &IndexOptions) -> NitriteResult<()> {
            Ok(())
        }

        fn rebuild_index(&self, _field_names: Vec<&str>) -> NitriteResult<()> {
            Ok(())
        }

        fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
            Ok(vec![])
        }

        fn has_index(&self, _field_names: Vec<&str>) -> NitriteResult<bool> {
            Ok(false)
        }

        fn is_indexing(&self, _field_names: Vec<&str>) -> NitriteResult<bool> {
            Ok(false)
        }

        fn drop_index(&self, _field_names: Vec<&str>) -> NitriteResult<()> {
            Ok(())
        }

        fn drop_all_indexes(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn clear(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn dispose(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_dropped(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn is_open(&self) -> NitriteResult<bool> {
            Ok(true)
        }

        fn size(&self) -> NitriteResult<u64> {
            Ok(0)
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn store(&self) -> NitriteResult<NitriteStore> {
            Ok(NitriteStore::default())
        }
    }

    impl EventAware for MockBaseObjectRepository {
        fn subscribe(&self, _handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>> {
            Ok(Some(SubscriberRef::new(HandlerId::new())))
        }

        fn unsubscribe(&self, _subscriber: SubscriberRef) -> NitriteResult<()> {
            Ok(())
        }
    }

    impl AttributeAware for MockBaseObjectRepository {
        fn attributes(&self) -> NitriteResult<Option<Attributes>> {
            Ok(None)
        }

        fn set_attributes(&self, _attributes: Attributes) -> NitriteResult<()> {
            Ok(())
        }
    }

    impl<T> ObjectRepositoryProvider<T> for MockBaseObjectRepository
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync,
    {
        fn insert(&self, _object: T) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn insert_many(&self, _objects: Vec<T>) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn update_with_options(
            &self,
            _filter: Filter,
            _object: T,
            _update_options: &UpdateOptions,
        ) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn update_one(&self, _object: T, _insert_if_absent: bool) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn update_document(
            &self,
            _filter: Filter,
            _document: &Document,
            _just_once: bool,
        ) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn update_by_nitrite_id(
            &self,
            _id: &NitriteId,
            _object: T,
            _insert_if_absent: bool,
        ) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn remove_one(&self, _object: T) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn remove(&self, _filter: Filter, _just_once: bool) -> NitriteResult<WriteResult> {
            Ok(WriteResult::new(vec![]))
        }

        fn get_by_id(&self, _id: &T::Id) -> NitriteResult<Option<T>> {
            Ok(None)
        }

        fn find(&self, _filter: Filter) -> NitriteResult<ObjectCursor<T>> {
            Ok(ObjectCursor::new(DocumentCursor::new(Box::new(vec![].into_iter()), ProcessorChain::new())))
        }

        fn find_with_options(
            &self,
            _filter: Filter,
            _find_options: &FindOptions,
        ) -> NitriteResult<ObjectCursor<T>> {
            Ok(ObjectCursor::new(DocumentCursor::new(Box::new(vec![].into_iter()), ProcessorChain::new())))
        }

        fn document_collection(&self) -> NitriteCollection {
            todo!()
        }
    }

    #[derive(Default)]
    struct TestEntity {
        id: String,
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let mut document = Document::new();
            document.put("id", Value::from(self.id.clone()))?;
            Ok(Value::Document(document))
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            match value {
                Value::Document(document) => {
                    let item = document.get("id")?;
                    let id = item.as_string().unwrap();
                    Ok(TestEntity { id: id.clone() })
                },
                _ => Err(NitriteError::new(
                    "Repository conversion error: expected document value but found another type",
                    ErrorKind::InvalidOperation
                )),
            }
        }
    }

    impl NitriteEntity for TestEntity {
        type Id = String;

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            None
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", Some(false), None))
        }
    }

    #[test]
    fn test_insert() {
        let repo = ObjectRepository::new(MockBaseObjectRepository);
        let entity = TestEntity::default();
        let result = repo.insert(entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_batch() {
        let repo = ObjectRepository::new(MockBaseObjectRepository);
        let entities = vec![TestEntity::default(), TestEntity::default()];
        let result = repo.insert_many(entities);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update() {
        let repo = ObjectRepository::new(MockBaseObjectRepository);
        let entity = TestEntity::default();
        let filter = field("id").eq("test_id");
        let update_options = UpdateOptions::default();
        let result = repo.update_with_options(filter, entity, &update_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_one() {
        let repo = ObjectRepository::new(MockBaseObjectRepository);
        let entity = TestEntity::default();
        let result = repo.update_one(entity, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_document() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let filter = all();
        let document = Document::default();
        let result = repo.update_document(filter, &document, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_one() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let entity = TestEntity::default();
        let result = repo.remove_one(entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let filter = all();
        let result = repo.remove(filter, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_by_id() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let id = "test_id".to_string();
        let result = repo.get_by_id(&id);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_find() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let filter = all();
        let result = repo.find(filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_options() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let filter = all();
        let find_options = FindOptions::default();
        let result = repo.find_with_options(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_subscribe() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let handler = CollectionEventListener::new(Box::new(|_event| Ok(())));
        let result = repo.subscribe(handler);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unsubscribe() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let subscriber = SubscriberRef::new(HandlerId::new());
        let result = repo.unsubscribe(subscriber);
        assert!(result.is_ok());
    }

    #[test]
    fn test_attributes() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.attributes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_attributes() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let attributes = Attributes::default();
        let result = repo.set_attributes(attributes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_processor() {
        struct MockProcessor;
        
        impl ProcessorProvider for MockProcessor {
            fn name(&self) -> String {
                "MockProcessor".to_string()
            }

            fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
                Ok(doc)
            }

            fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
                Ok(doc)
            }
        }
        
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let processor = Processor::new(MockProcessor);
        let result = repo.add_processor(processor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let field_names = vec!["field1"];
        let index_options = IndexOptions::default();
        let result = repo.create_index(field_names, &index_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_rebuild_index() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let field_names = vec!["field1"];
        let result = repo.rebuild_index(field_names);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_indexes() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.list_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_index() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let field_names = vec!["field1"];
        let result = repo.has_index(field_names);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_indexing() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let field_names = vec!["field1"];
        let result = repo.is_indexing(field_names);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_index() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let field_names = vec!["field1"];
        let result = repo.drop_index(field_names);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_all_indexes() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.drop_all_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.dispose();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_dropped() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.is_dropped();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_open() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.is_open();
        assert!(result.is_ok());
    }

    #[test]
    fn test_size() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.size();
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_store() {
        let repo: ObjectRepository<TestEntity> = ObjectRepository::new(MockBaseObjectRepository);
        let result = repo.store();
        assert!(result.is_ok());
    }
}