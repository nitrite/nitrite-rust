use std::sync::Arc;

use crate::collection::operation::WriteResult;
use crate::collection::{CollectionEventListener, Document, FindOptions, NitriteCollection, NitriteId, UpdateOptions};
use crate::common::{AttributeAware, Attributes, Convertible, EventAware, PersistentCollection, Processor, SubscriberRef, Value};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::filter::Filter;
use crate::index::{IndexDescriptor, IndexOptions};
use crate::nitrite_config::NitriteConfig;
use crate::repository::{NitriteEntity, ObjectCursor, ObjectRepository, ObjectRepositoryProvider, RepositoryOperations};
use crate::store::NitriteStore;

/// A transactional repository providing isolated object repository operations.
///
/// Wraps an `ObjectRepository<T>` with transactional isolation, enabling changes to be
/// made to objects within a transaction context. Implements the repository interface
/// by delegating to a transactional backing collection while maintaining isolation
/// from other concurrent transactions.
///
/// # Purpose
/// Provides transactional semantics for object repository operations:
/// - **Isolated Updates**: Changes made to objects are visible only within the transaction
/// - **Lazy Evaluation**: Queries operate on the transactional view combining backing and primary data
/// - **Write-Through**: Write operations update the backing collection, read operations fall back to primary
/// - **Index Support**: Maintains indexes in the transactional context for query optimization
///
/// # Characteristics
/// - **Thread-Safe**: All state protected by Arc and Mutex through backing collection
/// - **Generic**: Supports any type implementing `NitriteEntity` and `Convertible`
/// - **Interface Compliance**: Implements `ObjectRepositoryProvider`, `PersistentCollection`, `EventAware`, and `AttributeAware`
/// - **Cloneable**: Arc-based shared ownership enables cheap cloning with shared state
/// - **Trait Delegation**: All operations delegated to inner implementation through trait methods
///
/// # Usage
/// Created by `NitriteTransaction` when accessing repositories within a transaction.
/// Users interact through the `ObjectRepository` public API which delegates to this type.
#[derive(Clone)]
pub(crate) struct TransactionalRepository<T> where T: NitriteEntity + Convertible<Output = T> {
    inner: Arc<TransactionalRepositoryInner<T>>,
}

struct TransactionalRepositoryInner<T> where T: NitriteEntity + Convertible<Output = T> {
    primary: ObjectRepository<T>,
    backing_collection: NitriteCollection,
    nitrite_config: NitriteConfig,
    operation: RepositoryOperations,
}

impl<T> TransactionalRepository<T>
where
    T: NitriteEntity + Convertible<Output = T> + Send + Sync,
{
    /// Creates a new transactional repository.
    ///
    /// # Arguments
    /// * `primary` - The primary object repository from the database
    /// * `backing_collection` - The transactional collection for isolation
    /// * `nitrite_config` - Configuration for index operations within the transaction
    /// * `operation` - Repository operations handler for type conversion
    ///
    /// # Returns
    /// A new `TransactionalRepository` with isolated write context and primary read fallback
    pub fn new(
        primary: ObjectRepository<T>,
        backing_collection: NitriteCollection,
        nitrite_config: NitriteConfig,
        operation: RepositoryOperations,
    ) -> Self {
        TransactionalRepository {
            inner: Arc::new(TransactionalRepositoryInner {
                primary,
                backing_collection,
                nitrite_config,
                operation,
            }),
        }
    }
}

impl<T> TransactionalRepositoryInner<T>
where
    T: NitriteEntity + Convertible<Output = T> + Send + Sync,
{
    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        self.backing_collection.add_processor(processor)
    }

    fn create_index(&self, field_names: Vec<&str>, index_options: &IndexOptions) -> NitriteResult<()> {
        self.backing_collection.create_index(field_names, index_options)
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.backing_collection.rebuild_index(field_names)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.backing_collection.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.backing_collection.has_index(field_names)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.backing_collection.is_indexing(field_names)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.backing_collection.drop_index(field_names)
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.backing_collection.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        self.backing_collection.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.backing_collection.dispose()
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        self.backing_collection.is_dropped()
    }

    fn is_open(&self) -> NitriteResult<bool> {
        self.backing_collection.is_open()
    }

    fn size(&self) -> NitriteResult<u64> {
        self.backing_collection.size()
    }

    fn close(&self) -> NitriteResult<()> {
        self.backing_collection.close()
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        self.backing_collection.store()
    }

    fn subscribe(&self, handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.backing_collection.subscribe(handler)
    }

    fn unsubscribe(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        self.backing_collection.unsubscribe(subscriber)
    }

    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.backing_collection.attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.backing_collection.set_attributes(attributes)
    }

    fn insert(&self, object: T) -> NitriteResult<WriteResult> {
        let document = self.operation.to_document(&object, false)?;
        self.backing_collection.insert(document)
    }

    fn insert_batch(&self, objects: Vec<T>) -> NitriteResult<WriteResult> {
        let refs: Vec<&T> = objects.iter().collect();
        let documents = self.operation.to_documents(refs)?;
        self.backing_collection.insert_many(documents)
    }

    fn update_with_options(&self, filter: Filter, object: T, update_options: &UpdateOptions) -> NitriteResult<WriteResult> {
        let mut document = self.operation.to_document(&object, true)?;
        if !update_options.is_insert_if_absent() {
            self.operation.remove_nitrite_id(&mut document)?;
        }
        self.backing_collection.update_with_options(filter, &document, update_options)
    }

    fn update_one(&self, object: T, insert_if_absent: bool) -> NitriteResult<WriteResult> {
        let update_options = UpdateOptions::new(insert_if_absent, true);
        let filter = self.operation.create_unique_filter(&object)?;
        self.update_with_options(filter, object, &update_options)
    }

    fn update_document(&self, filter: Filter, document: &Document, just_once: bool) -> NitriteResult<WriteResult> {
        let mut document = document.clone();
        self.operation.remove_nitrite_id(&mut document)?;
        self.backing_collection
            .update_with_options(filter, &document, &UpdateOptions::new(false, just_once))
    }

    fn update_by_nitrite_id(
        &self,
        id: &NitriteId,
        object: T,
        insert_if_absent: bool,
    ) -> NitriteResult<WriteResult> {
        let value = object.to_value()?;
        let mut document = match value {
            Value::Document(doc) => doc,
            other => {
                log::error!("Expected Document from entity Convertible, got {:?}", other);
                return Err(NitriteError::new(
                    "Cannot update: Expected Document from Convertible",
                    ErrorKind::ObjectMappingError,
                ));
            }
        };
        self.operation.remove_nitrite_id(&mut document)?;
        self.backing_collection.update_by_id(id, &document, insert_if_absent)
    }

    fn remove_one(&self, object: T) -> NitriteResult<WriteResult> {
        let filter = self.operation.create_unique_filter(&object)?;
        self.remove(filter, true)
    }

    fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.backing_collection.remove(filter, just_once)
    }

    fn get_by_id(&self, id: &T::Id) -> NitriteResult<Option<T>> {
        // First try to get from the primary repository (for committed data)
        let result = self.primary.get_by_id(id)?;
        if result.is_some() {
            return Ok(result);
        }
        // Then try the transactional collection (for uncommitted data in this transaction)
        let id_filter = self.operation.create_id_filter(id.to_value()?)?;
        let mut cursor = self.find(id_filter)?;
        cursor.first().transpose()
    }

    fn find(&self, filter: Filter) -> NitriteResult<ObjectCursor<T>> {
        let cursor = self.backing_collection.find(filter)?;
        Ok(ObjectCursor::new(cursor))
    }

    fn find_with_options(&self, filter: Filter, find_options: &FindOptions) -> NitriteResult<ObjectCursor<T>> {
        let cursor = self.backing_collection.find_with_options(filter, find_options)?;
        Ok(ObjectCursor::new(cursor))
    }

    fn document_collection(&self) -> NitriteCollection {
        self.backing_collection.clone()
    }
}

impl<T> PersistentCollection for TransactionalRepository<T>
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync,
{
    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        self.inner.add_processor(processor)
    }

    fn create_index(&self, field_names: Vec<&str>, index_options: &IndexOptions) -> NitriteResult<()> {
        self.inner.create_index(field_names, index_options)
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.inner.rebuild_index(field_names)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.inner.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.inner.has_index(field_names)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.inner.is_indexing(field_names)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.inner.drop_index(field_names)
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.inner.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.inner.dispose()
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        self.inner.is_dropped()
    }

    fn is_open(&self) -> NitriteResult<bool> {
        self.inner.is_open()
    }

    fn size(&self) -> NitriteResult<u64> {
        self.inner.size()
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        self.inner.store()
    }
}

impl<T> EventAware for TransactionalRepository<T>
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync,
{
    fn subscribe(&self, handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.inner.subscribe(handler)
    }

    fn unsubscribe(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        self.inner.unsubscribe(subscriber)
    }
}

impl<T> AttributeAware for TransactionalRepository<T>
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync,
{
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.inner.attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.inner.set_attributes(attributes)
    }
}

impl<T> ObjectRepositoryProvider<T> for TransactionalRepository<T> where T: Convertible<Output = T> + NitriteEntity + Send + Sync, {
    fn insert(&self, object: T) -> NitriteResult<WriteResult> {
        self.inner.insert(object)
    }

    fn insert_many(&self, objects: Vec<T>) -> NitriteResult<WriteResult> {
        self.inner.insert_batch(objects)
    }

    fn update_with_options(&self, filter: Filter, object: T, update_options: &UpdateOptions) -> NitriteResult<WriteResult> {
        self.inner.update_with_options(filter, object, update_options)
    }

    fn update_one(&self, object: T, insert_if_absent: bool) -> NitriteResult<WriteResult> {
        self.inner.update_one(object, insert_if_absent)
    }

    fn update_document(&self, filter: Filter, document: &Document, just_once: bool) -> NitriteResult<WriteResult> {
        self.inner.update_document(filter, document, just_once)
    }

    fn update_by_nitrite_id(&self, id: &NitriteId, object: T, insert_if_absent: bool) -> NitriteResult<WriteResult> {
        self.inner.update_by_nitrite_id(id, object, insert_if_absent)
    }

    fn remove_one(&self, object: T) -> NitriteResult<WriteResult> {
        self.inner.remove_one(object)
    }

    fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.inner.remove(filter, just_once)
    }

    fn get_by_id(&self, id: &T::Id) -> NitriteResult<Option<T>> {
        self.inner.get_by_id(id)
    }

    fn find(&self, filter: Filter) -> NitriteResult<ObjectCursor<T>> {
        self.inner.find(filter)
    }

    fn find_with_options(&self, filter: Filter, find_options: &FindOptions) -> NitriteResult<ObjectCursor<T>> {
        self.inner.find_with_options(filter, find_options)
    }

    fn document_collection(&self) -> NitriteCollection {
        self.inner.document_collection()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Convertible, LockRegistry, ProcessorProvider, Value, NON_UNIQUE_INDEX};
    use crate::errors::{ErrorKind, NitriteError};
    use crate::filter::field;
    use crate::nitrite::Nitrite;
    use crate::repository::{EntityId, EntityIndex, NitriteEntity};
    use crate::transaction::NitriteTransaction;

    /// Create a test database
    fn create_test_db() -> Nitrite {
        Nitrite::builder().open_or_create(None, None).unwrap()
    }

    /// Test entity for repository tests
    #[derive(Clone, Debug, Default, PartialEq)]
    struct TestEntity {
        id: i64,
        name: String,
        age: i32,
    }

    impl TestEntity {
        fn new(id: i64, name: &str, age: i32) -> Self {
            Self {
                id,
                name: name.to_string(),
                age,
            }
        }
    }

    impl NitriteEntity for TestEntity {
        type Id = i64;

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            Some(vec![EntityIndex::new(vec!["name"], Some(NON_UNIQUE_INDEX))])
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", None, None))
        }
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let mut doc = Document::new();
            doc.put("id", Value::I64(self.id))?;
            doc.put("name", Value::String(self.name.clone()))?;
            doc.put("age", Value::I32(self.age))?;
            doc.to_value()
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            if let Value::Document(doc) = value {
                let id = match doc.get("id") {
                    Ok(Value::I64(i)) => i,
                    _ => 0,
                };
                let name = match doc.get("name") {
                    Ok(Value::String(s)) => s.clone(),
                    _ => String::new(),
                };
                let age = match doc.get("age") {
                    Ok(Value::I32(i)) => i,
                    _ => 0,
                };
                Ok(TestEntity { id, name, age })
            } else {
                Err(NitriteError::new(
                    "Invalid value type",
                    ErrorKind::ValidationError,
                ))
            }
        }
    }

    // ==================== Creation Tests ====================

    /// Tests that a transactional repository can be created via transaction
    #[test]
    fn test_transactional_repository_creation() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>();

        assert!(repo.is_ok());
    }

    /// Tests that a transactional repository can be created with a key
    #[test]
    fn test_transactional_repository_creation_with_key() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.keyed_repository::<TestEntity>("my_key");

        assert!(repo.is_ok());
    }

    // ==================== Insert Tests ====================

    /// Tests inserting a single object
    #[test]
    fn test_insert_single_object() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let entity = TestEntity::new(1, "Alice", 30);
        let result = repo.insert(entity);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.affected_nitrite_ids().len(), 1);
    }

    /// Tests inserting multiple objects
    #[test]
    fn test_insert_many_objects() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let entities = vec![
            TestEntity::new(1, "Alice", 30),
            TestEntity::new(2, "Bob", 25),
            TestEntity::new(3, "Charlie", 35),
        ];
        let result = repo.insert_many(entities);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.affected_nitrite_ids().len(), 3);
    }

    // ==================== Find Tests ====================

    /// Tests finding objects with a filter
    #[test]
    fn test_find_with_filter() {
        

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        repo.insert(TestEntity::new(2, "Bob", 25)).unwrap();

        let cursor = repo.find(field("name").eq("Alice"));
        assert!(cursor.is_ok());

        let results: Vec<TestEntity> = cursor.unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Alice");
    }

    /// Tests finding with options (limit and skip)
    #[test]
    fn test_find_with_options() {
        use crate::filter::all;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        for i in 0..10 {
            repo.insert(TestEntity::new(i, &format!("User{}", i), 20 + i as i32)).unwrap();
        }

        let options = FindOptions::new().limit(5).skip(2);
        let cursor = repo.find_with_options(all(), &options);
        assert!(cursor.is_ok());

        let results: Vec<TestEntity> = cursor.unwrap().collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(results.len(), 5);
    }

    // ==================== Get By ID Tests ====================

    /// Tests getting an object by its ID
    #[test]
    fn test_get_by_id() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(42, "Alice", 30)).unwrap();

        let result = repo.get_by_id(&42);
        assert!(result.is_ok());

        let entity = result.unwrap();
        assert!(entity.is_some());
        let entity = entity.unwrap();
        assert_eq!(entity.id, 42);
        assert_eq!(entity.name, "Alice");
    }

    /// Tests getting a non-existent object by ID
    #[test]
    fn test_get_by_id_not_found() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let result = repo.get_by_id(&999);
        assert!(result.is_ok());

        let entity = result.unwrap();
        assert!(entity.is_none());
    }

    // ==================== Update Tests ====================

    /// Tests updating a single object
    #[test]
    fn test_update_one() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        let updated = TestEntity::new(1, "Alice Updated", 31);
        let result = repo.update_one(updated, false);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result.affected_nitrite_ids().len(), 1);

        // Verify update
        let entity = repo.get_by_id(&1).unwrap().unwrap();
        assert_eq!(entity.name, "Alice Updated");
        assert_eq!(entity.age, 31);
    }

    /// Tests update_one with insert_if_absent=true when entity doesn't exist
    #[test]
    fn test_update_one_insert_if_absent() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let new_entity = TestEntity::new(99, "New Entity", 25);
        let result = repo.update_one(new_entity, true);

        assert!(result.is_ok());

        // Verify entity was inserted
        let entity = repo.get_by_id(&99).unwrap();
        assert!(entity.is_some());
    }

    /// Tests updating with options
    #[test]
    fn test_update_with_options() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        let options = UpdateOptions::new(false, true);
        let updated = TestEntity::new(1, "Alice New", 35);
        let result = repo.update_with_options(field("id").eq(1), updated, &options);

        assert!(result.is_ok());
    }

    /// Tests updating a document directly
    #[test]
    fn test_update_document() {
        use crate::doc;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        let update_doc = doc!{"name": "Alice Modified"};
        let result = repo.update_document(field("id").eq(1), &update_doc, true);

        assert!(result.is_ok());

        // Verify update
        let entity = repo.get_by_id(&1).unwrap().unwrap();
        assert_eq!(entity.name, "Alice Modified");
    }

    // ==================== Remove Tests ====================

    /// Tests removing a single object by filter
    #[test]
    fn test_remove_with_filter() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        repo.insert(TestEntity::new(2, "Bob", 25)).unwrap();

        let result = repo.remove(field("name").eq("Alice"), true);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);

        // Verify Alice was removed
        assert!(repo.get_by_id(&1).unwrap().is_none());
        // Verify Bob still exists
        assert!(repo.get_by_id(&2).unwrap().is_some());
    }

    /// Tests removing an object by its instance
    #[test]
    fn test_remove_one() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let entity = TestEntity::new(1, "Alice", 30);
        repo.insert(entity.clone()).unwrap();

        let result = repo.remove_one(entity);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 1);

        // Verify removed
        assert!(repo.get_by_id(&1).unwrap().is_none());
    }

    // ==================== Size Tests ====================

    /// Tests getting the size of the repository
    #[test]
    fn test_size() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        assert_eq!(repo.size().unwrap(), 0);

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        assert_eq!(repo.size().unwrap(), 1);

        repo.insert(TestEntity::new(2, "Bob", 25)).unwrap();
        assert_eq!(repo.size().unwrap(), 2);
    }

    // ==================== Clear Tests ====================

    /// Tests clearing the repository
    #[test]
    fn test_clear() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        repo.insert(TestEntity::new(2, "Bob", 25)).unwrap();
        assert_eq!(repo.size().unwrap(), 2);

        let result = repo.clear();
        assert!(result.is_ok());
        assert_eq!(repo.size().unwrap(), 0);
    }

    // ==================== Index Tests ====================

    /// Tests creating an index
    #[test]
    fn test_create_index() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        let result = repo.create_index(vec!["age"], &options);
        assert!(result.is_ok());
    }

    /// Tests listing indexes
    #[test]
    fn test_list_indexes() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        repo.create_index(vec!["age"], &options).unwrap();

        let indexes = repo.list_indexes();
        assert!(indexes.is_ok());
        // Should have at least the age index
        assert!(!indexes.unwrap().is_empty());
    }

    /// Tests checking if index exists
    #[test]
    fn test_has_index() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        repo.create_index(vec!["age"], &options).unwrap();

        assert!(repo.has_index(vec!["age"]).unwrap());
        assert!(!repo.has_index(vec!["nonexistent"]).unwrap());
    }

    /// Tests dropping an index
    #[test]
    fn test_drop_index() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        repo.create_index(vec!["age"], &options).unwrap();
        assert!(repo.has_index(vec!["age"]).unwrap());

        let result = repo.drop_index(vec!["age"]);
        assert!(result.is_ok());
        assert!(!repo.has_index(vec!["age"]).unwrap());
    }

    /// Tests dropping all indexes
    #[test]
    fn test_drop_all_indexes() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        repo.create_index(vec!["age"], &options).unwrap();

        let result = repo.drop_all_indexes();
        assert!(result.is_ok());
    }

    /// Tests rebuilding an index
    #[test]
    fn test_rebuild_index() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let options = IndexOptions::new(NON_UNIQUE_INDEX);
        repo.create_index(vec!["age"], &options).unwrap();

        // Add some data
        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        let result = repo.rebuild_index(vec!["age"]);
        assert!(result.is_ok());
    }

    /// Tests is_indexing
    #[test]
    fn test_is_indexing() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        // is_indexing should return false for non-existent field
        let result = repo.is_indexing(vec!["nonexistent"]);
        assert!(result.is_ok());
    }

    // ==================== Collection Access Tests ====================

    /// Tests getting the document collection
    #[test]
    fn test_document_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let collection = repo.document_collection();
        assert!(collection.is_open().unwrap());
    }

    // ==================== Store Tests ====================

    /// Tests getting the store
    #[test]
    fn test_store() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let store = repo.store();
        assert!(store.is_ok());
    }

    // ==================== State Tests ====================

    /// Tests is_open
    #[test]
    fn test_is_open() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        assert!(repo.is_open().unwrap());
    }

    /// Tests is_dropped
    #[test]
    fn test_is_dropped() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        assert!(!repo.is_dropped().unwrap());
    }

    // ==================== Attribute Tests ====================

    /// Tests getting attributes when none set
    #[test]
    fn test_attributes_none() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let attrs = repo.attributes();
        assert!(attrs.is_ok());
    }

    /// Tests setting and getting attributes
    #[test]
    fn test_set_and_get_attributes() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let attrs = Attributes::default();
        let result = repo.set_attributes(attrs);
        assert!(result.is_ok());

        let attrs = repo.attributes();
        assert!(attrs.is_ok());
    }

    // ==================== Clone Tests ====================

    /// Tests that TransactionalRepository can be cloned
    #[test]
    fn test_clone() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        let cloned = repo.clone();
        assert_eq!(cloned.size().unwrap(), 1);
    }

    // ==================== Commit/Rollback Integration Tests ====================

    /// Tests that changes are visible before commit
    #[test]
    fn test_changes_visible_before_commit() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db.clone(), lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

        // Should be visible within the transaction
        let result = repo.get_by_id(&1).unwrap();
        assert!(result.is_some());
    }

    /// Tests commit persists changes
    #[test]
    fn test_commit_persists_changes() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db.clone(), lock_registry.clone()).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        
        // Commit the transaction
        tx.commit().unwrap();

        // Verify changes are persisted by getting a new repository
        let tx2 = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo2 = tx2.repository::<TestEntity>().unwrap();
        let result = repo2.get_by_id(&1).unwrap();
        assert!(result.is_some());
    }

    /// Tests rollback discards changes
    #[test]
    fn test_rollback_discards_changes() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        {
            let tx = NitriteTransaction::new(db.clone(), lock_registry.clone()).unwrap();
            let repo = tx.repository::<TestEntity>().unwrap();

            repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();

            // Rollback the transaction
            tx.rollback().unwrap();
        }
        // tx is now dropped, releasing all resources

        // Verify changes are not persisted
        let tx2 = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo2 = tx2.repository::<TestEntity>().unwrap();
        let result = repo2.get_by_id(&1).unwrap();
        assert!(result.is_none());
    }

    /// Tests complex transaction with multiple operations
    #[test]
    fn test_complex_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db.clone(), lock_registry.clone()).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        // Insert multiple entities
        repo.insert(TestEntity::new(1, "Alice", 30)).unwrap();
        repo.insert(TestEntity::new(2, "Bob", 25)).unwrap();
        repo.insert(TestEntity::new(3, "Charlie", 35)).unwrap();

        // Update one
        let updated = TestEntity::new(2, "Bob Updated", 26);
        repo.update_one(updated, false).unwrap();

        // Remove one
        repo.remove(field("name").eq("Charlie"), true).unwrap();

        // Commit
        tx.commit().unwrap();

        // Verify final state
        let tx2 = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo2 = tx2.repository::<TestEntity>().unwrap();

        // Alice should exist
        let alice = repo2.get_by_id(&1).unwrap();
        assert!(alice.is_some());
        assert_eq!(alice.unwrap().name, "Alice");

        // Bob should be updated
        let bob = repo2.get_by_id(&2).unwrap();
        assert!(bob.is_some());
        assert_eq!(bob.unwrap().name, "Bob Updated");

        // Charlie should be removed
        let charlie = repo2.get_by_id(&3).unwrap();
        assert!(charlie.is_none());
    }

    // ==================== Processor Tests ====================

    /// Tests adding a processor
    #[test]
    fn test_add_processor() {
        use crate::common::Processor;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        struct TestProcessor;

        impl ProcessorProvider for TestProcessor {
            fn name(&self) -> String {
                "test_processor".to_string()
            }
        
            fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
                Ok(doc)
            }
        
            fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
                Ok(doc)
            }
        }

        // Create a simple processor
        let processor = Processor::new(
            TestProcessor,
        );

        let result = repo.add_processor(processor);
        assert!(result.is_ok());
    }

    // ==================== Event Tests ====================

    /// Tests subscribing to events
    #[test]
    fn test_subscribe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use crate::collection::CollectionEventListener;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();
        let listener: CollectionEventListener = CollectionEventListener::new(move |_| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });

        let result = repo.subscribe(listener);
        assert!(result.is_ok());
    }

    /// Tests unsubscribing from events
    #[test]
    fn test_unsubscribe() {
        use crate::collection::CollectionEventListener;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let listener: CollectionEventListener = CollectionEventListener::new(|_| Ok(()));
        let subscriber_ref = repo.subscribe(listener).unwrap();

        if let Some(sub_ref) = subscriber_ref {
            let result = repo.unsubscribe(sub_ref);
            assert!(result.is_ok());
        }
    }

    // ==================== Close/Dispose Tests ====================

    /// Tests closing the repository
    #[test]
    fn test_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let result = repo.close();
        assert!(result.is_ok());
    }

    /// Tests disposing the repository
    #[test]
    fn test_dispose() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repo = tx.repository::<TestEntity>().unwrap();

        let result = repo.dispose();
        assert!(result.is_ok());
    }
}
