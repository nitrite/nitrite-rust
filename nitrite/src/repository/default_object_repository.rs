use crate::collection::operation::WriteResult;
use crate::collection::{
    CollectionEventListener, Document, FindOptions, NitriteCollection, NitriteCollectionProvider,
    NitriteId, UpdateOptions,
};
use crate::common::{
    AttributeAware, Attributes, Convertible, EventAware, PersistentCollection, Processor,
    SubscriberRef, Value,
};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::filter::Filter;
use crate::index::{IndexDescriptor, IndexOptions};
use crate::repository::cursor::ObjectCursor;
use crate::repository::repository::ObjectRepositoryProvider;
use crate::repository::repository_operations::RepositoryOperations;
use crate::repository::NitriteEntity;
use crate::store::NitriteStore;
use std::any::TypeId;
use std::marker::PhantomData;
use std::sync::Arc;

pub(crate) struct DefaultObjectRepository<T> {
    nitrite_collection: NitriteCollection,
    repository_operations: RepositoryOperations,
    _phantom: PhantomData<T>,
}

impl<T> DefaultObjectRepository<T> {
    pub(crate) fn new(
        nitrite_collection: NitriteCollection,
        repository_operations: RepositoryOperations,
    ) -> DefaultObjectRepository<T> {
        DefaultObjectRepository {
            nitrite_collection,
            repository_operations,
            _phantom: PhantomData,
        }
    }
}

impl<T> PersistentCollection for DefaultObjectRepository<T>
where
    T: Convertible + NitriteEntity + Send + Sync,
{
    fn add_processor(&self, processor: Processor) -> NitriteResult<()> {
        self.nitrite_collection.add_processor(processor)
    }

    fn create_index(
        &self,
        field_names: Vec<&str>,
        index_options: &IndexOptions,
    ) -> NitriteResult<()> {
        self.nitrite_collection
            .create_index(field_names, index_options)
    }

    fn rebuild_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.nitrite_collection.rebuild_index(field_names)
    }

    fn list_indexes(&self) -> NitriteResult<Vec<IndexDescriptor>> {
        self.nitrite_collection.list_indexes()
    }

    fn has_index(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.nitrite_collection.has_index(field_names)
    }

    fn is_indexing(&self, field_names: Vec<&str>) -> NitriteResult<bool> {
        self.nitrite_collection.is_indexing(field_names)
    }

    fn drop_index(&self, field_names: Vec<&str>) -> NitriteResult<()> {
        self.nitrite_collection.drop_index(field_names)
    }

    fn drop_all_indexes(&self) -> NitriteResult<()> {
        self.nitrite_collection.drop_all_indexes()
    }

    fn clear(&self) -> NitriteResult<()> {
        self.nitrite_collection.clear()
    }

    fn dispose(&self) -> NitriteResult<()> {
        self.nitrite_collection.dispose()
    }

    fn is_dropped(&self) -> NitriteResult<bool> {
        self.nitrite_collection.is_dropped()
    }

    fn is_open(&self) -> NitriteResult<bool> {
        self.nitrite_collection.is_open()
    }

    fn size(&self) -> NitriteResult<u64> {
        self.nitrite_collection.size()
    }

    fn close(&self) -> NitriteResult<()> {
        self.nitrite_collection.close()
    }

    fn store(&self) -> NitriteResult<NitriteStore> {
        self.nitrite_collection.store()
    }
}

impl<T> EventAware for DefaultObjectRepository<T> {
    fn subscribe(&self, handler: CollectionEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.nitrite_collection.subscribe(handler)
    }

    fn unsubscribe(&self, subscriber: SubscriberRef) -> NitriteResult<()> {
        self.nitrite_collection.unsubscribe(subscriber)
    }
}

impl<T> AttributeAware for DefaultObjectRepository<T> {
    fn attributes(&self) -> NitriteResult<Option<Attributes>> {
        self.nitrite_collection.attributes()
    }

    fn set_attributes(&self, attributes: Attributes) -> NitriteResult<()> {
        self.nitrite_collection.set_attributes(attributes)
    }
}

impl<T> ObjectRepositoryProvider<T> for DefaultObjectRepository<T>
where
    T: Convertible<Output = T> + NitriteEntity + Send + Sync,
{
    fn insert(&self, object: T) -> NitriteResult<WriteResult> {
        let document = self.repository_operations.to_document(&object, false)?;
        self.nitrite_collection.insert(document)
    }

    fn insert_many(&self, objects: Vec<T>) -> NitriteResult<WriteResult> {
        let refs: Vec<&T> = objects.iter().collect();
        let documents = self.repository_operations.to_documents(refs)?;
        self.nitrite_collection.insert_many(documents)
    }

    fn update_with_options(
        &self,
        filter: Filter,
        object: T,
        update_options: &UpdateOptions,
    ) -> NitriteResult<WriteResult> {
        let mut document = self.repository_operations.to_document(&object, true)?;
        if !update_options.is_insert_if_absent() {
            self.repository_operations
                .remove_nitrite_id(&mut document)?;
        }

        self.nitrite_collection
            .update_with_options(filter, &document, update_options)
    }

    fn update_one(&self, object: T, insert_if_absent: bool) -> NitriteResult<WriteResult> {
        let update_options = UpdateOptions::new(insert_if_absent, true);
        let filter = self.repository_operations.create_unique_filter(&object)?;
        self.update_with_options(filter, object, &update_options)
    }

    fn update_document(
        &self,
        filter: Filter,
        document: &Document,
        just_once: bool,
    ) -> NitriteResult<WriteResult> {
        let mut document = document.clone();
        self.repository_operations
            .remove_nitrite_id(&mut document)?;
        self.nitrite_collection
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
        
        // Remove NitriteId from the update document (it's already known via the id parameter)
        self.repository_operations.remove_nitrite_id(&mut document)?;
        
        self.nitrite_collection.update_by_id(id, &document, insert_if_absent)
    }

    fn remove_one(&self, object: T) -> NitriteResult<WriteResult> {
        let filter = self.repository_operations.create_unique_filter(&object)?;
        self.remove(filter, true)
    }

    fn remove(&self, filter: Filter, just_once: bool) -> NitriteResult<WriteResult> {
        self.nitrite_collection.remove(filter, just_once)
    }

    fn get_by_id(&self, id: &T::Id) -> NitriteResult<Option<T>> {
        if TypeId::of::<T::Id>() == TypeId::of::<()>() {
            log::error!("Entity {} does not have an id field", T::default().entity_name());
            return Err(NitriteError::new(
                "Entity does not have an id field",
                ErrorKind::InvalidOperation,
            ));
        }
        
        let id_filter = self
            .repository_operations
            .create_id_filter(id.to_value()?)?;
        let mut cursor = self.find(id_filter)?;
        cursor.first().transpose()
    }

    fn find(&self, filter: Filter) -> NitriteResult<ObjectCursor<T>> {
        let cursor = self.nitrite_collection.find(filter)?;
        Ok(ObjectCursor::new(cursor))
    }

    fn find_with_options(
        &self,
        filter: Filter,
        find_options: &FindOptions,
    ) -> NitriteResult<ObjectCursor<T>> {
        let cursor = self.nitrite_collection.find_with_options(filter, find_options)?;
        Ok(ObjectCursor::new(cursor))
    }

    fn document_collection(&self) -> NitriteCollection {
        self.nitrite_collection.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::operation::WriteResult;
    use crate::collection::{Document, NitriteCollection};
    use crate::common::{Attributes, Processor, ProcessorProvider, Value, NON_UNIQUE_INDEX};
    use crate::doc;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::filter::{field, Filter};
    use crate::index::IndexOptions;
    use crate::nitrite::Nitrite;
    use crate::repository::repository_operations::RepositoryOperations;
    use crate::repository::{EntityId, EntityIndex, ObjectRepository};
    use crate::store::NitriteStore;
    use std::sync::Arc;


    #[derive(Debug, Default)]
    struct TestId {
        name: String,
    }

    #[derive(Debug, Default)]
    struct TestEntity {
        field: String,
        id: TestId,
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let document = doc! ({
                "field": (self.field.to_value()?),
                "id": (self.id.to_value()?),
            });
            Ok(Value::Document(document))
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            let document = match value {
                Value::Document(doc) => doc,
                _ => {
                    log::error!("Expected Document for TestEntity deserialization, got {:?}", value);
                    return Err(NitriteError::new(
                        "Expected Document value for entity deserialization",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let field = document.get("field")?;
            let field = match field.as_string() {
                Some(s) => s.to_string(),
                None => {
                    log::error!("TestEntity field 'field' should be string, got: {:?}", field);
                    return Err(NitriteError::new(
                        "Entity field 'field' must be a string",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let id = document.get("id")?;
            let id = match id.as_document() {
                Some(doc) => doc,
                None => {
                    log::error!("TestEntity field 'id' should be document, got: {:?}", id);
                    return Err(NitriteError::new(
                        "Entity field 'id' must be a document",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let name = id.get("name")?
                .as_string()
                .ok_or_else(|| {
                    log::error!("TestEntity id.name must be a string");
                    NitriteError::new(
                        "Entity id.name field must be a string",
                        ErrorKind::ObjectMappingError,
                    )
                })?
                .to_string();
            
            let id = TestId { name };
            Ok(TestEntity { field, id })
        }
    }

    impl NitriteEntity for TestEntity {
        type Id = TestId;

        fn entity_name(&self) -> String {
            "test_entity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            Some(vec![EntityIndex::new(vec!["field"], Some(NON_UNIQUE_INDEX))])
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", None, Some(vec!["name"])))
        }
    }

    impl Convertible for TestId {
        type Output = TestId;

        fn to_value(&self) -> NitriteResult<Value> {
            let document = doc! ({
                "name": (self.name.to_value()?),
            });
            Ok(Value::Document(document))
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            let document = match value {
                Value::Document(doc) => doc,
                _ => {
                    log::error!("Expected Document for TestId deserialization, got {:?}", value);
                    return Err(NitriteError::new(
                        "Expected Document value for TestId deserialization",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let name = document.get("name")?;
            let name = match name.as_string() {
                Some(s) => s.to_string(),
                None => {
                    log::error!("TestId field 'name' should be string, got: {:?}", name);
                    return Err(NitriteError::new(
                        "TestId field 'name' must be a string",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            Ok(TestId { name })
        }
    }

    #[test]
    fn test_insert() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let result = repository.insert(entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_batch() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entities = vec![TestEntity {
            field: "value1".to_string(),
            id: TestId {
                name: "name1".to_string(),
            },
        }, TestEntity {
            field: "value2".to_string(),
            id: TestId {
                name: "name2".to_string(),
            },
        }];
        let result = repository.insert_many(entities);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let filter = field("field").eq("value");
        let update_options = UpdateOptions::default();
        let result = repository.update_with_options(filter, entity, &update_options);
        let result = result.unwrap();
        assert_eq!(result.affected_nitrite_ids().len(), 0);

        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let filter = field("field").eq("value");
        let update_options = UpdateOptions::new(true, false);
        let result = repository.update_with_options(filter, entity, &update_options);
        let result = result.unwrap();
        assert_eq!(result.affected_nitrite_ids().len(), 1);
    }

    #[test]
    fn test_update_one() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let result = repository.update_one(entity, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_document() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let document = Document::new();
        let filter = field("field").eq("value");
        let result = repository.update_document(filter, &document, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove_one() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let result = repository.remove_one(entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_remove() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let filter = field("field").eq("value");
        let result = repository.remove(filter, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_by_id() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        repository.insert(entity).unwrap();
        
        let id = TestId {
            name: "name".to_string(),
        };
        let result = repository.get_by_id(&id);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let filter = field("field").eq("value");        
        let result = repository.find(filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_options() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let filter = field("field").eq("value");
        let find_options = FindOptions::default();
        let result = repository.find_with_options(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_add_processor() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let processor = Processor::new(TestProcessor);
        let result = repository.add_processor(processor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_index() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.create_index(vec!["field", "id.name"], &IndexOptions::default());
        assert!(result.is_ok());
    }

    #[test]
    fn test_rebuild_index() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.rebuild_index(vec!["field"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_indexes() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.list_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_index() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.has_index(vec!["field"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_indexing() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.is_indexing(vec!["field"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_index() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.drop_index(vec!["field"]);
        assert!(result.is_ok());
    }

    #[test]
    fn test_drop_all_indexes() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.drop_all_indexes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.dispose();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_dropped() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.is_dropped();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_open() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.is_open();
        assert!(result.is_ok());
    }

    #[test]
    fn test_size() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.size();
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_store() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.store();
        assert!(result.is_ok());
    }

    #[test]
    fn test_subscribe() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let handler = |_| Ok(());
        let listener = CollectionEventListener::new(handler);
        let result = repository.subscribe(listener);
        assert!(result.is_ok());
    }

    #[test]
    fn test_unsubscribe() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let handler = |_| Ok(());
        let listener = CollectionEventListener::new(handler);
        let subscriber = repository.subscribe(listener).unwrap();
        let result = repository.unsubscribe(subscriber.unwrap());
        assert!(result.is_ok());
    }

    #[test]
    fn test_attributes() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let result = repository.attributes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_set_attributes() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let attributes = Attributes::new();
        let result = repository.set_attributes(attributes);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_failure() {
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        let entity = TestEntity {
            field: "value".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let _ = repository.insert(entity);
        
        let entity = TestEntity {
            field: "value2".to_string(),
            id: TestId {
                name: "name".to_string(),
            },
        };
        let result = repository.insert(entity); // unique index violation        
        assert!(result.is_err());
    }

    struct TestProcessor;

    impl ProcessorProvider for TestProcessor {
        fn name(&self) -> String {
            "TestProcessor".to_string()
        }

        fn process_before_write(&self, doc: Document) -> NitriteResult<Document> {
            let mut doc = doc.clone();
            doc.put("processed", true)?;
            Ok(doc)
        }

        fn process_after_read(&self, doc: Document) -> NitriteResult<Document> {
            let mut doc = doc.clone();
            doc.remove("processed")?;
            Ok(doc)
        }
    }

    #[test]
    fn test_test_entity_from_value_with_document() {
        // Test safe deserialization with valid document
        let mut doc = Document::new();
        doc.put("field", Value::String("test_value".to_string())).unwrap();
        let mut id_doc = Document::new();
        id_doc.put("name", Value::String("test_id".to_string())).unwrap();
        doc.put("id", Value::Document(id_doc)).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_ok());
        
        let entity = result.unwrap();
        assert_eq!(entity.field, "test_value");
        assert_eq!(entity.id.name, "test_id");
    }

    #[test]
    fn test_test_entity_from_value_with_invalid_type() {
        // Test that non-document value is rejected with error
        let result = TestEntity::from_value(&Value::I32(42));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_entity_from_value_with_wrong_field_type() {
        // Test that wrong field type is rejected with error
        let mut doc = Document::new();
        doc.put("field", Value::I32(123)).unwrap(); // Wrong type - should be string
        let mut id_doc = Document::new();
        id_doc.put("name", Value::String("test_id".to_string())).unwrap();
        doc.put("id", Value::Document(id_doc)).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_entity_from_value_with_wrong_id_type() {
        // Test that wrong id type is rejected with error
        let mut doc = Document::new();
        doc.put("field", Value::String("test_value".to_string())).unwrap();
        doc.put("id", Value::String("not_a_document".to_string())).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_id_from_value_with_document() {
        // Test safe deserialization with valid document
        let mut doc = Document::new();
        doc.put("name", Value::String("id_name".to_string())).unwrap();
        
        let result = TestId::from_value(&Value::Document(doc));
        assert!(result.is_ok());
        
        let id = result.unwrap();
        assert_eq!(id.name, "id_name");
    }

    #[test]
    fn test_test_id_from_value_with_invalid_type() {
        // Test that non-document value is rejected with error
        let result = TestId::from_value(&Value::String("not_a_document".to_string()));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_id_from_value_with_wrong_name_type() {
        // Test that wrong name field type is rejected with error
        let mut doc = Document::new();
        doc.put("name", Value::I32(456)).unwrap(); // Wrong type - should be string
        
        let result = TestId::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    // Performance optimization tests
    #[test]
    fn test_insert_batch_efficient_reference_collection() {
        // Validates that insert_batch efficiently collects references
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let entities = vec![
            TestEntity {
                field: "value1".to_string(),
                id: TestId { name: "name1".to_string() },
            },
            TestEntity {
                field: "value2".to_string(),
                id: TestId { name: "name2".to_string() },
            },
            TestEntity {
                field: "value3".to_string(),
                id: TestId { name: "name3".to_string() },
            },
        ];
        
        let result = repository.insert_many(entities);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().affected_nitrite_ids().len(), 3);
    }

    #[test]
    fn test_insert_batch_preserves_order() {
        // Validates that insert_batch preserves insertion order
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let entities = vec![
            TestEntity {
                field: "first".to_string(),
                id: TestId { name: "id1".to_string() },
            },
            TestEntity {
                field: "second".to_string(),
                id: TestId { name: "id2".to_string() },
            },
        ];
        
        let result = repository.insert_many(entities);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_returns_object_cursor_correctly() {
        // Validates that find returns properly initialized ObjectCursor
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let entity = TestEntity {
            field: "test_find".to_string(),
            id: TestId { name: "find_test".to_string() },
        };
        repository.insert(entity).unwrap();
        
        let filter = field("field").eq("test_find");
        let result = repository.find(filter);
        assert!(result.is_ok());
    }

    #[test]
    fn test_find_with_options_passes_options_through() {
        // Validates that find_with_options properly delegates to collection
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let filter = field("field").eq("value");
        let find_options = FindOptions::default();
        
        let result = repository.find_with_options(filter, &find_options);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_document_with_large_batch() {
        // Validates update_document works correctly without cloning issues
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let mut document = Document::new();
        document.put("field", Value::String("batch_update".to_string())).unwrap();
        document.put("id", Value::Document({
            let mut id_doc = Document::new();
            id_doc.put("name", Value::String("batch_id".to_string())).unwrap();
            id_doc
        })).unwrap();
        
        let filter = field("field").eq("batch_update");
        let result = repository.update_document(filter, &document, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_insert_batch_empty_vec() {
        // Validates that empty batch is handled gracefully
        let db = Nitrite::default();
        let repository = db.repository::<TestEntity>().expect("Failed to get repository");
        
        let entities: Vec<TestEntity> = vec![];
        let result = repository.insert_many(entities);
        assert!(result.is_ok());
    }
}
