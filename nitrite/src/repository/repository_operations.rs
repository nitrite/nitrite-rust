use crate::collection::{Document, NitriteCollection};
use crate::common::{Convertible, PersistentCollection, Value, DOC_ID, UNIQUE_INDEX};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::filter::Filter;
use crate::index::IndexOptions;
use crate::repository::{EntityId, NitriteEntity};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
pub(crate) struct RepositoryOperations {
    inner: Arc<RepositoryOperationsInner>
}

impl RepositoryOperations {
    pub fn new() -> Self {
        let inner = RepositoryOperationsInner {
            entity_id: OnceLock::new(),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    pub(crate) fn initialize<T>(&self, collection: NitriteCollection) -> NitriteResult<()>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        self.inner.initialize::<T>(collection)
    }
    
    pub(crate) fn to_documents<T>(&self, entities: Vec<&T>) -> NitriteResult<Vec<Document>>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        self.inner.to_documents(entities)
    }
    
    pub(crate) fn to_document<T>(&self, entity: &T, update: bool) -> NitriteResult<Document>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        self.inner.to_document(entity, update)
    }
    
    pub(crate) fn remove_nitrite_id(&self, document: &mut Document) -> NitriteResult<()> {
        self.inner.remove_nitrite_id(document)
    }
    
    pub(crate) fn create_unique_filter<T>(&self, entity: &T) -> NitriteResult<Filter>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        self.inner.create_unique_filter(entity)
    }
    
    pub(crate) fn create_id_filter<Id>(&self, id: Id) -> NitriteResult<Filter>
    where
        Id: Convertible,
    {
        self.inner.create_id_filter(id)
    }
}

pub(crate) struct RepositoryOperationsInner {
    entity_id: OnceLock<EntityId>
}

impl RepositoryOperationsInner {
    fn initialize<T>(&self, collection: NitriteCollection) -> NitriteResult<()>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        self.create_id_index::<T>(&collection)?;
        self.create_indexes::<T>(&collection)?;
        Ok(())
    }
    
    fn to_documents<T>(&self, entities: Vec<&T>) -> NitriteResult<Vec<Document>>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        let mut documents = Vec::with_capacity(entities.len());
        for entity in entities {
            let document = self.to_document(entity, false)?;
            documents.push(document);
        }
        Ok(documents)
    }
    
    fn to_document<T>(&self, entity: &T, update: bool) -> NitriteResult<Document>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        let entity_id = entity.entity_id();
        let value = entity.to_value()?;
        
        // Validate that entity.to_value() returns a Document type
        // This protects against malformed Convertible implementations
        let mut document = match value {
            Value::Document(doc) => doc,
            other => {
                log::error!("Expected Document from entity Convertible, got {:?}", other);
                return Err(NitriteError::new(
                    &format!("Entity conversion failed: Expected Document but got {:?}. Ensure the Convertible implementation returns a valid Document", other),
                    ErrorKind::ObjectMappingError,
                ));
            }
        };
        
        if entity_id.is_some() {
            let entity_id = entity_id.unwrap();
            
            let id_value = document.get(entity_id.field_name())?;
            if entity_id.is_nitrite_id() {
                if id_value.is_null() {
                    let id = document.id()?;
                    document.put(entity_id.field_name(), id)?;
                } else if !update {
                    // if it is an insert, then we should not allow to insert the
                    // document with user provided id
                    log::error!("Cannot insert entity with user provided NitriteId on field '{}'", entity_id.field_name());
                    return Err(NitriteError::new(
                        &format!("Cannot insert entity with user-provided NitriteId on field '{}'. Auto-generated IDs cannot be overwritten on insert", entity_id.field_name()),
                        ErrorKind::InvalidId
                    ));
                }
            }
            
            let id_value = document.get(entity_id.field_name())?;
            if id_value.is_null() {
                log::error!("Entity ID field '{}' cannot be null", entity_id.field_name());
                return Err(NitriteError::new(
                    &format!("Entity ID field '{}' cannot be null. Ensure all entities have a valid ID value set", entity_id.field_name()),
                    ErrorKind::InvalidId
                ));
            }
        }
        
        Ok(document)
    }
    
    fn remove_nitrite_id(&self, document: &mut Document) -> NitriteResult<()> {
        document.remove(DOC_ID)?;
        if let Some(entity_id) = self.entity_id.get() {
            if !entity_id.is_embedded() && entity_id.is_nitrite_id() {
                return document.remove(entity_id.field_name())
            }
        }
        Ok(())
    }
    
    fn create_unique_filter<T>(&self, entity: &T) -> NitriteResult<Filter>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        if let Some(entity_id) = self.entity_id.get() {
            let value = entity.to_value()?;
            
            // Validate that entity.to_value() returns a Document type
            let document = match value {
                Value::Document(doc) => doc,
                other => {
                    log::error!("Expected Document from entity Convertible in create_unique_filter, got {:?}", other);
                    return Err(NitriteError::new(
                        &format!("Cannot create unique filter: Expected Document from Convertible but got {:?}. Check your entity's Convertible implementation", other),
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let id_value = document.get(entity_id.field_name())?;
            entity_id.create_unique_filter(id_value)
        } else {
            log::error!("Failed to create unique filter: entity id is not defined");
            Err(NitriteError::new(
                "Cannot create unique filter: Entity ID is not defined. Ensure the entity class has a @Id field or use @Embedded ID",
                ErrorKind::NotIdentifiable
            ))
        }
    }
    
    fn create_id_filter<Id>(&self, id: Id) -> NitriteResult<Filter>
    where
        Id: Convertible,
    {
        if let Some(entity_id) = self.entity_id.get() {
            entity_id.create_id_filter(id.to_value()?)
        } else {
            log::error!("Failed to create id filter: entity id is not defined");
            Err(NitriteError::new(
                "Cannot create ID filter: Entity ID is not defined. Ensure the entity class has a @Id field or use @Embedded ID",
                ErrorKind::NotIdentifiable
            ))
        }
    }
    
    fn create_id_index<T>(&self, collection: &NitriteCollection) -> NitriteResult<()>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        let default_entity = T::default();
        let entity_id = default_entity.entity_id();
        if let Some(entity_id) = entity_id {
            self.entity_id.get_or_init(|| entity_id.clone());
            
            let field_name = if entity_id.is_embedded() {
                entity_id.encoded_field_names()
            } else {
                vec![entity_id.field_name().to_string()]
            };
            let field_name: Vec<&str> = field_name.iter().map(|s| s.as_str()).collect();
            let index_options = IndexOptions::new(UNIQUE_INDEX);
            collection.create_index(field_name, &index_options)?;
        } 
        Ok(())
    }
    
    fn create_indexes<T>(&self, collection: &NitriteCollection) -> NitriteResult<()>
    where
        T: Convertible<Output = T> + NitriteEntity,
    {
        let default_entity = T::default();
        if let Some(entity_indexes) = default_entity.entity_indexes() {
            for entity_index in entity_indexes {
                let field_names = entity_index.field_names();
                let field_names: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();
                let index_options = IndexOptions::new(entity_index.index_type());
                collection.create_index(field_names, &index_options)?;
            }
        }
        Ok(())
    }    
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Value, DOC_ID};
    use crate::errors::NitriteError;
    use crate::filter::Filter;
    use crate::index::IndexOptions;
    use crate::nitrite::Nitrite;
    use crate::nitrite_config::NitriteConfig;
    use crate::repository::{EntityId, EntityIndex, NitriteEntity};
    use std::sync::Arc;

    #[derive(Default)]
    struct TestEntity {
        id: Option<i32>,
    }

    impl NitriteEntity for TestEntity {
        type Id = i32;

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            Some(vec![EntityIndex::new(vec!["id"], Some(UNIQUE_INDEX))])
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", None, None))
        }
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let mut doc = Document::new();
            if let Some(id) = self.id {
                doc.put("id", id)?;
            }
            Ok(doc.to_value()?)
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            let doc = match value {
                Value::Document(d) => d,
                _ => {
                    log::error!("Expected Document for TestEntity deserialization, got {:?}", value);
                    return Err(NitriteError::new(
                        "Expected Document value for entity deserialization",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            let temp = doc.get("id")?;
            let id = match temp.as_i32() {
                Some(i) => Some(*i),
                None => {
                    log::error!("TestEntity id field must be i32, got: {:?}", temp);
                    return Err(NitriteError::new(
                        "TestEntity id field must be an i32",
                        ErrorKind::ObjectMappingError,
                    ));
                }
            };
            
            Ok(TestEntity { id })
        }
    }

    #[test]
    fn test_initialize() {
        let db = Nitrite::default();
        let collection = db.collection("test").unwrap();
                
        let operations = RepositoryOperations::new();
        let result = operations.initialize::<TestEntity>(collection);
        assert!(result.is_ok());
    }

    #[test]
    fn test_to_documents() {
        let operations = RepositoryOperations::new();
        let entities = vec![&TestEntity { id: Some(1) }];
        let result = operations.to_documents(entities);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_to_document() {
        let operations = RepositoryOperations::new();
        let entity = TestEntity { id: Some(1) };
        let result = operations.to_document(&entity, false);
        assert!(result.is_ok());
        let doc = result.unwrap();
        let temp = doc.get("id").unwrap();
        assert_eq!(temp.as_i32().unwrap(), &1);
    }

    #[test]
    fn test_remove_nitrite_id() {
        let operations = RepositoryOperations::new();
        let mut doc = Document::new();
        let _ = doc.id();
        let result = operations.remove_nitrite_id(&mut doc);
        assert!(result.is_ok());
        assert!(doc.get(DOC_ID).unwrap().is_null());
    }

    #[test]
    fn test_create_unique_filter() {
        let db = Nitrite::default();
        let collection = db.collection("test").unwrap();
        
        let operations = RepositoryOperations::new();
        let _ = operations.initialize::<TestEntity>(collection);
        
        let entity = TestEntity { id: Some(1) };
        let result = operations.create_unique_filter(&entity);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_id_filter() {
        let db = Nitrite::default();
        let collection = db.collection("test").unwrap();
        
        let operations = RepositoryOperations::new();
        let _ = operations.initialize::<TestEntity>(collection);
        let result = operations.create_id_filter(1);
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize_with_error() {
        let db = Nitrite::default();
        let collection = db.collection("test").unwrap();
        
        let operations = RepositoryOperations::new();
        let result = operations.initialize::<TestEntity>(collection);
        assert!(result.is_ok());
    }

    #[test]
    fn test_to_documents_with_error() {
        let operations = RepositoryOperations::new();
        let entities: Vec<&TestEntity> = vec![];
        let result = operations.to_documents(entities);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_to_document_with_error() {
        let operations = RepositoryOperations::new();
        let entity = TestEntity { id: None };
        let result = operations.to_document(&entity, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_remove_nitrite_id_with_error() {
        let operations = RepositoryOperations::new();
        let mut doc = Document::new();
        let result = operations.remove_nitrite_id(&mut doc);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_unique_filter_with_error() {
        let operations = RepositoryOperations::new();
        let entity = TestEntity { id: None };
        let result = operations.create_unique_filter(&entity);
        assert!(result.is_err());
    }

    #[test]
    fn test_create_id_filter_with_error() {
        let operations = RepositoryOperations::new();
        let result = operations.create_id_filter("invalid_id");
        assert!(result.is_err());
    }

    #[derive(Default)]
    struct BadConvertibleEntity {
        value: i32,
    }

    impl NitriteEntity for BadConvertibleEntity {
        type Id = i32;

        fn entity_name(&self) -> String {
            "BadConvertibleEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            None
        }

        fn entity_id(&self) -> Option<EntityId> {
            None
        }
    }

    impl Convertible for BadConvertibleEntity {
        type Output = BadConvertibleEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            // Deliberately return non-Document to test error handling
            Ok(Value::I32(self.value))
        }

        fn from_value(_value: &Value) -> NitriteResult<Self::Output> {
            Ok(BadConvertibleEntity { value: 42 })
        }
    }

    #[test]
    fn test_to_document_validates_convertible_document_type() {
        // Test that to_document properly validates Document type from Convertible
        let operations = RepositoryOperations::new();
        let bad_entity = BadConvertibleEntity { value: 100 };
        
        let result = operations.to_document(&bad_entity, false);
        
        // Should return an error, not panic on unwrap
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Document") || error_msg.contains("Convertible"));
    }

    #[test]
    fn test_to_document_with_valid_convertible() {
        // Test that to_document works correctly with proper Convertible implementation
        let operations = RepositoryOperations::new();
        let entity = TestEntity { id: Some(42) };
        
        let result = operations.to_document(&entity, false);
        
        // Should succeed with valid Document type
        assert!(result.is_ok());
        let doc = result.unwrap();
        let id_value = doc.get("id").unwrap();
        assert_eq!(id_value.as_i32().unwrap(), &42);
    }

    #[test]
    fn test_create_unique_filter_validates_document_type() {
        // Test that create_unique_filter validates Document type from Convertible
        let db = Nitrite::default();
        let collection = db.collection("test_bad").unwrap();
        
        let operations = RepositoryOperations::new();
        let _ = operations.initialize::<BadConvertibleEntity>(collection);
        
        let bad_entity = BadConvertibleEntity { value: 100 };
        let result = operations.create_unique_filter(&bad_entity);
        
        // Should return an error for non-Document type, not panic
        assert!(result.is_err());
    }

    #[test]
    fn test_create_unique_filter_with_valid_document() {
        // Test that create_unique_filter works with valid Document type
        let db = Nitrite::default();
        let collection = db.collection("test_good").unwrap();
        
        let operations = RepositoryOperations::new();
        let _ = operations.initialize::<TestEntity>(collection);
        
        let entity = TestEntity { id: Some(999) };
        let result = operations.create_unique_filter(&entity);
        
        // Should succeed with valid Document
        assert!(result.is_ok());
    }

    #[test]
    fn test_test_entity_from_value_with_valid_document() {
        // Test safe deserialization with valid document
        let mut doc = Document::new();
        doc.put("id", Value::I32(42)).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_ok());
        
        let entity = result.unwrap();
        assert_eq!(entity.id, Some(42));
    }

    #[test]
    fn test_test_entity_from_value_with_invalid_type() {
        // Test that non-document value is rejected with error
        let result = TestEntity::from_value(&Value::String("not_a_document".to_string()));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_entity_from_value_with_wrong_id_type() {
        // Test that wrong id field type is rejected with error
        let mut doc = Document::new();
        doc.put("id", Value::String("not_an_i32".to_string())).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_entity_from_value_with_null_id() {
        // Test that null id field is handled gracefully
        let mut doc = Document::new();
        doc.put("id", Value::Null).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        // This should error because as_i32() returns None for Null
        assert!(result.is_err());
    }

    #[test]
    fn test_test_entity_from_value_with_numeric_id() {
        // Test that numeric ids work correctly
        let mut doc = Document::new();
        doc.put("id", Value::I32(999)).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_ok());
        
        let entity = result.unwrap();
        assert_eq!(entity.id, Some(999));
    }

    #[test]
    fn test_to_documents_batch_efficiency() {
        // Test that to_documents pre-allocates correctly for multiple entities
        let operations = RepositoryOperations::new();
        let mut entities = Vec::new();
        for i in 1..=10 {
            entities.push(TestEntity { id: Some(i) });
        }
        let entity_refs: Vec<&TestEntity> = entities.iter().collect();
        
        let result = operations.to_documents(entity_refs);
        assert!(result.is_ok());
        
        let documents = result.unwrap();
        assert_eq!(documents.len(), 10);
        
        // Verify all documents were created correctly
        for (i, doc) in documents.iter().enumerate() {
            let id_value = doc.get("id").unwrap();
            assert_eq!(id_value.as_i32().unwrap(), &((i as i32) + 1));
        }
    }

    #[test]
    fn test_to_documents_empty_batch() {
        // Test that to_documents handles empty batch correctly
        let operations = RepositoryOperations::new();
        let entities: Vec<&TestEntity> = vec![];
        
        let result = operations.to_documents(entities);
        assert!(result.is_ok());
        
        let documents = result.unwrap();
        assert_eq!(documents.len(), 0);
        assert!(documents.is_empty());
    }

    #[test]
    fn test_to_documents_single_entity() {
        // Test that to_documents handles single entity correctly
        let operations = RepositoryOperations::new();
        let entity = TestEntity { id: Some(42) };
        let entities = vec![&entity];
        
        let result = operations.to_documents(entities);
        assert!(result.is_ok());
        
        let documents = result.unwrap();
        assert_eq!(documents.len(), 1);
        let id_value = documents[0].get("id").unwrap();
        assert_eq!(id_value.as_i32().unwrap(), &42);
    }

    #[test]
    fn test_to_documents_large_batch() {
        // Test that to_documents handles large batches efficiently
        let operations = RepositoryOperations::new();
        let mut entities = Vec::new();
        for i in 1..=100 {
            entities.push(TestEntity { id: Some(i) });
        }
        let entity_refs: Vec<&TestEntity> = entities.iter().collect();
        
        let result = operations.to_documents(entity_refs);
        assert!(result.is_ok());
        
        let documents = result.unwrap();
        assert_eq!(documents.len(), 100);
        
        // Spot check a few documents
        assert_eq!(documents[0].get("id").unwrap().as_i32().unwrap(), &1);
        assert_eq!(documents[49].get("id").unwrap().as_i32().unwrap(), &50);
        assert_eq!(documents[99].get("id").unwrap().as_i32().unwrap(), &100);
    }

    #[test]
    fn test_to_documents_preserves_entity_order() {
        // Test that to_documents preserves entity order
        let operations = RepositoryOperations::new();
        let entities = vec![
            TestEntity { id: Some(5) },
            TestEntity { id: Some(1) },
            TestEntity { id: Some(9) },
            TestEntity { id: Some(3) },
        ];
        let entity_refs: Vec<&TestEntity> = entities.iter().collect();
        
        let result = operations.to_documents(entity_refs);
        assert!(result.is_ok());
        
        let documents = result.unwrap();
        assert_eq!(documents.len(), 4);
        
        // Verify order is preserved (not sorted)
        assert_eq!(documents[0].get("id").unwrap().as_i32().unwrap(), &5);
        assert_eq!(documents[1].get("id").unwrap().as_i32().unwrap(), &1);
        assert_eq!(documents[2].get("id").unwrap().as_i32().unwrap(), &9);
        assert_eq!(documents[3].get("id").unwrap().as_i32().unwrap(), &3);
    }
}