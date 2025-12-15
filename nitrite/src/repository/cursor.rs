use crate::collection::{Document, FindPlan, NitriteId};
use crate::common::{
    Convertible, DocumentCursor, JoinedDocumentCursor, Lookup, ProjectedDocumentCursor, Value,
};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::repository::NitriteEntity;
use std::marker::PhantomData;

pub struct ObjectCursor<T> {
    cursor: DocumentCursor,
    _phantom: PhantomData<T>,
}

impl<T> ObjectCursor<T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    pub fn new(cursor: DocumentCursor) -> Self {
        ObjectCursor {
            cursor,
            _phantom: PhantomData,
        }
    }

    pub fn reset(&mut self) {
        self.cursor.reset();
    }
    
    pub fn size(&mut self) -> usize {
        // Reset the underlying cursor to ensure we count from the beginning.
        self.reset();
        let count = self.cursor.size();
        // Reset again for replayability.
        self.reset();
        count
    }

    pub fn first(&mut self) -> Option<NitriteResult<T>> {
        let doc_result = self.cursor.first();
        match doc_result {
            Some(Ok(doc)) => {
                let result = T::from_value(&Value::Document(doc));
                match result {
                    Ok(obj) => Some(Ok(obj)),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }

    pub fn find_plan(&self) -> Option<&FindPlan> {
        self.cursor.find_plan()
    }

    pub(crate) fn set_find_plan(mut self, find_plan: FindPlan) -> Self {
        self.cursor = self.cursor.set_find_plan(find_plan);
        self
    }

    pub fn join<'a, J: Convertible<Output = J> + NitriteEntity>(
        &'a mut self,
        foreign_cursor: &'a mut ObjectCursor<J>,
        lookup: &'a Lookup,
    ) -> NitriteResult<JoinedObjectCursor<'a, J>> {
        let joined_doc_cursor = self.cursor.join(&mut foreign_cursor.cursor, lookup)?;
        Ok(JoinedObjectCursor::new(joined_doc_cursor))
    }

    pub fn project<P>(&'_ mut self) -> NitriteResult<ProjectedObjectCursor<'_, P>>
    where
        P: Convertible<Output = P> + NitriteEntity + Default,
    {
        ProjectedObjectCursor::new(&mut self.cursor)
    }
    
    /// Returns an iterator that yields `(NitriteId, T)` pairs.
    /// This is useful when you need to update entities after retrieving them,
    /// as it provides the NitriteId needed for efficient O(1) updates via
    /// `update_by_nitrite_id`.
    pub fn iter_with_id(&mut self) -> ObjectCursorWithId<'_, T> {
        ObjectCursorWithId {
            cursor: &mut self.cursor,
            _phantom: PhantomData,
        }
    }
}

/// An iterator adapter that yields `(NitriteId, T)` pairs from an ObjectCursor.
/// This enables efficient updates after retrieval by providing the NitriteId.
pub struct ObjectCursorWithId<'a, T> {
    cursor: &'a mut DocumentCursor,
    _phantom: PhantomData<T>,
}

impl<'a, T> Iterator for ObjectCursorWithId<'a, T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    type Item = NitriteResult<(NitriteId, T)>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc_result = self.cursor.next();
        match doc_result {
            Some(Ok(mut doc)) => {
                // Get the NitriteId from the document
                let id = match doc.id() {
                    Ok(id) => id,
                    Err(e) => return Some(Err(e)),
                };
                
                // Convert document to entity
                let result = T::from_value(&Value::Document(doc));
                match result {
                    Ok(obj) => Some(Ok((id, obj))),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl<T> Iterator for ObjectCursor<T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    type Item = NitriteResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc_result = self.cursor.next();
        match doc_result {
            Some(Ok(doc)) => {
                let result = T::from_value(&Value::Document(doc));
                match result {
                    Ok(obj) => Some(Ok(obj)),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct JoinedObjectCursor<'a, T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    cursor: JoinedDocumentCursor<'a>,
    _phantom: PhantomData<T>,
}

impl<'a, T> JoinedObjectCursor<'a, T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    pub fn new(cursor: JoinedDocumentCursor<'a>) -> Self {
        JoinedObjectCursor {
            cursor,
            _phantom: PhantomData,
        }
    }

    pub fn size(&mut self) -> usize {
        let mut count = 0;
        // Consume all joined documents.
        while self.next().is_some() {
            count += 1;
        }
        // Reset the underlying cursor for replayability.
        self.cursor.reset();
        count
    }
}

impl<'a, T> Iterator for JoinedObjectCursor<'a, T>
where
    T: Convertible<Output = T> + NitriteEntity,
{
    type Item = NitriteResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc_result = self.cursor.next();
        match doc_result {
            Some(Ok(doc)) => {
                let result = T::from_value(&Value::Document(doc));
                match result {
                    Ok(obj) => Some(Ok(obj)),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

pub struct ProjectedObjectCursor<'a, P>
where
    P: Convertible<Output = P> + NitriteEntity + Default,
{
    cursor: ProjectedDocumentCursor<'a>,
    _phantom: PhantomData<P>,
}

impl<'a, P> ProjectedObjectCursor<'a, P>
where
    P: Convertible<Output = P> + NitriteEntity + Default,
{
    pub fn new(cursor: &'a mut DocumentCursor) -> NitriteResult<Self> {
        let projection = P::default().to_value()?;
        let projection = projection.as_document().cloned().ok_or_else(|| {
            log::error!("Projection type is not convertible to document");
            NitriteError::new(
                "Projection type is not convertible to document",
                ErrorKind::ObjectMappingError,
            )
        })?;


        Ok(ProjectedObjectCursor {
            cursor: cursor.project(projection)?,
            _phantom: PhantomData,
        })
    }

    pub fn size(&mut self) -> usize {
        let count = self.cursor.size();
        // Reset the underlying cursor for replayability.
        self.cursor.reset();
        count
    }
}

impl<'a, P> Iterator for ProjectedObjectCursor<'a, P>
where
    P: Convertible<Output = P> + NitriteEntity + Default,
{
    type Item = NitriteResult<P>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc_result = self.cursor.next();
        match doc_result {
            Some(Ok(doc)) => {
                let result = P::from_value(&Value::Document(doc));
                match result {
                    Ok(obj) => Some(Ok(obj)),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::{ProcessorChain, NON_UNIQUE_INDEX};
    use crate::doc;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::repository::{EntityId, EntityIndex};

    struct TestEntity {
        first: String,
        last: String,
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let doc = doc!{
                "first": (self.first.to_string()),
                "last": (self.last.to_string()),
            };
            Ok(Value::Document(doc))
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            match value {
                Value::Document(doc) => {
                    let first = doc.get("first")?;
                    let first = match first.as_string() {
                        Some(s) => s.to_string(),
                        None => {
                            log::error!("TestEntity field 'first' should be string, got: {:?}", first);
                            return Err(NitriteError::new(
                                "TestEntity field 'first' must be a string",
                                ErrorKind::ObjectMappingError,
                            ));
                        }
                    };
                    
                    let last = doc.get("last")?;
                    let last = match last.as_string() {
                        Some(s) => s.to_string(),
                        None => {
                            log::error!("TestEntity field 'last' should be string, got: {:?}", last);
                            return Err(NitriteError::new(
                                "TestEntity field 'last' must be a string",
                                ErrorKind::ObjectMappingError,
                            ));
                        }
                    };
                    
                    Ok(TestEntity { first, last })
                }
                _ => {
                    log::error!("Expected Document for TestEntity, got: {:?}", value);
                    Err(NitriteError::new(
                        "Object cursor deserialization error: expected document but found another value type for TestEntity",
                        ErrorKind::ObjectMappingError
                    ))
                }
            }
        }
    }
    
    impl NitriteEntity for TestEntity {
        type Id = ();

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            Some(vec![
                EntityIndex::new(vec!["first", "last"], Some(NON_UNIQUE_INDEX)),
            ])
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("first", None, None))
        }
    }
    
    impl Default for TestEntity {
        fn default() -> Self {
            TestEntity {
                first: "".to_string(),
                last: "".to_string(),
            }
        }
    }

    fn create_document(first: &str, last: &str) -> Document {
        let doc = doc!{
            "first": first,
            "last": last,
        };
        doc
    }

    #[test]
    fn test_new_object_cursor() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        assert_eq!(object_cursor.cursor.into_iter().count(), 2);
    }

    #[test]
    fn test_object_cursor_first() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let first = object_cursor.first().unwrap().unwrap();
        assert_eq!(first.first, "John");
    }

    #[test]
    fn test_object_cursor_first_with_error() {
        let docs = vec![
            Err(NitriteError::new("Test Error", ErrorKind::IOError)),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let object_cursor = (&mut object_cursor).into_iter();
        assert!(object_cursor.next().unwrap().is_err());
    }

    #[test]
    fn test_object_cursor_next() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let iter = (&mut object_cursor).into_iter();
        let first = iter.next().unwrap().unwrap();
        assert_eq!(first.first, "John");
        let second = iter.next().unwrap().unwrap();
        assert_eq!(second.first, "Jane");
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_object_cursor_next_with_error() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Err(NitriteError::new("Test Error", ErrorKind::IOError)),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let iter = (&mut object_cursor).into_iter();
        let first = iter.next().unwrap().unwrap();
        assert_eq!(first.first, "John");
        assert!(iter.next().unwrap().is_err());
    }

    #[test]
    fn test_object_cursor_find_plan() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        assert!(object_cursor.find_plan().is_none());
    }

    #[test]
    fn test_object_cursor_set_find_plan() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let find_plan = FindPlan::new(); // Assuming FindPlan has a default() method
        let cursor = DocumentCursor::new(iter, ProcessorChain::new()).set_find_plan(find_plan.clone());
        let object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor).set_find_plan(find_plan.clone());
        assert!(object_cursor.find_plan().is_some());
        assert_eq!(object_cursor.find_plan().unwrap().index_descriptor(), find_plan.index_descriptor());
    }

    #[test]
    fn test_object_cursor_join() {
        let docs1 = vec![Ok(create_document("John", "Doe"))];
        let docs2 = vec![Ok(create_document("Jane", "Doe"))];
        let iter1 = Box::new(docs1.into_iter());
        let iter2 = Box::new(docs2.into_iter());
        let cursor1 = DocumentCursor::new(iter1, ProcessorChain::new());
        let cursor2 = DocumentCursor::new(iter2, ProcessorChain::new());
        let mut object_cursor1: ObjectCursor<TestEntity> = ObjectCursor::new(cursor1);
        let mut object_cursor2: ObjectCursor<TestEntity> = ObjectCursor::new(cursor2);
        let lookup = Lookup {
            local_field: "last".to_string(),
            foreign_field: "last".to_string(),
            target_field: "surname".to_string(),
        };
        let joined_cursor = object_cursor1.join(&mut object_cursor2, &lookup).expect("Failed to join");
        assert_eq!(joined_cursor.count(), 1);
    }

    #[test]
    fn test_object_cursor_project() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let mut projected_cursor = object_cursor.project::<TestEntity>().expect("Failed to project");
        assert_eq!(projected_cursor.size(), 1);
    }

    #[test]
    fn test_test_entity_from_value_with_valid_document() {
        // Test safe deserialization with valid document
        let doc = create_document("John", "Doe");
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_ok());
        
        let entity = result.unwrap();
        assert_eq!(entity.first, "John");
        assert_eq!(entity.last, "Doe");
    }

    #[test]
    fn test_test_entity_from_value_with_invalid_type() {
        // Test that non-document value is rejected with error
        let result = TestEntity::from_value(&Value::I32(42));
        assert!(result.is_err());
    }

    #[test]
    fn test_test_entity_from_value_with_wrong_first_type() {
        // Test that wrong first field type is rejected with error
        let mut doc = Document::new();
        doc.put("first", Value::I32(123)).unwrap(); // Wrong type - should be string
        doc.put("last", Value::String("Doe".to_string())).unwrap();
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_test_entity_from_value_with_wrong_last_type() {
        // Test that wrong last field type is rejected with error
        let mut doc = Document::new();
        doc.put("first", Value::String("John".to_string())).unwrap();
        doc.put("last", Value::Bool(true)).unwrap(); // Wrong type - should be string
        
        let result = TestEntity::from_value(&Value::Document(doc));
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(e.kind(), &ErrorKind::ObjectMappingError);
        }
    }

    #[test]
    fn test_object_cursor_iteration_with_type_validation() {
        // Test that cursor iteration validates types and doesn't panic
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Smith")),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let mut object_cursor: ObjectCursor<TestEntity> = ObjectCursor::new(cursor);
        let iter = (&mut object_cursor).into_iter();
        
        let mut count = 0;
        for result in iter {
            assert!(result.is_ok());
            let entity = result.unwrap();
            assert!(!entity.first.is_empty());
            assert!(!entity.last.is_empty());
            count += 1;
        }
        assert_eq!(count, 2);
    }
}