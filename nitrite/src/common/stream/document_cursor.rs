use crate::collection::{Document, FindPlan, NitriteId};
use crate::common::processor::ProcessorChain;
use crate::common::stream::joined_cursor::{JoinedDocumentCursor, Lookup};
use crate::common::stream::projected_cursor::ProjectedDocumentCursor;
use crate::common::{ReadExecutor, WriteExecutor};
use crate::errors::NitriteResult;
use crate::ProcessorProvider;

pub struct DocumentCursor {
    underlying: Option<Box<dyn Iterator<Item = NitriteResult<Document>>>>,
    cache: Vec<NitriteResult<Document>>,
    current_index: usize,
    processor_chain: ProcessorChain,
    find_plan: Option<FindPlan>,
}

impl DocumentCursor {
    pub fn new(
        iter: Box<dyn Iterator<Item = NitriteResult<Document>>>,
        processor_chain: ProcessorChain,
    ) -> Self {
        DocumentCursor {
            underlying: Some(iter),
            cache: Vec::new(),
            current_index: 0,
            processor_chain,
            find_plan: None,
        }
    }

    /// Resets the cursor so that it can be iterated from the beginning.
    pub fn reset(&mut self) {
        self.current_index = 0;
    }

    pub fn size(&mut self) -> usize {
        // If the underlying iterator is already exhausted,
        // then no need to iterate again.
        if self.underlying.is_none() {
            self.reset();
            return self.cache.len();
        }
        // Otherwise, iterate through the remaining items.
        for _ in self.by_ref() {}
        self.reset();
        self.cache.len()
    }

    pub fn first(&mut self) -> Option<NitriteResult<Document>> {
        self.reset();
        self.next()
    }

    pub fn find_plan(&self) -> Option<&FindPlan> {
        self.find_plan.as_ref()
    }

    pub(crate) fn set_find_plan(mut self, find_plan: FindPlan) -> Self {
        self.find_plan = Some(find_plan);
        self
    }

    pub fn join<'a>(
        &'a mut self,
        foreign_cursor: &'a mut DocumentCursor,
        lookup: &'a Lookup,
    ) -> NitriteResult<JoinedDocumentCursor<'a>> {
        Ok(JoinedDocumentCursor::new(self, foreign_cursor, lookup))
    }

    pub fn project<'a>(&'a mut self, projection: Document) -> NitriteResult<ProjectedDocumentCursor<'a>> {
        Ok(ProjectedDocumentCursor::new(self, projection))
    }

    /// Returns an iterator that yields `(NitriteId, Document)` pairs.
    /// This is useful when you need to update documents after retrieving them,
    /// as it provides the NitriteId needed for efficient O(1) updates via
    /// `update_by_id`.
    pub fn iter_with_id(&mut self) -> DocumentCursorWithId<'_> {
        DocumentCursorWithId { cursor: self }
    }
}

/// An iterator adapter that yields `(NitriteId, Document)` pairs from a DocumentCursor.
/// This enables efficient updates after retrieval by providing the NitriteId.
pub struct DocumentCursorWithId<'a> {
    cursor: &'a mut DocumentCursor,
}

impl<'a> Iterator for DocumentCursorWithId<'a> {
    type Item = NitriteResult<(NitriteId, Document)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.cursor.next() {
            Some(Ok(mut doc)) => {
                // Get the NitriteId from the document
                match doc.id() {
                    Ok(id) => Some(Ok((id, doc))),
                    Err(e) => Some(Err(e)),
                }
            }
            Some(Err(e)) => Some(Err(e)),
            None => None,
        }
    }
}

impl Iterator for DocumentCursor {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        // If we have cached items, return the next one.
        if self.current_index < self.cache.len() {
            let result = self.cache[self.current_index].clone();
            self.current_index += 1;
            return Some(result);
        }

        // Otherwise, try to pull from the underlying iterator.
        if let Some(ref mut iter) = self.underlying {
            if let Some(item) = iter.next() {
                // Process after read - combine Result<T, E> handling
                let processed = item.and_then(|doc| {
                    self.processor_chain.process_after_read(doc)
                });

                self.cache.push(processed.clone());
                self.current_index += 1;
                return Some(processed);
            }
            // Once exhausted, drop the underlying iterator.
            self.underlying = None;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::doc;
    use crate::errors::{ErrorKind, NitriteError};

    fn create_document(first: &str, last: &str) -> Document {
        let doc = doc!{
            first: first,
            last: last,
        };
        doc
    }

    #[test]
    fn test_new_document_cursor() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        assert_eq!(cursor.count(), 2);
    }

    #[test]
    fn test_next() {
        let docs = [
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        assert_eq!(
            cursor
                .next()
                .unwrap()
                .unwrap()
                .get("first")
                .unwrap()
                .as_string()
                .unwrap(),
            "John"
        );
        assert_eq!(
            cursor
                .next()
                .unwrap()
                .unwrap()
                .get("first")
                .unwrap()
                .as_string()
                .unwrap(),
            "Jane"
        );
        assert!(cursor.next().is_none());
    }

    #[test]
    fn test_next_with_error() {
        let docs = [
            Ok(create_document("John", "Doe")),
            Err(NitriteError::new("Test Error", ErrorKind::IOError)),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        assert_eq!(
            cursor
                .next()
                .unwrap()
                .unwrap()
                .get("first")
                .unwrap()
                .as_string()
                .unwrap(),
            "John"
        );
        assert!(cursor.next().unwrap().is_err());
    }

    #[test]
    fn test_first() {
        let docs = [
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        assert_eq!(
            cursor
                .first()
                .unwrap()
                .unwrap()
                .get("first")
                .unwrap()
                .as_string()
                .unwrap(),
            "John"
        );
    }

    #[test]
    fn test_find_plan() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());
        assert!(cursor.find_plan().is_none());
    }

    #[test]
    fn test_set_find_plan() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let find_plan = FindPlan::new(); // Assuming FindPlan has a default() method
        let cursor = DocumentCursor::new(iter, ProcessorChain::new()).set_find_plan(find_plan.clone());
        assert!(cursor.find_plan().is_some());
        assert_eq!(cursor.find_plan().unwrap().index_descriptor(), find_plan.index_descriptor());
    }

    #[test]
    fn test_join() {
        let docs1 = vec![Ok(create_document("John", "Doe"))];
        let docs2 = vec![Ok(create_document("Jane", "Doe"))];
        let iter1 = Box::new(docs1.into_iter());
        let iter2 = Box::new(docs2.into_iter());
        let mut cursor1 = DocumentCursor::new(iter1, ProcessorChain::new());
        let mut cursor2 = DocumentCursor::new(iter2, ProcessorChain::new());
        let lookup = Lookup {
            local_field: "last".to_string(),
            foreign_field: "last".to_string(),
            target_field: "surname".to_string(),
        };
        let joined_cursor = cursor1.join(&mut cursor2, &lookup).expect("Failed to join");
        assert_eq!(joined_cursor.count(), 1);
    }

    #[test]
    fn test_project() {
        let docs = vec![Ok(create_document("John", "Doe"))];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        let projection = doc!{ "first": "John" };
        let projected_cursor = cursor.project(projection).expect("Failed to project");
        assert_eq!(projected_cursor.into_iter().count(), 1);
    }

    #[test]
    fn bench_cursor_iteration() {
        let mut docs = Vec::new();
        for i in 0..1000 {
            docs.push(Ok(create_document(&format!("John{}", i), &format!("Doe{}", i))));
        }
        let iter = Box::new(docs.into_iter());
        let cursor = DocumentCursor::new(iter, ProcessorChain::new());

        let start = std::time::Instant::now();
        let count = cursor.count();
        let elapsed = start.elapsed();

        assert_eq!(count, 1000);
        println!("Cursor iteration (1000 docs): {:?} ({:.3}µs per doc)", 
                 elapsed, 
                 elapsed.as_micros() as f64 / 1000.0);
    }

    #[test]
    fn bench_cursor_cache_reuse() {
        let docs = vec![
            Ok(create_document("John", "Doe")),
            Ok(create_document("Jane", "Doe")),
            Ok(create_document("Bob", "Smith")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());

        let start = std::time::Instant::now();
        for _ in 0..1000 {
            cursor.reset();
            while let Some(_) = cursor.next() {}
        }
        let elapsed = start.elapsed();

        println!("Cursor cache reuse (1000 iterations): {:?}", elapsed);
    }

    #[test]
    fn bench_cursor_size_operation() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let mut cursor = DocumentCursor::new(Box::new(vec![
                Ok(create_document("John", "Doe")),
                Ok(create_document("Jane", "Doe")),
            ].into_iter()), ProcessorChain::new());
            let _ = cursor.size();
        }
        let elapsed = start.elapsed();

        println!("Cursor size operation (100 iterations): {:?}", elapsed);
    }

    fn create_document_with_id(first: &str, last: &str) -> Document {
        let mut doc = doc!{
            first: first,
            last: last,
        };
        // Assign a NitriteId to the document
        let _ = doc.id(); // This creates an ID if one doesn't exist
        doc
    }

    #[test]
    fn test_iter_with_id_basic() {
        let docs = vec![
            Ok(create_document_with_id("John", "Doe")),
            Ok(create_document_with_id("Jane", "Smith")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        let items: Vec<_> = cursor.iter_with_id().collect();
        assert_eq!(items.len(), 2);
        
        // Verify we got valid IDs and documents
        let (id1, doc1) = items[0].as_ref().unwrap();
        assert!(!id1.to_string().is_empty());
        assert_eq!(doc1.get("first").unwrap().as_string().unwrap(), "John");
        
        let (id2, doc2) = items[1].as_ref().unwrap();
        assert!(!id2.to_string().is_empty());
        assert_eq!(doc2.get("first").unwrap().as_string().unwrap(), "Jane");
        
        // IDs should be different
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_iter_with_id_empty() {
        let docs: Vec<NitriteResult<Document>> = vec![];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        let items: Vec<_> = cursor.iter_with_id().collect();
        assert!(items.is_empty());
    }

    #[test]
    fn test_iter_with_id_with_error() {
        let docs = vec![
            Ok(create_document_with_id("John", "Doe")),
            Err(NitriteError::new("Test Error", ErrorKind::IOError)),
            Ok(create_document_with_id("Jane", "Smith")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        let items: Vec<_> = cursor.iter_with_id().collect();
        assert_eq!(items.len(), 3);
        
        // First item should be Ok
        assert!(items[0].is_ok());
        
        // Second item should be an error
        assert!(items[1].is_err());
        
        // Third item should be Ok
        assert!(items[2].is_ok());
    }

    #[test]
    fn test_iter_with_id_preserves_document_content() {
        let mut doc = doc!{
            "name": "Test",
            "value": 42,
            "nested": {
                "field": "data"
            }
        };
        let _ = doc.id(); // Ensure ID is assigned
        
        let docs = vec![Ok(doc.clone())];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        let items: Vec<_> = cursor.iter_with_id().collect();
        assert_eq!(items.len(), 1);
        
        let (id, retrieved_doc) = items[0].as_ref().unwrap();
        
        // Verify ID matches original
        assert_eq!(*id, doc.id().unwrap());
        
        // Verify all fields are preserved
        assert_eq!(retrieved_doc.get("name").unwrap().as_string().unwrap(), "Test");
        assert_eq!(*retrieved_doc.get("value").unwrap().as_i32().unwrap(), 42);
    }

    #[test]
    fn test_iter_with_id_after_partial_iteration() {
        let docs = vec![
            Ok(create_document_with_id("John", "Doe")),
            Ok(create_document_with_id("Jane", "Smith")),
            Ok(create_document_with_id("Bob", "Brown")),
        ];
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        // Consume one item via regular next()
        let _ = cursor.next();
        
        // Now use iter_with_id for remaining items
        let items: Vec<_> = cursor.iter_with_id().collect();
        assert_eq!(items.len(), 2);
        
        let (_, doc1) = items[0].as_ref().unwrap();
        assert_eq!(doc1.get("first").unwrap().as_string().unwrap(), "Jane");
        
        let (_, doc2) = items[1].as_ref().unwrap();
        assert_eq!(doc2.get("first").unwrap().as_string().unwrap(), "Bob");
    }

    #[test]
    fn bench_iter_with_id() {
        let mut docs = Vec::new();
        for i in 0..1000 {
            docs.push(Ok(create_document_with_id(&format!("John{}", i), &format!("Doe{}", i))));
        }
        let iter = Box::new(docs.into_iter());
        let mut cursor = DocumentCursor::new(iter, ProcessorChain::new());

        let start = std::time::Instant::now();
        let items: Vec<_> = cursor.iter_with_id().collect();
        let elapsed = start.elapsed();

        assert_eq!(items.len(), 1000);
        assert!(items.iter().all(|r| r.is_ok()));
        
        println!("iter_with_id (1000 docs): {:?} ({:.3}µs per doc)", 
                 elapsed, 
                 elapsed.as_micros() as f64 / 1000.0);
    }
}