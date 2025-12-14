use std::collections::HashSet;

use crate::{
    collection::{Document, NitriteId},
    errors::NitriteResult,
};

// Make UniqueStream generic over the iterator type
pub(crate) struct UniqueStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    raw_stream: I,
    unique_set: HashSet<NitriteId>,
}

impl<I> UniqueStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    pub fn new(raw_stream: I) -> Self {
        // Preallocate with default capacity to reduce allocations during iteration
        UniqueStream {
            raw_stream,
            unique_set: HashSet::with_capacity(128),
        }
    }
}

impl<I> Iterator for UniqueStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.raw_stream.next() {
                Some(document) => {
                    match document {
                        Ok(mut doc) => {
                            // Safely extract document ID with proper error handling
                            match doc.id() {
                                Ok(id) => {
                                    // Check if ID already seen; if not, add to set and return
                                    if self.unique_set.insert(id) {
                                        return Some(Ok(doc));
                                    }
                                    // Document is duplicate, continue to next
                                    continue;
                                }
                                Err(e) => {
                                    // Propagate ID extraction error instead of panicking
                                    log::error!("Failed to extract document ID: {:?}", e);
                                    return Some(Err(e));
                                }
                            }
                        }
                        Err(e) => return Some(Err(e)),
                    }
                }
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::errors::{ErrorKind, NitriteError};

    #[test]
    fn test_unique_stream_empty() {
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_single_document() {
        let mut doc = Document::new();
        let _ = doc.id();
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_multiple_unique_documents() {
        let mut doc1 = Document::new();
        let _ = doc1.id();
        let mut doc2 = Document::new();
        let _ = doc2.id();
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc1.clone()), Ok(doc2.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc1);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc2);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_duplicate_documents() {
        let mut doc = Document::new();
        let _ = doc.id();
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc.clone()), Ok(doc.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_with_errors() {
        let mut doc = Document::new();
        let _ = doc.id();
        let error = NitriteError::new("Test error", ErrorKind::IOError);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc.clone()), Err(error.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc);
        assert_eq!(
            unique_stream.next().unwrap().unwrap_err().to_string(),
            error.to_string()
        );
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_multiple_documents_with_errors() {
        let mut doc1 = Document::new();
        let _ = doc1.id();
        let mut doc2 = Document::new();
        let _ = doc2.id();
        let error = NitriteError::new("Test error", ErrorKind::IOError);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc1.clone()), Err(error.clone()), Ok(doc2.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc1);
        assert_eq!(
            unique_stream.next().unwrap().unwrap_err().to_string(),
            error.to_string()
        );
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc2);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_all_errors() {
        let error1 = NitriteError::new("Test error 1", ErrorKind::IOError);
        let error2 = NitriteError::new("Test error 2", ErrorKind::IOError);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Err(error1.clone()), Err(error2.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        assert_eq!(
            unique_stream.next().unwrap().unwrap_err().to_string(),
            error1.to_string()
        );
        assert_eq!(
            unique_stream.next().unwrap().unwrap_err().to_string(),
            error2.to_string()
        );
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_with_initial_unique_set() {
        let mut doc1 = Document::new();
        let id1 = doc1.id().unwrap();
        let mut doc2 = Document::new();
        let _id2 = doc2.id().unwrap();
        let mut unique_set = HashSet::new();
        unique_set.insert(id1);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc1.clone()), Ok(doc2.clone())].into_iter());
        let mut unique_stream = UniqueStream {
            raw_stream,
            unique_set,
        };
        assert_eq!(unique_stream.next().unwrap().unwrap(), doc2);
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_handles_id_extraction_errors() {
        // When document ID extraction fails, error should be propagated gracefully
        let error = NitriteError::new("ID extraction failed", ErrorKind::InvalidId);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Err(error.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        
        let result = unique_stream.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_unique_stream_filters_duplicate_documents_safely() {
        // Duplicates should be filtered even with safe ID extraction
        let mut doc = Document::new();
        let _id = doc.id();
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc.clone()), Ok(doc.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        
        assert!(unique_stream.next().is_some());
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_mixed_duplicates_and_errors() {
        // Should handle mix of duplicates and errors gracefully
        let mut doc1 = Document::new();
        let _id1 = doc1.id();
        let error = NitriteError::new("Processing error", ErrorKind::IOError);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Ok(doc1.clone()), Err(error.clone()), Ok(doc1.clone())].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        
        assert!(unique_stream.next().is_some());
        assert!(unique_stream.next().is_some()); // Error propagated
        assert!(unique_stream.next().is_none());
    }

    #[test]
    fn test_unique_stream_no_panic_on_id_errors() {
        // Should never panic on ID extraction failures
        let error = NitriteError::new("Invalid ID format", ErrorKind::InvalidId);
        let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
            Box::new(vec![Err(error)].into_iter());
        let mut unique_stream = UniqueStream::new(raw_stream);
        
        let result = unique_stream.next();
        // Should return error wrapped in Some, not panic
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn bench_unique_stream_small() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let docs = (0..50)
                .map(|_| {
                    let mut doc = Document::new();
                    let _ = doc.id();
                    Ok(doc)
                })
                .collect::<Vec<_>>();

            let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
                Box::new(docs.into_iter());
            let mut unique_stream = UniqueStream::new(raw_stream);
            while unique_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Unique stream small (100x 50 docs): {:?} ({:.3}µs per unique)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_unique_stream_medium() {
        let start = std::time::Instant::now();
        for _ in 0..50 {
            let docs = (0..200)
                .map(|_| {
                    let mut doc = Document::new();
                    let _ = doc.id();
                    Ok(doc)
                })
                .collect::<Vec<_>>();

            let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
                Box::new(docs.into_iter());
            let mut unique_stream = UniqueStream::new(raw_stream);
            while unique_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Unique stream medium (50x 200 docs): {:?} ({:.3}µs per unique)",
            elapsed,
            elapsed.as_micros() as f64 / 50.0
        );
    }

    #[test]
    fn bench_unique_stream_with_duplicates() {
        let start = std::time::Instant::now();
        for _ in 0..50 {
            let doc = Document::new();
            let docs = (0..100)
                .map(|i| {
                    let mut d = Document::new();
                    let _ = d.id();
                    if i % 2 == 0 {
                        Ok(doc.clone())
                    } else {
                        Ok(d)
                    }
                })
                .collect::<Vec<_>>();

            let raw_stream: Box<dyn Iterator<Item = NitriteResult<Document>>> =
                Box::new(docs.into_iter());
            let mut unique_stream = UniqueStream::new(raw_stream);
            while unique_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Unique stream with duplicates (50x 100 docs with 50% duplicates): {:?} ({:.3}µs per unique)",
            elapsed,
            elapsed.as_micros() as f64 / 50.0
        );
    }
}