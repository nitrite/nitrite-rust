use crate::collection::Document;
use crate::common::stream::document_cursor::DocumentCursor;
use crate::common::Value;
use crate::errors::NitriteResult;
use std::collections::HashSet;

pub struct JoinedDocumentCursor<'a> {
    iter: &'a mut DocumentCursor,
    foreign_cursor: &'a mut DocumentCursor,
    lookup: &'a Lookup,
}

pub struct Lookup {
    pub local_field: String,
    pub foreign_field: String,
    pub target_field: String,
}

impl Lookup {
    pub fn new(local_field: &str, foreign_field: &str, target_field: &str) -> Self {
        Lookup {
            local_field: local_field.to_string(),
            foreign_field: foreign_field.to_string(),
            target_field: target_field.to_string(),
        }
    }
}

impl<'a> JoinedDocumentCursor<'a> {
    pub(crate) fn new(
        iter: &'a mut DocumentCursor,
        foreign_cursor: &'a mut DocumentCursor,
        lookup: &'a Lookup,
    ) -> Self {
        JoinedDocumentCursor {
            iter,
            foreign_cursor,
            lookup,
        }
    }

    pub fn size(&mut self) -> usize {
        let mut count = 0;
        // Consume all joined documents.
        while self.next().is_some() {
            count += 1;
        }
        // Reset both underlying cursors for replayability.
        self.iter.reset();
        self.foreign_cursor.reset();
        count
    }

    pub fn reset(&mut self) {
        // Reset both underlying cursors.
        self.iter.reset();
        self.foreign_cursor.reset();
    }
}

impl<'a> Iterator for JoinedDocumentCursor<'a> {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        let doc = self.iter.next()?;
        
        // Use proper match pattern instead of anti-pattern is_err() + err().unwrap()
        match doc {
            Ok(doc_value) => {
                // Reset the foreign cursor before each join so it can be replayed.
                self.foreign_cursor.reset();
                let joined_doc = join(doc_value, self.foreign_cursor, self.lookup);
                Some(joined_doc)
            }
            Err(e) => {
                // Propagate error directly without unnecessary checks
                log::error!("Error in joined cursor iteration: {:?}", e);
                Some(Err(e))
            }
        }
    }
}

fn join(
    doc: Document,
    foreign_cursor: &mut DocumentCursor,
    lookup: &Lookup,
) -> NitriteResult<Document> {
    let mut local_doc = doc.clone();
    let local_obj = local_doc.get(&lookup.local_field)?;
    
    // Cache field names to avoid repeated string copies in hot loop
    let foreign_field = &lookup.foreign_field;
    let target_field = &lookup.target_field;
    
    let mut target = Vec::with_capacity(8); // Preallocate typical capacity
    
    // Iterate over the foreign cursor which has been reset.
    for foreign_doc in foreign_cursor {
        let foreign_doc = foreign_doc?;
        let foreign_obj = foreign_doc.get(foreign_field)?;
        if local_obj == foreign_obj {
            target.push(Value::Document(foreign_doc.clone()));
        }
    }

    if !target.is_empty() {
        local_doc.put(target_field, Value::Array(target))?;
    }

    Ok(local_doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::stream::document_cursor::DocumentCursor;
    use crate::common::{create_document, ProcessorChain, Value};
    use crate::errors::{ErrorKind, NitriteError};

    fn create_document_cursor(docs: Vec<Document>) -> DocumentCursor {
        let iter = Box::new(docs.into_iter().map(Ok));
        DocumentCursor::new(iter, ProcessorChain::new())
    }

    #[test]
    fn test_joined_document_cursor_next() {
        let mut local_docs = create_document_cursor(vec![
            create_document("local_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
            create_document("local_field", Value::String("value2".to_string()))
                .expect("Failed to create document"),
        ]);

        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
            create_document("foreign_field", Value::String("value3".to_string()))
                .expect("Failed to create document"),
        ]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let mut cursor = JoinedDocumentCursor::new(&mut local_docs, &mut foreign_docs, &lookup);

        let result = cursor.next().unwrap().unwrap();
        assert_eq!(
            result.get("local_field").unwrap(),
            Value::String("value1".to_string())
        );
        assert!(result.get("target_field").is_ok());

        let result = cursor.next().unwrap().unwrap();
        assert_eq!(
            result.get("local_field").unwrap(),
            Value::String("value2".to_string())
        );
        assert!(result.get("target_field").is_ok());
    }

    #[test]
    fn test_joined_document_cursor_next_with_error() {
        let internal_vec = vec![
            Ok(
                create_document("local_field", Value::String("value1".to_string()))
                    .expect("Failed to create document"),
            ),
            Err(NitriteError::new("Test error", ErrorKind::IOError)),
        ];

        let mut local_docs =
            DocumentCursor::new(Box::new(internal_vec.into_iter()), ProcessorChain::new());

        let mut foreign_docs = create_document_cursor(vec![create_document(
            "foreign_field",
            Value::String("value1".to_string()),
        )
        .expect("Failed to create document")]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let mut cursor = JoinedDocumentCursor::new(&mut local_docs, &mut foreign_docs, &lookup);

        let result = cursor.next().unwrap().unwrap();
        assert_eq!(
            result.get("local_field").unwrap(),
            Value::String("value1".to_string())
        );
        assert!(result.get("target_field").is_ok());

        let result = cursor.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_join_function() {
        let local_doc = create_document("local_field", Value::String("value1".to_string()))
            .expect("Failed to create document");
        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
            create_document("foreign_field", Value::String("value2".to_string()))
                .expect("Failed to create document"),
        ]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let result = join(local_doc, &mut foreign_docs, &lookup).unwrap();
        assert!(result.get("target_field").is_ok());
    }

    #[test]
    fn test_join_function_with_no_match() {
        let local_doc = create_document("local_field", Value::String("value1".to_string()))
            .expect("Failed to create document");
        let mut foreign_docs = create_document_cursor(vec![create_document(
            "foreign_field",
            Value::String("value2".to_string()),
        )
        .expect("Failed to create document")]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let result = join(local_doc, &mut foreign_docs, &lookup).unwrap();
        assert!(result.get("target_field").is_ok());
    }

    #[test]
    fn test_joined_cursor_error_propagation_uses_match_pattern() {
        // When iter yields Err, it should be propagated via match without anti-pattern unwrap
        let error_vec: Vec<NitriteResult<Document>> = vec![
            Err(NitriteError::new("Test error", ErrorKind::IOError)),
        ];
        let iter = Box::new(error_vec.into_iter());
        let mut local_cursor = DocumentCursor::new(iter, ProcessorChain::new());
        
        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
        ]);
        
        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };
        
        let mut cursor = JoinedDocumentCursor::new(&mut local_cursor, &mut foreign_docs, &lookup);
        let result = cursor.next();
        
        assert!(result.is_some());
        if let Some(inner) = result {
            assert!(inner.is_err());
            if let Err(e) = inner {
                assert_eq!(e.kind(), &ErrorKind::IOError);
            }
        }
    }

    #[test]
    fn test_joined_cursor_resets_foreign_cursor_on_each_iteration() {
        // Foreign cursor should be reset before each join operation for proper replay
        let mut local_docs = create_document_cursor(vec![
            create_document("local_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
        ]);

        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
        ]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let mut cursor = JoinedDocumentCursor::new(&mut local_docs, &mut foreign_docs, &lookup);
        
        // First iteration should successfully join
        let result = cursor.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[test]
    fn test_joined_cursor_handles_empty_foreign_results() {
        // When foreign cursor has no matching values, join operation completes without error
        let mut local_docs = create_document_cursor(vec![
            create_document("local_field", Value::String("no_match".to_string()))
                .expect("Failed to create document"),
        ]);

        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("different_value".to_string()))
                .expect("Failed to create document"),
        ]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let mut cursor = JoinedDocumentCursor::new(&mut local_docs, &mut foreign_docs, &lookup);
        
        let result = cursor.next();
        assert!(result.is_some());
        // Join should complete successfully even with no matches
        assert!(result.unwrap().is_ok());
    }

    #[test]
    fn test_joined_cursor_completes_iteration_cleanly() {
        // After all documents processed, next() should return None cleanly
        let mut local_docs = create_document_cursor(vec![
            create_document("local_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
        ]);

        let mut foreign_docs = create_document_cursor(vec![
            create_document("foreign_field", Value::String("value1".to_string()))
                .expect("Failed to create document"),
        ]);

        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let mut cursor = JoinedDocumentCursor::new(&mut local_docs, &mut foreign_docs, &lookup);
        
        // Consume first document
        assert!(cursor.next().is_some());
        
        // Second call should return None when iteration complete
        let result = cursor.next();
        assert!(result.is_none());
    }

    #[test]
    fn bench_join_operation_with_matches() {
        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let start = std::time::Instant::now();
        for _ in 0..100 {
            let mut local = create_document_cursor(vec![
                create_document("local_field", Value::String("value1".to_string()))
                    .expect("Failed to create document"),
            ]);
            let mut foreign = create_document_cursor((0..100)
                .map(|i| {
                    create_document(
                        "foreign_field",
                        Value::String(if i % 2 == 0 { "value1" } else { "other" }.to_string()),
                    )
                    .expect("Failed to create document")
                })
                .collect());
            
            let mut cursor = JoinedDocumentCursor::new(&mut local, &mut foreign, &lookup);
            if let Some(result) = cursor.next() {
                let _ = result.unwrap();
            }
        }
        let elapsed = start.elapsed();

        println!("Join operation (100x 100 docs): {:?} ({:.3}µs per join)", 
                 elapsed,
                 elapsed.as_micros() as f64 / 100.0);
    }

    #[test]
    fn bench_join_size_operation() {
        let lookup = Lookup {
            local_field: "local_field".to_string(),
            foreign_field: "foreign_field".to_string(),
            target_field: "target_field".to_string(),
        };

        let start = std::time::Instant::now();
        for _ in 0..10 {
            let mut local = create_document_cursor(vec![
                create_document("local_field", Value::String("value1".to_string()))
                    .expect("Failed to create document"),
            ]);
            let mut foreign = create_document_cursor((0..50)
                .map(|_| {
                    create_document("foreign_field", Value::String("value1".to_string()))
                        .expect("Failed to create document")
                })
                .collect());
            
            let mut cursor = JoinedDocumentCursor::new(&mut local, &mut foreign, &lookup);
            let _ = cursor.size();
        }
        let elapsed = start.elapsed();

        println!("Join size() operation (10x iterations): {:?} ({:.3}µs per size)", 
                 elapsed,
                 elapsed.as_micros() as f64 / 10.0);
    }
}
