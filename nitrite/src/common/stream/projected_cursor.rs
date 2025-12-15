use crate::collection::Document;
use crate::common::stream::document_cursor::DocumentCursor;
use crate::errors::NitriteResult;

pub struct ProjectedDocumentCursor<'a> {
    cursor: &'a mut DocumentCursor,
    projection: Document,
}

impl<'a> ProjectedDocumentCursor<'a> {
    pub(crate) fn new(cursor: &'a mut DocumentCursor, projection: Document) -> Self {
        ProjectedDocumentCursor { cursor, projection }
    }

    /// Resets the projected cursor by resetting the underlying DocumentCursor.
    pub fn reset(&mut self) {
        self.cursor.reset();
    }

    pub fn size(&mut self) -> usize {
        // Reset the underlying DocumentCursor.
        self.reset();
        // Count items without extra allocations
        let count = (&mut *self.cursor).count();
        // Reset again for replayability.
        self.reset();
        count
    }
}

impl<'a> Iterator for ProjectedDocumentCursor<'a> {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        self.cursor.next().map(|doc_result| {
            doc_result.and_then(|doc| project(doc, &self.projection))
        })
    }
}

fn project(doc: Document, projection: &Document) -> NitriteResult<Document> {
    let mut projected_doc = Document::new();
    let fields = projection.fields();
    for field in fields {
        let value = doc.get(&field)?;
        projected_doc.put(&field, value.clone())?;
    }
    Ok(projected_doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::stream::document_cursor::DocumentCursor;
    use crate::common::{ProcessorChain, Value};
    use crate::errors::{ErrorKind, NitriteError, NitriteResult};

    fn create_document(fields: Vec<(&str, Value)>) -> Document {
        let mut doc = Document::new();
        for (key, value) in fields {
            doc.put(key, value).unwrap();
        }
        doc
    }

    #[test]
    fn test_projected_document_cursor_next() {
        let docs = vec![
            Ok(create_document(vec![(
                "field1",
                Value::String("value1".to_string()),
            )])),
            Ok(create_document(vec![(
                "field1",
                Value::String("value2".to_string()),
            )])),
        ];
        let mut cursor = DocumentCursor::new(Box::new(docs.into_iter()), ProcessorChain::new());
        let projection = create_document(vec![("field1", Value::String("".to_string()))]);

        let mut projected_cursor = ProjectedDocumentCursor::new(&mut cursor, projection);

        let iter = &mut projected_cursor;
        let result = iter.next().unwrap().unwrap();
        assert_eq!(
            result.get("field1").unwrap(),
            Value::String("value1".to_string())
        );

        let result = iter.next().unwrap().unwrap();
        assert_eq!(
            result.get("field1").unwrap(),
            Value::String("value2".to_string())
        );

        let result = iter.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_projected_document_cursor_next_with_error() {
        let docs = vec![
            Ok(create_document(vec![(
                "field1",
                Value::String("value1".to_string()),
            )])),
            Err(NitriteError::new("Test error", ErrorKind::IOError)),
        ];
        let mut cursor = DocumentCursor::new(Box::new(docs.into_iter()), ProcessorChain::new());
        let projection = create_document(vec![("field1", Value::String("".to_string()))]);

        let mut projected_cursor = ProjectedDocumentCursor::new(&mut cursor, projection);

        let iter = &mut projected_cursor;
        let result = iter.next().unwrap().unwrap();
        assert_eq!(
            result.get("field1").unwrap(),
            Value::String("value1".to_string())
        );

        let next_result = iter.next();
        assert!(next_result.is_some());
        assert!(next_result.unwrap().is_err());
    }

    #[test]
    fn test_project_function() {
        let doc = create_document(vec![
            ("field1", Value::String("value1".to_string())),
            ("field2", Value::String("value2".to_string())),
        ]);
        let projection = create_document(vec![("field1", Value::String("".to_string()))]);

        let result = project(doc, &projection).unwrap();
        assert_eq!(
            result.get("field1").unwrap(),
            Value::String("value1".to_string())
        );
        assert!(result.get("field2").unwrap().is_null());
    }

    #[test]
    fn test_project_function_with_error() {
        let doc = create_document(vec![("field1", Value::String("value1".to_string()))]);
        let projection = create_document(vec![("field2", Value::String("".to_string()))]);

        let result = project(doc, &projection);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(result.get("field1").unwrap().is_null());
    }

    #[test]
    fn bench_projected_cursor_iteration() {
        let docs: Vec<NitriteResult<Document>> = (0..1000)
            .map(|i| {
                Ok(create_document(vec![
                    ("field1", Value::String(format!("value{}", i))),
                    ("field2", Value::I32(i)),
                ]))
            })
            .collect();

        let start = std::time::Instant::now();
        for _ in 0..10 {
            let mut cursor = DocumentCursor::new(
                Box::new(docs.clone().into_iter()),
                ProcessorChain::new(),
            );
            let projection =
                create_document(vec![("field1", Value::String("".to_string()))]);

            let projected_cursor = ProjectedDocumentCursor::new(&mut cursor, projection);
            let count = projected_cursor.count();
            assert_eq!(count, 1000);
        }
        let elapsed = start.elapsed();

        println!(
            "Projected cursor iteration (10x 1000 docs): {:?} ({:.3}µs per iteration)",
            elapsed,
            elapsed.as_micros() as f64 / 10.0
        );
    }

    #[test]
    fn bench_projected_cursor_size() {
        let docs: Vec<NitriteResult<Document>> = (0..100)
            .map(|i| {
                Ok(create_document(vec![
                    ("field1", Value::String(format!("value{}", i))),
                    ("field2", Value::I32(i)),
                ]))
            })
            .collect();

        let start = std::time::Instant::now();
        for _ in 0..100 {
            let mut cursor = DocumentCursor::new(
                Box::new(docs.clone().into_iter()),
                ProcessorChain::new(),
            );
            let projection =
                create_document(vec![("field1", Value::String("".to_string()))]);

            let mut projected_cursor = ProjectedDocumentCursor::new(&mut cursor, projection);
            let _ = projected_cursor.size();
        }
        let elapsed = start.elapsed();

        println!(
            "Projected cursor size() (100x 100 docs): {:?} ({:.3}µs per size)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_projected_cursor_with_multiple_fields() {
        let docs: Vec<NitriteResult<Document>> = (0..500)
            .map(|i| {
                Ok(create_document(vec![
                    ("field1", Value::String(format!("value{}", i))),
                    ("field2", Value::I32(i)),
                    ("field3", Value::String(format!("data{}", i))),
                ]))
            })
            .collect();

        let start = std::time::Instant::now();
        for _ in 0..10 {
            let mut cursor = DocumentCursor::new(
                Box::new(docs.clone().into_iter()),
                ProcessorChain::new(),
            );
            let projection = create_document(vec![
                ("field1", Value::String("".to_string())),
                ("field3", Value::String("".to_string())),
            ]);

            let projected_cursor = ProjectedDocumentCursor::new(&mut cursor, projection);
            let count = projected_cursor.count();
            assert_eq!(count, 500);
        }
        let elapsed = start.elapsed();

        println!(
            "Projected cursor multi-field (10x 500 docs): {:?} ({:.3}µs per iteration)",
            elapsed,
            elapsed.as_micros() as f64 / 10.0
        );
    }
}
