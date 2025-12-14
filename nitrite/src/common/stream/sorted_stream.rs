use crate::{
    collection::Document,
    errors::{NitriteError, NitriteResult},
    SortOrder,
};
use icu_collator::options::CollatorOptions;
use icu_collator::{Collator, CollatorBorrowed, CollatorPreferences};

// Make SortedStream generic over the input iterator type
pub(crate) struct SortedStream {
    sorted: Vec<NitriteResult<Document>>,
    error: Option<NitriteError>,
    current_index: usize,
}

impl SortedStream {
    pub fn new<I: Iterator<Item = NitriteResult<Document>>> (
        raw_stream: I,
        sort_order: Vec<(String, SortOrder)>,
        collator: Option<CollatorBorrowed>,
    ) -> Self {
        let unsorted = raw_stream.collect::<Vec<NitriteResult<Document>>>();
        let mut error = None;

        let mut cleaned = Vec::with_capacity(unsorted.len());
        for doc in unsorted.iter() {
            if doc.is_err() {
                error = doc.as_ref().err().cloned();
                break;
            }
            cleaned.push(doc.clone());
        }

        let has_collator = collator.is_some();
        
        cleaned.sort_by(|a, b| {
            for (field, order) in sort_order.iter() {
                // Safe extraction with proper error handling - avoid double unwrap
                let a_value = match a.as_ref() {
                    Ok(doc) => match doc.get(field) {
                        Ok(val) => val,
                        Err(_) => {
                            // Field missing or error in document A
                            return std::cmp::Ordering::Less;
                        }
                    },
                    Err(_) => return std::cmp::Ordering::Less,
                };

                let b_value = match b.as_ref() {
                    Ok(doc) => match doc.get(field) {
                        Ok(val) => val,
                        Err(_) => {
                            // Field missing or error in document B
                            return std::cmp::Ordering::Greater;
                        }
                    },
                    Err(_) => return std::cmp::Ordering::Greater,
                };

                // Handle null values
                let cmp = if a_value.is_null() && !b_value.is_null() {
                    std::cmp::Ordering::Less
                } else if !a_value.is_null() && b_value.is_null() {
                    std::cmp::Ordering::Greater
                } else if a_value.is_null() && b_value.is_null() {
                    std::cmp::Ordering::Equal
                } else if a_value.is_string() && b_value.is_string() && has_collator {
                    let a = a_value.as_string().unwrap();
                    let b = b_value.as_string().unwrap();
                    collator.as_ref().map(|cb| cb.compare(a, b)).unwrap_or_else(|| a.cmp(b))
                } else if a_value.is_string() && b_value.is_string() {
                    let a = a_value.as_string().unwrap();
                    let b = b_value.as_string().unwrap();
                    a.cmp(b)
                } else {
                    a_value.cmp(&b_value)
                };

                if cmp != std::cmp::Ordering::Equal {
                    return match order {
                        SortOrder::Ascending => cmp,
                        SortOrder::Descending => cmp.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });

        Self {
            sorted: cleaned,
            error,
            current_index: 0,
        }
    }
}

impl Iterator for SortedStream {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        // fail fast if any error occurs
        if let Some(error) = self.error.clone() {
            return Some(Err(error));
        }

        if self.current_index < self.sorted.len() {
            let result = self.sorted[self.current_index].clone();
            self.current_index += 1;
            Some(result)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::Value;
    use crate::errors::{ErrorKind, NitriteError, NitriteResult};
    use crate::SortOrder;

    fn create_document(fields: Vec<(&str, &str)>) -> Document {
        let mut doc = Document::new();
        for (key, value) in fields {
            doc.put(key, value.to_string()).unwrap();
        }
        doc
    }

    #[test]
    fn test_sorted_stream_new() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value2"),
            ])),
            Ok(create_document(vec![
                ("field1", "value3"),
                ("field2", "value4"),
            ])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        assert_eq!(sorted_stream.sorted.len(), 2);
    }

    #[test]
    fn test_sorted_stream_new_with_error() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value2"),
            ])),
            Err(NitriteError::new("Test error", ErrorKind::IOError)),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_some());
        assert_eq!(sorted_stream.sorted.len(), 1);
    }

    #[test]
    fn test_sorted_stream_next() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "value3"),
                ("field2", "value4"),
            ])),
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value2"),
            ])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field1").unwrap(), "value1".into());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field1").unwrap(), "value3".into());

        let result = sorted_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_sorted_stream_next_with_error() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value2"),
            ])),
            Err(NitriteError::new("Test error", ErrorKind::IOError)),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_some());

        let result = sorted_stream.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_sorted_stream_with_collator() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "valueB"),
                ("field2", "value2"),
            ])),
            Ok(create_document(vec![
                ("field1", "valueA"),
                ("field2", "value4"),
            ])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];
        let collator = Collator::try_new(CollatorPreferences::default(), CollatorOptions::default()).unwrap();

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, Some(collator));
        assert!(sorted_stream.error.is_none());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field1").unwrap(), "valueA".into());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field1").unwrap(), "valueB".into());

        let result = sorted_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_sorted_stream_multiple_sort_orders() {
        let docs = vec![
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value2"),
            ])),
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "value1"),
            ])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![
            ("field1".to_string(), SortOrder::Ascending),
            ("field2".to_string(), SortOrder::Descending),
        ];

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field2").unwrap(), "value2".into());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field2").unwrap(), "value1".into());

        let result = sorted_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_sorted_stream_with_different_data_types() {
        let docs = vec![
            Ok(create_document(vec![("field1", "value1"), ("field2", "2")])),
            Ok(create_document(vec![
                ("field1", "value1"),
                ("field2", "10"),
            ])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field2".to_string(), SortOrder::Ascending)];

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field2").unwrap(), "10".into());

        let result = sorted_stream.next().unwrap().unwrap();
        assert_eq!(result.get("field2").unwrap(), "2".into());

        let result = sorted_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_sorted_stream_empty_raw_stream() {
        let docs: Vec<NitriteResult<Document>> = vec![];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let mut sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        assert_eq!(sorted_stream.sorted.len(), 0);

        let result = sorted_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn test_sorted_stream_with_missing_fields() {
        let docs = vec![
            Ok(create_document(vec![("field1", "value1")])),
            Ok(create_document(vec![("field2", "value2")])),
        ];
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];

        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        // documents with missing fields are not sorted, i.e., here string is compared with null
        assert_eq!(sorted_stream.sorted.len(), 2);
    }

    #[test]
    fn test_sorted_stream_with_missing_sort_field() {
        // Test that sorting on a field that doesn't exist in documents
        // doesn't panic, but handles gracefully
        let mut docs = Vec::new();
        
        let mut doc1 = Document::new();
        doc1.put("name", Value::from("Alice")).unwrap();
        docs.push(Ok(doc1));
        
        let mut doc2 = Document::new();
        doc2.put("name", Value::from("Bob")).unwrap();
        docs.push(Ok(doc2));
        
        // Sort by non-existent field
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("non_existent_field".to_string(), SortOrder::Ascending)];
        
        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        assert_eq!(sorted_stream.sorted.len(), 2);
    }

    #[test]
    fn test_sorted_stream_with_partial_missing_fields() {
        // Test sorting when some documents have the field and some don't
        let mut docs = Vec::new();
        
        let mut doc1 = Document::new();
        doc1.put("age", Value::from(30i32)).unwrap();
        docs.push(Ok(doc1));
        
        let mut doc2 = Document::new();
        doc2.put("name", Value::from("Bob")).unwrap();
        docs.push(Ok(doc2));
        
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("age".to_string(), SortOrder::Ascending)];
        
        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        assert_eq!(sorted_stream.sorted.len(), 2);
    }

    #[test]
    fn test_sorted_stream_with_all_missing_fields() {
        // Test sorting when all documents are missing the sort field
        let mut docs = Vec::new();
        
        let mut doc1 = Document::new();
        doc1.put("name", Value::from("Alice")).unwrap();
        docs.push(Ok(doc1));
        
        let mut doc2 = Document::new();
        doc2.put("name", Value::from("Bob")).unwrap();
        docs.push(Ok(doc2));
        
        let raw_stream = Box::new(docs.into_iter());
        let sort_order = vec![("score".to_string(), SortOrder::Ascending)];
        
        let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
        assert!(sorted_stream.error.is_none());
        assert_eq!(sorted_stream.sorted.len(), 2);
    }

    #[test]
    fn test_sorted_stream_handles_no_panic_on_missing_fields() {
        // Verify that the fix prevents panics when fields are missing
        for _ in 0..10 {
            let mut docs = Vec::new();
            
            let mut doc1 = Document::new();
            doc1.put("value", Value::from(10i32)).unwrap();
            docs.push(Ok(doc1));
            
            let mut doc2 = Document::new();
            // doc2 has no "value" field
            doc2.put("other", Value::from("test")).unwrap();
            docs.push(Ok(doc2));
            
            let raw_stream = Box::new(docs.into_iter());
            let sort_order = vec![("value".to_string(), SortOrder::Ascending)];
            
            // This should not panic even with missing fields
            let sorted_stream = SortedStream::new(raw_stream, sort_order, None);
            assert_eq!(sorted_stream.sorted.len(), 2);
        }
    }

    #[test]
    fn bench_sorted_stream_small() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let docs = (0..50)
                .map(|i| {
                    Ok(create_document(vec![
                        ("field1", &format!("value{}", i % 10)),
                        ("field2", &format!("data{}", i)),
                    ]))
                })
                .collect::<Vec<_>>();

            let raw_stream = Box::new(docs.into_iter());
            let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];
            let _ = SortedStream::new(raw_stream, sort_order, None);
        }
        let elapsed = start.elapsed();

        println!(
            "Sorted stream small (100x 50 docs): {:?} ({:.3}µs per sort)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_sorted_stream_medium() {
        let start = std::time::Instant::now();
        for _ in 0..10 {
            let docs = (0..500)
                .map(|i| {
                    Ok(create_document(vec![
                        ("field1", &format!("value{}", i % 50)),
                        ("field2", &format!("data{}", i)),
                    ]))
                })
                .collect::<Vec<_>>();

            let raw_stream = Box::new(docs.into_iter());
            let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];
            let _ = SortedStream::new(raw_stream, sort_order, None);
        }
        let elapsed = start.elapsed();

        println!(
            "Sorted stream medium (10x 500 docs): {:?} ({:.3}µs per sort)",
            elapsed,
            elapsed.as_micros() as f64 / 10.0
        );
    }

    #[test]
    fn bench_sorted_stream_with_collator() {
        let _collator =
            Collator::try_new(CollatorPreferences::default(), CollatorOptions::default()).unwrap();

        let start = std::time::Instant::now();
        for _ in 0..5 {
            let docs = (0..100)
                .map(|i| {
                    Ok(create_document(vec![
                        ("field1", &format!("value{}", i % 20)),
                        ("field2", &format!("data{}", i)),
                    ]))
                })
                .collect::<Vec<_>>();

            let raw_stream = Box::new(docs.into_iter());
            let sort_order = vec![("field1".to_string(), SortOrder::Ascending)];
            // Create a new collator for each iteration since CollatorBorrowed is not cloneable
            let col = Collator::try_new(CollatorPreferences::default(), CollatorOptions::default()).unwrap();
            let _ = SortedStream::new(raw_stream, sort_order, Some(col));
        }
        let elapsed = start.elapsed();

        println!(
            "Sorted stream with collator (5x 100 docs): {:?} ({:.3}µs per sort)",
            elapsed,
            elapsed.as_micros() as f64 / 5.0
        );
    }
}
