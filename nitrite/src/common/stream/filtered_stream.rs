use crate::{collection::Document, errors::NitriteResult, filter::{Filter, FilterProvider}};

pub(crate) struct FilteredStream {
    raw_stream: Box<dyn Iterator<Item=NitriteResult<Document>>>,
    filter: Filter,
}

impl FilteredStream {
    pub fn new(raw_stream: Box<dyn Iterator<Item=NitriteResult<Document>>>, filter: Filter) -> Self {
        FilteredStream { raw_stream, filter }
    }
}

impl Iterator for FilteredStream {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.raw_stream.next() {
                Some(Ok(doc)) => {
                    // Inline filter application with minimal branching
                    match self.filter.apply(&doc) {
                        Ok(true) => return Some(Ok(doc)),
                        Ok(false) => continue,
                        Err(e) => return Some(Err(e)),
                    }
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::doc;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::filter::field;

    fn create_document(field1: &str) -> Document {
        let doc = doc!{
            "field1": field1,
        };
        doc
    }

    #[test]
    fn test_filtered_stream_with_matching_document() {
        let docs = vec![
            Ok(create_document("value")),
            Ok(create_document("other_value")),
        ];
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");
        let mut filtered_stream = FilteredStream::new(iter, filter);

        let doc = filtered_stream.next().unwrap().unwrap();
        assert_eq!(doc.get("field1").unwrap().as_string().unwrap(), "value");
        assert!(filtered_stream.next().is_none());
    }

    #[test]
    fn test_filtered_stream_with_no_matching_document() {
        let docs = vec![
            Ok(create_document("other_value")),
            Ok(create_document("another_value")),
        ];
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");
        let mut filtered_stream = FilteredStream::new(iter, filter);

        assert!(filtered_stream.next().is_none());
    }

    #[test]
    fn test_filtered_stream_with_error_document() {
        let docs = vec![
            Ok(create_document("value")),
            Err(NitriteError::new("Test Error", ErrorKind::IOError)),
        ];
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");
        let mut filtered_stream = FilteredStream::new(iter, filter);

        let doc = filtered_stream.next().unwrap().unwrap();
        assert_eq!(doc.get("field1").unwrap().as_string().unwrap(), "value");

        let err = filtered_stream.next().unwrap().err().unwrap();
        assert_eq!(err.to_string(), "Test Error");
    }

    #[test]
    fn test_filtered_stream_with_filter_error() {
        let docs = vec![
            Ok(create_document("value")),
        ];
        let iter = Box::new(docs.into_iter());
        let filter = field("non_existing_field").eq("value");
        let mut filtered_stream = FilteredStream::new(iter, filter);

        let result = filtered_stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn bench_filtered_stream_matching() {
        let mut docs = Vec::new();
        for i in 0..1000 {
            docs.push(Ok(create_document(if i % 2 == 0 { "value" } else { "other" })));
        }
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");

        let start = std::time::Instant::now();
        let filtered_stream = FilteredStream::new(iter, filter);
        let count = filtered_stream.count();
        let elapsed = start.elapsed();

        assert_eq!(count, 500);
        println!("Filtered stream (1000 docs, 50% match): {:?} ({:.3}µs per doc)", 
                 elapsed, 
                 elapsed.as_micros() as f64 / 1000.0);
    }

    #[test]
    fn bench_filtered_stream_no_match() {
        let mut docs = Vec::new();
        for _ in 0..1000 {
            docs.push(Ok(create_document("other_value")));
        }
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");

        let start = std::time::Instant::now();
        let filtered_stream = FilteredStream::new(iter, filter);
        let count = filtered_stream.count();
        let elapsed = start.elapsed();

        assert_eq!(count, 0);
        println!("Filtered stream (1000 docs, no match): {:?}", elapsed);
    }

    #[test]
    fn bench_filtered_stream_all_match() {
        let mut docs = Vec::new();
        for _ in 0..1000 {
            docs.push(Ok(create_document("value")));
        }
        let iter = Box::new(docs.into_iter());
        let filter = field("field1").eq("value");

        let start = std::time::Instant::now();
        let filtered_stream = FilteredStream::new(iter, filter);
        let count = filtered_stream.count();
        let elapsed = start.elapsed();

        assert_eq!(count, 1000);
        println!("Filtered stream (1000 docs, all match): {:?} ({:.3}µs per doc)", 
                 elapsed, 
                 elapsed.as_micros() as f64 / 1000.0);
    }
}