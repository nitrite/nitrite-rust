use crate::{collection::Document, errors::NitriteResult};

// Make UnionStream generic over the iterator type
pub(crate) struct UnionStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    streams: Vec<I>,
    current_stream: Option<I>,
    remaining_streams: Vec<I>,
}

impl<I> UnionStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    pub fn new(mut streams: Vec<I>) -> Self {
        // Preallocate remaining_streams to avoid reallocation during iteration
        let mut remaining = Vec::with_capacity(streams.len().saturating_sub(1));
        
        let current_stream = if !streams.is_empty() {
            Some(streams.remove(0))
        } else {
            None
        };
        
        // Move remaining streams to remaining_streams
        remaining.extend(streams.into_iter());
        
        Self {
            streams: Vec::new(), // Keep for compatibility, but we don't use it
            current_stream,
            remaining_streams: remaining,
        }
    }
}

impl<I> Iterator for UnionStream<I>
where
    I: Iterator<Item = NitriteResult<Document>>,
{
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get from current stream if it exists
            if let Some(ref mut stream) = self.current_stream {
                if let Some(document) = stream.next() {
                    return Some(document);
                }
            }
            
            // Move to next stream if current is exhausted
            if !self.remaining_streams.is_empty() {
                self.current_stream = Some(self.remaining_streams.remove(0));
            } else {
                // All streams exhausted
                self.current_stream = None;
                return None;
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
    fn test_union_stream_empty() {
        let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = vec![];
        let mut union_stream = UnionStream::new(streams);
        assert!(union_stream.next().is_none());
    }

    #[test]
    fn test_union_stream_single_stream() {
        let doc1 = Document::new();
        let stream1 = Box::new(vec![Ok(doc1.clone())].into_iter());
        let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = vec![stream1];
        let mut union_stream = UnionStream::new(streams);
        assert_eq!(union_stream.next().unwrap().unwrap(), doc1);
        assert!(union_stream.next().is_none());
    }

    #[test]
    fn test_union_stream_multiple_streams() {
        let doc1 = Document::new();
        let doc2 = Document::new();
        let stream1 = Box::new(vec![Ok(doc1.clone())].into_iter());
        let stream2 = Box::new(vec![Ok(doc2.clone())].into_iter());
        let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = vec![stream1, stream2];
        let mut union_stream = UnionStream::new(streams);
        assert_eq!(union_stream.next().unwrap().unwrap(), doc1);
        assert_eq!(union_stream.next().unwrap().unwrap(), doc2);
        assert!(union_stream.next().is_none());
    }

    #[test]
    fn test_union_stream_with_errors() {
        let doc1 = Document::new();
        let error = NitriteError::new("Test error", ErrorKind::IOError);
        let stream1 = Box::new(vec![Ok(doc1.clone()), Err(error.clone())].into_iter());
        let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = vec![stream1];
        let mut union_stream = UnionStream::new(streams);
        assert_eq!(union_stream.next().unwrap().unwrap(), doc1);
        assert_eq!(union_stream.next().unwrap().unwrap_err().to_string(), error.to_string());
        assert!(union_stream.next().is_none());
    }

    #[test]
    fn test_union_stream_multiple_streams_with_errors() {
        let doc1 = Document::new();
        let error = NitriteError::new("Test error", ErrorKind::IOError);
        let stream1 = Box::new(vec![Ok(doc1.clone())].into_iter());
        let stream2 = Box::new(vec![Err(error.clone())].into_iter());
        let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = vec![stream1, stream2];
        let mut union_stream = UnionStream::new(streams);
        assert_eq!(union_stream.next().unwrap().unwrap(), doc1);
        assert_eq!(union_stream.next().unwrap().unwrap_err().to_string(), error.to_string());
        assert!(union_stream.next().is_none());
    }

    #[test]
    fn bench_union_stream_small() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = (0..5)
                .map(|_| {
                    let docs = (0..10)
                        .map(|_| Ok(Document::new()))
                        .collect::<Vec<_>>();
                    Box::new(docs.into_iter()) as Box<dyn Iterator<Item = NitriteResult<Document>>>
                })
                .collect();
            
            let mut union_stream = UnionStream::new(streams);
            while union_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Union stream small (100x 5 streams of 10 docs): {:?} ({:.3}µs per union)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_union_stream_medium() {
        let start = std::time::Instant::now();
        for _ in 0..50 {
            let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = (0..10)
                .map(|_| {
                    let docs = (0..50)
                        .map(|_| Ok(Document::new()))
                        .collect::<Vec<_>>();
                    Box::new(docs.into_iter()) as Box<dyn Iterator<Item = NitriteResult<Document>>>
                })
                .collect();
            
            let mut union_stream = UnionStream::new(streams);
            while union_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Union stream medium (50x 10 streams of 50 docs): {:?} ({:.3}µs per union)",
            elapsed,
            elapsed.as_micros() as f64 / 50.0
        );
    }

    #[test]
    fn bench_union_stream_large() {
        let start = std::time::Instant::now();
        for _ in 0..10 {
            let streams: Vec<Box<dyn Iterator<Item = NitriteResult<Document>>>> = (0..20)
                .map(|_| {
                    let docs = (0..100)
                        .map(|_| Ok(Document::new()))
                        .collect::<Vec<_>>();
                    Box::new(docs.into_iter()) as Box<dyn Iterator<Item = NitriteResult<Document>>>
                })
                .collect();
            
            let mut union_stream = UnionStream::new(streams);
            while union_stream.next().is_some() {}
        }
        let elapsed = start.elapsed();

        println!(
            "Union stream large (10x 20 streams of 100 docs): {:?} ({:.3}µs per union)",
            elapsed,
            elapsed.as_micros() as f64 / 10.0
        );
    }
}