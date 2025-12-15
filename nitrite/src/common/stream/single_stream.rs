use crate::{collection::Document, errors::NitriteResult};

pub(crate) struct SingleStream {
    pub(crate) document: Option<Document>,
}

impl SingleStream {
    pub fn new(document: Option<Document>) -> Self {
        Self {
            document
        }
    }
}

impl Iterator for SingleStream {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        // Avoid take() allocation - directly match and consume
        self.document.take().map(Ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;

    #[test]
    fn test_single_stream_new_with_document() {
        let doc = Document::new();
        let stream = SingleStream::new(Some(doc.clone()));
        assert!(stream.document.is_some());
        assert_eq!(stream.document.unwrap(), doc);
    }

    #[test]
    fn test_single_stream_new_without_document() {
        let stream = SingleStream::new(None);
        assert!(stream.document.is_none());
    }

    #[test]
    fn test_single_stream_next_with_document() {
        let doc = Document::new();
        let mut stream = SingleStream::new(Some(doc.clone()));
        let result = stream.next().unwrap().unwrap();
        assert_eq!(result, doc);
        assert!(stream.document.is_none());
    }

    #[test]
    fn test_single_stream_next_without_document() {
        let mut stream = SingleStream::new(None);
        let result = stream.next();
        assert!(result.is_none());
    }

    #[test]
    fn bench_single_stream_with_document() {
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let doc = Document::new();
            let mut stream = SingleStream::new(Some(doc));
            let _ = stream.next().unwrap().unwrap();
        }
        let elapsed = start.elapsed();

        println!(
            "Single stream iteration (10000x): {:?} ({:.3}µs per operation)",
            elapsed,
            elapsed.as_micros() as f64 / 10000.0
        );
    }

    #[test]
    fn bench_single_stream_without_document() {
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let mut stream = SingleStream::new(None);
            let _ = stream.next();
        }
        let elapsed = start.elapsed();

        println!(
            "Single stream empty (10000x): {:?} ({:.3}µs per operation)",
            elapsed,
            elapsed.as_micros() as f64 / 10000.0
        );
    }

    #[test]
    fn bench_single_stream_exhaustion() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let doc = Document::new();
            let mut stream = SingleStream::new(Some(doc));
            // Call next() twice to test exhaustion pattern
            let _ = stream.next();
            let _ = stream.next();
        }
        let elapsed = start.elapsed();

        println!(
            "Single stream exhaustion (1000x): {:?} ({:.3}µs per iteration)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }
}