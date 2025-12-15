use crate::{
    collection::{Document, NitriteId},
    errors::NitriteResult,
    store::{NitriteMap, NitriteMapProvider},
    Value,
};

pub(crate) struct IndexedStream {
    nitrite_map: NitriteMap,
    id_set: Vec<NitriteId>,
    current: usize,
}

impl IndexedStream {
    pub fn new(nitrite_map: NitriteMap, id_set: Vec<NitriteId>) -> Self {
        IndexedStream {
            nitrite_map,
            id_set,
            current: 0,
        }
    }
}

impl Iterator for IndexedStream {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        let id_set_len = self.id_set.len();
        loop {
            // Bounds check with early exit
            if self.current >= id_set_len {
                log::debug!("IndexedStream::next - exhausted all {} ids", id_set_len);
                return None;
            }

            let id = &self.id_set[self.current];
            self.current += 1;
            
            log::debug!("IndexedStream::next - looking up id: {:?}", id);
            
            // Inline the match for better branch prediction
            match self.nitrite_map.get(&Value::NitriteId(*id)) {
                Ok(Some(value)) => {
                    // Direct as_document() check without nested match
                    if let Some(doc) = value.as_document() {
                        log::debug!("IndexedStream::next - found document");
                        return Some(Ok(doc.clone()));
                    } else {
                        log::warn!("Data corruption: Expected Document in indexed stream, found {:?}", value);
                        // continue loop to next id
                    }
                }
                Ok(None) => {
                    log::debug!("IndexedStream::next - id not found in map");
                    // continue to next id
                }
                Err(e) => {
                    return Some(Err(e));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::nitrite_config::NitriteConfig;
    use crate::store::NitriteStoreProvider;
    use crate::Value;

    fn create_nitrite_map() -> NitriteMap {
        let config = NitriteConfig::default();
        config.auto_configure().expect("Failed to auto configure");
        config.initialize().expect("Failed to initialize");
        let store = config.nitrite_store().expect("Failed to get store");
        
        store.open_map("test").expect("Failed to open map")
    }

    fn create_document(id: &str) -> Document {
        let mut doc = Document::new();
        doc.put("id", id).expect("Failed to put value");
        doc
    }

    #[test]
    fn test_indexed_stream_with_matching_document() {
        let map = create_nitrite_map();
        let mut doc = create_document("1");
        let id = doc.id().expect("Failed to get id");
        map.put(Value::NitriteId(id), Value::from(doc.clone()))
            .unwrap();

        let id_set = vec![id];
        let mut indexed_stream = IndexedStream::new(map.clone(), id_set);

        let result = indexed_stream.next().unwrap().unwrap();
        assert_eq!(result.get("id").unwrap().as_string().unwrap(), "1");
        assert!(indexed_stream.next().is_none());
    }

    #[test]
    fn test_indexed_stream_with_no_matching_document() {
        let map = create_nitrite_map();
        let id_set = vec![NitriteId::new()];
        let mut indexed_stream = IndexedStream::new(map, id_set);

        assert!(indexed_stream.next().is_none());
    }

    #[test]
    fn test_indexed_stream_with_multiple_documents() {
        let map = create_nitrite_map();
        let mut doc1 = create_document("1");
        let id1 = doc1.id().expect("Failed to get id");
        let mut doc2 = create_document("2");
        let id2 = doc2.id().expect("Failed to get id");
        map.put(Value::NitriteId(id1), Value::from(doc1.clone()))
            .unwrap();
        map.put(Value::NitriteId(id2), Value::from(doc2.clone()))
            .unwrap();

        let id_set = vec![id1, id2];
        let mut indexed_stream = IndexedStream::new(map, id_set);

        let result1 = indexed_stream.next().unwrap().unwrap();
        assert!(
            result1.get("id").unwrap().as_string().unwrap() == "1"
                || result1.get("id").unwrap().as_string().unwrap() == "2"
        );

        let result2 = indexed_stream.next().unwrap().unwrap();
        assert!(
            result2.get("id").unwrap().as_string().unwrap() == "1"
                || result2.get("id").unwrap().as_string().unwrap() == "2"
        );

        assert!(indexed_stream.next().is_none());
    }

    // as_document().unwrap() error handling tests
    #[test]
    fn test_indexed_stream_with_corrupted_document_type() {
        // Test that IndexedStream handles non-Document values gracefully
        // instead of panicking with as_document().unwrap()
        let map = create_nitrite_map();
        let id = NitriteId::new();
        
        // Insert a corrupted value (non-Document) into the map
        map.put(Value::NitriteId(id), Value::String("not a document".to_string()))
            .unwrap();

        let id_set = vec![id];
        let mut indexed_stream = IndexedStream::new(map, id_set);

        // Should skip corrupted entry and return None instead of panicking
        assert!(indexed_stream.next().is_none());
    }

    #[test]
    fn test_indexed_stream_gracefully_handles_mixed_types() {
        // Test that IndexedStream can iterate through mixed valid and invalid entries
        let map = create_nitrite_map();
        
        let mut doc = create_document("valid");
        let valid_id = doc.id().expect("Failed to get id");
        map.put(Value::NitriteId(valid_id), Value::from(doc))
            .unwrap();
        
        let invalid_id = NitriteId::new();
        map.put(Value::NitriteId(invalid_id), Value::I32(42))
            .unwrap();

        let id_set = vec![valid_id, invalid_id];
        let mut indexed_stream = IndexedStream::new(map, id_set);

        // Should return the valid document
        let result = indexed_stream.next();
        assert!(result.is_some());
        if let Some(Ok(doc)) = result {
            assert_eq!(doc.get("id").unwrap().as_string().unwrap(), "valid");
        }

        // The invalid entry should be skipped
        assert!(indexed_stream.next().is_none());
    }

    #[test]
    fn bench_indexed_stream_iteration() {
        let map = create_nitrite_map();
        let mut id_set = Vec::new();
        
        for i in 0..100 {
            let mut doc = create_document(&i.to_string());
            let id = doc.id().expect("Failed to get id");
            map.put(Value::NitriteId(id), Value::from(doc))
                .unwrap();
            id_set.push(id);
        }

        let start = std::time::Instant::now();
        for _ in 0..10 {
            let indexed_stream = IndexedStream::new(map.clone(), id_set.clone());
            let count = indexed_stream.count();
            assert_eq!(count, 100);
        }
        let elapsed = start.elapsed();

        println!("Indexed stream iteration (10x 100 docs): {:?} ({:.3}µs per iteration)", 
                 elapsed,
                 elapsed.as_micros() as f64 / 10.0);
    }

    #[test]
    fn bench_indexed_stream_with_sparse_ids() {
        let map = create_nitrite_map();
        let mut id_set = Vec::new();
        
        // Insert 100 documents
        for i in 0..100 {
            let mut doc = create_document(&i.to_string());
            let id = doc.id().expect("Failed to get id");
            map.put(Value::NitriteId(id), Value::from(doc))
                .unwrap();
            id_set.push(id);
        }
        
        // But only reference 10 of them in the stream
        let sparse_ids: Vec<_> = id_set.iter().step_by(10).cloned().collect();

        let start = std::time::Instant::now();
        for _ in 0..100 {
            let indexed_stream = IndexedStream::new(map.clone(), sparse_ids.clone());
            let count = indexed_stream.count();
            assert_eq!(count, 10);
        }
        let elapsed = start.elapsed();

        println!("Indexed stream with sparse IDs (100x 10 docs from 100): {:?} ({:.3}µs per iteration)", 
                 elapsed,
                 elapsed.as_micros() as f64 / 100.0);
    }

    #[test]
    fn bench_indexed_stream_single_lookup() {
        let map = create_nitrite_map();
        let mut doc = create_document("single");
        let id = doc.id().expect("Failed to get id");
        map.put(Value::NitriteId(id), Value::from(doc))
            .unwrap();

        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let indexed_stream = IndexedStream::new(map.clone(), vec![id]);
            let count = indexed_stream.count();
            assert_eq!(count, 1);
        }
        let elapsed = start.elapsed();

        println!("Indexed stream single lookup (1000 iterations): {:?} ({:.3}µs per lookup)", 
                 elapsed,
                 elapsed.as_micros() as f64 / 1000.0);
    }
}
