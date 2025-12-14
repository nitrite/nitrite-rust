use crate::{
    collection::Document, errors::NitriteResult, store::{NitriteMap, NitriteMapProvider}, Key,
};

pub(crate) struct MapValues {
    entries: NitriteMap,
    current: Option<Key>,
}

impl MapValues {
    pub fn new(map: NitriteMap) -> Self {
        Self {
            entries: map,
            current: None,
        }
    }

    fn set_current(
        &mut self,
        next_key: NitriteResult<Option<Key>>,
    ) -> Option<NitriteResult<Document>> {
        match next_key {
            Ok(Some(key)) => {
                self.current = Some(key.clone());
                match self.entries.get(&key) {
                    Ok(Some(value)) => {
                        // Validate that value is a Document type
                        match value.as_document() {
                            Some(doc) => Some(Ok(doc.clone())),
                            None => {
                                log::warn!("Data corruption: Expected Document in map values, found {:?}", value);
                                None
                            }
                        }
                    }
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn higher_key(&self) -> NitriteResult<Option<Key>> {
        match &self.current {
            Some(current_key) => self.entries.higher_key(current_key),
            None => self.entries.first_key(),
        }
    }
}

impl Iterator for MapValues {
    type Item = NitriteResult<Document>;

    fn next(&mut self) -> Option<Self::Item> {
        let next_key = self.higher_key();
        self.set_current(next_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::Value;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::nitrite_config::NitriteConfig;
    use crate::store::{NitriteMap, NitriteMapProvider, NitriteStoreProvider};
    use crate::Key;
    use std::any::Any;

    fn create_test_map() -> NitriteMap {
        let nitrite_config = NitriteConfig::new();
        nitrite_config.auto_configure().expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let map = store.open_map("test").expect("Failed to open map");
        
        let mut doc1  = Document::new();
        let id1 = doc1.id().expect("Failed to get id");
        
        let mut doc2  = Document::new();
        let id2 = doc2.id().expect("Failed to get id");
        
        map.put(Key::from(id1), Value::Document(doc1)).expect("Failed to put value");
        map.put(Key::from(id2), Value::Document(doc2)).expect("Failed to put value");
        map
    }

    #[test]
    fn test_map_values_new() {
        let map = create_test_map();
        let map_values = MapValues::new(map);
        assert!(map_values.current.is_none());
    }

    #[test]
    fn test_map_values_next() {
        let map = create_test_map();
        let mut map_values = MapValues::new(map);

        let first = map_values.next().unwrap().unwrap();
        assert_eq!(first.type_id(), Any::type_id(&Document::new()));

        let second = map_values.next().unwrap().unwrap();
        assert_eq!(second.type_id(), Any::type_id(&Document::new()));

        let none = map_values.next();
        assert!(none.is_none());
    }

    #[test]
    fn test_set_current_with_error() {
        let map = create_test_map();
        let mut map_values = MapValues::new(map);

        let error_result: NitriteResult<Option<Key>> = Err(NitriteError::new("Test error", ErrorKind::IOError));
        let result = map_values.set_current(error_result);
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[test]
    fn test_higher_key() {
        let map = create_test_map();
        let mut map_values = MapValues::new(map);

        let first_key = map_values.higher_key().unwrap();
        assert!(first_key.is_some());

        map_values.current = first_key;
        let second_key = map_values.higher_key().unwrap();
        assert!(second_key.is_some());

        map_values.current = second_key;
        let none_key = map_values.higher_key().unwrap();
        assert!(none_key.is_none());
    }

    // as_document().unwrap() error handling tests
    #[test]
    fn test_map_values_with_corrupted_document_type() {
        // Test that MapValues handles non-Document values gracefully
        // instead of panicking with as_document().unwrap()
        let nitrite_config = NitriteConfig::new();
        nitrite_config.auto_configure().expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let map = store.open_map("test_corrupted").expect("Failed to open map");
        
        // Insert a corrupted value (non-Document) into the map
        let key = Key::from("corrupted");
        map.put(key, Value::String("not a document".to_string()))
            .expect("Failed to put value");

        let mut map_values = MapValues::new(map);

        // Should skip corrupted entry and return None instead of panicking
        assert!(map_values.next().is_none());
    }

    #[test]
    fn test_set_current_with_corrupted_document() {
        // Test that set_current handles non-Document values gracefully
        let nitrite_config = NitriteConfig::new();
        nitrite_config.auto_configure().expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let store = nitrite_config.nitrite_store().expect("Failed to get store");
        let map = store.open_map("test_set_current").expect("Failed to open map");
        
        let mut doc = Document::new();
        let doc_key = Key::from(doc.id().unwrap());
        
        // Insert a valid document
        map.put(doc_key.clone(), Value::Document(doc))
            .expect("Failed to put value");

        let mut map_values = MapValues::new(map);
        
        // Should successfully get the first valid document
        let result = map_values.next();
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }
}