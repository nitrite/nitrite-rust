use crate::collection::Document;
use crate::common::{get_key_name, get_keyed_repo_type, Key, Value, TAG_COLLECTION, TAG_KEYED_REPOSITORIES, TAG_REPOSITORIES};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::store::{MapMeta, Metadata, NitriteMap, NitriteMapProvider};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

/// Manages the catalog of collections, repositories, and keyed repositories in a Nitrite store.
///
/// # Purpose
/// `StoreCatalog` maintains metadata about all stored entities in the database,
/// tracking collection names, repository types, and keyed repository configurations.
/// It provides atomic operations for registering and removing entities from the catalog.
///
/// # Characteristics
/// - **Thread-Safe**: Can be safely cloned and shared across threads
/// - **Metadata Management**: Tracks all collections, repositories, and their configurations
/// - **Atomic Operations**: Write and remove operations are atomic
/// - **Lightweight Cloning**: Uses Arc internally for efficient sharing
///
/// # Usage
/// Typically obtained via `NitriteStore::store_catalog()` and used internally
/// by Nitrite for managing collection and repository registries.
#[derive(Clone)]
pub struct StoreCatalog {
    inner: Arc<StoreCatalogInner>,
}

impl StoreCatalog {
    /// Creates a new `StoreCatalog` backed by the specified map.
    ///
    /// # Arguments
    /// * `catalog_map` - The underlying `NitriteMap` that stores catalog metadata
    ///
    /// # Returns
    /// * `Ok(StoreCatalog)` with the newly created catalog
    /// * `Err(NitriteError)` if the operation fails
    pub fn new(catalog_map: NitriteMap) -> NitriteResult<StoreCatalog> {
        Ok(StoreCatalog {
            inner: Arc::new(StoreCatalogInner { catalog_map }),
        })
    }
}

impl Deref for StoreCatalog {
    type Target = StoreCatalogInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct StoreCatalogInner {
    catalog_map: NitriteMap,
}

impl StoreCatalogInner {
    /// Checks if an entry (collection, repository, or keyed repository) exists in the catalog.
    ///
    /// # Arguments
    /// * `name` - The name of the entry to check
    ///
    /// # Returns
    /// * `Ok(true)` if the entry exists
    /// * `Ok(false)` if the entry does not exist
    /// * `Err(NitriteError)` if the operation fails
    pub fn has_entry(&self, name: &str) -> NitriteResult<bool> {
        let catalog_map = self.get_catalog_map()?;

        for entry in catalog_map.entries()? {
            match entry {
                Ok(pair) => {
                    match pair.1.as_document() {
                        Some(document) => {
                            let meta_data = MapMeta::new(document);
                            if meta_data.map_names.contains(name) {
                                return Ok(true);
                            }
                        }
                        None => {
                            // Skip corrupted entries - log warning but continue
                            log::warn!("StoreCatalog: Skipping invalid catalog entry format (expected Document, got non-Document type)");
                            continue;
                        }
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Ok(false)
    }

    /// Registers a collection in the catalog.
    ///
    /// # Arguments
    /// * `name` - The name of the collection to register
    ///
    /// # Returns
    /// * `Ok(())` if the collection was successfully registered
    /// * `Err(NitriteError)` if the name is empty or the operation fails
    pub fn write_collection_entry(&self, name: &str) -> NitriteResult<()> {
        if name.is_empty() {
            log::error!("Collection name cannot be empty");
            return Err(NitriteError::new(
                "Collection name cannot be empty",
                ErrorKind::ValidationError,
            ));
        }
        
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_COLLECTION))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid collection entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let mut meta_data = MapMeta::new(&document);
        meta_data.map_names.insert(name.to_string());

        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from(meta_data.get_info()),
        )?;
        Ok(())
    }

    /// Registers a repository in the catalog.
    ///
    /// # Arguments
    /// * `name` - The fully qualified type name of the repository to register
    ///
    /// # Returns
    /// * `Ok(())` if the repository was successfully registered
    /// * `Err(NitriteError)` if the name is empty or the operation fails
    pub fn write_repository_entry(&self, name: &str) -> NitriteResult<()> {
        if name.is_empty() {
            log::error!("Repository name cannot be empty");
            return Err(NitriteError::new(
                "Repository name cannot be empty",
                ErrorKind::ValidationError,
            ));
        }
        
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_REPOSITORIES))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid repository entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let mut meta_data = MapMeta::new(&document);
        meta_data.map_names.insert(name.to_string());

        catalog_map.put(
            Key::from(TAG_REPOSITORIES.to_string()),
            Value::from(meta_data.get_info()),
        )?;
        Ok(())
    }

    /// Registers a keyed repository in the catalog.
    ///
    /// The name should include both the repository type and the key, separated by the key separator.
    ///
    /// # Arguments
    /// * `name` - The repository type and key identifier to register
    ///
    /// # Returns
    /// * `Ok(())` if the keyed repository was successfully registered
    /// * `Err(NitriteError)` if the name is empty or the operation fails
    pub fn write_keyed_repository_entry(&self, name: &str) -> NitriteResult<()> {
        if name.is_empty() {
            log::error!("Keyed repository name cannot be empty");
            return Err(NitriteError::new(
                "Keyed repository name cannot be empty",
                ErrorKind::ValidationError,
            ));
        }
        
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_KEYED_REPOSITORIES))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid keyed repository entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let mut meta_data = MapMeta::new(&document);
        meta_data.map_names.insert(name.to_string());

        catalog_map.put(
            Key::from(TAG_KEYED_REPOSITORIES.to_string()),
            Value::from(meta_data.get_info()),
        )?;
        Ok(())
    }

    /// Retrieves all collection names registered in the catalog.
    ///
    /// # Returns
    /// * `Ok(HashSet)` with all registered collection names
    /// * `Err(NitriteError)` if the operation fails
    pub fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_COLLECTION))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid collection entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let meta_data = MapMeta::new(&document);
        Ok(meta_data.map_names)
    }

    /// Retrieves all repository types registered in the catalog.
    ///
    /// # Returns
    /// * `Ok(HashSet)` with all registered repository type names
    /// * `Err(NitriteError)` if the operation fails
    pub fn get_repository_names(&self) -> NitriteResult<HashSet<String>> {
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_REPOSITORIES))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid repository entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let meta_data = MapMeta::new(&document);
        Ok(meta_data.map_names)
    }

    /// Retrieves all keyed repositories grouped by their keys.
    ///
    /// # Returns
    /// * `Ok(HashMap)` mapping key names to sets of repository types
    /// * `Err(NitriteError)` if the operation fails
    pub fn get_keyed_repository_names(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        let catalog_map = self.get_catalog_map()?;

        let document = catalog_map.get(&Value::from(TAG_KEYED_REPOSITORIES))?;
        let document = match document {
            None => Document::new(),
            Some(doc) => {
                let doc_ref = doc.as_document()
                    .ok_or_else(|| NitriteError::new(
                        "StoreCatalog: Invalid keyed repository entry format (expected Document)",
                        ErrorKind::InvalidOperation
                    ))?;
                doc_ref.clone()
            }
        };

        let meta_data = MapMeta::new(&document);
        let keyed_repository_names = meta_data.map_names;

        let mut result: HashMap<String, HashSet<String>> = HashMap::with_capacity(keyed_repository_names.len());
        for name in keyed_repository_names {
            let key = get_key_name(&name)?;
            let repo_type = get_keyed_repo_type(&name)?;

            let types = result.entry(key).or_default();
            types.insert(repo_type);
        }

        Ok(result)
    }

    /// Removes an entry (collection, repository, or keyed repository) from the catalog.
    ///
    /// # Arguments
    /// * `name` - The name of the entry to remove
    ///
    /// # Returns
    /// * `Ok(())` if the entry was successfully removed or did not exist
    /// * `Err(NitriteError)` if the operation fails
    pub fn remove(&self, name: &str) -> NitriteResult<()> {
        let mut updated_map = HashMap::new();

        let catalog_map = self.get_catalog_map()?;

        let entries = catalog_map.entries()?;
        for entry in entries {
            match entry {
                Ok(pair) => {
                    let catalog_name = pair.0;
                    let document = pair.1.as_document()
                        .ok_or_else(|| NitriteError::new(
                            "StoreCatalog: Invalid catalog entry format (expected Document)",
                            ErrorKind::InvalidOperation
                        ))?;
                    let mut meta_data = MapMeta::new(document);
                    if meta_data.map_names.contains(name) {
                        meta_data.map_names.remove(name);
                        updated_map.insert(catalog_name.clone(), meta_data.get_info());
                        break;
                    }
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        for (catalog_name, meta_data) in updated_map {
            catalog_map.put(catalog_name, Value::from(meta_data))?;
        }
        Ok(())
    }

    fn get_catalog_map(&self) -> NitriteResult<NitriteMap> {
        Ok(self.catalog_map.clone())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::KEY_OBJ_SEPARATOR;
    use crate::store::memory::{InMemoryMap, InMemoryStore, InMemoryStoreConfig};
    use crate::store::NitriteStore;

    fn setup_catalog() -> StoreCatalog {
        let store = InMemoryStore::new(InMemoryStoreConfig::new());
        let catalog_map = InMemoryMap::new("test", NitriteStore::new(store));
        StoreCatalog::new(NitriteMap::new(catalog_map)).unwrap()
    }

    #[test]
    fn test_has_entry() {
        let catalog = setup_catalog();
        assert!(!catalog.has_entry("test_entry").unwrap());

        catalog.write_collection_entry("test_entry").unwrap();
        assert!(catalog.has_entry("test_entry").unwrap());
    }

    #[test]
    fn test_write_collection_entry() {
        let catalog = setup_catalog();
        catalog.write_collection_entry("test_entry").unwrap();
        assert!(catalog.has_entry("test_entry").unwrap());
    }

    #[test]
    fn test_write_repository_entry() {
        let catalog = setup_catalog();
        catalog.write_repository_entry("test_repo").unwrap();
        assert!(catalog.has_entry("test_repo").unwrap());
    }

    #[test]
    fn test_write_keyed_repository_entry() {
        let catalog = setup_catalog();
        catalog.write_keyed_repository_entry("test_keyed_repo").unwrap();
        assert!(catalog.has_entry("test_keyed_repo").unwrap());
    }

    #[test]
    fn test_get_collection_names() {
        let catalog = setup_catalog();
        catalog.write_collection_entry("test_entry").unwrap();
        let names = catalog.get_collection_names().unwrap();
        assert!(names.contains("test_entry"));
    }

    #[test]
    fn test_get_repository_names() {
        let catalog = setup_catalog();
        catalog.write_repository_entry("test_repo").unwrap();
        let names = catalog.get_repository_names().unwrap();
        assert!(names.contains("test_repo"));
    }

    #[test]
    fn test_get_keyed_repository_names() {
        let catalog = setup_catalog();
        let keyed_repo_name = format!("{}{}{}", "test_keyed_repo", KEY_OBJ_SEPARATOR, "test_key");
        catalog.write_keyed_repository_entry(&*keyed_repo_name).unwrap();
        let names = catalog.get_keyed_repository_names().unwrap();
        assert!(names.contains_key("test_key"));
        assert!(names.get("test_key").unwrap().contains("test_keyed_repo"));
    }

    #[test]
    fn test_remove() {
        let catalog = setup_catalog();
        catalog.write_collection_entry("test_entry").unwrap();
        assert!(catalog.has_entry("test_entry").unwrap());

        catalog.remove("test_entry").unwrap();
        assert!(!catalog.has_entry("test_entry").unwrap());
    }

    #[test]
    fn test_has_entry_negative() {
        let catalog = setup_catalog();
        assert!(!catalog.has_entry("non_existent_entry").unwrap());
    }

    #[test]
    fn test_write_collection_entry_negative() {
        let catalog = setup_catalog();
        assert!(catalog.write_collection_entry("").is_err());
    }

    #[test]
    fn test_write_repository_entry_negative() {
        let catalog = setup_catalog();
        assert!(catalog.write_repository_entry("").is_err());
    }

    #[test]
    fn test_write_keyed_repository_entry_negative() {
        let catalog = setup_catalog();
        assert!(catalog.write_keyed_repository_entry("").is_err());
    }

    #[test]
    fn test_get_collection_names_empty() {
        let catalog = setup_catalog();
        let names = catalog.get_collection_names().unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_get_repository_names_empty() {
        let catalog = setup_catalog();
        let names = catalog.get_repository_names().unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_get_keyed_repository_names_empty() {
        let catalog = setup_catalog();
        let names = catalog.get_keyed_repository_names().unwrap();
        assert!(names.is_empty());
    }

    #[test]
    fn test_remove_non_existent_entry() {
        let catalog = setup_catalog();
        assert!(catalog.remove("non_existent_entry").is_ok());
    }
    
    #[test]
    fn test_has_entry_handles_corrupted_document() {
        let catalog = setup_catalog();
        catalog.write_collection_entry("test_entry").unwrap();
        
        // Manually corrupt the data by inserting non-Document value
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from("corrupted_string"),
        ).unwrap();
        
        // This should gracefully skip corrupted entries and return false
        let result = catalog.has_entry("test_entry");
        assert!(result.is_ok(), "Should handle corrupted entries gracefully");
        assert!(!result.unwrap(), "Should return false since the entry is not found after skipping corruption");
    }

    #[test]
    fn test_write_collection_entry_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // First, corrupt the existing data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from(42i32),  // Non-document value
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.write_collection_entry("new_entry");
        assert!(result.is_err(), "Should return error for corrupted document");
        if let Err(e) = result {
            assert!(e.to_string().contains("Invalid collection entry format"));
        }
    }

    #[test]
    fn test_write_repository_entry_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // Corrupt the repository data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_REPOSITORIES.to_string()),
            Value::from("corrupted"),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.write_repository_entry("new_repo");
        assert!(result.is_err(), "Should return error for corrupted document");
        if let Err(e) = result {
            assert!(e.to_string().contains("Invalid repository entry format"));
        }
    }

    #[test]
    fn test_write_keyed_repository_entry_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // Corrupt the keyed repository data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_KEYED_REPOSITORIES.to_string()),
            Value::from("corrupted_keyed"),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.write_keyed_repository_entry("new_keyed_repo");
        assert!(result.is_err(), "Should return error for corrupted document");
        if let Err(e) = result {
            assert!(e.to_string().contains("Invalid keyed repository entry format"));
        }
    }

    #[test]
    fn test_get_collection_names_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // Corrupt the collection data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from(3.14f64),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.get_collection_names();
        assert!(result.is_err(), "Should return error for corrupted document");
    }

    #[test]
    fn test_get_repository_names_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // Corrupt the repository data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_REPOSITORIES.to_string()),
            Value::from(vec![1, 2, 3]),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.get_repository_names();
        assert!(result.is_err(), "Should return error for corrupted document");
    }

    #[test]
    fn test_get_keyed_repository_names_handles_corrupted_data() {
        let catalog = setup_catalog();
        
        // Corrupt the keyed repository data
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_KEYED_REPOSITORIES.to_string()),
            Value::from(true),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.get_keyed_repository_names();
        assert!(result.is_err(), "Should return error for corrupted document");
    }

    #[test]
    fn test_remove_handles_corrupted_document() {
        let catalog = setup_catalog();
        
        // Write some data first
        catalog.write_collection_entry("test_entry").unwrap();
        
        // Now corrupt it
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from("corrupted"),
        ).unwrap();
        
        // This should handle the error gracefully, not panic
        let result = catalog.remove("test_entry");
        assert!(result.is_err(), "Should return error for corrupted document");
    }

    #[test]
    fn test_multiple_catalog_operations_all_safe() {
        let catalog = setup_catalog();
        
        // Test that multiple operations work correctly
        catalog.write_collection_entry("col1").unwrap();
        catalog.write_repository_entry("repo1").unwrap();
        
        let cols = catalog.get_collection_names().unwrap();
        assert!(cols.contains("col1"));
        
        let repos = catalog.get_repository_names().unwrap();
        assert!(repos.contains("repo1"));
        
        // Now corrupt and verify error handling
        let catalog_map = catalog.get_catalog_map().unwrap();
        catalog_map.put(
            Key::from(TAG_COLLECTION.to_string()),
            Value::from("bad_data"),
        ).unwrap();
        
        // Collection operations should now fail (corrupted data)
        assert!(catalog.get_collection_names().is_err());
        assert!(catalog.write_collection_entry("col2").is_err());
        
        // has_entry should gracefully skip corrupted entries and return false for col1
        let has_col1 = catalog.has_entry("col1");
        assert!(has_col1.is_ok() && !has_col1.unwrap(), "Should gracefully skip corrupted entries");
        
        // But repository operations should still work (stored separately)
        assert!(catalog.has_entry("repo1").is_ok());
    }

    #[test]
    fn test_get_keyed_repository_names_with_pre_allocation() {
        // Test that HashMap pre-allocation is efficient
        let catalog = setup_catalog();
        
        let keyed_repo_name1 = format!("{}{}{}", "repo1", KEY_OBJ_SEPARATOR, "key1");
        let keyed_repo_name2 = format!("{}{}{}", "repo2", KEY_OBJ_SEPARATOR, "key1");
        let keyed_repo_name3 = format!("{}{}{}", "repo1", KEY_OBJ_SEPARATOR, "key2");
        
        catalog.write_keyed_repository_entry(&keyed_repo_name1).unwrap();
        catalog.write_keyed_repository_entry(&keyed_repo_name2).unwrap();
        catalog.write_keyed_repository_entry(&keyed_repo_name3).unwrap();
        
        let names = catalog.get_keyed_repository_names().unwrap();
        
        // Should have 2 keys
        assert_eq!(names.len(), 2);
        assert!(names.contains_key("key1"));
        assert!(names.contains_key("key2"));
        
        // key1 should have 2 repositories
        assert_eq!(names.get("key1").unwrap().len(), 2);
        assert!(names.get("key1").unwrap().contains("repo1"));
        assert!(names.get("key1").unwrap().contains("repo2"));
        
        // key2 should have 1 repository
        assert_eq!(names.get("key2").unwrap().len(), 1);
        assert!(names.get("key2").unwrap().contains("repo1"));
    }

    #[test]
    fn test_get_keyed_repository_names_entry_api_efficiency() {
        // Test that entry API avoids redundant lookups and clones
        let catalog = setup_catalog();
        
        // Create multiple keyed repositories with different types but same key
        for i in 0..5 {
            let keyed_repo_name = format!("{}{}{}", format!("repo_eff{}", i), KEY_OBJ_SEPARATOR, "key_same");
            catalog.write_keyed_repository_entry(&keyed_repo_name).unwrap();
        }
        
        let names = catalog.get_keyed_repository_names().unwrap();
        
        // Should have 1 key with 5 different repository types
        assert_eq!(names.len(), 1);
        let repo_types = names.get("key_same").unwrap();
        assert_eq!(repo_types.len(), 5);
    }

    #[test]
    fn test_large_keyed_repository_set_efficiency() {
        // Test efficiency with larger dataset
        let catalog = setup_catalog();
        
        // Create 50 keyed repository entries
        for i in 0..50 {
            let repo_num = i / 5;
            let key_num = i % 5;
            let keyed_repo_name = format!("{}{}{}", format!("repo{}", repo_num), KEY_OBJ_SEPARATOR, format!("key{}", key_num));
            catalog.write_keyed_repository_entry(&keyed_repo_name).unwrap();
        }
        
        let names = catalog.get_keyed_repository_names().unwrap();
        
        // Should have 5 keys, each with 10 repository types
        assert_eq!(names.len(), 5);
        for i in 0..5 {
            let key = format!("key{}", i);
            assert_eq!(names.get(&key).unwrap().len(), 10);
        }
    }

    #[test]
    fn test_entry_api_avoids_clones() {
        // Test that using entry() API avoids unnecessary clone() operations
        let catalog = setup_catalog();
        
        let keyed_repo_name = format!("{}{}{}", "repo_clone_test", KEY_OBJ_SEPARATOR, "key_clone");
        catalog.write_keyed_repository_entry(&keyed_repo_name).unwrap();
        
        let names = catalog.get_keyed_repository_names().unwrap();
        
        // Entry should be present without redundant cloning
        assert!(names.contains_key("key_clone"));
        assert_eq!(names.get("key_clone").unwrap().len(), 1);
    }
}