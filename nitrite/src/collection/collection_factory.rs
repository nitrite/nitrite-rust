use super::{default_nitrite_collection::DefaultNitriteCollection, NitriteCollection};
use crate::{
    common::{atomic, Atomic, LockRegistry}, errors::{ErrorKind, NitriteError, NitriteResult}, nitrite_config::NitriteConfig, store::NitriteStoreProvider, PersistentCollection
};
use dashmap::DashMap;
use std::sync::Arc;
use std::{collections::HashMap, ops::Deref};

#[derive(Clone)]
pub(crate) struct CollectionFactory {
    inner: Arc<CollectionFactoryInner>,
}

impl CollectionFactory {
    pub fn new(lock_registry: LockRegistry) -> Self {
        CollectionFactory {
            inner: Arc::new(CollectionFactoryInner::new(lock_registry)),
        }
    }
}

impl Deref for CollectionFactory {
    type Target = Arc<CollectionFactoryInner>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub(crate) struct CollectionFactoryInner {
    collection_map: Atomic<HashMap<String, NitriteCollection>>,
    lock_registry: LockRegistry,
}

impl CollectionFactoryInner {
    fn new(lock_registry: LockRegistry) -> Self {
        Self {
            collection_map: atomic(HashMap::new()),
            lock_registry,
        }
    }

    pub fn has_collection(&self, name: &str) -> NitriteResult<bool> {
        // Use collection_map's own synchronization, not the lock registry
        // The lock registry is for collection data operations, not factory state
        Ok(self.collection_map.read().contains_key(name))
    }

    pub fn get_collection(
        &self,
        name: &str,
        nitrite_config: NitriteConfig,
        write_catalog: bool,
    ) -> NitriteResult<NitriteCollection> {
        // Use collection_map's own synchronization for factory state
        // Don't use lock_registry here as it will be used by the collection itself,
        // and calling collection methods while holding the same lock causes deadlock
        let collection_opt = self.collection_map.read().get(name).cloned();
        
        match collection_opt {
            Some(collection) => {
                if collection.is_dropped()? || !collection.is_open()? {
                    self.collection_map.write().remove(name);
                    return self.create_collection(name, nitrite_config, write_catalog);
                }
                return Ok(collection);
            }
            None => return self.create_collection(name, nitrite_config, write_catalog),
        }
    }

    fn create_collection(
        &self,
        name: &str,
        nitrite_config: NitriteConfig,
        write_catalog: bool,
    ) -> NitriteResult<NitriteCollection> {
        // Early exit if collection already exists
        if self.collection_map.read().contains_key(name) {
            log::error!("A collection with name {} already exists", name);
            return Err(NitriteError::new(
                "A collection with same name already exists",
                ErrorKind::ValidationError,
            ));
        }

        let store = nitrite_config.nitrite_store()?;

        if write_catalog {
            let repository_registry = store.get_repository_registry()?;
            if repository_registry.contains(name) {
                log::error!("A repository with name {} already exists", name);
                return Err(NitriteError::new(
                    "A repository with same name already exists",
                    ErrorKind::ValidationError,
                ));
            }

            let keyed_repository_registry = store.get_keyed_repository_registry()?;
            for set in keyed_repository_registry.values() {
                if set.contains(name) {
                    log::error!("A keyed repository with name {} already exists", name);
                    return Err(NitriteError::new(
                        "A keyed repository with same name already exists",
                        ErrorKind::ValidationError,
                    ));
                }
            }
        }

        let nitrite_map = store.open_map(name)?;
        let lock_handle = self.lock_registry.get_lock(name);

        let collection = NitriteCollection::new(DefaultNitriteCollection::new(
            name,
            nitrite_map.clone(),
            nitrite_config.clone(),
            lock_handle,
        )?);

        // Insert into map before catalog write for atomic behavior
        self.collection_map.write()
            .insert(name.to_string(), collection.clone());

        if write_catalog {
            let store = nitrite_config.nitrite_store()?;
            let store_catalog = store.store_catalog()?;
            if !store_catalog.has_entry(name)? {
                store_catalog.write_collection_entry(name)?;
            }
        }

        Ok(collection)
    }

    pub fn destroy_collection(&self, name: &str) -> NitriteResult<()> {
        if let Some(collection) = self.collection_map.write().remove(name) {
            collection.close()?;
        }
        Ok(())
    }

    pub fn clear(&self) -> NitriteResult<()> {
        for collection in self.collection_map.read().values() {
            if collection.is_open()? {
                collection.close()?;
            }
        }
        self.collection_map.write().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nitrite_config::NitriteConfig;

    fn setup_collection_factory() -> CollectionFactory {
        CollectionFactory::new(LockRegistry::default())
    }

    #[test]
    fn test_has_collection() {
        let factory = setup_collection_factory();
        assert!(!factory
            .has_collection("test_collection")
            .expect("Failed to check collection"));
    }

    #[test]
    fn test_get_collection() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        assert!(factory.get_collection("test_collection", nitrite_config, true).is_ok());
    }

    #[test]
    fn test_create_collection() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        let result = factory.create_collection("test_collection", nitrite_config, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy_collection() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        factory
            .create_collection("test_collection", nitrite_config, true)
            .unwrap();
        let result = factory.destroy_collection("test_collection");
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let factory = setup_collection_factory();
        let result = factory.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_collection_duplicate() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");
        factory
            .create_collection("test_collection", nitrite_config.clone(), true)
            .unwrap();
        let result = factory.create_collection("test_collection", nitrite_config, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_early_exit_duplicate_check() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");

        // Create first collection
        factory
            .create_collection("early_exit_test", nitrite_config.clone(), false)
            .unwrap();

        // Try to create duplicate - should fail fast before store operations
        let start = std::time::Instant::now();
        let result = factory.create_collection("early_exit_test", nitrite_config.clone(), false);
        let _elapsed = start.elapsed();

        // Main assertion: duplicate check fails with correct error
        assert!(result.is_err());

        // Secondary: Verify error message is about duplicate collection
        if let Err(e) = result {
            assert!(e.to_string().contains("already exists"));
        }
    }

    #[test]
    fn test_collection_factory_string_efficiency() {
        let factory = setup_collection_factory();
        let nitrite_config = NitriteConfig::default();
        nitrite_config
            .auto_configure()
            .expect("Failed to auto configure");
        nitrite_config.initialize().expect("Failed to initialize");

        // Create multiple collections
        let start = std::time::Instant::now();
        for i in 0..10 {
            let name = format!("test_coll_{}", i);
            factory
                .create_collection(&name, nitrite_config.clone(), false)
                .ok();
        }
        let elapsed = start.elapsed();

        println!("Created 10 collections in {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }
}
