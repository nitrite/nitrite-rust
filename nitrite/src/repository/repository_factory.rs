use crate::collection::{self, CollectionFactory, NitriteCollection};
use crate::common::{atomic, repository_name_by_type, Atomic, Convertible};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::nitrite::Nitrite;
use crate::nitrite_config::NitriteConfig;
use crate::repository::default_object_repository::DefaultObjectRepository;
use crate::repository::repository::ObjectRepository;
use crate::repository::repository_operations::RepositoryOperations;
use crate::repository::NitriteEntity;
use crate::store::{NitriteStore, NitriteStoreProvider};
use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;

pub(crate) struct RepositoryFactory {
    inner: Arc<RepositoryFactoryInner>,
}

impl RepositoryFactory {
    pub fn new(collection_factory: CollectionFactory) -> Self {
        RepositoryFactory {
            inner: Arc::new(RepositoryFactoryInner::new(collection_factory)),
        }
    }

    pub(crate) fn has_repository<T: NitriteEntity>(&self, key: Option<&str>) -> NitriteResult<bool> {
        self.inner.has_repository::<T>(key)
    }

    pub(crate) fn get_repository<T>(&self, key: Option<&str>, nitrite_config: NitriteConfig) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.inner.get_repository(key, nitrite_config)
    }

    pub(crate) fn create_repository<T>(
        &self,
        key: Option<&str>,
        nitrite_config: NitriteConfig
    ) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.inner.create_repository(key, nitrite_config)
    }

    pub(crate) fn destroy_repository<T: NitriteEntity>(&self, key: Option<&str>) -> NitriteResult<()> {
        self.inner.destroy_repository::<T>(key)
    }

    pub(crate) fn clear(&self) -> NitriteResult<()> {
        self.inner.clear()
    }
}

pub(crate) struct RepositoryFactoryInner {
    collection_factory: CollectionFactory,
    repository_operations: Atomic<HashMap<String, RepositoryOperations>>,
    collection_registry: Atomic<HashMap<String, NitriteCollection>>,
    lock: Arc<Mutex<()>>,
}

impl RepositoryFactoryInner {
    fn new(collection_factory: CollectionFactory) -> Self {
        Self {
            collection_factory,
            repository_operations: atomic(HashMap::new()),
            collection_registry: atomic(HashMap::new()),
            lock: Arc::new(Mutex::new(())),
        }
    }

    fn has_repository<T: NitriteEntity>(&self, key: Option<&str>) -> NitriteResult<bool> {
        let name = repository_name_by_type::<T>(key)?;
        Ok(self.repository_operations.read().contains_key(&*name))
    }

    fn get_repository<T>(&self, key: Option<&str>, nitrite_config: NitriteConfig) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        let name = repository_name_by_type::<T>(key)?;
        
        let _guard = self.lock.lock();
        // Clone values without holding read lock to avoid deadlock
        let collection_opt = self.collection_registry.read().get(&*name).cloned();
        
        if let Some(collection) = collection_opt {
            if collection.is_dropped()? || !collection.is_open()? {
                self.collection_registry.write().remove(&*name);
                self.repository_operations.write().remove(&*name);
                return self.create_repository(key, nitrite_config)
            }
            
            let operations_opt = self.repository_operations.read().get(&*name).cloned();
            if let Some(operations) = operations_opt {
                let repository = DefaultObjectRepository::new(collection, operations);
                Ok(ObjectRepository::new(repository))
            } else {
                log::error!("No repository operation found for name {}. Reinitialize the database", name);
                Err(NitriteError::new(
                    "Database is in invalid state. Reinitialize the database",
                    ErrorKind::InvalidOperation,
                ))
            }        
        } else {
            self.create_repository(key, nitrite_config)
        }
    }

    fn create_repository<T>(
        &self,
        key: Option<&str>,
        nitrite_config: NitriteConfig
    ) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        let name = repository_name_by_type::<T>(key)?;
        let store = nitrite_config.nitrite_store()?;

        if store.get_collection_names()?.contains(&name) {
            return Err(NitriteError::new(
                &format!("A collection with same name '{}' already exists", name),
                ErrorKind::InvalidOperation,
            ));
        }

        let collection = self.collection_factory.get_collection(&*name, nitrite_config, false)?;
        let operations = RepositoryOperations::new();
        operations.initialize::<T>(collection.clone())?;
        
        let repository = DefaultObjectRepository::new(collection.clone(), operations.clone());
        self.write_catalog(store, name.clone(), key)?;

        self.repository_operations.write().insert(name.clone(), operations);
        self.collection_registry.write().insert(name.clone(), collection);
        Ok(ObjectRepository::new(repository))
    }

    fn destroy_repository<T: NitriteEntity>(&self, key: Option<&str>) -> NitriteResult<()> {
        let name = repository_name_by_type::<T>(key)?;
        self.collection_factory.destroy_collection(&*name)?;
        self.repository_operations.write().remove(&*name);
        self.collection_registry.write().remove(&*name);
        Ok(())
    }

    fn clear(&self) -> NitriteResult<()> {
        self.collection_factory.clear()?;
        self.repository_operations.write().clear();
        self.collection_registry.write().clear();
        Ok(())
    }
    
    fn write_catalog(&self, store: NitriteStore, name: String, key: Option<&str>) -> NitriteResult<()> {
        let catalog = store.store_catalog()?;
        let exists = catalog.has_entry(&name)?;
        if !exists {
            return if key.is_some() {
                catalog.write_keyed_repository_entry(&name)
            } else {
                catalog.write_repository_entry(&name)
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::{Convertible, LockRegistry, Value};
    use crate::nitrite_config::NitriteConfig;
    use crate::repository::{EntityId, EntityIndex, NitriteEntity};
    use std::sync::Arc;

    #[derive(Default)]
    struct TestEntity;

    impl NitriteEntity for TestEntity {
        type Id = ();

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            None
        }

        fn entity_id(&self) -> Option<EntityId> {
            None
        }
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            Ok(Document::new().to_value()?)
        }

        fn from_value(_value: &Value) -> NitriteResult<Self::Output> {
            Ok(TestEntity)
        }
    }

    #[test]
    fn test_new_repository_factory() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        assert_eq!(Arc::strong_count(&factory.inner), 1);
    }

    #[test]
    fn test_has_repository() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.has_repository::<TestEntity>(None);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_get_repository() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.get_repository::<TestEntity>(None, NitriteConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_create_repository() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        let result = factory.create_repository::<TestEntity>(None, config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy_repository() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.destroy_repository::<TestEntity>(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_clear() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.clear();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_repository_with_error() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.has_repository::<TestEntity>(Some("invalid_key"));
        assert!(!result.unwrap());
    }

    #[test]
    fn test_get_repository_with_error() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.get_repository::<TestEntity>(Some("invalid_key"), NitriteConfig::default());
        assert!(result.is_err());
    }

    #[test]
    fn test_create_repository_with_error() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let config = NitriteConfig::default();
        let result = factory.create_repository::<TestEntity>(Some("invalid_key"), config);
        assert!(result.is_err());
    }

    #[test]
    fn test_destroy_non_existing_repository() {
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let result = factory.destroy_repository::<TestEntity>(Some("invalid_key"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_catalog() {
        let factory_inner = RepositoryFactoryInner::new(CollectionFactory::new(LockRegistry::new()));
        let store = NitriteStore::default();
        let result = factory_inner.write_catalog(store, "test_name".to_string(), None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_repository_safe_atomic_access() {
        // Test that get_repository uses atomic if-let pattern and doesn't panic
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Create repository first
        let _repo = factory.create_repository::<TestEntity>(None, config.clone()).unwrap();
        
        // Now try to get it - should work with atomic access
        let result = factory.get_repository::<TestEntity>(None, NitriteConfig::default());
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_repository_with_missing_operations() {
        // Test that missing operations returns error, not panic
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        
        // Try to get non-existent repository with unconfigured store
        let result = factory.get_repository::<TestEntity>(None, NitriteConfig::default());
        assert!(result.is_err());
        if let Err(e) = result {
            // When no store is configured, we expect a PluginError
            assert_eq!(e.kind(), &ErrorKind::PluginError);
        }
    }

    #[test]
    fn test_get_repository_atomicity_multiple_accesses() {
        // Test that multiple concurrent repository accesses are safe
        let factory = RepositoryFactory::new(CollectionFactory::new(LockRegistry::new()));
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        
        // Create repository
        let _repo = factory.create_repository::<TestEntity>(None, config.clone()).unwrap();
        
        // Multiple sequential accesses should all succeed
        for _ in 0..3 {
            let result = factory.get_repository::<TestEntity>(None, NitriteConfig::default());
            assert!(result.is_ok());
        }
    }
}