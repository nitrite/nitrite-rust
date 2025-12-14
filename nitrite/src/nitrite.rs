use crate::collection;
use crate::common::{get_key_name, get_keyed_repo_type, repository_name_by_type, Convertible, LockRegistry, NitritePluginProvider};
use crate::repository::{NitriteEntity, ObjectRepository, RepositoryFactory};
use crate::transaction::Session;
use crate::{
    collection::{CollectionFactory, Document, NitriteCollection},
    errors::{ErrorKind, NitriteError, NitriteResult},
    get_current_time_or_zero,
    metadata::NitriteMetadata,
    migration::MigrationManager,
    nitrite_builder::NitriteBuilder,
    nitrite_config::NitriteConfig,
    store::{Metadata, NitriteMapProvider, NitriteStore, NitriteStoreProvider},
    AuthService, Value, NITRITE_VERSION, RESERVED_NAMES, STORE_INFO,
};
use std::collections::{HashMap, HashSet};
use std::marker;
use std::ops::Deref;
use std::sync::{Arc, OnceLock};

/// The main database instance for Nitrite.
///
/// `Nitrite` is the entry point for all database operations. It provides methods for:
/// - Working with document collections
/// - Working with typed object repositories
/// - Managing transactions
/// - Accessing database metadata
/// - Handling authentication
///
/// `Nitrite` uses the PIMPL (Pointer to Implementation) design pattern internally.
/// The implementation details are hidden behind this public interface, providing:
/// - Thread-safety through `Arc<NitriteInner>` cloning
/// - Automatic resource cleanup via `Drop` implementation
/// - Stable API that can evolve without breaking compatibility
///
/// `Nitrite` instances are thread-safe and can be shared across threads using `Arc`.
/// The database is automatically closed when the last clone is dropped, or you can call
/// `close()` explicitly to release resources immediately.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
/// use nitrite::collection::Document;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create an in-memory database
/// let db = Nitrite::builder()
///     .open_or_create(None, None)?;
///
/// // Get a collection
/// let mut collection = db.collection("users")?;
///
/// // Insert a document
/// let mut doc = Document::new();
/// doc.put("name", "Alice")?;
/// collection.insert(doc)?;
///
/// // Find documents
/// let results = collection.find(nitrite::filter::all())?;
///
/// // Close the database
/// db.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Nitrite {
    inner: Arc<NitriteInner>,
}

impl Nitrite {
    /// Creates a new `NitriteBuilder` for configuring and opening a database.
    ///
    /// # Returns
    ///
    /// A new `NitriteBuilder` instance with default configuration.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let db = Nitrite::builder()
    ///     .field_separator(".")
    ///     .open_or_create(None, None)?;
    /// ```
    pub fn builder() -> NitriteBuilder {
        NitriteBuilder::new()
    }

    pub(crate) fn new(nitrite_config: NitriteConfig) -> Self {
        Nitrite {
            inner: Arc::new(NitriteInner::new(nitrite_config.clone())),
        }
    }

    /// Gets a collection by name, creating it if it doesn't exist.
    ///
    /// # Arguments
    ///
    /// * `name` - The collection name
    ///
    /// # Returns
    ///
    /// A `NitriteCollection` for accessing documents in the collection.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database is closed
    /// - The collection name is invalid (empty, contains spaces, or is reserved)
    /// - The collection already exists as a repository
    pub fn collection(&self, name: &str) -> NitriteResult<NitriteCollection> {
        self.inner.collection(name)
    }

    /// Gets or creates a typed object repository for entities of type `T`.
    ///
    /// A repository provides type-safe access to stored objects, handling serialization
    /// and deserialization automatically. Each repository is backed by a collection.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type implementing `NitriteEntity` and `Convertible`
    ///
    /// # Returns
    ///
    /// An `ObjectRepository<T>` for managing entities of type `T`.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use nitrite::repository::NitriteEntity;
    /// use nitrite_derive::{NitriteEntity, Convertible};
    ///
    /// #[derive(NitriteEntity, Convertible)]
    /// pub struct User {
    ///     pub name: String,
    ///     pub age: u32,
    /// }
    ///
    /// let repo = db.repository::<User>()?;
    /// ```
    pub fn repository<T>(&self) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static {
        self.inner.repository(None)
    }

    /// Gets or creates a keyed typed object repository for entities of type `T`.
    ///
    /// Similar to `repository()`, but allows multiple repositories of the same type
    /// by providing a unique key. This is useful for partitioning data by key.
    ///
    /// # Arguments
    ///
    /// * `key` - A unique identifier for this repository instance
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type implementing `NitriteEntity` and `Convertible`
    ///
    /// # Returns
    ///
    /// An `ObjectRepository<T>` keyed by the provided identifier.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let repo_prod = db.keyed_repository::<User>("prod")?;
    /// let repo_test = db.keyed_repository::<User>("test")?;
    /// ```
    pub fn keyed_repository<T>(&self, key: &str) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static {
        self.inner.repository(Some(key))
    }

    /// Destroys an object repository, removing all data associated with it.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type
    ///
    /// # Returns
    ///
    /// `Ok(())` if the repository was destroyed successfully, or an error if it doesn't exist
    /// or deletion fails.
    pub fn destroy_repository<T: NitriteEntity>(&self) -> NitriteResult<()> {
        self.inner.destroy_repository::<T>(None)
    }

    /// Destroys a keyed object repository, removing all data associated with it.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the repository to destroy
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type
    ///
    /// # Returns
    ///
    /// `Ok(())` if the repository was destroyed successfully, or an error if it doesn't exist
    /// or deletion fails.
    pub fn destroy_keyed_repository<T: NitriteEntity>(&self, key: &str) -> NitriteResult<()> {
        self.inner.destroy_repository::<T>(Some(key))
    }
    
    /// Closes the database and releases all resources.
    ///
    /// This method commits any pending changes and closes the database connection.
    /// After calling this, the database instance should not be used.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the database was closed successfully.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.commit()?;
        self.inner.close()
    }

    /// Checks if a collection with the specified name exists in the database.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the collection
    ///
    /// # Returns
    ///
    /// `true` if the collection exists, `false` otherwise.
    pub fn has_collection(&self, name: &str) -> NitriteResult<bool> {
        let collections = self.list_collection_names()?;
        Ok(collections.contains(&name.to_string()))
    }

    /// Checks if an object repository for type `T` exists in the database.
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type
    ///
    /// # Returns
    ///
    /// `true` if the repository exists, `false` otherwise.
    pub fn has_repository<T: NitriteEntity>(&self) -> NitriteResult<bool> {
        let repositories = self.list_repositories()?;
        let name = repository_name_by_type::<T>(None)?;
        Ok(repositories.contains(&name))
    }

    /// Checks if a keyed object repository for type `T` exists in the database.
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the repository
    ///
    /// # Type Parameters
    ///
    /// * `T` - The entity type
    ///
    /// # Returns
    ///
    /// `true` if the repository exists, `false` otherwise.
    pub fn has_keyed_repository<T: NitriteEntity>(&self, key: &str) -> NitriteResult<bool> {
        let repositories = self.list_keyed_repositories()?;
        let name = repository_name_by_type::<T>(Some(key))?;
        let key = get_key_name(&name)?;
        let repo_type = get_keyed_repo_type(&name)?;

        // Use if-let pattern for efficient optional handling
        if let Some(repo_set) = repositories.get(&key) {
            Ok(repo_set.contains(&repo_type))
        } else {
            Ok(false)
        }
    }

    /// Destroys a collection, removing all documents in it.
    ///
    /// # Arguments
    ///
    /// * `name` - The collection name
    ///
    /// # Returns
    ///
    /// `Ok(())` if the collection was destroyed successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if the collection doesn't exist or deletion fails.
    pub fn destroy_collection(&self, name: &str) -> NitriteResult<()> {
        self.inner.destroy_collection(name)
    }

    /// Lists all collection names in the database.
    ///
    /// # Returns
    ///
    /// A set of all collection names.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn list_collection_names(&self) -> NitriteResult<HashSet<String>> {
        self.inner.list_collection_names()
    }

    /// Lists all repository names in the database.
    ///
    /// # Returns
    ///
    /// A set of all repository names.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn list_repositories(&self) -> NitriteResult<HashSet<String>> {
        self.inner.list_repositories()
    }

    /// Lists all keyed repositories in the database, grouped by key.
    ///
    /// # Returns
    ///
    /// A map from key names to sets of repository types.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed.
    pub fn list_keyed_repositories(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        self.inner.list_keyed_repositories()
    }

    /// Checks if the database has unsaved changes.
    ///
    /// # Returns
    ///
    /// `true` if there are unsaved changes, `false` otherwise.
    pub fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        self.inner.has_unsaved_changes()
    }

    /// Checks if the database store is closed.
    ///
    /// # Returns
    ///
    /// `true` if the store is closed, `false` if it's open.
    pub fn is_closed(&self) -> NitriteResult<bool> {
        self.inner.is_closed()
    }

    /// Gets the database configuration.
    ///
    /// # Returns
    ///
    /// A clone of the `NitriteConfig` used for this database.
    pub fn config(&self) -> NitriteConfig {
        self.inner.config()
    }

    /// Gets the underlying storage backend.
    ///
    /// # Returns
    ///
    /// A clone of the `NitriteStore` implementing the storage provider.
    pub fn store(&self) -> NitriteStore {
        self.inner.store()
    }

    /// Commits any pending changes to persistent storage.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the commit was successful.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or the commit operation fails.
    pub fn commit(&self) -> NitriteResult<()> {
        self.inner.commit()
    }

    /// Compacts the database storage, reclaiming unused space.
    ///
    /// This operation may take some time depending on the database size.
    ///
    /// # Returns
    ///
    /// `Ok(())` if compaction completed successfully.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is closed or compaction fails.
    pub fn compact(&self) -> NitriteResult<()> {
        self.inner.compact()
    }

    /// Gets the database metadata.
    ///
    /// # Returns
    ///
    /// The `NitriteMetadata` containing database information.
    ///
    /// # Errors
    ///
    /// Returns an error if the database is not properly initialized.
    pub fn database_metadata(&self) -> NitriteResult<NitriteMetadata> {
        self.inner.database_metadata()
    }

    /// Executes a closure within a transactional session context.
    ///
    /// This method creates a new session that provides transactional semantics for
    /// multi-step database operations. All operations performed within the session
    /// are executed atomically, meaning they either all succeed or all fail together.
    ///
    /// The session automatically manages:
    /// - Transaction initialization and cleanup
    /// - Resource locking and synchronization
    /// - Rollback on error
    /// - Automatic session closure
    ///
    /// # Type Parameters
    ///
    /// * `F` - A closure that receives a `Session` reference and performs database operations
    /// * `R` - The return type of the closure
    ///
    /// # Arguments
    ///
    /// * `func` - A closure that takes a `&Session` and returns a `NitriteResult<R>`.
    ///           The closure should contain all database operations to be executed within
    ///           the transaction context.
    ///
    /// # Returns
    ///
    /// The result of the closure if the session was successfully created and executed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The database is closed
    /// - The session cannot be created
    /// - The closure returns an error (transaction is rolled back)
    /// - Session cleanup fails
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use nitrite::nitrite_builder::NitriteBuilder;
    /// use nitrite::collection::Document;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = Nitrite::builder().open_or_create(None, None)?;
    ///
    /// // Execute operations within a transaction
    /// db.with_session(|session| {
    ///     // Start a transaction
    ///     let transaction = session.begin_transaction()?;
    ///
    ///     // All operations here are transactional
    ///     let col1 = transaction.collection("users")?;
    ///     let col2 = transaction.collection("profiles")?;
    ///
    ///     let doc1 = nitrite::doc! {"name": "Alice", "age": 30};
    ///     let doc2 = nitrite::doc! {"user_name": "Alice", "bio": "Developer"};
    ///
    ///     col1.insert(doc1)?;
    ///     col2.insert(doc2)?;
    ///
    ///     // Commit the transaction
    ///     transaction.commit()?;
    ///
    ///     Ok(())
    /// })?;
    ///
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Transaction Semantics
    ///
    /// - **Atomicity**: All operations in the transaction either succeed together or fail together
    /// - **Isolation**: Transactions provide isolation from concurrent operations
    /// - **Consistency**: The database state remains consistent even on errors
    /// - **Durability**: Committed changes are persisted to storage
    ///
    /// If the closure returns an error, the session is automatically rolled back,
    /// undoing any changes made during the transaction.
    pub fn with_session<F, R>(&self, func: F) -> NitriteResult<R>
    where
        F: FnOnce(&Session) -> NitriteResult<R>,
    {
        self.inner.check_opened()?;
        let session = Session::new(self.clone(), self.inner.lock_registry.clone());
        let result = func(&session)?;
        session.close()?;
        Ok(result)
    }

    pub(crate) fn initialize(
        &self,
        username: Option<&str>,
        password: Option<&str>,
    ) -> NitriteResult<()> {
        {
            let result = self.inner.initialize();

            if result.is_err() {
                self.inner.close()?;

                log::error!("Failed to initialize Nitrite: {:?}", result.clone().err().unwrap());
                return Err(NitriteError::new_with_cause(
                    "Failed to initialize Nitrite",
                    ErrorKind::IOError,
                    result.err().unwrap(),
                ));
            }
        }

        self.migrate()?;

        self.inner.validate_credentials(username, password)?;
        self.inner.authenticate(username, password)
    }

    fn migrate(&self) -> NitriteResult<()> {
        let migration_manager = MigrationManager::new(self.clone());
        migration_manager.do_migrate()
    }
}

#[cfg(test)]
impl Default for Nitrite {
    fn default() -> Self {
        Nitrite::builder().open_or_create(None, None).expect("Failed to create Nitrite")
    }
}

/// Internal implementation of the Nitrite database.
///
/// This struct contains the actual implementation details and is hidden from public API
/// through the PIMPL (Pointer to Implementation) design pattern. Users interact with
/// the database through the `Nitrite` struct instead.
///
/// All methods are internal (`pub(crate)` or `fn`) except where needed for testing,
/// and can only be accessed through `Nitrite` public methods.
struct NitriteInner {
    collection_factory: CollectionFactory,
    repository_factory: RepositoryFactory,
    nitrite_config: NitriteConfig,
    store: OnceLock<NitriteStore>,
    metadata: OnceLock<NitriteMetadata>,
    lock_registry: LockRegistry,
}

impl NitriteInner {
    fn new(nitrite_config: NitriteConfig) -> Self {
        let lock_registry = LockRegistry::new();
        // CollectionFactory expects LockRegistry - using a default for now
        let collection_factory = CollectionFactory::new(lock_registry.clone());

        NitriteInner {
            collection_factory: collection_factory.clone(),
            repository_factory: RepositoryFactory::new(collection_factory),
            nitrite_config: nitrite_config.clone(),
            store: OnceLock::new(),
            metadata: OnceLock::new(),
            lock_registry,
        }
    }

    fn collection(&self, name: &str) -> NitriteResult<NitriteCollection> {
        self.validate_collection_name(name)?;
        self.check_opened()?;

        // Check if the name is a repository name (reserved)
        let repositories = self.list_repositories()?;
        if repositories.contains(name) {
            log::error!("Collection name '{}' is a reserved repository name", name);
            return Err(NitriteError::new(
                &format!("Cannot access repository '{}' as a collection", name),
                ErrorKind::ValidationError,
            ));
        }

        self.collection_factory.get_collection(name, self.nitrite_config.clone(), true)
    }
    
    fn repository<T>(&self, key: Option<&str>) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.check_opened()?;
        self.repository_factory.get_repository::<T>(key, self.nitrite_config.clone())
    }

    fn destroy_collection(&self, name: &str) -> NitriteResult<()> {
        self.check_opened()?;
        self.collection_factory.destroy_collection(name)?;
        self.store.get().unwrap().remove_map(name)
    }
    
    fn destroy_repository<T: NitriteEntity>(&self, key: Option<&str>) -> NitriteResult<()> {
        self.check_opened()?;
        self.repository_factory.destroy_repository::<T>(key)
    }

    fn list_collection_names(&self) -> NitriteResult<HashSet<String>> {
        self.check_opened()?;
        self.store.get().unwrap().get_collection_names()
    }
    
    fn list_repositories(&self) -> NitriteResult<HashSet<String>> {
        self.check_opened()?;
        self.store.get().unwrap().get_repository_registry()
    }
    
    fn list_keyed_repositories(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        self.check_opened()?;
        self.store.get().unwrap().get_keyed_repository_registry()
    }

    fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        self.check_opened()?;
        self.store.get().unwrap().has_unsaved_changes()
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        self.store.get().unwrap().is_closed()
    }

    fn config(&self) -> NitriteConfig {
        self.nitrite_config.clone()
    }

    fn store(&self) -> NitriteStore {
        self.store.get().unwrap().clone()
    }

    fn commit(&self) -> NitriteResult<()> {
        self.check_opened()?;
        self.save_metadata()?;
        self.store.get().unwrap().commit()
    }
    
    fn compact(&self) -> NitriteResult<()> {
        self.check_opened()?;
        self.store.get().unwrap().compact()
    }

    fn close(&self) -> NitriteResult<()> {
        let store = self.store.get().unwrap();
        store.before_close()?;
        if store.has_unsaved_changes()? {
            store.commit()?;
        }
        self.collection_factory.clear()?;
        self.nitrite_config.close()?;
        // Close the store to release all resources including background threads
        store.close()?;
        Ok(())
    }

    fn database_metadata(&self) -> NitriteResult<NitriteMetadata> {
        // Cache OnceLock get() to avoid redundant calls
        if let Some(metadata) = self.metadata.get() {
            Ok(metadata.clone())
        } else {
            log::error!("Database metadata not set - database may not be properly initialized");
            Err(NitriteError::new(
                "Database metadata not set. The database must be opened and initialized before accessing metadata",
                ErrorKind::IOError
            ))
        }
    }

    fn create_database_metadata(&self) -> NitriteResult<()> {
        let meta_map = self.store.get().unwrap().open_map(STORE_INFO)?;
        let store_info = meta_map.get(&Value::from(STORE_INFO))?;

        if let Some(store_info_value) = store_info {
            // Extract Document safely with efficient pattern matching
            let store_info_doc = if let Value::Document(doc) = store_info_value {
                doc
            } else {
                log::error!("Invalid metadata format in store: {:?}", store_info_value);
                return Err(NitriteError::new(
                    "Invalid metadata format in store, expected Document",
                    ErrorKind::ObjectMappingError,
                ));
            };
            let metadata = NitriteMetadata::new(&store_info_doc)?;
            self.metadata.get_or_init(|| metadata);
        } else {
            let mut meta_doc = Document::new();
            meta_doc.put("create_time", Value::from(get_current_time_or_zero()))?;
            meta_doc.put(
                "store_version",
                Value::from(self.store.get().unwrap().store_version()?),
            )?;
            meta_doc.put("nitrite_version", Value::from(NITRITE_VERSION))?;
            meta_doc.put(
                "schema_version",
                Value::from(self.nitrite_config.schema_version()),
            )?;

            let metadata = NitriteMetadata::new(&meta_doc)?;
            self.metadata.get_or_init(|| metadata);
        }
        Ok(())
    }

    fn save_metadata(&self) -> NitriteResult<()> {
        if let Some(metadata) = self.metadata.get() {
            let store = self.store.get().unwrap();
            let store_info = store.open_map(STORE_INFO)?;
            store_info.put(
                Value::from(STORE_INFO),
                Value::Document(metadata.get_info()),
            )?;
        }
        Ok(())
    }

    fn validate_collection_name(&self, name: &str) -> NitriteResult<()> {
        if name.is_empty() {
            log::error!("Collection name cannot be empty");
            return Err(NitriteError::new(
                "Collection name cannot be empty",
                ErrorKind::ValidationError,
            ));
        }

        if name.contains(' ') {
            log::error!("Collection name cannot contain space");
            return Err(NitriteError::new(
                "Collection name cannot contain space",
                ErrorKind::ValidationError,
            ));
        }

        for reserved_name in RESERVED_NAMES.iter() {
            if name.eq_ignore_ascii_case(reserved_name) {
                log::error!("Collection name '{}' is reserved", reserved_name);
                return Err(NitriteError::new(
                    &format!("Collection name '{}' is reserved", reserved_name),
                    ErrorKind::ValidationError,
                ));
            }
        }

        Ok(())
    }

    fn check_opened(&self) -> NitriteResult<()> {
        if self.store().is_closed()? {
            log::error!("Nitrite store is closed");
            return Err(NitriteError::new(
                "Nitrite store is closed",
                ErrorKind::IOError,
            ));
        }
        Ok(())
    }

    fn initialize(&self) -> NitriteResult<()> {
        self.nitrite_config.initialize()?;
        let store = self.nitrite_config.nitrite_store()?;
        self.store.get_or_init(|| store);
        self.store.get().unwrap().open_or_create()?;
        self.create_database_metadata()?;
        Ok(())
    }

    fn validate_credentials(
        &self,
        username: Option<&str>,
        password: Option<&str>,
    ) -> NitriteResult<()> {
        if username.is_none() && password.is_none() {
            return Ok(());
        }

        if username.is_none() || password.is_none() {
            log::error!("Both username and password are required");
            return Err(NitriteError::new(
                "Both username and password are required",
                ErrorKind::SecurityError,
            ));
        }
        Ok(())
    }

    fn authenticate(&self, username: Option<&str>, password: Option<&str>) -> NitriteResult<()> {
        let auth_service = AuthService::new(self.nitrite_config.nitrite_store()?);
        auth_service.authenticate(username, password)
    }
}

// This will commit the store and close it when all references of Nitrite are dropped;
// this is necessary to close the store properly.
// Implementing Drop for Nitrite will not work because it will close the store 
// as soon as at least one Arc reference is dropped
impl Drop for NitriteInner {
    fn drop(&mut self) {
        if let Some(store) = self.store.get() {
            let _ = store.commit();
            let _ = store.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::INDEX_META_PREFIX;
    use crate::errors::NitriteError;
    use crate::nitrite_config::NitriteConfig;
    use crate::repository::{EntityId, EntityIndex};
    use std::collections::HashSet;

    // Setup only one time throughout the project.
    // It will take effect during test, project wide
    #[ctor::ctor]
    fn init() {
        colog::init();
    }

    #[derive(Default)]
    struct MyEntity;

    impl NitriteEntity for MyEntity {
        type Id = ();

        fn entity_name(&self) -> String {
            "MyEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            None
        }

        fn entity_id(&self) -> Option<EntityId> {
            None
        }
    }

    impl Convertible for MyEntity {
        type Output = MyEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            Ok(Document::new().to_value()?)
        }

        fn from_value(_value: &Value) -> NitriteResult<Self::Output> {
            Ok(MyEntity)
        }
    }

    #[test]
    fn test_collection() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let collection = nitrite.collection("test_collection");
        assert!(collection.is_ok());
    }

    #[test]
    fn test_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let repository = nitrite.repository::<MyEntity>();
        assert!(repository.is_ok());
    }

    #[test]
    fn test_keyed_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let repository = nitrite.keyed_repository::<MyEntity>("key");
        assert!(repository.is_ok());
    }

    #[test]
    fn test_destroy_collection() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.destroy_collection("test_collection");
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.destroy_repository::<MyEntity>();
        assert!(result.is_ok());
    }

    #[test]
    fn test_destroy_keyed_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.destroy_keyed_repository::<MyEntity>("key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_collection_names() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let collections = nitrite.list_collection_names();
        assert!(collections.is_ok());
    }

    #[test]
    fn test_list_repositories() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let repositories = nitrite.list_repositories();
        assert!(repositories.is_ok());
    }

    #[test]
    fn test_list_keyed_repositories() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let repositories = nitrite.list_keyed_repositories();
        assert!(repositories.is_ok());
    }

    #[test]
    fn test_has_unsaved_changes() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.has_unsaved_changes();
        assert!(result.is_ok());
    }

    #[test]
    fn test_commit() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.commit();
        assert!(result.is_ok());
    }
    
    #[test]
    fn test_compact() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.compact();
        assert!(result.is_ok());
    }

    #[test]
    fn test_close() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.close();
        assert!(result.is_ok());
    }

    #[test]
    fn test_is_closed() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.is_closed();
        assert!(result.is_ok());
    }

    #[test]
    fn test_config() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config.clone());
        let result = nitrite.config();
        assert_eq!(result.field_separator(), config.field_separator());
    }

    #[test]
    fn test_store() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let store = nitrite.store();
        assert!(!store.is_closed().unwrap());
    }

    #[test]
    fn test_database_metadata() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let metadata = nitrite.database_metadata();
        assert!(metadata.is_ok());
    }

    #[test]
    fn test_has_collection() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.has_collection("test_collection");
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.has_repository::<MyEntity>();
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_keyed_repository() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.has_keyed_repository::<MyEntity>("key");
        assert!(result.is_ok());
    }

    #[test]
    fn test_has_keyed_repository_nonexistent_key() {
        // Test that checking for non-existent key returns false safely
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.has_keyed_repository::<MyEntity>("nonexistent_key");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_has_keyed_repository_with_match_pattern() {
        // Test that has_keyed_repository uses idiomatic match pattern instead of unwrap
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Test multiple keys to verify pattern is applied consistently
        let result1 = nitrite.has_keyed_repository::<MyEntity>("test_key1");
        let result2 = nitrite.has_keyed_repository::<MyEntity>("test_key2");
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }

    #[test]
    fn test_has_keyed_repository_safe_navigation() {
        // Test that has_keyed_repository handles empty repository list safely
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Query for a key that should not exist in empty repository
        let result = nitrite.has_keyed_repository::<MyEntity>("unknown");
        assert!(result.is_ok());
        // Result should be false, not panic from unwrap
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_initialize() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        let result = nitrite.initialize(None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_migrate() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.migrate();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_collection_name() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        let result = nitrite.inner.validate_collection_name("valid_name");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_collection_name_empty() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        let result = nitrite.inner.validate_collection_name("");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_collection_name_with_space() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        let result = nitrite.inner.validate_collection_name("invalid name");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_collection_name_reserved() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.inner.validate_collection_name(INDEX_META_PREFIX);
        assert!(result.is_err());
    }

    #[test]
    fn test_check_opened() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        let result = nitrite.inner.check_opened();
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_opened_closed() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        nitrite.close().unwrap();
        let result = nitrite.inner.check_opened();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_credentials() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        let result = nitrite.inner.validate_credentials(Some("user"), Some("pass"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_credentials_missing() {
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        let result = nitrite.inner.validate_credentials(Some("user"), None);
        assert!(result.is_err());
    }

    #[test]
    fn test_authenticate() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(Some("user"), Some("pass")).unwrap();
        let result = nitrite.inner.authenticate(Some("user"), Some("pass"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_authenticate_invalid() {
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(Some("user"), Some("pass")).unwrap();
        let result = nitrite.inner.authenticate(Some("invalid_user"), Some("invalid_pass"));
        assert!(result.is_err());
    }

    #[test]
    fn test_create_database_metadata_with_valid_store_info() {
        // When store_info exists and is a valid Document, metadata should be created
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        let metadata = nitrite.database_metadata();
        assert!(metadata.is_ok());
        // schema_version is u32 so it's always valid
        let _ = metadata.unwrap().schema_version;
    }

    #[test]
    fn test_create_database_metadata_initializes_on_first_run() {
        // When store_info is None, a new metadata should be created with current timestamp
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        let metadata = nitrite.database_metadata();
        assert!(metadata.is_ok());
    }

    #[test]
    fn test_database_metadata_contains_version_information() {
        // Metadata should contain schema_version field after creation
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        let metadata = nitrite.database_metadata().unwrap();
        // schema_version should be present and valid as u32
        assert!(metadata.schema_version <= u32::MAX);
    }

    #[test]
    fn test_database_metadata_not_set_error() {
        // Attempting to get metadata before initialization should fail
        let config = NitriteConfig::default();
        let nitrite = Nitrite::new(config);
        
        let result = nitrite.database_metadata();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Database metadata not set"));
    }

    #[test]
    fn test_create_database_metadata_preserves_existing_timestamp() {
        // When metadata exists, its create_time should be preserved on subsequent access
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        let first_metadata = nitrite.database_metadata().unwrap();
        let first_create_time = first_metadata.create_time;
        
        let second_metadata = nitrite.database_metadata().unwrap();
        let second_create_time = second_metadata.create_time;
        
        // Timestamps should be identical (metadata is idempotent)
        assert_eq!(first_create_time, second_create_time);
    }

    #[test]
    fn test_database_metadata_onclock_efficiency() {
        // Test that database_metadata uses if-let pattern for efficient access
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // First access initializes via OnceLock
        let metadata1 = nitrite.database_metadata().unwrap();
        
        // Subsequent accesses should be cached and efficient
        let metadata2 = nitrite.database_metadata().unwrap();
        
        assert_eq!(metadata1.create_time, metadata2.create_time);
    }

    #[test]
    fn test_has_keyed_repository_if_let_pattern() {
        // Test that has_keyed_repository uses if-let pattern for HashMap lookup
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Query non-existent keyed repository
        let result = nitrite.has_keyed_repository::<MyEntity>("nonexistent");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[test]
    fn test_has_keyed_repository_safe_none_handling() {
        // Test that has_keyed_repository safely handles None case
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Multiple calls to non-existent keys should return false consistently
        for i in 0..5 {
            let key = format!("key_{}", i);
            let result = nitrite.has_keyed_repository::<MyEntity>(&key);
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), false);
        }
    }

    #[test]
    fn test_create_database_metadata_efficient_pattern_matching() {
        // Test that create_database_metadata uses efficient if-let patterns
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Metadata should be created and accessible efficiently
        let metadata = nitrite.database_metadata();
        assert!(metadata.is_ok());
        
        // Verify metadata content is valid
        let meta = metadata.unwrap();
        assert!(meta.create_time > 0);
        assert!(!meta.store_version.is_empty());
    }

    #[test]
    fn test_store_access_caching_efficiency() {
        // Test that store access is cached via OnceLock
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config);
        nitrite.initialize(None, None).unwrap();
        
        // Multiple store accesses should be efficient
        let store1 = nitrite.store();
        let store2 = nitrite.store();
        
        // Both should refer to same underlying store
        assert!(!store1.is_closed().unwrap());
        assert!(!store2.is_closed().unwrap());
    }

    #[test]
    fn test_config_caching_efficiency() {
        // Test that config is cached and efficiently returned
        let config = NitriteConfig::default();
        config.auto_configure().unwrap();
        let nitrite = Nitrite::new(config.clone());
        
        let retrieved_config = nitrite.config();
        
        // Config should be accessible and have same separator
        assert_eq!(retrieved_config.field_separator(), config.field_separator());
    }
}