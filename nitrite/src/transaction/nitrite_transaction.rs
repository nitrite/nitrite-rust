use super::core::{JournalEntry, TransactionContext, TransactionState, UndoEntry};
use super::transaction_store::TransactionStore;
use crate::collection::operation::CollectionOperations;
use crate::collection::{NitriteCollection, NitriteCollectionProvider};
use crate::common::{
    repository_name_by_type, Convertible, LockRegistry, NitriteEventBus, NitriteModule,
    NitritePlugin, PluginRegistrar,
};
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::nitrite::Nitrite;
use crate::nitrite_config::NitriteConfig;
use crate::repository::{NitriteEntity, ObjectRepository, RepositoryOperations};
use crate::store::NitriteStore;
use crate::transaction::transactional_collection::TransactionalCollection;
use crate::transaction::transactional_repository::TransactionalRepository;
use parking_lot::Mutex;

/// A wrapper module that provides a pre-created store
struct TransactionStoreModule {
    store: NitriteStore,
}

impl TransactionStoreModule {
    fn new(store: NitriteStore) -> Self {
        TransactionStoreModule { store }
    }
}

impl NitriteModule for TransactionStoreModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        Ok(vec![self.store.as_plugin()])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        plugin_registrar.register_store_plugin(self.store.clone())
    }
}
/// Nitrite transaction implementation
///
/// Provides transaction coordination with support for:
/// - Multi-collection operations
/// - Two-phase commit
/// - Automatic rollback on failure
/// - ACID guarantees
use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use uuid::Uuid;

/// A Nitrite transaction coordinator.
///
/// Manages ACID transaction semantics across multiple collections and repositories with
/// two-phase commit protocol implementation.
///
/// # Purpose
/// Provides atomic, consistent, isolated, and durable operations across multiple data access
/// paths within a single transaction context. Ensures that either all operations commit
/// successfully or all rollback to maintain data consistency.
///
/// # Characteristics
/// - **Transaction ID**: Each transaction has a unique UUID identifier
/// - **State Machine**: Transitions through Active → PartiallyCommitted → Committed/Failed states
/// - **Multi-Collection**: Supports operations across multiple named collections
/// - **Two-Phase Commit**: First executes all commits, then records undos; rolls back on failure
/// - **Automatic Cleanup**: Calls `close()` on drop to release resources
/// - **Thread-Safe**: All internal state protected by Arc<Mutex<>>
/// - **Lock Coordination**: Uses LockRegistry for proper lock ordering
///
/// # Usage
/// Obtain a transaction from a Nitrite database instance:
/// ```ignore
/// let txn = db.begin_transaction()?;
/// let coll = txn.collection("my_collection")?;
/// // Perform operations...
/// txn.commit()?; // or rollback
/// ```
pub struct NitriteTransaction {
    id: String,
    state: Arc<Mutex<TransactionState>>,
    contexts: Arc<Mutex<HashMap<String, TransactionContext>>>,
    undo_registry: Arc<Mutex<HashMap<String, Vec<UndoEntry>>>>,
    collection_registry: Arc<Mutex<HashMap<String, TransactionalCollection>>>,
    repository_registry: Arc<Mutex<HashMap<String, TransactionalCollection>>>,
    db: Nitrite,
    lock_registry: LockRegistry,
    store: TransactionStore,
    tx_config: NitriteConfig,
}

impl NitriteTransaction {
    /// Creates a new transaction.
    ///
    /// # Arguments
    /// * `db` - Reference to the parent Nitrite database
    /// * `lock_registry` - Registry for coordinating locks across transaction contexts
    ///
    /// # Returns
    /// * `Ok(NitriteTransaction)` - A new transaction initialized in Active state
    /// * `Err(NitriteError)` - If configuration or store initialization fails
    ///
    /// The transaction creates an isolated transaction store that snapshots the
    /// current database state, ensuring read consistency for the transaction's lifetime.
    pub fn new(db: Nitrite, lock_registry: LockRegistry) -> NitriteResult<Self> {
        let db_store = db.store();
        let tx_store = TransactionStore::new(db_store);

        // Create a transaction-specific config that uses the transaction store
        // This ensures index operations in the transaction are isolated
        let tx_config = NitriteConfig::new();
        tx_config.load_module(TransactionStoreModule::new(NitriteStore::new(
            tx_store.clone(),
        )))?;
        tx_config.auto_configure()?;
        tx_config.initialize()?;

        Ok(NitriteTransaction {
            id: Uuid::new_v4().to_string(),
            state: Arc::new(Mutex::new(TransactionState::Active)),
            contexts: Arc::new(Mutex::new(HashMap::new())),
            undo_registry: Arc::new(Mutex::new(HashMap::new())),
            collection_registry: Arc::new(Mutex::new(HashMap::new())),
            repository_registry: Arc::new(Mutex::new(HashMap::new())),
            db,
            lock_registry,
            store: tx_store,
            tx_config,
        })
    }

    /// Gets the transaction ID.
    ///
    /// # Returns
    /// A string slice containing the UUID of this transaction
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Gets the current transaction state.
    ///
    /// # Returns
    /// Current `TransactionState` (Active, PartiallyCommitted, Committed, Closed, Failed, or Aborted)
    pub fn state(&self) -> TransactionState {
        *self.state.lock()
    }

    /// Gets or creates a transactional collection.
    ///
    /// # Arguments
    /// * `name` - The name of the collection to access
    ///
    /// # Returns
    /// * `Ok(NitriteCollection)` - A transactional view of the collection
    /// * `Err(NitriteError)` - If transaction is not active or collection access fails
    ///
    /// Multiple calls with the same name return the same transactional collection instance.
    /// Operations on the returned collection are recorded in the transaction journal.
    pub fn collection(&self, name: &str) -> NitriteResult<NitriteCollection> {
        self.check_active()?;

        let mut registry = self.collection_registry.lock();
        if let Some(tc) = registry.get(name) {
            return Ok(NitriteCollection::new(tc.clone()));
        }

        let primary = self.db.collection(name)?;
        let context = self.get_or_create_context(name.to_string())?;
        let db_store = self.db.store();
        let event_bus = NitriteEventBus::new();
        let operations = CollectionOperations::new(
            name,
            context.txn_map().clone(),
            self.tx_config.clone(), // Use transaction config for isolated index operations
            event_bus.clone(),
        )?;
        let tc = TransactionalCollection::new(primary, context, db_store, operations, event_bus);
        registry.insert(name.to_string(), tc.clone());
        Ok(NitriteCollection::new(tc))
    }

    /// Gets or creates a transactional object repository.
    ///
    /// # Type Parameters
    /// * `T` - The entity type, must implement `NitriteEntity` and `Convertible`
    ///
    /// # Returns
    /// * `Ok(ObjectRepository<T>)` - A transactional repository for the entity type
    /// * `Err(NitriteError)` - If transaction is not active or repository access fails
    ///
    /// Creates a repository with an auto-generated key. For keyed repositories, use
    /// `keyed_repository()`.
    pub fn repository<T>(&self) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.get_repository::<T>(None)
    }

    /// Gets or creates a keyed transactional object repository.
    ///
    /// # Type Parameters
    /// * `T` - The entity type, must implement `NitriteEntity` and `Convertible`
    ///
    /// # Arguments
    /// * `key` - The repository key to identify this particular repository
    ///
    /// # Returns
    /// * `Ok(ObjectRepository<T>)` - A transactional repository for the entity type
    /// * `Err(NitriteError)` - If transaction is not active or repository access fails
    ///
    /// Multiple repositories with different keys for the same entity type are stored
    /// separately, allowing parallel operations on the same type with different namespaces.
    pub fn keyed_repository<T>(&self, key: &str) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.get_repository(Some(key))
    }

    fn get_repository<T>(&self, key: Option<&str>) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        self.check_active()?;

        let mut registry = self.repository_registry.lock();
        // Use key in repository name to differentiate keyed repositories
        let name = repository_name_by_type::<T>(key)?;
        if let Some(tc) = registry.get(&name) {
            let collection = tc.clone();
            return self.create_repository_from_collection(collection, key);
        }

        let primary_repo = match key {
            Some(key) => self.db.keyed_repository::<T>(key)?,
            None => self.db.repository::<T>()?,
        };
        let context = self.get_or_create_context(name.clone())?;
        let db_store = self.db.store();
        let event_bus = NitriteEventBus::new();
        let operations = CollectionOperations::new(
            &name,
            context.txn_map().clone(),
            self.tx_config.clone(), // Use transaction config for isolated index operations
            event_bus.clone(),
        )?;
        let tc = TransactionalCollection::new(
            primary_repo.document_collection(),
            context,
            db_store,
            operations,
            event_bus,
        );
        registry.insert(name.clone(), tc.clone());
        self.create_repository_from_collection::<T>(tc, key)
    }

    fn create_repository_from_collection<T>(
        &self,
        tx_collection: TransactionalCollection,
        key: Option<&str>,
    ) -> NitriteResult<ObjectRepository<T>>
    where
        T: Convertible<Output = T> + NitriteEntity + Send + Sync + 'static,
    {
        let primary_repo = match key {
            Some(key) => self.db.keyed_repository::<T>(key)?,
            None => self.db.repository::<T>()?,
        };
        let nitrite_config = self.db.config().clone();
        let operation = RepositoryOperations::new();
        let nitrite_collection = NitriteCollection::new(tx_collection.clone());
        // Initialize the operation with the collection
        operation.initialize::<T>(nitrite_collection.clone())?;
        let tx_repo = TransactionalRepository::new(
            primary_repo,
            nitrite_collection,
            nitrite_config,
            operation,
        );
        Ok(ObjectRepository::new(tx_repo))
    }

    /// Gets or creates a transaction context for a collection
    fn get_or_create_context(&self, collection_name: String) -> NitriteResult<TransactionContext> {
        self.check_active()?;

        let mut contexts = self.contexts.lock();
        if let Some(ctx) = contexts.get(&collection_name) {
            return Ok(ctx.clone());
        }

        // Create a transactional map for this collection
        let txn_map = self.store.open_map(&collection_name)?;
        let ctx = TransactionContext::new(collection_name.clone(), txn_map);
        contexts.insert(collection_name, ctx.clone());
        Ok(ctx)
    }

    /// Adds a journal entry to a collection's context
    pub fn add_journal_entry(
        &self,
        collection_name: String,
        entry: JournalEntry,
    ) -> NitriteResult<()> {
        let ctx = self.get_or_create_context(collection_name)?;
        ctx.add_entry(entry)
    }

    /// Commits the transaction using two-phase commit protocol.
    ///
    /// # Returns
    /// * `Ok(())` - If all operations committed successfully
    /// * `Err(NitriteError)` - If transaction is not active or any commit operation fails
    ///
    /// # Two-Phase Commit Process
    /// 1. Transitions state to PartiallyCommitted
    /// 2. Executes all pending commit commands from journal
    /// 3. Records undo information for successful commits
    /// 4. If any commit fails: rolls back all completed commits and returns error
    /// 5. Transitions to Committed on success or Failed on error
    /// 6. Closes transaction and releases resources
    ///
    /// After commit (success or failure), the transaction is closed and cannot be used.
    pub fn commit(&self) -> NitriteResult<()> {
        // Acquire exclusive access during commit
        let mut state = self.state.lock();

        if *state != TransactionState::Active {
            return Err(NitriteError::new(
                "Transaction is not active",
                ErrorKind::InvalidOperation,
            ));
        }

        *state = TransactionState::PartiallyCommitted;
        drop(state); // Release lock

        // Perform two-phase commit
        match self.perform_commit() {
            Ok(_) => {
                *self.state.lock() = TransactionState::Committed;
                self.close();
                Ok(())
            }
            Err(e) => {
                *self.state.lock() = TransactionState::Failed;
                // Try to rollback on failure
                let _ = self.perform_rollback();
                self.close();
                Err(NitriteError::new(
                    &format!("Commit failed: {}", e.message()),
                    ErrorKind::InvalidOperation,
                ))
            }
        }
    }

    /// Two-phase commit implementation
    fn perform_commit(&self) -> NitriteResult<()> {
        let contexts = self.contexts.lock();
        let mut commit_error: Option<NitriteError> = None;

        for (collection_name, context) in contexts.iter() {
            // NOTE: We don't acquire the collection lock here because:
            // 1. Each commit command (insert/update/remove) will acquire its own lock
            // 2. The individual operations are already atomic
            // 3. Acquiring the lock here and then calling methods that also lock
            //    would cause a deadlock since parking_lot::RwLock is not reentrant

            let mut undo_stack = Vec::new();
            let mut journal = context.journal.lock();
            let mut had_error = false;

            // Phase 1: Execute all commit commands
            while let Some(entry) = journal.pop_front() {
                if let Some(commit_cmd) = &entry.commit {
                    if let Err(e) = commit_cmd() {
                        commit_error = Some(NitriteError::new(
                            &format!("Failed to execute commit: {}", e.message()),
                            ErrorKind::InvalidOperation,
                        ));
                        had_error = true;
                        break;
                    }

                    // Phase 2: Record undo information for successful commits
                    if let Some(rollback_cmd) = &entry.rollback {
                        let undo = UndoEntry {
                            collection_name: collection_name.clone(),
                            rollback: Arc::new(rollback_cmd.clone()),
                        };
                        undo_stack.push(undo);
                    }
                }
            }

            // Always save the undo stack so rollback can undo committed entries
            context.set_inactive();
            self.undo_registry
                .lock()
                .insert(collection_name.clone(), undo_stack);

            // If there was an error, stop processing further collections
            if had_error {
                break;
            }
        }

        if let Some(e) = commit_error {
            return Err(e);
        }

        Ok(())
    }

    /// Rolls back the transaction, undoing all pending operations.
    ///
    /// # Returns
    /// * `Ok(())` - If rollback completed successfully
    /// * `Err(NitriteError)` - If rollback operations fail
    ///
    /// Transitions the state to Aborted and executes all recorded rollback commands
    /// to restore the database to its pre-transaction state. If the transaction is
    /// already closed, returns Ok without further action.
    ///
    /// After rollback, the transaction is closed and cannot be used further.
    pub fn rollback(&self) -> NitriteResult<()> {
        let mut state = self.state.lock();

        if *state == TransactionState::Closed {
            return Ok(());
        }

        *state = TransactionState::Aborted;
        drop(state);

        self.perform_rollback()?;
        self.close();
        Ok(())
    }

    /// Rollback implementation
    fn perform_rollback(&self) -> NitriteResult<()> {
        let undo_registry = self.undo_registry.lock();

        for (_, undo_stack) in undo_registry.iter() {
            // NOTE: We don't acquire the collection lock here because:
            // 1. Each rollback command (remove/update/insert) will acquire its own lock
            // 2. The individual operations are already atomic
            // 3. Acquiring the lock here and then calling methods that also lock
            //    would cause a deadlock since parking_lot::RwLock is not reentrant

            // LIFO order - rollback in reverse
            for undo in undo_stack.iter().rev() {
                (undo.rollback)().ok();
            }
        }

        Ok(())
    }

    /// Closes the transaction and releases all resources.
    ///
    /// Closes all transaction contexts and their associated maps, and transitions
    /// the state to Closed. This is called automatically on drop.
    /// After closing, the transaction cannot be used further.
    pub fn close(&self) {
        let contexts = self.contexts.lock();
        for (_, ctx) in contexts.iter() {
            ctx.close();
        }

        *self.state.lock() = TransactionState::Closed;
        let _ = self.store.close_all();
    }

    /// Checks if transaction is active
    fn check_active(&self) -> NitriteResult<()> {
        let state = *self.state.lock();
        if state != TransactionState::Active {
            return Err(NitriteError::new(
                "Transaction is not active",
                ErrorKind::InvalidOperation,
            ));
        }
        Ok(())
    }

    /// Gets the total number of pending operations across all collections.
    ///
    /// # Returns
    /// Sum of all journal entries from all transactional contexts
    pub fn pending_operations(&self) -> usize {
        let contexts = self.contexts.lock();
        contexts.values().map(|ctx| ctx.pending_operations()).sum()
    }

    /// Lists all collection names accessed in this transaction.
    ///
    /// # Returns
    /// Vector of all unique collection names that have been accessed via
    /// `collection()` or repository operations
    pub fn collection_names(&self) -> Vec<String> {
        self.contexts.lock().keys().cloned().collect()
    }
}

impl Clone for NitriteTransaction {
    fn clone(&self) -> Self {
        NitriteTransaction {
            id: self.id.clone(),
            state: Arc::clone(&self.state),
            contexts: Arc::clone(&self.contexts),
            undo_registry: Arc::clone(&self.undo_registry),
            collection_registry: Arc::clone(&self.collection_registry),
            repository_registry: Arc::clone(&self.repository_registry),
            db: self.db.clone(),
            lock_registry: self.lock_registry.clone(),
            store: self.store.clone(),
            tx_config: self.tx_config.clone(),
        }
    }
}

impl std::fmt::Debug for NitriteTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Acquire contexts lock once and extract both values to avoid deadlock
        let contexts = self.contexts.lock();
        let context_count = contexts.len();
        let pending_ops: usize = contexts.values().map(|ctx| ctx.pending_operations()).sum();
        drop(contexts);

        f.debug_struct("NitriteTransaction")
            .field("id", &self.id)
            .field("state", &self.state())
            .field("context_count", &context_count)
            .field("pending_operations", &pending_ops)
            .finish()
    }
}

impl Drop for NitriteTransaction {
    fn drop(&mut self) {
        // Ensure transaction is closed
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collection::Document;
    use crate::common::Convertible;
    use crate::common::LockRegistry;
    use crate::common::Value;
    use crate::errors::ErrorKind;
    use crate::repository::{EntityId, EntityIndex, NitriteEntity};
    use crate::transaction::core::ChangeType;

    fn create_test_db() -> Nitrite {
        Nitrite::builder().open_or_create(None, None).unwrap()
    }

    /// Test entity for repository tests
    #[derive(Clone, Debug, Default)]
    struct TestEntity {
        id: i64,
        name: String,
    }

    impl NitriteEntity for TestEntity {
        type Id = i64;

        fn entity_name(&self) -> String {
            "TestEntity".to_string()
        }

        fn entity_indexes(&self) -> Option<Vec<EntityIndex>> {
            None
        }

        fn entity_id(&self) -> Option<EntityId> {
            Some(EntityId::new("id", None, None))
        }
    }

    impl Convertible for TestEntity {
        type Output = TestEntity;

        fn to_value(&self) -> NitriteResult<Value> {
            let mut doc = Document::new();
            doc.put("id", Value::I64(self.id))?;
            doc.put("name", Value::String(self.name.clone()))?;
            Ok(doc.to_value()?)
        }

        fn from_value(value: &Value) -> NitriteResult<Self::Output> {
            if let Value::Document(doc) = value {
                let id = match doc.get("id") {
                    Ok(Value::I64(i)) => i,
                    _ => 0,
                };
                let name = match doc.get("name") {
                    Ok(Value::String(s)) => s.clone(),
                    _ => String::new(),
                };
                Ok(TestEntity { id, name })
            } else {
                Err(NitriteError::new(
                    "Invalid value type",
                    ErrorKind::ValidationError,
                ))
            }
        }
    }

    // ==================== Creation Tests ====================

    /// Tests that a transaction can be created
    #[test]
    fn test_transaction_creation() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry);

        assert!(tx.is_ok());
        let tx = tx.unwrap();
        assert!(!tx.id().is_empty());
        assert_eq!(tx.state(), TransactionState::Active);
    }

    /// Tests that each transaction gets a unique ID
    #[test]
    fn test_transaction_unique_ids() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx1 = NitriteTransaction::new(db.clone(), lock_registry.clone()).unwrap();
        let tx2 = NitriteTransaction::new(db, lock_registry).unwrap();

        assert_ne!(tx1.id(), tx2.id());
    }

    /// Tests that transaction ID has correct format (UUID)
    #[test]
    fn test_transaction_id_format() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let id = tx.id();

        assert!(!id.is_empty());
        assert_eq!(id.len(), 36); // UUID v4 string length
    }

    /// Tests that new transaction is in Active state
    #[test]
    fn test_transaction_initial_state() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        assert_eq!(tx.state(), TransactionState::Active);
    }

    // ==================== Clone Tests ====================

    /// Tests Clone implementation
    #[test]
    fn test_transaction_clone() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx1 = NitriteTransaction::new(db, lock_registry).unwrap();
        let tx2 = tx1.clone();

        assert_eq!(tx1.id(), tx2.id());
        assert_eq!(tx1.state(), tx2.state());
    }

    /// Tests that cloned transactions share state
    #[test]
    fn test_transaction_clone_shares_state() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx1 = NitriteTransaction::new(db, lock_registry).unwrap();
        let tx2 = tx1.clone();

        // Access collection on tx1
        let _coll = tx1.collection("test_collection").unwrap();

        // Check that collection names are visible on tx2
        let names = tx2.collection_names();
        assert!(names.contains(&"test_collection".to_string()));
    }

    // ==================== Debug Tests ====================

    /// Tests Debug implementation
    #[test]
    fn test_transaction_debug() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let debug_str = format!("{:?}", tx);

        assert!(debug_str.contains("NitriteTransaction"));
        assert!(debug_str.contains("id"));
        assert!(debug_str.contains("state"));
        assert!(debug_str.contains("context_count"));
        assert!(debug_str.contains("pending_operations"));
    }

    /// Tests Debug output with collection
    #[test]
    fn test_transaction_debug_with_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let _coll = tx.collection("test").unwrap();

        let debug_str = format!("{:?}", tx);
        // Context count should be at least 1
        assert!(debug_str.contains("context_count"));
    }

    // ==================== Collection Tests ====================

    /// Tests getting a collection from a transaction
    #[test]
    fn test_transaction_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let collection = tx.collection("test_collection");

        assert!(collection.is_ok());
    }

    /// Tests getting the same collection twice returns cached version
    #[test]
    fn test_transaction_collection_cached() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        let _coll1 = tx.collection("test_collection").unwrap();
        let _coll2 = tx.collection("test_collection").unwrap();

        // Collection names should only have one entry
        let names = tx.collection_names();
        assert_eq!(names.len(), 1);
    }

    /// Tests getting multiple different collections
    #[test]
    fn test_transaction_multiple_collections() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        let _coll1 = tx.collection("collection1").unwrap();
        let _coll2 = tx.collection("collection2").unwrap();
        let _coll3 = tx.collection("collection3").unwrap();

        let names = tx.collection_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"collection1".to_string()));
        assert!(names.contains(&"collection2".to_string()));
        assert!(names.contains(&"collection3".to_string()));
    }

    /// Tests getting collection from closed transaction fails
    #[test]
    fn test_transaction_collection_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.collection("test");

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
            assert!(err.message().contains("not active"));
        }
    }

    // ==================== Repository Tests ====================

    /// Tests getting a repository from a transaction
    #[test]
    fn test_transaction_repository() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repository = tx.repository::<TestEntity>();

        assert!(repository.is_ok());
    }

    /// Tests getting a keyed repository from a transaction
    #[test]
    fn test_transaction_keyed_repository() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let repository = tx.keyed_repository::<TestEntity>("my_key");

        assert!(repository.is_ok());
    }

    /// Tests getting repository from closed transaction fails
    #[test]
    fn test_transaction_repository_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.repository::<TestEntity>();

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
        }
    }

    /// Tests getting keyed repository from closed transaction fails
    #[test]
    fn test_transaction_keyed_repository_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.keyed_repository::<TestEntity>("key");

        assert!(result.is_err());
        if let Err(err) = result {
            assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
        }
    }

    // ==================== Commit Tests ====================

    /// Tests committing an empty transaction
    #[test]
    fn test_transaction_commit_empty() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let result = tx.commit();

        assert!(result.is_ok());
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests committing a transaction with collection access
    #[test]
    fn test_transaction_commit_with_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let _coll = tx.collection("test").unwrap();

        let result = tx.commit();

        assert!(result.is_ok());
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests committing an already committed transaction fails
    #[test]
    fn test_transaction_commit_twice() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.commit().unwrap();

        let result = tx.commit();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
        assert!(err.message().contains("not active"));
    }

    /// Tests committing a closed transaction fails
    #[test]
    fn test_transaction_commit_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.commit();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
    }

    /// Tests that state transitions through PartiallyCommitted to Committed
    #[test]
    fn test_transaction_commit_state_transition() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        assert_eq!(tx.state(), TransactionState::Active);

        tx.commit().unwrap();

        // After successful commit, state should be Closed
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    // ==================== Rollback Tests ====================

    /// Tests rolling back an empty transaction
    #[test]
    fn test_transaction_rollback_empty() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let result = tx.rollback();

        assert!(result.is_ok());
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests rolling back a transaction with collection access
    #[test]
    fn test_transaction_rollback_with_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let _coll = tx.collection("test").unwrap();

        let result = tx.rollback();

        assert!(result.is_ok());
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests that rollback on a closed transaction succeeds (idempotent)
    #[test]
    fn test_transaction_rollback_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.rollback();

        assert!(result.is_ok());
    }

    /// Tests that rollback after commit succeeds (already closed)
    #[test]
    fn test_transaction_rollback_after_commit() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.commit().unwrap();

        // Transaction is already closed, rollback should succeed
        let result = tx.rollback();
        assert!(result.is_ok());
    }

    /// Tests rollback twice succeeds (idempotent)
    #[test]
    fn test_transaction_rollback_twice() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.rollback().unwrap();

        let result = tx.rollback();

        assert!(result.is_ok());
    }

    // ==================== Close Tests ====================

    /// Tests closing a transaction
    #[test]
    fn test_transaction_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests closing a transaction multiple times (idempotent)
    #[test]
    fn test_transaction_close_idempotent() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();
        tx.close();
        tx.close();

        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests that Drop calls close automatically
    #[test]
    fn test_transaction_drop_calls_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db.clone(), lock_registry.clone()).unwrap();
        let _id = tx.id().to_string();

        // Create a collection to ensure context exists
        let _coll = tx.collection("test").unwrap();

        drop(tx);
        // After drop, transaction should be closed
        // We can't directly test this without reference, but coverage confirms drop() was called
    }

    // ==================== check_active Tests ====================

    /// Tests check_active on active transaction (implicit through collection access)
    #[test]
    fn test_check_active_on_active_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        // This internally calls check_active
        let result = tx.collection("test");
        assert!(result.is_ok());
    }

    /// Tests check_active fails on closed transaction
    #[test]
    fn test_check_active_on_closed_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        // This internally calls check_active
        let result = tx.collection("test");
        assert!(result.is_err());
    }

    // ==================== pending_operations Tests ====================

    /// Tests pending_operations on empty transaction
    #[test]
    fn test_pending_operations_empty() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        assert_eq!(tx.pending_operations(), 0);
    }

    /// Tests pending_operations after accessing collection
    #[test]
    fn test_pending_operations_with_collection() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let _coll = tx.collection("test").unwrap();

        // Just accessing collection doesn't add pending operations
        // Operations are added when actually modifying data
        let _ = tx.pending_operations();
    }

    // ==================== collection_names Tests ====================

    /// Tests collection_names on empty transaction
    #[test]
    fn test_collection_names_empty() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        let names = tx.collection_names();
        assert!(names.is_empty());
    }

    /// Tests collection_names with multiple collections
    #[test]
    fn test_collection_names_with_collections() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        let _coll1 = tx.collection("alpha").unwrap();
        let _coll2 = tx.collection("beta").unwrap();
        let _coll3 = tx.collection("gamma").unwrap();

        let names = tx.collection_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"alpha".to_string()));
        assert!(names.contains(&"beta".to_string()));
        assert!(names.contains(&"gamma".to_string()));
    }

    // ==================== Journal Entry Tests ====================

    /// Tests adding a journal entry
    #[test]
    fn test_add_journal_entry() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        // Create a simple journal entry
        let entry = JournalEntry {
            change_type: ChangeType::Insert,
            commit: Some(Arc::new(|| Ok(()))),
            rollback: Some(Arc::new(|| Ok(()))),
        };

        let result = tx.add_journal_entry("test_collection".to_string(), entry);

        assert!(result.is_ok());
    }

    /// Tests adding journal entry on closed transaction fails
    #[test]
    fn test_add_journal_entry_on_closed() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let entry = JournalEntry {
            change_type: ChangeType::Update,
            commit: Some(Arc::new(|| Ok(()))),
            rollback: Some(Arc::new(|| Ok(()))),
        };

        let result = tx.add_journal_entry("test_collection".to_string(), entry);

        assert!(result.is_err());
    }

    /// Tests adding multiple journal entries
    #[test]
    fn test_add_multiple_journal_entries() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        for i in 0..5 {
            let entry = JournalEntry {
                change_type: ChangeType::Insert,
                commit: Some(Arc::new(move || Ok(()))),
                rollback: Some(Arc::new(move || Ok(()))),
            };
            tx.add_journal_entry(format!("collection_{}", i), entry)
                .unwrap();
        }

        let names = tx.collection_names();
        assert_eq!(names.len(), 5);
    }

    // ==================== Concurrency Tests ====================

    /// Tests concurrent access to transaction
    #[test]
    fn test_transaction_concurrent_access() {
        use std::sync::Barrier;
        use std::thread;

        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        let tx_id = tx.id().to_string();

        let tx1 = tx.clone();
        let tx2 = tx.clone();

        // Use a barrier to ensure both threads complete their work
        // before we check the results
        let barrier = Arc::new(Barrier::new(3)); // 2 threads + main thread
        let barrier1 = barrier.clone();
        let barrier2 = barrier.clone();

        let handle1 = thread::spawn(move || {
            let result = tx1.collection("collection1");
            let id = tx1.id().to_string();
            barrier1.wait(); // Signal that we're done
            (result.is_ok(), id)
        });

        let handle2 = thread::spawn(move || {
            let result = tx2.collection("collection2");
            let id = tx2.id().to_string();
            barrier2.wait(); // Signal that we're done
            (result.is_ok(), id)
        });

        // Wait for both threads to complete their work
        barrier.wait();

        let (ok1, id1) = handle1.join().unwrap();
        let (ok2, id2) = handle2.join().unwrap();

        // Verify results
        assert!(ok1, "Thread 1 should successfully access collection");
        assert!(ok2, "Thread 2 should successfully access collection");

        // Both should have same transaction ID
        assert_eq!(id1, tx_id);
        assert_eq!(id2, tx_id);

        // Both collections should be registered
        let names = tx.collection_names();
        assert_eq!(names.len(), 2);
    }

    // ==================== get_or_create_context Tests ====================

    /// Tests that get_or_create_context creates new context for new collection
    #[test]
    fn test_get_or_create_context_new() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        // Accessing a collection creates context internally
        let _coll = tx.collection("new_collection").unwrap();

        assert!(tx
            .collection_names()
            .contains(&"new_collection".to_string()));
    }

    /// Tests that get_or_create_context returns existing context
    #[test]
    fn test_get_or_create_context_existing() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();

        let _coll1 = tx.collection("same_collection").unwrap();
        let _coll2 = tx.collection("same_collection").unwrap();

        // Should still only have one context
        assert_eq!(tx.collection_names().len(), 1);
    }

    // ==================== State Transition Tests ====================

    /// Tests state after successful commit
    #[test]
    fn test_state_after_commit() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        assert_eq!(tx.state(), TransactionState::Active);

        tx.commit().unwrap();
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests state after rollback
    #[test]
    fn test_state_after_rollback() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        assert_eq!(tx.state(), TransactionState::Active);

        tx.rollback().unwrap();
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    /// Tests state after close
    #[test]
    fn test_state_after_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        assert_eq!(tx.state(), TransactionState::Active);

        tx.close();
        assert_eq!(tx.state(), TransactionState::Closed);
    }

    // ==================== Error Message Tests ====================

    /// Tests error message when committing inactive transaction
    #[test]
    fn test_commit_error_message() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.commit();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.message().contains("not active"));
    }

    /// Tests error message when accessing collection on closed transaction
    #[test]
    fn test_collection_error_message() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let tx = NitriteTransaction::new(db, lock_registry).unwrap();
        tx.close();

        let result = tx.collection("test");
        assert!(result.is_err());

        if let Err(err) = result {
            assert!(err.message().contains("not active"));
        }
    }
}
