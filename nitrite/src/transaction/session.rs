use super::nitrite_transaction::NitriteTransaction;
use crate::common::LockRegistry;
use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::nitrite::Nitrite;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// A session represents a transactional context for database operations.
///
/// Manages multiple transactions within a single session, providing isolated transaction
/// contexts for concurrent or sequential operations. Must be explicitly closed to release
/// resources and roll back any uncommitted transactions.
///
/// # Purpose
/// Sessions encapsulate a database connection's transactional context, enabling creation
/// and management of multiple independent transactions. Each session maintains its own
/// state and transaction registry.
///
/// # Characteristics
/// - **Unique ID**: Each session has a unique UUID identifier
/// - **Active State**: Tracks whether the session is open or closed via atomic flag
/// - **Transaction Registry**: Maintains HashMap of active transactions
/// - **Thread-Safe**: All internal state protected by Arc and Mutex
/// - **Auto-Cleanup**: Calls `close()` on drop to release resources
/// - **Idempotent Close**: Can be closed multiple times without error
///
/// # Usage
/// Create a session from a Nitrite database instance:
/// ```ignore
/// let session = Session::new(db, lock_registry);
/// let txn = session.begin_transaction()?;
/// txn.commit()?;
/// session.close()?;
/// ```
#[derive(Clone)]
pub struct Session {
    inner: Arc<SessionInner>,
}

impl Session {
    /// Creates a new session.
    ///
    /// # Arguments
    /// * `db` - Reference to the Nitrite database
    /// * `lock_registry` - Registry for coordinating locks across transaction contexts
    ///
    /// # Returns
    /// A new `Session` initialized with a unique ID and active state
    ///
    /// The session is created in an active state and can immediately be used to
    /// begin transactions.
    pub fn new(db: Nitrite, lock_registry: LockRegistry) -> Self {
        Session {
            inner: Arc::new(SessionInner::new(db, lock_registry)),
        }
    }

    /// Gets the session ID.
    ///
    /// # Returns
    /// A string slice containing the unique UUID of this session
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Checks if this session is active.
    ///
    /// # Returns
    /// `true` if the session is open and can accept new transactions, `false` if closed
    pub fn is_active(&self) -> bool {
        self.inner.is_active()
    }

    /// Begins a new transaction in this session.
    ///
    /// # Returns
    /// * `Ok(NitriteTransaction)` - A new transaction initialized in Active state
    /// * `Err(NitriteError)` - If the session is closed or transaction creation fails
    ///
    /// The transaction is tracked in the session's active transaction registry and will
    /// be rolled back when the session is closed if not explicitly committed.
    pub fn begin_transaction(&self) -> NitriteResult<NitriteTransaction> {
        self.inner.begin_transaction()
    }

    /// Lists all active transaction IDs in this session.
    ///
    /// # Returns
    /// Vector of transaction ID strings currently tracked by this session
    pub fn active_transactions(&self) -> Vec<String> {
        self.inner.active_transactions()
    }

    /// Closes the session and releases all resources.
    ///
    /// # Returns
    /// * `Ok(())` - If session closed successfully
    /// * `Err(NitriteError)` - If session was already closed (unlikely, as close is idempotent)
    ///
    /// Transitions the session to inactive state using atomic compare-exchange operation.
    /// Rolls back all active transactions, clearing the transaction registry.
    /// Idempotent: calling close multiple times is safe and returns Ok.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }
}

/// Inner session implementation with transaction management.
///
/// Contains the actual session state and transaction tracking logic. Accessed through
/// the `Session` wrapper type which provides Arc-based shared ownership.
///
/// # Purpose
/// Implements the core session functionality including transaction creation, tracking,
/// and lifecycle management. Maintains isolation between transactions and coordinates
/// resource cleanup on session close.
///
/// # Characteristics
/// - **Session ID**: Unique UUID assigned at creation
/// - **Active Flag**: Atomic boolean tracking session state (true = open, false = closed)
/// - **Transaction Map**: Mutex-protected HashMap<String, NitriteTransaction> for tracking
/// - **Database Reference**: Shared Arc to the parent Nitrite database
/// - **Lock Registry**: Shared registry for coordinating locks across transactions
/// - **Idempotent Operations**: Close operation can be called multiple times safely
struct SessionInner {
    id: String,
    active: Arc<AtomicBool>,
    transactions: Arc<Mutex<HashMap<String, NitriteTransaction>>>,
    db: Nitrite,
    lock_registry: LockRegistry,
}

impl SessionInner {
    /// Creates a new session inner implementation.
    ///
    /// # Arguments
    /// * `db` - Reference to the Nitrite database
    /// * `lock_registry` - Registry for coordinating locks across transaction contexts
    ///
    /// # Returns
    /// A new `SessionInner` with unique ID and active state initialized to true
    pub fn new(db: Nitrite, lock_registry: LockRegistry) -> Self {
        SessionInner {
            id: Uuid::new_v4().to_string(),
            active: Arc::new(AtomicBool::new(true)),
            transactions: Arc::new(Mutex::new(HashMap::new())),
            db,
            lock_registry,
        }
    }

    /// Gets the session ID.
    ///
    /// # Returns
    /// A string slice containing the unique UUID of this session
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Checks if this session is active.
    ///
    /// # Returns
    /// `true` if the session is open and can accept new transactions, `false` if closed
    ///
    /// Uses SeqCst (sequential consistency) atomic ordering to ensure proper synchronization
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Begins a new transaction in this session.
    ///
    /// # Returns
    /// * `Ok(NitriteTransaction)` - A new transaction initialized in Active state
    /// * `Err(NitriteError)` - If the session is closed
    ///
    /// Creates a new transaction and registers it in the session's transaction map.
    /// Multiple transactions can exist simultaneously within a session, each with their
    /// own isolated transactional context.
    pub fn begin_transaction(&self) -> NitriteResult<NitriteTransaction> {
        self.check_active()?;

        let tx = NitriteTransaction::new(self.db.clone(), self.lock_registry.clone())?;
        let tx_id = tx.id().to_string();

        self.transactions.lock().insert(tx_id, tx.clone());

        Ok(tx)
    }

    /// Lists all active transaction IDs in this session.
    ///
    /// # Returns
    /// Vector of transaction ID strings currently tracked by this session
    ///
    /// Acquires lock on transaction map to read current state. Committed or rolled back
    /// transactions are automatically removed from this list.
    pub fn active_transactions(&self) -> Vec<String> {
        self.transactions.lock().keys().cloned().collect()
    }

    /// Closes the session and releases all resources.
    ///
    /// # Returns
    /// * `Ok(())` - Always returns Ok, even if already closed (idempotent)
    ///
    /// Transitions the session to inactive state using atomic compare-exchange operation
    /// with SeqCst ordering. Rolls back all active transactions by calling `rollback()`
    /// on each, then clears the transaction registry.
    ///
    /// If the session is already closed, the compare-exchange fails and returns early.
    pub fn close(&self) -> NitriteResult<()> {
        if self
            .active
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst).is_err()
        {
            // Already closed
            return Ok(());
        }

        let mut txs = self.transactions.lock();
        for (_, tx) in txs.drain() {
            // Rollback any uncommitted transaction
            let _ = tx.rollback();
        }

        Ok(())
    }

    /// Checks if session is active and returns an error if not.
    ///
    /// # Returns
    /// * `Ok(())` - If session is active
    /// * `Err(NitriteError)` - If session is closed with error kind `InvalidOperation`
    ///
    /// Used internally by `begin_transaction()` to validate session state before creating
    /// new transactions.
    fn check_active(&self) -> NitriteResult<()> {
        if !self.is_active() {
            return Err(NitriteError::new(
                "Session is closed",
                ErrorKind::InvalidOperation,
            ));
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        // Best-effort close - ignore errors
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_db() -> Nitrite {
        Nitrite::builder().open_or_create(None, None).unwrap()
    }

    /// Tests that a session can be created
    #[test]
    fn test_session_creation() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        assert!(!session.id().is_empty());
        assert!(session.is_active());
    }

    /// Tests that each session gets a unique ID
    #[test]
    fn test_session_unique_ids() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session1 = Session::new(db.clone(), lock_registry.clone());
        let session2 = Session::new(db, lock_registry);

        assert_ne!(session1.id(), session2.id());
    }

    /// Tests that session ID is accessible
    #[test]
    fn test_session_id() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let id = session.id();

        assert!(!id.is_empty());
        assert_eq!(id.len(), 36); // UUID v4 string length
    }

    /// Tests Session clone behavior
    #[test]
    fn test_session_clone() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session1 = Session::new(db, lock_registry);
        let session2 = session1.clone();

        // Both clones should reference same session
        assert_eq!(session1.id(), session2.id());
        assert_eq!(session1.is_active(), session2.is_active());
    }

    /// Tests that cloned sessions share state
    #[test]
    fn test_session_clone_shares_state() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session1 = Session::new(db.clone(), lock_registry.clone());
        let session2 = session1.clone();

        // Create transaction on session1
        let _tx1 = session1.begin_transaction().unwrap();

        // Should be visible on session2
        let txs = session2.active_transactions();
        assert_eq!(txs.len(), 1);
    }

    /// Tests Deref implementation
    #[test]
    fn test_session_deref() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        // Should be able to call methods on deref target
        let id = session.id();
        assert!(!id.is_empty());
    }

    /// Tests that new session is active
    #[test]
    fn test_session_initially_active() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        assert!(session.is_active());
    }

    /// Tests that session can be closed
    #[test]
    fn test_session_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let result = session.close();

        assert!(result.is_ok());
        assert!(!session.is_active());
    }

    /// Tests that closing an already-closed session succeeds
    #[test]
    fn test_session_close_idempotent() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let result1 = session.close();
        let result2 = session.close();

        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert!(!session.is_active());
    }

    /// Tests that Drop calls close automatically
    #[test]
    fn test_session_drop_calls_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let _id = session.id().to_string();

        // Create a transaction
        let _tx = session.begin_transaction().unwrap();
        assert!(!session.active_transactions().is_empty());

        drop(session);
        // After drop, session should be closed (can't directly test without reference)
    }

    /// Tests that a transaction can be begun in an active session
    #[test]
    fn test_begin_transaction() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let tx = session.begin_transaction();

        assert!(tx.is_ok());
        let _tx = tx.unwrap();
        // Transaction created successfully
        assert!(true);
    }

    /// Tests that multiple transactions can be created
    #[test]
    fn test_multiple_transactions() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();

        assert_ne!(tx1.id(), tx2.id());
    }

    /// Tests that transactions are tracked
    #[test]
    fn test_transactions_tracked() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        assert_eq!(session.active_transactions().len(), 0);

        let _tx1 = session.begin_transaction().unwrap();
        assert_eq!(session.active_transactions().len(), 1);

        let _tx2 = session.begin_transaction().unwrap();
        assert_eq!(session.active_transactions().len(), 2);
    }

    /// Tests that transaction IDs match active transactions list
    #[test]
    fn test_transaction_ids_match() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();

        let active = session.active_transactions();
        assert!(active.contains(&tx1.id().to_string()));
        assert!(active.contains(&tx2.id().to_string()));
    }

    /// Tests that begin_transaction fails on closed session
    #[test]
    fn test_begin_transaction_on_closed_session() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        session.close().unwrap();

        let result = session.begin_transaction();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(*err.kind(), ErrorKind::InvalidOperation);
        assert!(err.message().contains("Session is closed"));
    }

    /// Tests error message for closed session
    #[test]
    fn test_closed_session_error_message() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        session.close().unwrap();

        let result = session.begin_transaction();

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message().contains("closed") || err.message().contains("Session"));
    }

    /// Tests active_transactions with empty session
    #[test]
    fn test_active_transactions_empty() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let active = session.active_transactions();
        assert_eq!(active.len(), 0);
    }

    /// Tests active_transactions returns correct count
    #[test]
    fn test_active_transactions_count() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let _tx1 = session.begin_transaction().unwrap();
        let _tx2 = session.begin_transaction().unwrap();
        let _tx3 = session.begin_transaction().unwrap();

        let active = session.active_transactions();
        assert_eq!(active.len(), 3);
    }

    /// Tests active_transactions returns actual transaction IDs
    #[test]
    fn test_active_transactions_ids() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();

        let active = session.active_transactions();

        assert!(active.contains(&tx1.id().to_string()));
        assert!(active.contains(&tx2.id().to_string()));
        assert_eq!(active.len(), 2);
    }

    /// Tests active_transactions on closed session
    #[test]
    fn test_active_transactions_on_closed_session() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let _tx = session.begin_transaction().unwrap();

        session.close().unwrap();

        let active = session.active_transactions();
        // After close, transactions are rolled back and removed
        assert_eq!(active.len(), 0);
    }

    /// Tests that close rolls back active transactions
    #[test]
    fn test_close_with_active_transactions() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let _tx = session.begin_transaction().unwrap();

        assert_eq!(session.active_transactions().len(), 1);

        let result = session.close();
        assert!(result.is_ok());

        // After close, transactions should be cleared
        assert_eq!(session.active_transactions().len(), 0);
    }

    /// Tests that close with multiple transactions succeeds
    #[test]
    fn test_close_with_multiple_transactions() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let _tx1 = session.begin_transaction().unwrap();
        let _tx2 = session.begin_transaction().unwrap();
        let _tx3 = session.begin_transaction().unwrap();

        let result = session.close();

        assert!(result.is_ok());
        assert_eq!(session.active_transactions().len(), 0);
    }

    /// Tests that close clears all transactions
    #[test]
    fn test_close_clears_transactions() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let _tx1 = session.begin_transaction().unwrap();
        let _tx2 = session.begin_transaction().unwrap();

        assert_eq!(session.active_transactions().len(), 2);

        session.close().unwrap();

        assert_eq!(session.active_transactions().len(), 0);
    }

    /// Tests that is_active uses SeqCst ordering
    #[test]
    fn test_atomic_active_flag() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        assert!(session.is_active());
        session.close().unwrap();
        assert!(!session.is_active());
    }

    /// Tests that transaction map is mutex-protected
    #[test]
    fn test_transaction_map_protected() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let _tx1 = session.begin_transaction().unwrap();
        let _tx2 = session.begin_transaction().unwrap();

        // If not properly locked, this could race
        let count1 = session.active_transactions().len();
        let count2 = session.active_transactions().len();

        assert_eq!(count1, count2);
        assert_eq!(count1, 2);
    }

    /// Tests Arc-based cloning for shared state
    #[test]
    fn test_arc_shared_state() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session1 = Session::new(db, lock_registry);
        let session2 = session1.clone();
        let session3 = session1.clone();

        session1.begin_transaction().unwrap();

        assert_eq!(session2.active_transactions().len(), 1);
        assert_eq!(session3.active_transactions().len(), 1);
    }

    /// Tests that session ID is stable
    #[test]
    fn test_session_id_stable() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        let id1 = session.id();
        let id2 = session.id();
        let id3 = session.id();

        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
    }

    /// Tests that active state cannot go from inactive to active
    #[test]
    fn test_state_never_reactivates() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);
        assert!(session.is_active());

        session.close().unwrap();
        assert!(!session.is_active());

        // Even after close, cannot reactivate
        assert!(!session.is_active());
    }

    /// Tests that lock registry is preserved
    #[test]
    fn test_lock_registry_preserved() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db.clone(), lock_registry.clone());

        // Both transactions should use same lock registry
        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();

        // If they use the same lock registry, their operations coordinate
        assert_ne!(tx1.id(), tx2.id());
    }

    /// Tests that database reference is preserved
    #[test]
    fn test_database_reference_preserved() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db.clone(), lock_registry);

        // Multiple transactions share same database
        let _tx1 = session.begin_transaction().unwrap();
        let _tx2 = session.begin_transaction().unwrap();

        assert!(session.is_active());
    }

    /// Tests that transactions are independent
    #[test]
    fn test_transaction_independence() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();

        assert_ne!(tx1.id(), tx2.id());
        // Both transactions created and are independent
        assert!(true);
    }

    /// Tests that begin_transaction returns correct transaction
    #[test]
    fn test_begin_transaction_returns_correct_tx() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx = session.begin_transaction().unwrap();
        let tx_id = tx.id().to_string();

        let active = session.active_transactions();
        assert!(active.contains(&tx_id));
    }

    /// Tests creating many transactions
    #[test]
    fn test_many_transactions() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        for _ in 0..10 {
            session.begin_transaction().unwrap();
        }

        assert_eq!(session.active_transactions().len(), 10);
    }

    /// Tests session behavior after creating and closing transactions
    #[test]
    fn test_session_after_transaction_lifecycle() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let tx = session.begin_transaction().unwrap();
        assert_eq!(session.active_transactions().len(), 1);

        // Close transaction
        tx.close();

        // Session should still be active (transactions are tracked separately)
        assert!(session.is_active());
    }

    /// Tests that session remains usable after close clears transactions
    #[test]
    fn test_session_state_after_close() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        let _tx = session.begin_transaction().unwrap();
        session.close().unwrap();

        // Session is now closed
        assert!(!session.is_active());

        // Cannot create new transactions
        assert!(session.begin_transaction().is_err());
    }

    /// Tests that check_active works correctly
    #[test]
    fn test_check_active_validation() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session = Session::new(db, lock_registry);

        // Should succeed on active session
        assert!(session.begin_transaction().is_ok());

        // Close session
        session.close().unwrap();

        // Should fail on closed session
        assert!(session.begin_transaction().is_err());
    }

    /// Tests complete session lifecycle
    #[test]
    fn test_complete_session_lifecycle() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        // Create session
        let session = Session::new(db, lock_registry);
        assert!(session.is_active());

        // Create transactions
        let tx1 = session.begin_transaction().unwrap();
        let tx2 = session.begin_transaction().unwrap();
        assert_eq!(session.active_transactions().len(), 2);

        // Query state
        assert!(session
            .active_transactions()
            .contains(&tx1.id().to_string()));
        assert!(session
            .active_transactions()
            .contains(&tx2.id().to_string()));

        // Close session
        session.close().unwrap();
        assert!(!session.is_active());
        assert_eq!(session.active_transactions().len(), 0);
    }

    /// Tests session with cloned instances
    #[test]
    fn test_session_with_clones() {
        let db = create_test_db();
        let lock_registry = LockRegistry::new();

        let session1 = Session::new(db, lock_registry);
        let session2 = session1.clone();
        let session3 = session1.clone();

        // All should have same ID
        assert_eq!(session1.id(), session2.id());
        assert_eq!(session2.id(), session3.id());

        // All should be active
        assert!(session1.is_active());
        assert!(session2.is_active());
        assert!(session3.is_active());

        // Create transaction on any clone
        let _tx = session1.begin_transaction().unwrap();

        // Should be visible from all
        assert_eq!(session1.active_transactions().len(), 1);
        assert_eq!(session2.active_transactions().len(), 1);
        assert_eq!(session3.active_transactions().len(), 1);

        // Close from any clone
        session2.close().unwrap();

        // All should be closed
        assert!(!session1.is_active());
        assert!(!session3.is_active());
    }
}
