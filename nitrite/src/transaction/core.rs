use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::store::NitriteMap;
/// Core transaction data structures
/// 
/// Defines the fundamental types for transaction management:
/// - TransactionState: Transaction lifecycle states
/// - ChangeType: Type of operations performed
/// - Command: Executable operations (commit/rollback)
/// - JournalEntry: Record of a single operation
/// - UndoEntry: Rollback information
/// - TransactionContext: Per-collection transaction state

use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Represents the state of a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TransactionState {
    /// Transaction is actively receiving operations
    Active,
    /// Started commit process, not yet complete
    PartiallyCommitted,
    /// Successfully committed all changes
    Committed,
    /// Transaction resources released
    Closed,
    /// Commit failed during execution
    Failed,
    /// Transaction rolled back
    Aborted,
}

/// Type of change performed in a transaction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeType {
    /// Document insertion
    Insert,
    /// Document update
    Update,
    /// Document removal
    Remove,
    /// Clear all documents (auto-committed)
    Clear,
    /// Create index (auto-committed)
    CreateIndex,
    /// Rebuild index (auto-committed)
    RebuildIndex,
    /// Drop index (auto-committed)
    DropIndex,
    /// Drop all indices (auto-committed)
    DropAllIndexes,
    /// Drop collection (auto-committed)
    DropCollection,
    /// Set collection attributes (transactional)
    SetAttributes,
}

/// Represents a transaction-specific error condition.
///
/// # Purpose
/// `TransactionError` encapsulates transaction-related errors with descriptive messages.
/// Implements standard Rust error traits for integration with error handling pipelines.
///
/// # Characteristics
/// - **Clone-Safe**: Can be safely cloned across thread boundaries
/// - **Display Format**: Formatted as "Transaction error: {message}"
/// - **Standard Error Trait**: Implements `std::error::Error` for compatibility
#[derive(Debug, Clone)]
pub struct TransactionError {
    message: String,
}

impl TransactionError {
    /// Creates a new `TransactionError` with the specified message.
    ///
    /// # Arguments
    /// * `message` - The error message, can be a string or string reference
    ///
    /// # Returns
    /// A new `TransactionError` instance
    pub fn new(message: impl Into<String>) -> Self {
        TransactionError {
            message: message.into(),
        }
    }

    /// Retrieves the error message.
    ///
    /// # Returns
    /// A string reference containing the error message
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction error: {}", self.message)
    }
}

impl std::error::Error for TransactionError {}

/// Executable command for transaction operations.
///
/// # Purpose
/// `Command` is a type alias for a thread-safe, callable that performs transaction operations
/// such as commit or rollback. Commands are wrapped in `Arc` for safe sharing across threads.
///
/// # Characteristics
/// - **Thread-Safe**: Implements `Send + Sync` for safe concurrent execution
/// - **Fallible**: Returns `NitriteResult<()>` to handle operation errors
/// - **Arc-Wrapped**: Allows sharing the same command across multiple owners
///
/// # Usage
/// Commands are used in journal entries to define forward (commit) and reverse (rollback) operations
pub type Command = Arc<dyn Fn() -> NitriteResult<()> + Send + Sync>;

/// Record of a single operation in a transaction.
///
/// # Purpose
/// Each `JournalEntry` represents one operation performed in a transaction, storing both
/// the forward operation (commit) and inverse operation (rollback) for two-phase commit protocol.
///
/// # Fields
/// - `change_type` - The type of change (Insert, Update, Remove, etc.)
/// - `commit` - Optional command to execute during commit phase
/// - `rollback` - Optional command to execute during rollback phase
///
/// # Characteristics
/// - **Reversible**: Stores both forward and backward operations
/// - **Optional Commands**: Either or both commands may be None for certain operations
/// - **Cloneable**: Cheap cloning with Arc-wrapped commands
#[derive(Clone)]
pub struct JournalEntry {
    pub change_type: ChangeType,
    pub commit: Option<Command>,
    pub rollback: Option<Command>,
}

impl JournalEntry {
    /// Creates a new `JournalEntry` with specified change type and commands.
    ///
    /// # Arguments
    /// * `change_type` - The type of change being recorded
    /// * `commit` - Optional command to execute on commit
    /// * `rollback` - Optional command to execute on rollback
    ///
    /// # Returns
    /// A new `JournalEntry` instance
    pub fn new(
        change_type: ChangeType,
        commit: Option<Command>,
        rollback: Option<Command>,
    ) -> Self {
        JournalEntry {
            change_type,
            commit,
            rollback,
        }
    }
}

impl std::fmt::Debug for JournalEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JournalEntry")
            .field("change_type", &self.change_type)
            .field("has_commit", &self.commit.is_some())
            .field("has_rollback", &self.rollback.is_some())
            .finish()
    }
}

/// Rollback information for a collection operation.
///
/// # Purpose
/// `UndoEntry` stores the rollback command for a single collection's operation,
/// used during the rollback phase if commit fails.
///
/// # Fields
/// - `collection_name` - Name of the collection affected by this operation
/// - `rollback` - Command to execute to undo the operation
///
/// # Characteristics
/// - **Per-Collection**: Each entry is associated with a specific collection
/// - **Arc-Wrapped**: Commands are shared efficiently across the undo registry
#[derive(Clone)]
pub struct UndoEntry {
    pub collection_name: String,
    pub rollback: Arc<Command>,
}

impl UndoEntry {
    /// Creates a new `UndoEntry` for rollback operations.
    ///
    /// # Arguments
    /// * `collection_name` - The name of the collection to undo
    /// * `rollback` - The command that undoes the operation
    ///
    /// # Returns
    /// A new `UndoEntry` instance
    pub fn new(collection_name: String, rollback: Arc<Command>) -> Self {
        UndoEntry {
            collection_name,
            rollback,
        }
    }
}

impl std::fmt::Debug for UndoEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UndoEntry")
            .field("collection_name", &self.collection_name)
            .field("has_rollback", &true)
            .finish()
    }
}

/// Per-collection transaction state container.
///
/// # Purpose
/// `TransactionContext` maintains transaction-specific state for a single collection,
/// including the operation journal and active status. It provides a transactional view
/// of the collection's data through a separate map.
///
/// # Characteristics
/// - **Thread-Safe**: Can be safely cloned and shared across threads
/// - **Lightweight Cloning**: Uses Arc internally for efficient sharing
/// - **Journal Management**: Maintains FIFO queue of operations
/// - **Lifecycle Tracking**: Tracks active/inactive status
///
/// # Usage
/// Created per collection when a transaction begins, used to coordinate
/// all operations on that collection within the transaction
#[derive(Clone)]
pub struct TransactionContext {
    inner: Arc<TransactionContextInner>,
}

impl TransactionContext {
    /// Creates a new transaction context for a collection.
    ///
    /// # Arguments
    /// * `collection_name` - The name of the collection this context manages
    /// * `txn_map` - The transactional map for this collection's operations
    ///
    /// # Returns
    /// A new `TransactionContext` instance
    pub fn new(collection_name: String, txn_map: crate::store::NitriteMap) -> Self {
        TransactionContext {
            inner: Arc::new(TransactionContextInner::new(collection_name, txn_map)),
        }
    }
}

impl Deref for TransactionContext {
    type Target = TransactionContextInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct TransactionContextInner {
    collection_name: String,
    pub journal: Arc<parking_lot::Mutex<VecDeque<JournalEntry>>>,
    active: Arc<AtomicBool>,
    txn_map: NitriteMap,
}

impl TransactionContextInner {
    /// Creates a new transaction context for a collection.
    ///
    /// # Arguments
    /// * `collection_name` - The name of the collection this context manages
    /// * `txn_map` - The transactional map for this collection's operations
    ///
    /// # Returns
    /// A new `TransactionContextInner` instance initialized in active state
    pub fn new(collection_name: String, txn_map: NitriteMap) -> Self {
        TransactionContextInner {
            collection_name,
            journal: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
            active: Arc::new(AtomicBool::new(true)),
            txn_map,
        }
    }

    /// Checks if this context is active.
    ///
    /// # Returns
    /// `true` if the context is active and accepting operations, `false` otherwise
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Marks this context as inactive.
    ///
    /// After marking inactive, no new operations can be added to the journal.
    pub fn set_inactive(&self) {
        self.active.store(false, Ordering::SeqCst);
    }

    /// Adds a journal entry to the operation log.
    ///
    /// # Arguments
    /// * `entry` - The `JournalEntry` to add
    ///
    /// # Returns
    /// * `Ok(())` if the entry was added successfully
    /// * `Err(NitriteError)` if the context is inactive
    pub fn add_entry(&self, entry: JournalEntry) -> NitriteResult<()> {
        if !self.is_active() {
            return Err(NitriteError::new(
                "Cannot add entry to inactive transaction context",
                ErrorKind::InvalidOperation,
            ));
        }
        self.journal.lock().push_back(entry);
        Ok(())
    }

    /// Gets the number of pending operations in the journal.
    ///
    /// # Returns
    /// The count of `JournalEntry` items waiting to be committed
    pub fn pending_operations(&self) -> usize {
        self.journal.lock().len()
    }

    /// Clears all entries from the journal.
    ///
    /// This removes all recorded operations from the transaction journal but does not
    /// change the active state. Used during rollback operations to discard uncommitted
    /// changes.
    pub fn clear(&self) {
        self.journal.lock().clear();
    }

    /// Closes the context and releases resources.
    ///
    /// Marks the context as inactive and clears the journal, preventing further operations.
    /// This is typically called when the transaction is completed (committed or rolled back).
    pub fn close(&self) {
        self.clear();
        self.set_inactive();
    }

    /// Returns the transactional map associated with this context.
    ///
    /// # Returns
    /// A cloned reference to the `NitriteMap` used for this transaction context
    pub fn txn_map(&self) -> NitriteMap {
        self.txn_map.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc as StdArc;

    #[test]
    fn test_transaction_state_active() {
        let state = TransactionState::Active;
        assert_eq!(state, TransactionState::Active);
        assert_ne!(state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_state_partially_committed() {
        let state = TransactionState::PartiallyCommitted;
        assert_eq!(state, TransactionState::PartiallyCommitted);
        assert_ne!(state, TransactionState::Active);
    }

    #[test]
    fn test_transaction_state_committed() {
        let state = TransactionState::Committed;
        assert_eq!(state, TransactionState::Committed);
        assert_ne!(state, TransactionState::Active);
    }

    #[test]
    fn test_transaction_state_closed() {
        let state = TransactionState::Closed;
        assert_eq!(state, TransactionState::Closed);
        assert_ne!(state, TransactionState::Active);
    }

    #[test]
    fn test_transaction_state_failed() {
        let state = TransactionState::Failed;
        assert_eq!(state, TransactionState::Failed);
        assert_ne!(state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_state_aborted() {
        let state = TransactionState::Aborted;
        assert_eq!(state, TransactionState::Aborted);
        assert_ne!(state, TransactionState::Committed);
    }

    #[test]
    fn test_transaction_state_debug_format() {
        let state = TransactionState::Active;
        let debug_str = format!("{:?}", state);
        assert_eq!(debug_str, "Active");
    }

    #[test]
    fn test_transaction_state_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let state1 = TransactionState::Active;
        let state2 = TransactionState::Active;
        let state3 = TransactionState::Committed;

        let mut hasher1 = DefaultHasher::new();
        state1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        state2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        state3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_change_type_insert() {
        let ct = ChangeType::Insert;
        assert_eq!(ct, ChangeType::Insert);
        assert_ne!(ct, ChangeType::Update);
    }

    #[test]
    fn test_change_type_update() {
        let ct = ChangeType::Update;
        assert_eq!(ct, ChangeType::Update);
        assert_ne!(ct, ChangeType::Insert);
    }

    #[test]
    fn test_change_type_remove() {
        let ct = ChangeType::Remove;
        assert_eq!(ct, ChangeType::Remove);
        assert_ne!(ct, ChangeType::Insert);
    }

    #[test]
    fn test_change_type_clear() {
        let ct = ChangeType::Clear;
        assert_eq!(ct, ChangeType::Clear);
    }

    #[test]
    fn test_change_type_create_index() {
        let ct = ChangeType::CreateIndex;
        assert_eq!(ct, ChangeType::CreateIndex);
    }

    #[test]
    fn test_change_type_rebuild_index() {
        let ct = ChangeType::RebuildIndex;
        assert_eq!(ct, ChangeType::RebuildIndex);
    }

    #[test]
    fn test_change_type_drop_index() {
        let ct = ChangeType::DropIndex;
        assert_eq!(ct, ChangeType::DropIndex);
    }

    #[test]
    fn test_change_type_drop_all_indexes() {
        let ct = ChangeType::DropAllIndexes;
        assert_eq!(ct, ChangeType::DropAllIndexes);
    }

    #[test]
    fn test_change_type_drop_collection() {
        let ct = ChangeType::DropCollection;
        assert_eq!(ct, ChangeType::DropCollection);
    }

    #[test]
    fn test_change_type_set_attributes() {
        let ct = ChangeType::SetAttributes;
        assert_eq!(ct, ChangeType::SetAttributes);
    }

    #[test]
    fn test_change_type_debug_format() {
        let ct = ChangeType::Insert;
        let debug_str = format!("{:?}", ct);
        assert_eq!(debug_str, "Insert");
    }

    #[test]
    fn test_change_type_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let ct1 = ChangeType::Insert;
        let ct2 = ChangeType::Insert;
        let ct3 = ChangeType::Update;

        let mut hasher1 = DefaultHasher::new();
        ct1.hash(&mut hasher1);
        let hash1 = hasher1.finish();

        let mut hasher2 = DefaultHasher::new();
        ct2.hash(&mut hasher2);
        let hash2 = hasher2.finish();

        let mut hasher3 = DefaultHasher::new();
        ct3.hash(&mut hasher3);
        let hash3 = hasher3.finish();

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_transaction_error_new_string() {
        let error = TransactionError::new("Test error message");
        assert_eq!(error.message(), "Test error message");
    }

    #[test]
    fn test_transaction_error_new_string_literal() {
        let error = TransactionError::new("Test");
        assert_eq!(error.message(), "Test");
    }

    #[test]
    fn test_transaction_error_message() {
        let error = TransactionError::new("Custom message");
        assert_eq!(error.message(), "Custom message");
    }

    #[test]
    fn test_transaction_error_display() {
        let error = TransactionError::new("Display test");
        let display_str = format!("{}", error);
        assert_eq!(display_str, "Transaction error: Display test");
    }

    #[test]
    fn test_transaction_error_debug() {
        let error = TransactionError::new("Debug test");
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Debug test"));
    }

    #[test]
    fn test_transaction_error_as_error() {
        let error = TransactionError::new("Error trait test");
        let _err: &dyn std::error::Error = &error;
        // Verifies that TransactionError implements std::error::Error
    }

    #[test]
    fn test_transaction_error_clone() {
        let error1 = TransactionError::new("Clone test");
        let error2 = error1.clone();
        assert_eq!(error1.message(), error2.message());
    }

    #[test]
    fn test_journal_entry_new_with_all_commands() {
        let commit_cmd: Command = Arc::new(|| Ok(()));
        let rollback_cmd: Command = Arc::new(|| Ok(()));

        let entry = JournalEntry::new(
            ChangeType::Insert,
            Some(commit_cmd),
            Some(rollback_cmd),
        );

        assert_eq!(entry.change_type, ChangeType::Insert);
        assert!(entry.commit.is_some());
        assert!(entry.rollback.is_some());
    }

    #[test]
    fn test_journal_entry_new_with_commit_only() {
        let commit_cmd: Command = Arc::new(|| Ok(()));

        let entry = JournalEntry::new(ChangeType::Update, Some(commit_cmd), None);

        assert_eq!(entry.change_type, ChangeType::Update);
        assert!(entry.commit.is_some());
        assert!(entry.rollback.is_none());
    }

    #[test]
    fn test_journal_entry_new_with_no_commands() {
        let entry = JournalEntry::new(ChangeType::Remove, None, None);

        assert_eq!(entry.change_type, ChangeType::Remove);
        assert!(entry.commit.is_none());
        assert!(entry.rollback.is_none());
    }

    #[test]
    fn test_journal_entry_clone() {
        let commit_cmd: Command = Arc::new(|| Ok(()));
        let entry1 = JournalEntry::new(ChangeType::Insert, Some(commit_cmd), None);
        let entry2 = entry1.clone();

        assert_eq!(entry1.change_type, entry2.change_type);
        assert!(entry2.commit.is_some());
    }

    #[test]
    fn test_journal_entry_debug_with_commands() {
        let commit_cmd: Command = Arc::new(|| Ok(()));
        let rollback_cmd: Command = Arc::new(|| Ok(()));
        let entry = JournalEntry::new(
            ChangeType::Insert,
            Some(commit_cmd),
            Some(rollback_cmd),
        );

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("JournalEntry"));
        assert!(debug_str.contains("has_commit: true"));
        assert!(debug_str.contains("has_rollback: true"));
    }

    #[test]
    fn test_journal_entry_debug_without_commands() {
        let entry = JournalEntry::new(ChangeType::Insert, None, None);

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("JournalEntry"));
        assert!(debug_str.contains("has_commit: false"));
        assert!(debug_str.contains("has_rollback: false"));
    }

    #[test]
    fn test_journal_entry_debug_partial_commands() {
        let commit_cmd: Command = Arc::new(|| Ok(()));
        let entry = JournalEntry::new(ChangeType::Update, Some(commit_cmd), None);

        let debug_str = format!("{:?}", entry);
        assert!(debug_str.contains("has_commit: true"));
        assert!(debug_str.contains("has_rollback: false"));
    }

    #[test]
    fn test_undo_entry_creation() {
        let rollback_cmd: Command = Arc::new(|| Ok(()));
        let undo = UndoEntry {
            collection_name: "test_collection".to_string(),
            rollback: Arc::new(rollback_cmd),
        };

        assert_eq!(undo.collection_name, "test_collection");
    }

    #[test]
    fn test_undo_entry_debug() {
        let rollback_cmd: Command = Arc::new(|| Ok(()));
        let undo = UndoEntry {
            collection_name: "debug_test".to_string(),
            rollback: Arc::new(rollback_cmd),
        };

        let debug_str = format!("{:?}", undo);
        assert!(debug_str.contains("UndoEntry"));
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("has_rollback: true"));
    }

    #[test]
    fn test_undo_entry_multiple_collections() {
        let rollback_cmd1: Command = Arc::new(|| Ok(()));
        let rollback_cmd2: Command = Arc::new(|| Ok(()));

        let undo1 = UndoEntry {
            collection_name: "col1".to_string(),
            rollback: Arc::new(rollback_cmd1),
        };

        let undo2 = UndoEntry {
            collection_name: "col2".to_string(),
            rollback: Arc::new(rollback_cmd2),
        };

        assert_eq!(undo1.collection_name, "col1");
        assert_eq!(undo2.collection_name, "col2");
    }

    // Mock NitriteMap for testing
    #[derive(Copy, Clone)]
    struct MockNitriteMap;

    impl crate::common::AttributeAware for MockNitriteMap {
        fn attributes(&self) -> NitriteResult<Option<crate::common::Attributes>> {
            Ok(None)
        }

        fn set_attributes(&self, _attributes: crate::common::Attributes) -> NitriteResult<()> {
            Ok(())
        }
    }

    impl crate::store::NitriteMapProvider for MockNitriteMap {
        fn contains_key(&self, _key: &crate::common::Key) -> NitriteResult<bool> {
            Ok(true)
        }

        fn get(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Value>> {
            Ok(Some(crate::common::Value::from("test")))
        }

        fn clear(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_closed(&self) -> NitriteResult<bool> {
            Ok(false)
        }

        fn close(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn values(&self) -> NitriteResult<crate::store::ValueIterator> {
            Err(NitriteError::new(
                "Invalid operation",
                ErrorKind::InvalidOperation,
            ))
        }

        fn keys(&self) -> NitriteResult<crate::store::KeyIterator> {
            Err(NitriteError::new(
                "Invalid operation",
                ErrorKind::InvalidOperation,
            ))
        }

        fn remove(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Value>> {
            Ok(None)
        }

        fn put(&self, _key: crate::common::Key, _value: crate::common::Value) -> NitriteResult<()> {
            Ok(())
        }

        fn size(&self) -> NitriteResult<u64> {
            Ok(0)
        }

        fn put_if_absent(
            &self,
            _key: crate::common::Key,
            _value: crate::common::Value,
        ) -> NitriteResult<Option<crate::common::Value>> {
            Ok(None)
        }

        fn first_key(&self) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn last_key(&self) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn higher_key(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn ceiling_key(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn lower_key(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn floor_key(&self, _key: &crate::common::Key) -> NitriteResult<Option<crate::common::Key>> {
            Ok(None)
        }

        fn is_empty(&self) -> NitriteResult<bool> {
            Ok(true)
        }

        fn get_store(&self) -> NitriteResult<crate::store::NitriteStore> {
            Err(NitriteError::new(
                "Invalid operation",
                ErrorKind::InvalidOperation,
            ))
        }

        fn get_name(&self) -> NitriteResult<String> {
            Ok("mock".to_string())
        }

        fn entries(&self) -> NitriteResult<crate::store::EntryIterator> {
            Err(NitriteError::new(
                "Invalid operation",
                ErrorKind::InvalidOperation,
            ))
        }

        fn reverse_entries(&self) -> NitriteResult<std::iter::Rev<crate::store::EntryIterator>> {
            Err(NitriteError::new(
                "Invalid operation",
                ErrorKind::InvalidOperation,
            ))
        }

        fn dispose(&self) -> NitriteResult<()> {
            Ok(())
        }

        fn is_dropped(&self) -> NitriteResult<bool> {
            Ok(false)
        }
    }

    #[test]
    fn test_transaction_context_creation() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("test_collection".to_string(), map);

        assert!(context.is_active());
    }

    #[test]
    fn test_transaction_context_is_active() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        assert!(context.is_active());
    }

    #[test]
    fn test_transaction_context_set_inactive() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        assert!(context.is_active());
        context.set_inactive();
        assert!(!context.is_active());
    }

    #[test]
    fn test_transaction_context_add_entry() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        let entry = JournalEntry::new(ChangeType::Insert, None, None);
        let result = context.add_entry(entry);

        assert!(result.is_ok());
        assert_eq!(context.pending_operations(), 1);
    }

    #[test]
    fn test_transaction_context_add_entry_when_inactive() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        context.set_inactive();

        let entry = JournalEntry::new(ChangeType::Insert, None, None);
        let result = context.add_entry(entry);

        assert!(result.is_err());
        assert_eq!(context.pending_operations(), 0);
    }

    #[test]
    fn test_transaction_context_multiple_entries() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        for i in 0..5 {
            let change_type = match i % 3 {
                0 => ChangeType::Insert,
                1 => ChangeType::Update,
                _ => ChangeType::Remove,
            };
            let entry = JournalEntry::new(change_type, None, None);
            assert!(context.add_entry(entry).is_ok());
        }

        assert_eq!(context.pending_operations(), 5);
    }

    #[test]
    fn test_transaction_context_pending_operations_empty() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        assert_eq!(context.pending_operations(), 0);
    }

    #[test]
    fn test_transaction_context_clear_journal() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        for _ in 0..3 {
            let entry = JournalEntry::new(ChangeType::Insert, None, None);
            let _ = context.add_entry(entry);
        }

        assert_eq!(context.pending_operations(), 3);
        context.clear();
        assert_eq!(context.pending_operations(), 0);
    }

    #[test]
    fn test_transaction_context_close() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        for _ in 0..2 {
            let entry = JournalEntry::new(ChangeType::Update, None, None);
            let _ = context.add_entry(entry);
        }

        assert!(context.is_active());
        assert_eq!(context.pending_operations(), 2);

        context.close();

        assert!(!context.is_active());
        assert_eq!(context.pending_operations(), 0);
    }

    #[test]
    fn test_transaction_context_txn_map() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map.clone());

        let retrieved_map = context.txn_map();
        // Verify that we get a cloned map by ensuring it can be used
        let _ = retrieved_map.is_empty();
    }

    #[test]
    fn test_transaction_context_clone() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context1 = TransactionContext::new("col".to_string(), map);
        let context2 = context1.clone();

        let entry = JournalEntry::new(ChangeType::Insert, None, None);
        assert!(context1.add_entry(entry).is_ok());

        // Both should see the same journal due to Arc sharing
        assert_eq!(context1.pending_operations(), 1);
        assert_eq!(context2.pending_operations(), 1);
    }

    #[test]
    fn test_transaction_context_deref() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        // Test Deref trait
        let inner: &TransactionContextInner = &*context;
        assert!(inner.is_active());
    }

    #[test]
    fn test_transaction_context_journal_fifo_order() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = TransactionContext::new("col".to_string(), map);

        let entry1 = JournalEntry::new(ChangeType::Insert, None, None);
        let entry2 = JournalEntry::new(ChangeType::Update, None, None);
        let entry3 = JournalEntry::new(ChangeType::Remove, None, None);

        assert!(context.add_entry(entry1).is_ok());
        assert!(context.add_entry(entry2).is_ok());
        assert!(context.add_entry(entry3).is_ok());

        let journal = context.journal.lock();
        assert_eq!(journal.len(), 3);
        assert_eq!(journal[0].change_type, ChangeType::Insert);
        assert_eq!(journal[1].change_type, ChangeType::Update);
        assert_eq!(journal[2].change_type, ChangeType::Remove);
    }

    #[test]
    fn test_transaction_context_concurrent_access() {
        let map = crate::store::NitriteMap::new(MockNitriteMap);
        let context = StdArc::new(TransactionContext::new("col".to_string(), map));

        let counter = StdArc::new(AtomicUsize::new(0));
        let mut handles = vec![];

        for _ in 0..4 {
            let ctx = StdArc::clone(&context);
            let cnt = StdArc::clone(&counter);

            let handle = std::thread::spawn(move || {
                let entry = JournalEntry::new(ChangeType::Insert, None, None);
                if ctx.add_entry(entry).is_ok() {
                    cnt.fetch_add(1, AtomicOrdering::SeqCst);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(context.pending_operations(), 4);
        assert_eq!(counter.load(AtomicOrdering::SeqCst), 4);
    }

    #[test]
    fn test_command_type_execution_success() {
        let executed = StdArc::new(AtomicUsize::new(0));
        let executed_clone = StdArc::clone(&executed);

        let cmd: Command = Arc::new(move || {
            executed_clone.fetch_add(1, AtomicOrdering::SeqCst);
            Ok(())
        });

        let result = cmd();
        assert!(result.is_ok());
        assert_eq!(executed.load(AtomicOrdering::SeqCst), 1);
    }

    #[test]
    fn test_command_type_execution_error() {
        let cmd: Command = Arc::new(|| {
            Err(NitriteError::new("Test error", ErrorKind::InvalidOperation))
        });

        let result = cmd();
        assert!(result.is_err());
    }
}