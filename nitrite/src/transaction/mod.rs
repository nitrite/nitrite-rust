//! Transaction module for Nitrite
//!
//! Provides ACID transaction support for embedded database operations
//! with journal-based undo/redo mechanism and Copy-On-Write pattern.

pub mod core;
pub mod iters;
pub mod nitrite_transaction;
pub mod session;
pub mod transaction_store;
pub mod transactional_collection;
pub mod transactional_map;
pub mod transactional_repository;

pub use core::{
    ChangeType, Command, JournalEntry, TransactionContext, TransactionError, TransactionState,
    UndoEntry,
};
pub use iters::{TransactionEntryProvider, TransactionKeyProvider, TransactionValueProvider};
pub use nitrite_transaction::NitriteTransaction;
pub use session::Session;
pub use transaction_store::TransactionStore;
pub use transactional_map::TransactionalMap;
