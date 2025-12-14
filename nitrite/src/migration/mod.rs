//! Schema migration support for database evolution.
//!
//! This module provides mechanisms to manage database schema changes over time.
//! As applications evolve, their data structures change, and migrations enable
//! safe transformation of existing data.
//!
//! # Migration Process
//!
//! A migration:
//! 1. Defines source and target schema versions
//! 2. Specifies transformation steps for affected data
//! 3. Is executed automatically when opening a database with a different schema version
//!
//! # Creating Migrations
//!
//! ```rust,ignore
//! use nitrite::migration::{Migration, MigrationStep};
//!
//! let migration = Migration::create(1, 2)
//!     .add_instruction(/* ... */)
//!     .finalize();
//!
//! let db = Nitrite::builder()
//!     .schema_version(2)
//!     .add_migration(migration)
//!     .open_or_create(None, None)?;
//! ```
//!
//! # Migration Types
//!
//! - **Collection Instructions**: Add/remove collections, create indexes
//! - **Repository Instructions**: Define object repository structure
//! - **Database Instructions**: Global database-level changes
//!
//! # Atomicity
//!
//! Migrations are applied atomically - either all changes succeed or none are applied.
//! If a migration fails, the database is rolled back to its previous state.

mod manager;
mod migration;
mod instructions;
mod commands;

pub use instructions::*;
pub use manager::MigrationManager;
pub use migration::{
    Migration, MigrationArguments, MigrationStep,
};
