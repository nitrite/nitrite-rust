#![allow(
    dead_code,
    unused_imports,
    clippy::invisible_characters,
    clippy::approx_constant,
)]
//! # Nitrite - Embedded NoSQL Database
//!
//! Nitrite is a lightweight, feature-rich, embedded NoSQL database written in Rust.
//! It provides document and object storage with rich querying capabilities, indexing,
//! and transaction support.
//!
//! ## Key Features
//!
//! - **Embedded**: No separate server process required
//! - **NoSQL**: Document-based and object-oriented storage
//! - **Rich Querying**: Powerful filter API with support for complex queries
//! - **Indexing**: Support for unique, non-unique, and full-text indexes
//! - **Spatial**: Geographic and spatial data support through the `nitrite-spatial` crate
//! - **Transactions**: ACID transaction support
//! - **Migration**: Schema migration management
//! - **Events**: Event listeners for database, collection, and store events
//! - **Multiple Storage Backends**: In-memory storage and pluggable store providers
//! - **Clean API**: PIMPL pattern provides stable, encapsulated interface
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use nitrite::nitrite_builder::NitriteBuilder;
//! use nitrite::collection::Document;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! // Create or open a database
//! let db = Nitrite::builder()
//!     .open_or_create(None, None)?;
//!
//! // Get or create a collection
//! let mut collection = db.collection("users")?;
//!
//! // Create a document
//! let mut doc = Document::new();
//! doc.put("name", "John Doe")?;
//! doc.put("age", 30i64)?;
//!
//! // Insert the document
//! collection.insert(doc)?;
//!
//! // Find documents using filters
//! let filter = nitrite::filter::all();
//! let results = collection.find(filter)?;
//!
//! // Close the database
//! db.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Design Pattern
//!
//! Nitrite uses the **PIMPL (Pointer To IMPLementation)** design pattern to provide:
//!
//! - **Encapsulation**: Implementation details are completely hidden
//! - **API Stability**: Public interface is stable and can evolve independently
//! - **Implementation Flexibility**: Internal structure can be refactored without affecting users
//! - **Thread Safety**: All clones share the same underlying state through `Arc<NitriteInner>`
//!
//! This design ensures that the Nitrite database provides a clean, stable API while keeping implementation 
//! complexity hidden.
//!
//! ## Module Organization
//!
//! - [`collection`] - Document collections, repositories, and document operations
//! - [`common`] - Common types, traits, and utilities
//! - [`errors`] - Error types and result definitions
//! - [`filter`] - Query filters and filter providers
//! - [`index`] - Indexing support (unique, non-unique, full-text)
//! - [`metadata`] - Database metadata management
//! - [`migration`] - Schema migration support
//! - [`nitrite`] - Core database interface
//! - [`nitrite_builder`] - Database builder for initialization
//! - [`nitrite_config`] - Database configuration
//! - [`repository`] - Type-safe object repositories
//! - [`store`] - Storage backend abstractions
//! - [`transaction`] - Transaction support

use crate::collection::snowflake::SnowflakeIdGenerator;
use crate::common::*;
use std::sync::LazyLock;
use std::thread::available_parallelism;


pub mod collection;
pub mod common;
pub mod errors;
pub mod filter;
pub mod index;
pub mod metadata;
pub mod migration;
pub mod nitrite;
pub mod nitrite_builder;
pub mod nitrite_config;
pub mod repository;
pub mod store;
pub mod transaction;


pub(crate) static FIELD_SEPARATOR: LazyLock<Atomic<String>> = LazyLock::new(|| atomic(".".to_string()));
pub(crate) static ID_GENERATOR: LazyLock<SnowflakeIdGenerator> =
    LazyLock::new(SnowflakeIdGenerator::new);

pub(crate) static SCHEDULER: LazyLock<Scheduler> = LazyLock::new(Scheduler::new);

/// Returns the number of available CPU cores.
///
/// This function attempts to detect the number of available processors on the system.
/// If detection fails, it defaults to 1.
///
/// # Returns
///
/// A `usize` representing the number of available CPU cores.
///
/// # Examples
///
/// ```rust
/// use nitrite::get_cpu_count;
///
/// let cpu_count = get_cpu_count();
/// println!("Available CPUs: {}", cpu_count);
/// assert!(cpu_count > 0);
/// ```
pub fn get_cpu_count() -> usize {
    available_parallelism()
        .map(|p| p.get())
        .unwrap_or_else(|err| {
            log::warn!("Failed to detect available parallelism: {}. Defaulting to single thread.", err);
            1
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_cpu_count_positive() {
        // Test that get_cpu_count returns a positive value
        let count = get_cpu_count();
        assert!(count > 0);
    }

    #[test]
    fn test_scheduler_initialization() {
        // Test that SCHEDULER initializes successfully
        let _scheduler = &*SCHEDULER;
        // If we can access it, initialization was successful
        assert!(true);
    }
}