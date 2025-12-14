//! Storage backends and abstractions.
//!
//! This module provides the storage layer abstraction for Nitrite.
//! The storage system is pluggable, allowing different implementations
//! (in-memory, file-based, etc.).
//!
//! # Storage Providers
//!
//! Storage providers implement `NitriteStoreProvider` and are loaded as plugins.
//! Nitrite includes:
//! - **In-Memory Store**: `InMemoryStoreModule` for testing and temporary data
//! - **Fjall Store**: `nitrite-fjall-adapter` for persistent, LSM-based storage
//!
//! # Key-Value Abstraction
//!
//! The storage layer provides a key-value map interface through `NitriteMapProvider`.
//! Maps support:
//! - Basic operations: get, put, remove
//! - Iteration: keys, values, entries
//! - Metadata: size, attributes
//!
//! # Persistence
//!
//! Storage is abstracted behind the `NitriteStoreProvider` trait, allowing:
//! - Multiple store implementations
//! - Custom storage backends
//! - Testing with in-memory stores
//!
//! # Events
//!
//! The store layer supports event listeners for monitoring store state changes,
//! useful for debugging and metrics collection.

mod event;
mod iters;
pub mod memory;
mod meta;
mod nitrite_map;
mod nitrite_store;
mod store_catalog;
mod store_config;
mod store_module;

pub use event::*;
pub use iters::*;
pub use meta::*;
pub use nitrite_map::*;
pub use nitrite_store::*;
pub use store_catalog::*;
pub use store_config::*;
pub use store_module::*;