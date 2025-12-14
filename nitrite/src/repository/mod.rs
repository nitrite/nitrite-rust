//! Object repositories for type-safe data persistence.
//!
//! This module provides `ObjectRepository`, a type-safe wrapper around collections
//! that automatically handles serialization and deserialization of Rust types.
//!
//! # Repositories vs Collections
//!
//! - **Collections** work with `Document` objects and are schemaless
//! - **Repositories** work with strongly-typed Rust structs and provide compile-time safety
//!
//! # Creating Repositories
//!
//! ```rust,ignore
//! use nitrite::repository::ObjectRepository;
//! use nitrite_derive::{NitriteEntity, Convertible};
//!
//! #[derive(NitriteEntity, Convertible)]
//! pub struct User {
//!     pub name: String,
//!     pub age: u32,
//! }
//!
//! // Get or create a repository
//! let repo = db.repository::<User>()?;
//!
//! // Or create a keyed repository for multiple instances
//! let repo_prod = db.keyed_repository::<User>("prod")?;
//! let repo_test = db.keyed_repository::<User>("test")?;
//! ```
//!
//! # Operations
//!
//! Repositories support the same operations as collections:
//! - Insert/update/remove documents
//! - Query with filters and options
//! - Create indexes
//! - Event listeners

mod entity;
mod repository;
mod cursor;
mod repository_factory;
mod repository_operations;
mod default_object_repository;

pub use cursor::*;
pub use entity::*;
pub use repository::*;
pub(crate) use repository_factory::*;
pub(crate) use repository_operations::*;
