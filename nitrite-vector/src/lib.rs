//! # Nitrite Vector — HNSW ANN index & RAG store for Nitrite
//!
//! This crate adds an approximate-nearest-neighbour (ANN) vector index to the
//! Nitrite embedded database, backed by a hand-rolled, persistent
//! **HNSW** (Hierarchical Navigable Small World) graph, plus a thin
//! [`RagStore`] convenience layer for retrieval-augmented-generation workloads.
//!
//! Embeddings are **provided by the caller** (bring-your-own vectors); this
//! crate does not generate embeddings.
//!
//! ## Quick start (raw collection API)
//!
//! ```rust,ignore
//! use nitrite::nitrite::Nitrite;
//! use nitrite::common::PersistentCollection;
//! use nitrite_vector::{VectorModule, VectorIndexConfig, vector_index_options, vector_field};
//! use nitrite_vector::distance::Metric;
//!
//! let db = Nitrite::builder()
//!     .load_module(VectorModule::new(VectorIndexConfig::new(3, Metric::Cosine)))
//!     .open_or_create(None, None)?;
//!
//! let collection = db.collection("docs")?;
//! collection.create_index(vec!["embedding"], &vector_index_options())?;
//!
//! // ... insert documents whose `embedding` field is a numeric array ...
//!
//! let filter = vector_field("embedding").nearest(vec![0.1, 0.2, 0.3], 5).build();
//! let results = collection.find(filter)?;
//! ```
//!
//! ## RAG store
//!
//! ```rust,ignore
//! use nitrite_vector::{RagStore, VectorIndexConfig};
//! use nitrite_vector::distance::Metric;
//!
//! let store = RagStore::create(&db, "kb", VectorIndexConfig::new(384, Metric::Cosine))?;
//! store.add("hello world", embedding, doc!{ "source": "wiki" })?;
//! let hits = store.search(query_vector, 5).run()?;
//! ```

pub mod diskann;
pub mod distance;
pub mod filter;
pub mod fluent;
pub mod hnsw;
pub mod indexer;
pub mod module;
pub mod node;
pub mod precision;
pub mod rag;
pub mod vector_index;

pub use diskann::{DiskAnnConfig, DiskAnnIndex};
pub use distance::Metric;
pub use filter::{value_to_vector, vector_to_value, VectorNearestFilter, VECTOR_INDEX};
pub use fluent::{vector_field, VectorFluentFilter, VectorNearestBuilder};
pub use indexer::VectorIndexer;
pub use module::{VectorModule, VectorModuleBuilder};
pub use precision::Precision;
pub use rag::{RagStore, SearchHit, SearchQuery};
pub use vector_index::{
    derive_vector_map_name, HnswBackend, IndexBackend, VectorIndex, VectorIndexConfig,
};

/// Creates [`IndexOptions`](nitrite::index::IndexOptions) for a vector index.
///
/// Use with `collection.create_index(vec!["embedding"], &vector_index_options())`.
pub fn vector_index_options() -> nitrite::index::IndexOptions {
    nitrite::index::IndexOptions::new(VECTOR_INDEX)
}
