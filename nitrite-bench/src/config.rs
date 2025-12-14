//! Benchmark configuration

use std::path::PathBuf;

/// Storage backend type for benchmarks
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreType {
    /// In-memory storage (fast, no persistence)
    InMemory,
    /// Fjall persistent storage
    Fjall,
}

impl std::fmt::Display for StoreType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreType::InMemory => write!(f, "inmemory"),
            StoreType::Fjall => write!(f, "fjall"),
        }
    }
}

/// Benchmark category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchmarkCategory {
    Crud,
    Indexing,
    IndexedSearch,
    SpatialIndexing,
    SpatialSearch,
    FtsIndexing,
    FtsSearch,
}

impl std::fmt::Display for BenchmarkCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BenchmarkCategory::Crud => write!(f, "CRUD"),
            BenchmarkCategory::Indexing => write!(f, "Indexing"),
            BenchmarkCategory::IndexedSearch => write!(f, "Indexed Search"),
            BenchmarkCategory::SpatialIndexing => write!(f, "Spatial Indexing"),
            BenchmarkCategory::SpatialSearch => write!(f, "Spatial Search"),
            BenchmarkCategory::FtsIndexing => write!(f, "FTS Indexing"),
            BenchmarkCategory::FtsSearch => write!(f, "FTS Search"),
        }
    }
}

/// Configuration for benchmark runs
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Base path for temporary databases
    pub base_path: PathBuf,
    /// Document counts to benchmark
    pub document_counts: Vec<usize>,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            base_path: std::env::temp_dir().join("nitrite-bench"),
            document_counts: vec![100, 1_000, 10_000, 50_000],
        }
    }
}

impl BenchmarkConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Quick config with smaller document counts for fast testing
    pub fn quick() -> Self {
        Self {
            document_counts: vec![100, 1_000],
            ..Default::default()
        }
    }
}
