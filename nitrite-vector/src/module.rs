//! Nitrite module that registers the vector indexer, plus a fluent builder that
//! surfaces every configurable knob.
//!
//! ```rust,ignore
//! use nitrite::nitrite::Nitrite;
//! use nitrite_vector::{VectorModule, VectorIndexConfig, IndexBackend, Precision, Metric};
//!
//! // Simple in-memory HNSW:
//! let db = Nitrite::builder()
//!     .load_module(VectorModule::builder(384, Metric::Cosine).build())
//!     .open_or_create(None, None)?;
//!
//! // Disk-resident DiskANN sized to the device:
//! let module = VectorModule::builder(384, Metric::Cosine)
//!     .backend(IndexBackend::DiskAnn)
//!     .precision(Precision::F16)
//!     .cache_bytes(128 * 1024 * 1024)
//!     .degree(64)
//!     .pq_subvectors(16)
//!     .build();
//!
//! // Different dimensions/metrics per index in one database:
//! let module = VectorModule::builder(384, Metric::Cosine)
//!     .index_config("images", "clip", VectorIndexConfig::new(512, Metric::Dot))
//!     .build();
//! ```

use std::collections::HashMap;

use nitrite::common::{NitriteModule, NitritePlugin, PluginRegistrar};
use nitrite::errors::NitriteResult;
use nitrite::index::NitriteIndexer;

use crate::distance::Metric;
use crate::indexer::VectorIndexer;
use crate::precision::Precision;
use crate::vector_index::{IndexBackend, VectorIndexConfig};

/// Module enabling vector indexing. Holds the parameters used for indexes
/// created while it is loaded.
pub struct VectorModule {
    indexer: VectorIndexer,
}

impl VectorModule {
    /// Creates a module from an explicit [`VectorIndexConfig`].
    pub fn new(config: VectorIndexConfig) -> Self {
        VectorModule {
            indexer: VectorIndexer::new(config),
        }
    }

    /// Starts a fluent builder exposing every knob.
    pub fn builder(dim: usize, metric: Metric) -> VectorModuleBuilder {
        VectorModuleBuilder {
            config: VectorIndexConfig::new(dim, metric),
            per_index: HashMap::new(),
        }
    }
}

impl NitriteModule for VectorModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        Ok(vec![NitritePlugin::new(self.indexer.clone())])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        plugin_registrar.register_indexer_plugin(NitriteIndexer::new(self.indexer.clone()))
    }
}

/// Fluent builder for [`VectorModule`]. Every setter maps to a
/// [`VectorIndexConfig`] field that drives backend behavior.
pub struct VectorModuleBuilder {
    config: VectorIndexConfig,
    per_index: HashMap<(String, String), VectorIndexConfig>,
}

impl VectorModuleBuilder {
    /// Registers a dedicated config for one `(collection, field)` index, so
    /// collections with different embedding dimensions, metrics, or backends
    /// can coexist in a single database. Indexes without a dedicated config
    /// use the builder's defaults.
    pub fn index_config(
        mut self,
        collection: impl Into<String>,
        field: impl Into<String>,
        config: VectorIndexConfig,
    ) -> Self {
        self.per_index.insert((collection.into(), field.into()), config);
        self
    }

    /// Selects the backend (`Hnsw` default, or `DiskAnn` for disk-resident).
    pub fn backend(mut self, backend: IndexBackend) -> Self {
        self.config = self.config.backend(backend);
        self
    }

    /// Stored-vector precision (F32 / F16 / I8).
    pub fn precision(mut self, precision: Precision) -> Self {
        self.config = self.config.precision(precision);
        self
    }

    // ---- HNSW knobs ----

    /// HNSW graph connectivity `M`.
    pub fn m(mut self, m: usize) -> Self {
        self.config = self.config.with_m(m);
        self
    }

    /// HNSW `ef_construction`.
    pub fn ef_construction(mut self, ef: usize) -> Self {
        self.config = self.config.with_ef_construction(ef);
        self
    }

    /// HNSW default `ef_search`.
    pub fn ef_search(mut self, ef: usize) -> Self {
        self.config = self.config.with_ef_search(ef);
        self
    }

    // ---- DiskANN knobs ----

    /// DiskANN LRU cache budget in bytes (size to the device's RAM).
    pub fn cache_bytes(mut self, bytes: usize) -> Self {
        self.config = self.config.cache_bytes(bytes);
        self
    }

    /// DiskANN graph out-degree `R`.
    pub fn degree(mut self, degree: usize) -> Self {
        self.config = self.config.degree(degree);
        self
    }

    /// DiskANN construction search width `L`.
    pub fn build_beam(mut self, beam: usize) -> Self {
        self.config = self.config.build_beam(beam);
        self
    }

    /// DiskANN default query search width `L`.
    pub fn search_beam(mut self, beam: usize) -> Self {
        self.config = self.config.search_beam(beam);
        self
    }

    /// DiskANN RobustPrune slack `alpha`.
    pub fn alpha(mut self, alpha: f32) -> Self {
        self.config = self.config.alpha(alpha);
        self
    }

    /// DiskANN PQ subvector count (bytes per code; `0` disables PQ).
    pub fn pq_subvectors(mut self, m: usize) -> Self {
        self.config = self.config.pq_subvectors(m);
        self
    }

    /// DiskANN PQ training threshold (vectors).
    pub fn pq_train_threshold(mut self, n: usize) -> Self {
        self.config = self.config.pq_train_threshold(n);
        self
    }

    /// DiskANN background delete-consolidation threshold (pending slots).
    pub fn consolidate_threshold(mut self, n: usize) -> Self {
        self.config = self.config.consolidate_threshold(n);
        self
    }

    /// The assembled [`VectorIndexConfig`].
    pub fn config(&self) -> VectorIndexConfig {
        self.config
    }

    /// Builds the module.
    pub fn build(self) -> VectorModule {
        VectorModule {
            indexer: VectorIndexer::with_configs(self.config, self.per_index),
        }
    }
}
