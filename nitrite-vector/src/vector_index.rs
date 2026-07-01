//! Durable HNSW index: bridges the in-memory [`Hnsw`] graph with a Nitrite
//! `NitriteMap` so the graph survives restarts and participates in the store's
//! atomicity.
//!
//! Layout (one map per index, named by [`derive_vector_map_name`]):
//! - `Value::String("__hnsw_meta__")` → bincode of [`HnswHeader`].
//! - `Value::U64(doc_id)` → bincode of a [`NodeRecord`].
//!
//! On open the whole graph is loaded into memory. Each mutation updates memory
//! then writes through only the touched node records plus the header.

use std::sync::Arc;

use parking_lot::RwLock;

use nitrite::collection::NitriteId;
use nitrite::common::Value;
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::IndexDescriptor;
use nitrite::nitrite_config::NitriteConfig;
use nitrite::store::NitriteMap;

use crate::diskann::{DiskAnnConfig, DiskAnnIndex};
use crate::distance::Metric;
use crate::hnsw::Hnsw;
use crate::node::{from_bytes, to_bytes, HnswHeader, NodeRecord};
use crate::precision::Precision;

/// Reserved key holding the serialized [`HnswHeader`].
const META_KEY: &str = "__hnsw_meta__";

/// Which index backend to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBackend {
    /// In-memory HNSW graph persisted to fjall (default). Best when the index
    /// fits in RAM.
    Hnsw,
    /// Disk-resident DiskANN/Vamana + PQ. RAM-bounded; for indexes larger than
    /// memory (e.g. mobile).
    DiskAnn,
}

/// Static parameters for a vector index, supplied at index-creation time and
/// then persisted in the index header. Every field drives backend behavior.
#[derive(Debug, Clone, Copy)]
pub struct VectorIndexConfig {
    pub dim: usize,
    pub metric: Metric,
    pub backend: IndexBackend,
    /// Stored-vector precision (both backends honor this for vector storage).
    pub precision: Precision,
    // HNSW knobs
    pub m: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    // DiskANN knobs
    pub diskann: DiskAnnConfig,
}

impl VectorIndexConfig {
    /// Creates a config with sensible defaults (HNSW backend, F32 precision).
    pub fn new(dim: usize, metric: Metric) -> Self {
        VectorIndexConfig {
            dim,
            metric,
            backend: IndexBackend::Hnsw,
            precision: Precision::F32,
            m: 16,
            ef_construction: 200,
            ef_search: 64,
            diskann: DiskAnnConfig::default(),
        }
    }

    /// Selects the index backend.
    pub fn backend(mut self, backend: IndexBackend) -> Self {
        self.backend = backend;
        self
    }

    /// Sets the stored-vector precision.
    pub fn precision(mut self, precision: Precision) -> Self {
        self.precision = precision;
        self
    }

    /// Overrides the HNSW graph connectivity `M`.
    pub fn with_m(mut self, m: usize) -> Self {
        self.m = m;
        self
    }

    /// Overrides HNSW `ef_construction` (build-time search width).
    pub fn with_ef_construction(mut self, ef: usize) -> Self {
        self.ef_construction = ef;
        self
    }

    /// Overrides the default HNSW query-time `ef_search`.
    pub fn with_ef_search(mut self, ef: usize) -> Self {
        self.ef_search = ef;
        self
    }

    /// DiskANN: advisory resident-memory budget (the flat store is mmap'd, so
    /// the OS page cache bounds resident memory; reserved for `madvise` hints).
    pub fn cache_bytes(mut self, bytes: usize) -> Self {
        self.diskann.cache_bytes = bytes;
        self
    }

    /// DiskANN: graph out-degree `R`.
    pub fn degree(mut self, degree: usize) -> Self {
        self.diskann.degree = degree;
        self
    }

    /// DiskANN: construction search width `L`.
    pub fn build_beam(mut self, beam: usize) -> Self {
        self.diskann.build_beam = beam;
        self
    }

    /// DiskANN: default query search width `L`.
    pub fn search_beam(mut self, beam: usize) -> Self {
        self.diskann.search_beam = beam;
        self
    }

    /// DiskANN: RobustPrune slack `alpha`.
    pub fn alpha(mut self, alpha: f32) -> Self {
        self.diskann.alpha = alpha;
        self
    }

    /// DiskANN: PQ subvector count (bytes per code; `0` disables PQ).
    pub fn pq_subvectors(mut self, m: usize) -> Self {
        self.diskann.pq_subvectors = m;
        self
    }

    /// DiskANN: train PQ once this many vectors are indexed.
    pub fn pq_train_threshold(mut self, n: usize) -> Self {
        self.diskann.pq_train_threshold = n;
        self
    }

    /// DiskANN: run background delete-consolidation once this many slots are
    /// pending (`0` disables auto-consolidation).
    pub fn consolidate_threshold(mut self, n: usize) -> Self {
        self.diskann.consolidate_threshold = n;
        self
    }
}

impl Default for VectorIndexConfig {
    fn default() -> Self {
        // Dimension 0 means the dimension was not set; callers must set an
        // explicit dimension. This default exists only so the type is `Default`.
        VectorIndexConfig::new(0, Metric::Cosine)
    }
}

/// Derives the backing-map name for a vector index.
pub fn derive_vector_map_name(descriptor: &IndexDescriptor) -> String {
    let collection = descriptor.collection_name();
    let fields = descriptor.index_fields().field_names().join("_");
    format!("{}_{}_vector_idx", collection, fields)
}

/// A persistent vector index for one collection field. Dispatches to the
/// configured backend; `insert`/`remove`/`search`/`metric`/`len` are the shared
/// surface used by the indexer and RAG layers.
#[derive(Clone)]
pub enum VectorIndex {
    Hnsw(HnswBackend),
    DiskAnn(DiskAnnIndex),
}

impl VectorIndex {
    /// Opens (loading from the store) or creates a vector index using the
    /// configured backend.
    pub fn open(
        descriptor: &IndexDescriptor,
        config: &NitriteConfig,
        params: &VectorIndexConfig,
    ) -> NitriteResult<Self> {
        let base = derive_vector_map_name(descriptor);
        match params.backend {
            IndexBackend::Hnsw => Ok(VectorIndex::Hnsw(HnswBackend::open(&base, config, params)?)),
            IndexBackend::DiskAnn => Ok(VectorIndex::DiskAnn(DiskAnnIndex::open(
                config,
                &base,
                params.dim,
                params.metric,
                params.precision,
                &params.diskann,
            )?)),
        }
    }

    /// The index metric.
    pub fn metric(&self) -> Metric {
        match self {
            VectorIndex::Hnsw(b) => b.metric(),
            VectorIndex::DiskAnn(b) => b.metric(),
        }
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        match self {
            VectorIndex::Hnsw(b) => b.len(),
            VectorIndex::DiskAnn(b) => b.len(),
        }
    }

    /// Whether the index has no vectors.
    pub fn is_empty(&self) -> bool {
        match self {
            VectorIndex::Hnsw(b) => b.is_empty(),
            VectorIndex::DiskAnn(b) => b.is_empty(),
        }
    }

    /// Inserts or replaces the vector for `id`.
    pub fn insert(&self, id: u64, vector: Vec<f32>) -> NitriteResult<()> {
        match self {
            VectorIndex::Hnsw(b) => b.insert(id, vector),
            VectorIndex::DiskAnn(b) => b.insert(id, vector),
        }
    }

    /// Removes the vector for `id`.
    pub fn remove(&self, id: u64) -> NitriteResult<()> {
        match self {
            VectorIndex::Hnsw(b) => b.remove(id),
            VectorIndex::DiskAnn(b) => b.remove(id),
        }
    }

    /// Returns the `k` nearest document ids to `query`, ordered ascending by
    /// distance. `ef` overrides the backend's default search width.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> NitriteResult<Vec<(NitriteId, f32)>> {
        match self {
            VectorIndex::Hnsw(b) => b.search(query, k, ef),
            VectorIndex::DiskAnn(b) => b.search(query, k, ef),
        }
    }

    /// Persists any buffered state (DiskANN sidecar; HNSW is already durable).
    pub fn flush(&self) -> NitriteResult<()> {
        match self {
            VectorIndex::Hnsw(_) => Ok(()),
            VectorIndex::DiskAnn(b) => b.flush(),
        }
    }

    /// Removes the index's backing storage.
    pub fn destroy(&self, base: &str, config: &NitriteConfig) -> NitriteResult<()> {
        match self {
            VectorIndex::Hnsw(_) => config.nitrite_store()?.remove_map(base),
            VectorIndex::DiskAnn(b) => b.destroy(),
        }
    }
}

/// The in-memory HNSW backend persisted to a single fjall map.
#[derive(Clone)]
pub struct HnswBackend {
    inner: Arc<HnswInner>,
}

struct HnswInner {
    hnsw: RwLock<Hnsw>,
    map: NitriteMap,
}

impl HnswBackend {
    /// Opens (loading from the store) or creates the HNSW backend.
    pub fn open(
        base: &str,
        config: &NitriteConfig,
        params: &VectorIndexConfig,
    ) -> NitriteResult<Self> {
        let store = config.nitrite_store()?;
        let map = store.open_map(base)?;

        let hnsw = match map.get(&Value::String(META_KEY.to_string()))? {
            Some(Value::Bytes(bytes)) => {
                let header: HnswHeader = decode(&bytes)?;
                let records = load_records(&map)?;
                Hnsw::from_records(header, records)
            }
            _ => {
                if params.dim == 0 {
                    return Err(NitriteError::new(
                        "Vector index dimension must be greater than zero",
                        ErrorKind::IndexingError,
                    ));
                }
                let graph = Hnsw::new(
                    params.dim,
                    params.metric,
                    params.m,
                    params.ef_construction,
                    params.ef_search,
                );
                persist_header(&map, &graph.header())?;
                graph
            }
        };

        Ok(HnswBackend {
            inner: Arc::new(HnswInner {
                hnsw: RwLock::new(hnsw),
                map,
            }),
        })
    }

    /// The index dimension.
    pub fn dim(&self) -> usize {
        self.inner.hnsw.read().header().dim
    }

    /// The index metric.
    pub fn metric(&self) -> Metric {
        self.inner.hnsw.read().metric()
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.inner.hnsw.read().len()
    }

    /// Whether the index has no vectors.
    pub fn is_empty(&self) -> bool {
        self.inner.hnsw.read().is_empty()
    }

    /// Inserts or replaces the vector for `id`, persisting the change.
    pub fn insert(&self, id: u64, vector: Vec<f32>) -> NitriteResult<()> {
        let mut guard = self.inner.hnsw.write();
        guard.insert(id, vector).map_err(|e| {
            NitriteError::new(&format!("Vector insert failed: {e}"), ErrorKind::IndexingError)
        })?;
        self.persist(&mut guard)
    }

    /// Removes the vector for `id`, persisting the change.
    pub fn remove(&self, id: u64) -> NitriteResult<()> {
        let mut guard = self.inner.hnsw.write();
        guard.remove(id);
        self.persist(&mut guard)
    }

    /// Returns the `k` nearest document ids to `query` with their distances
    /// (smaller = closer), ordered ascending.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        ef: Option<usize>,
    ) -> NitriteResult<Vec<(NitriteId, f32)>> {
        let guard = self.inner.hnsw.read();
        guard
            .search(query, k, ef)
            .into_iter()
            .map(|(id, dist)| NitriteId::create_id(id).map(|nid| (nid, dist)))
            .collect()
    }

    /// Writes through the graph's dirty/deleted nodes and the header.
    fn persist(&self, hnsw: &mut Hnsw) -> NitriteResult<()> {
        let (dirty, deleted) = hnsw.take_dirty();
        for rec in dirty {
            let bytes = encode(&rec)?;
            self.inner
                .map
                .put(Value::U64(rec.id), Value::Bytes(bytes))?;
        }
        for id in deleted {
            self.inner.map.remove(&Value::U64(id))?;
        }
        persist_header(&self.inner.map, &hnsw.header())
    }
}

fn load_records(map: &NitriteMap) -> NitriteResult<Vec<NodeRecord>> {
    // Every non-header entry is a node record; the record carries its own id, so
    // we ignore the key entirely (the store's codec may normalize the integer
    // key type, e.g. U64 -> I64, on round-trip).
    let meta_key = Value::String(META_KEY.to_string());
    let mut records = Vec::new();
    for entry in map.entries()? {
        let (key, value) = entry?;
        if key == meta_key {
            continue;
        }
        if let Value::Bytes(bytes) = value {
            records.push(decode::<NodeRecord>(&bytes)?);
        }
    }
    Ok(records)
}

fn persist_header(map: &NitriteMap, header: &HnswHeader) -> NitriteResult<()> {
    let bytes = encode(header)?;
    map.put(Value::String(META_KEY.to_string()), Value::Bytes(bytes))
}

fn encode<T: serde::Serialize>(value: &T) -> NitriteResult<Vec<u8>> {
    to_bytes(value)
        .map_err(|e| NitriteError::new(&format!("encode failed: {e}"), ErrorKind::IndexingError))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> NitriteResult<T> {
    from_bytes(bytes)
        .map_err(|e| NitriteError::new(&format!("decode failed: {e}"), ErrorKind::IndexingError))
}
