//! Durable HNSW index: bridges the in-memory [`Hnsw`] graph with a Nitrite
//! `NitriteMap` so the graph survives restarts and participates in the store's
//! atomicity.
//!
//! Layout (one map per index, named by [`derive_vector_map_name`]):
//! - `Value::String("__hnsw_meta__")` → bincode of [`StoredHeader`]
//!   (format version + stored-vector precision + graph header).
//! - `Value::String("v{doc_id}")` → the node's vector, encoded at the
//!   configured [`Precision`].
//! - `Value::String("a{doc_id}")` → bincode of the node's per-level neighbor
//!   lists.
//!
//! Vectors and adjacency are separate records because adjacency churns on
//! every insert while vectors are written once; splitting them cuts the
//! per-insert write amplification by roughly the vector/neighbor-list size
//! ratio (an order of magnitude for typical embedding dimensions).
//!
//! Each persist batches every touched record plus the header into a single
//! atomic `put_all`, and the batch is written *after* the graph lock is
//! released so searches are never blocked on storage I/O. On open the whole
//! graph is loaded and sanitized ([`Hnsw::from_parts`]); if the header is
//! missing or unreadable while data exists, the map is wiped and the caller is
//! told to rebuild the index from the collection.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};

use nitrite::collection::NitriteId;
use nitrite::common::Value;
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::index::IndexDescriptor;
use nitrite::nitrite_config::NitriteConfig;
use nitrite::store::NitriteMap;

use crate::diskann::{DiskAnnConfig, DiskAnnIndex};
use crate::distance::Metric;
use crate::filter::value_to_vector;
use crate::hnsw::{DirtyChanges, Hnsw};
use crate::node::{from_bytes, to_bytes, HnswHeader};
use crate::precision::Precision;

/// Reserved key holding the serialized [`StoredHeader`].
const META_KEY: &str = "__hnsw_meta__";

/// Bumped whenever the persisted HNSW layout changes; a mismatch triggers a
/// rebuild from the collection instead of misreading old bytes.
const HNSW_FORMAT_VERSION: u32 = 2;

/// The persisted per-index header: storage format concerns plus the graph
/// parameters. Kept separate from [`HnswHeader`] so the in-memory graph stays
/// oblivious to storage details like precision.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct StoredHeader {
    format_version: u32,
    precision: Precision,
    graph: HnswHeader,
}

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
    ///
    /// If the persisted index is detected as stale or corrupt (torn HNSW
    /// header, DiskANN files from a crashed session, checksum mismatch, …) the
    /// damaged storage is wiped and the index is **rebuilt automatically** from
    /// the collection's documents — the index is derived data, so a rebuild is
    /// always safe.
    pub fn open(
        descriptor: &IndexDescriptor,
        config: &NitriteConfig,
        params: &VectorIndexConfig,
    ) -> NitriteResult<Self> {
        let base = derive_vector_map_name(descriptor);
        let (index, needs_rebuild) = match params.backend {
            IndexBackend::Hnsw => {
                let (backend, rebuild) = HnswBackend::open(&base, config, params)?;
                (VectorIndex::Hnsw(backend), rebuild)
            }
            IndexBackend::DiskAnn => {
                let (backend, rebuild) = DiskAnnIndex::open(
                    config,
                    &base,
                    params.dim,
                    params.metric,
                    params.precision,
                    &params.diskann,
                )?;
                (VectorIndex::DiskAnn(backend), rebuild)
            }
        };
        if needs_rebuild {
            let n = rebuild_from_collection(&index, descriptor, config)?;
            log::info!("vector index '{base}' was stale or damaged; rebuilt from collection ({n} vectors)");
            index.flush()?;
        }
        Ok(index)
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

/// Rebuilds a vector index from its collection's documents (the collection map
/// is named after the collection). Used to heal a stale or corrupt index.
fn rebuild_from_collection(
    index: &VectorIndex,
    descriptor: &IndexDescriptor,
    config: &NitriteConfig,
) -> NitriteResult<usize> {
    let field = descriptor
        .index_fields()
        .field_names()
        .first()
        .cloned()
        .ok_or_else(|| {
            NitriteError::new("Vector index has no field", ErrorKind::IndexingError)
        })?;
    let store = config.nitrite_store()?;
    let collection = store.open_map(&descriptor.collection_name())?;

    let mut count = 0usize;
    for entry in collection.entries()? {
        let (key, value) = entry?;
        let Value::Document(mut doc) = value else { continue };
        let id = match key {
            Value::NitriteId(nid) => nid.id_value(),
            _ => match doc.id() {
                Ok(nid) => nid.id_value(),
                Err(_) => continue,
            },
        };
        let Ok(field_value) = doc.get(&field) else { continue };
        let Some(vector) = value_to_vector(&field_value) else { continue };
        index.insert(id, vector)?;
        count += 1;
    }
    Ok(count)
}

/// The in-memory HNSW backend persisted to a single fjall map.
#[derive(Clone)]
pub struct HnswBackend {
    inner: Arc<HnswInner>,
}

struct HnswInner {
    hnsw: RwLock<Hnsw>,
    /// Serializes persist batches so two write-throughs cannot interleave
    /// their `put_all` calls out of order. Held *after* the graph lock is
    /// released, so searches never wait on storage I/O.
    persist_gate: Mutex<()>,
    map: NitriteMap,
    precision: Precision,
}

impl HnswBackend {
    /// Opens (loading from the store) or creates the HNSW backend. The second
    /// return value is `true` when existing index data was unreadable and the
    /// (now wiped) index must be rebuilt from the collection.
    pub fn open(
        base: &str,
        config: &NitriteConfig,
        params: &VectorIndexConfig,
    ) -> NitriteResult<(Self, bool)> {
        let store = config.nitrite_store()?;
        let map = store.open_map(base)?;

        let stored = match map.get(&Value::String(META_KEY.to_string()))? {
            Some(Value::Bytes(bytes)) => match decode::<StoredHeader>(&bytes) {
                Ok(h) if h.format_version == HNSW_FORMAT_VERSION => Some(Ok(h)),
                _ => Some(Err(())), // unreadable or old format
            },
            _ if map.is_empty()? => None, // fresh index
            _ => Some(Err(())),           // data without a readable header
        };

        let (graph, precision, needs_rebuild) = match stored {
            Some(Ok(header)) => {
                let precision = header.precision;
                let (vectors, adjacency) = load_parts(&map, &header)?;
                (Hnsw::from_parts(header.graph, vectors, adjacency), precision, false)
            }
            Some(Err(())) => {
                log::warn!("HNSW index '{base}' has an unreadable header; wiping for rebuild");
                map.clear()?;
                let graph = new_graph(params)?;
                (graph, params.precision, true)
            }
            None => (new_graph(params)?, params.precision, false),
        };

        let backend = HnswBackend {
            inner: Arc::new(HnswInner {
                hnsw: RwLock::new(graph),
                persist_gate: Mutex::new(()),
                map,
                precision,
            }),
        };
        // Make sure a fresh index has its header on disk before first use.
        let header = backend.inner.hnsw.read().header();
        backend.persist(DirtyChanges::default(), header)?;
        Ok((backend, needs_rebuild))
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
        let (changes, header) = {
            let mut guard = self.inner.hnsw.write();
            guard.insert(id, vector).map_err(|e| {
                NitriteError::new(&format!("Vector insert failed: {e}"), ErrorKind::IndexingError)
            })?;
            (guard.take_dirty(), guard.header())
        };
        self.persist(changes, header)
    }

    /// Removes the vector for `id`, persisting the change.
    pub fn remove(&self, id: u64) -> NitriteResult<()> {
        let (changes, header) = {
            let mut guard = self.inner.hnsw.write();
            guard.remove(id);
            (guard.take_dirty(), guard.header())
        };
        self.persist(changes, header)
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

    /// Writes through the drained changes: deletions first, then one atomic
    /// `put_all` batch containing every touched record plus the header, so a
    /// crash can never persist half an insert. (A crash between the deletions
    /// and the batch leaves dangling links, which the sanitizing loader
    /// prunes.) Runs outside the graph lock — searches proceed concurrently.
    fn persist(&self, changes: DirtyChanges, header: HnswHeader) -> NitriteResult<()> {
        let _gate = self.inner.persist_gate.lock();

        for id in &changes.deleted {
            self.inner.map.remove(&Value::String(format!("v{id}")))?;
            self.inner.map.remove(&Value::String(format!("a{id}")))?;
        }

        let mut batch: Vec<(Value, Value)> =
            Vec::with_capacity(changes.vectors.len() + changes.adjacency.len() + 1);
        for (id, vector) in &changes.vectors {
            batch.push((
                Value::String(format!("v{id}")),
                Value::Bytes(self.inner.precision.encode(vector)),
            ));
        }
        for (id, neighbors) in &changes.adjacency {
            batch.push((
                Value::String(format!("a{id}")),
                Value::Bytes(encode(neighbors)?),
            ));
        }
        let stored = StoredHeader {
            format_version: HNSW_FORMAT_VERSION,
            precision: self.inner.precision,
            graph: header,
        };
        batch.push((Value::String(META_KEY.to_string()), Value::Bytes(encode(&stored)?)));
        self.inner.map.put_all(batch)
    }
}

fn new_graph(params: &VectorIndexConfig) -> NitriteResult<Hnsw> {
    if params.dim == 0 {
        return Err(NitriteError::new(
            "Vector index dimension must be greater than zero",
            ErrorKind::IndexingError,
        ));
    }
    Ok(Hnsw::new(
        params.dim,
        params.metric,
        params.m,
        params.ef_construction,
        params.ef_search,
    ))
}

/// Loads all vector and adjacency records. Unparseable entries are skipped
/// (the sanitizing loader also drops any node left inconsistent by that).
#[allow(clippy::type_complexity)]
fn load_parts(
    map: &NitriteMap,
    header: &StoredHeader,
) -> NitriteResult<(Vec<(u64, Vec<f32>)>, FxHashMap<u64, Vec<Vec<u64>>>)> {
    let mut vectors = Vec::new();
    let mut adjacency = FxHashMap::default();
    let expected_len = header.precision.encoded_len(header.graph.dim);

    for entry in map.entries()? {
        let (key, value) = entry?;
        let Value::String(key) = key else { continue };
        let Value::Bytes(bytes) = value else { continue };
        if let Some(id_str) = key.strip_prefix('v') {
            if let Ok(id) = id_str.parse::<u64>() {
                if bytes.len() == expected_len {
                    vectors.push((id, header.precision.decode(&bytes, header.graph.dim)));
                }
            }
        } else if let Some(id_str) = key.strip_prefix('a') {
            if let Ok(id) = id_str.parse::<u64>() {
                if let Ok(neighbors) = decode::<Vec<Vec<u64>>>(&bytes) {
                    adjacency.insert(id, neighbors);
                }
            }
        }
    }
    Ok((vectors, adjacency))
}

fn encode<T: serde::Serialize>(value: &T) -> NitriteResult<Vec<u8>> {
    to_bytes(value)
        .map_err(|e| NitriteError::new(&format!("encode failed: {e}"), ErrorKind::IndexingError))
}

fn decode<T: for<'de> serde::Deserialize<'de>>(bytes: &[u8]) -> NitriteResult<T> {
    from_bytes(bytes)
        .map_err(|e| NitriteError::new(&format!("decode failed: {e}"), ErrorKind::IndexingError))
}
