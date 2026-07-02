//! Disk-resident DiskANN backend: a single-layer Vamana graph plus full vectors
//! stored on disk in a memory-mapped flat file ([`flat_store`]), with
//! product-quantized codes resident in RAM for fast approximate traversal and
//! exact re-ranking from the on-disk vectors.
//!
//! Resident RAM is bounded by the OS page cache + PQ codes + the id↔slot map, so
//! the index can be much larger than memory — the target being mobile devices.
//!
//! # Concurrency model
//!
//! Queries are lock-free per node (one store read lock per query, see
//! [`flat_store::FlatStore::read_view`]). All *structural* writers — inserts,
//! removes, delete-consolidation, and the PQ encode step — serialize on a
//! single `write_gate` mutex, because each of them performs multi-step
//! read-modify-write sequences over the adjacency that would otherwise lose
//! updates when interleaved. Background work (consolidation, PQ training)
//! acquires the gate per chunk so writers interleave instead of stalling.

pub mod flat_store;
pub mod pq;
pub mod vamana;

use rustc_hash::FxHashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use nitrite::collection::NitriteId;
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::nitrite_config::NitriteConfig;

use crate::distance::{squared_l2, Metric};
use crate::precision::Precision;

use flat_store::FlatStore;
use pq::ProductQuantizer;
use vamana::{greedy_search, robust_prune, GraphStore};

/// Cap on how many vectors are sampled to train the PQ codebook.
const PQ_TRAIN_SAMPLE: usize = 25_000;

/// Checkpoint the sidecar every this many mutations, so a crash loses at most
/// this window (detected on reopen → automatic rebuild) instead of the whole
/// session. Amortized cost is a few KB per mutation at mobile-scale indexes.
const AUTOSAVE_OPS: usize = 8_192;

/// Tunable DiskANN parameters. Every field drives behavior (see call sites).
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct DiskAnnConfig {
    /// `R`: maximum out-degree of a graph node.
    pub degree: usize,
    /// `L` used while searching during construction.
    pub build_beam: usize,
    /// Default `L` used while answering queries (overridable per query).
    pub search_beam: usize,
    /// RobustPrune diversity slack (`>= 1.0`).
    pub alpha: f32,
    /// PQ subvector count (= bytes per code). `0` disables PQ (exact traversal).
    pub pq_subvectors: usize,
    /// Train PQ once at least this many vectors are indexed.
    pub pq_train_threshold: usize,
    /// Advisory resident-memory budget in bytes. The flat store is
    /// memory-mapped, so the OS page cache actually bounds resident memory
    /// (hot pages stay, cold pages are reclaimed under pressure — the index
    /// cannot OOM from its vector data). Reserved for future `madvise` hinting.
    pub cache_bytes: usize,
    /// Run background consolidation once this many deleted slots accumulate
    /// (repairs dangling edges + reclaims slots). `0` disables auto-consolidation
    /// (call [`DiskAnnIndex::consolidate`] manually).
    pub consolidate_threshold: usize,
}

impl Default for DiskAnnConfig {
    fn default() -> Self {
        DiskAnnConfig {
            degree: 64,
            build_beam: 100,
            search_beam: 100,
            alpha: 1.2,
            pq_subvectors: 16,
            pq_train_threshold: 10_000,
            cache_bytes: 64 * 1024 * 1024,
            consolidate_threshold: 1000,
        }
    }
}

/// Persisted header: structural params + medoid + trained codebook.
#[derive(Serialize, Deserialize, Clone)]
struct DiskHeader {
    dim: usize,
    metric: Metric,
    precision: Precision,
    degree: usize,
    alpha: f32,
    build_beam: usize,
    search_beam: usize,
    pq_subvectors: usize,
    pq_train_threshold: usize,
    medoid: Option<u64>,
    pq: Option<ProductQuantizer>,
}

struct MutableState {
    medoid: Option<u64>,
    pq: Option<ProductQuantizer>,
}

/// A disk-resident DiskANN index for one collection field.
#[derive(Clone)]
pub struct DiskAnnIndex {
    inner: Arc<Inner>,
}

struct Inner {
    store: FlatStore,
    metric: Metric,
    dim: usize,
    precision: Precision,
    degree: usize,
    alpha: f32,
    build_beam: usize,
    search_beam: usize,
    pq_subvectors: usize,
    pq_train_threshold: usize,
    consolidate_threshold: usize,
    /// Serializes structural writers (insert / remove / consolidation chunks /
    /// PQ encode). Their multi-step adjacency updates would lose writes if
    /// interleaved; queries never take this.
    write_gate: Mutex<()>,
    /// True while a background consolidation is in progress (single-flight).
    consolidating: AtomicBool,
    /// True while background PQ training is in progress (single-flight).
    training: AtomicBool,
    /// Set during flush so background work stops promptly; cleared afterwards.
    shutdown: AtomicBool,
    /// Mutations since the last sidecar checkpoint (see [`AUTOSAVE_OPS`]).
    ops_since_save: AtomicUsize,
    state: RwLock<MutableState>,
}

impl DiskAnnIndex {
    /// Opens (loading from the store) or creates a DiskANN index. On reopen the
    /// persisted header's structural params + precision + metric win; only
    /// `cache_bytes` comes from the passed config (runtime).
    ///
    /// The second return value is `true` when existing index files were stale
    /// or corrupt (crash before flush, checksum mismatch, format change): the
    /// files have been wiped and the caller must rebuild from the collection.
    pub fn open(
        config: &NitriteConfig,
        base: &str,
        dim: usize,
        metric: Metric,
        precision: Precision,
        params: &DiskAnnConfig,
    ) -> NitriteResult<(Self, bool)> {
        // Read the header (if any) before choosing dim/precision, so a reopened
        // index keeps the settings it was built with. An unreadable header is
        // treated as absent; the store-level staleness check below wipes it.
        let header = FlatStore::peek_header(config, base)?
            .and_then(|b| decode::<DiskHeader>(&b).ok());

        let (dim, metric, precision, degree, alpha, build_beam, search_beam, pq_subvectors, pq_train_threshold, medoid, pq) =
            match header {
                Some(h) => (
                    h.dim, h.metric, h.precision, h.degree, h.alpha, h.build_beam,
                    h.search_beam, h.pq_subvectors, h.pq_train_threshold, h.medoid, h.pq,
                ),
                None => {
                    if dim == 0 {
                        return Err(NitriteError::new(
                            "Vector index dimension must be greater than zero",
                            ErrorKind::IndexingError,
                        ));
                    }
                    (
                        dim, metric, precision, params.degree, params.alpha,
                        params.build_beam, params.search_beam, params.pq_subvectors,
                        params.pq_train_threshold, None, None,
                    )
                }
            };

        let (store, needs_rebuild) =
            FlatStore::open(config, base, dim, precision, degree, params.cache_bytes)?;
        // A wiped store must not keep a medoid/codebook from the dead session.
        let (medoid, pq) = if needs_rebuild { (None, None) } else { (medoid, pq) };

        let index = DiskAnnIndex {
            inner: Arc::new(Inner {
                store,
                metric,
                dim,
                precision,
                degree,
                alpha,
                build_beam,
                search_beam,
                pq_subvectors,
                pq_train_threshold,
                consolidate_threshold: params.consolidate_threshold,
                write_gate: Mutex::new(()),
                consolidating: AtomicBool::new(false),
                training: AtomicBool::new(false),
                shutdown: AtomicBool::new(false),
                ops_since_save: AtomicUsize::new(0),
                state: RwLock::new(MutableState { medoid, pq }),
            }),
        };
        index.persist_header()?;
        Ok((index, needs_rebuild))
    }

    /// The index metric.
    pub fn metric(&self) -> Metric {
        self.inner.metric
    }

    /// Checkpoints the index: stops background work, runs a final synchronous
    /// consolidation so the persisted sidecar is clean, and flushes the data
    /// file + sidecar. Safe to call at any time (background consolidation and
    /// PQ training resume on later mutations); called automatically on close
    /// and periodically every [`AUTOSAVE_OPS`] mutations.
    pub fn flush(&self) -> NitriteResult<()> {
        self.inner.shutdown.store(true, Ordering::SeqCst);
        // Wait out in-flight background work so we don't flush a half-updated
        // graph (both abort promptly on the shutdown flag).
        while self.inner.consolidating.load(Ordering::SeqCst)
            || self.inner.training.load(Ordering::SeqCst)
        {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let result = self
            .consolidate_impl(false)
            .and_then(|_| self.inner.store.flush());
        // Not a permanent close: re-enable background work either way.
        self.inner.shutdown.store(false, Ordering::SeqCst);
        self.inner.ops_since_save.store(0, Ordering::Relaxed);
        result
    }

    /// Deletes the index's on-disk files.
    pub fn destroy(&self) -> NitriteResult<()> {
        self.inner.store.destroy()
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.inner.store.len()
    }

    /// Whether the index has no vectors.
    pub fn is_empty(&self) -> bool {
        self.inner.store.is_empty()
    }

    /// Resident LRU cache size in bytes (for tests / introspection).
    pub fn cache_bytes(&self) -> usize {
        self.inner.store.cache_bytes()
    }

    /// Whether PQ has been trained.
    pub fn pq_trained(&self) -> bool {
        self.inner.state.read().pq.is_some()
    }

    /// Whether background PQ training is currently running (tests/introspection).
    pub fn pq_training(&self) -> bool {
        self.inner.training.load(Ordering::SeqCst)
    }

    /// Number of deleted slots awaiting consolidation (introspection/tests).
    pub fn pending_len(&self) -> usize {
        self.inner.store.pending_len()
    }

    /// The largest out-degree across all nodes (introspection; O(n)).
    pub fn max_out_degree(&self) -> usize {
        self.inner
            .store
            .ids()
            .into_iter()
            .map(|id| GraphStore::neighbors(&self.inner.store, id).len())
            .max()
            .unwrap_or(0)
    }

    fn persist_header(&self) -> NitriteResult<()> {
        let st = self.inner.state.read();
        let header = DiskHeader {
            dim: self.inner.dim,
            metric: self.inner.metric,
            precision: self.inner.precision,
            degree: self.inner.degree,
            alpha: self.inner.alpha,
            build_beam: self.inner.build_beam,
            search_beam: self.inner.search_beam,
            pq_subvectors: self.inner.pq_subvectors,
            pq_train_threshold: self.inner.pq_train_threshold,
            medoid: st.medoid,
            pq: st.pq.clone(),
        };
        self.inner.store.store_header(encode(&header)?)
    }

    /// Exact distance from a prepared query vector to stored node `id`.
    /// Allocation-free: the stored vector is decoded into a reused buffer.
    fn dist_to(&self, query: &[f32], id: u64) -> f32 {
        let metric = self.inner.metric;
        self.inner.store.with_vector(id, |v| match v {
            Some(v) => metric.distance(query, v),
            None => f32::INFINITY,
        })
    }

    /// Exact distance between two stored nodes.
    fn pair_dist(&self, a: u64, b: u64) -> f32 {
        match (self.inner.store.vector(a), self.inner.store.vector(b)) {
            (Ok(Some(va)), Ok(Some(vb))) => self.inner.metric.distance(&va, &vb),
            _ => f32::INFINITY,
        }
    }

    /// Inserts or replaces the vector for `id`.
    pub fn insert(&self, id: u64, raw: Vec<f32>) -> NitriteResult<()> {
        if raw.len() != self.inner.dim {
            return Err(NitriteError::new(
                &format!("vector has dimension {}, expected {}", raw.len(), self.inner.dim),
                ErrorKind::IndexingError,
            ));
        }

        let gate = self.inner.write_gate.lock();

        if self.inner.store.contains(id) {
            self.remove_locked(id)?;
        }

        let prepared = self.inner.metric.prepare(raw);
        self.inner.store.put_vector(id, &prepared)?;

        // First node becomes the medoid; nothing to connect. Encode the PQ code
        // and release the state lock BEFORE persist_header (which re-locks state;
        // parking_lot locks are not reentrant).
        let mut first = false;
        {
            let mut st = self.inner.state.write();
            if st.medoid.is_none() {
                st.medoid = Some(id);
                first = true;
            }
            let pq_code = st.pq.as_ref().map(|pq| pq.encode(&prepared));
            drop(st);
            if let Some(code) = pq_code {
                self.inner.store.set_pq_code(id, code)?;
            }
        }
        if first {
            self.persist_header()?;
            drop(gate);
            self.after_mutation();
            return Ok(());
        }
        let medoid = self.inner.state.read().medoid.expect("medoid set above");

        // Vamana insert with EXACT distances (build reads full vectors).
        let store = &self.inner.store;
        let (_, mut visited) = greedy_search(store, medoid, |o| self.dist_to(&prepared, o), self.inner.build_beam);
        // Bound the candidate pool: RobustPrune is O(candidates * degree), so
        // feeding it the whole visited set makes insertion scale with N. The
        // closest `build_beam` candidates are what Vamana actually needs.
        visited.truncate(self.inner.build_beam);
        let metric = self.inner.metric;
        robust_prune(store, id, visited, self.inner.alpha, self.inner.degree, metric, |gid| {
            store.vector(gid).ok().flatten()
        });

        for n in store.neighbors(id) {
            let mut nn = store.neighbors(n);
            if !nn.contains(&id) {
                nn.push(id);
            }
            if nn.len() > self.inner.degree {
                let cand: Vec<(f32, u64)> = nn.iter().map(|&x| (self.pair_dist(n, x), x)).collect();
                robust_prune(store, n, cand, self.inner.alpha, self.inner.degree, metric, |gid| {
                    store.vector(gid).ok().flatten()
                });
            } else {
                store.set_neighbors(n, nn);
            }
        }

        drop(gate);
        self.maybe_spawn_pq_training();
        self.after_mutation();
        Ok(())
    }

    /// Bumps the mutation counter and checkpoints the sidecar every
    /// [`AUTOSAVE_OPS`] mutations, so a crash costs a bounded rebuild window.
    fn after_mutation(&self) {
        let n = self.inner.ops_since_save.fetch_add(1, Ordering::Relaxed) + 1;
        if n >= AUTOSAVE_OPS {
            self.inner.ops_since_save.store(0, Ordering::Relaxed);
            if let Err(e) = self.inner.store.flush() {
                log::warn!("DiskANN autosave failed (state stays recoverable by rebuild): {e}");
            }
        }
    }

    /// Spawns single-flight background PQ training once the index crosses the
    /// threshold. Training (k-means over up to [`PQ_TRAIN_SAMPLE`] vectors) can
    /// take seconds, so it must never run on a caller's insert.
    fn maybe_spawn_pq_training(&self) {
        if self.inner.pq_subvectors == 0
            || self.inner.shutdown.load(Ordering::SeqCst)
            || self.inner.state.read().pq.is_some()
            || self.inner.store.len() < self.inner.pq_train_threshold
        {
            return;
        }
        if self
            .inner
            .training
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let index = self.clone();
        std::thread::spawn(move || {
            if let Err(e) = index.train_pq_impl() {
                log::warn!("DiskANN background PQ training failed: {e}");
            }
            index.inner.training.store(false, Ordering::SeqCst);
        });
    }

    /// Trains the PQ codebook (no locks held), then — under the write gate —
    /// encodes every node and publishes the quantizer. Nodes inserted while
    /// training ran are picked up because the id list is re-read under the
    /// gate; queries fall back to exact distances for any node still missing a
    /// code, so correctness never depends on complete coverage.
    fn train_pq_impl(&self) -> NitriteResult<()> {
        // Sample vectors for training (reads only; writers keep running).
        let ids = self.inner.store.ids();
        let mut sample = Vec::new();
        for id in ids.iter().take(PQ_TRAIN_SAMPLE) {
            if let Some(v) = self.inner.store.vector(*id)? {
                sample.push(v);
            }
        }
        if sample.is_empty() {
            return Ok(());
        }
        let pq = ProductQuantizer::train(&sample, self.inner.dim, self.inner.pq_subvectors);

        // Encode every node once, under the gate so no insert interleaves
        // between the id snapshot and its encoding.
        let _gate = self.inner.write_gate.lock();
        if self.inner.shutdown.load(Ordering::SeqCst) {
            return Ok(()); // a later mutation re-triggers training
        }
        for id in self.inner.store.ids() {
            if let Some(v) = self.inner.store.vector(id)? {
                self.inner.store.set_pq_code(id, pq.encode(&v))?;
            }
        }
        self.inner.state.write().pq = Some(pq);
        self.persist_header()?;
        Ok(())
    }

    /// Removes the vector for `id`. The freed slot is held `pending` (not reused)
    /// and its in-edges resolve to a dead sentinel that queries skip, so deletes
    /// are correct immediately; a background pass later reclaims the slot and
    /// repairs adjacency.
    pub fn remove(&self, id: u64) -> NitriteResult<()> {
        {
            let _gate = self.inner.write_gate.lock();
            if !self.inner.store.contains(id) {
                return Ok(());
            }
            self.remove_locked(id)?;
        }
        self.maybe_spawn_consolidation();
        self.after_mutation();
        Ok(())
    }

    /// Delete body; caller must hold the write gate.
    fn remove_locked(&self, id: u64) -> NitriteResult<()> {
        self.inner.store.remove_node(id)?;
        let mut st = self.inner.state.write();
        if st.medoid == Some(id) {
            st.medoid = self.inner.store.ids().into_iter().next();
            drop(st);
            self.persist_header()?;
        }
        Ok(())
    }

    /// Spawns a single-flight background consolidation once enough slots are
    /// pending. Cheap no-op if disabled, already running, or below threshold.
    fn maybe_spawn_consolidation(&self) {
        let threshold = self.inner.consolidate_threshold;
        if threshold == 0
            || self.inner.shutdown.load(Ordering::SeqCst)
            || self.inner.store.pending_len() < threshold
        {
            return;
        }
        // Claim the single-flight slot; bail if another consolidation holds it.
        if self
            .inner
            .consolidating
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let index = self.clone();
        std::thread::spawn(move || {
            if let Err(e) = index.consolidate_impl(true) {
                log::warn!("DiskANN background consolidation failed: {e}");
            }
            index.inner.consolidating.store(false, Ordering::SeqCst);
        });
    }

    /// Repairs adjacency after deletes and reclaims freed slots (synchronous).
    ///
    /// For every live node it drops neighbor references to deleted nodes and, to
    /// preserve connectivity (FreshDiskANN-style), replaces each dropped edge
    /// with the deleted node's still-live neighbors, then re-prunes to `degree`.
    /// Finally the pending slots become reusable.
    pub fn consolidate(&self) -> NitriteResult<()> {
        self.consolidate_impl(false)
    }

    /// The sweep acquires the write gate **per chunk**, so inserts and removes
    /// interleave with a running consolidation instead of stalling behind it.
    /// Only the pending slots snapshotted at the start are reclaimed: a slot
    /// deleted mid-sweep has unrepaired in-edges and must stay quarantined
    /// until the next pass.
    fn consolidate_impl(&self, check_shutdown: bool) -> NitriteResult<()> {
        let store = &self.inner.store;
        let pending = store.pending_slots();
        if pending.is_empty() {
            return Ok(());
        }
        const CHUNK: usize = 256;
        let metric = self.inner.metric;
        let degree = self.inner.degree;
        let alpha = self.inner.alpha;

        let live = store.live_slots();
        for chunk in live.chunks(CHUNK) {
            let _gate = self.inner.write_gate.lock();
            if check_shutdown && self.inner.shutdown.load(Ordering::SeqCst) {
                return Ok(()); // pending stays; a later pass finishes the job
            }
            for &slot in chunk {
                self.repair_slot(slot, metric, degree, alpha);
            }
        }
        let _gate = self.inner.write_gate.lock();
        store.reclaim(&pending);
        Ok(())
    }

    /// Rebuilds one node's neighbor list, replacing dead references with the
    /// dead nodes' live neighbors and re-pruning to `degree`.
    fn repair_slot(&self, slot: u32, metric: Metric, degree: usize, alpha: f32) {
        let store = &self.inner.store;
        let raw = store.raw_neighbor_slots(slot);
        if raw.iter().all(|&s| store.is_slot_alive(s)) {
            return; // nothing dead referenced
        }

        let mut seen: FxHashSet<u32> = FxHashSet::default();
        let mut cands: Vec<u32> = Vec::new();
        for s in raw {
            if store.is_slot_alive(s) {
                if seen.insert(s) {
                    cands.push(s);
                }
            } else {
                // Reconnect through the deleted node's still-live out-neighbors.
                for s2 in store.raw_neighbor_slots(s) {
                    if s2 != slot && store.is_slot_alive(s2) && seen.insert(s2) {
                        cands.push(s2);
                    }
                }
            }
        }

        if cands.len() <= degree {
            store.set_neighbor_slots(slot, &cands);
            return;
        }

        // RobustPrune over the candidate slots (fetch each vector once).
        let base = store.vector_at_slot(slot);
        let mut scored: Vec<(f32, u32, Vec<f32>)> = cands
            .iter()
            .map(|&s| {
                let v = store.vector_at_slot(s);
                (metric.distance(&base, &v), s, v)
            })
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));

        let mut selected: Vec<(u32, Vec<f32>)> = Vec::with_capacity(degree);
        for (d, s, v) in scored {
            if selected.len() >= degree {
                break;
            }
            let dominated = selected
                .iter()
                .any(|(_, vq)| alpha * metric.distance(&v, vq) < d);
            if !dominated {
                selected.push((s, v));
            }
        }
        let final_slots: Vec<u32> = selected.into_iter().map(|(s, _)| s).collect();
        store.set_neighbor_slots(slot, &final_slots);
    }

    /// Returns the `k` nearest document ids to `query` with exact distances
    /// (ascending), using PQ-guided traversal + exact re-ranking.
    pub fn search(
        &self,
        query: &[f32],
        k: usize,
        beam: Option<usize>,
    ) -> NitriteResult<Vec<(NitriteId, f32)>> {
        if k == 0 || self.inner.store.is_empty() {
            return Ok(Vec::new());
        }
        let prepared = self.inner.metric.prepare(query.to_vec());
        let medoid = match self.inner.state.read().medoid {
            Some(m) => m,
            None => return Ok(Vec::new()),
        };
        let beam = beam.unwrap_or(self.inner.search_beam).max(k);
        let metric = self.inner.metric;

        // Hold ONE store read lock for the whole query (the `view`) so node
        // access is lock-free and concurrent queries scale instead of contending
        // on the lock's cache line per node.
        let view = self.inner.store.read_view();
        let st = self.inner.state.read();

        // Traverse with PQ approximate distances when available, else exact.
        let candidates = if let Some(pq) = &st.pq {
            let tables = pq.query_tables(&prepared);
            // A node inserted after training but not yet encoded has no code;
            // fall back to an exact distance in the SAME space ADC lives in
            // (squared L2 over prepared vectors) so the ordering stays
            // consistent — such a node must never become invisible.
            let buf = std::cell::RefCell::new(Vec::with_capacity(self.inner.dim));
            greedy_search(
                &view,
                medoid,
                |id| match view.pq_code(id) {
                    Some(c) => pq.adc_distance(&tables, c),
                    None => {
                        let mut b = buf.borrow_mut();
                        if view.vector_into(id, &mut b) {
                            squared_l2(&prepared, &b)
                        } else {
                            f32::INFINITY
                        }
                    }
                },
                beam,
            )
            .0
        } else {
            let buf = std::cell::RefCell::new(Vec::with_capacity(self.inner.dim));
            greedy_search(
                &view,
                medoid,
                |id| {
                    let mut b = buf.borrow_mut();
                    if view.vector_into(id, &mut b) {
                        metric.distance(&prepared, &b)
                    } else {
                        f32::INFINITY
                    }
                },
                beam,
            )
            .0
        };
        drop(st);

        // Exact re-rank of the candidate finalists (still lock-free via `view`).
        let mut buf = Vec::with_capacity(self.inner.dim);
        let mut ranked: Vec<(f32, u64)> = candidates
            .into_iter()
            .map(|(_, id)| {
                let d = if view.vector_into(id, &mut buf) {
                    metric.distance(&prepared, &buf)
                } else {
                    f32::INFINITY
                };
                (d, id)
            })
            .filter(|(d, _)| d.is_finite())
            .collect();
        drop(view);
        ranked.sort_by(|a, b| a.0.total_cmp(&b.0));
        ranked.dedup_by_key(|(_, id)| *id);
        ranked.truncate(k);

        ranked
            .into_iter()
            .map(|(d, id)| NitriteId::create_id(id).map(|nid| (nid, d)))
            .collect()
    }
}

fn encode<T: Serialize>(value: &T) -> NitriteResult<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| NitriteError::new(&format!("encode failed: {e}"), ErrorKind::IndexingError))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> NitriteResult<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| NitriteError::new(&format!("decode failed: {e}"), ErrorKind::IndexingError))
}
