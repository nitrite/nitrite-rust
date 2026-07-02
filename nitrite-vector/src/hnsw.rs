//! In-memory HNSW (Hierarchical Navigable Small World) graph.
//!
//! Implements the Malkov & Yashunin algorithm with the pruned, diversity
//! preserving neighbor-selection heuristic (Algorithm 4), a multi-layer
//! navigable graph, and exact deletion with neighbor unlinking plus
//! FreshDiskANN-style reconnection of the orphaned neighborhood.
//!
//! The graph owns [`NodeRecord`]s directly. Mutations mark touched nodes
//! dirty — vectors and adjacency separately, so the persistence layer only
//! rewrites what actually changed — and the persistence layer drains those
//! via [`Hnsw::take_dirty`] to write through to a `NitriteMap`.
//!
//! Traversal never assumes referential integrity: a neighbor id whose node is
//! missing (possible after a torn persist) is skipped, and [`Hnsw::from_parts`]
//! prunes such links on load, so a damaged graph degrades instead of panicking.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rustc_hash::{FxHashMap, FxHashSet};

use crate::distance::Metric;
use crate::node::{HnswHeader, NodeRecord};

/// Hard cap on a node's level to avoid a pathological RNG draw allocating a
/// huge number of (empty) neighbor lists.
const MAX_LEVEL_CAP: usize = 32;

/// Error returned by graph mutations.
#[derive(Debug, thiserror::Error)]
pub enum HnswError {
    #[error("vector has dimension {got}, expected {expected}")]
    DimensionMismatch { got: usize, expected: usize },
}

/// A distance/id pair ordered by distance (ascending via `total_cmp`).
///
/// `BinaryHeap<Cand>` is therefore a max-heap by distance (farthest on top).
#[derive(Clone, Copy, PartialEq)]
struct Cand {
    dist: f32,
    id: u64,
}

impl Eq for Cand {}
impl Ord for Cand {
    fn cmp(&self, other: &Self) -> Ordering {
        self.dist.total_cmp(&other.dist)
    }
}
impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Changes accumulated since the last [`Hnsw::take_dirty`], for write-through.
#[derive(Debug, Default)]
pub struct DirtyChanges {
    /// Nodes whose vector must be (re)written: `(id, prepared vector)`.
    pub vectors: Vec<(u64, Vec<f32>)>,
    /// Nodes whose adjacency must be (re)written: `(id, per-level neighbors)`.
    pub adjacency: Vec<(u64, Vec<Vec<u64>>)>,
    /// Nodes whose records must be deleted from the backing store.
    pub deleted: Vec<u64>,
}

impl DirtyChanges {
    /// Whether there is nothing to persist.
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty() && self.adjacency.is_empty() && self.deleted.is_empty()
    }
}

/// The HNSW graph.
pub struct Hnsw {
    nodes: FxHashMap<u64, NodeRecord>,
    entry_point: Option<u64>,
    max_level: usize,

    metric: Metric,
    dim: usize,
    m: usize,
    m_max0: usize,
    ef_construction: usize,
    ef_search: usize,
    ml: f64,

    rng: SmallRng,
    /// Nodes whose vector changed (new/updated inserts).
    dirty_vec: FxHashSet<u64>,
    /// Nodes whose adjacency changed. Vectors are much larger than neighbor
    /// lists, so tracking them separately avoids rewriting a full vector every
    /// time one of its neighbors gains a link.
    dirty_adj: FxHashSet<u64>,
    deleted: FxHashSet<u64>,
}

impl Hnsw {
    /// Creates an empty graph with the given parameters.
    pub fn new(
        dim: usize,
        metric: Metric,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
    ) -> Self {
        let m = m.max(2);
        Hnsw {
            nodes: FxHashMap::default(),
            entry_point: None,
            max_level: 0,
            metric,
            dim,
            m,
            m_max0: m * 2,
            ef_construction: ef_construction.max(m),
            ef_search: ef_search.max(1),
            ml: 1.0 / (m as f64).ln(),
            // Fixed seed: reproducible builds and tests. Level distribution
            // quality does not depend on cryptographic randomness.
            rng: SmallRng::seed_from_u64(0x9E3779B97F4A7C15),
            dirty_vec: FxHashSet::default(),
            dirty_adj: FxHashSet::default(),
            deleted: FxHashSet::default(),
        }
    }

    /// Rebuilds a graph from persisted parts, sanitizing as it goes:
    /// vectors of the wrong dimension are dropped, a node without adjacency
    /// becomes an isolated level-0 node, neighbor links to absent nodes are
    /// pruned, and a missing entry point is re-elected. A torn persist thus
    /// degrades recall instead of corrupting the graph.
    ///
    /// Nothing is marked dirty; sanitization is re-applied on every load and
    /// the next organic mutation persists the touched nodes anyway.
    pub fn from_parts(
        header: HnswHeader,
        vectors: Vec<(u64, Vec<f32>)>,
        mut adjacency: FxHashMap<u64, Vec<Vec<u64>>>,
    ) -> Self {
        let mut graph = Hnsw::new(
            header.dim,
            header.metric,
            header.m,
            header.ef_construction,
            header.ef_search,
        );
        graph.entry_point = header.entry_point;
        graph.max_level = header.max_level;

        let mut dropped = 0usize;
        for (id, vector) in vectors {
            if vector.len() != graph.dim {
                dropped += 1;
                continue;
            }
            let neighbors = adjacency.remove(&id).unwrap_or_else(|| vec![Vec::new()]);
            let neighbors = if neighbors.is_empty() { vec![Vec::new()] } else { neighbors };
            graph.nodes.insert(id, NodeRecord { id, vector, neighbors });
        }

        // Prune links to nodes that don't exist (torn persist / lost record).
        let ids: Vec<u64> = graph.nodes.keys().copied().collect();
        let present: FxHashSet<u64> = ids.iter().copied().collect();
        let mut pruned = 0usize;
        for id in ids {
            if let Some(node) = graph.nodes.get_mut(&id) {
                for level in node.neighbors.iter_mut() {
                    let before = level.len();
                    level.retain(|n| present.contains(n));
                    pruned += before - level.len();
                }
            }
        }

        // Entry point must exist; max_level must not exceed any real level.
        let entry_ok = graph
            .entry_point
            .map(|ep| graph.nodes.contains_key(&ep))
            .unwrap_or(false);
        if !entry_ok || graph.nodes.is_empty() {
            graph.reelect_entry_point();
        }

        if dropped > 0 || pruned > 0 {
            log::warn!(
                "HNSW load repaired a damaged graph: dropped {dropped} bad node(s), pruned {pruned} dangling link(s)"
            );
        }
        graph
    }

    /// Current header snapshot (for persistence).
    pub fn header(&self) -> HnswHeader {
        HnswHeader {
            dim: self.dim,
            metric: self.metric,
            m: self.m,
            ef_construction: self.ef_construction,
            ef_search: self.ef_search,
            entry_point: self.entry_point,
            max_level: self.max_level,
        }
    }

    /// Number of indexed vectors.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the graph is empty.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The configured metric.
    pub fn metric(&self) -> Metric {
        self.metric
    }

    /// The stored (metric-prepared) vector for a node, if present.
    pub fn vector_of(&self, id: u64) -> Option<&[f32]> {
        self.nodes.get(&id).map(|n| n.vector.as_slice())
    }

    /// Drains everything that must be persisted since the last call. Nodes with
    /// a changed vector also report their adjacency (a new node needs both).
    pub fn take_dirty(&mut self) -> DirtyChanges {
        let mut adj_ids: FxHashSet<u64> = self.dirty_adj.drain().collect();
        let mut vectors = Vec::with_capacity(self.dirty_vec.len());
        for id in self.dirty_vec.drain() {
            if let Some(node) = self.nodes.get(&id) {
                vectors.push((id, node.vector.clone()));
                adj_ids.insert(id);
            }
        }
        let adjacency = adj_ids
            .into_iter()
            .filter_map(|id| self.nodes.get(&id).map(|n| (id, n.neighbors.clone())))
            .collect();
        DirtyChanges {
            vectors,
            adjacency,
            deleted: self.deleted.drain().collect(),
        }
    }

    #[inline]
    fn m_max(&self, level: usize) -> usize {
        if level == 0 {
            self.m_max0
        } else {
            self.m
        }
    }

    fn random_level(&mut self) -> usize {
        let r: f64 = self.rng.gen::<f64>();
        // 1 - r is in (0, 1]; its ln is <= 0, negated >= 0.
        let level = (-(1.0 - r).ln() * self.ml).floor() as usize;
        level.min(MAX_LEVEL_CAP)
    }

    /// Distance from `query` to node `id`, or `None` if the node is absent
    /// (dangling reference). Traversal treats an absent node as unreachable
    /// rather than panicking, so a damaged graph stays usable.
    #[inline]
    fn distance_to(&self, query: &[f32], id: u64) -> Option<f32> {
        self.nodes.get(&id).map(|n| self.metric.distance(query, &n.vector))
    }

    /// Inserts (or replaces) a vector for `id`.
    pub fn insert(&mut self, id: u64, raw: Vec<f32>) -> Result<(), HnswError> {
        if raw.len() != self.dim {
            return Err(HnswError::DimensionMismatch {
                got: raw.len(),
                expected: self.dim,
            });
        }
        // Re-index on update: remove the old node first so stale links vanish.
        if self.nodes.contains_key(&id) {
            self.remove(id);
        }

        let vector = self.metric.prepare(raw);
        let level = self.random_level();

        self.nodes.insert(
            id,
            NodeRecord {
                id,
                vector: vector.clone(),
                neighbors: vec![Vec::new(); level + 1],
            },
        );
        self.dirty_vec.insert(id);
        // An update (remove-then-insert of the same id) must not leave the id
        // in the deleted set: the persist pass would remove-then-put the same
        // keys, and a crash between those two steps would lose a live node.
        self.deleted.remove(&id);

        let entry = match self.entry_point {
            Some(ep) => ep,
            None => {
                // First node becomes the entry point.
                self.entry_point = Some(id);
                self.max_level = level;
                return Ok(());
            }
        };

        // Phase 1: greedy descent from the top down to the layer just above the
        // new node's top level, tracking the single closest node.
        let mut cur = entry;
        let mut cur_dist = self.distance_to(&vector, cur).unwrap_or(f32::INFINITY);
        let mut lc = self.max_level;
        while lc > level {
            let mut changed = true;
            while changed {
                changed = false;
                let neighbors = match self.nodes.get(&cur) {
                    Some(n) if lc < n.neighbors.len() => n.neighbors[lc].clone(),
                    _ => Vec::new(),
                };
                for n in neighbors {
                    let Some(d) = self.distance_to(&vector, n) else { continue };
                    if d < cur_dist {
                        cur_dist = d;
                        cur = n;
                        changed = true;
                    }
                }
            }
            lc -= 1;
        }

        // Phase 2: from the new node's top level down to 0, find candidates,
        // select diverse neighbors, and connect bidirectionally.
        let mut entry_points = vec![cur];
        let top = level.min(self.max_level);
        for lc in (0..=top).rev() {
            let candidates = self.search_layer(&vector, &entry_points, self.ef_construction, lc);
            let m_lc = self.m_max(lc);
            let selected = self.select_neighbors(&vector, &candidates, m_lc);

            // Connect the new node.
            if let Some(node) = self.nodes.get_mut(&id) {
                node.neighbors[lc] = selected.clone();
            }
            // Connect each neighbor back and prune if over capacity.
            for &n in &selected {
                self.connect_and_prune(n, id, lc, m_lc);
                self.dirty_adj.insert(n);
            }

            // Next level starts from all candidates found here.
            entry_points = candidates.iter().map(|c| c.id).collect();
            if entry_points.is_empty() {
                entry_points = vec![cur];
            }
        }

        if level > self.max_level {
            self.max_level = level;
            self.entry_point = Some(id);
        }
        Ok(())
    }

    /// Adds `new_neighbor` to `node`'s list at `level`, pruning back to `m_lc`
    /// with the selection heuristic if the list overflows.
    fn connect_and_prune(&mut self, node: u64, new_neighbor: u64, level: usize, m_lc: usize) {
        let mut list = match self.nodes.get(&node) {
            Some(n) if level < n.neighbors.len() => n.neighbors[level].clone(),
            _ => return,
        };
        if list.contains(&new_neighbor) {
            return;
        }
        list.push(new_neighbor);

        if list.len() > m_lc {
            let base = match self.nodes.get(&node) {
                Some(n) => n.vector.clone(),
                None => return,
            };
            let cands: Vec<Cand> = list
                .iter()
                .filter_map(|&e| self.distance_to(&base, e).map(|dist| Cand { dist, id: e }))
                .collect();
            list = self.select_neighbors(&base, &cands, m_lc);
        }

        if let Some(n) = self.nodes.get_mut(&node) {
            n.neighbors[level] = list;
        }
    }

    /// Algorithm 2: greedily explores `level` from `entry_points`, returning up
    /// to `ef` closest nodes (unordered `Cand`s).
    fn search_layer(&self, query: &[f32], entry_points: &[u64], ef: usize, level: usize) -> Vec<Cand> {
        let mut visited: FxHashSet<u64> = FxHashSet::default();
        // Min-heap of nodes still to explore (closest first).
        let mut candidates: BinaryHeap<std::cmp::Reverse<Cand>> = BinaryHeap::new();
        // Max-heap of the best `ef` results (farthest on top).
        let mut results: BinaryHeap<Cand> = BinaryHeap::new();

        for &ep in entry_points {
            if !visited.insert(ep) {
                continue;
            }
            let Some(d) = self.distance_to(query, ep) else { continue };
            candidates.push(std::cmp::Reverse(Cand { dist: d, id: ep }));
            results.push(Cand { dist: d, id: ep });
        }

        while let Some(std::cmp::Reverse(c)) = candidates.pop() {
            let farthest = results.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
            if c.dist > farthest && results.len() >= ef {
                break;
            }
            let neighbors = match self.nodes.get(&c.id) {
                Some(n) if level < n.neighbors.len() => n.neighbors[level].clone(),
                _ => continue,
            };
            for e in neighbors {
                if !visited.insert(e) {
                    continue;
                }
                let Some(d) = self.distance_to(query, e) else { continue };
                let farthest = results.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
                if d < farthest || results.len() < ef {
                    candidates.push(std::cmp::Reverse(Cand { dist: d, id: e }));
                    results.push(Cand { dist: d, id: e });
                    if results.len() > ef {
                        results.pop();
                    }
                }
            }
        }

        results.into_sorted_vec() // ascending by distance
    }

    /// Algorithm 4: from `candidates` (relative to `base`), pick up to `m`
    /// neighbors, preferring diversity — an element is dropped if it is closer
    /// to an already-selected neighbor than to `base`.
    fn select_neighbors(&self, _base: &[f32], candidates: &[Cand], m: usize) -> Vec<u64> {
        let mut sorted = candidates.to_vec();
        sorted.sort_unstable_by(|a, b| a.dist.total_cmp(&b.dist));

        let mut selected: Vec<u64> = Vec::with_capacity(m);
        for cand in sorted {
            if selected.len() >= m {
                break;
            }
            let Some(cand_vec) = self.nodes.get(&cand.id).map(|n| &n.vector) else {
                continue; // dangling reference: never select it
            };
            let mut keep = true;
            for &r in &selected {
                let Some(r_vec) = self.nodes.get(&r).map(|n| &n.vector) else { continue };
                let d_to_r = self.metric.distance(cand_vec, r_vec);
                if d_to_r < cand.dist {
                    keep = false;
                    break;
                }
            }
            if keep {
                selected.push(cand.id);
            }
        }
        selected
    }

    /// Searches for the `k` nearest neighbors of `raw`, returning
    /// `(id, distance)` pairs ordered by ascending distance.
    pub fn search(&self, raw: &[f32], k: usize, ef: Option<usize>) -> Vec<(u64, f32)> {
        if self.nodes.is_empty() || k == 0 {
            return Vec::new();
        }
        let query = self.metric.prepare(raw.to_vec());
        let Some(entry) = self.entry_point else { return Vec::new() };

        // Greedy descent from the top to layer 1.
        let mut cur = entry;
        let mut cur_dist = self.distance_to(&query, cur).unwrap_or(f32::INFINITY);
        let mut lc = self.max_level;
        while lc >= 1 {
            let mut changed = true;
            while changed {
                changed = false;
                let neighbors = match self.nodes.get(&cur) {
                    Some(n) if lc < n.neighbors.len() => n.neighbors[lc].clone(),
                    _ => Vec::new(),
                };
                for n in neighbors {
                    let Some(d) = self.distance_to(&query, n) else { continue };
                    if d < cur_dist {
                        cur_dist = d;
                        cur = n;
                        changed = true;
                    }
                }
            }
            lc -= 1;
        }

        let ef = ef.unwrap_or(self.ef_search).max(k);
        let results = self.search_layer(&query, &[cur], ef, 0);
        results
            .into_iter()
            .take(k)
            .map(|c| (c.id, c.dist))
            .collect()
    }

    /// Removes `id` from the graph, unlinking it from all neighbors,
    /// reconnecting the orphaned neighborhood through the deleted node's other
    /// neighbors (so sustained churn does not fragment the graph), and
    /// re-electing the entry point if necessary. Returns whether it existed.
    pub fn remove(&mut self, id: u64) -> bool {
        let node = match self.nodes.remove(&id) {
            Some(n) => n,
            None => return false,
        };

        // Unlink from every neighbor at every level the node participated in.
        for (level, neighbors) in node.neighbors.iter().enumerate() {
            for &n in neighbors {
                if let Some(nn) = self.nodes.get_mut(&n) {
                    if level < nn.neighbors.len() {
                        nn.neighbors[level].retain(|&x| x != id);
                        self.dirty_adj.insert(n);
                    }
                }
            }
        }

        // Reconnect: offer each surviving neighbor the deleted node's other
        // neighbors as candidates and re-select with the diversity heuristic,
        // so paths that ran through the deleted node are patched around it.
        for (level, neighbors) in node.neighbors.iter().enumerate() {
            let m_lc = self.m_max(level);
            for &n in neighbors {
                let (base, mut cand_ids) = match self.nodes.get(&n) {
                    Some(nn) if level < nn.neighbors.len() => {
                        (nn.vector.clone(), nn.neighbors[level].clone())
                    }
                    _ => continue,
                };
                let mut grew = false;
                for &o in neighbors {
                    if o != n && self.nodes.contains_key(&o) && !cand_ids.contains(&o) {
                        cand_ids.push(o);
                        grew = true;
                    }
                }
                if !grew {
                    continue;
                }
                let cands: Vec<Cand> = cand_ids
                    .iter()
                    .filter_map(|&e| self.distance_to(&base, e).map(|dist| Cand { dist, id: e }))
                    .collect();
                let selected = self.select_neighbors(&base, &cands, m_lc);
                if let Some(nn) = self.nodes.get_mut(&n) {
                    if level < nn.neighbors.len() {
                        nn.neighbors[level] = selected;
                        self.dirty_adj.insert(n);
                    }
                }
            }
        }

        self.dirty_vec.remove(&id);
        self.dirty_adj.remove(&id);
        self.deleted.insert(id);

        // Re-elect entry point / recompute max level if needed.
        if self.entry_point == Some(id) || self.nodes.is_empty() {
            self.reelect_entry_point();
        }
        true
    }

    fn reelect_entry_point(&mut self) {
        let mut best: Option<(u64, usize)> = None;
        for node in self.nodes.values() {
            let lvl = node.top_level();
            if best.map(|(_, b)| lvl > b).unwrap_or(true) {
                best = Some((node.id, lvl));
            }
        }
        match best {
            Some((id, lvl)) => {
                self.entry_point = Some(id);
                self.max_level = lvl;
            }
            None => {
                self.entry_point = None;
                self.max_level = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brute_force(vectors: &[(u64, Vec<f32>)], query: &[f32], k: usize, metric: Metric) -> Vec<u64> {
        let mut scored: Vec<(f32, u64)> = vectors
            .iter()
            .map(|(id, v)| {
                let pv = metric.prepare(v.clone());
                let pq = metric.prepare(query.to_vec());
                (metric.distance(&pq, &pv), *id)
            })
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0));
        scored.into_iter().take(k).map(|(_, id)| id).collect()
    }

    // Simple deterministic pseudo-random vectors (no rand dependency in test
    // math so ground truth stays reproducible).
    fn gen_vectors(n: usize, dim: usize) -> Vec<(u64, Vec<f32>)> {
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 40) as f32 / (1u64 << 24) as f32 - 0.5
        };
        (0..n)
            .map(|i| (i as u64, (0..dim).map(|_| next()).collect()))
            .collect()
    }

    #[test]
    fn insert_then_search_returns_exact_point() {
        let mut hnsw = Hnsw::new(3, Metric::Euclidean, 16, 200, 64);
        hnsw.insert(1, vec![1.0, 0.0, 0.0]).unwrap();
        hnsw.insert(2, vec![0.0, 1.0, 0.0]).unwrap();
        hnsw.insert(3, vec![0.0, 0.0, 1.0]).unwrap();

        let res = hnsw.search(&[1.0, 0.0, 0.0], 1, None);
        assert_eq!(res[0].0, 1);
        assert!(res[0].1 < 1e-6);
    }

    #[test]
    fn k_greater_than_n_returns_all() {
        let mut hnsw = Hnsw::new(2, Metric::Cosine, 16, 200, 64);
        hnsw.insert(1, vec![1.0, 0.0]).unwrap();
        hnsw.insert(2, vec![0.0, 1.0]).unwrap();
        let res = hnsw.search(&[1.0, 1.0], 10, None);
        assert_eq!(res.len(), 2);
    }

    #[test]
    fn dimension_mismatch_is_rejected() {
        let mut hnsw = Hnsw::new(3, Metric::Cosine, 16, 200, 64);
        assert!(hnsw.insert(1, vec![1.0, 2.0]).is_err());
    }

    #[test]
    fn recall_is_high_vs_brute_force() {
        let dim = 16;
        let n = 1500;
        let metric = Metric::Euclidean;
        let vectors = gen_vectors(n, dim);

        let mut hnsw = Hnsw::new(dim, metric, 16, 200, 64);
        for (id, v) in &vectors {
            hnsw.insert(*id, v.clone()).unwrap();
        }

        let k = 10;
        let queries = gen_vectors(50, dim);
        let mut hits = 0usize;
        let mut total = 0usize;
        for (_, q) in &queries {
            let truth: std::collections::HashSet<u64> = brute_force(&vectors, q, k, metric).into_iter().collect();
            let got = hnsw.search(q, k, Some(128));
            for (id, _) in got {
                if truth.contains(&id) {
                    hits += 1;
                }
            }
            total += k;
        }
        let recall = hits as f64 / total as f64;
        assert!(recall >= 0.95, "recall {recall} below 0.95");
    }

    #[test]
    fn delete_removes_from_results_and_keeps_graph_searchable() {
        let dim = 8;
        let vectors = gen_vectors(300, dim);
        let mut hnsw = Hnsw::new(dim, Metric::Euclidean, 16, 200, 64);
        for (id, v) in &vectors {
            hnsw.insert(*id, v.clone()).unwrap();
        }

        // Delete the exact nearest of a query, then confirm it is gone but the
        // graph still returns other close results.
        let query = vectors[10].1.clone();
        let before = hnsw.search(&query, 1, None);
        assert_eq!(before[0].0, 10);

        assert!(hnsw.remove(10));
        assert!(!hnsw.remove(10)); // idempotent

        let after = hnsw.search(&query, 5, Some(64));
        assert!(after.iter().all(|(id, _)| *id != 10));
        assert!(!after.is_empty());
    }

    #[test]
    fn entry_point_is_reelected_after_deletion() {
        let dim = 4;
        let vectors = gen_vectors(200, dim);
        let mut hnsw = Hnsw::new(dim, Metric::Cosine, 16, 200, 64);
        for (id, v) in &vectors {
            hnsw.insert(*id, v.clone()).unwrap();
        }
        // Repeatedly delete the current entry point; the graph must stay usable.
        for _ in 0..20 {
            if let Some(ep) = hnsw.entry_point {
                hnsw.remove(ep);
            }
            let res = hnsw.search(&vectors[0].1, 3, Some(64));
            assert!(!res.is_empty());
        }
    }

    #[test]
    fn from_parts_prunes_dangling_links_and_stays_searchable() {
        // Simulate a torn persist: adjacency references node 99 whose vector
        // record was never written, and the entry point is gone too.
        let header = HnswHeader {
            dim: 2,
            metric: Metric::Euclidean,
            m: 4,
            ef_construction: 32,
            ef_search: 16,
            entry_point: Some(99),
            max_level: 0,
        };
        let vectors = vec![
            (1u64, vec![0.0, 0.0]),
            (2u64, vec![1.0, 0.0]),
            (3u64, vec![0.0, 5.0]), // wrong-dim record below must be dropped
            (4u64, vec![1.0]),
        ];
        let mut adjacency = FxHashMap::default();
        adjacency.insert(1u64, vec![vec![2, 99]]);
        adjacency.insert(2u64, vec![vec![1, 99, 4]]);
        adjacency.insert(3u64, vec![vec![1]]);

        let graph = Hnsw::from_parts(header, vectors, adjacency);
        assert_eq!(graph.len(), 3); // node 4 dropped (bad dim)
        // Search must not panic and must return real nodes only.
        let res = graph.search(&[0.1, 0.1], 3, Some(16));
        assert!(!res.is_empty());
        assert!(res.iter().all(|(id, _)| [1, 2, 3].contains(id)));
    }

    #[test]
    fn heavy_delete_churn_keeps_graph_connected() {
        let dim = 8;
        let vectors = gen_vectors(400, dim);
        let mut hnsw = Hnsw::new(dim, Metric::Euclidean, 8, 100, 64);
        for (id, v) in &vectors {
            hnsw.insert(*id, v.clone()).unwrap();
        }
        // Delete half the graph in id order (worst case for fragmentation).
        for id in 0..200u64 {
            assert!(hnsw.remove(id));
        }
        // Every survivor must still be findable by its own vector.
        let mut found = 0;
        for (id, v) in vectors.iter().skip(200) {
            let res = hnsw.search(v, 1, Some(64));
            if res.first().map(|(rid, _)| rid == id).unwrap_or(false) {
                found += 1;
            }
        }
        assert!(found >= 190, "only {found}/200 survivors findable after churn");
    }

    #[test]
    fn take_dirty_splits_vectors_and_adjacency() {
        let mut hnsw = Hnsw::new(2, Metric::Euclidean, 4, 32, 16);
        hnsw.insert(1, vec![0.0, 0.0]).unwrap();
        hnsw.insert(2, vec![1.0, 0.0]).unwrap();
        let changes = hnsw.take_dirty();
        assert_eq!(changes.vectors.len(), 2);
        assert!(changes.adjacency.len() >= 2);
        assert!(changes.deleted.is_empty());

        // A delete only produces adjacency updates + a deletion, no vectors.
        hnsw.remove(1);
        let changes = hnsw.take_dirty();
        assert!(changes.vectors.is_empty());
        assert_eq!(changes.deleted, vec![1]);

        // Update of the same id in one persist window must NOT report it deleted.
        hnsw.remove(2);
        hnsw.insert(2, vec![5.0, 5.0]).unwrap();
        let changes = hnsw.take_dirty();
        assert!(changes.deleted.is_empty());
        assert_eq!(changes.vectors.len(), 1);
    }

    #[test]
    fn update_reindexes_vector() {
        let mut hnsw = Hnsw::new(2, Metric::Euclidean, 16, 200, 64);
        hnsw.insert(1, vec![0.0, 0.0]).unwrap();
        hnsw.insert(2, vec![10.0, 10.0]).unwrap();
        // Move node 1 far away; querying near origin should now prefer node 2.
        hnsw.insert(1, vec![100.0, 100.0]).unwrap();
        let res = hnsw.search(&[0.1, 0.1], 1, None);
        assert_eq!(res[0].0, 2);
        assert_eq!(hnsw.len(), 2);
    }
}
