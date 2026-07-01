//! In-memory HNSW (Hierarchical Navigable Small World) graph.
//!
//! Implements the Malkov & Yashunin algorithm with the pruned, diversity
//! preserving neighbor-selection heuristic (Algorithm 4), a multi-layer
//! navigable graph, and exact deletion with neighbor unlinking.
//!
//! The graph owns [`NodeRecord`]s directly, so the same structure is used
//! in memory and (via bincode) on disk. Mutations mark touched nodes dirty;
//! the persistence layer drains those to write through to a `NitriteMap`.

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
    dirty: FxHashSet<u64>,
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
            dirty: FxHashSet::default(),
            deleted: FxHashSet::default(),
        }
    }

    /// Rebuilds a graph from a persisted header and node records.
    pub fn from_records(header: HnswHeader, records: Vec<NodeRecord>) -> Self {
        let mut graph = Hnsw::new(
            header.dim,
            header.metric,
            header.m,
            header.ef_construction,
            header.ef_search,
        );
        graph.entry_point = header.entry_point;
        graph.max_level = header.max_level;
        for rec in records {
            graph.nodes.insert(rec.id, rec);
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

    /// Drains the set of nodes that must be persisted and the set of nodes that
    /// must be deleted from the backing store since the last call.
    pub fn take_dirty(&mut self) -> (Vec<NodeRecord>, Vec<u64>) {
        let dirty: Vec<NodeRecord> = self
            .dirty
            .drain()
            .filter_map(|id| self.nodes.get(&id).cloned())
            .collect();
        let deleted: Vec<u64> = self.deleted.drain().collect();
        (dirty, deleted)
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

    #[inline]
    fn distance_to(&self, query: &[f32], id: u64) -> f32 {
        // Node is guaranteed present when called from traversal.
        self.metric.distance(query, &self.nodes[&id].vector)
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
        self.dirty.insert(id);

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
        let mut cur_dist = self.distance_to(&vector, cur);
        let mut lc = self.max_level;
        while lc > level {
            let mut changed = true;
            while changed {
                changed = false;
                let neighbors = self.nodes[&cur].neighbors[lc].clone();
                for n in neighbors {
                    let d = self.distance_to(&vector, n);
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
                self.dirty.insert(n);
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
            let base = self.nodes[&node].vector.clone();
            let cands: Vec<Cand> = list
                .iter()
                .map(|&e| Cand {
                    dist: self.distance_to(&base, e),
                    id: e,
                })
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
            if !self.nodes.contains_key(&ep) || !visited.insert(ep) {
                continue;
            }
            let d = self.distance_to(query, ep);
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
                let d = self.distance_to(query, e);
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
            let mut keep = true;
            for &r in &selected {
                let d_to_r = self.metric.distance(&self.nodes[&cand.id].vector, &self.nodes[&r].vector);
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
        let entry = self.entry_point.expect("non-empty graph has an entry point");

        // Greedy descent from the top to layer 1.
        let mut cur = entry;
        let mut cur_dist = self.distance_to(&query, cur);
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
                    let d = self.distance_to(&query, n);
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

    /// Removes `id` from the graph, unlinking it from all neighbors and
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
                        self.dirty.insert(n);
                    }
                }
            }
        }

        self.dirty.remove(&id);
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
