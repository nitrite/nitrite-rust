//! The Vamana graph algorithm (DiskANN), independent of storage.
//!
//! A single-layer directed graph is navigated by [`greedy_search`] and pruned
//! for diversity by [`robust_prune`]. Distances are supplied by caller closures
//! so the same algorithm serves both build time (exact distances from full
//! vectors) and query time (approximate PQ distances) over any [`GraphStore`].

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use rustc_hash::FxHashSet;

use crate::distance::Metric;

/// A list of `(distance, node id)` pairs.
pub type Scored = Vec<(f32, u64)>;

/// Adjacency storage the Vamana algorithm operates on.
pub trait GraphStore {
    /// Out-neighbors of `id` (empty if the node has none / is absent).
    fn neighbors(&self, id: u64) -> Vec<u64>;
    /// Replaces the out-neighbors of `id`.
    fn set_neighbors(&self, id: u64, neighbors: Vec<u64>);
    /// Whether `id` is present in the graph.
    fn contains(&self, id: u64) -> bool;
}

/// A distance/id pair ordered ascending by distance via `total_cmp`.
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

/// Greedy beam search from `entry` toward the query defined by `query_dist`.
///
/// `query_dist(id)` returns the distance from the (implicit) query to node `id`.
/// Returns `(top, visited)`:
/// - `top`: the `beam` closest nodes found, ascending by distance;
/// - `visited`: every node whose distance was evaluated, ascending — used as the
///   candidate pool for [`robust_prune`] during insertion.
pub fn greedy_search<S, F>(
    store: &S,
    entry: u64,
    query_dist: F,
    beam: usize,
) -> (Scored, Scored)
where
    S: GraphStore,
    F: Fn(u64) -> f32,
{
    let beam = beam.max(1);
    let mut visited: FxHashSet<u64> = FxHashSet::default();
    let mut visited_out: Vec<(f32, u64)> = Vec::new();

    // Frontier to explore (min-heap: closest first).
    let mut frontier: BinaryHeap<std::cmp::Reverse<Cand>> = BinaryHeap::new();
    // Best `beam` results (max-heap: farthest on top).
    let mut result: BinaryHeap<Cand> = BinaryHeap::new();

    if !store.contains(entry) {
        return (Vec::new(), Vec::new());
    }
    let d0 = query_dist(entry);
    visited.insert(entry);
    visited_out.push((d0, entry));
    frontier.push(std::cmp::Reverse(Cand { dist: d0, id: entry }));
    result.push(Cand { dist: d0, id: entry });

    while let Some(std::cmp::Reverse(c)) = frontier.pop() {
        let farthest = result.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
        if c.dist > farthest && result.len() >= beam {
            break;
        }
        for n in store.neighbors(c.id) {
            if !visited.insert(n) {
                continue;
            }
            let d = query_dist(n);
            visited_out.push((d, n));
            let farthest = result.peek().map(|x| x.dist).unwrap_or(f32::INFINITY);
            if d < farthest || result.len() < beam {
                frontier.push(std::cmp::Reverse(Cand { dist: d, id: n }));
                result.push(Cand { dist: d, id: n });
                if result.len() > beam {
                    result.pop();
                }
            }
        }
    }

    let mut top = result.into_sorted_vec().into_iter().map(|c| (c.dist, c.id)).collect::<Vec<_>>();
    top.dedup_by_key(|(_, id)| *id);
    visited_out.sort_by(|a, b| a.0.total_cmp(&b.0));
    (top, visited_out)
}

/// RobustPrune: from `candidates` (distances relative to `node`), pick ≤ `degree`
/// diverse neighbors and store them. A candidate `p` is dropped once a nearer,
/// already-selected `q` satisfies `alpha * d(p, q) < d(node, p)`.
///
/// `get_vec(id)` returns a candidate's vector; each candidate is fetched **once**
/// and all pairwise distances are computed in local memory via `metric`, so the
/// cost is O(candidates) store reads + O(candidates²) arithmetic — not
/// O(candidates²) store reads. This is the difference between an insert doing a
/// few hundred reads versus tens of thousands.
pub fn robust_prune<S, F>(
    store: &S,
    node: u64,
    candidates: Vec<(f32, u64)>,
    alpha: f32,
    degree: usize,
    metric: Metric,
    get_vec: F,
) where
    S: GraphStore,
    F: Fn(u64) -> Option<Vec<f32>>,
{
    // Fetch each distinct candidate's vector once.
    let mut seen: FxHashSet<u64> = FxHashSet::default();
    let mut cands: Vec<(f32, u64, Vec<f32>)> = candidates
        .into_iter()
        .filter(|(_, id)| *id != node && seen.insert(*id))
        .filter_map(|(d, id)| get_vec(id).map(|v| (d, id, v)))
        .collect();
    cands.sort_by(|a, b| a.0.total_cmp(&b.0));

    let mut selected: Vec<(u64, Vec<f32>)> = Vec::with_capacity(degree);
    for (d_node_p, p, vp) in cands {
        if selected.len() >= degree {
            break;
        }
        let dominated = selected
            .iter()
            .any(|(_, vq)| alpha * metric.distance(&vp, vq) < d_node_p);
        if !dominated {
            selected.push((p, vp));
        }
    }
    store.set_neighbors(node, selected.into_iter().map(|(id, _)| id).collect());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// In-memory GraphStore + coordinates for testing the pure algorithm.
    struct MemGraph {
        adj: Mutex<HashMap<u64, Vec<u64>>>,
        pts: HashMap<u64, Vec<f32>>,
    }
    impl MemGraph {
        fn dist(&self, a: u64, b: u64) -> f32 {
            l2(&self.pts[&a], &self.pts[&b])
        }
    }
    impl GraphStore for MemGraph {
        fn neighbors(&self, id: u64) -> Vec<u64> {
            self.adj.lock().unwrap().get(&id).cloned().unwrap_or_default()
        }
        fn set_neighbors(&self, id: u64, n: Vec<u64>) {
            self.adj.lock().unwrap().insert(id, n);
        }
        fn contains(&self, id: u64) -> bool {
            self.pts.contains_key(&id)
        }
    }

    fn l2(a: &[f32], b: &[f32]) -> f32 {
        a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
    }

    #[test]
    fn robust_prune_caps_degree_and_keeps_nearest() {
        let pts: HashMap<u64, Vec<f32>> = (0..10u64)
            .map(|i| (i, vec![i as f32, 0.0]))
            .collect();
        let g = MemGraph { adj: Mutex::new(HashMap::new()), pts };
        let node = 0;
        let cands: Vec<(f32, u64)> = (1..10).map(|i| (g.dist(node, i), i)).collect();
        let pts = g.pts.clone();
        robust_prune(&g, node, cands, 1.2, 3, Metric::Euclidean, |id| pts.get(&id).cloned());
        let nbrs = g.neighbors(node);
        assert!(nbrs.len() <= 3);
        assert!(nbrs.contains(&1), "nearest neighbor must be kept");
    }

    #[test]
    fn greedy_search_finds_true_nearest_on_a_line() {
        // Points on a line 0..20; chain graph each connected to +-1, +-2.
        let pts: HashMap<u64, Vec<f32>> = (0..20u64).map(|i| (i, vec![i as f32])).collect();
        let mut adj = HashMap::new();
        for i in 0..20i64 {
            let mut n = vec![];
            for d in [-2, -1, 1, 2] {
                let j = i + d;
                if (0..20).contains(&j) {
                    n.push(j as u64);
                }
            }
            adj.insert(i as u64, n);
        }
        let g = MemGraph { adj: Mutex::new(adj), pts };
        let query = vec![13.4f32];
        let (top, _) = greedy_search(&g, 0, |id| l2(&g.pts[&id], &query), 5);
        assert_eq!(top[0].1, 13, "nearest to 13.4 is node 13");
    }
}
