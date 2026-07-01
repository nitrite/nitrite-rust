//! Product quantization (PQ) for compact, RAM-resident approximate distances.
//!
//! A D-dimensional vector is split into `m` contiguous subvectors; each subspace
//! has its own codebook of up to 256 centroids trained by k-means. A vector is
//! then encoded as `m` bytes (one centroid index per subspace). During search we
//! precompute, once per query, a table of query-subvector→centroid distances and
//! approximate a candidate's distance by summing table lookups (asymmetric
//! distance computation, ADC). This lets the Vamana walk rank candidates using
//! only the tiny in-RAM codes; exact distances are computed later by re-ranking
//! the few finalists against their full on-disk vectors.
//!
//! PQ operates in squared-L2 space on metric-*prepared* vectors (cosine vectors
//! are L2-normalized upstream), which is monotone with cosine/Euclidean ordering.
//! It only needs to guide traversal — final results are exact after re-rank.

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

const KMEANS_ITERS: usize = 16;
const MAX_K: usize = 256;

/// A trained product quantizer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProductQuantizer {
    dim: usize,
    m: usize,
    sub_dim: usize,
    /// `codebook[subspace][code]` is a centroid of length `sub_dim`.
    codebook: Vec<Vec<Vec<f32>>>,
}

impl ProductQuantizer {
    /// Trains a quantizer on `training` (metric-prepared) vectors.
    ///
    /// `m` is the number of subvectors (= bytes per code). If `dim` is not
    /// divisible by `m`, vectors are zero-padded to `m * ceil(dim/m)`.
    pub fn train(training: &[Vec<f32>], dim: usize, m: usize) -> Self {
        let m = m.max(1).min(dim.max(1));
        let sub_dim = dim.div_ceil(m);
        let k = MAX_K.min(training.len().max(1));

        let mut rng = SmallRng::seed_from_u64(0xC0FFEE_D15EA5E);
        let mut codebook = Vec::with_capacity(m);
        for s in 0..m {
            let start = s * sub_dim;
            let subvectors: Vec<Vec<f32>> = training
                .iter()
                .map(|v| sub_slice(v, start, sub_dim))
                .collect();
            codebook.push(kmeans(&subvectors, k, sub_dim, &mut rng));
        }

        ProductQuantizer { dim, m, sub_dim, codebook }
    }

    /// Bytes per encoded vector.
    pub fn code_len(&self) -> usize {
        self.m
    }

    /// Encodes a (prepared) vector into `m` centroid indices.
    pub fn encode(&self, v: &[f32]) -> Vec<u8> {
        let mut code = Vec::with_capacity(self.m);
        for s in 0..self.m {
            let sub = sub_slice(v, s * self.sub_dim, self.sub_dim);
            code.push(nearest_centroid(&self.codebook[s], &sub) as u8);
        }
        code
    }

    /// Builds per-subspace query→centroid squared-distance tables for a query.
    pub fn query_tables(&self, query: &[f32]) -> Vec<Vec<f32>> {
        let mut tables = Vec::with_capacity(self.m);
        for s in 0..self.m {
            let sub = sub_slice(query, s * self.sub_dim, self.sub_dim);
            let row = self.codebook[s].iter().map(|c| sq_l2(&sub, c)).collect();
            tables.push(row);
        }
        tables
    }

    /// Approximate squared distance of an encoded vector via ADC.
    #[inline]
    pub fn adc_distance(&self, tables: &[Vec<f32>], code: &[u8]) -> f32 {
        let mut sum = 0.0;
        for s in 0..self.m {
            sum += tables[s][code[s] as usize];
        }
        sum
    }
}

fn sub_slice(v: &[f32], start: usize, sub_dim: usize) -> Vec<f32> {
    let mut out = vec![0.0; sub_dim];
    let end = (start + sub_dim).min(v.len());
    if start < v.len() {
        out[..end - start].copy_from_slice(&v[start..end]);
    }
    out
}

#[inline]
fn sq_l2(a: &[f32], b: &[f32]) -> f32 {
    let mut s = 0.0;
    for i in 0..a.len() {
        let d = a[i] - b[i];
        s += d * d;
    }
    s
}

fn nearest_centroid(centroids: &[Vec<f32>], v: &[f32]) -> usize {
    let mut best = 0;
    let mut best_d = f32::INFINITY;
    for (i, c) in centroids.iter().enumerate() {
        let d = sq_l2(v, c);
        if d < best_d {
            best_d = d;
            best = i;
        }
    }
    best
}

/// k-means with k-means++ init (seeded, deterministic).
fn kmeans(data: &[Vec<f32>], k: usize, sub_dim: usize, rng: &mut SmallRng) -> Vec<Vec<f32>> {
    if data.is_empty() {
        return vec![vec![0.0; sub_dim]; 1];
    }
    let k = k.min(data.len());

    // k-means++ initialization.
    let mut centroids: Vec<Vec<f32>> = Vec::with_capacity(k);
    centroids.push(data[rng.gen_range(0..data.len())].clone());
    while centroids.len() < k {
        let dists: Vec<f32> = data
            .iter()
            .map(|p| {
                centroids
                    .iter()
                    .map(|c| sq_l2(p, c))
                    .fold(f32::INFINITY, f32::min)
            })
            .collect();
        let total: f32 = dists.iter().sum();
        if total <= 0.0 {
            // All remaining points coincide with a centroid; pad with copies.
            centroids.push(data[rng.gen_range(0..data.len())].clone());
            continue;
        }
        let mut target = rng.gen::<f32>() * total;
        let mut chosen = data.len() - 1;
        for (i, d) in dists.iter().enumerate() {
            target -= d;
            if target <= 0.0 {
                chosen = i;
                break;
            }
        }
        centroids.push(data[chosen].clone());
    }

    // Lloyd iterations.
    for _ in 0..KMEANS_ITERS {
        let mut sums = vec![vec![0.0f32; sub_dim]; k];
        let mut counts = vec![0usize; k];
        for p in data {
            let c = nearest_centroid(&centroids, p);
            counts[c] += 1;
            for (acc, x) in sums[c].iter_mut().zip(p.iter()) {
                *acc += x;
            }
        }
        for c in 0..k {
            if counts[c] > 0 {
                let inv = 1.0 / counts[c] as f32;
                for (dst, s) in centroids[c].iter_mut().zip(sums[c].iter()) {
                    *dst = s * inv;
                }
            }
        }
    }

    centroids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gen(n: usize, dim: usize, seed: u64) -> Vec<Vec<f32>> {
        let mut s = seed;
        let mut next = || {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s >> 40) as f32 / (1u64 << 24) as f32 - 0.5
        };
        (0..n).map(|_| (0..dim).map(|_| next()).collect()).collect()
    }

    #[test]
    fn code_length_matches_m() {
        let data = gen(500, 16, 1);
        let pq = ProductQuantizer::train(&data, 16, 8);
        assert_eq!(pq.code_len(), 8);
        assert_eq!(pq.encode(&data[0]).len(), 8);
    }

    #[test]
    fn handles_dim_not_divisible_by_m() {
        // dim 10, m 4 -> sub_dim 3, padded to 12.
        let data = gen(300, 10, 2);
        let pq = ProductQuantizer::train(&data, 10, 4);
        let code = pq.encode(&data[0]);
        assert_eq!(code.len(), 4);
        let tables = pq.query_tables(&data[0]);
        assert!(pq.adc_distance(&tables, &code).is_finite());
    }

    #[test]
    fn adc_ranks_near_before_far() {
        let dim = 32;
        let data = gen(800, dim, 3);
        let pq = ProductQuantizer::train(&data, dim, 8);

        // For many queries, the ADC-nearest encoded point should usually be the
        // query itself (or very close), and clearly nearer than a random point.
        let mut agree = 0;
        for q in data.iter().take(50) {
            let tables = pq.query_tables(q);
            let self_code = pq.encode(q);
            let self_d = pq.adc_distance(&tables, &self_code);
            let far_code = pq.encode(&data[700]);
            let far_d = pq.adc_distance(&tables, &far_code);
            if self_d <= far_d {
                agree += 1;
            }
        }
        assert!(agree >= 48, "ADC ordering agreed only {agree}/50");
    }

    #[test]
    fn codebook_round_trips_through_bincode() {
        let data = gen(400, 16, 4);
        let pq = ProductQuantizer::train(&data, 16, 8);
        let bytes = bincode::serde::encode_to_vec(&pq, bincode::config::standard()).unwrap();
        let (back, _): (ProductQuantizer, _) =
            bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).unwrap();
        assert_eq!(pq.encode(&data[5]), back.encode(&data[5]));
    }

    #[test]
    fn trains_with_fewer_points_than_centroids() {
        let data = gen(10, 8, 5);
        let pq = ProductQuantizer::train(&data, 8, 4);
        assert_eq!(pq.encode(&data[0]).len(), 4);
    }
}
