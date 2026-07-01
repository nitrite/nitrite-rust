//! Distance metrics for vector similarity search.
//!
//! All [`Metric::distance`] functions return a value where **smaller means
//! closer**, so the HNSW graph can order candidates with a single min-heap
//! convention regardless of metric. [`Metric::score`] converts a raw distance
//! back into a human-facing similarity where **higher means more similar**
//! (used by the RAG layer and `min_score` cutoffs).

use serde::{Deserialize, Serialize};

/// The distance metric used by a vector index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Metric {
    /// Cosine distance (`1 - cosine_similarity`). Vectors are L2-normalized on
    /// insert, so cosine distance reduces to `1 - dot(a, b)`.
    Cosine,
    /// Squared Euclidean (L2) distance. Squared to avoid the `sqrt` on the hot
    /// path; ordering is identical to true Euclidean distance.
    Euclidean,
    /// Negated dot product (`-dot(a, b)`), so that larger dot products (more
    /// similar) sort first under the smaller-is-closer convention.
    Dot,
}

impl Metric {
    /// Whether stored vectors must be L2-normalized for this metric.
    #[inline]
    pub fn normalizes(&self) -> bool {
        matches!(self, Metric::Cosine)
    }

    /// Prepares a raw input vector for storage under this metric.
    ///
    /// For [`Metric::Cosine`] this L2-normalizes the vector so that later
    /// distance computations only need a dot product. Other metrics are stored
    /// as-is.
    #[inline]
    pub fn prepare(&self, mut v: Vec<f32>) -> Vec<f32> {
        if self.normalizes() {
            normalize(&mut v);
        }
        v
    }

    /// Computes the distance between two prepared vectors of equal length.
    ///
    /// Smaller is closer. Callers must pass vectors of the same dimension
    /// (the index enforces this on write); mismatched lengths are a bug.
    #[inline]
    pub fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        debug_assert_eq!(a.len(), b.len(), "vector dimension mismatch");
        match self {
            Metric::Cosine => 1.0 - dot(a, b),
            Metric::Euclidean => squared_l2(a, b),
            Metric::Dot => -dot(a, b),
        }
    }

    /// Converts a raw distance into a similarity score where higher is better.
    ///
    /// - Cosine: cosine similarity in `[-1, 1]` (`1 - distance`).
    /// - Dot: the raw dot product (`-distance`).
    /// - Euclidean: `1 / (1 + euclidean_distance)` in `(0, 1]`.
    #[inline]
    pub fn score(&self, distance: f32) -> f32 {
        match self {
            Metric::Cosine => 1.0 - distance,
            Metric::Dot => -distance,
            Metric::Euclidean => 1.0 / (1.0 + distance.max(0.0).sqrt()),
        }
    }
}

/// L2-normalizes a vector in place. A zero vector is left unchanged.
#[inline]
pub fn normalize(v: &mut [f32]) {
    let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        let inv = 1.0 / norm;
        for x in v.iter_mut() {
            *x *= inv;
        }
    }
}

/// Dot product of two equal-length slices.
///
/// Uses portable SIMD (`f32x8`) with a scalar tail — the distance kernels are
/// the query hot path, so explicit vectorization here is worth ~2–4× over a
/// scalar loop across dimensions typical of embeddings.
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let n = a.len();
    let mut acc = f32x8::splat(0.0);
    let mut i = 0;
    while i + 8 <= n {
        let va = f32x8::from(&a[i..i + 8]);
        let vb = f32x8::from(&b[i..i + 8]);
        acc = va.mul_add(vb, acc);
        i += 8;
    }
    let mut sum = acc.reduce_add();
    while i < n {
        sum += a[i] * b[i];
        i += 1;
    }
    sum
}

/// Squared Euclidean distance between two equal-length slices.
#[inline]
pub fn squared_l2(a: &[f32], b: &[f32]) -> f32 {
    use wide::f32x8;
    let n = a.len();
    let mut acc = f32x8::splat(0.0);
    let mut i = 0;
    while i + 8 <= n {
        let va = f32x8::from(&a[i..i + 8]);
        let vb = f32x8::from(&b[i..i + 8]);
        let d = va - vb;
        acc = d.mul_add(d, acc);
        i += 8;
    }
    let mut sum = acc.reduce_add();
    while i < n {
        let d = a[i] - b[i];
        sum += d * d;
        i += 1;
    }
    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f32, b: f32) {
        assert!((a - b).abs() < 1e-5, "expected {a} ≈ {b}");
    }

    #[test]
    fn dot_and_l2_basics() {
        let a = [1.0, 2.0, 3.0];
        let b = [4.0, 5.0, 6.0];
        approx(dot(&a, &b), 32.0);
        approx(squared_l2(&a, &b), 27.0); // 9+9+9
    }

    #[test]
    fn cosine_of_identical_normalized_vectors_is_zero_distance() {
        let v = Metric::Cosine.prepare(vec![3.0, 4.0]); // -> [0.6, 0.8]
        approx(v[0], 0.6);
        approx(v[1], 0.8);
        approx(Metric::Cosine.distance(&v, &v), 0.0);
        approx(Metric::Cosine.score(Metric::Cosine.distance(&v, &v)), 1.0);
    }

    #[test]
    fn cosine_of_orthogonal_vectors_is_one() {
        let a = Metric::Cosine.prepare(vec![1.0, 0.0]);
        let b = Metric::Cosine.prepare(vec![0.0, 1.0]);
        approx(Metric::Cosine.distance(&a, &b), 1.0);
        approx(Metric::Cosine.score(Metric::Cosine.distance(&a, &b)), 0.0);
    }

    #[test]
    fn cosine_of_opposite_vectors_is_two() {
        let a = Metric::Cosine.prepare(vec![1.0, 0.0]);
        let b = Metric::Cosine.prepare(vec![-1.0, 0.0]);
        approx(Metric::Cosine.distance(&a, &b), 2.0);
        approx(Metric::Cosine.score(Metric::Cosine.distance(&a, &b)), -1.0);
    }

    #[test]
    fn euclidean_distance_and_score() {
        let a = vec![0.0, 0.0];
        let b = vec![3.0, 4.0];
        // squared distance = 25
        approx(Metric::Euclidean.distance(&a, &b), 25.0);
        // score = 1/(1+5) = 0.16666
        approx(Metric::Euclidean.score(25.0), 1.0 / 6.0);
        // identical vectors -> score 1.0
        approx(Metric::Euclidean.score(Metric::Euclidean.distance(&a, &a)), 1.0);
    }

    #[test]
    fn dot_metric_orders_larger_dot_first() {
        let q = vec![1.0, 1.0];
        let near = vec![2.0, 2.0]; // dot 4
        let far = vec![0.5, 0.0]; // dot 0.5
        assert!(Metric::Dot.distance(&q, &near) < Metric::Dot.distance(&q, &far));
        approx(Metric::Dot.score(Metric::Dot.distance(&q, &near)), 4.0);
    }

    #[test]
    fn zero_vector_normalize_is_noop() {
        let mut v = vec![0.0, 0.0, 0.0];
        normalize(&mut v);
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }
}
