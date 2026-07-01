//! User-selectable stored-vector precision.
//!
//! [`Precision`] drives the codec used to persist full vectors, trading storage
//! size (on disk and in the LRU cache) for reconstruction exactness. The same
//! codec is used by both index backends and by cache-budget accounting, so a
//! configured precision literally changes the bytes written and the memory used.

use serde::{Deserialize, Serialize};

/// Stored-vector precision.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Precision {
    /// Full 32-bit float — exact, 4 bytes/dim.
    #[default]
    F32,
    /// IEEE half (16-bit) — ~exact for normalized embeddings, 2 bytes/dim.
    F16,
    /// Per-vector scalar quantization to `u8` — 4× smaller, approximate.
    /// Layout: `[min: f32][scale: f32][u8; dim]`.
    I8,
}

impl Precision {
    /// Encoded byte length for a vector of `dim` dimensions.
    #[inline]
    pub fn encoded_len(&self, dim: usize) -> usize {
        match self {
            Precision::F32 => dim * 4,
            Precision::F16 => dim * 2,
            Precision::I8 => 8 + dim, // min + scale (f32 each) + dim bytes
        }
    }

    /// Encodes a vector to bytes at this precision.
    pub fn encode(&self, v: &[f32]) -> Vec<u8> {
        match self {
            Precision::F32 => {
                let mut out = Vec::with_capacity(v.len() * 4);
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
                out
            }
            Precision::F16 => {
                let mut out = Vec::with_capacity(v.len() * 2);
                for x in v {
                    out.extend_from_slice(&half::f16::from_f32(*x).to_le_bytes());
                }
                out
            }
            Precision::I8 => {
                let min = v.iter().copied().fold(f32::INFINITY, f32::min);
                let max = v.iter().copied().fold(f32::NEG_INFINITY, f32::max);
                let range = max - min;
                // scale maps [0, 255] back to [min, max]; 0 range -> constant vector.
                let scale = if range > 0.0 { range / 255.0 } else { 0.0 };
                let mut out = Vec::with_capacity(self.encoded_len(v.len()));
                out.extend_from_slice(&min.to_le_bytes());
                out.extend_from_slice(&scale.to_le_bytes());
                for x in v {
                    let q = if scale > 0.0 {
                        (((x - min) / scale).round()).clamp(0.0, 255.0) as u8
                    } else {
                        0
                    };
                    out.push(q);
                }
                out
            }
        }
    }

    /// Decodes bytes produced by [`Precision::encode`] back into `dim` floats.
    pub fn decode(&self, bytes: &[u8], dim: usize) -> Vec<f32> {
        let mut out = Vec::with_capacity(dim);
        self.decode_into(bytes, dim, &mut out);
        out
    }

    /// Decodes into a caller-provided buffer, reusing its capacity — the
    /// allocation-free path used on the query hot loop.
    pub fn decode_into(&self, bytes: &[u8], dim: usize, out: &mut Vec<f32>) {
        out.clear();
        out.reserve(dim);
        match self {
            Precision::F32 => {
                for c in bytes.chunks_exact(4).take(dim) {
                    out.push(f32::from_le_bytes([c[0], c[1], c[2], c[3]]));
                }
            }
            Precision::F16 => {
                for c in bytes.chunks_exact(2).take(dim) {
                    out.push(half::f16::from_le_bytes([c[0], c[1]]).to_f32());
                }
            }
            Precision::I8 => {
                if bytes.len() < 8 {
                    out.resize(dim, 0.0);
                    return;
                }
                let min = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                let scale = f32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
                for &q in bytes[8..].iter().take(dim) {
                    out.push(min + q as f32 * scale);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<f32> {
        vec![-1.0, -0.25, 0.0, 0.1, 0.5, 0.9, 1.0, 2.5]
    }

    #[test]
    fn f32_round_trips_exactly() {
        let v = sample();
        let bytes = Precision::F32.encode(&v);
        assert_eq!(bytes.len(), Precision::F32.encoded_len(v.len()));
        assert_eq!(Precision::F32.decode(&bytes, v.len()), v);
    }

    #[test]
    fn f16_round_trips_within_half_precision() {
        let v = sample();
        let bytes = Precision::F16.encode(&v);
        assert_eq!(bytes.len(), Precision::F16.encoded_len(v.len()));
        let back = Precision::F16.decode(&bytes, v.len());
        for (a, b) in v.iter().zip(back.iter()) {
            assert!((a - b).abs() <= a.abs() * 1e-2 + 1e-3, "{a} vs {b}");
        }
    }

    #[test]
    fn i8_round_trips_within_quantization_error() {
        let v = sample();
        let bytes = Precision::I8.encode(&v);
        assert_eq!(bytes.len(), Precision::I8.encoded_len(v.len()));
        let back = Precision::I8.decode(&bytes, v.len());
        let range = 2.5 - (-1.0);
        let step = range / 255.0;
        for (a, b) in v.iter().zip(back.iter()) {
            assert!((a - b).abs() <= step, "{a} vs {b}");
        }
    }

    #[test]
    fn i8_preserves_ordering() {
        // Quantization must not reorder magnitudes on a monotone vector.
        let v: Vec<f32> = (0..32).map(|i| i as f32).collect();
        let back = Precision::I8.decode(&Precision::I8.encode(&v), v.len());
        for w in back.windows(2) {
            assert!(w[0] <= w[1]);
        }
    }

    #[test]
    fn constant_vector_is_stable_under_i8() {
        let v = vec![3.0; 5];
        let back = Precision::I8.decode(&Precision::I8.encode(&v), v.len());
        for x in back {
            assert!((x - 3.0).abs() < 1e-6);
        }
    }
}
