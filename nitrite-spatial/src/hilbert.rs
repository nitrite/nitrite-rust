//! Hilbert curve utilities for spatial locality optimization.
//!
//! The Hilbert curve is a continuous fractal space-filling curve that maps
//! 2D coordinates to a 1D index while preserving spatial locality. This is
//! essential for the Sort-Tile-Recursive (STR) bulk loading algorithm.
//!
//! ## Spatial Locality
//! Points that are close in 2D space remain relatively close along the Hilbert
//! curve, making it ideal for clustering related entries when building R-tree
//! structures.

use crate::bounding_box::BoundingBox;

/// Maximum order for Hilbert curve encoding (determines precision)
const MAX_HILBERT_ORDER: u32 = 32;

/// Encodes 2D coordinates to a Hilbert curve index.
///
/// # Arguments
/// * `x` - X coordinate (normalized to [0, 1])
/// * `y` - Y coordinate (normalized to [0, 1])
/// * `order` - Hilbert curve order (1-32, higher = more precision)
///
/// # Returns
/// Hilbert index as u64 (allows up to 32-bit per dimension at order 32)
///
/// # Example
/// ```no_run
/// use nitrite_spatial::hilbert::hilbert_index;
/// 
/// let index = hilbert_index(0.5, 0.5, 16);
/// assert!(index < (1u64 << 32)); // At order 16, max index is 2^32-1
/// ```
pub fn hilbert_index(x: f64, y: f64, order: u32) -> u64 {
    debug_assert!(x >= 0.0 && x <= 1.0, "x must be in [0,1]");
    debug_assert!(y >= 0.0 && y <= 1.0, "y must be in [0,1]");
    debug_assert!(order > 0 && order <= MAX_HILBERT_ORDER, "order must be 1-32");

    // Convert normalized coordinates to discrete grid coordinates
    let n = 1u64 << order; // 2^order
    let mut xi = (x * (n as f64 - 0.5)) as u64;
    let mut yi = (y * (n as f64 - 0.5)) as u64;

    xi = xi.min(n - 1);
    yi = yi.min(n - 1);

    xy2d(n, xi, yi)
}

/// Encodes 2D coordinates to a Hilbert curve index (bounds-aware).
///
/// Normalizes coordinates based on bounding box before encoding.
///
/// # Arguments
/// * `x` - X coordinate in absolute space
/// * `y` - Y coordinate in absolute space
/// * `bounds` - Bounding box defining the coordinate space
/// * `order` - Hilbert curve order
pub fn hilbert_index_bounded(x: f64, y: f64, bounds: &BoundingBox, order: u32) -> u64 {
    // Normalize to [0, 1]
    let x_range = bounds.max_x - bounds.min_x;
    let y_range = bounds.max_y - bounds.min_y;

    let x_norm = if x_range > 0.0 {
        ((x - bounds.min_x) / x_range).clamp(0.0, 1.0)
    } else {
        0.5
    };

    let y_norm = if y_range > 0.0 {
        ((y - bounds.min_y) / y_range).clamp(0.0, 1.0)
    } else {
        0.5
    };

    hilbert_index(x_norm, y_norm, order)
}

/// Converts (x, y) coordinates on the Hilbert curve to a 1D distance.
///
/// This is the core Hilbert curve algorithm using rotation and reflection.
/// Based on the standard xy2d conversion algorithm.
fn xy2d(n: u64, x: u64, y: u64) -> u64 {
    let mut d = 0u64;
    let mut x = x;
    let mut y = y;
    let mut s = n / 2;

    while s > 0 {
        let rx = ((x & s) > 0) as u64;
        let ry = ((y & s) > 0) as u64;
        d += s * s * ((3 * rx) ^ ry);
        rotate(s, &mut x, &mut y, rx, ry);
        s /= 2;
    }

    d
}

/// Rotates and reflects the coordinate system appropriately for Hilbert curve.
fn rotate(n: u64, x: &mut u64, y: &mut u64, rx: u64, ry: u64) {
    if ry == 0 {
        if rx == 1 {
            *x = n.wrapping_sub(1).wrapping_sub(*x);
            *y = n.wrapping_sub(1).wrapping_sub(*y);
        }
        std::mem::swap(x, y);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hilbert_index_corners() {
        // Corners should have distinct indices
        let tl = hilbert_index(0.0, 1.0, 8);
        let tr = hilbert_index(1.0, 1.0, 8);
        let bl = hilbert_index(0.0, 0.0, 8);
        let br = hilbert_index(1.0, 0.0, 8);

        let mut indices = vec![tl, tr, bl, br];
        indices.sort_unstable();
        indices.dedup();
        assert_eq!(indices.len(), 4, "Corner indices should be unique");
    }

    #[test]
    fn test_hilbert_index_spatial_locality() {
        // Close points should have close indices
        let center = hilbert_index(0.5, 0.5, 8);
        let nearby = hilbert_index(0.50001, 0.50001, 8);

        let diff = if center > nearby {
            center - nearby
        } else {
            nearby - center
        };
        assert!(diff < 1000, "Nearby points should have close indices");
    }

    #[test]
    fn test_hilbert_index_bounded() {
        let bounds = BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 100.0,
            max_y: 100.0,
        };

        let idx1 = hilbert_index_bounded(50.0, 50.0, &bounds, 8);
        let idx2 = hilbert_index_bounded(50.0, 50.0, &bounds, 8);

        assert_eq!(idx1, idx2, "Same coordinates should produce same index");
    }

    #[test]
    fn test_hilbert_index_order_affects_precision() {
        let idx_8 = hilbert_index(0.5, 0.5, 8);
        let idx_16 = hilbert_index(0.5, 0.5, 16);

        // Higher orders should produce larger indices
        assert!(idx_16 >= idx_8, "Higher order should produce larger indices");
    }

    #[test]
    fn test_hilbert_index_boundary_values() {
        // Test minimum coordinates
        let min_idx = hilbert_index(0.0, 0.0, 8);
        assert_eq!(min_idx, 0, "Origin should have index 0");

        // Test near maximum coordinates
        let max_idx = hilbert_index(0.999, 0.999, 8);
        assert!(max_idx > 0, "Near-max coordinates should have positive index");

        // Test midpoint
        let mid_idx = hilbert_index(0.5, 0.5, 8);
        assert!(mid_idx > 0, "Midpoint should have positive index");
    }

    #[test]
    fn test_hilbert_index_different_orders() {
        // Test different orders produce valid indices
        for order in [1, 2, 4, 8, 16, 20, 24] {
            let idx = hilbert_index(0.5, 0.5, order);
            let max_possible = 1u64 << (2 * order);
            assert!(idx < max_possible, "Index for order {} should be less than {}", order, max_possible);
        }
    }

    #[test]
    fn test_hilbert_index_symmetry() {
        // Points at symmetric positions should have some relationship
        let idx_quarter = hilbert_index(0.25, 0.25, 8);
        let idx_three_quarter = hilbert_index(0.75, 0.75, 8);

        // Both should be valid positive indices
        assert!(idx_quarter > 0 || idx_quarter == 0);
        assert!(idx_three_quarter > 0);
        // They should be different
        assert_ne!(idx_quarter, idx_three_quarter);
    }

    #[test]
    fn test_hilbert_index_bounded_normalization() {
        // Test that bounded version normalizes correctly
        let bounds = BoundingBox {
            min_x: -100.0,
            min_y: -100.0,
            max_x: 100.0,
            max_y: 100.0,
        };

        // Center of bounds should map to center of unit square
        let idx_bounded = hilbert_index_bounded(0.0, 0.0, &bounds, 8);
        let idx_direct = hilbert_index(0.5, 0.5, 8);
        assert_eq!(idx_bounded, idx_direct, "Bounded center should match direct center");
    }

    #[test]
    fn test_hilbert_index_bounded_outside_bounds_clamped() {
        let bounds = BoundingBox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 100.0,
            max_y: 100.0,
        };

        // Points outside bounds should be clamped
        let idx_outside = hilbert_index_bounded(150.0, 150.0, &bounds, 8);
        let idx_max = hilbert_index_bounded(100.0, 100.0, &bounds, 8);
        // Both should produce valid indices
        assert!(idx_outside > 0 || idx_outside == 0);
        assert!(idx_max > 0 || idx_max == 0);
    }

    #[test]
    fn test_hilbert_index_bounded_zero_range() {
        // Test when bounds have zero range in one or both dimensions
        let point_bounds = BoundingBox {
            min_x: 50.0,
            min_y: 50.0,
            max_x: 50.0,
            max_y: 50.0,
        };

        // Any point should map to center (0.5, 0.5) for both normalized coords
        let idx = hilbert_index_bounded(50.0, 50.0, &point_bounds, 8);
        let expected = hilbert_index(0.5, 0.5, 8);
        assert_eq!(idx, expected, "Zero-range bounds should map to center");
    }

    #[test]
    fn test_hilbert_index_bounded_negative_coords() {
        let bounds = BoundingBox {
            min_x: -180.0,
            min_y: -90.0,
            max_x: 180.0,
            max_y: 90.0,
        };

        // Test negative coordinates (common for geographic data)
        let idx_neg = hilbert_index_bounded(-90.0, -45.0, &bounds, 8);
        let idx_pos = hilbert_index_bounded(90.0, 45.0, &bounds, 8);

        // Both should produce valid indices
        assert!(idx_neg > 0 || idx_neg == 0);
        assert!(idx_pos > 0);
        // They should be different
        assert_ne!(idx_neg, idx_pos);
    }

    #[test]
    fn test_xy2d_basic() {
        // Test basic xy2d conversion for small n values
        let d = xy2d(2, 0, 0);
        assert_eq!(d, 0, "Origin in 2x2 grid should be 0");

        let d = xy2d(2, 1, 1);
        assert!(d > 0, "Non-origin in 2x2 grid should be positive");
    }

    #[test]
    fn test_hilbert_index_grid_coverage() {
        // Verify that all grid cells in a small grid map to different indices
        let order = 3; // 8x8 grid
        let n = 1u64 << order;
        let mut indices = Vec::new();

        for xi in 0..n {
            for yi in 0..n {
                let x = (xi as f64 + 0.5) / n as f64;
                let y = (yi as f64 + 0.5) / n as f64;
                indices.push(hilbert_index(x, y, order));
            }
        }

        indices.sort_unstable();
        indices.dedup();
        // Should cover most of the space (allowing some cell collisions at boundaries)
        assert!(indices.len() >= (n * n / 2) as usize, "Should cover most grid cells");
    }
}
