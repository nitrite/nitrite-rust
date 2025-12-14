use std::time::{SystemTime, SystemTimeError, UNIX_EPOCH};

#[inline]
pub fn get_current_time() -> Result<u128, SystemTimeError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
}

// Fast path: returns 0 on any error instead of double error handling
#[inline]
pub fn get_current_time_or_zero() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_current_time() {
        let current_time = get_current_time_or_zero();
        // Check if the current time is a positive number
        assert!(current_time > 0);
    }

    #[test]
    fn test_get_current_time_result_ok() {
        let result = get_current_time();
        assert!(result.is_ok());
        assert!(result.unwrap() > 0);
    }

    #[test]
    fn bench_get_current_time_or_zero() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = get_current_time_or_zero();
        }
        let elapsed = start.elapsed();
        println!(
            "get_current_time_or_zero (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_get_current_time_result() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _ = get_current_time();
        }
        let elapsed = start.elapsed();
        println!(
            "get_current_time (10,000 calls): {:?} ({:.3}µs per call)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }
}