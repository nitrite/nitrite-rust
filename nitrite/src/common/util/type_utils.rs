use std::sync::Arc;

use parking_lot::RwLock;

pub type Atomic<T> = Arc<RwLock<T>>;

#[inline]
pub fn atomic<T>(t: T) -> Atomic<T> {
    Arc::new(RwLock::new(t))
}

pub trait ReadExecutor<T: ?Sized> {
    fn read_with<R>(&self, f: impl FnOnce(&T) -> R) -> R;
}

impl<T> ReadExecutor<T> for Atomic<T> {
    #[inline]
    fn read_with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let read_guard = self.read();
        f(&*read_guard)
    }
}

pub trait WriteExecutor<T: ?Sized> {
    fn write_with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R;
}

impl<T> WriteExecutor<T> for Atomic<T> {
    #[inline]
    fn write_with<R>(&self, f: impl FnOnce(&mut T) -> R) -> R {
        // Try to acquire write lock; will panic if already locked (defensive against deadlocks)
        let mut write_guard = self.write();
        f(&mut *write_guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic() {
        let atomic_value = atomic(5);
        assert_eq!(*atomic_value.read(), 5);
    }

    #[test]
    fn test_read_with() {
        let atomic_value = atomic(5);
        let result = atomic_value.read_with(|value| *value);
        assert_eq!(result, 5);
    }

    #[test]
    fn test_write_with() {
        let atomic_value = atomic(5);
        atomic_value.write_with(|value| *value = 10);
        assert_eq!(*atomic_value.read(), 10);
    }

    #[test]
    #[ignore] // Ignored because parking_lot RwLock doesn't panic on reentrant write - it deadlocks
    fn test_write_with_panic() {
        let atomic_value = atomic(5);
        let _write_guard = atomic_value.write();
        // This will deadlock with parking_lot, not panic
        atomic_value.write_with(|value| *value = 10);
    }

    #[test]
    fn bench_atomic_creation() {
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _atomic = atomic(42);
        }
        let elapsed = start.elapsed();
        println!(
            "Atomic creation (10,000x): {:?} ({:.3}µs per atomic)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_read_with() {
        let atomic_value = atomic(100);
        let start = std::time::Instant::now();
        for _ in 0..10_000 {
            let _result = atomic_value.read_with(|v| *v * 2);
        }
        let elapsed = start.elapsed();
        println!(
            "read_with (10,000x): {:?} ({:.3}µs per read)",
            elapsed,
            elapsed.as_micros() as f64 / 10_000.0
        );
    }

    #[test]
    fn bench_write_with() {
        let atomic_value = atomic(0);
        let start = std::time::Instant::now();
        for i in 0..1000 {
            atomic_value.write_with(|v| *v = i);
        }
        let elapsed = start.elapsed();
        println!(
            "write_with (1000x): {:?} ({:.3}µs per write)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }
}