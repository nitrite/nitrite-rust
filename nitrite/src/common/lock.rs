use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::sync::Arc;

/// A handle to a read-write lock that can be stored and reused
pub struct LockHandle {
    lock: Arc<RwLock<()>>,
}

impl LockHandle {
    /// Creates a new lock handle.
    pub fn new() -> Self {
        LockHandle {
            lock: Arc::new(RwLock::new(())),
        }
    }

    /// Acquires a read lock
    pub fn read(&self) -> RwLockReadGuard<'_, ()> {
        self.lock.read()
    }

    /// Acquires a write lock
    pub fn write(&self) -> RwLockWriteGuard<'_, ()> {
        self.lock.write()
    }
}

/// Registry for managing named read-write locks.
///
/// This registry provides a way to create and manage named read-write locks that can be used
/// to synchronize access to resources. It's similar to the Java implementation
/// using `ReentrantReadWriteLock`.
///
/// This implementation uses `parking_lot`'s poison-free locks for better performance
/// and ergonomics compared to the standard library locks.
///
/// # Examples
///
/// ```
/// use nitrite::common::LockRegistry;
/// let lock_registry = LockRegistry::new();
/// let lock = lock_registry.get_lock("resource1");
/// {
/// let _read_guard = lock.read();
/// } // Read lock is held while _read_guard is in scope
/// {
/// let _write_guard = lock.write();
/// } // Write lock is held while _write_guard is in scope
/// ```
#[derive(Clone)]
pub struct LockRegistry {
    locks: Arc<RwLock<HashMap<String, Arc<RwLock<()>>>>>,
}

impl LockRegistry {
    /// Creates a new empty lock registry.
    ///
    /// # Examples
    ///
    /// ```
    /// use nitrite::common::LockRegistry;
    /// let lock_registry = LockRegistry::new();
    /// ```
    pub fn new() -> Self {
        LockRegistry {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Gets a lock for the given name.
    ///
    /// If a lock with the given name doesn't exist, creates a new one.
    /// Multiple read locks can be held simultaneously for the same resource.
    /// Only one write lock can be held at a time for a resource.
    /// No read locks can be held when a write lock is acquired.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lock
    ///
    /// # Returns
    ///
    /// A lock handle that can be used to acquire read or write locks
    ///
    /// # Examples
    ///
    /// ```
    /// use nitrite::common::LockRegistry;
    /// let lock_registry = LockRegistry::new();
    /// let handle = lock_registry.get_lock("resource1");
    /// {
    /// let _read_guard = handle.read();
    /// } // Read lock is held while _read_guard is in scope
    /// 
    /// let handle = lock_registry.get_lock("resource1");
    /// {
    /// let _write_guard = handle.write();
    /// } // Write lock is held while _write_guard is in scope
    /// ```
    pub fn get_lock(&self, name: &str) -> LockHandle {
        let lock = {
            let mut locks = self.locks.write();
            locks
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(RwLock::new(())))
                .clone()
        };
        LockHandle { lock }
    }

    /// Removes a lock from the registry if it's no longer needed.
    ///
    /// This is useful for cleaning up locks that are no longer in use.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the lock to remove
    ///
    /// # Returns
    ///
    /// `true` if the lock was removed, `false` if it didn't exist
    pub fn remove_lock(&self, name: &str) -> bool {
        let mut locks = self.locks.write();
        locks.remove(name).is_some()
    }

    /// Returns the number of locks currently registered.
    ///
    /// # Returns
    ///
    /// The count of locks in the registry
    pub fn lock_count(&self) -> usize {
        let locks = self.locks.read();
        locks.len()
    }
}

impl Default for LockRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc as StdArc;
    use std::thread;

    #[test]
    fn test_new_lock_registry() {
        let lock_registry = LockRegistry::new();
        assert_eq!(lock_registry.lock_count(), 0);
    }

    #[test]
    fn test_get_lock() {
        let lock_registry = LockRegistry::new();
        let _handle = lock_registry.get_lock("resource1");
        let _read_guard = _handle.read();
        assert_eq!(lock_registry.lock_count(), 1);
    }

    #[test]
    fn test_get_lock_write() {
        let lock_registry = LockRegistry::new();
        let _handle = lock_registry.get_lock("resource1");
        let _write_guard = _handle.write();
        assert_eq!(lock_registry.lock_count(), 1);
    }

    #[test]
    fn test_multiple_read_locks_same_name() {
        let lock_registry = StdArc::new(LockRegistry::new());
        let counter = StdArc::new(AtomicUsize::new(0));

        let mut handles = vec![];
        for _i in 0..3 {
            let registry = lock_registry.clone();
            let cnt = counter.clone();

            let handle = thread::spawn(move || {
                let lock_handle = registry.get_lock("resource1");
                let _read_guard = lock_handle.read();
                cnt.fetch_add(1, Ordering::SeqCst);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 3);
        assert_eq!(lock_registry.lock_count(), 1);
    }

    #[test]
    fn test_remove_lock() {
        let lock_registry = LockRegistry::new();
        let _handle = lock_registry.get_lock("resource1");
        let _read_guard = _handle.read();
        assert_eq!(lock_registry.lock_count(), 1);

        let removed = lock_registry.remove_lock("resource1");
        assert!(removed);
        assert_eq!(lock_registry.lock_count(), 0);
    }

    #[test]
    fn test_remove_nonexistent_lock() {
        let lock_registry = LockRegistry::new();
        let removed = lock_registry.remove_lock("nonexistent");
        assert!(!removed);
    }

    #[test]
    fn test_default() {
        let lock_registry = LockRegistry::default();
        assert_eq!(lock_registry.lock_count(), 0);
    }
}
