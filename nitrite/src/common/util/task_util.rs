use crate::SCHEDULER;
use parking_lot::Mutex;
use std::time::Duration;
use timer::{Guard, Timer};

/// Spawn an async task on a new thread.
/// This avoids global thread pool contention that can occur in parallel test runs.
pub fn async_task<OP>(op: OP)
where
    OP: FnOnce() + Send + 'static,
{
    std::thread::spawn(op);
}

#[inline]
pub fn schedule_task<F>(duration: Duration, f: F)
where
    F: 'static + FnMut() + Send,
{
    SCHEDULER.schedule(duration, f);
}

#[inline]
pub fn stop_scheduled_tasks() {
    SCHEDULER.stop();
}

pub(crate) struct Scheduler {
    timer: Timer,
    guards: Mutex<Vec<Guard>>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            timer: Timer::new(),
            // Preallocate with typical capacity to reduce allocations during task scheduling
            guards: Mutex::from(Vec::with_capacity(16)),
        }
    }

    #[inline]
    pub fn schedule<F>(&self, duration: Duration, f: F)
    where
        F: 'static + FnMut() + Send,
    {
        match chrono::Duration::from_std(duration) {
            Ok(chrono_duration) => {
                let guard = self.timer.schedule_repeating(chrono_duration, f);
                self.guards.lock().push(guard);
            }
            Err(e) => {
                log::error!("Failed to convert duration to chrono::Duration: {}, skipping task scheduling", e);
            }
        }
    }

    #[inline]
    pub fn stop(&self) {
        self.guards.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use test_retry::retry;

    #[test]
    fn test_async_task() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        async_task(move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        thread::sleep(Duration::from_millis(200));
        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    #[retry]
    fn test_schedule_task() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        schedule_task(Duration::from_millis(50), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        awaitility::at_most(Duration::from_millis(200)).until(|| {
            flag.load(Ordering::Relaxed)
        });
    }

    #[test]
    #[retry]
    fn test_stop_scheduled_tasks() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        schedule_task(Duration::from_millis(100), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        stop_scheduled_tasks();
        thread::sleep(Duration::from_millis(200));
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_scheduler_new() {
        let scheduler = Scheduler::new();
        assert!(scheduler.guards.lock().is_empty());
    }

    #[test]
    #[retry]
    fn test_scheduler_schedule() {
        let scheduler = Scheduler::new();
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        scheduler.schedule(Duration::from_millis(100), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        thread::sleep(Duration::from_millis(200));
        assert!(flag.load(Ordering::Relaxed));
    }

    #[test]
    #[retry]
    fn test_scheduler_stop() {
        let scheduler = Scheduler::new();
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        scheduler.schedule(Duration::from_millis(100), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        scheduler.stop();
        thread::sleep(Duration::from_millis(200));
        assert!(!flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_scheduler_handles_valid_duration() {
        let scheduler = Scheduler::new();
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        // Valid duration should schedule successfully
        scheduler.schedule(Duration::from_millis(50), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        // Verify guard was added (indicating successful scheduling)
        assert_eq!(scheduler.guards.lock().len(), 1);
    }

    #[test]
    fn test_scheduler_handles_maximum_safe_duration() {
        let scheduler = Scheduler::new();
        // Use a large but safe duration (1000 years = 31,536,000,000 seconds)
        let safe_max = Duration::from_secs(365 * 24 * 60 * 60 * 1000);
        
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        
        scheduler.schedule(safe_max, move || {
            flag_clone.store(true, Ordering::Relaxed);
        });
        
        // Should handle large duration without panicking
        assert_eq!(scheduler.guards.lock().len(), 1);
    }

    #[test]
    fn test_scheduler_rejects_out_of_range_duration() {
        let scheduler = Scheduler::new();
        // Create a duration that exceeds chrono's limits
        // chrono max is about i64::MAX milliseconds, so try u64::MAX seconds
        let out_of_range = Duration::from_secs(u64::MAX);
        
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        
        // This should not panic, but gracefully skip scheduling
        scheduler.schedule(out_of_range, move || {
            flag_clone.store(true, Ordering::Relaxed);
        });
        
        // Guard should not be added due to conversion failure
        assert_eq!(scheduler.guards.lock().len(), 0);
    }

    #[test]
    fn test_schedule_task_with_zero_duration() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);

        schedule_task(Duration::from_millis(0), move || {
            flag_clone.store(true, Ordering::Relaxed);
        });

        // Zero duration scheduling should execute quickly (or handle gracefully)
        thread::sleep(Duration::from_millis(50));
        // May or may not execute due to zero delay, but should not panic
    }

    #[test]
    fn bench_async_task_spawn() {
        let start = std::time::Instant::now();
        for _ in 0..100 {
            async_task(|| {
                std::thread::sleep(Duration::from_micros(1));
            });
        }
        let elapsed = start.elapsed();
        println!(
            "async_task spawn (100 tasks): {:?} ({:.3}µs per spawn)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
    }

    #[test]
    fn bench_scheduler_creation() {
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _scheduler = Scheduler::new();
        }
        let elapsed = start.elapsed();
        println!(
            "Scheduler::new (1000 instances): {:?} ({:.3}µs per instance)",
            elapsed,
            elapsed.as_micros() as f64 / 1000.0
        );
    }

    #[test]
    fn bench_scheduler_guard_storage() {
        let scheduler = Scheduler::new();
        let start = std::time::Instant::now();
        
        // Simulate adding guards (without actually scheduling to avoid wait times)
        for _ in 0..100 {
            let flag = Arc::new(AtomicBool::new(false));
            let flag_clone = Arc::clone(&flag);
            scheduler.schedule(Duration::from_secs(100), move || {
                flag_clone.store(true, Ordering::Relaxed);
            });
        }
        
        let elapsed = start.elapsed();
        println!(
            "Scheduler guard storage (100 guards): {:?} ({:.3}µs per guard)",
            elapsed,
            elapsed.as_micros() as f64 / 100.0
        );
        
        scheduler.stop();
    }
}
