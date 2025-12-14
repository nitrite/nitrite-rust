use crate::common::get_current_time_or_zero;
use log::{info, warn};
use rand::rngs::OsRng;
use rand::Rng;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;

pub struct SnowflakeIdGenerator {
    node_id: u64,
    sequence: AtomicU64,
    last_timestamp: AtomicU64,
    sequence_bits: u64,
    sequence_mask: u64,
    timestamp_left_shift: u64,
    epoch: u64,
    mutex: Mutex<()>,
}

impl SnowflakeIdGenerator {
    pub fn new() -> Self {
        let node_id_bits = 10;
        let sequence_bits = 12;
        let max_node_id = !0_u64 << node_id_bits;
        let sequence_mask = !0_u64 << sequence_bits;
        let timestamp_left_shift = sequence_bits + node_id_bits;
        let epoch = 1288834974657;

        let mut generator = SnowflakeIdGenerator {
            node_id: 0,
            sequence: AtomicU64::new(0),
            last_timestamp: AtomicU64::new(0),
            sequence_bits,
            sequence_mask,
            timestamp_left_shift,
            epoch,
            mutex: Mutex::new(()),
        };

        generator.node_id = generator.get_node_id();
        if generator.node_id > max_node_id {
            warn!("Node id can't be greater than {}", max_node_id);
            generator.node_id = OsRng.gen_range(1..=max_node_id);
        }
        info!("Initialized with node id: {}", generator.node_id);

        generator
    }
    
    pub fn get_id(&self) -> u64 {
        // Acquire the lock with poison recovery
        let _lock = match self.mutex.lock() {
            Ok(lock) => lock,
            Err(poisoned) => {
                warn!("Snowflake lock was poisoned, recovering");
                poisoned.into_inner()
            }
        };
        
        let current_time = get_current_time_or_zero() as u64;
        let mut timestamp = current_time;
        let last_timestamp = self.last_timestamp.load(std::sync::atomic::Ordering::Relaxed);
        let sequence = self.sequence.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        // Handle clock moving backwards with optimized branching
        if timestamp <= last_timestamp {
            timestamp = last_timestamp;
            let sleep_duration = timestamp.saturating_sub(current_time);
            if sleep_duration > 0 {
                std::thread::sleep(std::time::Duration::from_millis(sleep_duration));
            }
        }
        
        self.last_timestamp.store(timestamp, std::sync::atomic::Ordering::Relaxed);
        drop(_lock);
        
        ((timestamp - self.epoch) << self.timestamp_left_shift)
            | (self.node_id << self.sequence_bits)
            | sequence
    }

    fn get_node_id(&self) -> u64 {
        let uuid = uuid::Uuid::new_v4();
        let uid = uuid.as_bytes();
        let rnd_byte = OsRng.gen::<u64>() & 0x000000FF;

        ((0x000000FF & uid[uid.len() - 1] as u64) | (0x0000FF00 & ((rnd_byte) << 8))) >> 6
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_unique_ids() {
        let generator = SnowflakeIdGenerator::new();
        let mut ids = Vec::new();
        for _ in 0..100 {
            ids.push(generator.get_id());
        }

        let mut unique_ids = ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        assert_eq!(ids.len(), unique_ids.len());
    }

    #[test]
    fn handles_clock_backwards() {
        let mut generator = SnowflakeIdGenerator::new();
        generator.last_timestamp = AtomicU64::from(get_current_time_or_zero() as u64 + 1000);
        let id = generator.get_id();
        assert!(id > 0);
    }

    #[test]
    fn generates_id_with_correct_node_id() {
        let generator = SnowflakeIdGenerator::new();
        let id = generator.get_id();
        let node_id = (id >> generator.sequence_bits) & ((1 << 10) - 1);
        assert_eq!(node_id, generator.node_id);
    }

    #[test]
    fn generates_id_with_correct_timestamp() {
        let generator = SnowflakeIdGenerator::new();
        let id = generator.get_id();
        let timestamp = (id >> generator.timestamp_left_shift) + generator.epoch;
        assert!(timestamp >= get_current_time_or_zero() as u64);
    }

    #[test]
    fn handles_multiple_concurrent_id_generation() {
        use std::sync::Arc;
        use std::thread;

        let generator = Arc::new(SnowflakeIdGenerator::new());
        let mut handles = vec![];

        // Spawn 10 threads that each generate 100 IDs
        for _ in 0..10 {
            let gen = Arc::clone(&generator);
            let handle = thread::spawn(move || {
                let mut ids = Vec::new();
                for _ in 0..100 {
                    ids.push(gen.get_id());
                }
                ids
            });
            handles.push(handle);
        }

        // Collect all IDs from all threads
        let mut all_ids = Vec::new();
        for handle in handles {
            let ids = handle.join().unwrap();
            all_ids.extend(ids);
        }

        // Verify all IDs are unique
        let mut unique_ids = all_ids.clone();
        unique_ids.sort();
        unique_ids.dedup();
        assert_eq!(all_ids.len(), unique_ids.len());
    }

    #[test]
    fn lock_acquisition_succeeds() {
        let generator = SnowflakeIdGenerator::new();
        // This should not panic - it should successfully acquire the lock
        let id = generator.get_id();
        assert!(id > 0);

        // Multiple acquisitions should also succeed
        let id2 = generator.get_id();
        assert!(id2 > 0);
        assert!(id2 > id); // IDs should be increasing
    }

    #[test]
    fn bench_snowflake_id_generation() {
        let generator = SnowflakeIdGenerator::new();
        
        let start = std::time::Instant::now();
        for _ in 0..10000 {
            let _ = generator.get_id();
        }
        let elapsed = start.elapsed();
        
        println!("Generated 10000 IDs in {:?}", elapsed);
        assert!(elapsed.as_millis() < 500);
    }

    #[test]
    fn bench_concurrent_id_generation() {
        use std::sync::Arc;
        use std::thread;

        let generator = Arc::new(SnowflakeIdGenerator::new());
        let mut handles = vec![];

        let start = std::time::Instant::now();
        for _ in 0..10 {
            let gen = Arc::clone(&generator);
            let handle = thread::spawn(move || {
                for _ in 0..1000 {
                    let _ = gen.get_id();
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }
        let elapsed = start.elapsed();
        
        println!("10 threads x 1000 IDs in {:?}", elapsed);
        assert!(elapsed.as_millis() < 1000);
    }
}
