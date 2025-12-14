use fjall::compaction::Strategy;
use fjall::{CompressionType, Config, KvSeparationOptions, PartitionCreateOptions};
use nitrite::common::{atomic, Atomic, ReadExecutor, WriteExecutor};
use nitrite::store::{StoreConfigProvider, StoreEventListener};
use std::any::Any;
use std::sync::atomic::{
    AtomicBool, AtomicI8, AtomicU16, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
/// Fjall database configuration wrapper.
///
/// A cloneable, thread-safe configuration holder for Fjall database parameters. Uses PIMPL
/// pattern with `Arc<FjallConfigInner>` to enable efficient cloning and shared configuration.
///
/// Purpose: Centralizes all Fjall tuning parameters in a single interface that can be
/// shared across threads without locks. Each parameter uses atomic types for wait-free
/// concurrent access.
///
/// Characteristics:
/// - Thread-safe (all mutations use atomic operations)
/// - Cloneable (Arc-based, cheap clones)
/// - Atomic access (no mutex locks required)
/// - Memory-efficient (shared underlying data)
/// - Implements StoreConfigProvider trait for integration
///
/// Usage: Create via `FjallConfig::new()`, then configure via setter methods before
/// passing to FjallStore initialization.
pub struct FjallConfig {
    inner: Arc<FjallConfigInner>,
}

impl FjallConfig {
    /// Creates a new Fjall configuration with default values.
    ///
    /// The configuration uses sensible defaults optimized for typical database workloads:
    /// - Block cache: 64 MB (recommended 20-25% of available memory)
    /// - Blob cache: 32 MB (for large value storage)
    /// - Write buffer: 128 MB (for better batch write throughput)
    /// - Max journaling size: 512 MB (transaction journal limit)
    /// - Bloom filter: enabled with 10 bits per key (efficient key lookups)
    /// - Compression: LZ4 (fast compression with good ratio)
    /// - Flush workers: number of available CPU cores
    /// - Compaction workers: half of available CPU cores
    ///
    /// Returns: A new `FjallConfig` instance with default settings
    #[inline]
    pub fn new() -> FjallConfig {
        FjallConfig {
            inner: Arc::new(FjallConfigInner::new()),
        }
    }

    /// Builds a Fjall Keyspace configuration from this config.
    ///
    /// This method translates Nitrite config settings to Fjall's native config format,
    /// enabling control over database parameters like cache sizes, compression, and
    /// journal persistence behavior.
    ///
    /// Returns: A configured `fjall::Config` ready for keyspace initialization
    #[inline]
    pub(crate) fn keyspace_config(&self) -> Config {
        let mut config = Config::new(self.inner.db_path());
        config = config
            .manual_journal_persist(self.inner.manual_journal_persist())
            .flush_workers(self.inner.flush_workers())
            .compaction_workers(self.inner.compaction_workers())
            .cache_size(self.inner.block_cache_capacity() + self.inner.blob_cache_capacity())
            .max_journaling_size(self.inner.max_journaling_size())
            .max_write_buffer_size(self.inner.max_write_buffer_size());

        if self.inner.fsync_frequency() > 0 {
            config = config.fsync_ms(Some(self.inner.fsync_frequency()));
        }
        config
    }

    /// Builds a Fjall Partition configuration from this config.
    ///
    /// Creates partition-level settings including bloom filters, compression strategy,
    /// memtable sizing, block size, and optional key-value separation for handling
    /// large values efficiently.
    ///
    /// Returns: A configured `PartitionCreateOptions` for partition creation
    #[inline]
    pub(crate) fn partition_config(&self) -> PartitionCreateOptions {
        let mut config = PartitionCreateOptions::default();
        config = config
            .bloom_filter_bits(if self.inner.bloom_filter_bits() == -1 {
                None
            } else {
                Some(self.inner.bloom_filter_bits() as u8)
            })
            .compression(self.inner.compression_type())
            .compaction_strategy(self.inner.compaction_strategy())
            .max_memtable_size(self.inner.max_memtable_size())
            .block_size(self.inner.block_size());

        if self.inner.kv_separated() {
            config = config.with_kv_separation(KvSeparationOptions::default());
        }
        config
    }

    /// Public delegation methods to access FjallConfigInner
    #[inline]
    pub fn db_path(&self) -> &str {
        self.inner.db_path()
    }

    /// Sets the database file path.
    #[inline]
    pub(crate) fn set_db_path(&self, db_path: &str) {
        self.inner.set_db_path(db_path)
    }

    /// Returns manual journal persist setting.
    #[inline]
    pub fn manual_journal_persist(&self) -> bool {
        self.inner.manual_journal_persist()
    }

    /// Sets manual journal persistence.
    #[inline]
    pub(crate) fn set_manual_journal_persist(&self, v: bool) {
        self.inner.set_manual_journal_persist(v)
    }

    /// Returns flush workers count.
    #[inline]
    pub fn flush_workers(&self) -> usize {
        self.inner.flush_workers()
    }

    /// Sets flush workers count.
    #[inline]
    pub(crate) fn set_flush_workers(&self, c: usize) {
        self.inner.set_flush_workers(c)
    }

    /// Returns compaction workers count.
    #[inline]
    pub fn compaction_workers(&self) -> usize {
        self.inner.compaction_workers()
    }

    /// Sets compaction workers count.
    #[inline]
    pub(crate) fn set_compaction_workers(&self, c: usize) {
        self.inner.set_compaction_workers(c)
    }

    /// Returns block cache capacity.
    #[inline]
    pub fn block_cache_capacity(&self) -> u64 {
        self.inner.block_cache_capacity()
    }

    /// Sets block cache capacity.
    #[inline]
    pub(crate) fn set_block_cache_capacity(&self, c: u64) {
        self.inner.set_block_cache_capacity(c)
    }

    /// Returns blob cache capacity.
    #[inline]
    pub fn blob_cache_capacity(&self) -> u64 {
        self.inner.blob_cache_capacity()
    }

    /// Sets blob cache capacity.
    #[inline]
    pub(crate) fn set_blob_cache_capacity(&self, c: u64) {
        self.inner.set_blob_cache_capacity(c)
    }

    /// Returns max journaling size.
    #[inline]
    pub fn max_journaling_size(&self) -> u64 {
        self.inner.max_journaling_size()
    }

    /// Sets max journaling size.
    #[inline]
    pub(crate) fn set_max_journaling_size(&self, s: u64) {
        self.inner.set_max_journaling_size(s)
    }

    /// Returns max write buffer size.
    #[inline]
    pub fn max_write_buffer_size(&self) -> u64 {
        self.inner.max_write_buffer_size()
    }

    /// Sets max write buffer size.
    #[inline]
    pub(crate) fn set_max_write_buffer_size(&self, s: u64) {
        self.inner.set_max_write_buffer_size(s)
    }

    /// Returns fsync frequency.
    #[inline]
    pub fn fsync_frequency(&self) -> u16 {
        self.inner.fsync_frequency()
    }

    /// Sets fsync frequency.
    #[inline]
    pub(crate) fn set_fsync_frequency(&self, f: u16) {
        self.inner.set_fsync_frequency(f)
    }

    /// Returns event listeners.
    #[inline]
    pub fn event_listeners(&self) -> Vec<StoreEventListener> {
        self.inner.event_listeners()
    }

    /// Adds event listener.
    #[inline]
    pub(crate) fn add_event_listener(&self, listener: StoreEventListener) {
        self.inner.add_event_listener(listener)
    }

    /// Returns commit_before_close setting.
    #[inline]
    pub fn commit_before_close(&self) -> bool {
        self.inner.commit_before_close()
    }

    /// Sets commit_before_close setting.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_commit_before_close(&self, v: bool) {
        self.inner.set_commit_before_close(v)
    }

    /// Returns bloom filter bits.
    #[inline]
    pub fn bloom_filter_bits(&self) -> i8 {
        self.inner.bloom_filter_bits()
    }

    /// Sets bloom filter bits.
    #[inline]
    pub(crate) fn set_bloom_filter_bits(&self, b: i8) {
        self.inner.set_bloom_filter_bits(b)
    }

    /// Returns compression type.
    #[inline]
    pub fn compression_type(&self) -> CompressionType {
        self.inner.compression_type()
    }

    /// Sets compression type.
    #[inline]
    pub(crate) fn set_compression_type(&self, ct: CompressionType) {
        self.inner.set_compression_type(ct)
    }

    /// Returns compaction strategy.
    #[inline]
    pub fn compaction_strategy(&self) -> Strategy {
        self.inner.compaction_strategy()
    }

    /// Sets compaction strategy.
    #[inline]
    pub(crate) fn set_compaction_strategy(&self, s: Strategy) {
        self.inner.set_compaction_strategy(s)
    }

    /// Returns max memtable size.
    #[inline]
    pub fn max_memtable_size(&self) -> u32 {
        self.inner.max_memtable_size()
    }

    /// Sets max memtable size.
    #[inline]
    pub(crate) fn set_max_memtable_size(&self, s: u32) {
        self.inner.set_max_memtable_size(s)
    }

    /// Returns block size.
    #[inline]
    pub fn block_size(&self) -> u32 {
        self.inner.block_size()
    }

    /// Sets block size.
    #[inline]
    pub(crate) fn set_block_size(&self, s: u32) {
        self.inner.set_block_size(s)
    }

    /// Returns if KV separated.
    #[inline]
    pub fn kv_separated(&self) -> bool {
        self.inner.kv_separated()
    }

    /// Sets KV separation.
    #[inline]
    pub(crate) fn set_kv_separated(&self, v: bool) {
        self.inner.set_kv_separated(v)
    }

    /// Returns space amplification factor.
    #[inline]
    pub fn space_amp_factor(&self) -> f32 {
        self.inner.space_amp_factor()
    }

    /// Sets space amplification factor.
    #[inline]
    pub(crate) fn set_space_amp_factor(&self, f: f32) {
        self.inner.set_space_amp_factor(f)
    }

    /// Returns staleness threshold.
    #[inline]
    pub fn staleness_threshold(&self) -> f32 {
        self.inner.staleness_threshold()
    }

    /// Sets staleness threshold.
    #[inline]
    pub(crate) fn set_staleness_threshold(&self, t: f32) {
        self.inner.set_staleness_threshold(t)
    }
}

impl StoreConfigProvider for FjallConfig {
    /// Returns the database file path.
    ///
    /// Returns: String path to the database file
    fn file_path(&self) -> String {
        self.db_path().to_string()
    }

    /// Indicates if this store is read-only.
    ///
    /// Fjall adapter always supports writes, so this always returns false.
    ///
    /// Returns: `false` (Fjall is always read-write capable)
    fn is_read_only(&self) -> bool {
        false
    }

    /// Registers a listener for store events.
    ///
    /// Arguments:
    /// - `listener`: Event listener callback to register
    fn add_store_listener(&self, listener: StoreEventListener) {
        self.add_event_listener(listener)
    }

    /// Returns this config as an Any trait object for dynamic type checking.
    ///
    /// Returns: Reference to self as `&dyn Any`
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Internal configuration storage for Fjall database parameters.
///
/// This struct encapsulates all tunable Fjall database settings using atomic types for
/// thread-safe read-write access without locks. It manages:
/// - Path and persistence settings
/// - Memory cache configuration (block and blob caches)
/// - Journal and write buffer sizing
/// - Compression and compaction strategies
/// - Event listener registration
/// - Bloom filter and memtable tuning
///
/// Thread-safe: All fields use atomic types (AtomicBool, AtomicU64, etc.) or Atomic<T>
/// for efficient concurrent access without mutex overhead.
struct FjallConfigInner {
    db_path: OnceLock<String>,
    manual_journal_persist: AtomicBool,
    flush_workers_count: AtomicUsize,
    compaction_workers_count: AtomicUsize,
    block_cache_capacity: AtomicU64,
    blob_cache_capacity: AtomicU64,
    max_journaling_size: AtomicU64,
    max_write_buffer_size: AtomicU64,
    fsync_frequency: AtomicU16,
    event_listeners: Atomic<Vec<StoreEventListener>>,
    commit_before_close: AtomicBool,

    bloom_filter_bits: AtomicI8,
    compression_type: Atomic<CompressionType>,
    compaction_strategy: Atomic<Strategy>,
    max_memtable_size: AtomicU32,
    block_size: AtomicU32,
    kv_separated: AtomicBool,
    space_amp_factor: Atomic<f32>,
    staleness_threshold: Atomic<f32>,
}

impl FjallConfigInner {
    /// Default block cache size: 64 MB. Fjall recommends 20-25% of available memory.
    pub const DEFAULT_BLOCK_CACHE_MB: u64 = 64;

    /// Default blob cache size: 32 MB for large value storage
    pub const DEFAULT_BLOB_CACHE_MB: u64 = 32;

    /// Default write buffer size: 128 MB for better batch write throughput
    pub const DEFAULT_WRITE_BUFFER_MB: u64 = 128;

    /// Default max journaling size: 512 MB (transaction journal limit)
    pub const DEFAULT_MAX_JOURNALING_MB: u64 = 512;

    /// Default memtable size: 32 MB for in-memory write batching
    pub const DEFAULT_MEMTABLE_MB: u32 = 32;

    /// Creates a new FjallConfigInner with all default settings.
    ///
    /// Initializes atomic fields with defaults and auto-detects CPU count for worker threads:
    /// - Flush workers: all available CPU cores
    /// - Compaction workers: half of available CPU cores
    fn new() -> FjallConfigInner {
        let queried_cores = std::thread::available_parallelism().map(usize::from);

        // Use all available cores for flush workers (excluding 1 for main thread)
        let cpus = queried_cores.unwrap_or(4);
        let flush_workers = cpus.max(1);
        // Use half of cores for compaction workers to leave room for other work
        let compaction_workers = (cpus / 2).max(1);

        FjallConfigInner {
            db_path: OnceLock::new(),
            manual_journal_persist: AtomicBool::new(false),
            flush_workers_count: AtomicUsize::new(flush_workers),
            compaction_workers_count: AtomicUsize::new(compaction_workers),
            // Increased cache sizes for better production performance
            block_cache_capacity: AtomicU64::new(Self::DEFAULT_BLOCK_CACHE_MB * 1_024 * 1_024),
            blob_cache_capacity: AtomicU64::new(Self::DEFAULT_BLOB_CACHE_MB * 1_024 * 1_024),
            max_journaling_size: AtomicU64::new(Self::DEFAULT_MAX_JOURNALING_MB * 1_024 * 1_024),
            max_write_buffer_size: AtomicU64::new(Self::DEFAULT_WRITE_BUFFER_MB * 1_024 * 1_024),
            fsync_frequency: AtomicU16::new(0),
            event_listeners: atomic(Vec::new()),
            commit_before_close: AtomicBool::new(true),
            // Enable bloom filter by default with 10 bits per key for efficient lookups
            bloom_filter_bits: AtomicI8::new(10),
            compression_type: atomic(CompressionType::Lz4),
            compaction_strategy: atomic(Strategy::default()),
            // Increased memtable size for better write batching
            max_memtable_size: AtomicU32::new(Self::DEFAULT_MEMTABLE_MB * 1_024 * 1_024),
            block_size: AtomicU32::new(4 * 1_024),
            kv_separated: AtomicBool::new(false),
            space_amp_factor: atomic(1.5),
            staleness_threshold: atomic(0.8),
        }
    }

    #[inline]
    pub fn db_path(&self) -> &str {
        self.db_path.get_or_init(|| "".to_string()).as_str()
    }

    #[inline]
    pub(crate) fn set_db_path(&self, db_path: &str) {
        self.db_path.get_or_init(|| db_path.to_string());
    }

    #[inline]
    pub fn manual_journal_persist(&self) -> bool {
        self.manual_journal_persist.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_manual_journal_persist(&self, manual_journal_persist: bool) {
        self.manual_journal_persist
            .store(manual_journal_persist, Ordering::Relaxed)
    }

    #[inline]
    pub fn flush_workers(&self) -> usize {
        self.flush_workers_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_flush_workers(&self, flush_workers_count: usize) {
        self.flush_workers_count
            .store(flush_workers_count, Ordering::Relaxed)
    }

    #[inline]
    pub fn compaction_workers(&self) -> usize {
        self.compaction_workers_count.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_compaction_workers(&self, compaction_workers_count: usize) {
        self.compaction_workers_count
            .store(compaction_workers_count, Ordering::Relaxed)
    }

    #[inline]
    pub fn block_cache_capacity(&self) -> u64 {
        self.block_cache_capacity.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_block_cache_capacity(&self, block_cache_capacity: u64) {
        self.block_cache_capacity
            .store(block_cache_capacity, Ordering::Relaxed)
    }

    #[inline]
    pub fn blob_cache_capacity(&self) -> u64 {
        self.blob_cache_capacity.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_blob_cache_capacity(&self, blob_cache_capacity: u64) {
        self.blob_cache_capacity
            .store(blob_cache_capacity, Ordering::Relaxed)
    }

    #[inline]
    pub fn max_journaling_size(&self) -> u64 {
        self.max_journaling_size.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_max_journaling_size(&self, max_journaling_size: u64) {
        self.max_journaling_size
            .store(max_journaling_size, Ordering::Relaxed)
    }

    #[inline]
    pub fn max_write_buffer_size(&self) -> u64 {
        self.max_write_buffer_size.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_max_write_buffer_size(&self, max_write_buffer_size: u64) {
        self.max_write_buffer_size
            .store(max_write_buffer_size, Ordering::Relaxed)
    }

    #[inline]
    pub fn fsync_frequency(&self) -> u16 {
        self.fsync_frequency.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_fsync_frequency(&self, fsync_frequency: u16) {
        self.fsync_frequency
            .store(fsync_frequency, Ordering::Relaxed)
    }

    #[inline]
    pub fn event_listeners(&self) -> Vec<StoreEventListener> {
        self.event_listeners.read_with(|it| it.clone())
    }

    #[inline]
    pub(crate) fn add_event_listener(&self, listener: StoreEventListener) {
        self.event_listeners.write_with(|it| it.push(listener))
    }

    #[inline]
    pub fn commit_before_close(&self) -> bool {
        self.commit_before_close.load(Ordering::Relaxed)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn set_commit_before_close(&self, commit_before_close: bool) {
        self.commit_before_close
            .store(commit_before_close, Ordering::Relaxed)
    }

    #[inline]
    pub fn bloom_filter_bits(&self) -> i8 {
        self.bloom_filter_bits.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_bloom_filter_bits(&self, bloom_filter_bits: i8) {
        self.bloom_filter_bits
            .store(bloom_filter_bits, Ordering::Relaxed)
    }

    #[inline]
    pub fn compression_type(&self) -> CompressionType {
        self.compression_type.read_with(|it| it.clone())
    }

    #[inline]
    pub(crate) fn set_compression_type(&self, compression_type: CompressionType) {
        self.compression_type
            .write_with(|it| *it = compression_type)
    }

    #[inline]
    pub fn compaction_strategy(&self) -> Strategy {
        self.compaction_strategy.read_with(|it| it.clone())
    }

    #[inline]
    pub(crate) fn set_compaction_strategy(&self, compaction_strategy: Strategy) {
        self.compaction_strategy
            .write_with(|it| *it = compaction_strategy)
    }

    #[inline]
    pub fn max_memtable_size(&self) -> u32 {
        self.max_memtable_size.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_max_memtable_size(&self, max_memtable_size: u32) {
        self.max_memtable_size
            .store(max_memtable_size, Ordering::Relaxed)
    }

    #[inline]
    pub fn block_size(&self) -> u32 {
        self.block_size.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_block_size(&self, block_size: u32) {
        self.block_size.store(block_size, Ordering::Relaxed)
    }

    #[inline]
    pub fn kv_separated(&self) -> bool {
        self.kv_separated.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn set_kv_separated(&self, kv_separated: bool) {
        self.kv_separated.store(kv_separated, Ordering::Relaxed)
    }

    #[inline]
    pub fn space_amp_factor(&self) -> f32 {
        self.space_amp_factor.read_with(|it| it.clone())
    }

    #[inline]
    pub(crate) fn set_space_amp_factor(&self, space_amp_factor: f32) {
        self.space_amp_factor
            .write_with(|it| *it = space_amp_factor)
    }

    #[inline]
    pub fn staleness_threshold(&self) -> f32 {
        self.staleness_threshold.read_with(|it| it.clone())
    }

    #[inline]
    pub(crate) fn set_staleness_threshold(&self, staleness_threshold: f32) {
        self.staleness_threshold
            .write_with(|it| *it = staleness_threshold)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::CompressionType;
    use nitrite::store::StoreEventListener;

    #[ctor::ctor]
    fn init() {
        colog::init();
    }

    #[inline(never)]
    fn black_box<T>(x: T) -> T {
        x
    }

    #[test]
    fn test_fjall_config_new() {
        let config = FjallConfig::new();
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);

        assert_eq!(config.db_path(), "");
        assert_eq!(config.manual_journal_persist(), false);
        // New defaults: use all CPUs for flush, half for compaction
        assert_eq!(config.flush_workers(), cpus.max(1));
        assert_eq!(config.compaction_workers(), (cpus / 2).max(1));
        // New defaults: 64 MB block cache, 32 MB blob cache
        assert_eq!(config.block_cache_capacity(), 64 * 1_024 * 1_024);
        assert_eq!(config.blob_cache_capacity(), 32 * 1_024 * 1_024);
        assert_eq!(config.max_journaling_size(), 512 * 1_024 * 1_024);
        // New default: 128 MB write buffer
        assert_eq!(config.max_write_buffer_size(), 128 * 1_024 * 1_024);
        assert_eq!(config.fsync_frequency(), 0);
        assert_eq!(config.commit_before_close(), true);
        // New default: bloom filter enabled with 10 bits
        assert_eq!(config.bloom_filter_bits(), 10);
        assert_eq!(config.compression_type(), CompressionType::Lz4);
        // New default: 32 MB memtable
        assert_eq!(config.max_memtable_size(), 32 * 1_024 * 1_024);
        assert_eq!(config.block_size(), 4 * 1_024);
        assert_eq!(config.kv_separated(), false);
        assert_eq!(config.space_amp_factor(), 1.5);
        assert_eq!(config.staleness_threshold(), 0.8);
    }

    #[test]
    fn test_setters_and_getters() {
        let config = FjallConfig::new();
        config.set_db_path("test_path");
        assert_eq!(config.db_path(), "test_path");

        config.set_manual_journal_persist(true);
        assert_eq!(config.manual_journal_persist(), true);

        config.set_flush_workers(8);
        assert_eq!(config.flush_workers(), 8);

        config.set_compaction_workers(8);
        assert_eq!(config.compaction_workers(), 8);

        config.set_block_cache_capacity(32 * 1_024 * 1_024);
        assert_eq!(config.block_cache_capacity(), 32 * 1_024 * 1_024);

        config.set_blob_cache_capacity(32 * 1_024 * 1_024);
        assert_eq!(config.blob_cache_capacity(), 32 * 1_024 * 1_024);

        config.set_max_journaling_size(1_024 * 1_024);
        assert_eq!(config.max_journaling_size(), 1_024 * 1_024);

        config.set_max_write_buffer_size(128 * 1_024 * 1_024);
        assert_eq!(config.max_write_buffer_size(), 128 * 1_024 * 1_024);

        config.set_fsync_frequency(100);
        assert_eq!(config.fsync_frequency(), 100);

        let listener = StoreEventListener::new(|_| Ok(()));
        config.add_event_listener(listener.clone());
        assert_eq!(config.event_listeners().len(), 1);

        config.set_commit_before_close(false);
        assert_eq!(config.commit_before_close(), false);

        config.set_bloom_filter_bits(10);
        assert_eq!(config.bloom_filter_bits(), 10);

        config.set_compression_type(CompressionType::Lz4);
        assert_eq!(config.compression_type(), CompressionType::Lz4);

        config.set_max_memtable_size(32 * 1_024 * 1_024);
        assert_eq!(config.max_memtable_size(), 32 * 1_024 * 1_024);

        config.set_block_size(8 * 1_024);
        assert_eq!(config.block_size(), 8 * 1_024);

        config.set_kv_separated(false);
        assert_eq!(config.kv_separated(), false);

        config.set_space_amp_factor(2.0);
        assert_eq!(config.space_amp_factor(), 2.0);

        config.set_staleness_threshold(0.9);
        assert_eq!(config.staleness_threshold(), 0.9);
    }

    #[test]
    fn test_config_creation_perf() {
        for _ in 0..1000 {
            let config = black_box(FjallConfig::new());
            black_box(config);
        }
    }

    #[test]
    fn test_config_atomic_getters_perf() {
        let config = FjallConfig::new();
        for _ in 0..10000 {
            black_box(config.flush_workers());
            black_box(config.compaction_workers());
            black_box(config.block_cache_capacity());
            black_box(config.blob_cache_capacity());
        }
    }

    #[test]
    fn test_config_building_perf() {
        for _ in 0..1000 {
            let cfg = FjallConfig::new();
            cfg.set_flush_workers(4);
            cfg.set_compaction_workers(2);
            cfg.set_block_cache_capacity(256 * 1_024 * 1_024);
            cfg.set_blob_cache_capacity(64 * 1_024 * 1_024);
            black_box(cfg);
        }
    }
}
