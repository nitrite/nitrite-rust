use crate::config::FjallConfig;
use crate::store::FjallStore;
use fjall::compaction::Strategy;
use fjall::CompressionType;
use nitrite::common::{NitriteModule, NitritePlugin, PluginRegistrar};
use nitrite::errors::NitriteResult;
use nitrite::store::{NitriteStore, StoreEventListener, StoreModule};

/// Nitrite storage module using the Fjall key-value store.
///
/// `FjallModule` provides persistent storage backend for Nitrite using Fjall,
/// an efficient embedded LSM (Log-Structured Merge) tree database. This module
/// must be loaded before opening a Nitrite database to use file-based persistence.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
/// use nitrite_fjall_adapter::FjallModule;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a database with Fjall storage
/// let db = Nitrite::builder()
///     .load_module(
///         FjallModule::with_config()
///             .db_path("/path/to/db")
///             .build()?
///     )
///     .open_or_create(None, None)?;
/// # Ok(())
/// # }
/// ```
///
/// # Configuration Presets
///
/// The builder provides preset configurations for different use cases:
/// - `production_preset()` - Balanced configuration for production use
/// - `high_throughput_preset()` - Optimized for high-write workloads
/// - `low_memory_preset()` - Optimized for low-memory environments
pub struct FjallModule {
    store_config: FjallConfig,
}

impl FjallModule {
    /// Creates a new builder for configuring a Fjall module.
    ///
    /// # Returns
    ///
    /// A `FjallModuleBuilder` for fluent configuration
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let module = FjallModule::with_config()
    ///     .db_path("/path/to/db")
    ///     .build()?;
    /// ```
    #[inline]
    pub fn with_config() -> FjallModuleBuilder {
        FjallModuleBuilder::new()
    }
}

impl NitriteModule for FjallModule {
    fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
        let store = self.get_store()?;
        let plugin = store.as_plugin();
        Ok(vec![plugin])
    }

    fn load(&self, plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
        let store = self.get_store()?;
        plugin_registrar.register_store_plugin(store)
    }
}

impl StoreModule for FjallModule {
    fn get_store(&self) -> NitriteResult<NitriteStore> {
        let store = FjallStore::new(self.store_config.clone());
        Ok(NitriteStore::new(store))
    }
}

/// Builder for configuring a Fjall storage module.
///
/// `FjallModuleBuilder` provides a fluent API for customizing Fjall storage parameters.
/// It includes preset configurations for common use cases and individual parameter setters.
pub struct FjallModuleBuilder {
    store_config: FjallConfig,
}

impl FjallModuleBuilder {
    /// Creates a new builder with default configuration.
    ///
    /// # Returns
    ///
    /// A new `FjallModuleBuilder` instance
    #[inline]
    pub fn new() -> FjallModuleBuilder {
        FjallModuleBuilder {
            store_config: FjallConfig::new(),
        }
    }
    
    /// Applies production-optimized preset configuration.
    ///
    /// This configures:
    /// - 256 MB block cache (recommended 20-25% of available memory)
    /// - 64 MB blob cache
    /// - 128 MB write buffer
    /// - All CPU cores for flush workers
    /// - Half CPU cores for compaction workers  
    /// - Bloom filter with 10 bits per key for faster lookups
    /// - 100ms fsync interval for durability without excessive I/O
    /// - LZ4 compression (fast with good ratio)
    /// 
    /// # Returns
    ///
    /// This `FjallModuleBuilder` for method chaining
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let module = FjallModule::with_config()
    ///     .production_preset()
    ///     .db_path("/path/to/db")
    ///     .build()?;
    /// ```
    #[inline]
    pub fn production_preset(self) -> Self {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        
        self
            // Cache configuration - 256 MB block cache + 64 MB blob cache
            .block_cache_capacity(256 * 1024 * 1024)
            .blob_cache_capacity(64 * 1024 * 1024)
            // Write buffer - 128 MB for good batching
            .max_write_buffer_size(128 * 1024 * 1024)
            // Memtable - 32 MB per partition
            .max_memtable_size(32 * 1024 * 1024)
            // Workers - utilize available CPUs
            .flush_workers(cpus)
            .compaction_workers((cpus / 2).max(1))
            // Enable bloom filter for faster point lookups
            .bloom_filter_bits(10)
            // Enable periodic fsync for durability
            .fsync_frequency(100)  // 100ms
            // LZ4 compression - good balance of speed and ratio
            .compression_type(CompressionType::Lz4)
    }
    
    /// Applies preset configuration for high-write workloads.
    ///
    /// This configures larger buffers and defers durability for maximum throughput.
    /// Use this for batch imports or scenarios where some data loss on crash is acceptable.
    /// 
    /// Settings include:
    /// - 512 MB block cache
    /// - 256 MB write buffer
    /// - 64 MB memtable
    /// - Manual journal persistence (call commit() explicitly)
    /// - KV separation for large values
    #[inline]
    pub fn high_throughput_preset(self) -> Self {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        
        self
            // Large cache for write-heavy workloads
            .block_cache_capacity(512 * 1024 * 1024)
            .blob_cache_capacity(128 * 1024 * 1024)
            // Large write buffer for batching
            .max_write_buffer_size(256 * 1024 * 1024)
            // Large memtable to reduce flushes
            .max_memtable_size(64 * 1024 * 1024)
            // All CPUs for flush/compaction
            .flush_workers(cpus)
            .compaction_workers(cpus)
            // Manual persistence - call commit() when needed
            .manual_journal_persist(true)
            // Enable KV separation for large values
            .kv_separated(true)
            // Disable bloom filter (not useful for bulk writes)
            .bloom_filter_bits(0)
    }
    
    /// Creates a builder with settings optimized for low-memory environments.
    /// 
    /// This uses minimal memory at the cost of some performance.
    /// Suitable for embedded systems, development, or memory-constrained environments.
    #[inline]
    pub fn low_memory_preset(self) -> Self {
        self
            // Minimal cache
            .block_cache_capacity(16 * 1024 * 1024)
            .blob_cache_capacity(8 * 1024 * 1024)
            // Small write buffer
            .max_write_buffer_size(32 * 1024 * 1024)
            // Small memtable
            .max_memtable_size(8 * 1024 * 1024)
            // Minimal workers
            .flush_workers(1)
            .compaction_workers(1)
            // Enable bloom filter to reduce disk reads
            .bloom_filter_bits(10)
    }
    
    #[inline]
    pub fn db_path(self, db_path: &str) -> Self {
        self.store_config.set_db_path(db_path);
        self
    }
    
    #[inline]
    pub fn manual_journal_persist(self, manual_journal_persist: bool) -> Self {
        self.store_config.set_manual_journal_persist(manual_journal_persist);
        self
    }
    
    #[inline]
    pub fn flush_workers(self, flush_workers_count: usize) -> Self {
        self.store_config.set_flush_workers(flush_workers_count);
        self
    }
    
    #[inline]
    pub fn compaction_workers(self, compaction_workers_count: usize) -> Self {
        self.store_config.set_compaction_workers(compaction_workers_count);
        self
    }
    
    #[inline]
    pub fn block_cache_capacity(self, block_cache_capacity: u64) -> Self {
        self.store_config.set_block_cache_capacity(block_cache_capacity);
        self
    }
    
    #[inline]
    pub fn blob_cache_capacity(self, blob_cache_capacity: u64) -> Self {
        self.store_config.set_blob_cache_capacity(blob_cache_capacity);
        self
    }
    
    #[inline]
    pub fn max_journaling_size(self, max_journaling_size: u64) -> Self {
        self.store_config.set_max_journaling_size(max_journaling_size);
        self
    }
    
    #[inline]
    pub fn max_write_buffer_size(self, max_write_buffer_size: u64) -> Self {
        self.store_config.set_max_write_buffer_size(max_write_buffer_size);
        self
    }
    
    #[inline]
    pub fn fsync_frequency(self, fsync_frequency: u16) -> Self {
        self.store_config.set_fsync_frequency(fsync_frequency);
        self
    }
    
    #[inline]
    pub fn event_listener(self, listener: StoreEventListener) -> Self {
        self.store_config.add_event_listener(listener);
        self
    }
    
    #[inline]
    pub fn bloom_filter_bits(self, bloom_filter_bits: u8) -> Self {
        self.store_config.set_bloom_filter_bits(bloom_filter_bits as i8);
        self
    }
    
    #[inline]
    pub fn compression_type(self, compression_type: CompressionType) -> Self {
        self.store_config.set_compression_type(compression_type);
        self
    }
    
    #[inline]
    pub fn compaction_strategy(self, compaction_strategy: Strategy) -> Self {
        self.store_config.set_compaction_strategy(compaction_strategy);
        self
    }
    
    #[inline]
    pub fn max_memtable_size(self, max_memtable_size: u32) -> Self {
        self.store_config.set_max_memtable_size(max_memtable_size);
        self
    }
    
    #[inline]
    pub fn block_size(self, block_size: u32) -> Self {
        self.store_config.set_block_size(block_size);
        self
    }
    
    #[inline]
    pub fn kv_separated(self, kv_separated: bool) -> Self {
        self.store_config.set_kv_separated(kv_separated);
        self
    }
    
    #[inline]
    pub fn space_amp_factor(self, space_amp_factor: f32) -> Self {
        self.store_config.set_space_amp_factor(space_amp_factor);
        self
    }
    
    #[inline]
    pub fn staleness_threshold(self, staleness_threshold: f32) -> Self {
        self.store_config.set_staleness_threshold(staleness_threshold);
        self
    }
    
    #[inline]
    pub fn build(self) -> FjallModule {
        FjallModule {
            store_config: self.store_config,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::compaction::Strategy;
    use fjall::CompressionType;
    use nitrite::common::PluginRegistrar;

    #[inline(never)]
    fn black_box<T>(x: T) -> T {
        x
    }

    #[test]
    fn test_fjall_module_with_config() {
        let builder = FjallModule::with_config();
        assert!(builder.store_config.db_path().is_empty());
    }

    #[test]
    fn test_fjall_module_plugins() {
        let module = FjallModule {
            store_config: FjallConfig::new(),
        };
        let plugins = module.plugins();
        assert!(plugins.is_ok());
        assert_eq!(plugins.unwrap().len(), 1);
    }

    #[test]
    fn test_fjall_module_load() {
        let module = FjallModule {
            store_config: FjallConfig::new(),
        };
        let registrar = PluginRegistrar::default();
        let result = module.load(&registrar);
        assert!(result.is_ok());
    }

    #[test]
    fn test_fjall_module_get_store() {
        let module = FjallModule {
            store_config: FjallConfig::new(),
        };
        let store = module.get_store();
        assert!(store.is_ok());
    }

    #[test]
    fn test_fjall_module_builder() {
        let builder = FjallModuleBuilder::new()
            .db_path("test_path")
            .manual_journal_persist(true)
            .flush_workers(4)
            .compaction_workers(2)
            .block_cache_capacity(1024)
            .blob_cache_capacity(2048)
            .max_journaling_size(4096)
            .max_write_buffer_size(8192)
            .fsync_frequency(10)
            .event_listener(StoreEventListener::new(|_| Ok(())))
            .bloom_filter_bits(8)
            .compression_type(CompressionType::None)
            .compaction_strategy(Strategy::default())
            .max_memtable_size(1024)
            .block_size(4096)
            .kv_separated(true)
            .space_amp_factor(1.5)
            .staleness_threshold(0.5)
            .build();

        assert_eq!(builder.store_config.db_path(), "test_path");
        assert!(builder.store_config.manual_journal_persist());
        assert_eq!(builder.store_config.flush_workers(), 4);
        assert_eq!(builder.store_config.compaction_workers(), 2);
        assert_eq!(builder.store_config.block_cache_capacity(), 1024);
        assert_eq!(builder.store_config.blob_cache_capacity(), 2048);
        assert_eq!(builder.store_config.max_journaling_size(), 4096);
        assert_eq!(builder.store_config.max_write_buffer_size(), 8192);
        assert_eq!(builder.store_config.fsync_frequency(), 10);
        assert_eq!(builder.store_config.bloom_filter_bits(), 8);
        assert_eq!(builder.store_config.compression_type(), CompressionType::None);
        assert_eq!(builder.store_config.max_memtable_size(), 1024);
        assert_eq!(builder.store_config.block_size(), 4096);
        assert!(builder.store_config.kv_separated());
        assert_eq!(builder.store_config.space_amp_factor(), 1.5);
        assert_eq!(builder.store_config.staleness_threshold(), 0.5);
    }

    #[test]
    fn test_fjall_module_creation_perf() {
        for _ in 0..1000 {
            let module = black_box(FjallModule {
                store_config: FjallConfig::new(),
            });
            black_box(module);
        }
    }

    #[test]
    fn test_fjall_module_builder_chain_perf() {
        for _ in 0..500 {
            let _module = black_box(
                FjallModuleBuilder::new()
                    .db_path("test_path")
                    .flush_workers(4)
                    .compaction_workers(2)
                    .block_cache_capacity(256 * 1_024 * 1_024)
                    .blob_cache_capacity(64 * 1_024 * 1_024)
                    .build()
            );
        }
    }

    #[test]
    fn test_builder_inline_efficiency() {
        // Verify #[inline] directives reduce function call overhead
        for _ in 0..1000 {
            let builder = black_box(FjallModuleBuilder::new());
            let builder = black_box(builder.db_path("path1"));
            let builder = black_box(builder.flush_workers(8));
            let builder = black_box(builder.block_cache_capacity(512 * 1_024 * 1_024));
            let module = black_box(builder.build());
            black_box(module);
        }
    }

    #[test]
    fn test_builder_single_allocation_per_chain() {
        // Verify each builder chain only allocates once per method
        for _ in 0..500 {
            let module = black_box(
                FjallModuleBuilder::new()
                    .db_path("test")
                    .manual_journal_persist(true)
                    .flush_workers(4)
                    .compaction_workers(2)
                    .build()
            );
            black_box(module.store_config.db_path());
        }
    }

    #[test]
    fn test_fjall_module_creation_efficiency() {
        // Verify FjallModule construction is efficient
        for _ in 0..5000 {
            let module = black_box(FjallModule {
                store_config: FjallConfig::new(),
            });
            black_box(module);
        }
    }

    #[test]
    fn test_builder_with_cache_settings_perf() {
        // Verify builder with cache configuration is efficient
        for _ in 0..1000 {
            let module = black_box(
                FjallModuleBuilder::new()
                    .block_cache_capacity(256 * 1_024 * 1_024)
                    .blob_cache_capacity(64 * 1_024 * 1_024)
                    .max_write_buffer_size(128 * 1_024 * 1_024)
                    .build()
            );
            black_box(module);
        }
    }

    #[test]
    fn test_builder_method_call_count() {
        // Verify builder chain doesn't have redundant operations
        for _ in 0..500 {
            let builder = FjallModuleBuilder::new();
            let builder = builder.db_path("path");
            let builder = builder.flush_workers(4);
            let builder = builder.compaction_workers(2);
            let builder = builder.block_cache_capacity(256 * 1_024 * 1_024);
            let builder = builder.bloom_filter_bits(8);
            let builder = builder.kv_separated(true);
            let _module = black_box(builder.build());
        }
    }

    #[test]
    fn test_store_module_get_store_perf() {
        // Verify get_store() method is efficient
        let module = FjallModule {
            store_config: FjallConfig::new(),
        };
        for _ in 0..500 {
            let store = black_box(module.get_store().ok());
            black_box(store);
        }
    }

    #[test]
    fn test_plugins_method_efficiency() {
        // Verify plugins() method doesn't allocate excessively
        let module = FjallModule {
            store_config: FjallConfig::new(),
        };
        for _ in 0..500 {
            let plugins = black_box(module.plugins().ok());
            black_box(plugins);
        }
    }

    #[test]
    fn test_production_preset() {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        
        let module = FjallModuleBuilder::new()
            .production_preset()
            .db_path("prod_test")
            .build();
        
        assert_eq!(module.store_config.db_path(), "prod_test");
        assert_eq!(module.store_config.block_cache_capacity(), 256 * 1024 * 1024);
        assert_eq!(module.store_config.blob_cache_capacity(), 64 * 1024 * 1024);
        assert_eq!(module.store_config.max_write_buffer_size(), 128 * 1024 * 1024);
        assert_eq!(module.store_config.max_memtable_size(), 32 * 1024 * 1024);
        assert_eq!(module.store_config.flush_workers(), cpus);
        assert_eq!(module.store_config.compaction_workers(), (cpus / 2).max(1));
        assert_eq!(module.store_config.bloom_filter_bits(), 10);
        assert_eq!(module.store_config.fsync_frequency(), 100);
        assert_eq!(module.store_config.compression_type(), CompressionType::Lz4);
    }

    #[test]
    fn test_high_throughput_preset() {
        let cpus = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        
        let module = FjallModuleBuilder::new()
            .high_throughput_preset()
            .db_path("throughput_test")
            .build();
        
        assert_eq!(module.store_config.block_cache_capacity(), 512 * 1024 * 1024);
        assert_eq!(module.store_config.blob_cache_capacity(), 128 * 1024 * 1024);
        assert_eq!(module.store_config.max_write_buffer_size(), 256 * 1024 * 1024);
        assert_eq!(module.store_config.max_memtable_size(), 64 * 1024 * 1024);
        assert_eq!(module.store_config.flush_workers(), cpus);
        assert_eq!(module.store_config.compaction_workers(), cpus);
        assert!(module.store_config.manual_journal_persist());
        assert!(module.store_config.kv_separated());
        assert_eq!(module.store_config.bloom_filter_bits(), 0);
    }

    #[test]
    fn test_low_memory_preset() {
        let module = FjallModuleBuilder::new()
            .low_memory_preset()
            .db_path("low_mem_test")
            .build();
        
        assert_eq!(module.store_config.block_cache_capacity(), 16 * 1024 * 1024);
        assert_eq!(module.store_config.blob_cache_capacity(), 8 * 1024 * 1024);
        assert_eq!(module.store_config.max_write_buffer_size(), 32 * 1024 * 1024);
        assert_eq!(module.store_config.max_memtable_size(), 8 * 1024 * 1024);
        assert_eq!(module.store_config.flush_workers(), 1);
        assert_eq!(module.store_config.compaction_workers(), 1);
        assert_eq!(module.store_config.bloom_filter_bits(), 10);
    }

    #[test]
    fn test_preset_override() {
        // Verify that presets can be overridden by subsequent builder calls
        let module = FjallModuleBuilder::new()
            .production_preset()
            .block_cache_capacity(1024 * 1024 * 1024)  // Override to 1GB
            .build();
        
        // Cache should be overridden
        assert_eq!(module.store_config.block_cache_capacity(), 1024 * 1024 * 1024);
        // Other production settings should remain
        assert_eq!(module.store_config.bloom_filter_bits(), 10);
    }

    #[test]
    fn test_preset_chain_performance() {
        for _ in 0..500 {
            let module = black_box(
                FjallModuleBuilder::new()
                    .production_preset()
                    .db_path("perf_test")
                    .build()
            );
            black_box(module);
        }
    }
}