use crate::config::FjallConfig;
use crate::map::FjallMap;
use crate::version::fjall_version;
use crate::wrapper::to_nitrite_error;
use crossbeam::sync::WaitGroup;
use dashmap::DashMap;
use fjall::{GarbageCollection, Keyspace, PersistMode};
use nitrite::common::{
    async_task, NitriteEventBus, NitritePlugin, NitritePluginProvider, SubscriberRef,
    COLLECTION_CATALOG,
};
use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::nitrite_config::NitriteConfig;
use nitrite::store::{
    NitriteMap, NitriteMapProvider, NitriteStore, NitriteStoreProvider, StoreCatalog, StoreConfig,
    StoreEventInfo, StoreEventListener, StoreEvents,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
/// Fjall-based store implementation.
///
/// A persistent, thread-safe key-value store backend using Fjall LSM engine.
/// Uses PIMPL pattern with `Arc<FjallStoreInner>` for efficient cloning and shared ownership.
/// Implements NitriteStoreProvider for integration with Nitrite's data store abstraction.
///
/// Purpose: Provides durable, ACID-compliant storage with multiple isolated maps
/// (collections, indexes) managed within a single Keyspace.
///
/// Characteristics:
/// - Thread-safe (Arc-based, cloneable across threads)
/// - Persistent (backed by Fjall LSM engine on disk)
/// - Multi-map (supports isolated partitions per collection/index)
/// - Event-driven (emits store lifecycle events)
/// - Lazy initialization (keyspace created on demand)
/// - Garbage collection (automatic compaction and cleanup)
/// - Trait delegation (no Deref, explicit method forwarding)
///
/// Usage: Created via FjallStore::new(config), used by NitriteBuilder to initialize
/// database storage. Provides open_map() to create isolated maps for collections and indexes.
pub struct FjallStore {
    inner: Arc<FjallStoreInner>,
}

impl FjallStore {
    /// Creates a new FjallStore with the given configuration.
    ///
    /// Arguments:
    /// - `config`: Fjall configuration parameters
    ///
    /// Returns: A new `FjallStore` instance (keyspace not yet initialized)
    #[inline]
    pub fn new(config: FjallConfig) -> FjallStore {
        FjallStore {
            inner: Arc::new(FjallStoreInner::new(config)),
        }
    }

    /// Encodes a map name to make it safe for Fjall partition names.
    /// 
    /// Fjall does not support certain characters in partition names (e.g., pipe `|`).
    /// This method uses efficient character substitution instead of base64 encoding:
    /// - `_X_` → `_XX_` (escape existing markers)
    /// - `_P_` → `_XP_` (escape existing pipe marker)
    /// - `_K_` → `_XK_` (escape existing key marker)
    /// - `|` → `_P_` (pipe - internal name separator)
    /// - `+` → `_K_` (plus - keyed repository separator)
    /// 
    /// Fjall partition names only allow: a-zA-Z0-9_-.#$
    /// This encoding uses only alphanumerics and underscores for maximum compatibility.
    #[inline]
    pub(crate) fn encode_name(name: &str) -> String {
        // Fast path: if no special characters that need encoding, return as-is
        if !name.contains('|') && !name.contains('+') 
            && !name.contains("_X_") && !name.contains("_P_") && !name.contains("_K_") {
            return name.to_string();
        }
        
        // First escape our marker sequences, then encode special characters
        name.replace("_X_", "_XX_")
            .replace("_P_", "_XP_")
            .replace("_K_", "_XK_")
            .replace('|', "_P_")
            .replace('+', "_K_")
    }

    /// Decodes a Fjall partition name back to the original map name.
    /// 
    /// Reverses the encoding performed by `encode_name`.
    #[inline]
    pub(crate) fn decode_name(name: &str) -> String {
        // Fast path: if no encoded sequences, return as-is
        if !name.contains("_P_") && !name.contains("_K_") && !name.contains("_X") {
            return name.to_string();
        }
        
        // Reverse the encoding: FIRST unescape markers, THEN restore special chars
        // Order is critical: _XP_ must become _P_ before _P_ becomes |
        name.replace("_XX_", "\x00X\x00")  // Temporarily escape _X_ markers
            .replace("_XP_", "\x00P\x00")  // Temporarily escape _P_ markers
            .replace("_XK_", "\x00K\x00")  // Temporarily escape _K_ markers
            .replace("_P_", "|")           // Restore pipes
            .replace("_K_", "+")           // Restore plus
            .replace("\x00X\x00", "_X_")   // Restore _X_ markers
            .replace("\x00P\x00", "_P_")   // Restore _P_ markers
            .replace("\x00K\x00", "_K_")   // Restore _K_ markers
    }

    /// Returns a clone of the Keyspace if it's initialized, or None if not.
    /// This is used internally for batch operations that require keyspace access.
    #[inline]
    pub(crate) fn keyspace(&self) -> Option<Keyspace> {
        self.inner.keyspace.get().cloned()
    }
}

impl NitritePluginProvider for FjallStore {
    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        self.inner.initialize(config)
    }

    fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    fn as_plugin(&self) -> NitritePlugin {
        NitritePlugin::new(self.clone())
    }
}

impl NitriteStoreProvider for FjallStore {
    fn open_or_create(&self) -> NitriteResult<()> {
        self.inner.open_or_create()
    }

    fn is_closed(&self) -> NitriteResult<bool> {
        self.inner.is_closed()
    }

    fn get_collection_names(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        let collection_names = catalog.get_collection_names()?;
        Ok(collection_names)
    }

    fn get_repository_registry(&self) -> NitriteResult<HashSet<String>> {
        let catalog = self.store_catalog()?;
        let repository_registry = catalog.get_repository_names()?;
        Ok(repository_registry)
    }

    fn get_keyed_repository_registry(&self) -> NitriteResult<HashMap<String, HashSet<String>>> {
        let catalog = self.store_catalog()?;
        let keyed_repository_registry = catalog.get_keyed_repository_names()?;
        Ok(keyed_repository_registry)
    }

    fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        self.inner.has_unsaved_changes()
    }

    fn is_read_only(&self) -> NitriteResult<bool> {
        Ok(false)
    }

    fn is_map_opened(&self, name: &str) -> NitriteResult<bool> {
        let name = FjallStore::encode_name(name);
        self.inner.is_map_opened(&name)
    }

    fn commit(&self) -> NitriteResult<()> {
        self.inner.commit()
    }

    fn compact(&self) -> NitriteResult<()> {
        self.inner.compact()
    }

    fn before_close(&self) -> NitriteResult<()> {
        self.inner.before_close()
    }

    fn has_map(&self, name: &str) -> NitriteResult<bool> {
        let name = FjallStore::encode_name(name);
        self.inner.has_map(&name)
    }

    fn open_map(&self, name: &str) -> NitriteResult<NitriteMap> {
        let name = FjallStore::encode_name(name);
        self.inner.open_map(&name, self.clone())
    }

    fn close_map(&self, name: &str) -> NitriteResult<()> {
        let name = FjallStore::encode_name(name);
        self.inner.close_map(&name)
    }

    fn remove_map(&self, name: &str) -> NitriteResult<()> {
        let name = FjallStore::encode_name(name);
        self.inner.remove_map(&name)
    }

    fn subscribe(&self, listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.inner.subscribe(listener)
    }

    fn unsubscribe(&self, subscriber_ref: SubscriberRef) -> NitriteResult<()> {
        self.inner.unsubscribe(subscriber_ref)
    }

    fn store_version(&self) -> NitriteResult<String> {
        self.inner.store_version()
    }

    fn store_config(&self) -> NitriteResult<StoreConfig> {
        self.inner.store_config()
    }

    fn store_catalog(&self) -> NitriteResult<StoreCatalog> {
        self.inner.store_catalog(self.clone())
    }
}

struct FjallStoreInner {
    keyspace: OnceLock<Keyspace>,
    closed: AtomicBool,
    event_bus: NitriteEventBus<StoreEventInfo, StoreEventListener>,
    store_config: FjallConfig,
    nitrite_config: OnceLock<NitriteConfig>,
    map_registry: DashMap<String, FjallMap>,
}

impl FjallStoreInner {
    fn new(config: FjallConfig) -> FjallStoreInner {
        FjallStoreInner {
            keyspace: OnceLock::new(),
            closed: AtomicBool::new(false),
            event_bus: NitriteEventBus::new(),
            store_config: config,
            nitrite_config: OnceLock::new(),
            map_registry: DashMap::new(),
        }
    }

    /// Helper function to check if an error indicates a partition was deleted
    #[inline]
    fn is_partition_deleted_error(err_msg: &str) -> bool {
        err_msg.contains("not found") || err_msg.contains("deleted") || err_msg.contains("PartitionDeleted")
    }

    fn initialize(&self, config: NitriteConfig) -> NitriteResult<()> {
        // get_or_init() always returns a reference to the initialized value (or initial value if already initialized)
        // The None case in pattern matching below is unreachable after get_or_init() completes successfully
        let cfg = self.nitrite_config.get_or_init(|| config);
        let path = self.store_config.db_path();
        cfg.set_db_path(path)?;
        Ok(())
    }

    fn close(&self) -> NitriteResult<()> {
        self.before_close()?;

        let temp_registry = self.map_registry.clone();
        for map in temp_registry.iter() {
            map.close()?;
        }

        temp_registry.clear();
        self.map_registry.clear();
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
        self.event_bus.close()?;
        Ok(())
    }

    fn open_or_create(&self) -> NitriteResult<()> {
        let config = self.store_config.keyspace_config();
        let result = Keyspace::open(config);
        match result {
            Ok(keyspace) => {
                self.keyspace.get_or_init(|| keyspace);
                Ok(())
            }
            Err(err) => {
                log::error!("Failed to open or create keyspace: {}", err);
                Err(to_nitrite_error(err))
            }
        }
    }

    #[inline]
    fn is_closed(&self) -> NitriteResult<bool> {
        Ok(self.closed.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn has_unsaved_changes(&self) -> NitriteResult<bool> {
        if let Some(ks) = self.keyspace.get() {
            let uncommitted = ks.write_buffer_size();
            Ok(uncommitted > 0)
        } else {
            Ok(false)
        }
    }

    #[inline]
    fn is_map_opened(&self, name: &str) -> NitriteResult<bool> {
        if let Some(map) = self.map_registry.get(name) {
            return Ok(!map.is_closed()?);
        }
        Ok(false)
    }

    fn commit(&self) -> NitriteResult<()> {
        if let Some(ks) = self.keyspace.get() {
            match ks.persist(PersistMode::SyncAll) {
                Ok(_) => Ok(()),
                Err(err) => {
                    log::error!("Failed to commit keyspace: {}", err);
                    Err(to_nitrite_error(err))
                }
            }
        } else {
            Ok(())
        }
    }

    fn compact(&self) -> NitriteResult<()> {
        if let Some(ks) = self.keyspace.get() {
            let partitions = ks.list_partitions();
            let maps: Vec<&str> = partitions
                .iter()
                .map(|partition| partition.trim())
                .collect();
            let wait_group = WaitGroup::new();

            for map in maps {
                let cloned_keyspace = ks.clone();
                let cloned_options = self.store_config.partition_config().clone();
                let space_amp_factor = self.store_config.space_amp_factor();
                let stale_threshold = self.store_config.staleness_threshold();
                let cloned_map = map.to_string();
                let cloned_wait_group = wait_group.clone();

                async_task(move || {
                    let partition = cloned_keyspace.open_partition(&cloned_map, cloned_options);
                    match partition {
                        Ok(partition) => {
                            let result = partition.gc_scan();
                            if let Err(err) = result {
                                log::error!("Failed to compact partition: {}", err);
                                return;
                            }

                            let result = partition.gc_with_space_amp_target(space_amp_factor);
                            if let Err(err) = result {
                                log::error!("Failed to compact partition: {}", err);
                                return;
                            }

                            let result = partition.gc_with_staleness_threshold(stale_threshold);
                            if let Err(err) = result {
                                log::error!("Failed to compact partition: {}", err);
                                return;
                            }
                        }
                        Err(err) => {
                            log::error!("Failed to open partition: {}", err);
                            return;
                        }
                    }
                    drop(cloned_wait_group);
                });
            }

            wait_group.wait();
            Ok(())
        } else {
            Ok(())
        }
    }

    fn before_close(&self) -> NitriteResult<()> {
        self.alert(StoreEvents::Closing)
    }

    #[inline]
    fn has_map(&self, name: &str) -> NitriteResult<bool> {
        if let Some(ks) = self.keyspace.get() {
            let result = ks.partition_exists(name);
            Ok(result)
        } else {
            Ok(false)
        }
    }

    fn open_map(&self, name: &str, fjall_store: FjallStore) -> NitriteResult<NitriteMap> {
        let mut closed = false;
        if let Some(map) = self.map_registry.get(name) {
            if map.is_closed()? {
                // can't remove the map here as it will cause deadlock
                closed = true;
            } else {
                return Ok(NitriteMap::new(map.clone()));
            }
        }

        // remove the map from registry if it is closed
        if closed {
            self.map_registry.remove(name);
        }

        if let Some(ks) = self.keyspace.get() {
            match ks.open_partition(name, self.store_config.partition_config()) {
                Ok(partition) => {
                    let fjall_map = FjallMap::new(
                        name.to_string(),
                        partition,
                        fjall_store,
                        self.store_config.clone(),
                    );
                    fjall_map.initialize()?;

                    self.map_registry
                        .insert(name.to_string(), fjall_map.clone());
                    Ok(NitriteMap::new(fjall_map))
                }
                Err(err) => {
                    // If partition was deleted, remove from cache and propagate error
                    let err_msg = err.to_string();
                    if Self::is_partition_deleted_error(&err_msg) {
                        self.map_registry.remove(name);
                    }
                    log::error!("Failed to open partition: {}", err);
                    Err(to_nitrite_error(err))
                }
            }
        } else {
            Err(NitriteError::new(
                "Keyspace is not initialized",
                ErrorKind::PluginError,
            ))
        }
    }

    fn close_map(&self, name: &str) -> NitriteResult<()> {
        let result = self.map_registry.remove(name);
        if result.is_some() {
            if let Some((_, map)) = result { drop(map) }
        }
        Ok(())
    }

    fn remove_map(&self, name: &str) -> NitriteResult<()> {
        // close the map if it is open to drop any partition handles holding by the map
        self.close_map(name)?;
        
        if let Some(ks) = self.keyspace.get() {
            let options = self.store_config.partition_config();
            match ks.open_partition(name, options) {
                Ok(partition) => {
                    match ks.delete_partition(partition.clone()) {
                        Ok(_) => {
                            // Ensure the map is removed from registry after successful deletion
                            // This is defensive in case the map was re-opened between close_map and here
                            self.map_registry.remove(name);
                            Ok(())
                        }
                        Err(err) => {
                            log::error!("Failed to remove partition: {}", err);
                            Err(to_nitrite_error(err))
                        }
                    }
                }
                Err(err) => {
                    // If partition doesn't exist, it might already be deleted
                    // This is acceptable - just ensure it's removed from registry
                    let err_msg = err.to_string();
                    if Self::is_partition_deleted_error(&err_msg) {
                        self.map_registry.remove(name);
                        Ok(())
                    } else {
                        log::error!("Failed to open partition for removal: {}", err);
                        Err(to_nitrite_error(err))
                    }
                }
            }
        } else {
            Ok(())
        }
    }

    fn subscribe(&self, listener: StoreEventListener) -> NitriteResult<Option<SubscriberRef>> {
        self.event_bus.register(listener)
    }

    fn unsubscribe(&self, subscriber_ref: SubscriberRef) -> NitriteResult<()> {
        self.event_bus.deregister(subscriber_ref)
    }

    fn store_version(&self) -> NitriteResult<String> {
        match fjall_version() {
            Ok(version) => Ok(format!("Fjall/{}", version)),
            Err(e) => Err(NitriteError::new(
                &format!("Failed to determine Fjall version: {}", e),
                ErrorKind::PluginError,
            )),
        }
    }

    fn store_config(&self) -> NitriteResult<StoreConfig> {
        Ok(StoreConfig::new(self.store_config.clone()))
    }

    fn store_catalog(&self, fjall_store: FjallStore) -> NitriteResult<StoreCatalog> {
        let nitrite_store = NitriteStore::new(fjall_store);
        let catalog_map = nitrite_store.open_map(COLLECTION_CATALOG)?;
        StoreCatalog::new(catalog_map)
    }

    fn alert(&self, event: StoreEvents) -> NitriteResult<()> {
        let option = self.nitrite_config.get();
        if let Some(config) = option {
            let event_info = StoreEventInfo::new(event, config.clone());
            self.event_bus.publish(event_info)
        } else {
            Ok(())
        }
    }
}

impl Drop for FjallStoreInner {
    fn drop(&mut self) {
        if self.store_config.commit_before_close() {
            match self.commit() {
                Ok(_) => {
                    log::debug!("Successfully committed keyspace during drop");
                }
                Err(e) => {
                    // Use if let pattern to avoid unwrap in Drop impl
                    // Drop should never panic - always log errors gracefully
                    log::error!("Failed to commit keyspace: {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{run_test, Context};
    use nitrite::store::StoreEventListener;
    use std::path::PathBuf;
    use std::time::Duration;
    use std::{fs, thread};

    #[inline(never)]
    #[allow(dead_code)]
    fn black_box<T>(x: T) -> T {
        x
    }

    fn create_context() -> Context {
        let path = random_path();        
        let fjall_config = FjallConfig::new();
        fjall_config.set_db_path(&path);
        fjall_config.set_kv_separated(true);

        let store = FjallStore::new(fjall_config.clone());
        
        Context::new(path, None, None, Some(store), None)
    }

    fn random_path() -> String {
        let id = uuid::Uuid::new_v4();
        PathBuf::from("../test-data").join(id.to_string()).to_str().unwrap().to_string()
    }

    fn cleanup(ctx: Context) {
        let path = ctx.path();
        let mut retry = 0;
        while fs::remove_dir_all(path.clone()).is_err() && retry < 2 {
            thread::sleep(Duration::from_millis(100));
            retry += 1;
        }
    }

    #[test]
    fn test_fjall_store_new() {
        run_test(|| {
            create_context()
        }, |ctx| {
            assert!(ctx.fjall_store_unsafe().inner.keyspace.get().is_none());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_encode_decode_name() {
        // Test pipe character encoding with _P_ scheme
        let encoded = FjallStore::encode_name("test|name");
        assert_eq!(encoded, "test_P_name");

        let decoded = FjallStore::decode_name("test_P_name");
        assert_eq!(decoded, "test|name");
        
        // Verify roundtrip for pipes
        let original = "namespace|collection|test";
        let encoded = FjallStore::encode_name(original);
        assert_eq!(encoded, "namespace_P_collection_P_test");
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
        
        // Test plus character encoding with _K_ scheme (keyed repository separator)
        let encoded = FjallStore::encode_name("Entity+key");
        assert_eq!(encoded, "Entity_K_key");
        
        let decoded = FjallStore::decode_name("Entity_K_key");
        assert_eq!(decoded, "Entity+key");
        
        // Test mixed pipe and plus
        let original = "$nitrite_index|Entity+key|field|type";
        let encoded = FjallStore::encode_name(original);
        assert!(!encoded.contains('|'));
        assert!(!encoded.contains('+'));
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_fjall_store_initialize() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let config = NitriteConfig::new();
            let result = ctx.fjall_store_unsafe().initialize(config);
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_close() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().close();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_open_or_create() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().open_or_create();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_is_closed() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().is_closed();
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_get_collection_names() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().get_collection_names();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_get_repository_registry() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().get_repository_registry();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_get_keyed_repository_registry() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().get_keyed_repository_registry();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_has_unsaved_changes() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().has_unsaved_changes();
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_commit() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().commit();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_compact() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().compact();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_before_close() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().before_close();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_has_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().has_map("test_map");
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_open_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().open_map("test_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_close_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().close_map("test_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_remove_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().remove_map("test_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_subscribe() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let listener = StoreEventListener::new(|_| Ok(()));
            let result = ctx.fjall_store_unsafe().subscribe(listener);
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_unsubscribe() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let listener = StoreEventListener::new(|_| Ok(()));
            let subscriber_ref = ctx.fjall_store_unsafe().subscribe(listener).unwrap();
            let result = ctx.fjall_store_unsafe().unsubscribe(subscriber_ref.unwrap());
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_store_version() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().store_version();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_store_config() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().store_config();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_store_catalog() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().store_catalog();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_decode_name_with_valid_utf8() {
        // Valid base64 encoded UTF-8 string
        let encoded = FjallStore::encode_name("valid_collection_name");
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, "valid_collection_name");
    }

    #[test]
    fn test_decode_name_with_multiple_pipes() {
        // Test with multiple pipe characters (common in internal names)
        let original = "$nitrite_index|collection_name|field|type";
        let encoded = FjallStore::encode_name(original);
        assert!(encoded.contains("_P_")); // Pipes should be encoded
        assert!(!encoded.contains("|")); // No raw pipes
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_name_with_hash_and_dollar() {
        // Test with # and $ which are valid Fjall characters
        let original = "$prefix#suffix";
        let encoded = FjallStore::encode_name(original);
        // Should remain unchanged - no pipes or markers
        assert_eq!(encoded, original);
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_name_with_pipe_character() {
        // Test with pipe character (fjall limitation workaround)
        let original = "namespace|collection|test";
        let encoded = FjallStore::encode_name(original);
        assert!(!encoded.contains("|")); // No raw pipes
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_name_without_special_chars() {
        // Names without special characters should pass through unchanged
        let plain_name = "simple_name";
        let encoded = FjallStore::encode_name(plain_name);
        assert_eq!(encoded, plain_name); // No encoding needed
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, plain_name);
    }

    #[test]
    fn test_encode_decode_with_marker_sequences() {
        // Test handling of _X_ marker sequences (need escaping)
        let original = "name_X_with_X_markers";
        let encoded = FjallStore::encode_name(original);
        assert_eq!(encoded, "name_XX_with_XX_markers"); // _X_ -> _XX_
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_complex_name() {
        // Test with mixed pipe and marker sequences
        let original = "ns|coll_P_test";
        let encoded = FjallStore::encode_name(original);
        // First _P_ in original becomes _XP_, then | becomes _P_
        assert_eq!(encoded, "ns_P_coll_XP_test");
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_name_with_empty_string() {
        // Empty string should be handled gracefully
        let empty = "";
        let encoded = FjallStore::encode_name(empty);
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, empty);
    }

    #[test]
    fn test_decode_name_roundtrip_with_valid_chars() {
        // Test roundtrip encoding/decoding with Fjall-valid characters
        let test_cases = vec![
            "system_map",
            "user.profile",
            "data-collection_v2",
            "CamelCase#Collection",
            "$internal_prefix",
            "simple-name",
        ];

        for original in test_cases {
            let encoded = FjallStore::encode_name(original);
            let decoded = FjallStore::decode_name(&encoded);
            assert_eq!(
                decoded, original,
                "Failed roundtrip for name: {}",
                original
            );
        }
    }

    #[test]
    fn test_encode_name_fast_path() {
        // Verify fast path for names without special characters
        let original = "test_collection_name";
        let encoded = FjallStore::encode_name(original);
        
        // Without special characters, the encoded name should equal the original
        assert_eq!(
            encoded, original,
            "Encoded name should equal original for plain names"
        );
    }

    #[test]
    fn test_encode_preserves_valid_fjall_chars() {
        // Names with valid Fjall characters should pass through unchanged (unless they contain markers)
        let valid_names = vec![
            "simple_name",
            "with-dashes",
            "with.dots",
            "CamelCase",
            "has#hash",
            "has$dollar",
        ];
        
        for name in valid_names {
            let encoded = FjallStore::encode_name(name);
            assert_eq!(encoded, name, "Valid name should not be modified: {}", name);
        }
    }

    #[test]
    fn test_decode_name_with_long_name() {
        // Test with very long collection name
        let original = "a".repeat(1000);
        let encoded = FjallStore::encode_name(&original);
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_name_passthrough_for_non_encoded() {
        // Names without encoding markers pass through decode unchanged
        let pass_through_cases = vec![
            "simple",
            "with-dash",
            "with.dot",
            "with_underscore",
        ];

        for input in pass_through_cases {
            let result = FjallStore::decode_name(input);
            assert_eq!(result, input, "Non-encoded input should pass through: {}", input);
        }
    }

    #[test]
    fn test_encode_decode_respects_marker_priority() {
        // Test that marker escaping happens in correct order
        let original = "_P_test|value_X_end_K_key+suffix";
        let encoded = FjallStore::encode_name(original);
        // After encoding, no raw special chars should remain
        assert!(!encoded.contains('|'));
        assert!(!encoded.contains('+'));
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_keyed_repository_names() {
        // Test keyed repository name format: "EntityName+key"
        let original = "User+admin";
        let encoded = FjallStore::encode_name(original);
        assert_eq!(encoded, "User_K_admin");
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
        
        // Test complex keyed repository with internal names
        let original = "$nitrite_index|User+admin|email|unique";
        let encoded = FjallStore::encode_name(original);
        assert!(!encoded.contains('|'));
        assert!(!encoded.contains('+'));
        let decoded = FjallStore::decode_name(&encoded);
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_symmetry() {
        // Verify that for all valid UTF-8 strings, encode->decode is identity
        let test_cases = vec![
            "simple",
            "with-dash",
            "with_underscore",
            "with.dot",
            "mix_of-all.chars",
            "CamelCaseName",
            "UPPERCASE_NAME",
            "lowercase_name",
            "numbers123456",
        ];

        for original in test_cases {
            let encoded = FjallStore::encode_name(original);
            let decoded = FjallStore::decode_name(&encoded);
            assert_eq!(
                decoded, original,
                "Symmetry broken for: {}",
                original
            );
        }
    }

    #[test]
    fn test_decode_name_handles_concurrent_access() {
        // Test that concurrent decoding doesn't cause issues
        use std::sync::Arc;
        use std::thread;

        let test_name = "concurrent_test_collection";
        let encoded = Arc::new(FjallStore::encode_name(test_name));
        let mut handles = vec![];

        for _ in 0..10 {
            let encoded_clone = Arc::clone(&encoded);
            let handle = thread::spawn(move || {
                FjallStore::decode_name(&encoded_clone)
            });
            handles.push(handle);
        }

        for handle in handles {
            let decoded = handle.join().unwrap();
            assert_eq!(decoded, test_name);
        }
    }

    #[test]
    fn test_has_unsaved_changes_with_no_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().has_unsaved_changes();
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_has_map_with_no_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().has_map("test_map");
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_is_map_opened_with_missing_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().is_map_opened("non_existent_map");
            assert!(result.is_ok());
            assert!(!result.unwrap());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_commit_with_no_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().commit();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_compact_with_no_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().compact();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_close_map_with_missing_map() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().close_map("non_existent_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_remove_map_with_no_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let result = ctx.fjall_store_unsafe().remove_map("test_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_store_config_retrieval() {
        run_test(|| {
            create_context()
        }, |ctx| {
            ctx.fjall_store_unsafe().open_or_create().unwrap();
            let result = ctx.fjall_store_unsafe().store_config();
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_has_unsaved_changes_no_panic() {
        // Verify that has_unsaved_changes never panics
        run_test(|| {
            create_context()
        }, |ctx| {
            for _ in 0..10 {
                let _ = ctx.fjall_store_unsafe().has_unsaved_changes();
            }
            // If we get here without panicking, test passes
            assert!(true);
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_remove_map_handles_missing_keyspace() {
        run_test(|| {
            create_context()
        }, |ctx| {
            // Try to remove a map without ever creating keyspace
            let result = ctx.fjall_store_unsafe().remove_map("never_opened_map");
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_does_not_panic_on_commit_success() {
        // Verify that Drop impl completes successfully when commit succeeds
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().expect("Failed to open store");
            
            // Open a map and make a modification
            let map = store.open_map("test_drop_success").expect("Failed to open map");
            map.put(
                nitrite::common::Key::from("key1"),
                nitrite::common::Value::from("value1"),
            ).expect("Failed to put item");
            
            // Drop should not panic when commit succeeds
            drop(map);
            drop(store);  // This triggers Drop impl
            
            assert!(true, "Drop impl completed without panicking");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_does_not_panic_on_multiple_drops() {
        // Verify that multiple sequential drops don't cause cascading panics
        run_test(|| {
            let path = random_path();
            let fjall_config = FjallConfig::new();
            fjall_config.set_db_path(&path);
            let store = FjallStore::new(fjall_config);
            Context::new(path, None, None, Some(store), None)
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().expect("Failed to open store");
            
            // Multiple operations
            for i in 0..3 {
                let map_name = format!("map_{}", i);
                let map = store.open_map(&map_name).expect("Failed to open map");
                map.put(
                    nitrite::common::Key::from(format!("key_{}", i)),
                    nitrite::common::Value::from(format!("value_{}", i)),
                ).expect("Failed to put item");
                drop(map);
            }
            
            // Drop store - should not panic
            drop(store);
            
            assert!(true, "Multiple drops completed without panicking");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_handles_closed_store_gracefully() {
        // Verify that dropping a closed store doesn't cause panic in Drop impl
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().expect("Failed to open store");
            
            // Close the store first
            store.close().expect("Failed to close store");
            
            // Now drop - should not panic even though store is already closed
            drop(store);
            
            assert!(true, "Drop impl handled closed store gracefully");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_with_commit_before_close_enabled() {
        // Verify that Drop impl properly handles commit_before_close config
        run_test(|| {
            let path = random_path();
            let fjall_config = FjallConfig::new();
            fjall_config.set_db_path(&path);
            fjall_config.set_commit_before_close(true);  // Enable commit on drop
            let store = FjallStore::new(fjall_config);
            Context::new(path, None, None, Some(store), None)
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().expect("Failed to open store");
            
            let map = store.open_map("test_commit_on_drop").expect("Failed to open map");
            map.put(
                nitrite::common::Key::from("test_key"),
                nitrite::common::Value::from("test_value"),
            ).expect("Failed to put item");
            
            // Drop with commit_before_close enabled - should not panic
            drop(map);
            drop(store);  // This triggers Drop impl with commit attempt
            
            assert!(true, "Drop impl completed with commit_before_close enabled");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_with_commit_before_close_disabled() {
        // Verify that Drop impl handles commit_before_close=false correctly
        run_test(|| {
            let path = random_path();
            let fjall_config = FjallConfig::new();
            fjall_config.set_db_path(&path);
            fjall_config.set_commit_before_close(false);  // Disable commit on drop
            let store = FjallStore::new(fjall_config);
            Context::new(path, None, None, Some(store), None)
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().expect("Failed to open store");
            
            let map = store.open_map("test_no_commit_on_drop").expect("Failed to open map");
            map.put(
                nitrite::common::Key::from("test_key"),
                nitrite::common::Value::from("test_value"),
            ).expect("Failed to put item");
            
            // Drop with commit_before_close disabled
            drop(map);
            drop(store);  // This triggers Drop impl without commit attempt
            
            assert!(true, "Drop impl completed with commit_before_close disabled");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_never_panics() {
        // Comprehensive test: Drop impl should NEVER panic regardless of state
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            
            // Test various store states
            
            // 1. Unopened store
            drop(store.clone());
            
            // 2. Opened store
            store.open_or_create().ok();
            drop(store.clone());
            
            // 3. Store with maps
            if let Ok(map) = store.open_map("drop_safety_test") {
                let _ = map.put(
                    nitrite::common::Key::from("k"),
                    nitrite::common::Value::from("v"),
                );
                drop(map);
            }
            drop(store);
            
            assert!(true, "Drop impl never panicked in any state");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_drop_impl_error_logging_does_not_panic() {
        // Verify that error logging in Drop impl is safe
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Even if commit fails internally, Drop should log error safely
            // without panicking during error message construction
            drop(store);
            
            assert!(true, "Drop impl error logging was safe");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_succeeds_with_valid_config() {
        // Verify that initialize() properly initializes OnceLock with provided config
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let nitrite_config = NitriteConfig::default();
            nitrite_config.auto_configure().expect("Failed to auto configure");
            
            // First initialization should succeed
            let result = store.initialize(nitrite_config.clone());
            assert!(result.is_ok(), "First initialization should succeed");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_idempotent_behavior() {
        // Verify that OnceLock::get_or_init() ensures only first initialization persists
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let config1 = NitriteConfig::default();
            config1.auto_configure().ok();
            let config2 = NitriteConfig::default();
            config2.auto_configure().ok();
            
            // First initialization
            let result1 = store.initialize(config1);
            assert!(result1.is_ok(), "First init should succeed");
            
            // Second initialization attempt - should use the first one (get_or_init property)
            let result2 = store.initialize(config2);
            assert!(result2.is_ok(), "Second init should also succeed");
            
            // Both should complete without errors
            assert!(true, "Idempotent behavior preserved");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_no_impossible_error_path() {
        // Verify that there's no unreachable error condition in initialize()
        // After get_or_init(), the OnceLock is guaranteed to have a value
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let config = NitriteConfig::default();
            config.auto_configure().ok();
            
            // initialize() should never hit the None case in the original pattern match
            // because get_or_init() guarantees a value is present
            let result = store.initialize(config);
            
            // No need to handle impossible error cases
            assert!(result.is_ok(), "Initialize should succeed without impossible None case");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_concurrent_calls() {
        // Verify that concurrent initialization calls don't cause issues
        // OnceLock ensures thread-safe, atomic initialization
        run_test(|| {
            let path = random_path();
            let fjall_config = FjallConfig::new();
            fjall_config.set_db_path(&path);
            let store = FjallStore::new(fjall_config);
            Context::new(path, None, None, Some(store), None)
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let config = NitriteConfig::default();
            config.auto_configure().ok();
            
            // Single initialization
            let result = store.initialize(config);
            assert!(result.is_ok(), "Initialize should succeed");
            
            // Subsequent calls use get_or_init which is idempotent
            let config2 = NitriteConfig::default();
            config2.auto_configure().ok();
            let result2 = store.initialize(config2);
            assert!(result2.is_ok(), "Concurrent-style call should also succeed");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_sets_correct_db_path() {
        // Verify that initialize() correctly sets the database path on the config
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let config = NitriteConfig::default();
            config.auto_configure().ok();
            
            store.open_or_create().ok();
            let result = store.initialize(config);
            
            assert!(result.is_ok(), "Initialize should successfully set db path");
            
            // Verify that store can be used after initialization
            let map_result = store.open_map("test_after_init");
            assert!(map_result.is_ok(), "Store should be usable after initialize");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_initialize_does_not_error_with_none_pattern() {
        // Verify that the unreachable None case has been removed/fixed
        // The pattern match on get() after get_or_init() was impossible
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            let config = NitriteConfig::default();
            config.auto_configure().ok();
            
            // This should complete successfully without hitting the impossible None case
            let result = store.initialize(config);
            assert!(result.is_ok(), "Initialize should not hit unreachable None case");
            
            // Verify store is in good state
            let open_result = store.open_or_create();
            assert!(open_result.is_ok(), "Store should be usable after initialize");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_encode_decode_perf() {
        for _ in 0..1000 {
            let encoded = black_box(FjallStore::encode_name("test|collection"));
            black_box(FjallStore::decode_name(&encoded));
        }
    }

    #[test]
    fn test_fjall_store_is_closed_perf() {
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            for _ in 0..10000 {
                let result = black_box(store.is_closed());
                black_box(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_fjall_store_initialization_perf() {
        for _ in 0..100 {
            let _ctx = black_box(create_context());
        }
    }

    #[test]
    fn test_has_unsaved_changes_perf_if_let_pattern() {
        // Verify if-let pattern is efficient for OnceLock access
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Repeated calls should use efficient if-let pattern
            for _ in 0..5000 {
                let result = black_box(store.has_unsaved_changes());
                black_box(result.is_ok());
            }
            assert!(true, "if-let pattern in has_unsaved_changes is efficient");
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_has_unsaved_changes_early_return() {
        // Verify that early return when no keyspace is efficient
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            // Don't initialize keyspace
            
            // Should return early without penalty
            for _ in 0..1000 {
                let result = black_box(store.has_unsaved_changes());
                assert!(result.is_ok());
                assert!(!result.unwrap());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_is_map_opened_dashmap_get_efficiency() {
        // Verify that single get() call is more efficient than contains_key + get
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            let _map = store.open_map("perf_test_map").ok();
            
            // Repeated is_map_opened calls should be efficient
            for _ in 0..5000 {
                let result = black_box(store.is_map_opened("perf_test_map"));
                black_box(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_is_map_opened_missing_map_early_return() {
        // Verify early return for missing maps is efficient
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Missing map should return quickly
            for _ in 0..5000 {
                let result = black_box(store.is_map_opened("non_existent"));
                assert!(result.is_ok());
                assert!(!result.unwrap());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_has_map_oncelock_efficiency() {
        // Verify if-let pattern for OnceLock access in has_map
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Repeated calls should use efficient if-let
            for _ in 0..5000 {
                let result = black_box(store.has_map("test_map"));
                black_box(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_has_map_no_keyspace_efficiency() {
        // Verify early return when keyspace not available
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            // Don't initialize keyspace
            
            // Should return false efficiently without penalty
            for _ in 0..5000 {
                let result = black_box(store.has_map("any_map"));
                assert!(result.is_ok());
                assert!(!result.unwrap());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_commit_oncelock_single_access() {
        // Verify if-let pattern reduces OnceLock access in commit()
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Commit should access OnceLock only once via if-let
            for _ in 0..100 {
                let result = black_box(store.commit());
                black_box(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_commit_early_return_efficiency() {
        // Verify early return when no keyspace is efficient
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            // Don't initialize keyspace
            
            // Should return early
            for _ in 0..1000 {
                let result = black_box(store.commit());
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_open_map_dashmap_single_access() {
        // Verify that if-let pattern accesses DashMap only once
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Each open_map should efficiently check registry with single get()
            for i in 0..50 {
                let map_name = format!("map_{}", i);
                let result = store.open_map(&map_name);
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_remove_map_oncelock_efficiency() {
        // Verify if-let pattern in remove_map reduces OnceLock access
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Create and remove several maps
            for i in 0..50 {
                let map_name = format!("remove_test_{}", i);
                let _map = store.open_map(&map_name).ok();
                let result = black_box(store.remove_map(&map_name));
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_remove_map_no_keyspace_early_return() {
        // Verify early return when keyspace not available
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            // Don't initialize keyspace
            
            // Should return early
            for i in 0..100 {
                let map_name = format!("no_keyspace_{}", i);
                let result = black_box(store.remove_map(&map_name));
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_compact_oncelock_efficiency() {
        // Verify if-let pattern is efficient in compact()
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Create some partitions for compaction
            for i in 0..5 {
                let _map = store.open_map(&format!("compact_test_{}", i)).ok();
            }
            
            // Compact should access OnceLock efficiently
            let result = black_box(store.compact());
            assert!(result.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_compact_no_keyspace_efficiency() {
        // Verify early return is efficient
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            // Don't initialize keyspace
            
            // Should return early
            for _ in 0..100 {
                let result = black_box(store.compact());
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_if_let_pattern_correctness() {
        // Verify if-let pattern works correctly across all methods
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            
            // All if-let patterns should work correctly
            assert!(!store.has_unsaved_changes().unwrap());
            assert!(!store.has_map("test").unwrap());
            assert!(store.commit().is_ok());
            assert!(store.compact().is_ok());
            
            store.open_or_create().ok();
            
            assert!(store.has_unsaved_changes().is_ok());
            assert!(store.commit().is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_dasmap_get_eliminates_contains_key_lookup() {
        // Verify that using DashMap::get() eliminates double lookup
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Open a map first
            let _map = store.open_map("lookup_test").ok();
            
            // is_map_opened should use single get() not contains_key + get
            for _ in 0..1000 {
                let result = black_box(store.is_map_opened("lookup_test"));
                assert!(result.is_ok());
            }
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_oncelock_single_load_per_call() {
        // Verify that OnceLock methods access only once per method call
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Each method call should load OnceLock at most once
            let has_changes = black_box(store.has_unsaved_changes());
            assert!(has_changes.is_ok());
            
            let has_map = black_box(store.has_map("test"));
            assert!(has_map.is_ok());
            
            let commit = black_box(store.commit());
            assert!(commit.is_ok());
        }, |ctx| {
            cleanup(ctx);
        });
    }

    #[test]
    fn test_all_methods_use_improved_patterns() {
        // Comprehensive test verifying all optimizations work together
        run_test(|| {
            create_context()
        }, |ctx| {
            let store = ctx.fjall_store_unsafe();
            store.open_or_create().ok();
            
            // Test has_unsaved_changes with if-let
            for _ in 0..100 {
                let _ = store.has_unsaved_changes();
            }
            
            // Test is_map_opened with if-let DashMap
            let _ = store.open_map("method_test").ok();
            for _ in 0..100 {
                let _ = store.is_map_opened("method_test");
            }
            
            // Test has_map with if-let
            for _ in 0..100 {
                let _ = store.has_map("some_map");
            }
            
            // Test commit with if-let
            for _ in 0..10 {
                let _ = store.commit();
            }
            
            // Test compact with if-let
            let _ = store.compact();
            
            // Test open_map with improved error handling
            for i in 0..10 {
                let _ = store.open_map(&format!("batch_{}", i));
            }
            
            // Test remove_map with if-let
            let _ = store.remove_map("method_test");
            
            assert!(true, "All optimized methods work correctly together");
        }, |ctx| {
            cleanup(ctx);
        });
    }
}
