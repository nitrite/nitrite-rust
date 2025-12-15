//! Configuration management for Nitrite database.

use dashmap::DashMap;
use std::collections::BTreeMap;
use std::ops::Deref;

use crate::common::{ReadExecutor, WriteExecutor, PluginManager};
use crate::migration::Migration;
use crate::{
    errors::{ErrorKind, NitriteError, NitriteResult},
    index::NitriteIndexer,
    store::NitriteStore,
    NitriteModule, FIELD_SEPARATOR, INITIAL_SCHEMA_VERSION,
};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};

/// Public interface for Nitrite database configuration.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::NitriteBuilder;
///
/// let config = NitriteBuilder::default()
///     .field_separator(".")
///     .open_or_create()?;
/// ```
#[derive(Clone)]
pub struct NitriteConfig {
    /// The pointer to implementation. Uses Arc for cheap cloning and thread safety.
    inner: Arc<NitriteConfigInner>,
}

impl Default for NitriteConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl NitriteConfig {
    /// Creates a new configuration instance with default values.
    pub fn new() -> Self {
        NitriteConfig {
            inner: Arc::new(NitriteConfigInner::new()),
        }
    }

    /// Returns the current field separator string for nested document access.
    pub fn field_separator(&self) -> String {
        self.inner.field_separator()
    }

    /// Sets the field separator for nested document access.
    ///
    /// # Errors
    ///
    /// Returns error if already configured or if separator is empty.
    pub fn set_field_separator(&self, separator: &str) -> NitriteResult<()> {
        self.inner.set_field_separator(separator)
    }

    /// Gets the configured store plugin.
    ///
    /// # Errors
    ///
    /// Returns error if no store plugin is configured.
    pub fn nitrite_store(&self) -> NitriteResult<NitriteStore> {
        self.inner.nitrite_store()
    }

    /// Finds an indexer plugin by type.
    ///
    /// # Errors
    ///
    /// Returns error if no indexer plugin is found for the given type.
    pub fn find_indexer(&self, index_type: &str) -> NitriteResult<NitriteIndexer> {
        self.inner.find_indexer(index_type)
    }

    /// Loads a Nitrite module into the configuration.
    ///
    /// # Errors
    ///
    /// Returns error if already initialized or if module fails to load.
    pub fn load_module<T: NitriteModule + 'static>(&self, module: T) -> NitriteResult<()> {
        self.inner.load_module(module)
    }

    /// Automatically discovers and loads available plugins.
    ///
    /// # Errors
    ///
    /// Returns error if already initialized or plugin loading fails.
    pub fn auto_configure(&self) -> NitriteResult<()> {
        self.inner.auto_configure()
    }

    /// Closes the configuration and all plugins.
    ///
    /// # Errors
    ///
    /// Returns error if closing plugins fails.
    pub fn close(&self) -> NitriteResult<()> {
        self.inner.close()
    }

    /// Returns the current database schema version.
    pub fn schema_version(&self) -> u32 {
        self.inner.schema_version()
    }

    /// Sets the database schema version.
    ///
    /// # Errors
    ///
    /// Returns error if already initialized.
    pub fn set_schema_version(&self, version: u32) -> NitriteResult<()> {
        self.inner.set_schema_version(version)
    }

    /// Adds a migration to the configuration.
    ///
    /// # Errors
    ///
    /// Returns error if already initialized.
    pub fn add_migration(&self, migration: Migration) -> NitriteResult<()> {
        self.inner.add_migration(migration)
    }

    /// Gets all registered migrations.
    pub fn migrations(&self) -> DashMap<u32, BTreeMap<u32, Migration>> {
        self.inner.migrations()
    }

    /// Sets the database file path (can only be set once).
    pub fn set_db_path(&self, db_path: &str) -> NitriteResult<()> {
        self.inner.set_db_path(db_path)
    }

    /// Returns the database file path if set.
    pub fn db_path(&self) -> Option<String> {
        self.inner.db_path()
    }

    /// Initializes the configuration and all plugins.
    pub(crate) fn initialize(&self) -> NitriteResult<()> {
        self.inner.set_nitrite_config(self.clone());
        self.inner.initialize()
    }
}

/// Private implementation of Nitrite configuration.
///
/// This struct contains all internal state and implementation logic.
/// It is not exposed in the public API.
struct NitriteConfigInner {
    /// Indicates whether this configuration has been initialized
    configured: AtomicBool,
    /// Manages all registered plugins (stores, indexers, modules)
    plugin_manager: PluginManager,
    /// Current database schema version
    schema_version: AtomicU32,
    /// Path to the database file (set only once)
    db_path: OnceLock<String>,
    /// Map of migrations indexed by from_version -> to_version -> Migration
    migrations: DashMap<u32, BTreeMap<u32, Migration>>,
}

impl NitriteConfigInner {
    /// Creates a new configuration instance with default values.
    pub(crate) fn new() -> Self {
        NitriteConfigInner {
            configured: AtomicBool::from(false),
            plugin_manager: PluginManager::new(),
            schema_version: AtomicU32::from(INITIAL_SCHEMA_VERSION),
            db_path: OnceLock::new(),
            migrations: DashMap::new(),
        }
    }

    /// Returns the current field separator string for nested document access.
    pub(crate) fn field_separator(&self) -> String {
        FIELD_SEPARATOR.read_with(|it| it.clone())
    }

    /// Sets the field separator for nested document access.
    pub(crate) fn set_field_separator(&self, separator: &str) -> NitriteResult<()> {
        let is_configured = self.configured.load(Ordering::Relaxed);
        if is_configured {
            log::error!("Field separator cannot be changed after initialization");
            return Err(NitriteError::new(
                "Field separator cannot be changed after initialization",
                ErrorKind::InvalidOperation,
            ));
        }

        if separator.is_empty() {
            log::error!("Field separator cannot be empty");
            return Err(NitriteError::new(
                "Field separator cannot be empty",
                ErrorKind::InvalidOperation,
            ));
        }

        FIELD_SEPARATOR.write_with(|it| *it = separator.to_string());
        Ok(())
    }

    /// Gets the configured store plugin.
    pub(crate) fn nitrite_store(&self) -> NitriteResult<NitriteStore> {
        match self.plugin_manager.get_store() {
            Some(store) => Ok(store),
            None => {
                log::error!("No store plugin is configured");
                Err(NitriteError::new(
                    "No store plugin is configured",
                    ErrorKind::PluginError,
                ))
            }
        }
    }

    /// Finds an indexer plugin by type.
    pub(crate) fn find_indexer(&self, index_type: &str) -> NitriteResult<NitriteIndexer> {
        match self.plugin_manager.get_indexer(index_type) {
            Some(indexer) => Ok(indexer),
            None => {
                log::error!("No indexer plugin found for type: {}", index_type);
                Err(NitriteError::new(
                    &format!("No indexer plugin found for type: {}", index_type),
                    ErrorKind::PluginError,
                ))
            }
        }
    }

    /// Loads a Nitrite module into the configuration.
    pub(crate) fn load_module<T: NitriteModule + 'static>(&self, module: T) -> NitriteResult<()> {
        if self.configured.load(Ordering::Relaxed) {
            log::error!("Cannot load module after initialization");
            return Err(NitriteError::new(
                "Cannot load module after initialization",
                ErrorKind::InvalidOperation,
            ));
        }
        self.plugin_manager.load_module(Box::new(module))
    }

    /// Automatically discovers and loads available plugins.
    pub(crate) fn auto_configure(&self) -> NitriteResult<()> {
        if self.configured.load(Ordering::Relaxed) {
            log::error!("Cannot auto-configure after initialization");
            return Err(NitriteError::new(
                "Cannot auto-configure after initialization",
                ErrorKind::InvalidOperation,
            ));
        }

        self.plugin_manager.load_plugins()
            .map_err(|e| NitriteError::new(&format!("Failed to auto-configure plugins: {}", e), e.kind().clone()))?;
        Ok(())
    }

    /// Closes the configuration and all plugins.
    pub(crate) fn close(&self) -> NitriteResult<()> {
        self.plugin_manager.close()
            .map_err(|e| NitriteError::new(&format!("Failed to close nitrite configuration: {}", e), e.kind().clone()))
    }

    /// Returns the current database schema version.
    pub(crate) fn schema_version(&self) -> u32 {
        self.schema_version.load(Ordering::Relaxed)
    }

    /// Sets the database schema version.
    pub(crate) fn set_schema_version(&self, version: u32) -> NitriteResult<()> {
        if self.configured.load(Ordering::Relaxed) {
            log::error!("Schema version cannot be changed after initialization");
            return Err(NitriteError::new(
                "Schema version cannot be changed after initialization",
                ErrorKind::InvalidOperation,
            ));
        }
        self.schema_version.store(version, Ordering::Relaxed);
        Ok(())
    }

    /// Adds a migration to be executed during initialization.
    pub(crate) fn add_migration(&self, migration: Migration) -> NitriteResult<()> {
        if self.configured.load(Ordering::Relaxed) {
            log::error!("Cannot add migration after initialization");
            return Err(NitriteError::new(
                "Cannot add migration after initialization",
                ErrorKind::InvalidOperation,
            ));
        }

        let start = migration.from_version();
        let end = migration.to_version();

        let mut migration_map = if let Some(entry) = self.migrations.get(&start) {
            entry.value().clone()
        } else {
            BTreeMap::new()
        };

        migration_map.insert(end, migration);

        self.migrations.insert(start, migration_map);
        Ok(())
    }

    /// Returns all registered migrations.
    pub(crate) fn migrations(&self) -> DashMap<u32, BTreeMap<u32, Migration>> {
        self.migrations.clone()
    }

    /// Sets the database file path (can only be set once).
    pub(crate) fn set_db_path(&self, db_path: &str) -> NitriteResult<()> {
        self.db_path.get_or_init(|| db_path.to_string());
        Ok(())
    }

    /// Returns the database file path if set.
    pub(crate) fn db_path(&self) -> Option<String> {
        self.db_path.get().cloned()
    }

    /// Initializes all plugins. Called internally during setup.
    pub(crate) fn initialize(&self) -> NitriteResult<()> {
        self.configured.store(true, Ordering::Relaxed);
        self.plugin_manager.initialize_plugins()
            .map_err(|e| NitriteError::new(&format!("Failed to initialize nitrite configuration plugins: {}", e), e.kind().clone()))
    }

    /// Sets the parent Nitrite instance reference in the plugin manager.
    pub(crate) fn set_nitrite_config(&self, nitrite_config: NitriteConfig) {
        self.plugin_manager.set_nitrite_config(nitrite_config);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NitritePlugin;
    use crate::common::PluginRegistrar;
    use std::sync::atomic::Ordering;

    struct MockNitriteModule(bool);

    impl NitriteModule for MockNitriteModule {
        fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
            Ok(vec![])
        }

        fn load(&self, _plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
            if self.0 {
                Ok(())
            } else {
                Err(NitriteError::new("Failed to load module", ErrorKind::IOError))
            }
        }
    }

    #[test]
    fn test_arc_sharing() {
        // Verify that cloned configs share the same inner implementation
        let config1 = NitriteConfig::new();
        let config2 = config1.clone();

        assert!(Arc::ptr_eq(&config1.inner, &config2.inner),
            "Cloned configs should share the same Arc pointer");
    }

    #[test]
    fn test_new() {
        NitriteConfig::default().set_field_separator(".").ok();

        let config = NitriteConfig::new();
        assert_eq!(config.field_separator(), ".");
        assert_eq!(config.schema_version(), INITIAL_SCHEMA_VERSION);
        assert!(config.db_path().is_none());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_set_field_separator() {
        let config = NitriteConfig::new();
        assert!(config.set_field_separator("|").is_ok());
        assert_eq!(config.field_separator(), "|");
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_set_field_separator_after_initialization() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);
        let result = config.set_field_separator("|");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_set_field_separator_empty() {
        let config = NitriteConfig::new();
        let result = config.set_field_separator("");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_load_module() {
        let config = NitriteConfig::new();
        let module = MockNitriteModule(true);
        assert!(config.load_module(module).is_ok());
    }

    #[test]
    fn test_load_module_after_initialization() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);
        let module = MockNitriteModule(true);
        let result = config.load_module(module);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
    }

    #[test]
    fn test_auto_configure() {
        let config = NitriteConfig::new();
        assert!(config.auto_configure().is_ok());
    }

    #[test]
    fn test_auto_configure_after_initialization() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);
        let result = config.auto_configure();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
    }

    #[test]
    fn test_close() {
        let config = NitriteConfig::new();
        assert!(config.close().is_ok());
    }

    #[test]
    fn test_set_schema_version() {
        let config = NitriteConfig::new();
        assert!(config.set_schema_version(2).is_ok());
        assert_eq!(config.schema_version(), 2);
    }

    #[test]
    fn test_set_schema_version_after_initialization() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);
        let result = config.set_schema_version(2);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
    }

    #[test]
    fn test_set_db_path() {
        let config = NitriteConfig::new();
        assert!(config.set_db_path("/path/to/db").is_ok());
        assert_eq!(config.db_path().unwrap(), "/path/to/db");
    }

    #[test]
    fn test_set_db_path_after_initialization() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);
        let result = config.set_db_path("/path/to/db");
        assert!(result.is_ok());
    }

    #[test]
    fn test_initialize() {
        let config = NitriteConfig::new();
        assert!(config.initialize().is_ok());
        assert!(config.inner.configured.load(Ordering::Relaxed));
    }

    #[test]
    fn test_initialize_plugins() {
        let config = NitriteConfig::new();
        config.auto_configure().unwrap();
        config.initialize().unwrap();
        assert!(config.inner.plugin_manager.initialize_plugins().is_ok());
    }

    #[test]
    fn test_find_indexer() {
        let config = NitriteConfig::new();
        let result = config.find_indexer("unique");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), &ErrorKind::PluginError);
    }

    #[test]
    fn test_nitrite_store() {
        let config = NitriteConfig::new();
        let result = config.nitrite_store();
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().kind(), &ErrorKind::PluginError);
    }

    #[test]
    fn test_configured_state_atomic_access() {
        let config = NitriteConfig::new();

        assert!(!config.inner.configured.load(Ordering::Relaxed));
        assert!(config.set_field_separator("|").is_ok());

        config.inner.configured.store(true, Ordering::Relaxed);

        let result = config.set_field_separator(".");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);

        NitriteConfig::default().set_field_separator(".").ok();
    }

    #[test]
    fn test_set_field_separator_caches_state() {
        let config = NitriteConfig::new();

        assert!(config.set_field_separator("|").is_ok());
        assert!(config.set_field_separator("-").is_ok());

        assert!(config.set_field_separator("").is_err());
        assert!(!config.inner.configured.load(Ordering::Relaxed));

        NitriteConfig::default().set_field_separator(".").ok();
    }

    #[test]
    fn test_auto_configure_state_validation() {
        let config = NitriteConfig::new();

        assert!(config.auto_configure().is_ok());
        config.inner.configured.store(true, Ordering::Relaxed);

        let result = config.auto_configure();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Cannot auto-configure"));
    }

    #[test]
    fn test_load_module_early_state_check() {
        let config = NitriteConfig::new();
        config.inner.configured.store(true, Ordering::Relaxed);

        let result = config.load_module(MockNitriteModule(true));
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), &ErrorKind::InvalidOperation);
    }

    #[test]
    fn test_set_schema_version_concurrent_access() {
        let config = NitriteConfig::new();

        assert!(config.set_schema_version(1).is_ok());
        assert_eq!(config.schema_version(), 1);

        assert!(config.set_schema_version(5).is_ok());
        assert_eq!(config.schema_version(), 5);
    }

    #[test]
    fn test_db_path_onclock_efficiency() {
        let config = NitriteConfig::new();

        assert!(config.set_db_path("/path/to/db1").is_ok());
        assert_eq!(config.db_path(), Some("/path/to/db1".to_string()));

        assert!(config.set_db_path("/path/to/db2").is_ok());
        assert_eq!(config.db_path(), Some("/path/to/db1".to_string()),
            "OnceLock should preserve first value");
    }
}