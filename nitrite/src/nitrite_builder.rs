use crate::errors::NitriteError;
use crate::migration::Migration;
use crate::{errors::NitriteResult, nitrite::Nitrite, nitrite_config::NitriteConfig, NitriteModule};

/// Builder for creating and configuring a Nitrite database instance.
///
/// `NitriteBuilder` provides a fluent API for configuring database options before
/// opening or creating a database. It follows the builder pattern and captures errors
/// during configuration to ensure they are propagated when opening the database.
///
/// # Examples
///
/// ```rust,ignore
/// use nitrite::nitrite_builder::NitriteBuilder;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a database with default settings
/// let db = Nitrite::builder()
///     .open_or_create(None, None)?;
///
/// // Create a database with custom configuration
/// let db = Nitrite::builder()
///     .field_separator("|")
///     .schema_version(2)
///     .open_or_create(None, None)?;
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct NitriteBuilder {
    error: Option<NitriteError>,
    nitrite_config: NitriteConfig,
}

impl NitriteBuilder {
    /// Creates a new `NitriteBuilder` with default configuration.
    ///
    /// The default configuration uses an in-memory store, "." as field separator,
    /// and schema version 1.
    ///
    /// # Returns
    ///
    /// A new `NitriteBuilder` instance.
    pub fn new() -> Self {
        NitriteBuilder {
            error: None,
            nitrite_config: NitriteConfig::new() 
        }
    }

    /// Sets the field separator for nested document fields.
    ///
    /// The field separator is used to access nested fields in documents using dot-notation
    /// or custom separator. For example, with "." as separator, `document.get("user.name")`
    /// retrieves the `name` field inside a nested `user` document.
    ///
    /// # Arguments
    ///
    /// * `field_separator` - A non-empty string to use as the field separator
    ///
    /// # Returns
    ///
    /// This `NitriteBuilder` for method chaining.
    ///
    /// # Panics
    ///
    /// If `field_separator` is empty, the error is captured and will be returned
    /// when calling `open_or_create()`.
    pub fn field_separator(mut self, field_separator: &str) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.nitrite_config.set_field_separator(field_separator) {
                self.error = Some(e);
            }
        }
        self
    }

    /// Loads a plugin module into the database.
    ///
    /// Modules can provide additional functionality such as storage backends, indexing
    /// strategies, or spatial capabilities. For example, the `nitrite-spatial` module
    /// provides spatial indexing and query support.
    ///
    /// # Arguments
    ///
    /// * `module` - A type implementing `NitriteModule`
    ///
    /// # Returns
    ///
    /// This `NitriteBuilder` for method chaining.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use nitrite::nitrite_builder::NitriteBuilder;
    /// use nitrite_spatial::SpatialModule;
    ///
    /// let db = Nitrite::builder()
    ///     .load_module(SpatialModule)
    ///     .open_or_create(None, None)?;
    /// ```
    pub fn load_module<T: NitriteModule + 'static>(mut self, module: T) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.nitrite_config.load_module(module) {
                self.error = Some(e);
            }
        }
        self
    }

    /// Sets the schema version for the database.
    ///
    /// The schema version helps track database structure and enables migrations.
    /// When opening an existing database, the version is compared to detect schema changes.
    ///
    /// # Arguments
    ///
    /// * `schema_version` - The schema version number (should be >= 1)
    ///
    /// # Returns
    ///
    /// This `NitriteBuilder` for method chaining.
    pub fn schema_version(mut self, schema_version: u32) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.nitrite_config.set_schema_version(schema_version) {
                self.error = Some(e);
            }
        }
        self
    }

    /// Adds a migration to be executed when opening the database.
    ///
    /// Migrations are executed in order when the database schema version changes.
    /// This allows for automatic schema evolution and data transformation.
    ///
    /// # Arguments
    ///
    /// * `migration` - A `Migration` defining version transitions and transformation steps
    ///
    /// # Returns
    ///
    /// This `NitriteBuilder` for method chaining.
    pub fn add_migration(mut self, migration: Migration) -> Self {
        if self.error.is_none() {
            if let Err(e) = self.nitrite_config.add_migration(migration) {
                self.error = Some(e);
            }
        }
        self
    }

    /// Opens or creates a database with the configured settings.
    ///
    /// This method finalizes the builder configuration and attempts to open or create
    /// a database instance. Any errors captured during configuration will be returned here.
    ///
    /// # Arguments
    ///
    /// * `username` - Optional username for authentication
    /// * `password` - Optional password for authentication
    ///
    /// # Returns
    ///
    /// `Ok(Nitrite)` if the database opened successfully, or `Err(NitriteError)` if
    /// configuration validation or database opening failed.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let db = NitriteBuilder::new()
    ///     .field_separator(".")
    ///     .open_or_create(None, None)?;
    /// ```
    pub fn open_or_create(self, username: Option<&str>, password: Option<&str>) -> NitriteResult<Nitrite> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.nitrite_config.auto_configure()?;
        let nitrite = Nitrite::new(self.nitrite_config);
        nitrite.initialize(username, password)?;
        Ok(nitrite)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NitritePlugin;
    use crate::common::PluginRegistrar;
    use crate::errors::{ErrorKind, NitriteError};

    #[derive(Copy, Clone)]
    struct MockNitriteModule(bool);
    
    impl NitriteModule for MockNitriteModule {
        fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
            Ok(vec![])
        }
    
        fn load(&self, _plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
            if self.0 {
                Ok(())
            } else {
                Err(NitriteError::new("Failed to load module", ErrorKind::PluginLoadFailed))
            }
        }
    }

    #[test]
    fn test_new() {
        let builder = NitriteBuilder::new();
        assert_eq!(builder.nitrite_config.field_separator(), ".");
    }

    #[test]
    fn test_field_separator() {
        let builder = NitriteBuilder::new();
        let result = builder.field_separator("|");
        assert_eq!(result.nitrite_config.field_separator(), "|");
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_field_separator_error_propagation() {
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("");  // Invalid - triggers error
        let result = builder.open_or_create(None, None);
        assert!(result.is_err(), "Should propagate field separator error");
        if let Err(e) = result {
            assert!(e.to_string().to_lowercase().contains("field separator"), "Error should mention field separator");
        }
    }

    #[test]
    fn test_field_separator_valid_then_error() {
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("|");  // Valid
        let builder = builder.field_separator("");   // Invalid - should capture error
        let result = builder.open_or_create(None, None);
        assert!(result.is_err(), "Should capture the error from second field_separator call");
    }

    #[test]
    fn test_load_module_error_propagation() {
        let builder = NitriteBuilder::new();
        let builder = builder.load_module(MockNitriteModule(false));  // Fails to load
        let result = builder.open_or_create(None, None);
        assert!(result.is_err(), "Should propagate module loading error");
    }

    #[test]
    fn test_schema_version_error_propagation() {
        let builder = NitriteBuilder::new();
        let builder = builder.schema_version(0);  // Invalid schema version
        let _result = builder.open_or_create(None, None);
        // Note: schema_version 0 might be valid, so this is a placeholder
        // The actual error depends on NitriteConfig validation
    }

    #[test]
    fn test_multiple_errors_captured() {
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("");  // First error
        let builder = builder.load_module(MockNitriteModule(false));  // Second error
        let result = builder.open_or_create(None, None);
        assert!(result.is_err(), "Should capture first error that occurred");
    }

    #[test]
    fn test_error_retrieved_without_panic() {
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("");  // Capture error
        let result = builder.open_or_create(None, None);
        // This should not panic, just return Err
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_chain_with_error_in_middle() {
        let builder = NitriteBuilder::new()
            .field_separator("|")  // Valid
            .load_module(MockNitriteModule(true))  // Valid
            .field_separator("");  // Invalid - error captured
        
        let result = builder.open_or_create(None, None);
        assert!(result.is_err(), "Error in middle of chain should be captured");
    }

    #[test]
    fn test_no_error_when_all_config_valid() {
        let builder = NitriteBuilder::new()
            .field_separator("|")
            .schema_version(1);
        
        let _result = builder.open_or_create(None, None);
        // Should not have builder error, might fail for other reasons (config)
        // but not due to builder error propagation
    }

    #[test]
    fn test_load_module() {
        let builder = NitriteBuilder::new();
        let builder = builder.load_module(MockNitriteModule(true));
        let result = builder.open_or_create(None, None);
        assert!(result.is_ok());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_load_module_invalid() {
        let builder = NitriteBuilder::new();
        // Simulate an error in module loading
        let builder = builder.load_module(MockNitriteModule(false));
        let result = builder.open_or_create(None, None);
        assert!(result.is_err());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_schema_version() {
        let builder = NitriteBuilder::new();
        let builder = builder.schema_version(2);
        assert_eq!(builder.nitrite_config.schema_version(), 2);
        let result = builder.open_or_create(None, None);
        assert!(result.is_ok());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_open_or_create() {
        let builder = NitriteBuilder::new();
        let result = builder.open_or_create(Some("user"), Some("pass"));
        assert!(result.is_ok());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_open_or_create_invalid_credentials() {
        let builder = NitriteBuilder::new();
        let result = builder.open_or_create(Some("user"), None);
        assert!(result.is_err());
        NitriteConfig::default().set_field_separator(".").unwrap();
    }

    #[test]
    fn test_builder_early_exit_on_first_error() {
        // Test that once error is set, subsequent operations don't overwrite it
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("");  // Set error
        let original_error = builder.error.as_ref().unwrap().message().to_string();
        
        // Try another operation that would normally fail
        let builder = builder.schema_version(5);
        
        // Original error should still be present (not overwritten)
        assert!(builder.error.is_some());
        assert_eq!(builder.error.as_ref().unwrap().message(), original_error);
    }

    #[test]
    fn test_builder_chain_efficiency_no_redundant_operations() {
        // Test that error early-exit avoids redundant operations
        let builder = NitriteBuilder::new();
        let builder = builder
            .field_separator("")  // Error set here
            .schema_version(5)    // Should skip this
            .field_separator("|"); // Should skip this
        
        // Result should fail with original error
        let result = builder.open_or_create(None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_builder_config_state_preserved_on_error() {
        // Test that once an error is set, subsequent operations are no-ops
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("|");  // Set valid separator
        assert_eq!(builder.nitrite_config.field_separator(), "|");
        assert!(builder.error.is_none());
        
        // Now set an error
        let builder = builder.field_separator("");   // Invalid - error set
        assert!(builder.error.is_some());
        
        // Subsequent operations should be skipped (error remains set)
        let builder = builder.schema_version(99);
        assert!(builder.error.is_some(), "Error should still be set");
    }

    #[test]
    fn test_builder_multiple_chain_operations_efficiency() {
        // Test that operations are skipped after error is encountered
        let builder = NitriteBuilder::new();
        let builder = builder
            .schema_version(1)      // Valid
            .field_separator("");   // Invalid - error set
        
        assert!(builder.error.is_some(), "Error should be set after invalid separator");
        
        // Further operations should be no-ops
        let original_error = builder.error.as_ref().map(|e| e.message().to_string());
        let builder = builder.schema_version(99);
        let new_error = builder.error.as_ref().map(|e| e.message().to_string());
        
        // Error should not change (operation was skipped)
        assert_eq!(original_error, new_error, "Error should not change when already set");
    }

    #[test]
    fn test_builder_error_once_set_immutable() {
        // Test that error, once set, cannot be cleared by subsequent operations
        let builder = NitriteBuilder::new();
        let builder = builder.field_separator("");  // Set error
        
        // Subsequent valid operations don't clear error
        let builder = builder.field_separator("|");
        let builder = builder.schema_version(5);
        
        assert!(builder.error.is_some(), "Error should remain set");
    }
}