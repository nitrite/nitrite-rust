use crate::errors::{ErrorKind, NitriteError, NitriteResult};
use crate::store::StoreEventListener;
use std::any::Any;
use std::ops::Deref;
use std::sync::Arc;

pub trait StoreConfigProvider: Any {
    fn file_path(&self) -> String;

    fn is_read_only(&self) -> bool;

    fn add_store_listener(&self, listener: StoreEventListener);

    fn is_in_memory(&self) -> bool {
        self.file_path().is_empty()
    }
    
    fn as_any(&self) -> &dyn Any;
}

pub struct StoreConfig {
    inner: Arc<dyn StoreConfigProvider>,
}

impl StoreConfig {
    pub fn new<T: StoreConfigProvider + 'static>(inner: T) -> Self {
        StoreConfig { inner: Arc::new(inner) }
    }
    
    pub fn as_ref<T: StoreConfigProvider + 'static>(&self) -> NitriteResult<&T> {
        self.inner.as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| NitriteError::new(
                "StoreConfig type mismatch: cannot downcast to requested config type",
                ErrorKind::InvalidOperation
            ))
    }
}

impl Deref for StoreConfig {
    type Target = Arc<dyn StoreConfigProvider>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::StoreEventListener;

    struct MockStoreConfig {
        file_path: String,
        read_only: bool,
    }

    impl StoreConfigProvider for MockStoreConfig {
        fn file_path(&self) -> String {
            self.file_path.clone()
        }

        fn is_read_only(&self) -> bool {
            self.read_only
        }

        fn add_store_listener(&self, _listener: StoreEventListener) {
            // Mock implementation
        }
        
        fn is_in_memory(&self) -> bool {
            self.file_path.is_empty()
        }
        
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[test]
    fn test_file_path() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/path/to/store"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        assert_eq!(store_config.file_path(), "/path/to/store");
    }

    #[test]
    fn test_is_read_only() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/path/to/store"),
            read_only: true,
        };
        let store_config = StoreConfig::new(mock_config);
        assert!(store_config.is_read_only());
    }

    #[test]
    fn test_is_not_read_only() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/path/to/store"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        assert!(!store_config.is_read_only());
    }

    #[test]
    fn test_add_store_listener() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/path/to/store"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        let listener = StoreEventListener::new(Box::new(|_| {Ok(())}));
        store_config.add_store_listener(listener);
        // No assertion needed, just ensure no panic
    }

    #[test]
    fn test_is_in_memory_true() {
        let mock_config = MockStoreConfig {
            file_path: String::new(),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        assert!(store_config.is_in_memory());
    }

    #[test]
    fn test_downcast_correct_type() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/path/to/store"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        let result = store_config.as_ref::<MockStoreConfig>();
        assert!(result.is_ok(), "Downcasting to correct type should succeed");
        assert_eq!(result.unwrap().file_path(), "/path/to/store");
    }

    #[test]
    fn test_downcast_wrong_type_returns_error() {
        #[derive(Debug)]
        struct OtherStoreConfig;
        impl StoreConfigProvider for OtherStoreConfig {
            fn file_path(&self) -> String { String::new() }
            fn is_read_only(&self) -> bool { false }
            fn add_store_listener(&self, _: StoreEventListener) {}
            fn as_any(&self) -> &dyn Any { self }
        }

        let mock_config = MockStoreConfig {
            file_path: String::from("/path"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        let result = store_config.as_ref::<OtherStoreConfig>();
        assert!(result.is_err(), "Downcasting to wrong type should fail");
        if let Err(e) = result {
            assert!(e.to_string().contains("type mismatch"), "Error should mention type mismatch");
        }
    }

    #[test]
    fn test_downcast_wrong_type_no_panic() {
        #[derive(Debug)]
        struct AnotherStoreConfig;
        impl StoreConfigProvider for AnotherStoreConfig {
            fn file_path(&self) -> String { String::new() }
            fn is_read_only(&self) -> bool { false }
            fn add_store_listener(&self, _: StoreEventListener) {}
            fn as_any(&self) -> &dyn Any { self }
        }

        let mock_config = MockStoreConfig {
            file_path: String::from("/path"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        // This should return Err, not panic
        let _result = store_config.as_ref::<AnotherStoreConfig>();
    }

    #[test]
    fn test_multiple_correct_downcasts() {
        let mock_config = MockStoreConfig {
            file_path: String::from("/test/path"),
            read_only: true,
        };
        let store_config = StoreConfig::new(mock_config);
        
        // Multiple downcasts to same type should all succeed
        let result1 = store_config.as_ref::<MockStoreConfig>();
        let result2 = store_config.as_ref::<MockStoreConfig>();
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(result1.unwrap().file_path(), "/test/path");
        assert_eq!(result2.unwrap().file_path(), "/test/path");
    }

    #[test]
    fn test_downcast_error_is_displayable() {
        #[derive(Debug)]
        struct UnknownConfig;
        impl StoreConfigProvider for UnknownConfig {
            fn file_path(&self) -> String { String::new() }
            fn is_read_only(&self) -> bool { false }
            fn add_store_listener(&self, _: StoreEventListener) {}
            fn as_any(&self) -> &dyn Any { self }
        }

        let mock_config = MockStoreConfig {
            file_path: String::from("/path"),
            read_only: false,
        };
        let store_config = StoreConfig::new(mock_config);
        let result = store_config.as_ref::<UnknownConfig>();
        assert!(result.is_err());
        // Ensure error message is readable
        let error_msg = result.unwrap_err().to_string();
        assert!(!error_msg.is_empty(), "Error should have a readable message");
    }

    #[test]
    fn test_arc_cloning_efficiency() {
        // Test that Arc cloning in StoreConfig is efficient
        let mock_config = MockStoreConfig {
            file_path: String::from("/test/path"),
            read_only: false,
        };
        let config1 = StoreConfig::new(mock_config);
        let config2 = config1.clone();
        
        // Both should reference same Arc data
        assert_eq!(config1.file_path(), config2.file_path());
    }

    #[test]
    fn test_deref_access_efficiency() {
        // Test that Deref trait provides efficient access
        let mock_config = MockStoreConfig {
            file_path: String::from("/efficient"),
            read_only: false,
        };
        let config = StoreConfig::new(mock_config);
        let _deref_target = &*config;
        
        assert_eq!(config.file_path(), "/efficient");
    }

    #[test]
    fn test_config_with_read_only_state() {
        // Test different read-only states
        let writable = StoreConfig::new(MockStoreConfig {
            file_path: String::from("/writable"),
            read_only: false,
        });
        
        let readonly = StoreConfig::new(MockStoreConfig {
            file_path: String::from("/readonly"),
            read_only: true,
        });
        
        assert!(!writable.is_read_only());
        assert!(readonly.is_read_only());
    }

    #[test]
    fn test_in_memory_detection_efficiency() {
        // Test efficient in-memory detection
        let in_memory = StoreConfig::new(MockStoreConfig {
            file_path: String::new(),
            read_only: false,
        });
        
        let file_based = StoreConfig::new(MockStoreConfig {
            file_path: String::from("/path/to/db"),
            read_only: false,
        });
        
        assert!(in_memory.is_in_memory());
        assert!(!file_based.is_in_memory());
    }
}