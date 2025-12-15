extern crate core;

mod config;
mod map;
mod module;
mod store;
mod version;
mod wrapper;

pub use config::*;
pub use module::*;

#[cfg(test)]
mod tests {
    use crate::map::FjallMap;
    use crate::store::FjallStore;
    use fjall::{Keyspace, PartitionHandle};
    use nitrite::common::NitritePluginProvider;
    use nitrite::store::NitriteMapProvider;
    use std::mem;

    #[derive(Clone)]
    pub struct Context {
        path: String,
        keyspace: Option<Keyspace>,
        partition_handle: Option<PartitionHandle>,
        fjall_store: Option<FjallStore>,
        fjall_map: Option<FjallMap>,
    }

    impl Context {
        pub fn new(
            path: String,
            keyspace: Option<Keyspace>,
            partition_handle: Option<PartitionHandle>,
            fjall_store: Option<FjallStore>,
            fjall_map: Option<FjallMap>,
        ) -> Self {
            Context {
                path,
                keyspace,
                partition_handle,
                fjall_store,
                fjall_map,
            }
        }

        pub fn path(&self) -> String {
            self.path.clone()
        }

        pub fn keyspace(&self) -> Result<Keyspace, String> {
            self.keyspace
                .clone()
                .ok_or_else(|| "Keyspace not available".to_string())
        }

        pub fn partition_handle(&self) -> Result<PartitionHandle, String> {
            self.partition_handle
                .clone()
                .ok_or_else(|| "Partition handle not available".to_string())
        }

        pub fn fjall_store(&self) -> Result<FjallStore, String> {
            self.fjall_store
                .clone()
                .ok_or_else(|| "FjallStore not available".to_string())
        }

        pub fn fjall_map(&self) -> Result<FjallMap, String> {
            self.fjall_map
                .clone()
                .ok_or_else(|| "FjallMap not available".to_string())
        }

        #[cfg(test)]
        pub fn fjall_store_unsafe(&self) -> FjallStore {
            self.fjall_store.clone().expect("FjallStore not available")
        }

        #[cfg(test)]
        pub fn fjall_map_unsafe(&self) -> FjallMap {
            self.fjall_map.clone().expect("FjallMap not available")
        }
    }

    impl Drop for Context {
        fn drop(&mut self) {
            let keyspace = mem::replace(&mut self.keyspace, None);
            let partition_handle = mem::replace(&mut self.partition_handle, None);
            let fjall_store = mem::replace(&mut self.fjall_store, None);
            let fjall_map = mem::replace(&mut self.fjall_map, None);

            // Close fjall_map if available, log on failure but don't panic
            if let Some(map) = fjall_map {
                match map.close() {
                    Ok(_) => {
                        drop(map);
                    }
                    Err(e) => {
                        log::error!("Failed to close FjallMap during cleanup: {}", e);
                    }
                }
            }

            // Close fjall_store if available, log on failure but don't panic
            if let Some(store) = fjall_store {
                match store.close() {
                    Ok(_) => {
                        drop(store);
                    }
                    Err(e) => {
                        log::error!("Failed to close FjallStore during cleanup: {}", e);
                    }
                }
            }

            // Drop partition_handle gracefully
            if let Some(handle) = partition_handle {
                drop(handle)
            }

            // Drop keyspace gracefully
            if let Some(ks) = keyspace {
                drop(ks)
            }
        }
    }

    pub fn run_test<T, B, A>(before: B, test: T, after: A) -> ()
    where
        T: FnOnce(Context) -> () + std::panic::UnwindSafe,
        B: FnOnce() -> Context + std::panic::UnwindSafe,
        A: FnOnce(Context) -> () + std::panic::UnwindSafe,
    {
        let result = std::panic::catch_unwind(|| {
            let ctx = before();
            test(ctx.clone());
            after(ctx.clone());
            drop(ctx);
        });

        match result {
            Ok(_) => (),
            Err(e) => {
                // Preserve original panic message instead of masking with generic error
                let panic_msg = if let Some(msg) = e.downcast_ref::<String>() {
                    msg.clone()
                } else if let Some(msg) = e.downcast_ref::<&str>() {
                    msg.to_string()
                } else {
                    format!("{:?}", e)
                };

                eprintln!("Test execution failed with panic: {}", panic_msg);
                panic!("Test execution failed with panic: {}", panic_msg);
            }
        }
    }

    #[cfg(test)]
    mod context_tests {
        use super::*;

        #[test]
        fn test_context_keyspace_returns_result_on_error() {
            let ctx = Context::new("test".to_string(), None, None, None, None);

            let result = ctx.keyspace();
            assert!(result.is_err());
        }

        #[test]
        fn test_context_partition_handle_returns_result_on_error() {
            let ctx = Context::new("test".to_string(), None, None, None, None);

            let result = ctx.partition_handle();
            assert!(result.is_err());
        }

        #[test]
        fn test_context_fjall_store_returns_result_on_error() {
            let ctx = Context::new("test".to_string(), None, None, None, None);

            let result = ctx.fjall_store();
            assert!(result.is_err());
        }

        #[test]
        fn test_context_fjall_map_returns_result_on_error() {
            let ctx = Context::new("test".to_string(), None, None, None, None);

            let result = ctx.fjall_map();
            assert!(result.is_err());
        }

        #[test]
        fn test_context_drop_handles_empty_state() {
            let _ctx = Context::new("test".to_string(), None, None, None, None);
            // Drop implicitly called, should not panic
        }

        #[test]
        fn test_context_drop_handles_error_gracefully() {
            // Create a context with None values
            let ctx = Context::new("test".to_string(), None, None, None, None);

            // Drop should handle None values gracefully without panicking
            drop(ctx);
        }

        #[test]
        fn test_harness_preserves_string_panic_message() {
            let test_msg = "Expected string panic message";
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {
                        panic!("{}", test_msg);
                    },
                    |_ctx| {},
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                assert!(
                    panic_msg.contains(test_msg),
                    "Panic message should contain original message, got: {}",
                    panic_msg
                );
            } else {
                panic!("Expected String panic message");
            }
        }

        #[test]
        fn test_harness_does_not_mask_assertion_failures() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {
                        assert!(false, "Assertion failure with custom message");
                    },
                    |_ctx| {},
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                assert!(
                    panic_msg.contains("Assertion failure with custom message"),
                    "Should preserve assertion failure message: {}",
                    panic_msg
                );
            }
        }

        #[test]
        fn test_harness_preserves_panic_from_before_hook() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || {
                        panic!("Setup failed: database connection error");
                    },
                    |_ctx| {},
                    |_ctx| {},
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                assert!(
                    panic_msg.contains("Setup failed"),
                    "Should preserve setup failure message: {}",
                    panic_msg
                );
            }
        }

        #[test]
        fn test_harness_preserves_panic_from_after_hook() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {},
                    |_ctx| {
                        panic!("Cleanup failed: resource leak detected");
                    },
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                assert!(
                    panic_msg.contains("Cleanup failed"),
                    "Should preserve cleanup failure message: {}",
                    panic_msg
                );
            }
        }

        #[test]
        fn test_harness_succeeds_with_no_panic() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {
                        // Success - no panic
                    },
                    |_ctx| {},
                );
            });

            assert!(caught.is_ok(), "Test should succeed without panic");
        }

        #[test]
        fn test_harness_error_message_format_includes_panic_prefix() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {
                        panic!("Specific error condition");
                    },
                    |_ctx| {},
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                // Check that error includes both the prefix and the original message
                assert!(
                    panic_msg.contains("Test execution failed with panic"),
                    "Should include error prefix: {}",
                    panic_msg
                );
                assert!(
                    panic_msg.contains("Specific error condition"),
                    "Should include original panic message: {}",
                    panic_msg
                );
            }
        }

        #[test]
        fn test_harness_handles_generic_panic_without_message() {
            let caught = std::panic::catch_unwind(|| {
                run_test(
                    || Context::new("test".to_string(), None, None, None, None),
                    |_ctx| {
                        std::panic::panic_any(42); // Panic with non-string value
                    },
                    |_ctx| {},
                );
            });

            assert!(caught.is_err());
            let err = caught.err().unwrap();
            if let Some(panic_msg) = err.downcast_ref::<String>() {
                // Should still provide useful error message for non-string panics
                assert!(
                    panic_msg.contains("Test execution failed with panic"),
                    "Should provide error prefix for generic panics: {}",
                    panic_msg
                );
            }
        }
    }
}
