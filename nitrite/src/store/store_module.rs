use crate::common::NitriteModule;
use crate::errors::NitriteResult;
use crate::store::NitriteStore;

pub trait StoreModule: NitriteModule {
    fn get_store(&self) -> NitriteResult<NitriteStore>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::NitritePlugin;
    use crate::common::PluginRegistrar;
    use crate::errors::{ErrorKind, NitriteError};
    use crate::store::NitriteStore;

    struct MockStoreModule {
        store: Option<NitriteStore>,
    }

    impl NitriteModule for MockStoreModule {
        fn plugins(&self) -> NitriteResult<Vec<NitritePlugin>> {
            Ok(vec![])
        }

        fn load(&self, _plugin_registrar: &PluginRegistrar) -> NitriteResult<()> {
            Ok(())
        }
    }

    impl StoreModule for MockStoreModule {
        fn get_store(&self) -> NitriteResult<NitriteStore> {
            match &self.store {
                Some(store) => Ok(store.clone()),
                None => Err(NitriteError::new("Store is closed", ErrorKind::IOError)),
            }
        }
    }

    #[test]
    fn test_get_store_positive() {
        let store = NitriteStore::default(); // Assuming NitriteStore has a new() method
        let module = MockStoreModule { store: Some(store.clone()) };
        let result = module.get_store();
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_store_negative() {
        let module = MockStoreModule { store: None };
        let result = module.get_store();
        assert!(result.is_err());
    }
}