extern crate nitrite;


#[cfg(test)]
mod tests {
    use nitrite::common::Value;
    use nitrite::filter::field;
    use nitrite::nitrite::Nitrite;
    use nitrite::nitrite_config::NitriteConfig;
    use nitrite::store::memory::InMemoryStoreModule;
    use nitrite::store::StoreModule;
    use nitrite::{doc, key, val};
    use nitrite_fjall_adapter::FjallModule;

    #[test]
    fn test_in_memory_store() {
        let nitrite_config = NitriteConfig::new();
        let store_module = InMemoryStoreModule::with_config().build();
        let store = store_module.get_store().unwrap();
        store.initialize(nitrite_config.clone()).unwrap();

        assert!(!store.is_closed().unwrap());
        store.close().unwrap();

        assert!(store.is_closed().unwrap());

        let store_module = InMemoryStoreModule::with_config().build();
        let store = store_module.get_store().unwrap();
        store.initialize(nitrite_config.clone()).unwrap();

        let map = store.open_map("test").unwrap();
        map.put(key!("key1"), val!("value1")).unwrap();
        map.put(key!("key2"), val!("value2")).unwrap();
        map.put(key!("key3"), val!("value3")).unwrap();
        map.put(key!("key4"), val!("value4")).unwrap();

        assert_eq!(map.size().unwrap(), 4);
        assert_eq!(map.get(&key!("key1")).unwrap().unwrap(), val!("value1"));
        assert_eq!(map.get(&key!("key2")).unwrap().unwrap(), val!("value2"));
        assert_eq!(map.get(&key!("key3")).unwrap().unwrap(), val!("value3"));
        assert_eq!(map.get(&key!("key4")).unwrap().unwrap(), val!("value4"));

        map.remove(&key!("key1")).unwrap();
        assert_eq!(map.get(&key!("key1")).unwrap(), None);
        assert_eq!(map.size().unwrap(), 3);

        map.remove(&key!("key2")).unwrap();
        assert_eq!(map.get(&key!("key2")).unwrap(), None);
        assert_eq!(map.size().unwrap(), 2);

        map.remove(&key!("key3")).unwrap();
        assert_eq!(map.get(&key!("key3")).unwrap(), None);
        assert_eq!(map.size().unwrap(), 1);

        map.remove(&key!("key4")).unwrap();
        assert_eq!(map.get(&key!("key4")).unwrap(), None);
        assert_eq!(map.size().unwrap(), 0);

        for i in 0..3 {
            map.put(key!(i), val!(i)).unwrap();
        }

        for i in 0..3 {
            assert_eq!(map.get(&key!(i)).unwrap().unwrap(), val!(i));
        }
        assert_eq!(map.size().unwrap(), 3);

        // iterate map keys
        let mut count = 0;
        for key in map.keys().unwrap() {
            assert_eq!(key.unwrap(), key!(count));
            count += 1;
        }
        assert_eq!(count, 3);

        for keys in map.keys().unwrap().rev() {
            count -= 1;
            assert_eq!(keys.unwrap(), key!(count));
        }
        assert_eq!(count, 0);

        for value in map.values().unwrap() {
            assert_eq!(value.unwrap(), val!(count));
            count += 1;
        }
        assert_eq!(count, 3);

        for value in map.values().unwrap().rev() {
            count -= 1;
            assert_eq!(value.unwrap(), val!(count));
        }
        assert_eq!(count, 0);

        for entry in map.entries().unwrap() {
            let (key, value) = entry.unwrap();
            assert_eq!(key, key!(count));
            assert_eq!(value, val!(count));
            count += 1;
        }
        assert_eq!(count, 3);

        for entry in map.entries().unwrap().rev() {
            count -= 1;
            let (key, value) = entry.unwrap();
            assert_eq!(key, key!(count));
            assert_eq!(value, val!(count));
        }
        assert_eq!(count, 0);
    }
    

    #[test]
    fn test_nitrite_db() {
        let db = Nitrite::builder()
            .open_or_create(None, None)
            .expect("Failed to open or create database");

        let collection = db.collection("test").unwrap();
        collection.insert(doc!{"key1": "value1"}).unwrap();

        let mut cursor = collection.find(field("key1").eq("value1")).unwrap();
        let result = cursor.first().unwrap().unwrap();

        assert_eq!(result.get("key1").unwrap(), Value::from("value1"));
        db.close().unwrap();
    }

    #[test]
    fn test_destroy_collection() {
        let db = Nitrite::builder()
            .open_or_create(None, None)
            .expect("Failed to create database");

        // Create a collection (accessed but not explicitly created)
        let collection = db.collection("test_collection").unwrap();
        collection.insert(doc!{"test": "data"}).unwrap();
        assert!(db.has_collection("test_collection").unwrap());

        // Close and reopen the database
        db.close().unwrap();

        let db = Nitrite::builder()
            .open_or_create(None, None)
            .expect("Failed to reopen database");

        // Check that collection exists
        let collection_exists = db.has_collection("test_collection").unwrap();
        if collection_exists {
            // Destroy the collection
            db.destroy_collection("test_collection").unwrap();

            // Verify collection is destroyed
            assert!(!db.has_collection("test_collection").unwrap());
        }

        db.close().unwrap();
    }

    #[test]
    fn test_db_write_close_read() {
        // Use a temporary file path for persistence across close/reopen
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_test_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();
            
            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            // Write data
            let collection = db.collection("test").unwrap();
            collection.insert(doc!{"first_name": "fn1", "last_name": "ln1"}).unwrap();
            collection.insert(doc!{"first_name": "fn2", "last_name": "ln2"}).unwrap();
            collection.insert(doc!{"first_name": "fn3", "last_name": "ln2"}).unwrap();

            // Create indices
            collection.create_index(vec!["first_name"], &nitrite::index::unique_index()).unwrap();
            collection.create_index(vec!["last_name"], &nitrite::index::non_unique_index()).unwrap();

            db.close().unwrap();
        }

        // Reopen and read
        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();
            
            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to reopen database");

            let collection = db.collection("test").unwrap();
            let cursor = collection.find(field("last_name").eq("ln2")).unwrap();
            assert_eq!(cursor.count(), 2);

            db.close().unwrap();
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_production_preset_configuration() {
        // Use a temporary file path for persistence
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_prod_preset_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            // Use production preset with default optimized settings
            let storage_module = FjallModule::with_config()
                .production_preset()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database with production preset");

            // Insert a batch of documents to test production settings
            let collection = db.collection("production_test").unwrap();
            for i in 0..100 {
                let name = format!("item_{}", i);
                let data = format!("payload_data_{}", i);
                collection
                    .insert(doc!{"id": i, "name": name, "data": data})
                    .unwrap();
            }

            // Create an index to test bloom filter benefit
            collection
                .create_index(vec!["id"], &nitrite::index::unique_index())
                .unwrap();

            // Query using the indexed field (bloom filter should help)
            let cursor = collection.find(field("id").eq(50)).unwrap();
            assert_eq!(cursor.count(), 1);

            // Query for non-existent value (bloom filter should quickly reject)
            let cursor = collection.find(field("id").eq(9999)).unwrap();
            assert_eq!(cursor.count(), 0);

            db.close().unwrap();
        }

        // Reopen and verify data persisted
        {
            let storage_module = FjallModule::with_config()
                .production_preset()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to reopen database");

            let collection = db.collection("production_test").unwrap();
            assert_eq!(collection.size().unwrap(), 100);

            db.close().unwrap();
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_high_throughput_preset_configuration() {
        // Use a temporary file path
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_throughput_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            // Use high throughput preset for batch imports
            let storage_module = FjallModule::with_config()
                .high_throughput_preset()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database with high throughput preset");

            let collection = db.collection("batch_import").unwrap();

            // Simulate batch import with many documents
            let start = std::time::Instant::now();
            for i in 0..500 {
                let content = format!("batch_data_{}", i);
                let timestamp = i as i64 * 1000;
                collection
                    .insert(doc!{"batch_id": i, "content": content, "timestamp": timestamp})
                    .unwrap();
            }
            let duration = start.elapsed();

            // High throughput preset should complete quickly
            // (this is a sanity check, not a strict benchmark)
            assert!(
                duration.as_secs() < 10,
                "Batch insert took too long: {:?}",
                duration
            );

            assert_eq!(collection.size().unwrap(), 500);

            // Manually commit since manual_journal_persist is enabled
            db.commit().unwrap();
            db.close().unwrap();
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_low_memory_preset_configuration() {
        // Use a temporary file path
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_low_mem_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            // Use low memory preset for constrained environments
            let storage_module = FjallModule::with_config()
                .low_memory_preset()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database with low memory preset");

            let collection = db.collection("low_mem_test").unwrap();

            // Insert moderate amount of data
            for i in 0..50 {
                let value = format!("low_mem_value_{}", i);
                collection
                    .insert(doc!{"id": i, "value": value})
                    .unwrap();
            }

            // Create index (bloom filter is enabled in low memory mode for disk savings)
            collection
                .create_index(vec!["id"], &nitrite::index::unique_index())
                .unwrap();

            // Verify basic operations work
            let cursor = collection.find(field("id").lt(10)).unwrap();
            assert_eq!(cursor.count(), 10);

            db.close().unwrap();
        }

        // Reopen and verify
        {
            let storage_module = FjallModule::with_config()
                .low_memory_preset()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to reopen database");

            let collection = db.collection("low_mem_test").unwrap();
            assert_eq!(collection.size().unwrap(), 50);

            db.close().unwrap();
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_preset_with_custom_overrides() {
        // Test that presets can be customized
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_custom_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            // Start with production preset but override some settings
            let storage_module = FjallModule::with_config()
                .production_preset()
                .block_cache_capacity(128 * 1024 * 1024) // Override to 128MB
                .fsync_frequency(50) // More frequent fsyncs
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database with custom overrides");

            let collection = db.collection("custom_test").unwrap();
            collection.insert(doc!{"test": "custom_preset"}).unwrap();

            assert_eq!(collection.size().unwrap(), 1);

            db.close().unwrap();
        }

        // Cleanup
        let _ = std::fs::remove_dir_all(&db_path);
    }

    // =================== put_all batch write integration tests ===================

    #[test]
    fn test_put_all_integration_basic() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_put_all_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            // Get the underlying store to test put_all directly
            let store = db.store();
            let map = store.open_map("test_put_all").unwrap();

            // Create batch of entries
            let entries: Vec<_> = (0..100)
                .map(|i| (key!(format!("batch_key_{:04}", i)), val!(i)))
                .collect();

            // Execute batch write
            map.put_all(entries).expect("put_all should succeed");

            // Verify all entries
            assert_eq!(map.size().unwrap(), 100);
            assert_eq!(map.get(&key!("batch_key_0000")).unwrap(), Some(val!(0)));
            assert_eq!(map.get(&key!("batch_key_0050")).unwrap(), Some(val!(50)));
            assert_eq!(map.get(&key!("batch_key_0099")).unwrap(), Some(val!(99)));

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_put_all_persistence() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_put_all_persist_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        // Write data
        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            let store = db.store();
            let map = store.open_map("persist_test").unwrap();

            let entries: Vec<_> = (0..50)
                .map(|i| (key!(format!("persist_{}", i)), val!(format!("data_{}", i))))
                .collect();

            map.put_all(entries).expect("put_all should succeed");
            
            // Commit to ensure durability
            db.commit().unwrap();
            db.close().unwrap();
        }

        // Reopen and verify
        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to reopen database");

            let store = db.store();
            let map = store.open_map("persist_test").unwrap();

            // All data should be persisted
            assert_eq!(map.size().unwrap(), 50);
            assert_eq!(
                map.get(&key!("persist_0")).unwrap(),
                Some(val!("data_0"))
            );
            assert_eq!(
                map.get(&key!("persist_49")).unwrap(),
                Some(val!("data_49"))
            );

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_put_all_mixed_with_individual_puts() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_put_all_mixed_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            let store = db.store();
            let map = store.open_map("mixed_test").unwrap();

            // Individual puts first
            for i in 0..10 {
                map.put(key!(format!("individual_{}", i)), val!(i)).unwrap();
            }

            // Batch write
            let batch_entries: Vec<_> = (0..20)
                .map(|i| (key!(format!("batch_{}", i)), val!(i * 10)))
                .collect();
            map.put_all(batch_entries).expect("put_all should succeed");

            // More individual puts
            for i in 10..20 {
                map.put(key!(format!("individual_{}", i)), val!(i)).unwrap();
            }

            // Verify all data
            assert_eq!(map.size().unwrap(), 40);

            // Verify individual puts
            assert_eq!(map.get(&key!("individual_5")).unwrap(), Some(val!(5)));
            assert_eq!(map.get(&key!("individual_15")).unwrap(), Some(val!(15)));

            // Verify batch puts
            assert_eq!(map.get(&key!("batch_5")).unwrap(), Some(val!(50)));
            assert_eq!(map.get(&key!("batch_15")).unwrap(), Some(val!(150)));

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_put_all_performance_comparison() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_put_all_perf_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            let store = db.store();

            // Test individual puts
            let map_individual = store.open_map("perf_individual").unwrap();
            let start = std::time::Instant::now();
            for i in 0..500 {
                map_individual
                    .put(key!(format!("key_{:04}", i)), val!(i))
                    .unwrap();
            }
            let individual_time = start.elapsed();

            // Test batch puts
            let map_batch = store.open_map("perf_batch").unwrap();
            let entries: Vec<_> = (0..500)
                .map(|i| (key!(format!("key_{:04}", i)), val!(i)))
                .collect();
            
            let start = std::time::Instant::now();
            map_batch.put_all(entries).expect("put_all should succeed");
            let batch_time = start.elapsed();

            println!(
                "Performance comparison (500 entries): individual={:?}, batch={:?}, speedup={:.2}x",
                individual_time,
                batch_time,
                individual_time.as_secs_f64() / batch_time.as_secs_f64()
            );

            // Both should have same data
            assert_eq!(map_individual.size().unwrap(), 500);
            assert_eq!(map_batch.size().unwrap(), 500);

            // Batch should generally be faster (or at least not slower)
            // This assertion is lenient to avoid flaky tests
            assert!(
                batch_time.as_millis() <= individual_time.as_millis() * 2,
                "Batch should not be significantly slower than individual puts"
            );

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }

    // =================== Collection name encoding tests ===================

    #[test]
    fn test_collection_names_with_pipe_character() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_pipe_names_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            // Test collection names with pipe characters (namespace separator)
            let collection1 = db.collection("namespace|collection").unwrap();
            collection1.insert(doc!{"test_field": 1}).unwrap();
            assert_eq!(collection1.size().unwrap(), 1);

            let collection2 = db.collection("app|users|profiles").unwrap();
            collection2.insert(doc!{"test_field": 2}).unwrap();
            assert_eq!(collection2.size().unwrap(), 1);

            let collection3 = db.collection("system|metadata|indexes").unwrap();
            collection3.insert(doc!{"test_field": 3}).unwrap();
            assert_eq!(collection3.size().unwrap(), 1);

            // Verify retrieval
            let docs: Vec<_> = collection1.find(field("test_field").eq(1)).unwrap().collect();
            assert_eq!(docs.len(), 1);

            db.close().unwrap();
        }

        // Reopen and verify persistence
        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to reopen database");

            // All collections should still be accessible
            let collection = db.collection("namespace|collection").unwrap();
            assert_eq!(collection.size().unwrap(), 1);

            let collection = db.collection("app|users|profiles").unwrap();
            assert_eq!(collection.size().unwrap(), 1);

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }

    #[test]
    fn test_collection_names_with_special_characters() {
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join(format!("nitrite_special_names_{}", uuid::Uuid::new_v4()));
        let db_path_str = db_path.to_str().unwrap().to_string();

        {
            let storage_module = FjallModule::with_config()
                .db_path(&db_path_str)
                .build();

            let db = Nitrite::builder()
                .load_module(storage_module)
                .open_or_create(None, None)
                .expect("Failed to create database");

            // Test various special characters in collection names
            let c1 = db.collection("simple_collection").unwrap();
            c1.insert(doc!{"idx": 1}).unwrap();
            assert_eq!(c1.size().unwrap(), 1, "Failed for simple_collection");

            let c2 = db.collection("collection.with.dots").unwrap();
            c2.insert(doc!{"idx": 2}).unwrap();
            assert_eq!(c2.size().unwrap(), 1, "Failed for collection.with.dots");

            let c3 = db.collection("collection-with-dashes").unwrap();
            c3.insert(doc!{"idx": 3}).unwrap();
            assert_eq!(c3.size().unwrap(), 1, "Failed for collection-with-dashes");

            let c4 = db.collection("CamelCaseCollection").unwrap();
            c4.insert(doc!{"idx": 4}).unwrap();
            assert_eq!(c4.size().unwrap(), 1, "Failed for CamelCaseCollection");

            db.close().unwrap();
        }

        let _ = std::fs::remove_dir_all(&db_path);
    }
}
