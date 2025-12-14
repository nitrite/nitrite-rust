#[cfg(test)]
mod tests {
    // use fjall::{Config, PartitionCreateOptions, PersistMode};
    use nitrite::doc;


    #[ctor::ctor]
    fn init() {
        colog::init();
    }

    // #[test]
    // fn test_memory() {
    //     run_test(
    //         || Nitrite::builder().open_or_create(None, None),
    //         |db| {
    //             let coll = db.collection("test")?;
    //             for i in 0..10000 {
    //                 let doc = create_document(
    //                     &format!("key-{}", i),
    //                     Value::from(format!("value-{}", i)),
    //                 )?;
    // 
    //                 let _ = coll.insert(doc)?;
    //                 // assert_eq!(result.get_nitrite_ids().len(), 1);
    //             }
    // 
    //             // let result = coll.find(all())?;
    //             // assert_eq!(result.count(), 10000);
    //             Ok(())
    //         },
    //         |_| Ok(()),
    //     );
    // }
    // 
    // #[test]
    // fn test_fjall_with_file() {
    //     run_test(
    //         || {
    //             let store_module = FjallModule::with_config()
    //                 .db_path("test.fjall")
    //                 .build();
    // 
    //             let db = Nitrite::builder()
    //                 .load_module(store_module)
    //                 .open_or_create(None, None)?;
    //             Ok(db)
    //         },
    //         |db| {
    //             let coll = db.collection("test")?;
    //             for i in 0..10000 {
    //                 let document = create_document(
    //                     &format!("key-{}", i),
    //                     Value::from(format!("value-{}", i)),
    //                 )?;
    //                 let result = coll.insert(document)?;
    //                 assert_eq!(result.get_nitrite_ids().len(), 1);
    //             }
    // 
    //             let result = coll.find(all())?;
    //             let mut count = 0;
    //             for doc in result {
    //                 let doc = doc?;
    //                 let key = &format!("key-{}", count);
    //                 let result = doc.get(key);
    //                 match result {
    //                     Ok(value) => {
    //                         let str_value_opt = value.as_string();
    //                         match str_value_opt {
    //                             Some(str_value) => {
    //                                 assert!(str_value.contains("value"));
    //                             },
    //                             None => {
    //                                 println!("non matching value: {:?} for key: {:?}", value, key);
    //                             }
    //                         }
    //                         assert!(value.as_string().unwrap().contains("value"));
    //                     },
    //                     Err(e) => {
    //                         println!("error: {:?}", e);
    //                     }
    //                 }
    //                 
    //                 assert!(doc.get(&format!("key-{}", count)).unwrap().as_string().unwrap().contains("value"));
    //                 count += 1;
    //             }
    //             assert_eq!(count, 10000);
    //             Ok(())
    //         },
    //         |db| {
    //             db.compact()?;
    //             // db.close()?;
    //             // fs::remove_file("test.fjall").unwrap();
    //             Ok(())
    //         },
    //     );
    // }
    // 
    // #[test]
    // fn test_fjall() {
    //     // fs::remove_dir_all("test2").unwrap_or_default();
    //     // A keyspace is a database, which may contain multiple collections ("partitions")
    //     // You should probably only use a single keyspace for your application
    //     //
    //     // let keyspace = Config::new("test2").open().unwrap(); // or open_transactional for transactional semantics
    //     // 
    //     // // Each partition is its own physical LSM-tree
    //     // let items = keyspace.open_partition("my_items", PartitionCreateOptions::default()).unwrap();
    //     // 
    //     // for i in 0..10000 {
    //     //     items.insert(format!("key-{}", i), format!("value-{}", i)).unwrap();
    //     // }
    //     // 
    //     // // for kv in items.iter() {
    //     // //     let (key, value) = kv.unwrap();
    //     // //     println!("key: {:?}, value: {:?}", key, value);
    //     // // }
    //     // 
    //     // // Sync the journal to disk to make sure data is definitely durable
    //     // // When the keyspace is dropped, it will try to persist with `PersistMode::SyncAll` as well
    //     // keyspace.persist(PersistMode::SyncAll).unwrap();
    // }
}
