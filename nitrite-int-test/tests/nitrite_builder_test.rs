// Based on Java NitriteBuilderTest.java
use nitrite::nitrite::Nitrite;
use nitrite::doc;
use nitrite::filter::all;

#[test]
fn test_builder_open_or_create_memory_db() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create in-memory database");

    assert!(!db.is_closed().unwrap());

    let collection = db.collection("test").unwrap();
    collection.insert(doc!{"key": "value"}).unwrap();

    let cursor = collection.find(all()).unwrap();
    assert_eq!(cursor.count(), 1);

    db.close().unwrap();
    assert!(db.is_closed().unwrap());
}

#[test]
fn test_builder_create_collection() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create database");

    let col1 = db.collection("collection1").unwrap();
    let col2 = db.collection("collection2").unwrap();

    col1.insert(doc!{"data": "col1"}).unwrap();
    col2.insert(doc!{"data": "col2"}).unwrap();

    assert_eq!(col1.find(all()).unwrap().count(), 1);
    assert_eq!(col2.find(all()).unwrap().count(), 1);

    db.close().unwrap();
}

#[test]
fn test_builder_has_collection() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create database");

    let collection = db.collection("test").unwrap();
    collection.insert(doc!{"test": "data"}).unwrap();

    assert!(db.has_collection("test").unwrap());
    assert!(!db.has_collection("non_existent").unwrap());

    db.close().unwrap();
}

#[test]
fn test_builder_list_collections() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create database");

    db.collection("col1").unwrap();
    db.collection("col2").unwrap();
    db.collection("col3").unwrap();

    let collections = db.list_collection_names().unwrap();
    assert!(collections.len() >= 3);

    db.close().unwrap();
}

#[test]
fn test_builder_commit() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create database");

    let collection = db.collection("test").unwrap();
    collection.insert(doc!{"key": "value"}).unwrap();

    // Commit should succeed
    let result = db.commit();
    assert!(result.is_ok());

    db.close().unwrap();
}

#[test]
fn test_builder_close() {
    let db = Nitrite::builder()
        .open_or_create(None, None)
        .expect("Failed to create database");

    assert!(!db.is_closed().unwrap());

    db.close().unwrap();

    assert!(db.is_closed().unwrap());
}
