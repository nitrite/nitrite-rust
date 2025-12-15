use nitrite::collection::{insert_if_absent, just_once, CollectionEventListener, UpdateOptions};
use nitrite::common::Value;
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite_int_test::test_util::{cleanup, create_test_context, insert_test_documents, run_test};

#[test]
fn test_update() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            let mut cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.size(), 1);
            for doc in cursor {
                assert_eq!(doc?.get("last_name").unwrap().as_string().unwrap(), "ln1");
            }

            let update_result = collection.update(
                field("first_name").eq("fn1"),
                &doc!{ "last_name": "new-last-name" },
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 1);

            let mut cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.size(), 1);
            for doc in cursor {
                assert_eq!(doc?.get("last_name").unwrap().as_string().unwrap(), "new-last-name");
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_upsert_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            let update = doc!{ "last_name": "ln4" };
            let result = collection.update_one(&update, true);
            assert!(result.is_ok());
            assert_eq!(collection.size()?, 4);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_upsert() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;
            assert_eq!(collection.size()?, 3);

            let update = doc!{ "last_name": "ln4" };
            let write_result = collection.update_one(&update, true)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);
            assert_eq!(collection.size()?, 4);

            let cursor = collection.find(field("last_name").eq("ln4"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_option_upsert() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            let doc1 = doc!{
                "first_name": "fn1",
                "last_name": "ln1",
                "birth_day": (Value::Null),
                "data": [1_u8, 2_u8, 3_u8],
                "list": ["one", "two", "three"],
                "body": "a quick brown fox jump over the lazy dog"
            };

            let update_result = collection.update_with_options(
                field("first_name").eq("fn1"),
                &doc1.clone(),
                &insert_if_absent(),
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 1);

            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 1);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_multiple() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            insert_test_documents(&collection)?;

            let update = doc!{ "last_name": "newLastName1" };
            let update_result = collection.update(field("first_name").ne("fn1"), &update)?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 2);

            let cursor = collection.find(field("last_name").eq("newLastName1"))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_with_options_upsert_false() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            let doc1 = doc!{
                "first_name": "fn1",
                "last_name": "ln1",
                "birth_day": (Value::Null),
                "data": [1_u8, 2_u8, 3_u8],
                "list": ["one", "two", "three"],
                "body": "a quick brown fox jump over the lazy dog"
            };

            let options = UpdateOptions::default();
            let update_result = collection.update_with_options(
                field("first_name").eq("fn1"),
                &doc1,
                &options,
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 0);

            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_multiple_with_just_once_false() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            insert_test_documents(&collection)?;

            let options = UpdateOptions::default();

            let update = doc!{ "last_name": "newLastName1" };
            let update_result = collection.update_with_options(
                field("first_name").ne("fn1"),
                &update,
                &options,
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 2);

            let cursor = collection.find(field("last_name").eq("newLastName1"))?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_multiple_with_just_once_true() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.count(), 0);

            insert_test_documents(&collection)?;

            let update = doc!{ "last_name": "newLastName1" };
            let _ = collection.update_with_options(
                field("first_name").ne("fn1"),
                &update,
                &just_once(),
            )?;

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_with_new_field() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            let mut cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.size(), 1);
            for doc in cursor {
                assert_eq!(doc?.get("last_name").unwrap().as_string().unwrap(), "ln1");
            }

            let update_result = collection.update(
                field("first_name").eq("fn1"),
                &doc!{ "new_value": "new-value-value" },
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 1);

            let mut cursor = collection.find(field("first_name").eq("fn1"))?;
            assert_eq!(cursor.size(), 1);
            for doc in cursor {
                assert_eq!(doc?.get("new_value").unwrap().as_string().unwrap(), "new-value-value");
            }

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_update_invalid_filter() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_test_documents(&collection)?;

            let cursor = collection.find(field("last_name").eq("ln1"))?;
            assert_eq!(cursor.count(), 1);

            let update_result = collection.update(
                field("some_value").eq("some_value"),
                &doc!{ "last_name": "new-last-name" },
            )?;
            assert_eq!(update_result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn update_after_attribute_removal() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_updateAfterAttributeRemoval")?;
            collection.remove(all(), false)?;

            let doc = doc!{ "id": "test-1", "group": "groupA" };
            assert_eq!(collection.insert(doc.clone())?.affected_nitrite_ids().len(), 1);

            let saved_doc1 = collection.find(all())?.first().unwrap();
            let mut cloned_doc1 = saved_doc1?.clone();
            cloned_doc1.put("group", Value::Null)?;

            assert_eq!(collection.update_one(&cloned_doc1, false)?.affected_nitrite_ids().len(), 1);

            let saved_doc2 = collection.find(all())?.first().unwrap();
            assert!(saved_doc2?.get("group").unwrap().is_null());

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn update_nested_document() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test_updateNestedDocument")?;
            collection.remove(all(), false)?;

            let doc1 = doc!{ "conversation": { "unread": { "me": 1, "other": 2 } } };
            let doc2 = doc!{ "conversation": { "unread": { "me": 10, "other": 4 } } };
            collection.insert_many(vec![doc1, doc2])?;

            let cursor = collection.find(field("conversation.unread.me").gt(5))?;
            assert_eq!(cursor.count(), 1);

            let update = doc!{ "conversation": { "unread": { "me": 0 } } };
            collection.update(all(), &update)?;

            let cursor = collection.find(field("conversation.unread.me").gt(5))?;
            assert_eq!(cursor.count(), 0);

            let cursor = collection.find(field("conversation.unread.other").lt(5))?;
            assert_eq!(cursor.count(), 2);

            let cursor = collection.find(field("conversation.unread.other").lt(5).not())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        cleanup,
    )
}

// The following tests expect errors, so use should_panic or assert error as appropriate.

#[test]
#[should_panic]
fn test_update_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let doc = doc!{ "test": "test123" };
            let _ = collection.update_one(&doc, false)?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_remove_without_id() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let doc = doc!{ "test": "test123" };
            let _ = collection.remove_one(&doc)?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_register_listener_after_drop() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.dispose()?;
            let _ = collection.subscribe(CollectionEventListener::new(|_| panic!("should not happen")))?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_register_listener_after_close() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            collection.close()?;
            let _ = collection.subscribe(CollectionEventListener::new(|_| panic!("should not happen")))?;
            Ok(())
        },
        cleanup,
    )
}

#[test]
#[should_panic]
fn test_issue151() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            let doc1 = doc!{ "id": "test-1", "fruit": "Apple" };
            let doc2 = doc!{ "id": "test-2", "fruit": "Ôrange" };
            collection.insert_many(vec![doc1, doc2])?;

            collection.create_index(vec!["fruit"], &nitrite::index::unique_index())?;

            assert_eq!(collection.find(field("fruit").eq("Apple"))?.count(), 1);

            let mut doc3 = collection.find(field("id").eq("test-2"))?.first().unwrap()?;
            doc3.put("fruit", "Apple")?;
            collection.update_one(&doc3, false)?;

            assert_eq!(collection.find(field("fruit").eq("Apple"))?.count(), 1);

            Ok(())
        },
        cleanup,
    )
}