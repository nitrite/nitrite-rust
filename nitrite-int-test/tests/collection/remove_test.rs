use nitrite::common::Value;
use nitrite::doc;
use nitrite::filter::{all, field};
use nitrite::index::unique_index;
use nitrite_int_test::test_util::{
    cleanup, create_test_context, insert_test_documents, run_test, NitriteDateTime,
};

#[test]
fn test_delete() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let write_result = coll.remove(field("last_name").ne(Value::Null), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 3);

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_with_options() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let write_result = coll.remove(field("last_name").ne(Value::Null), true)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 2);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_with_non_matching_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);

            let write_result = coll.remove(field("last_name").eq("a"), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_in_empty_collection() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 0);

            let write_result = coll.remove(field("last_name").ne(Value::Null), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_clear() {
    run_test(
        || create_test_context(),
        |ctx| {
            let dt1 = NitriteDateTime::parse_from_rfc3339("2012-07-01T02:15:22+02:00");
            let doc1 = doc!{
                first_name: "fn1",
                last_name: "ln1",
                birth_day: dt1,
                data: (vec![1u8, 2u8, 3u8]),
                arr: [1, 2, 3],
                list: (vec!["one", "two", "three"]),
                body: "a quick brown fox jump over the lazy dog",
            };

            let coll = ctx.db().collection("test")?;
            coll.create_index(vec!["first_name"], &unique_index())?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);
            assert!(coll.has_index(vec!["first_name"])?);

            let mut unique_error = false;
            if let Err(_) = coll.insert(doc1.clone()) {
                unique_error = true;
            }
            assert!(unique_error);

            coll.remove(all(), false)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 0);
            assert!(coll.has_index(vec!["first_name"])?);

            coll.insert(doc1)?;
            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 1);
            assert!(coll.has_index(vec!["first_name"])?);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_remove_all() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let write_result = coll.remove(all(), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_remove_document() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let write_result = coll.remove(field("first_name").eq("fn1"), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);
            assert_eq!(coll.size()?, 2);

            let write_result = coll.remove(field("first_name").eq("fn2"), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 1);
            assert_eq!(coll.size()?, 1);

            assert_eq!(coll.find(field("first_name").eq("fn1"))?.count(), 0);
            assert_eq!(coll.find(field("first_name").eq("fn2"))?.count(), 0);
            assert_eq!(coll.find(field("first_name").eq("fn3"))?.count(), 1);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
#[should_panic]
fn test_destroy() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            coll.dispose()?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

#[test]
fn test_delete_with_invalid_filter() {
    run_test(
        || create_test_context(),
        |ctx| {
            let coll = ctx.db().collection("test")?;
            insert_test_documents(&coll)?;

            let cursor = coll.find(all())?;
            assert_eq!(cursor.count(), 3);

            let write_result = coll.remove(field("last_name").gt(Value::Null), false)?;
            assert_eq!(write_result.affected_nitrite_ids().len(), 0);

            Ok(())
        },
        |ctx| cleanup(ctx),
    )
}

