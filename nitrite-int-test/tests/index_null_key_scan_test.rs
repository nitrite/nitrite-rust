// Range scans on an indexed field containing null values.
//
// Mirrors the Java regression for nitrite-java issue #1262: the index range
// scan must never let the null index key leak into (or terminate) a range
// result. Nulls are never lesser or greater than a search term, so lt/lte and
// gt/gte must return only the matching non-null values.
use nitrite::collection::{order_by, NitriteCollection};
use nitrite::common::{SortOrder, Value};
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::field;
use nitrite::index::{non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

fn insert_docs_with_null(collection: &NitriteCollection) -> NitriteResult<()> {
    collection.insert(doc! {"idx": 0, "value": (Value::Null)})?;
    for i in 1..=10 {
        collection.insert(doc! {"idx": i, "value": (i as f64)})?;
    }
    Ok(())
}

fn assert_range_scans(collection: &NitriteCollection) -> NitriteResult<()> {
    assert_eq!(collection.find(field("value").lt(5.0))?.count(), 4);
    assert_eq!(collection.find(field("value").lte(5.0))?.count(), 5);
    assert_eq!(collection.find(field("value").gt(5.0))?.count(), 5);
    assert_eq!(collection.find(field("value").gte(5.0))?.count(), 6);

    // descending sort on the indexed field drives the reverse index scan
    let options = order_by("value", SortOrder::Descending);
    let cursor = collection.find_with_options(field("value").lt(5.0), &options)?;
    let mut count = 0;
    for doc in cursor {
        let doc = doc?;
        assert!(
            !doc.get("value")?.is_null(),
            "null-valued document leaked into lt result"
        );
        count += 1;
    }
    assert_eq!(count, 4);

    let cursor = collection.find_with_options(field("value").gt(5.0), &options)?;
    let mut count = 0;
    for doc in cursor {
        let doc = doc?;
        assert!(
            !doc.get("value")?.is_null(),
            "null-valued document leaked into gt result"
        );
        count += 1;
    }
    assert_eq!(count, 5);
    Ok(())
}

#[test]
fn test_range_scan_with_null_values_non_unique_index() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_docs_with_null(&collection)?;
            collection.create_index(vec!["value"], &non_unique_index())?;
            assert_range_scans(&collection)
        },
        cleanup,
    )
}

#[test]
fn test_range_scan_with_null_values_unique_index() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_docs_with_null(&collection)?;
            collection.create_index(vec!["value"], &unique_index())?;
            assert_range_scans(&collection)
        },
        cleanup,
    )
}

#[test]
fn test_range_scan_with_null_values_no_index() {
    run_test(
        create_test_context,
        |ctx| {
            let collection = ctx.db().collection("test")?;
            insert_docs_with_null(&collection)?;
            // sanity: collection scan path
            assert_eq!(collection.find(field("value").lt(5.0))?.count(), 4);
            assert_eq!(collection.find(field("value").lte(5.0))?.count(), 5);
            assert_eq!(collection.find(field("value").gt(5.0))?.count(), 5);
            assert_eq!(collection.find(field("value").gte(5.0))?.count(), 6);
            Ok(())
        },
        cleanup,
    )
}
