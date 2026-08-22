//! Regression tests for `order_by(field).limit(n)` on an indexed field.
//!
//! The blocking sort collects and fully deserializes *every* document matching the filter
//! before the first row can be returned, so a 20-row page cost the same as draining the
//! whole collection - and an index on the sort field bought nothing, because the index was
//! only ever used to filter. Sorted, limited reads now take their sort keys from the index
//! and fetch only the documents they return.
//!
//! The change must be invisible: these tests pin the result of every sorted query against
//! the same query on an unindexed collection, including the cases where the index is *not*
//! a faithful stand-in for the collection (a multi-valued field is indexed once per element,
//! a non-comparable value is not indexed at all) and the blocking sort has to run anyway.

use std::time::Instant;

use nitrite::collection::{order_by, Document, NitriteCollection};
use nitrite::common::{SortOrder, Value};
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::all;
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

/// Documents with a distinct `seq`, a `bucket` that repeats (so sorts have ties), and a
/// `name` for string/collated ordering.
fn seed(coll: &NitriteCollection, count: i64) -> NitriteResult<()> {
    let docs: Vec<_> = (0..count)
        .map(|i| {
            doc! {
                "seq": i,
                "bucket": (i % 5),
                "name": (format!("name-{:04}", count - i)),
            }
        })
        .collect();
    coll.insert_many(docs)?;
    Ok(())
}

fn seq_values(coll: &NitriteCollection, field: &str, options: &nitrite::collection::FindOptions) -> NitriteResult<Vec<Value>> {
    let mut out = Vec::new();
    for doc in coll.find_with_options(all(), options)? {
        out.push(doc?.get(field)?);
    }
    Ok(out)
}

/// Runs `options` against an indexed and an unindexed copy of the same data and asserts the
/// two agree - document for document, in order.
fn assert_index_matches_scan<F>(seed_fn: F, options: &nitrite::collection::FindOptions, read_field: &str)
where
    F: Fn(&NitriteCollection) -> NitriteResult<()>
        + Copy
        + std::panic::UnwindSafe
        + std::panic::RefUnwindSafe,
{
    run_test(
        create_test_context,
        |ctx| {
            let indexed = ctx.db().collection("indexed")?;
            indexed.create_index(vec!["seq"], &non_unique_index())?;
            indexed.create_index(vec!["bucket"], &non_unique_index())?;
            indexed.create_index(vec!["name"], &non_unique_index())?;
            seed_fn(&indexed)?;

            let scanned = ctx.db().collection("scanned")?;
            seed_fn(&scanned)?;

            assert_eq!(
                seq_values(&indexed, read_field, options)?,
                seq_values(&scanned, read_field, options)?,
                "index-ordered sort disagreed with the blocking sort"
            );
            Ok(())
        },
        cleanup,
    );
}

#[test]
fn test_sorted_page_matches_full_scan_ascending() {
    assert_index_matches_scan(
        |coll| seed(coll, 500),
        &order_by("seq", SortOrder::Ascending).limit(20),
        "seq",
    );
}

#[test]
fn test_sorted_page_matches_full_scan_descending() {
    assert_index_matches_scan(
        |coll| seed(coll, 500),
        &order_by("seq", SortOrder::Descending).limit(20),
        "seq",
    );
}

#[test]
fn test_deep_page_matches_full_scan() {
    assert_index_matches_scan(
        |coll| seed(coll, 500),
        &order_by("seq", SortOrder::Descending).skip(400).limit(20),
        "seq",
    );
}

#[test]
fn test_ties_break_the_same_way() {
    // 100 documents share each `bucket` value; the order within a tie group must not change.
    for order in [SortOrder::Ascending, SortOrder::Descending] {
        assert_index_matches_scan(
            |coll| seed(coll, 500),
            &order_by("bucket", order).limit(50),
            "seq",
        );
    }
}

#[test]
fn test_string_sort_matches_full_scan() {
    assert_index_matches_scan(
        |coll| seed(coll, 200),
        &order_by("name", SortOrder::Ascending).limit(20),
        "name",
    );
}

#[test]
fn test_missing_sort_field_sorts_first() {
    // A document with no `seq` is indexed under null, and null sorts before everything.
    assert_index_matches_scan(
        |coll| {
            seed(coll, 100)?;
            coll.insert_many(vec![
                doc! { "bucket": 0, "name": "no-seq-a" },
                doc! { "bucket": 1, "name": "no-seq-b" },
            ])?;
            Ok(())
        },
        &order_by("seq", SortOrder::Ascending).limit(10),
        "name",
    );
}

#[test]
fn test_multi_valued_field_falls_back_to_blocking_sort() {
    // An array value is indexed once per element, so the index holds more entries than the
    // collection holds documents. Taking the order from it would return a document twice.
    assert_index_matches_scan(
        |coll| {
            seed(coll, 50)?;
            coll.insert(doc! {
                "seq": (Value::Array(vec![Value::from(3i64), Value::from(9i64)])),
                "bucket": 2,
                "name": "multi",
            })?;
            Ok(())
        },
        &order_by("seq", SortOrder::Ascending).limit(20),
        "name",
    );
}

#[test]
fn test_non_comparable_field_falls_back_to_blocking_sort() {
    // A sub-document is not comparable, so it is left out of the index entirely. Ordering
    // from the index would silently drop the row.
    assert_index_matches_scan(
        |coll| {
            seed(coll, 50)?;
            coll.insert(doc! {
                "seq": (Value::Document(doc! { "nested": 1 })),
                "bucket": 2,
                "name": "nested",
            })?;
            Ok(())
        },
        &order_by("seq", SortOrder::Ascending).limit(20),
        "name",
    );
}

#[test]
fn test_paging_covers_the_collection_exactly_once() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("paged")?;
            coll.create_index(vec!["bucket"], &non_unique_index())?;
            seed(&coll, 200)?;

            let mut paged = Vec::new();
            for page in 0..10 {
                let options = order_by("bucket", SortOrder::Descending)
                    .skip(page * 20)
                    .limit(20);
                for document in coll.find_with_options(all(), &options)? {
                    paged.push(document?.get("seq")?);
                }
            }

            let all_at_once = seq_values(
                &coll,
                "seq",
                &order_by("bucket", SortOrder::Descending).limit(200),
            )?;

            assert_eq!(paged.len(), 200, "paging lost or duplicated rows");
            assert_eq!(paged, all_at_once, "paged order differs from a single read");
            Ok(())
        },
        cleanup,
    );
}

/// The same query, the same row count, the same index - only the size of the documents
/// differs. A sorted page that decodes every row pays for the payload of every row, so the
/// fat collection costs many times the lean one. A sorted page that decodes only the row it
/// returns costs about the same either way.
///
/// Both halves walk an index of identical size and shape, so that cost cancels; what does not
/// cancel is the payload of the rows each one decodes. The page is deliberately one row rather
/// than twenty: returning a fat document legitimately costs more than returning a lean one, and
/// that difference is the floor of this ratio, so the fewer rows the page returns the more of
/// the ratio is the `COST_ROWS` rows it should never have touched.
///
/// Deliberately not "sorted page vs. full drain", whose halves share no work at all. Both
/// halves here are measured back to back, under whatever load the machine is under.
#[test]
fn test_sorted_page_cost_does_not_follow_document_size() {
    run_test(
        create_test_context,
        |ctx| {
            let lean = sorted_page_cost(&ctx, "lean", 0)?;
            let fat = sorted_page_cost(&ctx, "fat", 150)?;

            assert!(
                fat < lean * 3.0,
                "a sorted page over fat documents took {fat:.4}s against {lean:.4}s over lean \
                 ones, same row count - it is still decoding rows it discards"
            );
            Ok(())
        },
        cleanup,
    );
}

const COST_ROWS: i64 = 1_000;

fn sorted_page_cost(
    ctx: &nitrite_int_test::test_util::TestContext,
    name: &str,
    payload_size: i64,
) -> NitriteResult<f64> {
    let coll = ctx.db().collection(name)?;
    coll.create_index(vec!["seq"], &non_unique_index())?;

    let docs: Vec<Document> = (0..COST_ROWS)
        .map(|i| {
            let mut d = doc! { "seq": i };
            if payload_size > 0 {
                let payload: Vec<Value> = (0..payload_size)
                    .map(|w| {
                        Value::Document(doc! {
                            "text": (format!("word{w}")),
                            "start": (w * 300),
                        })
                    })
                    .collect();
                d.put("payload", Value::Array(payload)).unwrap();
            }
            d
        })
        .collect();
    coll.insert_many(docs)?;

    let options = order_by("seq", SortOrder::Descending).limit(1);
    let mut run = || -> NitriteResult<()> {
        for document in coll.find_with_options(all(), &options)? {
            document?;
        }
        Ok(())
    };

    run()?; // warm
    let start = Instant::now();
    for _ in 0..3 {
        run()?;
    }
    Ok(start.elapsed().as_secs_f64() / 3.0)
}

#[test]
fn test_unlimited_sort_is_unchanged() {
    // With no limit every document is fetched anyway, so the index path is not worth taking;
    // the result must still be the plain sorted collection.
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("unlimited")?;
            coll.create_index(vec!["seq"], &non_unique_index())?;
            seed(&coll, 100)?;

            let sorted: Vec<Document> = coll
                .find_with_options(all(), &order_by("seq", SortOrder::Descending))?
                .collect::<NitriteResult<Vec<_>>>()?;

            assert_eq!(sorted.len(), 100);
            for (i, document) in sorted.iter().enumerate() {
                assert_eq!(document.get("seq")?, Value::from(99i64 - i as i64));
            }
            Ok(())
        },
        cleanup,
    );
}
