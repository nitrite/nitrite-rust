// Paging a collection, page by page, against the same rows read in one pass.
//
// `skip` is served at the source where nothing between the source and the page drops or
// reorders rows: the offset is reached by walking keys, without fetching the documents it
// passes over. That is a different code path from `next`, so what has to hold is that it
// lands on exactly the same row - an off-by-one in the skip is a page that silently starts
// one row late, which no other test would notice. The shapes that must *decline* the
// push-down (a scanned filter, a blocking sort, an OR plan) are here for the same reason:
// getting those wrong returns the wrong rows rather than merely slowly.
use nitrite::collection::{order_by, FindOptions, NitriteCollection};
use nitrite::common::SortOrder;
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::{all, field, or};
use nitrite::index::non_unique_index;
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

const ROWS: i64 = 500;
const PAGE: u64 = 25;

fn seed(collection: &NitriteCollection) -> NitriteResult<()> {
    for i in 0..ROWS {
        collection.insert(doc! {"index": i, "group": (i % 5), "name": (format!("row {i}"))})?;
    }
    Ok(())
}

fn indexes_of(collection: &NitriteCollection, options: &FindOptions) -> NitriteResult<Vec<i64>> {
    let mut out = Vec::new();
    for doc in collection.find_with_options(all(), options)? {
        out.push(*doc?.get("index")?.as_i64().expect("index must be an i64"));
    }
    Ok(out)
}

#[test]
fn every_page_is_the_slice_of_the_full_scan_it_claims_to_be() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("paged")?;
            seed(&collection)?;

            let whole = indexes_of(&collection, &FindOptions::default())?;
            assert_eq!(whole.len(), ROWS as usize);

            let mut offset = 0u64;
            while offset < ROWS as u64 {
                let options = FindOptions::default().skip(offset).limit(PAGE);
                let page = indexes_of(&collection, &options)?;
                let start = offset as usize;
                let end = std::cmp::min(start + PAGE as usize, ROWS as usize);
                assert_eq!(page, whole[start..end], "page at offset {offset}");
                offset += PAGE;
            }
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn a_page_past_the_end_is_empty() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("paged")?;
            seed(&collection)?;

            for offset in [ROWS as u64, ROWS as u64 + 1, ROWS as u64 * 3] {
                let options = FindOptions::default().skip(offset).limit(PAGE);
                assert_eq!(
                    collection.find_with_options(all(), &options)?.count(),
                    0,
                    "a page starting at {offset} must be empty, not wrap to the start"
                );
            }
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn the_edges_of_the_skip() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("paged")?;
            seed(&collection)?;

            let whole = indexes_of(&collection, &FindOptions::default())?;
            for offset in [0u64, 1, 2, ROWS as u64 - 2, ROWS as u64 - 1] {
                let options = FindOptions::default().skip(offset).limit(1);
                let page = indexes_of(&collection, &options)?;
                assert_eq!(page, vec![whole[offset as usize]], "skip {offset} limit 1");
            }
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn an_empty_collection_pages_to_nothing() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("empty")?;
            let options = FindOptions::default().skip(0).limit(PAGE);
            assert_eq!(collection.find_with_options(all(), &options)?.count(), 0);
            let options = FindOptions::default().skip(10).limit(PAGE);
            assert_eq!(collection.find_with_options(all(), &options)?.count(), 0);
            Ok(())
        },
        cleanup,
    )
}

#[test]
fn an_indexed_query_pages_the_same_way() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("paged")?;
            seed(&collection)?;
            collection.create_index(vec!["index"], &non_unique_index())?;

            let filter = field("index").gte(100i64);
            let mut whole = Vec::new();
            for doc in collection.find(filter.clone())? {
                whole.push(*doc?.get("index")?.as_i64().unwrap());
            }
            assert_eq!(whole.len(), (ROWS - 100) as usize);

            let mut paged = Vec::new();
            let mut offset = 0u64;
            while offset < whole.len() as u64 {
                let options = FindOptions::default().skip(offset).limit(PAGE);
                for doc in collection.find_with_options(filter.clone(), &options)? {
                    paged.push(*doc?.get("index")?.as_i64().unwrap());
                }
                offset += PAGE;
            }
            assert_eq!(paged, whole);
            Ok(())
        },
        cleanup,
    )
}

/// The shapes the source-level skip must decline: the offset would land on a different row
/// than the page wants, so these have to keep paying for the pipeline skip.
#[test]
fn a_scanned_filter_a_sort_and_an_or_plan_still_page_correctly() {
    run_test(
        create_test_context,
        |ctx| -> NitriteResult<()> {
            let collection = ctx.db().collection("paged")?;
            seed(&collection)?;

            // a filter with no index behind it - a post-filter drops rows after the source
            assert_pages_match(&collection, field("group").eq(3i64), None, 7)?;
            // a blocking sort - it reorders everything behind it
            assert_pages_match(
                &collection,
                all(),
                Some(("index", SortOrder::Descending)),
                31,
            )?;
            // an OR plan unions its sub-plans
            assert_pages_match(
                &collection,
                or(vec![field("index").lt(50i64), field("index").gte(450i64)]),
                None,
                13,
            )?;
            Ok(())
        },
        cleanup,
    )
}

fn assert_pages_match(
    collection: &NitriteCollection,
    filter: nitrite::filter::Filter,
    sort_field: Option<(&str, SortOrder)>,
    page_size: u64,
) -> NitriteResult<()> {
    let base = match sort_field {
        Some((f, o)) => order_by(f, o),
        None => FindOptions::default(),
    };
    let mut whole = Vec::new();
    for doc in collection.find_with_options(filter.clone(), &base)? {
        whole.push(*doc?.get("index")?.as_i64().unwrap());
    }
    assert!(!whole.is_empty(), "the fixture must return rows");

    let mut paged = Vec::new();
    let mut offset = 0u64;
    while offset < whole.len() as u64 {
        let options = match sort_field {
            Some((f, o)) => order_by(f, o),
            None => FindOptions::default(),
        }
        .skip(offset)
        .limit(page_size);
        for doc in collection.find_with_options(filter.clone(), &options)? {
            paged.push(*doc?.get("index")?.as_i64().unwrap());
        }
        offset += page_size;
    }
    assert_eq!(paged, whole);
    Ok(())
}
