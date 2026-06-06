//! Regression tests for the composite-key layout of non-unique indexes.
//!
//! Non-unique indexes used to store every matching `NitriteId` in a single array keyed
//! by the indexed value, which made a bulk load on a low-cardinality field O(n²) in time
//! and O(n) per-insert in serialized memory. They now use a composite `(value, id)` key
//! layout (one row per pair), so inserts are O(1) point writes. These tests assert that
//! the new layout is functionally equivalent to the old one across the full read surface
//! (equality, `in`, `not in`, range, reverse/sorted scans) and survives removals, and
//! that a low-cardinality bulk load scales roughly linearly rather than quadratically.

use std::time::Instant;

use nitrite::collection::{order_by, NitriteCollection};
use nitrite::common::SortOrder;
use nitrite::doc;
use nitrite::errors::NitriteResult;
use nitrite::filter::field;
use nitrite::filter::and;
use nitrite::index::{non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

/// Inserts `count` documents, all sharing a single `account_id` (the realistic
/// worst-case low-cardinality field) plus a distinct `seq` for range/sort checks.
fn insert_low_cardinality(coll: &NitriteCollection, count: i64) -> NitriteResult<()> {
    let docs: Vec<_> = (0..count)
        .map(|i| {
            doc! {
                "account_id": "acc-1",
                "folder_id": (if i % 2 == 0 { "inbox" } else { "archive" }),
                "seq": i,
            }
        })
        .collect();
    coll.insert_many(docs)?;
    Ok(())
}

#[test]
fn test_non_unique_low_cardinality_equality_and_membership() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("messages")?;
            coll.create_index(vec!["account_id"], &non_unique_index())?;
            coll.create_index(vec!["folder_id"], &non_unique_index())?;

            insert_low_cardinality(&coll, 1_000)?;

            // Equality on a field where every row shares one value returns them all.
            assert_eq!(coll.find(field("account_id").eq("acc-1"))?.count(), 1_000);
            assert_eq!(coll.find(field("account_id").eq("missing"))?.count(), 0);

            // A few distinct values, thousands of rows each (folder listing).
            assert_eq!(coll.find(field("folder_id").eq("inbox"))?.count(), 500);
            assert_eq!(coll.find(field("folder_id").eq("archive"))?.count(), 500);

            // in / not in parity.
            assert_eq!(
                coll.find(field("folder_id").in_array(vec!["inbox", "archive"]))?
                    .count(),
                1_000
            );
            assert_eq!(
                coll.find(field("folder_id").in_array(vec!["inbox", "spam"]))?
                    .count(),
                500
            );
            assert_eq!(
                coll.find(field("folder_id").not_in_array(vec!["inbox"]))?
                    .count(),
                500
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_non_unique_integer_range_and_sorted_scan_exact() {
    // With the order-preserving key codec, integer range and sorted scans are exact on the
    // byte-ordered backend — including values that cross byte boundaries (255/256) and
    // negatives, which the previous little-endian bincode key ordering got wrong.
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("messages")?;
            coll.create_index(vec!["seq"], &non_unique_index())?;

            // -50..450 — distinct integers spanning negatives and the 256 byte boundary.
            let docs: Vec<_> = (-50..450i64).map(|i| doc! { "seq": i }).collect();
            coll.insert_many(docs)?;

            // Bounded range, one-sided, exclusive, and a negative range.
            assert_eq!(coll.find(field("seq").between(100, 199, true, true))?.count(), 100);
            assert_eq!(coll.find(field("seq").gte(400))?.count(), 50);
            assert_eq!(coll.find(field("seq").lt(0))?.count(), 50);
            assert_eq!(coll.find(field("seq").gt(448))?.count(), 1);
            assert_eq!(coll.find(field("seq").between(-10, 9, true, true))?.count(), 20);

            // Ascending sorted scan — negatives first, strictly increasing.
            let asc: Vec<i64> = coll
                .find_with_options(field("seq").gte(-50), &order_by("seq", SortOrder::Ascending))?
                .into_iter()
                .map(|d| d.unwrap().get("seq").unwrap().as_i64().copied().unwrap())
                .collect();
            assert_eq!(asc.len(), 500);
            assert!(asc.windows(2).all(|w| w[0] < w[1]), "ascending scan not strictly sorted");
            assert_eq!(asc[0], -50);

            // Descending sorted scan — newest-first style, strictly decreasing.
            let desc: Vec<i64> = coll
                .find_with_options(field("seq").gte(-50), &order_by("seq", SortOrder::Descending))?
                .into_iter()
                .map(|d| d.unwrap().get("seq").unwrap().as_i64().copied().unwrap())
                .collect();
            assert_eq!(desc.len(), 500);
            assert!(desc.windows(2).all(|w| w[0] > w[1]), "descending scan not strictly sorted");
            assert_eq!(desc[0], 449);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_non_unique_composite_matches_unique_array_layout() {
    // The composite-key layout must be a drop-in replacement for the classic array layout
    // across the full read surface. Build the *same* data under a unique index (array
    // layout) and a non-unique index (composite layout) and assert every query returns the
    // identical result — true "parity with the array implementation" regardless of the
    // backend's key-ordering quirks.
    run_test(
        create_test_context,
        |ctx| {
            let uniq = ctx.db().collection("uniq")?;
            uniq.create_index(vec!["seq"], &unique_index())?;
            let nonu = ctx.db().collection("nonu")?;
            nonu.create_index(vec!["seq"], &non_unique_index())?;

            let docs: Vec<_> = (0..500i64).map(|i| doc! { "seq": i }).collect();
            uniq.insert_many(docs.clone())?;
            nonu.insert_many(docs)?;

            let queries = [
                field("seq").eq(42),
                field("seq").between(100, 199, true, true),
                field("seq").gte(450),
                field("seq").lt(50),
                field("seq").gt(498),
                field("seq").in_array(vec![1, 2, 300, 499]),
                field("seq").not_in_array(vec![0, 1, 2]),
            ];
            for q in queries {
                assert_eq!(
                    uniq.find(q.clone())?.count(),
                    nonu.find(q.clone())?.count(),
                    "composite layout diverged from array layout for query {}",
                    q
                );
            }

            // Reverse / sorted-scan parity: the full ordered result sequence must match.
            let collect_seq = |coll: &nitrite::collection::NitriteCollection| -> NitriteResult<Vec<i64>> {
                Ok(coll
                    .find_with_options(field("seq").gte(0), &order_by("seq", SortOrder::Descending))?
                    .into_iter()
                    .map(|d| d.unwrap().get("seq").unwrap().as_i64().copied().unwrap())
                    .collect())
            };
            assert_eq!(
                collect_seq(&uniq)?,
                collect_seq(&nonu)?,
                "composite layout diverged from array layout on a descending sorted scan"
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_non_unique_removal_keeps_index_consistent() {
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("messages")?;
            coll.create_index(vec!["account_id"], &non_unique_index())?;
            coll.create_index(vec!["folder_id"], &non_unique_index())?;

            insert_low_cardinality(&coll, 400)?;
            assert_eq!(coll.find(field("account_id").eq("acc-1"))?.count(), 400);
            assert_eq!(coll.find(field("folder_id").eq("inbox"))?.count(), 200);

            // Remove one whole folder via an (indexed) equality filter.
            let removed = coll.remove(field("folder_id").eq("inbox"), false)?;
            assert_eq!(removed.affected_nitrite_ids().len(), 200);

            // The shared account value reflects the removals with no orphaned ids, and the
            // other folder is untouched.
            assert_eq!(coll.find(field("account_id").eq("acc-1"))?.count(), 200);
            assert_eq!(coll.find(field("folder_id").eq("inbox"))?.count(), 0);
            assert_eq!(coll.find(field("folder_id").eq("archive"))?.count(), 200);

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_non_unique_low_cardinality_bulk_load_is_subquadratic() {
    // The old array layout made each insert O(k) where k grows to n for a low-cardinality
    // field, so total time grows ~quadratically. With the composite-key layout each insert
    // is O(1), so 4x the rows should take far less than the ~16x a quadratic load would.
    // We use a generous 6x ceiling to stay robust against CI jitter while still catching a
    // regression back to O(n²).
    run_test(
        create_test_context,
        |ctx| {
            let small = 1_000i64;
            let large = 4_000i64;

            let coll_small = ctx.db().collection("scale_small")?;
            coll_small.create_index(vec!["account_id"], &non_unique_index())?;
            let t0 = Instant::now();
            insert_low_cardinality(&coll_small, small)?;
            let small_elapsed = t0.elapsed();

            let coll_large = ctx.db().collection("scale_large")?;
            coll_large.create_index(vec!["account_id"], &non_unique_index())?;
            let t1 = Instant::now();
            insert_low_cardinality(&coll_large, large)?;
            let large_elapsed = t1.elapsed();

            assert_eq!(coll_large.find(field("account_id").eq("acc-1"))?.count() as i64, large);

            // Guard against division by zero on very fast machines.
            let small_ms = small_elapsed.as_micros().max(1) as f64;
            let large_ms = large_elapsed.as_micros() as f64;
            let ratio = large_ms / small_ms;
            assert!(
                ratio < 6.0,
                "non-unique bulk load scaled super-linearly: 4x rows took {:.1}x time \
                 (small={:?}, large={:?}) — possible O(n^2) regression",
                ratio,
                small_elapsed,
                large_elapsed
            );

            Ok(())
        },
        cleanup,
    )
}

#[test]
fn test_compound_index_low_cardinality_bulk_load_and_queries() {
    // A compound index whose *first* field is low-cardinality used to be O(n²) to build (each
    // insert rewrote the first field's whole nested sub-map). With the flat composite-key
    // layout each insert is O(1). Verify both the scaling and full equality/prefix correctness.
    run_test(
        create_test_context,
        |ctx| {
            let coll = ctx.db().collection("messages")?;
            // (account_id, folder_id) — account_id is low cardinality (one value).
            coll.create_index(vec!["account_id", "folder_id"], &non_unique_index())?;

            let n = 3_000i64;
            let small = 750i64;

            let coll_small = ctx.db().collection("compound_small")?;
            coll_small.create_index(vec!["account_id", "folder_id"], &non_unique_index())?;
            let t0 = Instant::now();
            insert_low_cardinality(&coll_small, small)?;
            let small_elapsed = t0.elapsed();

            let t1 = Instant::now();
            insert_low_cardinality(&coll, n)?;
            let large_elapsed = t1.elapsed();

            // 4x rows should be far below the ~16x a quadratic build would cost.
            let small_us = small_elapsed.as_micros().max(1) as f64;
            let ratio = large_elapsed.as_micros() as f64 / small_us;
            assert!(
                ratio < 6.0,
                "compound bulk load scaled super-linearly: 4x rows took {:.1}x time \
                 (small={:?}, large={:?}) — possible O(n^2) regression",
                ratio,
                small_elapsed,
                large_elapsed
            );

            // Prefix query on just the first field returns the whole account.
            assert_eq!(coll.find(field("account_id").eq("acc-1"))?.count() as i64, n);
            // Full compound equality narrows to one folder (half the rows).
            assert_eq!(
                coll.find(and(vec![
                    field("account_id").eq("acc-1"),
                    field("folder_id").eq("inbox"),
                ]))?
                .count() as i64,
                n / 2
            );
            assert_eq!(
                coll.find(and(vec![
                    field("account_id").eq("acc-1"),
                    field("folder_id").eq("archive"),
                ]))?
                .count() as i64,
                n / 2
            );
            // Non-existent combination.
            assert_eq!(
                coll.find(and(vec![
                    field("account_id").eq("acc-2"),
                    field("folder_id").eq("inbox"),
                ]))?
                .count(),
                0
            );

            Ok(())
        },
        cleanup,
    )
}
