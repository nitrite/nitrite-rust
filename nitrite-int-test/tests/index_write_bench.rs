//! Throughput benchmark for the per-insert index-write path on a production-shaped collection
//! (unique `id` + low-cardinality non-unique `account_id` / `folder_id`), mirroring the Inbux
//! initial-sync worst case. Ignored by default; run with:
//!
//! ```sh
//! cargo test -p nitrite_int_test --test index_write_bench --release -- --ignored --nocapture
//! ```

use std::time::Instant;

use nitrite::collection::Document;
use nitrite::doc;
use nitrite::filter::field;
use nitrite::index::{non_unique_index, unique_index};
use nitrite_int_test::test_util::{cleanup, create_test_context, run_test};

fn sync_account(count: usize) -> nitrite::errors::NitriteResult<()> {
    run_test(
        create_test_context,
        move |ctx| {
            let coll = ctx.db().collection("messages")?;
            // Inbux Message schema: unique business id, non-unique account/folder scoping.
            coll.create_index(vec!["id"], &unique_index())?;
            coll.create_index(vec!["account_id"], &non_unique_index())?;
            coll.create_index(vec!["folder_id"], &non_unique_index())?;

            let start = Instant::now();
            // Insert one at a time so each write exercises the incremental index update path —
            // exactly where the old array layout was O(n²) for the low-cardinality fields.
            for i in 0..count {
                let d: Document = doc! {
                    "id": (format!("msg-{i}")),
                    "account_id": "acc-1",
                    "folder_id": (if i % 4 == 0 { "inbox" } else { "archive" }),
                    "received_at": (1_700_000_000_000i64 + i as i64),
                    "subject": (format!("Message subject number {i}")),
                };
                coll.insert(d)?;
            }
            let elapsed = start.elapsed();
            let per_s = count as f64 / elapsed.as_secs_f64();
            println!(
                "[index-write-bench] {count:>6} msgs in {elapsed:?}  =>  {per_s:>8.0} msg/s"
            );

            // Sanity: the indexes return correct counts.
            assert_eq!(coll.find(field("account_id").eq("acc-1"))?.count(), count);
            assert_eq!(
                coll.find(field("folder_id").eq("inbox"))?.count(),
                count.div_ceil(4)
            );
            Ok(())
        },
        cleanup,
    );
    Ok(())
}

#[test]
#[ignore = "benchmark; run explicitly in --release"]
fn bench_index_write_throughput() {
    for count in [2_000usize, 10_000, 50_000] {
        sync_account(count).unwrap();
    }
}
