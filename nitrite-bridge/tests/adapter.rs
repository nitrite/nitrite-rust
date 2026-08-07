//! The Nitrite adapter's own acceptance criteria — the ones the conformance
//! suite deliberately skips, because they are an *adapter's* obligation rather
//! than a bridge's.
//!
//! Two of them are named by `PLAN.md` §6 M4 in as many words: keyed
//! repositories are browsable, and the filter DSL round-trips every v1 operator
//! this implementation actually supports, with unsupported operators reported in
//! `capabilities` rather than failing at query time.
//!
//! Everything runs over **both stores**, which is how "browses correctly on
//! every engine" is held rather than asserted.

#![cfg(feature = "bridge")]

mod fixture;

use dbinspect_bridge::{BridgeAdapter, PageRequest};
use nitrite_bridge::filter_dsl::{NITRITE_FILTER_OPS, NITRITE_REGEX_OP};
use nitrite_bridge::NitriteAdapter;
use serde_json::{json, Map, Value};

const STORES: [&str; 2] = ["memory", "fjall"];

fn adapter_over(store: &str) -> (NitriteAdapter, fixture::Fixture) {
    let fixture = fixture::open(store).expect("could not open the fixture");
    let adapter = NitriteAdapter::new(fixture.db.clone(), "nitrite-main", "app data")
        .with_repositories(fixture.repositories.clone());
    (adapter, fixture)
}

fn params(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        other => panic!("not an object: {other}"),
    }
}

fn page(adapter: &NitriteAdapter, request: Value) -> dbinspect_bridge::QueryPage {
    adapter
        .query_page(&PageRequest::from_params(&params(request)).expect("invalid request"))
        .expect("the adapter refused a page it should have served")
}

// ------------------------------------------------------------------- browsing

#[test]
fn every_store_kind_is_listed_on_every_engine() {
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let listed = adapter.list_stores().expect("could not list stores");

        let named = |name: &str| {
            listed
                .iter()
                .find(|info| info.name == name)
                .unwrap_or_else(|| panic!("{store}: {name} was not listed"))
        };

        assert_eq!(named("users").kind, "collection", "{store}");
        assert_eq!(named("users").approx_count, Some(250), "{store}");
        assert_eq!(named("order-details").kind, "collection", "{store}");
        assert_eq!(named("empty_collection").approx_count, Some(0), "{store}");

        // A repository is a store of its own kind, and its name is the entity's.
        assert_eq!(named("Order").kind, "repository", "{store}");
        assert_eq!(named("Order").approx_count, Some(30), "{store}");
        assert!(named("Order").key.is_none(), "{store}");
    }
}

#[test]
fn a_keyed_repository_is_browsable_and_reports_its_key() {
    // `PLAN.md` §6 M4 names this one directly. Keyed repositories are
    // first-class only in `nitrite-rust`; the protocol models them as a `key`
    // attribute so the difference stays inside the adapter, and the name stays
    // the one addressable identity `store` carries.
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let listed = adapter.list_stores().unwrap();

        let keyed = listed
            .iter()
            .find(|info| info.key.is_some())
            .unwrap_or_else(|| panic!("{store}: no keyed repository was listed"));
        assert_eq!(keyed.key.as_deref(), Some("archive"), "{store}");
        assert_eq!(keyed.name, "Order+archive", "{store}");
        assert_eq!(keyed.kind, "repository", "{store}");
        assert_eq!(keyed.approx_count, Some(4), "{store}");

        // Browsable, not merely listed.
        let rows = page(&adapter, json!({"store": keyed.name, "pageSize": 10}));
        assert_eq!(rows.rows.len(), 4, "{store}");
        assert_eq!(rows.rows[0]["label"], json!("archived"), "{store}");

        let schema = adapter.get_schema(&keyed.name).expect("no schema");
        assert!(schema.inferred, "{store}");
        assert!(
            schema.columns.iter().any(|column| column.name == "qty"),
            "{store}"
        );
    }
}

#[test]
fn a_store_the_adapter_never_reported_is_refused_rather_than_created() {
    // Load-bearing for a reason particular to Nitrite: `Nitrite::collection`
    // *creates* a collection that does not exist, so an unchecked name would let
    // a paired client litter the developer's database.
    for store in STORES {
        let (adapter, fixture) = adapter_over(store);
        let before = fixture.db.list_collection_names().unwrap().len();

        for name in ["__no_store__", "../../../etc/passwd", "Order+nosuchkey"] {
            assert!(
                adapter.get_schema(name).is_err(),
                "{store}: {name} resolved"
            );
        }

        assert_eq!(
            fixture.db.list_collection_names().unwrap().len(),
            before,
            "{store}: a refused name still created a collection"
        );
    }
}

#[test]
fn a_sampled_schema_says_it_is_a_sample_and_marks_a_field_only_some_rows_carry() {
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let schema = adapter.get_schema("users").unwrap();

        assert!(schema.inferred, "{store}");
        assert_eq!(schema.sampled_docs, Some(50), "{store}");

        let column = |name: &str| {
            schema
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap_or_else(|| panic!("{store}: no {name} column"))
        };
        assert_eq!(column("_id").column_type, "id", "{store}");
        assert!(column("_id").pk, "{store}");
        assert!(!column("_id").nullable, "{store}");
        assert_eq!(column("name").column_type, "text", "{store}");
        assert_eq!(column("score").column_type, "real", "{store}");
        // Half the fixture's rows have no `age` at all.
        assert!(column("age").nullable, "{store}");
    }
}

#[test]
fn a_page_carries_encoded_values_a_total_and_whether_there_are_more() {
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let first = page(
            &adapter,
            json!({"store": "users", "page": 0, "pageSize": 10}),
        );

        assert_eq!(first.rows.len(), 10, "{store}");
        assert_eq!(first.total, Some(250), "{store}");
        assert!(first.has_more, "{store}");

        // A page past the end is empty rather than an error: the client pages
        // until it runs out.
        let beyond = page(
            &adapter,
            json!({"store": "users", "page": 1000000, "pageSize": 10}),
        );
        assert!(beyond.rows.is_empty(), "{store}");
        assert!(!beyond.has_more, "{store}");
    }
}

#[test]
fn a_blob_and_a_nested_document_survive_the_trip() {
    use base64::Engine as _;

    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let sorted = page(
            &adapter,
            json!({"store": "users", "pageSize": 1, "sortBy": "id"}),
        );
        let row = &sorted.rows[0];

        // The core truncates at 64 KB and keeps the real length beside it.
        assert_eq!(row["avatar"]["len"], json!(100 * 1024), "{store}");
        assert_eq!(row["avatar"]["truncated"], json!(true), "{store}");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(row["avatar"]["__blob"].as_str().unwrap())
            .unwrap();
        assert_eq!(decoded.len(), 64 * 1024, "{store}");

        // A nested document is unwrapped rather than rendered as a debug string.
        assert_eq!(row["address"]["city"], json!("Kolkata"), "{store}");
    }
}

#[test]
fn sorting_is_offered_only_for_a_column_the_adapter_reported() {
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);

        let ascending = page(
            &adapter,
            json!({"store": "users", "pageSize": 3, "sortBy": "id"}),
        );
        let ids: Vec<_> = ascending.rows.iter().map(|row| row["id"].clone()).collect();
        assert_eq!(ids, vec![json!(0), json!(1), json!(2)], "{store}");

        let descending = page(
            &adapter,
            json!({"store": "users", "pageSize": 3, "sortBy": "id", "desc": true}),
        );
        let ids: Vec<_> = descending
            .rows
            .iter()
            .map(|row| row["id"].clone())
            .collect();
        assert_eq!(ids, vec![json!(249), json!(248), json!(247)], "{store}");

        // Nitrite will happily sort by a field no document has — every value is
        // null and the order is arbitrary. Showing rows in an order the client
        // did not ask for is the same failure as showing rows it filtered out.
        let refused = adapter.query_page(
            &PageRequest::from_params(&params(
                json!({"store": "users", "sortBy": "__no_column__"}),
            ))
            .unwrap(),
        );
        assert!(refused.is_err(), "{store}");
    }
}

// --------------------------------------------------------------------- filter

#[test]
fn capabilities_advertise_exactly_what_the_filter_dsl_implements() {
    // §1: `filterOps` is authoritative, and the client greys out what is
    // missing. Criterion 10: every other gate stays shut.
    let (adapter, _fixture) = adapter_over("memory");
    let capabilities = adapter.capabilities();

    assert_eq!(capabilities.filter_ops, NITRITE_FILTER_OPS.to_vec());
    assert!(!capabilities
        .filter_ops
        .contains(&NITRITE_REGEX_OP.to_string()));
    assert!(!capabilities.edit);
    assert!(!capabilities.sql);
    assert!(!capabilities.snapshot);
    assert!(capabilities.watch);

    let allowed = NitriteAdapter::new(_fixture.db.clone(), "n", "n").allow_regex(true);
    assert!(allowed
        .capabilities()
        .filter_ops
        .contains(&NITRITE_REGEX_OP.to_string()));
}

#[test]
fn every_advertised_operator_round_trips_against_a_real_store() {
    // The other half of M4's filter criterion: not that the DSL parses, but that
    // what it parses to actually selects the rows the operator names.
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let matching = |filter: Value| {
            page(
                &adapter,
                json!({"store": "users", "pageSize": 200, "filter": filter}),
            )
            .total
            .unwrap()
        };

        assert_eq!(
            matching(json!({"field": "id", "op": "eq", "value": 7})),
            1,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "ne", "value": 7})),
            249,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "gt", "value": 244})),
            5,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "gte", "value": 245})),
            5,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "lt", "value": 5})),
            5,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "lte", "value": 4})),
            5,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "in", "value": [1, 2, 3]})),
            3,
            "{store}"
        );
        assert_eq!(
            matching(json!({"field": "id", "op": "notIn", "value": [1, 2, 3]})),
            247,
            "{store}"
        );

        // and / or / not, which is what a client's console actually sends.
        assert_eq!(
            matching(json!({"and": [
                {"field": "id", "op": "gte", "value": 10},
                {"field": "id", "op": "lt", "value": 20},
            ]})),
            10,
            "{store}"
        );
        assert_eq!(
            matching(json!({"or": [
                {"field": "id", "op": "eq", "value": 1},
                {"field": "id", "op": "eq", "value": 2},
            ]})),
            2,
            "{store}"
        );
        assert_eq!(
            matching(json!({"not": {"field": "id", "op": "gte", "value": 10}})),
            10,
            "{store}"
        );
    }
}

#[test]
fn an_operator_this_adapter_does_not_advertise_is_refused_rather_than_approximated() {
    // Reported in `capabilities` rather than failing at query time is the
    // criterion; this is the other side of it — an operator that got past the
    // client anyway is a `badRequest`, never an unfiltered page.
    let (adapter, _fixture) = adapter_over("memory");
    for op in ["exists", "between", "elemMatch", "regex"] {
        assert!(!adapter.capabilities().filter_ops.contains(&op.to_string()));
        let refused = adapter.query_page(
            &PageRequest::from_params(&params(json!({
                "store": "users",
                "filter": {"field": "name", "op": op, "value": "a"},
            })))
            .unwrap(),
        );
        assert!(refused.is_err(), "{op} was not refused");
        let count = page(&adapter, json!({"store": "users", "pageSize": 1}))
            .total
            .unwrap();
        assert_eq!(count, 250, "the store was disturbed by a refused filter");
    }
}

#[test]
fn text_round_trips_on_an_indexed_field_and_is_refused_legibly_on_an_unindexed_one() {
    // The JVM adapter found this first: Nitrite refuses a text filter it cannot
    // serve from an index, and `filterOps` is a flat operator list rather than a
    // per-column matrix — so the operator is advertised and the *column* is what
    // decides. Both halves matter: it has to work where it can, and the refusal
    // where it cannot has to be something a client can render.
    for store in STORES {
        let (adapter, _fixture) = adapter_over(store);
        let text_on = |field: &str| {
            adapter.query_page(
                &PageRequest::from_params(&params(json!({
                    "store": "users",
                    "pageSize": 200,
                    "filter": {"field": field, "op": "text", "value": "user"},
                })))
                .unwrap(),
            )
        };

        // `name` carries a full-text index in the fixture, and every row's name
        // contains the word.
        let matched = text_on("name")
            .unwrap_or_else(|failure| panic!("{store}: an indexed text filter failed: {failure}"));
        assert_eq!(matched.total, Some(250), "{store}");

        // `score` does not, and the engine refuses. Measured, not assumed: it
        // comes back as `adapter`, which a client renders as "the database said
        // no" rather than as a bug in the bridge.
        let refused = text_on("score").expect_err("an unindexed text filter must be refused");
        assert_ne!(
            refused.kind,
            dbinspect_bridge::BridgeErrorKind::Internal,
            "{store}: a condition the developer can fix must not be an internal error"
        );
    }
}

// ---------------------------------------------------------------------- watch

#[test]
fn a_write_from_anywhere_in_the_process_reaches_a_watcher() {
    use std::sync::mpsc;

    for store in STORES {
        let (adapter, fixture) = adapter_over(store);
        let (tx, rx) = mpsc::channel();

        let unsubscribe = adapter
            .watch(
                "users",
                Box::new(move |event| {
                    let _ = tx.send(event.to_string());
                }),
            )
            .expect("could not watch");

        // Not through the bridge: `watchScope` is `engine`, so a write made
        // anywhere in this process must be seen.
        fixture
            .db
            .collection("users")
            .unwrap()
            .insert(nitrite::doc! {"id": 9999})
            .unwrap();

        let event = rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap_or_else(|_| panic!("{store}: no change reached the watcher"));
        // Nitrite's five event names are the protocol's five; nothing is mapped.
        assert_eq!(event, "insert", "{store}");

        unsubscribe();
        fixture
            .db
            .collection("users")
            .unwrap()
            .insert(nitrite::doc! {"id": 10000})
            .unwrap();
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(500))
                .is_err(),
            "{store}: a listener was left behind in the host application"
        );
    }
}

#[test]
fn watching_a_store_the_adapter_never_reported_is_refused() {
    let (adapter, _fixture) = adapter_over("memory");
    assert!(adapter.watch("__no_store__", Box::new(|_| {})).is_err());
}
