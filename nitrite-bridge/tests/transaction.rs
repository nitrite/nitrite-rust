//! `docs/PROTOCOL.md` §3.1 against a real Nitrite database, on both engines.
//!
//! Nitrite's transaction lives above the storage engine, so what is proved here
//! — a rollback really takes the documents back, and a read inside the
//! transaction sees what is staged — has to hold on Fjall exactly as it does in
//! memory. Running the whole file over both is how that is held rather than
//! asserted.

#![cfg(feature = "bridge")]

mod fixture;

use dbinspect_bridge::{BridgeAdapter, BridgeErrorKind, PageRequest, WriteOp, WriteRequest};
use nitrite_bridge::NitriteAdapter;
use serde_json::{json, Map, Value};

const STORES: [&str; 2] = ["memory", "fjall"];

fn writable_over(store: &str) -> (NitriteAdapter, fixture::Fixture) {
    let fixture = fixture::open(store).expect("could not open the fixture");
    let adapter = NitriteAdapter::new(fixture.db.clone(), "nitrite-main", "app data")
        .with_repositories(fixture.repositories.clone())
        .allow_write(true);
    (adapter, fixture)
}

fn params(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(object) => object,
        other => panic!("not an object: {other}"),
    }
}

fn total(adapter: &dyn BridgeAdapter, store: &str) -> u64 {
    adapter
        .query_page(&PageRequest::from_params(&params(json!({"store": store}))).expect("request"))
        .expect("page")
        .total
        .expect("a total")
}

fn insert(adapter: &dyn BridgeAdapter, store: &str, name: &str) {
    let request = WriteRequest::from_params(
        &params(json!({"store": store, "values": {"name": name}})),
        &adapter.capabilities(),
        WriteOp::Insert,
    )
    .expect("a valid insert");
    adapter.write(&request).expect("the insert was refused");
}

/// The `_id` of the first document in `store`, which is what a write addresses.
fn first_id(adapter: &dyn BridgeAdapter, store: &str) -> Value {
    let page = adapter
        .query_page(&PageRequest::from_params(&params(json!({"store": store}))).expect("request"))
        .expect("page");
    page.rows
        .first()
        .expect("the fixture has rows")
        .get("_id")
        .expect("every document carries _id")
        .clone()
}

// ---- capability ---------------------------------------------------------

#[test]
fn a_writable_adapter_reports_transactions_on_every_engine() {
    for store in STORES {
        let (adapter, _fixture) = writable_over(store);
        assert!(
            adapter.capabilities().transactions,
            "{store} did not report transactions"
        );
    }
}

#[test]
fn a_read_only_adapter_does_not() {
    let fixture = fixture::open("memory").expect("fixture");
    // `allow_write` is the permission; `transactions` reports what the engine
    // can undo. Without the first there is nothing to undo.
    let adapter = NitriteAdapter::new(fixture.db.clone(), "ro", "app data");
    assert!(!adapter.capabilities().transactions);
}

#[test]
fn the_transactional_twin_does_not_offer_to_nest_another() {
    let (adapter, _fixture) = writable_over("memory");
    let transaction = adapter.begin_transaction().expect("begin");
    let scoped = transaction.adapter();
    assert!(!scoped.capabilities().transactions);
    // Everything else carried over: a gate that changed inside a transaction
    // would be a second, invisible permission model.
    assert!(scoped.capabilities().edit);
    assert!(scoped.capabilities().watch);
    assert_eq!(
        scoped.capabilities().filter_ops,
        adapter.capabilities().filter_ops
    );
    transaction.rollback().expect("rollback");
}

// ---- rollback and commit ------------------------------------------------

#[test]
fn a_rollback_takes_the_documents_back() {
    for store in STORES {
        let (adapter, _fixture) = writable_over(store);
        let before = total(&adapter, "users");

        let transaction = adapter.begin_transaction().expect("begin");
        insert(transaction.adapter().as_ref(), "users", "ada");
        insert(transaction.adapter().as_ref(), "users", "grace");
        transaction.rollback().expect("rollback");

        assert_eq!(total(&adapter, "users"), before, "{store} kept the rows");
    }
}

#[test]
fn a_commit_keeps_them() {
    for store in STORES {
        let (adapter, _fixture) = writable_over(store);
        let before = total(&adapter, "users");

        let transaction = adapter.begin_transaction().expect("begin");
        insert(transaction.adapter().as_ref(), "users", "ada");
        transaction.commit().expect("commit");

        assert_eq!(total(&adapter, "users"), before + 1, "{store} lost the row");
    }
}

#[test]
fn a_rolled_back_delete_brings_the_document_back() {
    for store in STORES {
        let (adapter, _fixture) = writable_over(store);
        let before = total(&adapter, "users");
        let id = first_id(&adapter, "users");

        let transaction = adapter.begin_transaction().expect("begin");
        let scoped = transaction.adapter();
        let request = WriteRequest::from_params(
            &params(json!({"store": "users", "rowId": id})),
            &scoped.capabilities(),
            WriteOp::Delete,
        )
        .expect("a valid delete");
        assert_eq!(scoped.write(&request).expect("delete").changes, 1);
        transaction.rollback().expect("rollback");

        assert_eq!(total(&adapter, "users"), before, "{store} lost the row");
    }
}

// ---- read-your-own-writes -----------------------------------------------

#[test]
fn a_read_inside_the_transaction_sees_the_pending_insert() {
    for store in STORES {
        let (adapter, _fixture) = writable_over(store);
        let before = total(&adapter, "users");

        let transaction = adapter.begin_transaction().expect("begin");
        let scoped = transaction.adapter();
        insert(scoped.as_ref(), "users", "ada");
        // A person who has just inserted a row and cannot see it has been told
        // their edit did not work.
        assert_eq!(total(scoped.as_ref(), "users"), before + 1, "{store}");
        transaction.rollback().expect("rollback");
    }
}

#[test]
fn list_stores_counts_through_the_transaction() {
    let (adapter, _fixture) = writable_over("memory");
    let before = total(&adapter, "users");

    let transaction = adapter.begin_transaction().expect("begin");
    let scoped = transaction.adapter();
    insert(scoped.as_ref(), "users", "ada");

    let counted = scoped
        .list_stores()
        .expect("stores")
        .into_iter()
        .find(|store| store.name == "users")
        .expect("users is listed")
        .approx_count
        .expect("a count");
    assert_eq!(counted, before + 1);
    transaction.rollback().expect("rollback");
}

#[test]
fn another_reader_does_not_see_uncommitted_documents() {
    let (adapter, _fixture) = writable_over("memory");
    let before = total(&adapter, "users");

    let transaction = adapter.begin_transaction().expect("begin");
    insert(transaction.adapter().as_ref(), "users", "ada");
    // The base adapter is what another connection resolves to, and §3.1 says it
    // must not see this connection's uncommitted rows.
    assert_eq!(total(&adapter, "users"), before);
    transaction.rollback().expect("rollback");
}

// ---- failures -----------------------------------------------------------

#[test]
fn a_refused_write_leaves_the_transaction_usable() {
    let (adapter, _fixture) = writable_over("memory");
    let before = total(&adapter, "users");

    let transaction = adapter.begin_transaction().expect("begin");
    let scoped = transaction.adapter();
    insert(scoped.as_ref(), "users", "ada");

    let bad = WriteRequest::from_params(
        &params(json!({"store": "no_such_store", "values": {"name": "x"}})),
        &scoped.capabilities(),
        WriteOp::Insert,
    )
    .expect("a valid shape for a store that is not there");
    assert_eq!(
        scoped.write(&bad).expect_err("expected a refusal").kind,
        BridgeErrorKind::BadRequest
    );

    insert(scoped.as_ref(), "users", "grace");
    transaction.commit().expect("commit");
    assert_eq!(total(&adapter, "users"), before + 2);
}

#[test]
fn a_repository_is_resolved_through_the_transaction_too() {
    let (adapter, fixture) = writable_over("memory");
    let store = fixture.repositories[0].name().to_string();
    let before = total(&adapter, &store);

    let transaction = adapter.begin_transaction().expect("begin");
    let scoped = transaction.adapter();
    insert(scoped.as_ref(), &store, "note in flight");
    assert_eq!(total(scoped.as_ref(), &store), before + 1);
    transaction.rollback().expect("rollback");

    assert_eq!(total(&adapter, &store), before);
}
