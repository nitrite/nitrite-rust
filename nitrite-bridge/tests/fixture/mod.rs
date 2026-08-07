//! One Nitrite database with something in every shape the protocol has to
//! carry, opened over either store.
//!
//! Shared by the unit tests and the reference bridge, so both are looking at the
//! same rows. Parallel to the JVM adapter's `StoreMatrixTest`: the point of
//! taking the store as an argument is that "browses correctly on every engine"
//! is held by running rather than asserted in prose.

#![allow(dead_code)]

use nitrite::collection::{Document, NitriteCollection};
use nitrite::common::Value;
use nitrite::doc;
use nitrite::index::full_text_index;
use nitrite::nitrite::Nitrite;
use nitrite_derive::{Convertible, NitriteEntity};
use nitrite_fjall_adapter::FjallModule;

/// A repository entity, so a keyed repository has something to hold.
///
/// M4's acceptance criterion names keyed repositories, which are first-class
/// only in `nitrite-rust`; the protocol models them as a `key` attribute on a
/// store, so the difference stays inside the adapter.
#[derive(NitriteEntity, Convertible, Default, Clone)]
#[entity(id(field = "id"))]
pub struct Order {
    pub id: i64,
    pub qty: i64,
    pub label: String,
}

pub struct Fixture {
    pub db: Nitrite,
    pub repositories: Vec<NitriteCollection>,
    /// Held for as long as the database is open: dropping it deletes the fjall
    /// keyspace out from under a reader.
    _scratch: Option<tempfile::TempDir>,
}

/// Opens a fixture database over `store`, which is `memory` or `fjall`.
pub fn open(store: &str) -> Result<Fixture, Box<dyn std::error::Error>> {
    let (db, scratch) = match store {
        "memory" => (Nitrite::builder().open_or_create(None, None)?, None),
        "fjall" => {
            let scratch = tempfile::tempdir()?;
            let module = FjallModule::with_config()
                .db_path(scratch.path().to_str().ok_or("scratch path is not utf-8")?)
                .low_memory_preset()
                .build();
            (
                Nitrite::builder()
                    .load_module(module)
                    .open_or_create(None, None)?,
                Some(scratch),
            )
        }
        other => return Err(format!("unknown store: {other}").into()),
    };

    let users = db.collection("users")?;
    // `DBINSPECT_FIXTURE_ROWS=50000` is how the page-latency budget gets a
    // 50k-row store to page through (`PLAN.md` §7). The default is what the
    // conformance suite and the unit tests expect.
    let rows: i64 = std::env::var("DBINSPECT_FIXTURE_ROWS")
        .ok()
        .and_then(|count| count.parse().ok())
        .unwrap_or(250);

    // One blob larger than the 64 KB inline ceiling, so the suite has a
    // truncated value to check rather than skipping that shape.
    let avatar: Vec<u8> = (0..100 * 1024).map(|i| (i % 251) as u8).collect();
    let mut documents: Vec<Document> = Vec::with_capacity(rows as usize);
    for i in 0..rows {
        let mut document = doc! {
            "id": i,
            "name": (if i == 3 { "user with a ünicode name".to_string() } else { format!("user {i}") }),
            "score": (i as f64 / 3.0),
        };
        // Half the rows have no `age` at all, which is what makes the sampled
        // schema's `nullable` mean something.
        if i % 2 == 0 {
            document.put("age", 20 + (i % 50))?;
        }
        if i == 0 {
            document.put("avatar", Value::Bytes(avatar.clone()))?;
            // A nested document, which the adapter must unwrap rather than
            // render as a debug string.
            document.put("address", Value::Document(doc! {"city": "Kolkata"}))?;
        }
        documents.push(document);
    }
    users.insert_many(documents)?;
    // `text` needs a full-text index on the field, in all three Nitrite
    // implementations. Without one the engine refuses at query time, so the
    // operator is only actually round-trippable against an indexed field — and
    // both halves of that are worth a test.
    users.create_index(vec!["name"], &full_text_index())?;

    // A name that needs quoting elsewhere. The Dart and JVM fixtures use
    // `order details`, with a space; `nitrite-rust` refuses a collection name
    // containing one, which the other two implementations do not — so this is
    // the nearest legal equivalent rather than the identical string.
    let orders = db.collection("order-details")?;
    orders.insert_many(
        (0..12i64)
            .map(|i| doc! {"id": i, "qty": i})
            .collect::<Vec<_>>(),
    )?;

    db.collection("empty_collection")?;

    // A plain repository and a keyed one, so `listStores` reports a `key` on
    // exactly one of them.
    let repository = db.repository::<Order>()?;
    for i in 0..30i64 {
        repository.insert(Order {
            id: i,
            qty: i * 2,
            label: format!("order {i}"),
        })?;
    }
    let archive = db.keyed_repository::<Order>("archive")?;
    for i in 0..4i64 {
        archive.insert(Order {
            id: i,
            qty: i,
            label: "archived".into(),
        })?;
    }

    Ok(Fixture {
        db,
        repositories: vec![
            repository.document_collection(),
            archive.document_collection(),
        ],
        _scratch: scratch,
    })
}
