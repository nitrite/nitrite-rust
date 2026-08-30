//! Inspects a running Nitrite database.

use std::sync::Arc;
use std::time::Instant;

use dbinspect_bridge::{
    encode_row, AdapterCapabilities, AdapterTransaction, BlobChunk, BlobRequest, BridgeAdapter,
    BridgeError, BridgeResult, ColumnInfo, PageRequest, QueryConsole, QueryPage, StoreInfo,
    StoreSchema, Unsubscribe, WatchScope, WriteOp, WriteRequest, WriteResult,
};
use nitrite::collection::{
    CollectionEventListener, CollectionEvents, Document, FindOptions, NitriteCollection, NitriteId,
};
use nitrite::common::{SortOrder, Value, DOC_ID, KEY_OBJ_SEPARATOR};
use serde_json::{json, Map, Value as JsonValue};

use nitrite::filter::all;
use nitrite::nitrite::Nitrite;
use nitrite::transaction::{NitriteTransaction, Session};

use crate::filter_dsl::{parse_filter, NITRITE_FILTER_OPS, NITRITE_REGEX_OP};
use crate::values;

/// How many documents `get_schema` reads before answering. The schema is a
/// sample, never a guarantee, and says so on the wire.
pub const DEFAULT_SAMPLE_SIZE: u64 = 50;

/// Inspects a running Nitrite database.
///
/// The bridge core — protocol, pairing, transport, release guard — is in
/// `dbinspect-bridge` and knows about no database at all. This adapter is the
/// only part that knows about Nitrite, so inspecting a `redb` or `sled`
/// database pulls in none of it.
///
/// ```no_run
/// # use std::sync::Arc;
/// # use nitrite::nitrite::Nitrite;
/// # use nitrite_bridge::NitriteAdapter;
/// # fn example(db: Nitrite) {
/// let adapter = Arc::new(NitriteAdapter::new(db, "main", "app data"));
/// # let _ = adapter;
/// # }
/// ```
///
/// **Collections are discovered; repositories are handed in.** Nitrite opens an
/// `ObjectRepository` by Rust type, and a name off the wire is not a type —
/// there is no `repository("org.example.Order")` to call. Worse, opening a
/// repository's name as a *collection* is refused by Nitrite itself, so a
/// repository nobody handed in is not reachable at all. The developer therefore
/// passes the repositories they want inspected:
///
/// ```no_run
/// # use nitrite::nitrite::Nitrite;
/// # use nitrite_bridge::NitriteAdapter;
/// # use nitrite::repository::NitriteEntity;
/// # use nitrite::common::Convertible;
/// # fn example<Order>(db: Nitrite) -> Result<(), Box<dyn std::error::Error>>
/// # where Order: Convertible<Output = Order> + NitriteEntity + Send + Sync + 'static {
/// let orders = db.repository::<Order>()?.document_collection();
/// let archive = db.keyed_repository::<Order>("archive")?.document_collection();
/// let adapter = NitriteAdapter::new(db, "main", "app data")
///     .with_repositories(vec![orders, archive]);
/// # let _ = adapter;
/// # Ok(())
/// # }
/// ```
///
/// A repository that was not passed in is not listed, rather than listed and
/// unopenable.
pub struct NitriteAdapter {
    db: Nitrite,
    id: String,
    display_name: String,
    engine: String,
    repositories: Vec<NitriteCollection>,
    sample_size: u64,
    allow_regex: bool,
    allow_write: bool,
    allow_snapshot: bool,
    capabilities: AdapterCapabilities,
    /// The open transaction this adapter is scoped to, or `None` on the one that
    /// is not.
    ///
    /// See [`NitriteAdapter::begin_transaction`]: a transaction is a second
    /// adapter over the same database rather than a mode on this one, so that
    /// one connection's uncommitted documents can never reach another
    /// connection's reads.
    transaction: Option<NitriteTransaction>,
}

impl NitriteAdapter {
    pub fn new(db: Nitrite, id: impl Into<String>, display_name: impl Into<String>) -> Self {
        let engine = engine_of(&db);
        Self {
            db,
            id: id.into(),
            display_name: display_name.into(),
            engine,
            repositories: Vec::new(),
            sample_size: DEFAULT_SAMPLE_SIZE,
            allow_regex: false,
            allow_write: false,
            allow_snapshot: false,
            capabilities: capabilities(false, false, false),
            transaction: None,
        }
    }

    pub fn with_repositories(mut self, repositories: Vec<NitriteCollection>) -> Self {
        self.repositories = repositories;
        self
    }

    pub fn with_sample_size(mut self, sample_size: u64) -> Self {
        self.sample_size = sample_size.max(1);
        self
    }

    /// Threat model F10, criterion 9: off unless the embedding developer turned
    /// it on for this adapter, and absent from `filterOps` when off.
    pub fn allow_regex(mut self, allow_regex: bool) -> Self {
        self.allow_regex = allow_regex;
        self.capabilities = capabilities(allow_regex, self.allow_write, self.allow_snapshot);
        self
    }

    /// Threat model rule 5: the three write methods are refused by the core
    /// until this, and `edit` is absent from `capabilities` while it is off.
    pub fn allow_write(mut self, allow_write: bool) -> Self {
        self.allow_write = allow_write;
        self.capabilities = capabilities(self.allow_regex, allow_write, self.allow_snapshot);
        self
    }

    /// Whole-store `snapshot`, off by default: it is the one method that returns
    /// everything in a single call.
    pub fn allow_snapshot(mut self, allow_snapshot: bool) -> Self {
        self.allow_snapshot = allow_snapshot;
        self.capabilities = capabilities(self.allow_regex, self.allow_write, allow_snapshot);
        self
    }

    /// Turns a client-supplied store name into one this adapter reported.
    ///
    /// An allow-list, and load-bearing for a reason particular to Nitrite:
    /// `Nitrite::collection` **creates** a collection that does not exist.
    /// Passing an unchecked name through would let a paired client litter the
    /// developer's database with empty collections.
    fn resolve(&self, store: &str) -> BridgeResult<NitriteCollection> {
        let known = self.repositories.iter().any(|r| r.name() == store)
            || self
                .db
                .list_collection_names()
                .map_err(|error| adapter_error("could not list collections", error))?
                .contains(store);
        if !known {
            return Err(BridgeError::bad_request("unknown store"));
        }

        let repository = self
            .repositories
            .iter()
            .find(|candidate| candidate.name() == store);

        // Inside a transaction, always the transaction's view — a repository's
        // handle included. A read through the primary would miss the documents
        // staged beside it, and a write through it would land outside the
        // transaction entirely.
        if let Some(transaction) = &self.transaction {
            return match repository {
                // `Nitrite::collection` refuses a name a repository owns, so a
                // repository cannot go through `NitriteTransaction::collection`
                // — and this adapter holds document collections rather than
                // typed repositories, so it has no `T` for the other door.
                // `view_of` is that door.
                Some(repository) => transaction.view_of(repository.clone()),
                None => transaction.collection(store),
            }
            .map_err(|error| adapter_error("could not open the store", error));
        }

        if let Some(repository) = repository {
            return Ok(repository.clone());
        }
        self.db
            .collection(store)
            .map_err(|error| adapter_error("could not open the store", error))
    }

    /// The column names a sample showed, in the order they were first seen.
    ///
    /// Insertion-ordered rather than sorted: the first document's fields come
    /// first, and a field only some documents carry lands after them, which is
    /// closer to how the developer thinks about the store.
    fn sampled_columns(&self, store: &str) -> BridgeResult<(Vec<ColumnInfo>, usize)> {
        let collection = self.resolve(store)?;
        let mut cursor = collection
            .find_with_options(all(), &FindOptions::new().limit(self.sample_size))
            .map_err(|error| adapter_error("could not sample the store", error))?;

        let mut order: Vec<String> = Vec::new();
        let mut types: Vec<Option<&'static str>> = Vec::new();
        let mut seen: Vec<usize> = Vec::new();
        let mut sampled = 0usize;

        for document in cursor.by_ref() {
            let document =
                document.map_err(|error| adapter_error("could not read a document", error))?;
            sampled += 1;
            for (name, value) in document.iter() {
                let at = match order.iter().position(|known| *known == name) {
                    Some(at) => at,
                    None => {
                        order.push(name.clone());
                        types.push(None);
                        seen.push(0);
                        order.len() - 1
                    }
                };
                seen[at] += 1;
                if types[at].is_none() {
                    types[at] = values::type_of(&value);
                }
            }
        }

        let columns = order
            .iter()
            .enumerate()
            .map(|(at, name)| {
                ColumnInfo::new(
                    name,
                    if name == DOC_ID {
                        "id"
                    } else {
                        types[at].unwrap_or("unknown")
                    },
                    // A document store has no declared nullability, so this is
                    // what the sample showed: a field missing from any sampled
                    // document.
                    name != DOC_ID && seen[at] != sampled,
                    name == DOC_ID,
                )
            })
            .collect();
        Ok((columns, sampled))
    }
}

fn capabilities(allow_regex: bool, allow_write: bool, allow_snapshot: bool) -> AdapterCapabilities {
    let mut ops: Vec<String> = NITRITE_FILTER_OPS.iter().map(|op| op.to_string()).collect();
    if allow_regex {
        ops.push(NITRITE_REGEX_OP.to_string());
    }
    // Everything dangerous stays off unless the embedding developer turned it on
    // for this adapter (threat model rule 5); `sql` never applies to a document
    // store.
    let mut capabilities = AdapterCapabilities::read_only(QueryConsole::Filter)
        // Nitrite's own collection-level subscription, so a write from anywhere
        // in this process is seen — not only this bridge's own.
        .watching(WatchScope::Engine)
        .with_filter_ops(ops);
    capabilities.edit = allow_write;
    capabilities.snapshot = allow_snapshot;
    // Not an opt-in (`docs/PROTOCOL.md` §3.1): Nitrite's transaction is
    // implemented above the store, so it is available on every engine this
    // adapter can be pointed at — Fjall and in-memory alike. `allow_write` is
    // the permission; this reports what the engine can undo.
    capabilities.transactions = allow_write;
    // Not an opt-in: `query_page` already showed the first 64 KB of this very
    // cell, and `docs/PROTOCOL.md` §2 has always promised the rest on request.
    // A document is addressed by `_id`, which every row has.
    capabilities.blob = true;
    capabilities
}

/// `memory`, `fjall`, or whatever the store calls itself — taken from
/// `store_version`, which reads `InMemory/0.5.0` or `Fjall/2.6.3`.
fn engine_of(db: &Nitrite) -> String {
    let version = db
        .store()
        .store_version()
        .unwrap_or_else(|_| "unknown".into());
    let name = version
        .split('/')
        .next()
        .unwrap_or("unknown")
        .to_lowercase();
    if name == "inmemory" {
        "memory".to_string()
    } else {
        name
    }
}

/// The developer gets the engine's own message from their application's log; the
/// wire gets a kind and a sentence that names no path.
fn adapter_error(what: &str, error: impl std::fmt::Display) -> BridgeError {
    log::warn!("dbinspect: {what}: {error}");
    BridgeError::adapter(what.to_string())
}

impl BridgeAdapter for NitriteAdapter {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        "nitrite"
    }

    fn display_name(&self) -> &str {
        &self.display_name
    }

    fn engine(&self) -> Option<&str> {
        Some(&self.engine)
    }

    fn capabilities(&self) -> AdapterCapabilities {
        self.capabilities.clone()
    }

    /// Opens one Nitrite transaction (`docs/PROTOCOL.md` §3.1).
    ///
    /// Nitrite's transaction lives above the storage engine — a transactional
    /// map buffers the writes and a journal replays them on commit — so this
    /// works identically on Fjall and in memory. Nothing here re-implements
    /// either half; the session and the transaction are the engine's own.
    ///
    /// The session is held alongside the transaction and closed by whichever of
    /// commit or rollback runs, because closing a session rolls back anything
    /// still open in it — which is what makes the ending safe on both paths.
    fn begin_transaction(&self) -> BridgeResult<Box<dyn AdapterTransaction>> {
        let session = self
            .db
            .create_session()
            .map_err(|error| adapter_error("could not open a session", error))?;
        let transaction = session.begin_transaction().map_err(|error| {
            let _ = session.close();
            adapter_error("the database would not begin a transaction", error)
        })?;

        let scoped = Arc::new(NitriteAdapter {
            db: self.db.clone(),
            id: self.id.clone(),
            display_name: self.display_name.clone(),
            engine: self.engine.clone(),
            repositories: self.repositories.clone(),
            sample_size: self.sample_size,
            allow_regex: self.allow_regex,
            allow_write: self.allow_write,
            allow_snapshot: self.allow_snapshot,
            // Every capability but `transactions` carried over: a gate that
            // changed inside a transaction would be a second, invisible
            // permission model. That one drops, because Nitrite does not nest.
            capabilities: AdapterCapabilities {
                transactions: false,
                ..self.capabilities.clone()
            },
            transaction: Some(transaction.clone()),
        });

        Ok(Box::new(NitriteAdapterTransaction {
            scoped,
            transaction,
            session,
        }))
    }

    fn list_stores(&self) -> BridgeResult<Vec<StoreInfo>> {
        let mut stores = Vec::new();

        let mut names: Vec<String> = self
            .db
            .list_collection_names()
            .map_err(|error| adapter_error("could not list collections", error))?
            .into_iter()
            .collect();
        // The store hands back a set; a client's list must not reshuffle between
        // calls.
        names.sort();

        for name in names {
            // Through `resolve` rather than off `db` directly, so a count taken
            // inside a transaction includes the documents staged there —
            // read-your-own-writes covers `listStores` as much as it covers a
            // page (§3.1).
            let collection = self.resolve(&name)?;
            stores.push(StoreInfo::new(name, "collection", size_of(&collection)));
        }

        for repository in &self.repositories {
            let name = repository.name();
            let scoped = self.resolve(&name)?;
            // A keyed repository is stored under `entityName+key`; the key is
            // reported beside the name so the client can label it, while the
            // name stays the one addressable identity `store` carries.
            let key = name
                .split_once(KEY_OBJ_SEPARATOR)
                .map(|(_, key)| key.to_string());
            let mut info = StoreInfo::new(name, "repository", size_of(&scoped));
            if let Some(key) = key {
                info = info.keyed(key);
            }
            stores.push(info);
        }

        Ok(stores)
    }

    fn get_schema(&self, store: &str) -> BridgeResult<StoreSchema> {
        let (columns, sampled) = self.sampled_columns(store)?;
        Ok(StoreSchema {
            columns,
            // Never false here. A developer must never mistake a sample for a
            // guarantee (`docs/PROTOCOL.md` §2).
            inferred: true,
            sampled_docs: Some(sampled),
        })
    }

    fn query_page(&self, request: &PageRequest) -> BridgeResult<QueryPage> {
        let collection = self.resolve(&request.store)?;
        let filter = match &request.filter {
            None => all(),
            Some(tree) => parse_filter(tree, self.allow_regex)?,
        };

        let mut options = FindOptions::new()
            .skip(request.offset())
            .limit(request.page_size);
        if let Some(sort_by) = &request.sort_by {
            // Nitrite will happily sort by a field no document has — every value
            // is null and the order is arbitrary. Showing rows in an order the
            // client did not ask for is the same failure as showing rows it
            // filtered out, so the sort column is checked against the sampled
            // schema: exactly the set of columns the client was given to offer.
            //
            // ponytail: re-samples per sorted page. Cache the schema per store
            // if a sorted page ever misses the budget.
            let (columns, _) = self.sampled_columns(&request.store)?;
            if !columns.iter().any(|column| &column.name == sort_by) {
                return Err(BridgeError::bad_request("unknown sort column"));
            }
            options = options.sort_by(
                sort_by.clone(),
                if request.desc {
                    SortOrder::Descending
                } else {
                    SortOrder::Ascending
                },
            );
        }

        let started = Instant::now();
        let mut cursor = collection
            .find_with_options(filter.clone(), &options)
            .map_err(|error| adapter_error("the store could not answer the query", error))?;

        let mut rows = Vec::new();
        for document in cursor.by_ref() {
            let document =
                document.map_err(|error| adapter_error("could not read a document", error))?;
            rows.push(encode_row(
                document
                    .iter()
                    .map(|(name, value)| (name, values::encode(&value)))
                    .collect::<Vec<_>>(),
            ));
        }

        // ponytail: a second pass to count. Nitrite answers an unfiltered count
        // from the map; cache it per store if a large filtered store ever misses
        // the budget.
        let total = match request.filter {
            None => size_of(&collection),
            Some(_) => {
                let mut counting = collection
                    .find(filter)
                    .map_err(|error| adapter_error("the store could not count the query", error))?;
                Some(counting.by_ref().count() as u64)
            }
        };

        Ok(QueryPage {
            has_more: total.is_some_and(|total| (request.offset() + rows.len() as u64) < total),
            total,
            rows,
            elapsed_ms: started.elapsed().as_millis() as u64,
            page_size_clamped: request.page_size_clamped,
        })
    }

    /// One row, addressed by `_id` — the identity `docs/PROTOCOL.md` §3 gives
    /// every Nitrite implementation.
    fn write(&self, request: &WriteRequest) -> BridgeResult<WriteResult> {
        let collection = self.resolve(&request.store)?;

        match request.op {
            WriteOp::Insert => {
                let document = document_of(&request.values)?;
                let written = collection
                    .insert(document)
                    .map_err(|error| adapter_error("the store refused the insert", error))?;
                let ids = written.affected_nitrite_ids();
                Ok(WriteResult {
                    changes: ids.len() as u64,
                    // The value the client addresses the row by afterwards, in
                    // the rendering `_id` already has in a page.
                    id: ids.first().map(|id| json!(id.to_string())),
                })
            }
            WriteOp::Update => {
                if request.values.contains_key(DOC_ID) {
                    // Nitrite merges the update document, so an `_id` in it
                    // would rewrite the identity of the row it just matched.
                    // The identity is `rowId`, and it is not editable.
                    return Err(BridgeError::bad_request("_id is not an editable field")
                        .with_detail("a row is addressed by rowId; the engine owns its identity"));
                }
                let id = row_id_of(request)?;
                let update = document_of(&request.values)?;
                let written = collection
                    .update_by_id(&id, &update, false)
                    .map_err(|error| adapter_error("the store refused the update", error))?;
                Ok(WriteResult::new(written.affected_nitrite_ids().len() as u64))
            }
            WriteOp::Delete => {
                let id = row_id_of(request)?;
                let existing = collection
                    .get_by_id(&id)
                    .map_err(|error| adapter_error("the store could not read the row", error))?;
                // `changes: 0` is an answer, not an error: the row the client
                // addressed was not there.
                let Some(existing) = existing else {
                    return Ok(WriteResult::new(0));
                };
                let written = collection
                    .remove_one(&existing)
                    .map_err(|error| adapter_error("the store refused the delete", error))?;
                Ok(WriteResult::new(written.affected_nitrite_ids().len() as u64))
            }
        }
    }

    /// One binary cell, whole, rather than the 64 KB `query_page` showed.
    ///
    /// Read by id rather than by filter: this is the O(1) lookup the engine
    /// already has, and the row the client is looking at is one it has an `_id`
    /// for by definition.
    fn fetch_blob(&self, request: &BlobRequest) -> BridgeResult<Option<BlobChunk>> {
        let collection = self.resolve(&request.store)?;
        let id = parse_id(&request.row_id)?;
        let document = collection
            .get_by_id(&id)
            .map_err(|error| adapter_error("the store could not read the row", error))?;
        let Some(document) = document else {
            return Ok(None);
        };

        let value = document
            .get(&request.column)
            .map_err(|error| adapter_error("the store could not read the field", error))?;
        match value {
            Value::Null | Value::Unknown => Ok(None),
            Value::Bytes(bytes) => Ok(Some(BlobChunk::slice(&bytes, request))),
            // A client asking for the bytes of a field that is not bytes has a
            // stale schema, and a rendering handed back as a file is a
            // fabricated download rather than a helpful one.
            other => Err(BridgeError::bad_request(format!(
                "\"{}\" is not a binary field",
                request.column
            ))
            .with_detail(format!(
                "it is {}",
                values::type_of(&other).unwrap_or("null")
            ))),
        }
    }

    fn watch(
        &self,
        store: &str,
        on_change: Box<dyn Fn(&str) + Send + Sync>,
    ) -> BridgeResult<Unsubscribe> {
        let collection = self.resolve(store)?;

        // Nitrite's five event names are the protocol's five: `insert`,
        // `update`, `remove`, `indexStart`, `indexEnd`. No mapping table needed,
        // and none invented — the coarser wire events exist for the engines that
        // need them.
        let listener = CollectionEventListener::new(move |event| {
            on_change(wire_event(event.event_type()));
            Ok(())
        });

        let subscription = collection
            .subscribe(listener)
            .map_err(|error| adapter_error("could not subscribe to the store", error))?;
        let Some(subscription) = subscription else {
            return Err(BridgeError::adapter("the store has no change feed"));
        };

        // The `SubscriberRef` moves into the closure and nowhere else: the core
        // owns exactly one of these per watched store and calls it once, so a
        // registry beside it would be a second copy of the same fact.
        Ok(Box::new(move || {
            if let Err(error) = collection.unsubscribe(subscription) {
                log::warn!("dbinspect: could not unsubscribe: {error}");
            }
        }))
    }
}

/// The document a write carries, with `_id` turned back into an identity.
///
/// The core already refused everything that is not a JSON scalar, so what is
/// left is the four shapes below and the one field Nitrite types itself.
fn document_of(values: &Map<String, JsonValue>) -> BridgeResult<Document> {
    let mut document = Document::new();
    for (field, value) in values {
        let value = if field == DOC_ID {
            Value::NitriteId(parse_id(value)?)
        } else {
            match value {
                JsonValue::Null => Value::Null,
                JsonValue::Bool(flag) => Value::Bool(*flag),
                JsonValue::Number(number) => match number.as_i64() {
                    Some(int) => Value::I64(int),
                    None => Value::F64(number.as_f64().unwrap_or_default()),
                },
                JsonValue::String(text) => Value::String(text.clone()),
                // Unreachable: `WriteRequest` refuses an array or an object.
                other => Value::String(other.to_string()),
            }
        };
        document
            .put(field.clone(), value)
            .map_err(|error| adapter_error("the store refused a field", error))?;
    }
    Ok(document)
}

/// `WriteRequest` guarantees an identity on update and delete; this turns it
/// into the one Nitrite addresses a row by.
fn row_id_of(request: &WriteRequest) -> BridgeResult<NitriteId> {
    parse_id(request.row_id.as_ref().expect("validated by the core"))
}

/// Accepts an id in the rendering a page carried — `[1755…]NO₂`, which is what
/// `NitriteId`'s `Display` produces and therefore what a client echoes back —
/// and the bare number underneath it, which is what a person types.
fn parse_id(value: &JsonValue) -> BridgeResult<NitriteId> {
    let text = match value {
        JsonValue::String(text) => text.clone(),
        JsonValue::Number(number) => number.to_string(),
        _ => return Err(BridgeError::bad_request("rowId is not a Nitrite _id")),
    };
    let digits = match (text.find('['), text.find(']')) {
        (Some(open), Some(close)) if open < close => &text[open + 1..close],
        _ => text.as_str(),
    };
    digits
        .parse::<u64>()
        .ok()
        .and_then(|id| NitriteId::create_id(id).ok())
        .ok_or_else(|| {
            BridgeError::bad_request("rowId is not a Nitrite _id")
                .with_detail("an _id is the value the store reported in that column")
        })
}

fn wire_event(event: CollectionEvents) -> &'static str {
    match event {
        CollectionEvents::Insert => "insert",
        CollectionEvents::Update => "update",
        CollectionEvents::Remove => "remove",
        CollectionEvents::IndexStart => "indexStart",
        CollectionEvents::IndexEnd => "indexEnd",
    }
}

/// `approxCount` may be omitted when counting is expensive, and the UI shows "—"
/// rather than guessing. A store that cannot answer is that case, not an error.
fn size_of(collection: &NitriteCollection) -> Option<u64> {
    collection.size().ok()
}

/// One open Nitrite transaction, and the session that owns it.
struct NitriteAdapterTransaction {
    scoped: Arc<NitriteAdapter>,
    transaction: NitriteTransaction,
    session: Session,
}

impl AdapterTransaction for NitriteAdapterTransaction {
    fn adapter(&self) -> Arc<dyn BridgeAdapter> {
        self.scoped.clone()
    }

    fn commit(self: Box<Self>) -> BridgeResult<()> {
        let outcome = self
            .transaction
            .commit()
            .map_err(|error| adapter_error("the database refused the commit", error));
        // Closed on both paths: a session that is not closed holds the
        // transactional maps, and a failed commit is exactly when letting go
        // matters most.
        let _ = self.session.close();
        outcome
    }

    fn rollback(self: Box<Self>) -> BridgeResult<()> {
        let outcome = self
            .transaction
            .rollback()
            .map_err(|error| adapter_error("could not roll back", error));
        let _ = self.session.close();
        outcome
    }
}
