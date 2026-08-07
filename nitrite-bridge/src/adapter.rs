//! Inspects a running Nitrite database.

use std::time::Instant;

use dbinspect_bridge::{
    encode_row, AdapterCapabilities, BridgeAdapter, BridgeError, BridgeResult, ColumnInfo,
    PageRequest, QueryConsole, QueryPage, StoreInfo, StoreSchema, Unsubscribe, WatchScope,
};
use nitrite::collection::{
    CollectionEventListener, CollectionEvents, FindOptions, NitriteCollection,
};
use nitrite::common::{SortOrder, DOC_ID, KEY_OBJ_SEPARATOR};

use nitrite::filter::all;
use nitrite::nitrite::Nitrite;

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
    capabilities: AdapterCapabilities,
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
            capabilities: capabilities(false),
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
        self.capabilities = capabilities(allow_regex);
        self
    }

    /// Turns a client-supplied store name into one this adapter reported.
    ///
    /// An allow-list, and load-bearing for a reason particular to Nitrite:
    /// `Nitrite::collection` **creates** a collection that does not exist.
    /// Passing an unchecked name through would let a paired client litter the
    /// developer's database with empty collections.
    fn resolve(&self, store: &str) -> BridgeResult<NitriteCollection> {
        for repository in &self.repositories {
            if repository.name() == store {
                return Ok(repository.clone());
            }
        }
        let names = self
            .db
            .list_collection_names()
            .map_err(|error| adapter_error("could not list collections", error))?;
        if names.contains(store) {
            return self
                .db
                .collection(store)
                .map_err(|error| adapter_error("could not open the store", error));
        }
        Err(BridgeError::bad_request("unknown store"))
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

fn capabilities(allow_regex: bool) -> AdapterCapabilities {
    let mut ops: Vec<String> = NITRITE_FILTER_OPS.iter().map(|op| op.to_string()).collect();
    if allow_regex {
        ops.push(NITRITE_REGEX_OP.to_string());
    }
    // Everything dangerous stays off: `edit` and `snapshot` arrive with the
    // milestones that implement them, and `sql` never applies to a document
    // store.
    AdapterCapabilities::read_only(QueryConsole::Filter)
        // Nitrite's own collection-level subscription, so a write from anywhere
        // in this process is seen — not only this bridge's own.
        .watching(WatchScope::Engine)
        .with_filter_ops(ops)
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
            let collection = self
                .db
                .collection(&name)
                .map_err(|error| adapter_error("could not open a collection", error))?;
            stores.push(StoreInfo::new(name, "collection", size_of(&collection)));
        }

        for repository in &self.repositories {
            let name = repository.name();
            // A keyed repository is stored under `entityName+key`; the key is
            // reported beside the name so the client can label it, while the
            // name stays the one addressable identity `store` carries.
            let key = name
                .split_once(KEY_OBJ_SEPARATOR)
                .map(|(_, key)| key.to_string());
            let mut info = StoreInfo::new(name, "repository", size_of(repository));
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
