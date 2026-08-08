# nitrite-bridge

**Inspect a running Nitrite database from a desktop client, over a paired
connection.**

No copying the keyspace off the machine, no guessing which files belong with it.
Add this crate to your dev profile, run your application, and browse your live
collections and repositories.

The engine-neutral core — wire protocol, pairing, transport, release guard —
is [`dbinspect-bridge`](https://github.com/nitrite/dbinspect) and knows about no
database at all. This crate is the Nitrite adapter, and nothing else.

## Use

```toml
[dev-dependencies]
nitrite-bridge = { version = "0.1", features = ["bridge"] }
```

```rust,ignore
use std::sync::Arc;
use dbinspect_bridge::{BridgeAdapter, BridgeMethods, BridgeServerOptions};
use nitrite_bridge::NitriteAdapter;

let adapter: Arc<dyn BridgeAdapter> =
    Arc::new(NitriteAdapter::new(db, "nitrite-main", "app data"));
let bridge = dbinspect_bridge::start(BridgeServerOptions::new(
    BridgeMethods::new("my_app", vec![adapter]),
))
.await?;
println!("{}", bridge.unwrap().banner());   // the pairing code
```

**Collections are discovered; repositories are handed in.** A store name off the
wire is not a Rust type, and there is no runtime registry to turn one back into
a repository — nor can the backing collection be opened by name, because Nitrite
refuses a collection name a repository owns. So pass the repositories you want
inspected:

```rust,ignore
let adapter = NitriteAdapter::new(db.clone(), "nitrite-main", "app data")
    .with_repositories(vec![
        db.repository::<Order>()?.document_collection(),
        db.keyed_repository::<Order>("archive")?.document_collection(),
    ]);
```

A keyed repository is listed under its `entityName+key` name with the key
reported beside it, and is browsable like any other store.

## Safe by default

**The `bridge` feature is the release guard.** It is off by default, so a build
that does not name it compiles no server, no protocol strings and no adapter
into your binary — there is nothing there to switch on, and nothing for a linker
to strip. Depend on it from `[dev-dependencies]` and it cannot reach a release
build at all.

**It binds `127.0.0.1`.** Reaching it from another machine is a deliberate act —
`adb forward tcp:9000 tcp:9000` or an SSH tunnel. Binding elsewhere forces TLS
with a certificate generated for the session, whose SHA-256 fingerprint goes in
the pairing banner for the client to pin.

**Read-only, and everything else is off.** `edit` and `snapshot` are `false`,
`regex` is absent from `filterOps` unless you call `.allow_regex(true)`. A
store name from a client is resolved against the set the adapter reported, so a
paired client cannot make `Nitrite::collection` create anything.

**Pairing is 40 bits**, regenerated per run, compared in constant time, with a
failure budget that is per bridge session rather than per connection.

## Filter operators

`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `notIn`, `exists`, `text`, plus
`and`, `or`, `not` — and `regex` behind `allow_regex`. `capabilities.filterOps`
is authoritative and the client greys out what is missing.

`exists` needs `nitrite` 0.7.0, which is the floor this crate sets. It tests
presence only: a field explicitly set to `Value::Null` is present and matches,
and "does not have the field" is `not` around it, never `exists` with
`value: false`. `between` and `elem_match` are the other direction — Rust has
them and the v1 protocol does not.

`text` needs a full-text index on the field; without one the engine refuses and
the adapter surfaces that as an `adapter` error rather than an opaque failure.

## Checks

This package is deliberately **outside** the workspace `members`: it depends on
`dbinspect-bridge`, which is not on crates.io yet, so a workspace build of a
clean checkout would fail on it. Build it on its own.

```bash
cargo test --manifest-path nitrite-bridge/Cargo.toml --features bridge
```

The protocol conformance suite is a language-neutral driver that lives in the
`dbinspect` repository. Point it at the reference bridge, over either store:

```bash
cd nitrite-bridge && ./tool/run_reference_bridge.sh fjall   # prints {"host":…,"port":…,"code":…}
cd <dbinspect>/conformance && dart run bin/dbinspect_conformance.dart <host:port> <code>
```

Criterion 2 — a release artifact contains none of the protocol strings — is
checked against the example application, which builds twice so the check has a
negative control:

```bash
cd nitrite-bridge/example && ./tool/verify_release_binary.sh
```

## Clients

[Fanlight](https://fanlight.dizitart.com) is a desktop client for this protocol
and is a separate, commercial product. The protocol is open and this crate is
Apache-2.0; writing another client requires only `dbinspect`'s `PROTOCOL.md`.

## Licence

Apache-2.0.
