//! Inspect a running Nitrite database from a desktop client.
//!
//! The engine-neutral core — wire protocol, pairing, transport, release guard —
//! is in the `dbinspect-bridge` crate and knows about no database at all. This
//! crate is the Nitrite adapter, and nothing else.
//!
//! **Everything here is behind the non-default `bridge` feature, and that is the
//! release guard** (threat model F5, criterion 2). A `cargo build` that does not
//! name the feature compiles no server, no protocol strings and no adapter into
//! the binary — there is nothing there to switch on. Depend on this crate from a
//! dev profile only:
//!
//! ```toml
//! [dev-dependencies]
//! nitrite-bridge = { version = "0.1", features = ["bridge"] }
//! ```

#![forbid(unsafe_code)]

#[cfg(feature = "bridge")]
mod adapter;
#[cfg(feature = "bridge")]
pub mod filter_dsl;
/// Nitrite's `Value` on its way to the wire.
///
/// Public because Fanlight's Rust sidecar encodes the same documents from the
/// same engine, and a second implementation of this is the thing that drifts.
#[cfg(feature = "bridge")]
pub mod values;

#[cfg(feature = "bridge")]
pub use adapter::{NitriteAdapter, DEFAULT_SAMPLE_SIZE};
