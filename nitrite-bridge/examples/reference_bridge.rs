//! A Nitrite-backed bridge you can point the conformance suite at.
//!
//! ```text
//! tool/run_reference_bridge.sh [memory|fjall]
//! dart run .../conformance/bin/dbinspect_conformance.dart 127.0.0.1:<port> <code>
//! ```
//!
//! It prints one line of JSON — `{"host":…,"port":…,"code":…}` — on stdout before
//! anything else, so a script does not have to parse the pairing banner; the
//! banner and every log line go to stderr. Then it stays up until it is killed.
//!
//! The adapter is constructed with **no options**, because that is what
//! `docs/THREAT-MODEL.md` §7 criterion 10 is about.

use std::sync::Arc;

use dbinspect_bridge::{BridgeAdapter, BridgeMethods, BridgeServerOptions};
use nitrite_bridge::NitriteAdapter;

#[path = "../tests/fixture/mod.rs"]
mod fixture;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = std::env::args().nth(1).unwrap_or_else(|| "memory".into());
    eprintln!("dbinspect reference bridge (rust / nitrite, {store})");

    // Held for the process lifetime: a temporary directory that drops would take
    // the fjall keyspace with it while the suite is still reading.
    let fixture = fixture::open(&store)?;
    let adapter: Arc<dyn BridgeAdapter> = Arc::new(
        NitriteAdapter::new(fixture.db.clone(), "nitrite-main", "app data")
            .with_repositories(fixture.repositories.clone()),
    );

    let bridge = dbinspect_bridge::start(BridgeServerOptions::new(BridgeMethods::new(
        "reference_bridge",
        vec![adapter],
    )))
    .await?;

    let Some(bridge) = bridge else {
        eprintln!("this build does not contain the bridge — see dbinspect_bridge::bridge_enabled");
        std::process::exit(70); // EX_SOFTWARE
    };

    println!(
        r#"{{"host":"{}","port":{},"code":"{}"}}"#,
        bridge.address().ip(),
        bridge.port(),
        bridge.pairing_code().value()
    );
    eprintln!("{}", bridge.banner());

    // Nothing to do but stay up. The suite kills the process when it is
    // finished, and it has to be a fresh process for the next run anyway — the
    // last phase closes pairing for good.
    std::future::pending::<()>().await;
    Ok(())
}
