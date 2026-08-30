//! The one door. Compiled only with `--features inspect`.

use std::sync::Arc;

use dbinspect_bridge::{BridgeAdapter, BridgeMethods, BridgeServerOptions};
use nitrite::nitrite::Nitrite;
use nitrite_bridge::NitriteAdapter;

pub fn serve(db: Nitrite) -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let adapter: Arc<dyn BridgeAdapter> =
            Arc::new(NitriteAdapter::new(db, "nitrite-main", "app data"));
        let bridge = dbinspect_bridge::start(BridgeServerOptions::new(BridgeMethods::new(
            "nitrite_example",
            vec![adapter],
        )))
        .await?;

        match bridge {
            None => println!("this build does not contain the bridge"),
            Some(bridge) => {
                // The banner carries the pairing code; the logger is the seam,
                // and an application without one still gets it here.
                println!("{}", bridge.banner());
                println!("connect Fanlight to 127.0.0.1:{}", bridge.port());
                tokio::signal::ctrl_c().await?;
            }
        }
        Ok::<_, Box<dyn std::error::Error>>(())
    })
}
