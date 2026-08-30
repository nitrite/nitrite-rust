//! A Nitrite application you can browse from Fanlight.
//!
//! ```text
//! cargo run --features inspect      # a bridge starts, and prints a pairing code
//! cargo run                         # no bridge, and none in the binary
//! ```
//!
//! **The release guard is the `inspect` feature and nothing else.** Everything
//! that touches the bridge is behind one door, and a release build does not open
//! it: no reflection, no runtime flag, and nothing for a linker to strip because
//! there is nothing there. `tool/verify_release_binary.sh` builds this twice and
//! greps both artifacts, because a check with no negative control proves nothing.

use nitrite::doc;
use nitrite::nitrite::Nitrite;

#[cfg(feature = "inspect")]
mod inspect;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Nitrite::builder().open_or_create(None, None)?;
    let users = db.collection("users")?;
    users.insert_many(
        (0..40i64)
            .map(|i| doc! {"id": i, "name": (format!("user {i}"))})
            .collect::<Vec<_>>(),
    )?;
    println!("opened a database with {} users", users.size()?);

    #[cfg(feature = "inspect")]
    inspect::serve(db)?;

    #[cfg(not(feature = "inspect"))]
    println!("built without --features inspect, so there is no bridge in this binary");

    Ok(())
}
