mod collection;
mod event;
mod fts;
mod migration;
mod repository;
mod spatial;
mod transaction;

pub use repository::*;

#[ctor::ctor(unsafe)]
fn init() {
    colog::init();
}
