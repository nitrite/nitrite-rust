//! Thread-local bridge for an *ambient* Fjall write transaction.
//!
//! Nitrite's storage-agnostic core wraps every logical write (an explicit transaction
//! commit, or a single collection insert/update/remove and all of its index updates) in an
//! atomic scope via `NitriteStore::with_atomic` → `NitriteStoreProvider::run_atomic`. To make
//! that scope atomic on disk, the Fjall adapter runs the whole scope inside one
//! [`fjall::WriteTransaction`], so the data-partition write and every index-partition write
//! land together or not at all.
//!
//! The challenge is plumbing: the write fan-out reaches many independent [`crate::map::FjallMap`]
//! instances, and the core write/read methods (`put`, `get`, …) are storage-agnostic, so they
//! cannot carry a Fjall-specific `&mut WriteTransaction` parameter. Fjall's single-writer
//! `WriteTransaction` also borrows the keyspace's write lock (`MutexGuard`), so it is **not**
//! `'static` and cannot be *owned* by a thread-local.
//!
//! This module bridges that gap with a *scoped* thread-local raw pointer to a transaction that
//! lives on the stack of the enclosing scope. [`run_with_active`] installs the pointer for the
//! synchronous extent of the scope and restores the previous value on the way out (even on a
//! panic); [`with_active`] hands the active transaction (if any) to a closure.
//!
//! In addition to the raw pointer, the scope also tracks an in-memory overlay of writes staged
//! into the active transaction. Fjall 2.11.2's transactional read helpers do not resolve
//! KV-separated values, so the adapter reconstructs read-your-writes semantics itself by reading
//! committed partition state directly and layering the active transaction's inserts/removes on top.
//!
//! # Safety invariants
//! The single `unsafe` deref in [`with_active`] is sound because:
//! 1. **Liveness** — the pointer is non-null only between [`run_with_active`] installing it and
//!    its guard restoring the previous value, during which the pointed-to `WriteTransaction`
//!    is alive on this thread's stack.
//! 2. **No aliasing** — the pointer is per-thread; the transaction is only ever reached on the
//!    thread that installed it. Map operations within a scope run sequentially, and each
//!    obtains the `&mut` for the duration of a single, non-reentrant `with_active` call, so no
//!    two live `&mut WriteTransaction` references exist at once. (Parallel read fan-outs run on
//!    *other* threads, whose thread-local pointer is null, so they read committed state and
//!    never touch the transaction.)
//! 3. **No escape** — `with_active` only lends the reference to a closure that runs to
//!    completion before the borrow ends; no reference outlives the call.

use fjall::SingleWriterWriteTx as WriteTransaction;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

pub(crate) type OverlayValue = Box<[u8]>;

#[derive(Default)]
pub(crate) struct PartitionOverlay {
    pub(crate) entries: BTreeMap<Vec<u8>, Option<OverlayValue>>,
    size_delta: Option<i64>,
}

impl PartitionOverlay {
    #[inline]
    fn mark_dirty(&mut self) {
        self.size_delta = None;
    }

    #[inline]
    pub(crate) fn cached_size_delta(&self) -> Option<i64> {
        self.size_delta
    }

    #[inline]
    pub(crate) fn cache_size_delta(&mut self, delta: i64) {
        self.size_delta = Some(delta);
    }
}

#[derive(Default)]
struct TxOverlay {
    partitions: HashMap<Arc<str>, PartitionOverlay>,
}

thread_local! {
    /// Type-erased pointer to the `WriteTransaction` active on this thread, or null.
    static ACTIVE_TX: Cell<*mut ()> = const { Cell::new(std::ptr::null_mut()) };
    /// Overlay of writes buffered into the active transaction on this thread.
    static ACTIVE_OVERLAY: RefCell<Vec<TxOverlay>> = const { RefCell::new(Vec::new()) };
}

/// Restores the previous active-transaction pointer when dropped, so the thread-local is
/// always cleaned up — even if the scope's body panics.
struct ScopeGuard {
    prev: *mut (),
}

impl Drop for ScopeGuard {
    fn drop(&mut self) {
        ACTIVE_TX.with(|cell| cell.set(self.prev));
        ACTIVE_OVERLAY.with(|stack| {
            stack.borrow_mut().pop();
        });
    }
}

/// Installs `tx` as the active write transaction for the current thread for the dynamic
/// extent of `f`, restoring the previous state afterwards (even on panic).
///
/// Nested calls are supported: the previous pointer is saved and restored, so an inner scope
/// can temporarily shadow an outer one. In practice the adapter only opens one transaction per
/// outermost scope (see [`crate::store`]); inner atomic scopes join the active transaction
/// instead of installing a new one.
pub(crate) fn run_with_active<R>(tx: &mut WriteTransaction<'_>, f: impl FnOnce() -> R) -> R {
    let ptr: *mut () = (tx as *mut WriteTransaction<'_>).cast();
    let prev = ACTIVE_TX.with(|cell| cell.replace(ptr));
    ACTIVE_OVERLAY.with(|stack| stack.borrow_mut().push(TxOverlay::default()));
    let _guard = ScopeGuard { prev };
    f()
}

/// Returns `true` if an ambient write transaction is active on the current thread.
#[inline]
pub(crate) fn in_scope() -> bool {
    ACTIVE_TX.with(|cell| !cell.get().is_null())
}

/// Invokes `f` with the active write transaction if one is installed on this thread, otherwise
/// with `None`.
///
/// See the [module-level safety invariants](self) for why the internal `unsafe` is sound.
pub(crate) fn with_active<R>(f: impl FnOnce(Option<&mut WriteTransaction<'_>>) -> R) -> R {
    let raw = ACTIVE_TX.with(|cell| cell.get());
    if raw.is_null() {
        f(None)
    } else {
        let tx_ptr: *mut WriteTransaction<'_> = raw.cast();
        // SAFETY: see module docs — pointer is live, unaliased, and the reference does not
        // escape this call.
        let tx = unsafe { &mut *tx_ptr };
        f(Some(tx))
    }
}

pub(crate) fn record_insert(partition: &Arc<str>, key: Vec<u8>, value: OverlayValue) {
    ACTIVE_OVERLAY.with(|stack| {
        if let Some(active) = stack.borrow_mut().last_mut() {
            let overlay = active
                .partitions
                .entry(partition.clone())
                .or_default();
            overlay.mark_dirty();
            overlay.entries.insert(key, Some(value));
        }
    });
}

pub(crate) fn record_remove(partition: &Arc<str>, key: Vec<u8>) {
    ACTIVE_OVERLAY.with(|stack| {
        if let Some(active) = stack.borrow_mut().last_mut() {
            let overlay = active
                .partitions
                .entry(partition.clone())
                .or_default();
            overlay.mark_dirty();
            overlay.entries.insert(key, None);
        }
    });
}

pub(crate) fn with_partition_overlay<R>(
    partition: &Arc<str>,
    f: impl FnOnce(Option<&PartitionOverlay>) -> R,
) -> R {
    ACTIVE_OVERLAY.with(|stack| {
        let borrowed = stack.borrow();
        let overlay = borrowed.last().and_then(|active| active.partitions.get(partition));
        f(overlay)
    })
}

pub(crate) fn with_partition_overlay_mut<R>(
    partition: &Arc<str>,
    f: impl FnOnce(Option<&mut PartitionOverlay>) -> R,
) -> R {
    ACTIVE_OVERLAY.with(|stack| {
        let mut borrowed = stack.borrow_mut();
        let overlay = borrowed
            .last_mut()
            .and_then(|active| active.partitions.get_mut(partition));
        f(overlay)
    })
}
