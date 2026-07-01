//! Memory-mapped flat node store for the DiskANN backend.
//!
//! This replaces a general-purpose KV store on the graph's hot path. Nodes live
//! in a single memory-mapped file as **fixed-size slots**, so a node read is
//! pointer arithmetic + a memcpy from mapped memory — no LSM point lookup, no
//! per-op lock on a shared cache, no deserialization. Resident memory is bounded
//! by the OS page cache (hot pages stay, cold pages are reclaimed under
//! pressure), which is exactly the DiskANN memory model and the reason it serves
//! indexes far larger than RAM.
//!
//! Slot layout (`stride` bytes each):
//! ```text
//! [neighbor_len: u32] [neighbors: max_degree × u32] [vector: precision bytes]
//! ```
//! Neighbors are stored as dense internal slot indices (`u32`); an in-RAM
//! `id ↔ slot` map (tiny per node) translates to external document ids. A
//! sidecar `.meta` file holds that map, the free list, PQ codes, and the opaque
//! header; it is written on [`FlatStore::flush`] (index close). The index is
//! derived data, so a crash before flush costs at most a reindex — the documents
//! themselves are safe in the main store.

use rustc_hash::FxHashMap;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use memmap2::MmapMut;
use parking_lot::{RwLock, RwLockReadGuard};
use serde::{Deserialize, Serialize};

use nitrite::errors::{ErrorKind, NitriteError, NitriteResult};
use nitrite::nitrite_config::NitriteConfig;

use crate::precision::Precision;

use super::vamana::GraphStore;

/// Slots to allocate in a freshly created file.
const INITIAL_SLOTS: usize = 64;
/// Sentinel marking a freed slot in `slot_to_id`.
const FREE_SENTINEL: u64 = u64::MAX;

fn err(msg: impl std::fmt::Display) -> NitriteError {
    NitriteError::new(&format!("DiskANN flat store: {msg}"), ErrorKind::IndexingError)
}

/// Serializable sidecar holding everything not in the mmap data file.
#[derive(Serialize, Deserialize, Default)]
struct MetaFile {
    dim: usize,
    max_degree: usize,
    precision: Precision,
    capacity: usize,
    id_to_slot: Vec<(u64, u32)>,
    slot_to_id: Vec<u64>,
    /// Slots cleaned and reusable by new inserts.
    free: Vec<u32>,
    /// Slots freed by delete but not yet consolidated (NOT reused, so stale
    /// in-edges cannot reattach to a new occupant).
    pending: Vec<u32>,
    pq_codes: Vec<(u64, Vec<u8>)>,
    header: Vec<u8>,
}

struct Inner {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    id_to_slot: FxHashMap<u64, u32>,
    slot_to_id: Vec<u64>,
    free: Vec<u32>,
    pending: Vec<u32>,
    pq_codes: FxHashMap<u64, Box<[u8]>>,
    header: Vec<u8>,
}

/// Memory-mapped, fixed-slot node store.
pub struct FlatStore {
    inner: RwLock<Inner>,
    dim: usize,
    precision: Precision,
    max_degree: usize,
    vec_bytes: usize,
    stride: usize,
    data_path: PathBuf,
    meta_path: PathBuf,
}

impl FlatStore {
    /// Directory where a store's files live for `base` (db dir, else temp).
    fn dir(config: &NitriteConfig) -> PathBuf {
        match config.db_path() {
            Some(p) => PathBuf::from(p),
            None => std::env::temp_dir().join("nitrite_diskann"),
        }
    }

    fn paths(config: &NitriteConfig, base: &str) -> (PathBuf, PathBuf) {
        let dir = Self::dir(config);
        (dir.join(format!("{base}.dann")), dir.join(format!("{base}.dann.meta")))
    }

    /// Reads the persisted opaque header (medoid/params/codebook) if present,
    /// without mapping the data file.
    pub fn peek_header(config: &NitriteConfig, base: &str) -> NitriteResult<Option<Vec<u8>>> {
        let (_, meta_path) = Self::paths(config, base);
        match std::fs::read(&meta_path) {
            Ok(bytes) => {
                let meta: MetaFile = decode(&bytes)?;
                Ok(Some(meta.header).filter(|h| !h.is_empty()))
            }
            Err(_) => Ok(None),
        }
    }

    /// Opens (loading the sidecar + mapping the data file) or creates a store.
    /// `_cache_bytes` is accepted for API parity but unused: the OS page cache
    /// bounds resident memory for a memory-mapped store.
    pub fn open(
        config: &NitriteConfig,
        base: &str,
        dim: usize,
        precision: Precision,
        max_degree: usize,
        _cache_bytes: usize,
    ) -> NitriteResult<Self> {
        let (data_path, meta_path) = Self::paths(config, base);
        if let Some(parent) = data_path.parent() {
            std::fs::create_dir_all(parent).map_err(err)?;
        }

        let vec_bytes = precision.encoded_len(dim);

        // Recover the sidecar if it exists (clean prior close).
        let meta: Option<MetaFile> = match std::fs::read(&meta_path) {
            Ok(bytes) => Some(decode(&bytes)?),
            Err(_) => None,
        };

        // The persisted layout wins on reopen so the data file is interpreted
        // exactly as written.
        let (dim, precision, max_degree, vec_bytes) = match &meta {
            Some(m) => (m.dim, m.precision, m.max_degree, m.precision.encoded_len(m.dim)),
            None => (dim, precision, max_degree.max(1), vec_bytes),
        };
        let stride = 4 + max_degree * 4 + vec_bytes;

        let capacity = meta.as_ref().map(|m| m.capacity.max(INITIAL_SLOTS)).unwrap_or(INITIAL_SLOTS);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)
            .map_err(err)?;
        file.set_len((capacity * stride) as u64).map_err(err)?;
        // SAFETY: we exclusively own this file; it is only mutated through this map.
        let mmap = unsafe { MmapMut::map_mut(&file).map_err(err)? };

        let inner = match meta {
            Some(m) => Inner {
                mmap,
                file,
                capacity,
                id_to_slot: m.id_to_slot.into_iter().collect(),
                slot_to_id: m.slot_to_id,
                free: m.free,
                pending: m.pending,
                pq_codes: m.pq_codes.into_iter().map(|(id, c)| (id, c.into_boxed_slice())).collect(),
                header: m.header,
            },
            None => Inner {
                mmap,
                file,
                capacity,
                id_to_slot: FxHashMap::default(),
                slot_to_id: Vec::new(),
                free: Vec::new(),
                pending: Vec::new(),
                pq_codes: FxHashMap::default(),
                header: Vec::new(),
            },
        };

        Ok(FlatStore {
            inner: RwLock::new(inner),
            dim,
            precision,
            max_degree,
            vec_bytes,
            stride,
            data_path,
            meta_path,
        })
    }

    #[inline]
    fn slot_offset(&self, slot: u32) -> usize {
        slot as usize * self.stride
    }

    /// Number of stored nodes.
    pub fn len(&self) -> usize {
        self.inner.read().id_to_slot.len()
    }

    /// Whether the store has no nodes.
    pub fn is_empty(&self) -> bool {
        self.inner.read().id_to_slot.is_empty()
    }

    /// All node ids.
    pub fn ids(&self) -> Vec<u64> {
        self.inner.read().id_to_slot.keys().copied().collect()
    }

    /// Resident bytes hint. For a memory-mapped store the OS page cache bounds
    /// residency; we report the mapped size for introspection.
    pub fn cache_bytes(&self) -> usize {
        let inner = self.inner.read();
        inner.capacity * self.stride
    }

    /// Reads and decodes the full vector for `id`.
    pub fn vector(&self, id: u64) -> NitriteResult<Option<Vec<f32>>> {
        let inner = self.inner.read();
        let Some(&slot) = inner.id_to_slot.get(&id) else {
            return Ok(None);
        };
        let base = self.slot_offset(slot) + 4 + self.max_degree * 4;
        let bytes = &inner.mmap[base..base + self.vec_bytes];
        Ok(Some(self.precision.decode(bytes, self.dim)))
    }

    /// Runs `f` with the decoded vector for `id` (or `None`), decoding into a
    /// reused thread-local buffer so the query hot loop performs no per-node
    /// heap allocation. `f` must not itself call `with_vector` (single buffer).
    pub fn with_vector<R>(&self, id: u64, f: impl FnOnce(Option<&[f32]>) -> R) -> R {
        thread_local! {
            static BUF: std::cell::RefCell<Vec<f32>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        let inner = self.inner.read();
        let slot = match inner.id_to_slot.get(&id) {
            Some(&s) => s,
            None => return f(None),
        };
        let base = self.slot_offset(slot) + 4 + self.max_degree * 4;
        let bytes = &inner.mmap[base..base + self.vec_bytes];
        BUF.with(|buf| {
            let mut buf = buf.borrow_mut();
            self.precision.decode_into(bytes, self.dim, &mut buf);
            f(Some(&buf))
        })
    }

    /// Writes the full vector for `id`, allocating a slot if new.
    pub fn put_vector(&self, id: u64, vector: &[f32]) -> NitriteResult<()> {
        let encoded = self.precision.encode(vector);
        let mut inner = self.inner.write();
        let slot = self.slot_for_insert(&mut inner, id)?;
        let base = self.slot_offset(slot) + 4 + self.max_degree * 4;
        inner.mmap[base..base + self.vec_bytes].copy_from_slice(&encoded);
        Ok(())
    }

    /// Finds or allocates the slot for `id`, growing the file if needed.
    fn slot_for_insert(&self, inner: &mut Inner, id: u64) -> NitriteResult<u32> {
        if let Some(&slot) = inner.id_to_slot.get(&id) {
            return Ok(slot);
        }
        let slot = match inner.free.pop() {
            Some(s) => {
                inner.slot_to_id[s as usize] = id;
                s
            }
            None => {
                let s = inner.slot_to_id.len() as u32;
                if s as usize >= inner.capacity {
                    self.grow(inner)?;
                }
                inner.slot_to_id.push(id);
                s
            }
        };
        inner.id_to_slot.insert(id, slot);
        // Initialize neighbor length to 0.
        let off = self.slot_offset(slot);
        inner.mmap[off..off + 4].copy_from_slice(&0u32.to_le_bytes());
        Ok(slot)
    }

    /// Doubles the mapped file capacity.
    fn grow(&self, inner: &mut Inner) -> NitriteResult<()> {
        let new_capacity = (inner.capacity * 2).max(INITIAL_SLOTS);
        inner.mmap.flush().map_err(err)?;
        inner.file.set_len((new_capacity * self.stride) as u64).map_err(err)?;
        // SAFETY: exclusive ownership; remapping the freshly grown file.
        inner.mmap = unsafe { MmapMut::map_mut(&inner.file).map_err(err)? };
        inner.capacity = new_capacity;
        Ok(())
    }

    /// PQ code for `id`, if present.
    pub fn pq_code(&self, id: u64) -> Option<Box<[u8]>> {
        self.inner.read().pq_codes.get(&id).cloned()
    }

    /// Stores a PQ code for `id`.
    pub fn set_pq_code(&self, id: u64, code: Vec<u8>) -> NitriteResult<()> {
        self.inner.write().pq_codes.insert(id, code.into_boxed_slice());
        Ok(())
    }

    /// Whether any PQ codes exist (PQ trained).
    pub fn pq_ready(&self) -> bool {
        !self.inner.read().pq_codes.is_empty()
    }

    /// Removes a node. Its slot becomes `pending` (dead but NOT reused until
    /// consolidated, so stale in-edges cannot reattach to a new occupant), and
    /// neighbor references to it map to the sentinel and are skipped at read
    /// time. The slot's own adjacency is left intact so consolidation can use
    /// the deleted node's out-neighbors to repair its in-neighbors.
    pub fn remove_node(&self, id: u64) -> NitriteResult<()> {
        let mut inner = self.inner.write();
        if let Some(slot) = inner.id_to_slot.remove(&id) {
            inner.slot_to_id[slot as usize] = FREE_SENTINEL;
            inner.pending.push(slot);
            inner.pq_codes.remove(&id);
        }
        Ok(())
    }

    /// Number of slots pending consolidation.
    pub fn pending_len(&self) -> usize {
        self.inner.read().pending.len()
    }

    /// Whether `slot` currently holds a live node.
    fn slot_alive(inner: &Inner, slot: u32) -> bool {
        inner
            .slot_to_id
            .get(slot as usize)
            .map(|&id| id != FREE_SENTINEL)
            .unwrap_or(false)
    }

    /// Slots of all live nodes (for a consolidation sweep).
    pub fn live_slots(&self) -> Vec<u32> {
        self.inner.read().id_to_slot.values().copied().collect()
    }

    /// Raw neighbor slots stored at `slot` (no liveness filtering).
    pub fn raw_neighbor_slots(&self, slot: u32) -> Vec<u32> {
        let inner = self.inner.read();
        self.read_neighbor_slots(&inner, slot)
    }

    fn read_neighbor_slots(&self, inner: &Inner, slot: u32) -> Vec<u32> {
        let off = self.slot_offset(slot);
        let len = (u32::from_le_bytes(inner.mmap[off..off + 4].try_into().unwrap()) as usize)
            .min(self.max_degree);
        (0..len)
            .map(|i| {
                let p = off + 4 + i * 4;
                u32::from_le_bytes(inner.mmap[p..p + 4].try_into().unwrap())
            })
            .collect()
    }

    /// Overwrites the neighbor slots stored at `slot`.
    pub fn set_neighbor_slots(&self, slot: u32, slots: &[u32]) {
        let mut inner = self.inner.write();
        let n = slots.len().min(self.max_degree);
        let off = self.slot_offset(slot);
        inner.mmap[off..off + 4].copy_from_slice(&(n as u32).to_le_bytes());
        for (i, s) in slots.iter().take(n).enumerate() {
            let p = off + 4 + i * 4;
            inner.mmap[p..p + 4].copy_from_slice(&s.to_le_bytes());
        }
    }

    /// Whether `slot` currently holds a live node.
    pub fn is_slot_alive(&self, slot: u32) -> bool {
        Self::slot_alive(&self.inner.read(), slot)
    }

    /// Decodes the vector stored at `slot` (regardless of liveness).
    pub fn vector_at_slot(&self, slot: u32) -> Vec<f32> {
        let inner = self.inner.read();
        let base = self.slot_offset(slot) + 4 + self.max_degree * 4;
        self.precision.decode(&inner.mmap[base..base + self.vec_bytes], self.dim)
    }

    /// Moves all `pending` slots to the reusable `free` list, clearing their
    /// adjacency. Callers must first strip references to these slots from live
    /// nodes ([`crate::diskann`] consolidation does this).
    pub fn reclaim_pending(&self) {
        let mut inner = self.inner.write();
        let pending = std::mem::take(&mut inner.pending);
        for slot in pending {
            let off = self.slot_offset(slot);
            inner.mmap[off..off + 4].copy_from_slice(&0u32.to_le_bytes());
            inner.free.push(slot);
        }
    }

    /// The persisted opaque header (in RAM; flushed on close).
    pub fn load_header(&self) -> NitriteResult<Option<Vec<u8>>> {
        let h = self.inner.read().header.clone();
        Ok(Some(h).filter(|h| !h.is_empty()))
    }

    /// Sets the opaque header (persisted on flush).
    pub fn store_header(&self, bytes: Vec<u8>) -> NitriteResult<()> {
        self.inner.write().header = bytes;
        Ok(())
    }

    /// Flushes the mmap and writes the sidecar. Call on index close.
    pub fn flush(&self) -> NitriteResult<()> {
        let inner = self.inner.read();
        inner.mmap.flush().map_err(err)?;
        let meta = MetaFile {
            dim: self.dim,
            max_degree: self.max_degree,
            precision: self.precision,
            capacity: inner.capacity,
            id_to_slot: inner.id_to_slot.iter().map(|(&id, &s)| (id, s)).collect(),
            slot_to_id: inner.slot_to_id.clone(),
            free: inner.free.clone(),
            pending: inner.pending.clone(),
            pq_codes: inner.pq_codes.iter().map(|(&id, c)| (id, c.to_vec())).collect(),
            header: inner.header.clone(),
        };
        std::fs::write(&self.meta_path, encode(&meta)?).map_err(err)?;
        Ok(())
    }

    /// Deletes the store's files (for `drop_index`).
    pub fn destroy(&self) -> NitriteResult<()> {
        let _ = std::fs::remove_file(&self.data_path);
        let _ = std::fs::remove_file(&self.meta_path);
        Ok(())
    }
}

impl GraphStore for FlatStore {
    fn neighbors(&self, id: u64) -> Vec<u64> {
        let inner = self.inner.read();
        let Some(&slot) = inner.id_to_slot.get(&id) else {
            return Vec::new();
        };
        let off = self.slot_offset(slot);
        let len = u32::from_le_bytes(inner.mmap[off..off + 4].try_into().unwrap()) as usize;
        let len = len.min(self.max_degree);
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let p = off + 4 + i * 4;
            let nslot = u32::from_le_bytes(inner.mmap[p..p + 4].try_into().unwrap());
            let nid = inner.slot_to_id.get(nslot as usize).copied().unwrap_or(FREE_SENTINEL);
            if nid != FREE_SENTINEL {
                out.push(nid);
            }
        }
        out
    }

    fn set_neighbors(&self, id: u64, neighbors: Vec<u64>) {
        let mut inner = self.inner.write();
        let Some(&slot) = inner.id_to_slot.get(&id) else {
            return;
        };
        // Translate external ids to slots (skip any unknown).
        let mut slots: Vec<u32> = Vec::with_capacity(neighbors.len().min(self.max_degree));
        for nid in neighbors {
            if slots.len() >= self.max_degree {
                break;
            }
            if let Some(&ns) = inner.id_to_slot.get(&nid) {
                slots.push(ns);
            }
        }
        let off = self.slot_offset(slot);
        inner.mmap[off..off + 4].copy_from_slice(&(slots.len() as u32).to_le_bytes());
        for (i, ns) in slots.iter().enumerate() {
            let p = off + 4 + i * 4;
            inner.mmap[p..p + 4].copy_from_slice(&ns.to_le_bytes());
        }
    }

    fn contains(&self, id: u64) -> bool {
        self.inner.read().id_to_slot.contains_key(&id)
    }
}

impl FlatStore {
    /// Acquires a read view that holds the store lock for the lifetime of a
    /// whole query. All node access then goes through the held guard with **no
    /// further locking**, so concurrent queries don't ping-pong the lock's
    /// cache line on every node (the difference between negative and near-linear
    /// multi-thread query scaling). A view blocks concurrent writers for its
    /// (short) lifetime, which is the right trade for read-heavy query load.
    pub fn read_view(&self) -> FlatRead<'_> {
        FlatRead { guard: self.inner.read(), store: self }
    }
}

/// A lock-free read view over the store (see [`FlatStore::read_view`]).
pub struct FlatRead<'a> {
    guard: RwLockReadGuard<'a, Inner>,
    store: &'a FlatStore,
}

impl FlatRead<'_> {
    /// PQ code for `id` (borrowed from the held map — no clone).
    #[inline]
    pub fn pq_code(&self, id: u64) -> Option<&[u8]> {
        self.guard.pq_codes.get(&id).map(|c| &c[..])
    }

    /// Decodes the vector for `id` into `buf`, returning whether it exists.
    #[inline]
    pub fn vector_into(&self, id: u64, buf: &mut Vec<f32>) -> bool {
        let Some(&slot) = self.guard.id_to_slot.get(&id) else {
            return false;
        };
        let base = self.store.slot_offset(slot) + 4 + self.store.max_degree * 4;
        let bytes = &self.guard.mmap[base..base + self.store.vec_bytes];
        self.store.precision.decode_into(bytes, self.store.dim, buf);
        true
    }
}

impl GraphStore for FlatRead<'_> {
    fn neighbors(&self, id: u64) -> Vec<u64> {
        let Some(&slot) = self.guard.id_to_slot.get(&id) else {
            return Vec::new();
        };
        let off = self.store.slot_offset(slot);
        let len = (u32::from_le_bytes(self.guard.mmap[off..off + 4].try_into().unwrap()) as usize)
            .min(self.store.max_degree);
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let p = off + 4 + i * 4;
            let nslot = u32::from_le_bytes(self.guard.mmap[p..p + 4].try_into().unwrap());
            let nid = self.guard.slot_to_id.get(nslot as usize).copied().unwrap_or(FREE_SENTINEL);
            if nid != FREE_SENTINEL {
                out.push(nid);
            }
        }
        out
    }

    /// The query path never writes.
    fn set_neighbors(&self, _id: u64, _neighbors: Vec<u64>) {}

    fn contains(&self, id: u64) -> bool {
        self.guard.id_to_slot.contains_key(&id)
    }
}

fn encode<T: Serialize>(value: &T) -> NitriteResult<Vec<u8>> {
    bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|e| err(format!("encode: {e}")))
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> NitriteResult<T> {
    bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map(|(v, _)| v)
        .map_err(|e| err(format!("decode: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nitrite::nitrite::Nitrite;
    use nitrite_fjall_adapter::FjallModule;

    fn db(dir: &std::path::Path) -> Nitrite {
        Nitrite::builder()
            .load_module(FjallModule::with_config().db_path(dir.to_str().unwrap()).low_memory_preset().build())
            .open_or_create(None, None)
            .unwrap()
    }

    #[test]
    fn vectors_and_neighbors_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let store = FlatStore::open(&d.config(), "t", 4, Precision::F32, 8, 0).unwrap();
        store.put_vector(10, &[1.0, 2.0, 3.0, 4.0]).unwrap();
        store.put_vector(20, &[5.0, 6.0, 7.0, 8.0]).unwrap();
        store.set_neighbors(10, vec![20]);
        assert_eq!(store.vector(10).unwrap().unwrap(), vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(store.neighbors(10), vec![20]);
        assert!(store.contains(20));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn grows_past_initial_capacity() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let store = FlatStore::open(&d.config(), "g", 2, Precision::F32, 8, 0).unwrap();
        for i in 0..500u64 {
            store.put_vector(i + 1, &[i as f32, 0.0]).unwrap();
        }
        assert_eq!(store.len(), 500);
        assert_eq!(store.vector(250).unwrap().unwrap(), vec![249.0, 0.0]);
    }

    #[test]
    fn removed_node_is_skipped_in_neighbors() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let store = FlatStore::open(&d.config(), "r", 2, Precision::F32, 8, 0).unwrap();
        for i in 1..=3u64 {
            store.put_vector(i, &[i as f32, 0.0]).unwrap();
        }
        store.set_neighbors(1, vec![2, 3]);
        store.remove_node(2).unwrap();
        assert_eq!(store.neighbors(1), vec![3]);
        assert!(!store.contains(2));
    }

    #[test]
    fn flush_and_reopen_restores_state() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        {
            let store = FlatStore::open(&d.config(), "p", 3, Precision::F16, 8, 0).unwrap();
            store.put_vector(7, &[1.0, 2.0, 3.0]).unwrap();
            store.put_vector(8, &[4.0, 5.0, 6.0]).unwrap();
            store.set_neighbors(7, vec![8]);
            store.set_pq_code(7, vec![1, 2, 3]).unwrap();
            store.store_header(vec![9, 9, 9]).unwrap();
            store.flush().unwrap();
        }
        let store = FlatStore::open(&d.config(), "p", 3, Precision::F16, 8, 0).unwrap();
        assert_eq!(store.len(), 2);
        assert_eq!(store.neighbors(7), vec![8]);
        assert_eq!(store.pq_code(7).unwrap().to_vec(), vec![1, 2, 3]);
        assert_eq!(store.load_header().unwrap().unwrap(), vec![9, 9, 9]);
        // F16 round-trip is approximate but exact for these small integers.
        assert_eq!(store.vector(8).unwrap().unwrap(), vec![4.0, 5.0, 6.0]);
    }
}
