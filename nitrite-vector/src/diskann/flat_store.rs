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
//! File layout: a 64-byte header page, then `stride`-byte slots:
//! ```text
//! [magic: 8B "NDANN002"] [dirty: u32] [generation: u64]
//! [dim: u32] [max_degree: u32] [vec_bytes: u32] [reserved...]
//! slot: [neighbor_len: u32] [neighbors: max_degree × u32] [vector: precision bytes]
//! ```
//! Neighbors are stored as dense internal slot indices (`u32`); an in-RAM
//! `id ↔ slot` map (tiny per node) translates to external document ids. A
//! sidecar `.meta` file holds that map, the free list, PQ codes, and the opaque
//! header; it is checksummed and replaced atomically (tmp + rename) on
//! [`FlatStore::flush`].
//!
//! # Crash consistency
//!
//! The index is derived data, so the recovery strategy is *detect and rebuild*,
//! never *trust and misread*:
//! - the first mutation after open/flush sets the header's **dirty bit** and
//!   syncs it before any data write can become durable;
//! - [`FlatStore::flush`] syncs the data, atomically writes the sidecar with a
//!   new **generation**, then clears the dirty bit and stamps the same
//!   generation into the data header;
//! - [`FlatStore::open`] declares the store **stale** if the sidecar is
//!   missing/corrupt (checksum), the magic is wrong, the dirty bit is set, the
//!   generations disagree, or the geometry doesn't match — and then wipes the
//!   files and reports `needs_rebuild` so the caller re-indexes from the
//!   collection. A crash therefore costs a rebuild, silently wrong results
//!   never.

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
/// Data-file magic: identifies the format and its version.
const MAGIC: [u8; 8] = *b"NDANN002";
/// Size of the data-file header page preceding slot 0.
const HEADER_BYTES: usize = 64;
/// Upper bounds on geometry read from disk; anything larger is corruption.
const MAX_DIM: usize = 65_536;
const MAX_DEGREE_LIMIT: usize = 4_096;

fn err(msg: impl std::fmt::Display) -> NitriteError {
    NitriteError::new(&format!("DiskANN flat store: {msg}"), ErrorKind::IndexingError)
}

/// FNV-1a 64-bit hash; used for the sidecar checksum and file-name digests.
fn fnv64(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf29ce484222325u64;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

/// Maps an index base name to a filesystem-safe file stem. Collection and
/// field names are user input and may contain path separators or other hostile
/// characters; every character outside `[A-Za-z0-9_-]` is replaced and a
/// digest of the original name is appended so distinct names cannot collide.
fn sanitize_base(base: &str) -> String {
    let mut cleaned: String = base
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') { c } else { '_' })
        .collect();
    cleaned.truncate(96);
    format!("{cleaned}-{:016x}", fnv64(base.as_bytes()))
}

/// Serializable sidecar holding everything not in the mmap data file.
#[derive(Serialize, Deserialize, Default)]
struct MetaFile {
    generation: u64,
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

impl MetaFile {
    /// Structural validation of values read from disk. Anything out of bounds
    /// means the sidecar is corrupt and the store must be rebuilt.
    fn validate(&self) -> bool {
        if self.dim == 0 || self.dim > MAX_DIM {
            return false;
        }
        if self.max_degree == 0 || self.max_degree > MAX_DEGREE_LIMIT {
            return false;
        }
        if self.slot_to_id.len() > self.capacity {
            return false;
        }
        let slots = self.slot_to_id.len();
        if !self.id_to_slot.iter().all(|&(id, slot)| {
            (slot as usize) < slots && self.slot_to_id[slot as usize] == id
        }) {
            return false;
        }
        if !self.free.iter().chain(self.pending.iter()).all(|&s| (s as usize) < slots) {
            return false;
        }
        true
    }
}

struct Inner {
    mmap: MmapMut,
    file: File,
    capacity: usize,
    generation: u64,
    /// Mirrors the on-disk dirty flag so it is written (and synced) only on
    /// the clean→dirty transition, not on every mutation.
    dirty: bool,
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
    /// Directory where a store's files live. DiskANN is disk-resident by
    /// definition, so a persistent database path is required; refusing an
    /// in-memory database also avoids writing embeddings into a predictable,
    /// world-shared temp directory.
    fn dir(config: &NitriteConfig) -> NitriteResult<PathBuf> {
        match config.db_path() {
            Some(p) if !p.is_empty() => Ok(PathBuf::from(p)),
            _ => Err(err(
                "the DiskANN backend requires a persistent database (db_path); \
                 use the HNSW backend for in-memory databases",
            )),
        }
    }

    fn paths(config: &NitriteConfig, base: &str) -> NitriteResult<(PathBuf, PathBuf)> {
        let dir = Self::dir(config)?;
        let stem = sanitize_base(base);
        Ok((dir.join(format!("{stem}.dann")), dir.join(format!("{stem}.dann.meta"))))
    }

    /// Reads and checksum-verifies the sidecar, if present and intact.
    fn read_meta(meta_path: &PathBuf) -> Option<MetaFile> {
        let bytes = std::fs::read(meta_path).ok()?;
        if bytes.len() < 8 {
            return None;
        }
        let stored = u64::from_le_bytes(bytes[..8].try_into().ok()?);
        if fnv64(&bytes[8..]) != stored {
            return None;
        }
        let meta: MetaFile = decode(&bytes[8..]).ok()?;
        meta.validate().then_some(meta)
    }

    /// Reads the persisted opaque header (medoid/params/codebook) if the
    /// sidecar is present and intact, without mapping the data file.
    pub fn peek_header(config: &NitriteConfig, base: &str) -> NitriteResult<Option<Vec<u8>>> {
        let (_, meta_path) = Self::paths(config, base)?;
        Ok(Self::read_meta(&meta_path)
            .map(|m| m.header)
            .filter(|h| !h.is_empty()))
    }

    /// Opens (loading the sidecar + mapping the data file) or creates a store.
    ///
    /// Returns `(store, needs_rebuild)`. `needs_rebuild` is `true` when
    /// existing index data was stale or corrupt: the files have been wiped and
    /// the caller must re-index from the collection.
    ///
    /// `_cache_bytes` is accepted for API parity but unused: the OS page cache
    /// bounds resident memory for a memory-mapped store.
    pub fn open(
        config: &NitriteConfig,
        base: &str,
        dim: usize,
        precision: Precision,
        max_degree: usize,
        _cache_bytes: usize,
    ) -> NitriteResult<(Self, bool)> {
        let (data_path, meta_path) = Self::paths(config, base)?;
        if let Some(parent) = data_path.parent() {
            std::fs::create_dir_all(parent).map_err(err)?;
        }

        let meta = Self::read_meta(&meta_path);
        let meta_file_exists = meta_path.exists();
        let data_len = std::fs::metadata(&data_path).map(|m| m.len()).unwrap_or(0);
        let data_exists = data_len as usize >= HEADER_BYTES;

        // Decide whether the persisted state is trustworthy.
        let mut stale = false;
        let mut had_content = false;
        if data_exists {
            match &meta {
                None => {
                    // Data without an intact sidecar: content unknown, assume some.
                    stale = true;
                    had_content = true;
                }
                Some(m) => {
                    had_content = !m.id_to_slot.is_empty();
                    match Self::check_data_header(&data_path, m) {
                        Ok(true) => {}
                        _ => stale = true,
                    }
                }
            }
        } else if meta_file_exists || data_len > 0 {
            // Sidecar without data (or a truncated data file): state is lost.
            stale = true;
            had_content = meta.as_ref().map(|m| !m.id_to_slot.is_empty()).unwrap_or(true);
        }

        if stale {
            log::warn!(
                "DiskANN store '{}' is stale or corrupt; wiping for rebuild",
                data_path.display()
            );
            let _ = std::fs::remove_file(&data_path);
            let _ = std::fs::remove_file(&meta_path);
        }
        let meta = if stale { None } else { meta };

        // The persisted layout wins on reopen so the data file is interpreted
        // exactly as written.
        let (dim, precision, max_degree) = match &meta {
            Some(m) => (m.dim, m.precision, m.max_degree),
            None => (dim, precision, max_degree.clamp(1, MAX_DEGREE_LIMIT)),
        };
        if dim == 0 || dim > MAX_DIM {
            return Err(err(format!("invalid dimension {dim}")));
        }
        let vec_bytes = precision.encoded_len(dim);
        let stride = 4usize
            .checked_add(max_degree.checked_mul(4).ok_or_else(|| err("degree overflow"))?)
            .and_then(|s| s.checked_add(vec_bytes))
            .ok_or_else(|| err("slot stride overflow"))?;

        let capacity = meta.as_ref().map(|m| m.capacity.max(INITIAL_SLOTS)).unwrap_or(INITIAL_SLOTS);
        let file_len = capacity
            .checked_mul(stride)
            .and_then(|s| s.checked_add(HEADER_BYTES))
            .ok_or_else(|| err("capacity overflow"))?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)
            .map_err(err)?;
        file.set_len(file_len as u64).map_err(err)?;
        // SAFETY: we exclusively own this file; it is only mutated through this map.
        let mut mmap = unsafe { MmapMut::map_mut(&file).map_err(err)? };

        let generation = meta.as_ref().map(|m| m.generation).unwrap_or(0);
        if meta.is_none() {
            // Fresh file: stamp the header (clean, generation 0).
            write_data_header(&mut mmap, generation, false, dim, max_degree, vec_bytes);
            mmap.flush_range(0, HEADER_BYTES).map_err(err)?;
        }

        let inner = match meta {
            Some(m) => Inner {
                mmap,
                file,
                capacity,
                generation,
                dirty: false,
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
                generation,
                dirty: false,
                id_to_slot: FxHashMap::default(),
                slot_to_id: Vec::new(),
                free: Vec::new(),
                pending: Vec::new(),
                pq_codes: FxHashMap::default(),
                header: Vec::new(),
            },
        };

        let store = FlatStore {
            inner: RwLock::new(inner),
            dim,
            precision,
            max_degree,
            vec_bytes,
            stride,
            data_path,
            meta_path,
        };
        Ok((store, stale && had_content))
    }

    /// Verifies the data-file header against the sidecar: magic, geometry,
    /// clean flag, matching generation.
    fn check_data_header(data_path: &PathBuf, meta: &MetaFile) -> std::io::Result<bool> {
        use std::io::Read;
        let mut file = File::open(data_path)?;
        let mut header = [0u8; HEADER_BYTES];
        file.read_exact(&mut header)?;
        let magic_ok = header[..8] == MAGIC;
        let dirty = u32::from_le_bytes(header[8..12].try_into().unwrap()) != 0;
        let generation = u64::from_le_bytes(header[12..20].try_into().unwrap());
        let dim = u32::from_le_bytes(header[20..24].try_into().unwrap()) as usize;
        let degree = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let vec_bytes = u32::from_le_bytes(header[28..32].try_into().unwrap()) as usize;
        Ok(magic_ok
            && !dirty
            && generation == meta.generation
            && dim == meta.dim
            && degree == meta.max_degree
            && vec_bytes == meta.precision.encoded_len(meta.dim))
    }

    /// Marks the store dirty (once per open/flush cycle), syncing the flag to
    /// disk *before* the caller's data mutation can become durable.
    fn mark_dirty(&self, inner: &mut Inner) {
        if inner.dirty {
            return;
        }
        inner.dirty = true;
        inner.mmap[8..12].copy_from_slice(&1u32.to_le_bytes());
        if let Err(e) = inner.mmap.flush_range(0, HEADER_BYTES) {
            log::warn!("DiskANN store: failed to sync dirty flag: {e}");
        }
    }

    #[inline]
    fn slot_offset(&self, slot: u32) -> usize {
        HEADER_BYTES + slot as usize * self.stride
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
        self.mark_dirty(&mut inner);
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
        let file_len = new_capacity
            .checked_mul(self.stride)
            .and_then(|s| s.checked_add(HEADER_BYTES))
            .ok_or_else(|| err("capacity overflow"))?;
        inner.mmap.flush().map_err(err)?;
        inner.file.set_len(file_len as u64).map_err(err)?;
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
        let mut inner = self.inner.write();
        self.mark_dirty(&mut inner);
        inner.pq_codes.insert(id, code.into_boxed_slice());
        Ok(())
    }

    /// Removes a node. Its slot becomes `pending` (dead but NOT reused until
    /// consolidated, so stale in-edges cannot reattach to a new occupant), and
    /// neighbor references to it map to the sentinel and are skipped at read
    /// time. The slot's own adjacency is left intact so consolidation can use
    /// the deleted node's out-neighbors to repair its in-neighbors.
    pub fn remove_node(&self, id: u64) -> NitriteResult<()> {
        let mut inner = self.inner.write();
        if let Some(slot) = inner.id_to_slot.remove(&id) {
            self.mark_dirty(&mut inner);
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
        self.mark_dirty(&mut inner);
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

    /// Snapshot of the slots currently pending consolidation.
    pub fn pending_slots(&self) -> Vec<u32> {
        self.inner.read().pending.clone()
    }

    /// Moves the given `pending` slots to the reusable `free` list, clearing
    /// their adjacency. Callers must first strip references to these slots
    /// from live nodes ([`crate::diskann`] consolidation does this). Taking an
    /// explicit snapshot (instead of draining everything pending) means slots
    /// deleted *during* a consolidation sweep — whose in-edges have not been
    /// repaired yet — stay quarantined until the next pass.
    pub fn reclaim(&self, slots: &[u32]) {
        if slots.is_empty() {
            return;
        }
        let mut inner = self.inner.write();
        self.mark_dirty(&mut inner);
        let reclaim: rustc_hash::FxHashSet<u32> = slots.iter().copied().collect();
        inner.pending.retain(|s| !reclaim.contains(s));
        for &slot in slots {
            let off = self.slot_offset(slot);
            inner.mmap[off..off + 4].copy_from_slice(&0u32.to_le_bytes());
            inner.free.push(slot);
        }
    }

    /// The persisted opaque header (in RAM; flushed with the sidecar).
    pub fn load_header(&self) -> NitriteResult<Option<Vec<u8>>> {
        let h = self.inner.read().header.clone();
        Ok(Some(h).filter(|h| !h.is_empty()))
    }

    /// Sets the opaque header (persisted on flush).
    pub fn store_header(&self, bytes: Vec<u8>) -> NitriteResult<()> {
        let mut inner = self.inner.write();
        self.mark_dirty(&mut inner);
        inner.header = bytes;
        Ok(())
    }

    /// Checkpoints the store: syncs the mapped data, atomically replaces the
    /// sidecar (checksummed, tmp + rename) under a new generation, then clears
    /// the dirty bit. Safe to call at any time; a crash at any point leaves
    /// either the old or the new state detectable, never a misread.
    pub fn flush(&self) -> NitriteResult<()> {
        let mut inner = self.inner.write();

        // 1. Make all slot data durable (dirty bit still set on disk).
        inner.mmap.flush().map_err(err)?;

        // 2. Atomically publish a sidecar describing exactly that data.
        let generation = inner.generation + 1;
        let meta = MetaFile {
            generation,
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
        let payload = encode(&meta)?;
        let mut bytes = Vec::with_capacity(payload.len() + 8);
        bytes.extend_from_slice(&fnv64(&payload).to_le_bytes());
        bytes.extend_from_slice(&payload);

        let tmp_path = self.meta_path.with_extension("meta.tmp");
        {
            use std::io::Write;
            let mut tmp = File::create(&tmp_path).map_err(err)?;
            tmp.write_all(&bytes).map_err(err)?;
            tmp.sync_all().map_err(err)?;
        }
        std::fs::rename(&tmp_path, &self.meta_path).map_err(err)?;

        // 3. Stamp the data header clean with the matching generation.
        write_data_header(
            &mut inner.mmap,
            generation,
            false,
            self.dim,
            self.max_degree,
            self.vec_bytes,
        );
        inner.mmap.flush_range(0, HEADER_BYTES).map_err(err)?;
        inner.generation = generation;
        inner.dirty = false;
        Ok(())
    }

    /// Deletes the store's files (for `drop_index`).
    pub fn destroy(&self) -> NitriteResult<()> {
        let _ = std::fs::remove_file(&self.data_path);
        let _ = std::fs::remove_file(&self.meta_path);
        let _ = std::fs::remove_file(self.meta_path.with_extension("meta.tmp"));
        Ok(())
    }
}

fn write_data_header(
    mmap: &mut MmapMut,
    generation: u64,
    dirty: bool,
    dim: usize,
    max_degree: usize,
    vec_bytes: usize,
) {
    mmap[0..8].copy_from_slice(&MAGIC);
    mmap[8..12].copy_from_slice(&(dirty as u32).to_le_bytes());
    mmap[12..20].copy_from_slice(&generation.to_le_bytes());
    mmap[20..24].copy_from_slice(&(dim as u32).to_le_bytes());
    mmap[24..28].copy_from_slice(&(max_degree as u32).to_le_bytes());
    mmap[28..32].copy_from_slice(&(vec_bytes as u32).to_le_bytes());
    mmap[32..HEADER_BYTES].fill(0);
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
        self.mark_dirty(&mut inner);
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

    fn open(d: &Nitrite, base: &str, dim: usize, precision: Precision) -> (FlatStore, bool) {
        FlatStore::open(&d.config(), base, dim, precision, 8, 0).unwrap()
    }

    #[test]
    fn vectors_and_neighbors_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let (store, rebuild) = open(&d, "t", 4, Precision::F32);
        assert!(!rebuild);
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
        let (store, _) = open(&d, "g", 2, Precision::F32);
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
        let (store, _) = open(&d, "r", 2, Precision::F32);
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
            let (store, _) = open(&d, "p", 3, Precision::F16);
            store.put_vector(7, &[1.0, 2.0, 3.0]).unwrap();
            store.put_vector(8, &[4.0, 5.0, 6.0]).unwrap();
            store.set_neighbors(7, vec![8]);
            store.set_pq_code(7, vec![1, 2, 3]).unwrap();
            store.store_header(vec![9, 9, 9]).unwrap();
            store.flush().unwrap();
        }
        let (store, rebuild) = open(&d, "p", 3, Precision::F16);
        assert!(!rebuild, "cleanly flushed store must reopen clean");
        assert_eq!(store.len(), 2);
        assert_eq!(store.neighbors(7), vec![8]);
        assert_eq!(store.pq_code(7).unwrap().to_vec(), vec![1, 2, 3]);
        assert_eq!(store.load_header().unwrap().unwrap(), vec![9, 9, 9]);
        // F16 round-trip is approximate but exact for these small integers.
        assert_eq!(store.vector(8).unwrap().unwrap(), vec![4.0, 5.0, 6.0]);
    }

    #[test]
    fn unflushed_mutations_are_detected_as_stale_on_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        {
            let (store, _) = open(&d, "crash", 2, Precision::F32);
            store.put_vector(1, &[1.0, 2.0]).unwrap();
            store.flush().unwrap();
            // Mutate after the flush and "crash" (drop without flushing).
            store.put_vector(2, &[3.0, 4.0]).unwrap();
        }
        let (store, rebuild) = open(&d, "crash", 2, Precision::F32);
        assert!(rebuild, "dirty store must demand a rebuild");
        assert!(store.is_empty(), "stale store must be wiped, not misread");
    }

    #[test]
    fn corrupt_sidecar_is_detected() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let meta_path = {
            let (store, _) = open(&d, "c", 2, Precision::F32);
            store.put_vector(1, &[1.0, 2.0]).unwrap();
            store.flush().unwrap();
            store.meta_path.clone()
        };
        // Flip a byte in the sidecar payload.
        let mut bytes = std::fs::read(&meta_path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&meta_path, bytes).unwrap();

        let (store, rebuild) = open(&d, "c", 2, Precision::F32);
        assert!(rebuild);
        assert!(store.is_empty());
    }

    #[test]
    fn hostile_base_names_stay_inside_the_db_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let d = db(tmp.path());
        let (store, _) = FlatStore::open(&d.config(), "../../evil/name", 2, Precision::F32, 8, 0).unwrap();
        assert!(store.data_path.starts_with(tmp.path()), "{:?}", store.data_path);
        assert!(!store.data_path.to_string_lossy().contains(".."));
        store.put_vector(1, &[1.0, 2.0]).unwrap();
        store.flush().unwrap();
        // Distinct hostile names must not collide after sanitization.
        let (other, _) = FlatStore::open(&d.config(), "__/__evil/name", 2, Precision::F32, 8, 0).unwrap();
        assert_ne!(store.data_path, other.data_path);
    }

    #[test]
    fn in_memory_database_is_rejected() {
        let d = Nitrite::builder().open_or_create(None, None).unwrap();
        let result = FlatStore::open(&d.config(), "mem", 2, Precision::F32, 8, 0);
        assert!(result.is_err());
    }
}
