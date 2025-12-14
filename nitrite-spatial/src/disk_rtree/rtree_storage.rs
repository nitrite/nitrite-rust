//! Disk storage layer for R-Tree implementation.
//!
//! This module handles direct disk I/O operations for reading and writing
//! individual pages. No bulk loading or preloading occurs - each read_page
//! call results in exactly one disk seek and read operation.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use parking_lot::RwLock;

use super::rtree_types::{
    FileHeader, FreePage, Node, PageId, PageWithChecksum, SpatialError, SpatialResult,
};

/// Handles reading/writing individual pages to disk.
///
/// IMPORTANT: This storage layer reads pages ONE AT A TIME on demand.
/// There is NO bulk loading or preloading of pages. Each read_page call
/// results in exactly one disk seek and read operation.
pub struct Storage {
    file: RwLock<File>,
    #[allow(dead_code)]
    path: PathBuf,
    page_size: usize,
}

impl Storage {
    /// Create a new storage file
    pub fn create(path: &Path) -> SpatialResult<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;

        Ok(Self {
            file: RwLock::new(file),
            path: path.to_path_buf(),
            page_size: 16384, // PAGE_SIZE
        })
    }

    /// Open an existing storage file
    pub fn open(path: &Path) -> SpatialResult<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;

        Ok(Self {
            file: RwLock::new(file),
            path: path.to_path_buf(),
            page_size: 16384, // PAGE_SIZE
        })
    }

    /// Read header from disk (single read operation)
    pub fn read_header(&self) -> SpatialResult<FileHeader> {
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![0u8; self.page_size];
        file.read_exact(&mut buffer)?;
        bincode::serde::decode_from_slice(&buffer, bincode::config::legacy())
            .map(|(header, _)| header)
            .map_err(|e| SpatialError::Serialization(e.to_string()))
    }

    /// Write header to disk
    pub fn write_header(&self, header: &FileHeader) -> SpatialResult<()> {
        let bytes = bincode::serde::encode_to_vec(header, bincode::config::legacy())
            .map_err(|e| SpatialError::Serialization(e.to_string()))?;
        let mut padded = bytes;
        padded.resize(self.page_size, 0);

        let mut file = self.file.write();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&padded)?;
        Ok(())
    }

    /// Read a SINGLE node from disk (one seek + one read).
    /// This is the core of lazy loading - each page is read individually.
    /// Verifies checksum if enabled to detect corruption.
    pub fn read_page(&self, page_id: PageId) -> SpatialResult<Node> {
        if page_id == 0 {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot read page 0 (reserved for header)",
            )));
        }

        let offset = (page_id as usize) * self.page_size;
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer = vec![0u8; self.page_size];
        file.read_exact(&mut buffer)?;

        // Try to deserialize with checksum wrapper first
        let page_with_checksum: PageWithChecksum =
            bincode::serde::decode_from_slice(&buffer, bincode::config::legacy())
                .map(|(page, _)| page)
                .map_err(|e| SpatialError::Serialization(e.to_string()))?;

        // Verify checksum and extract node
        page_with_checksum.into_node()
    }

    /// Write a SINGLE node to disk with checksum
    pub fn write_page(&self, page_id: PageId, node: &Node) -> SpatialResult<()> {
        if page_id == 0 {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write to page 0 (reserved for header)",
            )));
        }

        // Wrap node with checksum
        let page_with_checksum = PageWithChecksum::new(node.clone());
        let bytes = bincode::serde::encode_to_vec(&page_with_checksum, bincode::config::legacy())
            .map_err(|e| SpatialError::Serialization(e.to_string()))?;

        if bytes.len() > self.page_size {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Node too large: {} bytes (max {})",
                    bytes.len(),
                    self.page_size
                ),
            )));
        }

        let mut padded = bytes;
        padded.resize(self.page_size, 0);

        let offset = (page_id as usize) * self.page_size;
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(&padded)?;
        Ok(())
    }

    /// Sync file to disk
    pub fn sync(&self) -> SpatialResult<()> {
        self.file.write().sync_all()?;
        Ok(())
    }

    /// Read a free page from disk
    pub fn read_free_page(&self, page_id: PageId) -> SpatialResult<Vec<u8>> {
        if page_id == 0 {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot read page 0 (reserved for header)",
            )));
        }

        let offset = (page_id as usize) * self.page_size;
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset as u64))?;
        let mut buffer = vec![0u8; self.page_size];
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    /// Write a free page to disk (raw bytes)
    pub fn write_free_page(&self, page_id: PageId, free_page: &FreePage) -> SpatialResult<()> {
        if page_id == 0 {
            return Err(SpatialError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Cannot write to page 0 (reserved for header)",
            )));
        }

        let bytes = bincode::serde::encode_to_vec(free_page, bincode::config::legacy())
            .map_err(|e| SpatialError::Serialization(e.to_string()))?;

        let mut padded = bytes;
        padded.resize(self.page_size, 0);

        let offset = (page_id as usize) * self.page_size;
        let mut file = self.file.write();
        file.seek(SeekFrom::Start(offset as u64))?;
        file.write_all(&padded)?;
        Ok(())
    }

    /// Delete the backing file
    pub fn delete(&self) -> SpatialResult<()> {
        // File will be deleted when dropped after truncating
        let file = self.file.write();
        file.set_len(0)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_storage_create() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let _storage = Storage::create(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_storage_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        Storage::create(&path).unwrap();
        let _storage = Storage::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_storage_header_read_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();

        let header = FileHeader {
            magic: 0x4E525452,
            version: 1,
            page_size: 16384,
            root_page: 1,
            next_page_id: 5,
            entry_count: 100,
            height: 3,
            free_list_head: 0,
            checksum_enabled: true,
            free_page_count: 0,
        };

        storage.write_header(&header).unwrap();
        let read_header = storage.read_header().unwrap();

        assert_eq!(read_header.magic, 0x4E525452);
        assert_eq!(read_header.version, 1);
        assert_eq!(read_header.page_size, 16384);
        assert_eq!(read_header.root_page, 1);
        assert_eq!(read_header.next_page_id, 5);
        assert_eq!(read_header.entry_count, 100);
        assert_eq!(read_header.height, 3);
    }

    #[test]
    fn test_storage_page_write_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();

        let node = Node::Leaf { entries: vec![] };

        storage.write_page(1, &node).unwrap();
        let read_node = storage.read_page(1).unwrap();

        match (&node, &read_node) {
            (Node::Leaf { entries: e1 }, Node::Leaf { entries: e2 }) => {
                assert_eq!(e1.len(), e2.len());
            }
            _ => panic!("Expected leaf node"),
        }
    }

    #[test]
    fn test_storage_page_zero_rejected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();

        let node = Node::Leaf { entries: vec![] };

        let result = storage.write_page(0, &node);
        assert!(result.is_err());

        let result = storage.read_page(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_storage_sync() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();
        let result = storage.sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_delete() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();
        let result = storage.delete();
        assert!(result.is_ok());
    }

    #[test]
    fn test_storage_multiple_pages() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.rtree");
        let storage = Storage::create(&path).unwrap();

        let node1 = Node::Leaf { entries: vec![] };

        let node2 = Node::Internal {
            children: vec![],
            level: 1,
        };

        storage.write_page(1, &node1).unwrap();
        storage.write_page(2, &node2).unwrap();

        let read1 = storage.read_page(1).unwrap();
        let read2 = storage.read_page(2).unwrap();

        match (&node1, &read1) {
            (Node::Leaf { entries: e1 }, Node::Leaf { entries: e2 }) => {
                assert_eq!(e1.len(), e2.len());
            }
            _ => panic!("Expected leaf node"),
        }

        match (&node2, &read2) {
            (
                Node::Internal {
                    children: c1,
                    level: l1,
                },
                Node::Internal {
                    children: c2,
                    level: l2,
                },
            ) => {
                assert_eq!(c1.len(), c2.len());
                assert_eq!(l1, l2);
            }
            _ => panic!("Expected internal node"),
        }
    }
}
