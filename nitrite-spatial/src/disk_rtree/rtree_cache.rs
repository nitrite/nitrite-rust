//! LRU Cache implementation for R-Tree pages.
//!
//! This module provides an LRU (Least Recently Used) cache for managing
//! R-Tree node pages in memory. The cache uses true lazy loading - pages
//! are only loaded from disk when first accessed.

use std::collections::HashMap;
use std::collections::VecDeque;
use super::rtree_types::{PageId, Node};

/// A cached page with its data and dirty flag
pub struct CachedPage {
    pub node: Node,
    pub dirty: bool,
}

/// LRU cache for R-Tree pages
/// 
/// IMPORTANT: This cache does NOT preload any pages. Pages are loaded
/// from disk ONLY when first accessed via get(). This ensures true
/// lazy loading behavior.
pub struct PageCache {
    /// Page data storage - only contains pages that have been accessed
    pub pages: HashMap<PageId, CachedPage>,
    /// LRU order (front = oldest, back = newest)  
    lru_order: VecDeque<PageId>,
    /// Maximum number of pages to cache
    max_pages: usize,
}

#[allow(dead_code)]
impl PageCache {
    /// Create a new page cache with specified maximum size
    pub fn new(max_pages: usize) -> Self {
        Self {
            pages: HashMap::new(),
            lru_order: VecDeque::new(),
            max_pages,
        }
    }

    /// Get a page from cache, updating LRU order.
    /// Returns None if page is not in cache (must be loaded from disk).
    pub fn get(&mut self, page_id: PageId) -> Option<&Node> {
        if self.pages.contains_key(&page_id) {
            // Update LRU order - move to end (most recently used)
            self.lru_order.retain(|&id| id != page_id);
            self.lru_order.push_back(page_id);
            Some(&self.pages.get(&page_id).unwrap().node)
        } else {
            None
        }
    }

    /// Get a mutable reference to a page, marking it dirty
    pub fn get_mut(&mut self, page_id: PageId) -> Option<&mut Node> {
        if self.pages.contains_key(&page_id) {
            // Update LRU order
            self.lru_order.retain(|&id| id != page_id);
            self.lru_order.push_back(page_id);
            let cached = self.pages.get_mut(&page_id)?;
            cached.dirty = true;
            Some(&mut cached.node)
        } else {
            None
        }
    }

    /// Insert a page into cache (after loading from disk or creating new)
    pub fn insert(&mut self, page_id: PageId, node: Node, dirty: bool) {
        if self.pages.contains_key(&page_id) {
            self.lru_order.retain(|&id| id != page_id);
        }
        self.lru_order.push_back(page_id);
        self.pages.insert(page_id, CachedPage { node, dirty });
    }

    /// Check if we need to evict pages
    pub fn needs_eviction(&self) -> bool {
        self.pages.len() >= self.max_pages
    }

    /// Get the oldest page to evict (returns page_id, node, dirty flag)
    pub fn evict_oldest(&mut self) -> Option<(PageId, Node, bool)> {
        while let Some(page_id) = self.lru_order.pop_front() {
            if let Some(cached) = self.pages.remove(&page_id) {
                return Some((page_id, cached.node, cached.dirty));
            }
        }
        None
    }

    /// Get all dirty pages for flushing
    pub fn get_dirty_pages(&self) -> Vec<PageId> {
        self.pages
            .iter()
            .filter(|(_, cached)| cached.dirty)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Mark a page as clean
    pub fn mark_clean(&mut self, page_id: PageId) {
        if let Some(cached) = self.pages.get_mut(&page_id) {
            cached.dirty = false;
        }
    }

    /// Remove a page from cache
    pub fn remove(&mut self, page_id: PageId) -> Option<(Node, bool)> {
        self.lru_order.retain(|&id| id != page_id);
        self.pages.remove(&page_id).map(|c| (c.node, c.dirty))
    }

    /// Clear the cache, returning all dirty pages
    pub fn clear(&mut self) -> Vec<(PageId, Node, bool)> {
        let result: Vec<_> = self
            .pages
            .drain()
            .map(|(id, cached)| (id, cached.node, cached.dirty))
            .collect();
        self.lru_order.clear();
        result
    }

    /// Get number of cached pages
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Check if page is in cache
    pub fn contains(&self, page_id: PageId) -> bool {
        self.pages.contains_key(&page_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_cache_new() {
        let cache = PageCache::new(10);
        assert_eq!(cache.len(), 0);
        assert!(!cache.needs_eviction());
    }

    #[test]
    fn test_page_cache_insert_and_get() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node.clone(), false);
        assert_eq!(cache.len(), 1);
        assert!(cache.contains(1));
        
        let retrieved = cache.get(1);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_page_cache_get_mut() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node, false);
        let mut_ref = cache.get_mut(1);
        assert!(mut_ref.is_some());
        
        // Should mark as dirty
        assert!(cache.pages.get(&1).unwrap().dirty);
    }

    #[test]
    fn test_page_cache_lru_eviction() {
        let mut cache = PageCache::new(3);
        
        let node = Node::Leaf {
            entries: vec![],
        };
        
        // Insert 3 pages
        cache.insert(1, node.clone(), false);
        cache.insert(2, node.clone(), false);
        cache.insert(3, node.clone(), false);
        
        // Cache is now full (3 pages in 3-page cache)
        assert!(cache.needs_eviction());
        assert_eq!(cache.len(), 3);
        
        // Access page 1 to make it most recent
        let _ = cache.get(1);
        
        // Manually evict oldest before inserting 4th page (caller's responsibility)
        let evicted = cache.evict_oldest();
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().0, 2); // Page 2 was least recently used
        
        // Now insert 4th page
        cache.insert(4, node.clone(), false);
        
        // Verify the evicted page is gone and others remain
        assert!(!cache.contains(2));
        assert!(cache.contains(1));
        assert!(cache.contains(3));
        assert!(cache.contains(4));
    }

    #[test]
    fn test_page_cache_mark_clean() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node, true);
        assert!(cache.pages.get(&1).unwrap().dirty);
        
        cache.mark_clean(1);
        assert!(!cache.pages.get(&1).unwrap().dirty);
    }

    #[test]
    fn test_page_cache_get_dirty_pages() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node.clone(), true);
        cache.insert(2, node.clone(), false);
        cache.insert(3, node.clone(), true);
        
        let dirty = cache.get_dirty_pages();
        assert_eq!(dirty.len(), 2);
        assert!(dirty.contains(&1));
        assert!(dirty.contains(&3));
    }

    #[test]
    fn test_page_cache_remove() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node, true);
        assert_eq!(cache.len(), 1);
        
        let removed = cache.remove(1);
        assert!(removed.is_some());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_page_cache_clear() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node.clone(), true);
        cache.insert(2, node.clone(), false);
        cache.insert(3, node, true);
        
        let cleared = cache.clear();
        assert_eq!(cleared.len(), 3);
        assert_eq!(cache.len(), 0);
        
        // Check dirty flags
        assert_eq!(cleared.iter().filter(|(_, _, d)| *d).count(), 2);
    }

    #[test]
    fn test_page_cache_evict_oldest() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node.clone(), false);
        cache.insert(2, node.clone(), true);
        cache.insert(3, node, false);
        
        // Evict oldest (page 1)
        let evicted = cache.evict_oldest();
        assert!(evicted.is_some());
        assert_eq!(evicted.unwrap().0, 1);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_page_cache_get_none() {
        let mut cache = PageCache::new(10);
        let result = cache.get(999);
        assert!(result.is_none());
    }

    #[test]
    fn test_page_cache_get_mut_none() {
        let mut cache = PageCache::new(10);
        let result = cache.get_mut(999);
        assert!(result.is_none());
    }

    #[test]
    fn test_page_cache_lru_order_get() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node.clone(), false);
        cache.insert(2, node.clone(), false);
        cache.insert(3, node, false);
        
        // Access page 1 - makes it most recent
        let _ = cache.get(1);
        
        // Page 1 should be evicted last
        let evicted1 = cache.evict_oldest();
        assert_eq!(evicted1.unwrap().0, 2);
        
        let evicted2 = cache.evict_oldest();
        assert_eq!(evicted2.unwrap().0, 3);
        
        let evicted3 = cache.evict_oldest();
        assert_eq!(evicted3.unwrap().0, 1);
    }

    #[test]
    fn test_page_cache_contains() {
        let mut cache = PageCache::new(10);
        let node = Node::Leaf {
            entries: vec![],
        };
        
        cache.insert(1, node, false);
        assert!(cache.contains(1));
        assert!(!cache.contains(2));
    }
}
