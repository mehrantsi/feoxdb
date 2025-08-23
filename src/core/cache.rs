use crate::constants::*;
use crate::stats::Statistics;
use crate::utils::hash::murmur3_32;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;

/// CLOCK algorithm cache implementation
/// Uses reference bits and circular scanning for eviction
pub struct ClockCache {
    /// Cache entries organized in buckets for better locality
    buckets: Vec<RwLock<Vec<CacheEntry>>>,

    /// Global CLOCK hand position for eviction scanning
    clock_hand: AtomicUsize,

    /// High watermark for triggering eviction (bytes)
    high_watermark: AtomicUsize,

    /// Low watermark to evict down to (bytes)
    low_watermark: AtomicUsize,

    /// Lock for eviction process
    eviction_lock: Mutex<()>,

    /// Shared statistics
    stats: Arc<Statistics>,
}

#[derive(Clone)]
struct CacheEntry {
    key: Vec<u8>,
    value: Bytes,

    /// Reference bit for CLOCK algorithm (accessed recently)
    reference_bit: Arc<AtomicBool>,

    /// Size of this entry in bytes
    size: usize,

    /// Access count for statistics
    access_count: Arc<AtomicU32>,
}

impl ClockCache {
    pub fn new(stats: Arc<Statistics>) -> Self {
        let buckets = (0..CACHE_BUCKETS)
            .map(|_| RwLock::new(Vec::new()))
            .collect();

        Self {
            buckets,
            clock_hand: AtomicUsize::new(0),
            high_watermark: AtomicUsize::new(CACHE_HIGH_WATERMARK_MB * MB),
            low_watermark: AtomicUsize::new(CACHE_LOW_WATERMARK_MB * MB),
            eviction_lock: Mutex::new(()),
            stats,
        }
    }

    /// Get value from cache, setting reference bit on access
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let hash = murmur3_32(key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let bucket = self.buckets[bucket_idx].read();

        for entry in bucket.iter() {
            if entry.key == key {
                // Set reference bit on access (CLOCK algorithm)
                entry.reference_bit.store(true, Ordering::Release);
                entry.access_count.fetch_add(1, Ordering::Relaxed);
                return Some(entry.value.clone());
            }
        }

        None
    }

    /// Insert value into cache, triggering eviction if needed
    pub fn insert(&self, key: Vec<u8>, value: Bytes) {
        let size = key.len() + value.len() + std::mem::size_of::<CacheEntry>();

        // Don't cache very large values
        let high_watermark = self.high_watermark.load(Ordering::Acquire);
        if size > high_watermark / 4 {
            return;
        }

        // Check if we need to evict before inserting
        let current_usage = self.stats.cache_memory.load(Ordering::Acquire);
        let high_watermark = self.high_watermark.load(Ordering::Acquire);
        if current_usage + size > high_watermark {
            self.evict_entries();
        }

        let hash = murmur3_32(&key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let mut bucket = self.buckets[bucket_idx].write();

        // Check if key already exists and update
        for entry in bucket.iter_mut() {
            if entry.key == key {
                let old_size = entry.size;
                entry.value = value;
                entry.size = size;
                entry.reference_bit.store(true, Ordering::Release);

                // Update memory usage
                if size > old_size {
                    self.stats
                        .cache_memory
                        .fetch_add(size - old_size, Ordering::AcqRel);
                } else {
                    self.stats
                        .cache_memory
                        .fetch_sub(old_size - size, Ordering::AcqRel);
                }
                return;
            }
        }

        // Add new entry
        let entry = CacheEntry {
            key,
            value,
            reference_bit: Arc::new(AtomicBool::new(true)),
            size,
            access_count: Arc::new(AtomicU32::new(1)),
        };

        bucket.push(entry);
        self.stats.cache_memory.fetch_add(size, Ordering::AcqRel);
    }

    /// Remove specific key from cache
    pub fn remove(&self, key: &[u8]) {
        let hash = murmur3_32(key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let mut bucket = self.buckets[bucket_idx].write();

        if let Some(pos) = bucket.iter().position(|e| e.key == key) {
            let entry = bucket.remove(pos);
            self.stats
                .cache_memory
                .fetch_sub(entry.size, Ordering::AcqRel);
        }
    }

    /// CLOCK algorithm eviction - scan entries circularly, evicting those without reference bit
    pub fn evict_entries(&self) {
        // Try to acquire eviction lock, return if already evicting
        let _lock = match self.eviction_lock.try_lock() {
            Some(lock) => lock,
            None => return,
        };

        let target_usage = self.low_watermark.load(Ordering::Acquire);
        let mut current_usage = self.stats.cache_memory.load(Ordering::Acquire);

        if current_usage <= target_usage {
            return;
        }

        let mut scans = 0;
        const MAX_SCANS: usize = 3; // Maximum passes through cache

        while current_usage > target_usage && scans < MAX_SCANS {
            let mut entries_checked = 0;
            let mut bucket_count = 0;
            for bucket in &self.buckets {
                bucket_count += bucket.read().len();
            }
            let total_entries = bucket_count;

            // Scan through buckets using CLOCK hand
            while entries_checked < total_entries && current_usage > target_usage {
                let hand = self.clock_hand.fetch_add(1, Ordering::AcqRel) % CACHE_BUCKETS;

                let mut bucket = self.buckets[hand].write();
                let mut i = 0;

                while i < bucket.len() {
                    let entry = &bucket[i];

                    // Check reference bit
                    if entry.reference_bit.load(Ordering::Acquire) {
                        // Clear reference bit and give second chance with barrier
                        entry.reference_bit.store(false, Ordering::Release);
                        std::sync::atomic::fence(Ordering::Release);
                        i += 1;
                    } else {
                        // No reference bit - evict this entry
                        let removed = bucket.remove(i);
                        self.stats
                            .cache_memory
                            .fetch_sub(removed.size, Ordering::AcqRel);
                        self.stats.record_eviction(1);
                        current_usage -= removed.size;
                        // Don't increment i since we removed an element
                    }

                    entries_checked += 1;

                    if current_usage <= target_usage {
                        break;
                    }
                }
            }

            scans += 1;
        }
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        for bucket in &self.buckets {
            bucket.write().clear();
        }

        self.stats.cache_memory.store(0, Ordering::Release);
        self.clock_hand.store(0, Ordering::Release);
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: 0, // Calculate from buckets if needed
            memory_usage: self.stats.cache_memory.load(Ordering::Acquire),
            high_watermark: self.high_watermark.load(Ordering::Acquire),
            low_watermark: self.low_watermark.load(Ordering::Acquire),
        }
    }

    /// Adjust cache watermarks dynamically
    pub fn adjust_watermarks(&self, high_mb: usize, low_mb: usize) {
        let high = high_mb * MB;
        let low = low_mb * MB;

        if high > low && high <= CACHE_MAX_SIZE {
            // Max 1GB for cache
            // Update watermarks atomically
            self.high_watermark.store(high, Ordering::Release);
            self.low_watermark.store(low, Ordering::Release);

            // Trigger eviction if we're over the new high watermark
            let current_usage = self.stats.cache_memory.load(Ordering::Acquire);
            if current_usage > high {
                self.evict_entries();
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: u32,
    pub memory_usage: usize,
    pub high_watermark: usize,
    pub low_watermark: usize,
}
