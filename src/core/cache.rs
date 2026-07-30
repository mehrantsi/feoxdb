use crate::constants::*;
use crate::core::record::Record;
use crate::stats::Statistics;
use crate::utils::hash::murmur3_32;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock, RwLockWriteGuard};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

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

struct CacheEntry {
    key: Vec<u8>,
    value: Bytes,
    record: Option<Weak<Record>>,

    /// Reference bit for CLOCK algorithm (accessed recently)
    reference_bit: AtomicBool,

    /// Size of this entry in bytes
    size: usize,
}

pub(crate) struct RecordCacheEntry<'a> {
    bucket: RwLockWriteGuard<'a, Vec<CacheEntry>>,
    position: Option<usize>,
    stats: &'a Statistics,
}

impl RecordCacheEntry<'_> {
    pub(crate) fn value(&self) -> Option<Bytes> {
        self.position
            .map(|position| self.bucket[position].value.clone())
    }

    pub(crate) fn remove(mut self) {
        if let Some(position) = self.position {
            let entry = self.bucket.remove(position);
            self.stats
                .cache_memory
                .fetch_sub(entry.size, Ordering::Relaxed);
        }
    }
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
        self.get_entry(key, None)
    }

    pub(crate) fn get_for_record(&self, key: &[u8], record: &Arc<Record>) -> Option<Bytes> {
        self.get_entry(key, Some(record))
    }

    fn get_entry(&self, key: &[u8], record: Option<&Arc<Record>>) -> Option<Bytes> {
        let hash = murmur3_32(key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let bucket = self.buckets[bucket_idx].read();

        for entry in bucket.iter() {
            if entry.key != key {
                continue;
            }
            let generation_matches = match (record, entry.record.as_ref()) {
                (Some(expected), Some(cached)) => {
                    std::ptr::eq(cached.as_ptr(), Arc::as_ptr(expected))
                }
                (Some(_), None) => false,
                (None, _) => true,
            };
            if generation_matches {
                // Set reference bit on access (CLOCK algorithm)
                entry.reference_bit.store(true, Ordering::Relaxed);
                return Some(entry.value.clone());
            }
        }

        None
    }

    /// Insert value into cache, triggering eviction if needed
    pub fn insert(&self, key: Vec<u8>, value: Bytes) {
        self.insert_entry(key, value, None);
    }

    pub(crate) fn insert_for_record(&self, key: Vec<u8>, value: Bytes, record: &Arc<Record>) {
        self.insert_entry(key, value, Some(record));
    }

    fn insert_entry(&self, key: Vec<u8>, value: Bytes, record: Option<&Arc<Record>>) {
        let size = key.len() + value.len() + std::mem::size_of::<CacheEntry>();

        // Don't cache very large values
        let high_watermark = self.high_watermark.load(Ordering::Relaxed);
        if size > high_watermark / 4 {
            return;
        }

        // Check if we need to evict before inserting
        let current_usage = self.stats.cache_memory.load(Ordering::Relaxed);
        if current_usage + size > high_watermark {
            self.evict_entries();
        }

        let hash = murmur3_32(&key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let mut bucket = self.buckets[bucket_idx].write();

        // Check if key already exists and update
        for entry in bucket.iter_mut() {
            if entry.key == key {
                if !can_replace_generation(entry.record.as_ref(), record) {
                    return;
                }
                let old_size = entry.size;
                entry.value = value;
                entry.record = record.map(Arc::downgrade);
                entry.size = size;
                entry.reference_bit.store(true, Ordering::Relaxed);

                // Update memory usage
                if size > old_size {
                    self.stats
                        .cache_memory
                        .fetch_add(size - old_size, Ordering::Relaxed);
                } else {
                    self.stats
                        .cache_memory
                        .fetch_sub(old_size - size, Ordering::Relaxed);
                }
                return;
            }
        }

        // Add new entry
        let entry = CacheEntry {
            key,
            value,
            record: record.map(Arc::downgrade),
            reference_bit: AtomicBool::new(true),
            size,
        };

        bucket.push(entry);
        self.stats.cache_memory.fetch_add(size, Ordering::Relaxed);
    }

    /// Remove specific key from cache
    pub fn remove(&self, key: &[u8]) {
        self.remove_entry(key, None);
    }

    pub(crate) fn remove_for_record(&self, key: &[u8], record: &Arc<Record>) {
        self.remove_entry(key, Some(record));
    }

    pub(crate) fn record_entry<'a>(
        &'a self,
        key: &[u8],
        record: &Arc<Record>,
    ) -> RecordCacheEntry<'a> {
        let hash = murmur3_32(key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;
        let bucket = self.buckets[bucket_idx].write();
        let position = bucket.iter().position(|entry| {
            entry.key == key
                && entry
                    .record
                    .as_ref()
                    .is_some_and(|cached| std::ptr::eq(cached.as_ptr(), Arc::as_ptr(record)))
        });

        RecordCacheEntry {
            bucket,
            position,
            stats: self.stats.as_ref(),
        }
    }

    fn remove_entry(&self, key: &[u8], record: Option<&Arc<Record>>) {
        let hash = murmur3_32(key, 0);
        let bucket_idx = (hash as usize) % CACHE_BUCKETS;

        let mut bucket = self.buckets[bucket_idx].write();

        if let Some(pos) = bucket.iter().position(|entry| {
            entry.key == key
                && record.is_none_or(|expected| {
                    entry
                        .record
                        .as_ref()
                        .is_some_and(|cached| std::ptr::eq(cached.as_ptr(), Arc::as_ptr(expected)))
                })
        }) {
            let entry = bucket.remove(pos);
            self.stats
                .cache_memory
                .fetch_sub(entry.size, Ordering::Relaxed);
        }
    }

    /// CLOCK algorithm eviction - scan entries circularly, evicting those without reference bit
    pub fn evict_entries(&self) {
        // Try to acquire eviction lock, return if already evicting
        let _lock = match self.eviction_lock.try_lock() {
            Some(lock) => lock,
            None => return,
        };

        let target_usage = self.low_watermark.load(Ordering::Relaxed);
        let mut current_usage = self.stats.cache_memory.load(Ordering::Relaxed);

        if current_usage <= target_usage {
            return;
        }

        let mut scans = 0;
        const MAX_SCANS: usize = 3; // Maximum passes through cache
        let mut hand = self.clock_hand.load(Ordering::Relaxed);

        while current_usage > target_usage && scans < MAX_SCANS {
            for _ in 0..CACHE_BUCKETS {
                let bucket_index = hand % CACHE_BUCKETS;
                hand = hand.wrapping_add(1);
                let mut bucket = self.buckets[bucket_index].write();
                let mut i = 0;

                while i < bucket.len() {
                    let entry = &bucket[i];

                    // Check reference bit
                    if entry.reference_bit.load(Ordering::Relaxed) {
                        entry.reference_bit.store(false, Ordering::Relaxed);
                        i += 1;
                    } else {
                        // No reference bit - evict this entry
                        let removed = bucket.remove(i);
                        let previous_usage = self
                            .stats
                            .cache_memory
                            .fetch_sub(removed.size, Ordering::Relaxed);
                        self.stats.record_eviction(1);
                        current_usage = previous_usage - removed.size;
                        // Don't increment i since we removed an element
                    }

                    if current_usage <= target_usage {
                        break;
                    }
                }

                if current_usage <= target_usage {
                    break;
                }
            }

            scans += 1;
        }
        self.clock_hand.store(hand, Ordering::Relaxed);
    }

    /// Clear all cache entries
    pub fn clear(&self) {
        let _lock = self.eviction_lock.lock();
        for bucket in &self.buckets {
            let removed_size = {
                let mut entries = bucket.write();
                let removed_size = entries.iter().map(|entry| entry.size).sum();
                entries.clear();
                removed_size
            };
            self.stats
                .cache_memory
                .fetch_sub(removed_size, Ordering::Relaxed);
            #[cfg(test)]
            crate::test_hooks::pause_at(crate::test_hooks::AFTER_CACHE_BUCKET_CLEAR);
        }

        self.clock_hand.store(0, Ordering::Relaxed);
    }

    /// Get current cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            entries: 0, // Calculate from buckets if needed
            memory_usage: self.stats.cache_memory.load(Ordering::Relaxed),
            high_watermark: self.high_watermark.load(Ordering::Relaxed),
            low_watermark: self.low_watermark.load(Ordering::Relaxed),
        }
    }

    /// Adjust cache watermarks dynamically
    pub fn adjust_watermarks(&self, high_mb: usize, low_mb: usize) {
        let high = high_mb * MB;
        let low = low_mb * MB;

        if high > low && high <= CACHE_MAX_SIZE {
            // Max 1GB for cache
            // Update watermarks atomically
            self.high_watermark.store(high, Ordering::Relaxed);
            self.low_watermark.store(low, Ordering::Relaxed);

            // Trigger eviction if we're over the new high watermark
            let current_usage = self.stats.cache_memory.load(Ordering::Relaxed);
            if current_usage > high {
                self.evict_entries();
            }
        }
    }
}

fn can_replace_generation(cached: Option<&Weak<Record>>, incoming: Option<&Arc<Record>>) -> bool {
    let Some(incoming) = incoming else {
        return true;
    };
    if incoming.refcount.load(Ordering::Acquire) == 0 {
        return false;
    }
    let Some(cached) = cached else {
        return true;
    };
    if std::ptr::eq(cached.as_ptr(), Arc::as_ptr(incoming)) {
        return true;
    }

    cached.upgrade().is_none_or(|cached| {
        cached.refcount.load(Ordering::Acquire) == 0 || cached.timestamp < incoming.timestamp
    })
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: u32,
    pub memory_usage: usize,
    pub high_watermark: usize,
    pub low_watermark: usize,
}
