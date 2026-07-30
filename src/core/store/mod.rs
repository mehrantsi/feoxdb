use ahash::RandomState;
use crossbeam_skiplist::SkipMap;
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use scc::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::core::record::{Record, TreeSlot};
use crate::core::ttl_sweep::TtlSweeper;
use crate::error::Result;
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::metadata::Metadata;
use crate::storage::write_buffer::WriteBuffer;

// Re-export public types
pub use self::builder::{StoreBuilder, StoreConfig};
pub use self::migration::{
    migrate, MigrationError, MigrationOptions, MigrationReport, MigrationResult,
};

const VERSION_CLOCK_SHARDS: usize = 64;

// Module declarations
pub mod atomic;
pub mod builder;
pub mod init;
pub mod internal;
pub mod json_patch;
mod migration;
pub mod operations;
pub mod persistence;
pub mod range;
pub mod recovery;
pub mod ttl;

pub(super) struct VersionClock {
    hasher: RandomState,
    shards: Box<[CachePadded<AtomicU64>]>,
}

impl VersionClock {
    fn new(hasher: RandomState) -> Self {
        let shards = (0..VERSION_CLOCK_SHARDS)
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect();
        Self { hasher, shards }
    }

    #[inline]
    fn next(&self, key: &[u8], wall: u64) -> u64 {
        let clock = self.shard(key);
        let mut last = clock.load(Ordering::Relaxed);
        loop {
            let next = if wall > last {
                wall
            } else {
                last.saturating_add(1)
            };
            match clock.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return next,
                Err(current) => last = current,
            }
        }
    }

    #[inline]
    fn observe(&self, key: &[u8], timestamp: u64) {
        if timestamp == u64::MAX {
            return;
        }
        let clock = self.shard(key);
        let mut last = clock.load(Ordering::Relaxed);
        while timestamp > last {
            match clock.compare_exchange_weak(last, timestamp, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(current) => last = current,
            }
        }
    }

    #[inline]
    fn shard(&self, key: &[u8]) -> &AtomicU64 {
        &self.shards[self.shard_index(key)]
    }

    #[inline]
    fn shard_index(&self, key: &[u8]) -> usize {
        self.hasher.hash_one(key) as usize & (VERSION_CLOCK_SHARDS - 1)
    }
}

pub(super) struct MemoryReservation<'a> {
    usage: &'a AtomicUsize,
    amount: usize,
}

impl MemoryReservation<'_> {
    #[inline]
    fn commit(mut self) {
        self.amount = 0;
    }
}

impl Drop for MemoryReservation<'_> {
    fn drop(&mut self) {
        if self.amount != 0 {
            self.usage.fetch_sub(self.amount, Ordering::Relaxed);
        }
    }
}

/// High-performance embedded key-value store.
///
/// `FeoxStore` provides ultra-fast key-value storage with optional persistence.
/// It uses lock-free data structures for concurrent access and achieves
/// sub-microsecond latencies for most operations.
///
/// # Thread Safety
///
/// All methods are thread-safe and can be called concurrently from multiple threads.
pub struct FeoxStore {
    // Main hash table with fine-grained locking using AHash
    pub(super) hash_table: HashMap<Vec<u8>, Arc<Record>, RandomState>,

    // Lock-free skip list for ordered access
    pub(super) tree: Arc<SkipMap<Vec<u8>, TreeSlot>>,

    // Central statistics hub
    pub(super) stats: Arc<Statistics>,
    pub(super) version_clock: VersionClock,

    // Write buffering (optional for memory-only mode)
    pub(super) write_buffer: Option<Arc<WriteBuffer>>,

    // Free space management
    pub(super) free_space: Arc<RwLock<FreeSpaceManager>>,

    // Metadata
    pub(super) _metadata: Arc<RwLock<Metadata>>,
    pub(super) format_version: u32,

    // Set when the device was created or was all zeros: nothing to scan, and the
    // metadata signature has to be published before the first record is written.
    pub(super) fresh_device: bool,

    pub(super) allow_ambiguous_legacy_recovery: bool,
    pub(super) ambiguous_legacy_markers: u64,
    pub(super) read_only: bool,
    pub(super) initialized: bool,

    // Configuration
    pub(super) memory_only: bool,
    pub(super) enable_caching: bool,
    pub(super) max_memory: Option<usize>,

    // Cache (if enabled)
    pub(super) cache: Option<Arc<super::cache::ClockCache>>,
    #[cfg(unix)]
    pub(super) device_fd: Option<i32>,
    pub(super) device_size: u64,
    pub(super) device_file: Option<std::fs::File>,

    // Disk I/O
    pub(super) disk_io: Option<Arc<RwLock<crate::storage::io::DiskIO>>>,

    // TTL sweeper (if enabled)
    pub(super) ttl_sweeper: Arc<RwLock<Option<TtlSweeper>>>,

    // TTL feature flag
    pub(super) enable_ttl: bool,
}

impl FeoxStore {
    /// Create a builder for configuring FeoxStore.
    ///
    /// # Example
    ///
    /// ```rust
    /// use feoxdb::FeoxStore;
    ///
    /// # fn main() -> feoxdb::Result<()> {
    /// let store = FeoxStore::builder()
    ///     .max_memory(2_000_000_000)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder() -> StoreBuilder {
        StoreBuilder::new()
    }

    // ============ Utility Methods ============

    /// Check if a key exists
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.hash_table.contains(key)
    }

    /// Get the number of records in the store
    pub fn len(&self) -> usize {
        self.stats
            .record_count
            .load(std::sync::atomic::Ordering::Relaxed) as usize
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> usize {
        self.stats
            .memory_usage
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot()
    }

    /// Flush all pending writes to disk (for persistent mode)
    pub fn flush(&self) -> Result<()> {
        self.flush_all()
    }
}
