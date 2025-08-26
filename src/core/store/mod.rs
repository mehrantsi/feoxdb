use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::core::record::Record;
use crate::core::ttl_sweep::TtlSweeper;
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::metadata::Metadata;
use crate::storage::write_buffer::WriteBuffer;

// Re-export public types
pub use self::builder::{StoreBuilder, StoreConfig};

// Module declarations
pub mod atomic;
pub mod builder;
pub mod init;
pub mod internal;
pub mod json_patch;
pub mod operations;
pub mod persistence;
pub mod range;
pub mod recovery;
pub mod ttl;

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
    // Main hash table with lock-free operations
    pub(super) hash_table: DashMap<Vec<u8>, Arc<Record>>,

    // Lock-free skip list for ordered access
    pub(super) tree: Arc<SkipMap<Vec<u8>, Arc<Record>>>,

    // Central statistics hub
    pub(super) stats: Arc<Statistics>,

    // Write buffering (optional for memory-only mode)
    pub(super) write_buffer: Option<Arc<WriteBuffer>>,

    // Free space management
    pub(super) free_space: Arc<RwLock<FreeSpaceManager>>,

    // Metadata
    pub(super) _metadata: Arc<RwLock<Metadata>>,

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
        self.hash_table.contains_key(key)
    }

    /// Get the number of records in the store
    pub fn len(&self) -> usize {
        self.stats
            .record_count
            .load(std::sync::atomic::Ordering::Acquire) as usize
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> usize {
        self.stats
            .memory_usage
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Get statistics snapshot
    pub fn stats(&self) -> crate::stats::StatsSnapshot {
        self.stats.snapshot()
    }

    /// Flush all pending writes to disk (for persistent mode)
    pub fn flush(&self) {
        self.flush_all()
    }
}
