use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use parking_lot::RwLock;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub use crate::constants::Operation;
use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::metadata::Metadata;
use crate::storage::write_buffer::WriteBuffer;

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
    hash_table: DashMap<Vec<u8>, Arc<Record>>,

    // Lock-free skip list for ordered access
    tree: Arc<SkipMap<Vec<u8>, Arc<Record>>>,

    // Central statistics hub
    stats: Arc<Statistics>,

    // Write buffering (optional for memory-only mode)
    write_buffer: Option<Arc<WriteBuffer>>,

    // Free space management
    free_space: Arc<RwLock<FreeSpaceManager>>,

    // Metadata
    _metadata: Arc<RwLock<Metadata>>,

    // Configuration
    memory_only: bool,
    enable_caching: bool,
    max_memory: Option<usize>,

    // Cache (if enabled)
    cache: Option<Arc<super::cache::ClockCache>>,
    #[cfg(unix)]
    device_fd: Option<i32>,
    device_size: u64,
    device_file: Option<std::fs::File>,

    // Disk I/O
    disk_io: Option<Arc<RwLock<crate::storage::io::DiskIO>>>,
}

/// Configuration options for FeoxStore.
///
/// Use `StoreBuilder` for a more ergonomic way to configure the store.
pub struct StoreConfig {
    pub hash_bits: u32,
    pub memory_only: bool,
    pub enable_caching: bool,
    pub device_path: Option<String>,
    pub max_memory: Option<usize>,
}

/// Builder for creating FeoxStore with custom configuration.
///
/// Provides a fluent interface for configuring store parameters.
///
/// # Example
///
/// ```rust
/// use feoxdb::FeoxStore;
///
/// # fn main() -> feoxdb::Result<()> {
/// let store = FeoxStore::builder()
///     .max_memory(1_000_000_000)
///     .hash_bits(20)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct StoreBuilder {
    hash_bits: u32,
    device_path: Option<String>,
    max_memory: Option<usize>,
    enable_caching: Option<bool>,
}

impl StoreBuilder {
    pub fn new() -> Self {
        Self {
            hash_bits: DEFAULT_HASH_BITS,
            device_path: None,
            max_memory: Some(DEFAULT_MAX_MEMORY),
            enable_caching: None, // Auto-detect based on storage mode
        }
    }

    /// Set the device path for persistent storage.
    ///
    /// When set, data will be persisted to disk asynchronously.
    /// If not set, the store operates in memory-only mode.
    pub fn device_path(mut self, path: impl Into<String>) -> Self {
        self.device_path = Some(path.into());
        self
    }

    /// Set the maximum memory limit (in bytes).
    ///
    /// The store will start evicting entries when this limit is approached.
    /// Default: 1GB
    pub fn max_memory(mut self, limit: usize) -> Self {
        self.max_memory = Some(limit);
        self
    }

    /// Remove memory limit.
    ///
    /// Use with caution as the store can grow unbounded.
    pub fn no_memory_limit(mut self) -> Self {
        self.max_memory = None;
        self
    }

    /// Set number of hash bits (determines hash table size).
    ///
    /// More bits = larger hash table = better performance for large datasets.
    /// Default: 18 (256K buckets)
    pub fn hash_bits(mut self, bits: u32) -> Self {
        self.hash_bits = bits;
        self
    }

    /// Enable or disable caching.
    ///
    /// When enabled, frequently accessed values are kept in memory
    /// even after being written to disk. Uses CLOCK eviction algorithm.
    pub fn enable_caching(mut self, enable: bool) -> Self {
        self.enable_caching = Some(enable);
        self
    }

    /// Build the FeoxStore
    pub fn build(self) -> Result<FeoxStore> {
        let memory_only = self.device_path.is_none();
        let enable_caching = self.enable_caching.unwrap_or(!memory_only);

        let config = StoreConfig {
            hash_bits: self.hash_bits,
            memory_only,
            enable_caching,
            device_path: self.device_path,
            max_memory: self.max_memory,
        };

        FeoxStore::with_config(config)
    }
}

impl Default for StoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl FeoxStore {
    /// Create a new FeoxStore with default configuration
    pub fn new(device_path: Option<String>) -> Result<Self> {
        let memory_only = device_path.is_none();
        let config = StoreConfig {
            hash_bits: DEFAULT_HASH_BITS,
            memory_only,
            enable_caching: !memory_only, // Disable caching for memory-only mode
            device_path,
            max_memory: Some(DEFAULT_MAX_MEMORY),
        };
        let hash_table = DashMap::with_capacity(1 << config.hash_bits);

        let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
        let metadata = Arc::new(RwLock::new(Metadata::new()));
        let stats = Arc::new(Statistics::new());

        let cache = if config.enable_caching {
            Some(Arc::new(super::cache::ClockCache::new(stats.clone())))
        } else {
            None
        };

        let mut store = Self {
            hash_table,
            tree: Arc::new(SkipMap::new()),
            stats: stats.clone(),
            write_buffer: None,
            free_space: free_space.clone(),
            _metadata: metadata,
            memory_only: config.memory_only,
            enable_caching: config.enable_caching,
            max_memory: config.max_memory,
            cache,
            #[cfg(unix)]
            device_fd: None,
            device_size: 0,
            device_file: None,
            disk_io: None,
        };

        if !config.memory_only {
            store.open_device(&config.device_path)?;
            store.load_indexes()?;

            // Initialize write buffer for persistent mode
            if let Some(ref disk_io) = store.disk_io {
                let mut write_buffer = WriteBuffer::new(disk_io.clone(), free_space, stats.clone());
                let num_workers = (num_cpus::get() / 2).max(1);
                write_buffer.start_workers(num_workers);
                store.write_buffer = Some(Arc::new(write_buffer));
            }
        }

        Ok(store)
    }

    /// Create a new FeoxStore with custom configuration
    pub fn with_config(config: StoreConfig) -> Result<Self> {
        // Initialize hash table with configured capacity
        let hash_table = DashMap::with_capacity(1 << config.hash_bits);

        let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
        let metadata = Arc::new(RwLock::new(Metadata::new()));
        let stats = Arc::new(Statistics::new());

        let cache = if config.enable_caching {
            Some(Arc::new(super::cache::ClockCache::new(stats.clone())))
        } else {
            None
        };

        let mut store = Self {
            hash_table,
            tree: Arc::new(SkipMap::new()),
            stats: stats.clone(),
            write_buffer: None,
            free_space: free_space.clone(),
            _metadata: metadata,
            memory_only: config.memory_only,
            enable_caching: config.enable_caching,
            max_memory: config.max_memory,
            cache,
            #[cfg(unix)]
            device_fd: None,
            device_size: 0,
            device_file: None,
            disk_io: None,
        };

        if !config.memory_only {
            store.open_device(&config.device_path)?;
            store.load_indexes()?;

            // Initialize write buffer for persistent mode
            if let Some(ref disk_io) = store.disk_io {
                let mut write_buffer = WriteBuffer::new(disk_io.clone(), free_space, stats.clone());
                let num_workers = (num_cpus::get() / 2).max(1);
                write_buffer.start_workers(num_workers);
                store.write_buffer = Some(Arc::new(write_buffer));
            }
        }

        Ok(store)
    }

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

    /// Insert or update a key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (max 65KB)
    /// * `value` - The value to store (max 4GB)
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
    ///
    /// # Errors
    ///
    /// * `InvalidKey` - Key is empty or too large
    /// * `InvalidValue` - Value is too large
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    /// * `OutOfMemory` - Memory limit exceeded
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// store.insert(b"user:123", b"{\"name\":\"Mehran\"}")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~800ns
    /// * Persistent mode: ~1µs (buffered write)
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.insert_with_timestamp(key, value, None)
    }

    /// Insert or update a key-value pair with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control for
    /// conflict resolution. Most users should use `insert()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (max 65KB)
    /// * `value` - The value to store (max 4GB)
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn insert_with_timestamp(
        &self,
        key: &[u8],
        value: &[u8],
        timestamp: Option<u64>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };
        self.validate_key_value(key, value)?;

        // Check for existing record
        let is_update = self.hash_table.contains_key(key);
        if let Some(existing_record) = self.hash_table.get(key) {
            let existing_ts = existing_record.timestamp;
            let existing_clone = Arc::clone(&existing_record);
            drop(existing_record); // Release the reference before updating

            if timestamp < existing_ts {
                return Err(FeoxError::OlderTimestamp);
            }

            // Update existing record
            return self.update_record(&existing_clone, value, timestamp);
        }

        let record_size = self.calculate_record_size(key.len(), value.len());
        if !self.check_memory_limit(record_size) {
            return Err(FeoxError::OutOfMemory);
        }

        // Create new record
        let record = Arc::new(Record::new(key.to_vec(), value.to_vec(), timestamp));

        let key_vec = record.key.clone();

        // Insert into hash table - DashMap handles locking internally
        self.hash_table.insert(key_vec.clone(), Arc::clone(&record));

        // Insert into lock-free skip list for ordered access
        self.tree.insert(key_vec, Arc::clone(&record));

        // Update statistics
        self.stats.record_count.fetch_add(1, Ordering::AcqRel);
        self.stats
            .memory_usage
            .fetch_add(record_size, Ordering::AcqRel);
        self.stats
            .record_insert(start.elapsed().as_nanos() as u64, is_update);

        // Only do persistence and cache checks if not in memory-only mode
        if !self.memory_only {
            // Queue for persistence if write buffer exists
            if let Some(ref wb) = self.write_buffer {
                if let Err(_e) = wb.add_write(Operation::Insert, record, 0) {
                    // Don't fail the insert - data is still in memory
                    // Return code already indicates success since data is in memory
                }
            }

            // Check memory pressure and trigger cache eviction if needed
            if self.enable_caching {
                if let Some(ref cache) = self.cache {
                    let stats = cache.stats();
                    if stats.memory_usage > stats.high_watermark {
                        cache.evict_entries();
                    }
                }
            }
        }

        Ok(())
    }

    /// Retrieve a value by key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    /// * `expected_size` - Optional expected value size for validation
    ///
    /// # Returns
    ///
    /// Returns the value as a `Vec<u8>` if found.
    ///
    /// # Errors
    ///
    /// * `KeyNotFound` - Key does not exist
    /// * `InvalidKey` - Key is invalid
    /// * `SizeMismatch` - Value size doesn't match expected size
    /// * `IoError` - Failed to read from disk (persistent mode)
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// # store.insert(b"key", b"value")?;
    /// let value = store.get(b"key")?;
    /// assert_eq!(value, b"value");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~100ns
    /// * Persistent mode (cached): ~150ns
    /// * Persistent mode (disk read): ~500ns
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>> {
        let start = std::time::Instant::now();
        self.validate_key(key)?;

        let mut cache_hit = false;
        if self.enable_caching {
            if let Some(ref cache) = self.cache {
                if let Some(value) = cache.get(key) {
                    self.stats
                        .record_get(start.elapsed().as_nanos() as u64, true);
                    return Ok(value.to_vec());
                }
            }
        }

        let record = self.hash_table.get(key).ok_or(FeoxError::KeyNotFound)?;

        let value = if let Some(val) = record.get_value() {
            val.to_vec()
        } else {
            cache_hit = false; // Reading from disk
            self.load_value_from_disk(&record)?
        };

        if self.enable_caching {
            if let Some(ref cache) = self.cache {
                cache.insert(key.to_vec(), Bytes::from(value.clone()));
            }
        }

        self.stats
            .record_get(start.elapsed().as_nanos() as u64, cache_hit);
        Ok(value)
    }

    /// Delete a key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    /// * `timestamp` - Optional timestamp for conflict resolution
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the key was deleted.
    ///
    /// # Errors
    ///
    /// * `KeyNotFound` - Key does not exist
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// # store.insert(b"temp", b"data")?;
    /// store.delete(b"temp")?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~300ns
    /// * Persistent mode: ~400ns
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.delete_with_timestamp(key, None)
    }

    /// Delete a key-value pair with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control.
    /// Most users should use `delete()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn delete_with_timestamp(&self, key: &[u8], timestamp: Option<u64>) -> Result<()> {
        let start = std::time::Instant::now();
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };
        self.validate_key(key)?;

        // Remove from hash table and get the record
        let (_key, record) = self.hash_table.remove(key).ok_or(FeoxError::KeyNotFound)?;

        if timestamp < record.timestamp {
            // Put it back if timestamp is older
            self.hash_table.insert(key.to_vec(), record);
            return Err(FeoxError::OlderTimestamp);
        }

        let record_size = record.calculate_size();
        let old_value_len = record.value_len;

        // Mark record as deleted by setting refcount to 0
        record.refcount.store(0, Ordering::Release);

        // Remove from lock-free skip list
        self.tree.remove(key);

        // Update statistics
        self.stats.record_count.fetch_sub(1, Ordering::AcqRel);
        self.stats
            .memory_usage
            .fetch_sub(record_size, Ordering::AcqRel);

        // Clear from cache
        if self.enable_caching {
            if let Some(ref cache) = self.cache {
                cache.remove(key);
            }
        }

        // Queue deletion for persistence if write buffer exists and not memory-only
        if !self.memory_only {
            if let Some(ref wb) = self.write_buffer {
                if let Err(_e) = wb.add_write(Operation::Delete, record, old_value_len) {
                    // Silent failure - data operation succeeded in memory
                }
            }
        }

        self.stats.record_delete(start.elapsed().as_nanos() as u64);
        Ok(())
    }

    /// Apply a JSON patch to a value.
    ///
    /// Uses RFC 6902 JSON Patch format to modify specific fields in a JSON document.
    /// Both the existing value and the patch must be valid JSON.
    ///
    /// # Arguments
    ///
    /// * `key` - The key containing the JSON document to patch
    /// * `patch` - JSON Patch operations in RFC 6902 format
    /// * `timestamp` - Optional timestamp for conflict resolution
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the update was applied.
    ///
    /// # Errors
    ///
    /// * `KeyNotFound` - Key does not exist
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    /// * `JsonPatchError` - Invalid JSON document or patch format
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// // Insert initial JSON value
    /// let initial = br#"{"name":"Alice","age":30}"#;
    /// store.insert(b"user:1", initial)?;
    ///
    /// // Apply JSON patch to update age
    /// let patch = br#"[{"op":"replace","path":"/age","value":31}]"#;
    /// store.json_patch(b"user:1", patch)?;
    ///
    /// // Value now has age updated to 31
    /// let updated = store.get(b"user:1")?;
    /// assert_eq!(updated.len(), initial.len()); // Same length, just age changed
    /// # Ok(())
    /// # }
    /// ```
    pub fn json_patch(&self, key: &[u8], patch: &[u8]) -> Result<()> {
        self.json_patch_with_timestamp(key, patch, None)
    }

    /// Apply JSON patch with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control.
    /// Most users should use `json_patch()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key whose value to patch
    /// * `patch` - JSON Patch array (RFC 6902)
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn json_patch_with_timestamp(
        &self,
        key: &[u8],
        patch: &[u8],
        timestamp: Option<u64>,
    ) -> Result<()> {
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };
        self.validate_key(key)?;

        // Get the value and release the lock immediately
        let current_value = {
            let record = self.hash_table.get(key).ok_or(FeoxError::KeyNotFound)?;

            if timestamp < record.timestamp {
                return Err(FeoxError::OlderTimestamp);
            }

            if let Some(val) = record.get_value() {
                val.to_vec()
            } else {
                self.load_value_from_disk(&record)?
            }
        };

        let new_value = crate::utils::json_patch::apply_json_patch(&current_value, patch)?;

        // Now update without holding any references
        self.insert_with_timestamp(key, &new_value, Some(timestamp))
    }

    fn update_record(&self, old_record: &Record, value: &[u8], timestamp: u64) -> Result<()> {
        let new_record = Arc::new(Record::new(
            old_record.key.clone(),
            value.to_vec(),
            timestamp,
        ));

        let old_value_len = old_record.value_len;
        let old_size = old_record.calculate_size();
        let new_size = self.calculate_record_size(old_record.key.len(), value.len());

        let old_record_arc = if let Some(entry) = self.hash_table.get(&old_record.key) {
            Arc::clone(&*entry)
        } else {
            return Err(FeoxError::KeyNotFound);
        };

        let key_vec = new_record.key.clone();

        self.hash_table
            .insert(key_vec.clone(), Arc::clone(&new_record));
        self.tree.insert(key_vec.clone(), Arc::clone(&new_record));

        if new_size > old_size {
            self.stats
                .memory_usage
                .fetch_add(new_size - old_size, Ordering::AcqRel);
        } else {
            self.stats
                .memory_usage
                .fetch_sub(old_size - new_size, Ordering::AcqRel);
        }

        // Only do cache and persistence operations if not in memory-only mode
        if !self.memory_only {
            if self.enable_caching {
                if let Some(ref cache) = self.cache {
                    cache.remove(&key_vec);
                }
            }

            if let Some(ref wb) = self.write_buffer {
                if let Err(e) =
                    wb.add_write(Operation::Update, Arc::clone(&new_record), old_value_len)
                {
                    // Data operation succeeded in memory
                    let _ = e;
                }

                if let Err(e) = wb.add_write(Operation::Delete, old_record_arc, old_value_len) {
                    // Data operation succeeded in memory
                    let _ = e;
                }
            }
        }

        Ok(())
    }

    fn validate_key_value(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        if value.is_empty() || value.len() > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }

        Ok(())
    }

    fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        Ok(())
    }

    fn check_memory_limit(&self, size: usize) -> bool {
        match self.max_memory {
            Some(limit) => {
                let current = self.stats.memory_usage.load(Ordering::Acquire);
                current + size <= limit
            }
            None => true,
        }
    }

    fn calculate_record_size(&self, key_len: usize, value_len: usize) -> usize {
        std::mem::size_of::<Record>() + key_len + value_len
    }

    /// Perform a range query on the store.
    ///
    /// Returns all key-value pairs where the key is >= `start_key` and <= `end_key`.
    /// Both bounds are inclusive.
    ///
    /// # Arguments
    ///
    /// * `start_key` - Inclusive lower bound
    /// * `end_key` - Inclusive upper bound
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// Returns a vector of (key, value) pairs in sorted order.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// store.insert(b"user:001", b"Alice")?;
    /// store.insert(b"user:002", b"Bob")?;
    /// store.insert(b"user:003", b"Charlie")?;
    /// store.insert(b"user:004", b"David")?;
    ///
    /// // Get users 001 through 003 (inclusive)
    /// let results = store.range_query(b"user:001", b"user:003", 10)?;
    /// assert_eq!(results.len(), 3);
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_query(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key.len() > MAX_KEY_SIZE || end_key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        let mut results = Vec::new();

        for entry in self.tree.range(start_key.to_vec()..=end_key.to_vec()) {
            if results.len() >= limit {
                break;
            }

            let record = entry.value();
            let value = if let Some(val) = record.get_value() {
                val.to_vec()
            } else {
                self.load_value_from_disk(record)?
            };

            results.push((entry.key().clone(), value));
        }

        Ok(results)
    }

    /// Atomically increment a numeric counter.
    ///
    /// The value must be stored as an 8-byte little-endian i64. If the key doesn't exist,
    /// it will be created with the given delta value. If it exists, the value will be
    /// incremented atomically.
    ///
    /// # Value Format
    ///
    /// The value MUST be exactly 8 bytes representing a little-endian i64.
    /// Use `i64::to_le_bytes()` to create the initial value:
    /// ```rust,ignore
    /// let zero: i64 = 0;
    /// store.insert(b"counter", &zero.to_le_bytes())?;
    /// ```
    ///
    /// # Arguments
    ///
    /// * `key` - The key of the counter
    /// * `delta` - The amount to increment by (can be negative for decrement)
    /// * `timestamp` - Optional timestamp for conflict resolution
    ///
    /// # Returns
    ///
    /// Returns the new value after incrementing.
    ///
    /// # Errors
    ///
    /// * `InvalidOperation` - Existing value is not exactly 8 bytes (not a valid i64)
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// // Initialize counter with proper binary format
    /// let initial: i64 = 0;
    /// store.insert(b"visits", &initial.to_le_bytes())?;
    ///
    /// // Increment atomically
    /// let val = store.atomic_increment(b"visits", 1)?;
    /// assert_eq!(val, 1);
    ///
    /// // Increment by 5
    /// let val = store.atomic_increment(b"visits", 5)?;
    /// assert_eq!(val, 6);
    ///
    /// // Decrement by 2
    /// let val = store.atomic_increment(b"visits", -2)?;
    /// assert_eq!(val, 4);
    ///
    /// // Or create new counter directly (starts at delta value)
    /// let downloads = store.atomic_increment(b"downloads", 100)?;
    /// assert_eq!(downloads, 100);
    /// # Ok(())
    /// # }
    /// ```
    pub fn atomic_increment(&self, key: &[u8], delta: i64) -> Result<i64> {
        self.atomic_increment_with_timestamp(key, delta, None)
    }

    /// Atomically increment/decrement with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control.
    /// Most users should use `atomic_increment()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to increment/decrement
    /// * `delta` - Amount to add (negative to decrement)
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn atomic_increment_with_timestamp(
        &self,
        key: &[u8],
        delta: i64,
        timestamp: Option<u64>,
    ) -> Result<i64> {
        self.validate_key(key)?;

        let key_vec = key.to_vec();

        // Use DashMap's entry API for atomic operations
        let result = match self.hash_table.entry(key_vec.clone()) {
            dashmap::mapref::entry::Entry::Occupied(mut entry) => {
                let old_record = entry.get();

                // Get timestamp inside the critical section to ensure it's always newer
                let timestamp = match timestamp {
                    Some(0) | None => self.get_timestamp(),
                    Some(ts) => ts,
                };

                // Check if timestamp is valid
                if timestamp < old_record.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }

                // Load value from memory or disk
                let value = if let Some(val) = old_record.get_value() {
                    val.to_vec()
                } else {
                    // Try loading from disk if not in memory
                    self.load_value_from_disk(old_record)?
                };

                let current_val = if value.len() == 8 {
                    let bytes = value
                        .get(..8)
                        .and_then(|slice| slice.try_into().ok())
                        .ok_or(FeoxError::InvalidNumericValue)?;
                    i64::from_le_bytes(bytes)
                } else {
                    return Err(FeoxError::InvalidOperation);
                };

                let new_val = current_val.saturating_add(delta);
                let new_value = new_val.to_le_bytes().to_vec();

                // Create new record
                let new_record =
                    Arc::new(Record::new(old_record.key.clone(), new_value, timestamp));

                let old_value_len = old_record.value_len;
                let old_size = old_record.calculate_size();
                let new_size = self.calculate_record_size(old_record.key.len(), 8);
                let old_record_arc = Arc::clone(old_record);

                // Atomically update the entry
                entry.insert(Arc::clone(&new_record));

                // Update skip list as well
                self.tree.insert(key_vec.clone(), Arc::clone(&new_record));

                // Update memory usage
                if new_size > old_size {
                    self.stats
                        .memory_usage
                        .fetch_add(new_size - old_size, Ordering::AcqRel);
                } else {
                    self.stats
                        .memory_usage
                        .fetch_sub(old_size - new_size, Ordering::AcqRel);
                }

                // Only do cache and persistence operations if not in memory-only mode
                if !self.memory_only {
                    if self.enable_caching {
                        if let Some(ref cache) = self.cache {
                            cache.remove(&key_vec);
                        }
                    }

                    if let Some(ref wb) = self.write_buffer {
                        if let Err(e) =
                            wb.add_write(Operation::Update, Arc::clone(&new_record), old_value_len)
                        {
                            // Atomic operation succeeded in memory
                            let _ = e;
                        }

                        if let Err(e) =
                            wb.add_write(Operation::Delete, old_record_arc, old_value_len)
                        {
                            // Atomic operation succeeded in memory
                            let _ = e;
                        }
                    }
                }

                Ok(new_val)
            }
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                // Key doesn't exist, create it with initial value
                // Get timestamp inside the critical section
                let timestamp = match timestamp {
                    Some(0) | None => self.get_timestamp(),
                    Some(ts) => ts,
                };

                let initial_val = delta;
                let value = initial_val.to_le_bytes().to_vec();

                let new_record = Arc::new(Record::new(key_vec.clone(), value, timestamp));

                entry.insert(Arc::clone(&new_record));

                // Update skip list
                self.tree.insert(key_vec.clone(), Arc::clone(&new_record));

                // Update statistics
                self.stats.record_count.fetch_add(1, Ordering::AcqRel);
                let record_size = self.calculate_record_size(key.len(), 8);
                self.stats
                    .memory_usage
                    .fetch_add(record_size, Ordering::AcqRel);

                // Handle persistence if needed
                if !self.memory_only {
                    if let Some(ref wb) = self.write_buffer {
                        if let Err(e) = wb.add_write(Operation::Insert, Arc::clone(&new_record), 0)
                        {
                            // Operation succeeded in memory
                            let _ = e;
                        }
                    }
                }

                Ok(initial_val)
            }
        };

        result
    }

    /// Get the size of a value without loading it.
    ///
    /// Useful for checking value size before loading large values from disk.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// Returns the size in bytes of the value.
    ///
    /// # Errors
    ///
    /// * `KeyNotFound` - Key does not exist
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// store.insert(b"large_file", &vec![0u8; 1_000_000])?;
    ///
    /// // Check size before loading
    /// let size = store.get_size(b"large_file")?;
    /// assert_eq!(size, 1_000_000);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_size(&self, key: &[u8]) -> Result<usize> {
        self.validate_key(key)?;

        let record = self.hash_table.get(key).ok_or(FeoxError::KeyNotFound)?;

        Ok(record.value_len)
    }

    /// Force flush all pending writes to disk.
    ///
    /// In persistent mode, ensures all buffered writes are flushed to disk.
    /// In memory-only mode, this is a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// let store = FeoxStore::new(Some("/path/to/data.feox".to_string()))?;
    /// store.insert(b"important", b"data")?;
    /// store.flush_all();  // Ensure data is persisted
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_all(&self) {
        if !self.memory_only {
            // First flush the write buffer to ensure all data is written
            if let Some(ref wb) = self.write_buffer {
                let _ = wb.force_flush();
            }

            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                let _ = disk_io.write().write_metadata(metadata.as_bytes());
                let _ = disk_io.write().flush();
            }
        }
    }

    fn get_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    fn load_value_from_disk(&self, record: &Record) -> Result<Vec<u8>> {
        let sector = record.sector.load(Ordering::Acquire);
        if self.memory_only || sector == 0 {
            return Err(FeoxError::InvalidRecord);
        }

        // Calculate how many sectors we need to read
        let total_size = SECTOR_HEADER_SIZE + 2 + record.key.len() + 8 + 8 + record.value_len;
        let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);

        // Read the sectors
        let disk_io = self
            .disk_io
            .as_ref()
            .ok_or_else(|| {
                FeoxError::IoError(io::Error::new(
                    io::ErrorKind::NotFound,
                    "No disk IO available",
                ))
            })?
            .read();

        let data = disk_io.read_sectors_sync(sector, sectors_needed as u64)?;

        // Skip header and record metadata to get to the value
        let offset = SECTOR_HEADER_SIZE + 2 + record.key.len() + 8 + 8;
        if offset + record.value_len > data.len() {
            return Err(FeoxError::InvalidRecord);
        }

        Ok(data[offset..offset + record.value_len].to_vec())
    }

    fn open_device(&mut self, device_path: &Option<String>) -> Result<()> {
        if let Some(path) = device_path {
            // Open the device/file
            use std::fs::OpenOptions;
            #[cfg(target_os = "linux")]
            use std::os::unix::fs::OpenOptionsExt;

            #[cfg(unix)]
            let (file, use_direct_io) = if std::path::Path::new("/.dockerenv").exists() {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .map_err(FeoxError::IoError)?;
                (file, false) // Don't use O_DIRECT in Docker
            } else {
                // Try with O_DIRECT on Linux, fall back without it on other Unix systems
                #[cfg(target_os = "linux")]
                {
                    // Try to open with O_DIRECT first
                    match OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .custom_flags(libc::O_DIRECT)
                        .open(path)
                    {
                        Ok(file) => (file, true), // Successfully opened with O_DIRECT
                        Err(_) => {
                            // Fallback to regular open
                            let file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .truncate(false)
                                .open(path)
                                .map_err(FeoxError::IoError)?;
                            (file, false)
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(path)
                        .map_err(FeoxError::IoError)?;
                    (file, false) // O_DIRECT not supported on this platform
                }
            };

            #[cfg(not(unix))]
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(FeoxError::IoError)?;

            // Get file size
            let metadata = file.metadata().map_err(FeoxError::IoError)?;
            self.device_size = metadata.len();

            // Track whether this is a newly created file
            let was_newly_created = self.device_size == 0;

            if was_newly_created {
                // New empty file - set default size and initialize free space
                file.set_len(DEFAULT_DEVICE_SIZE)
                    .map_err(FeoxError::IoError)?;
                self.device_size = DEFAULT_DEVICE_SIZE;

                // Initialize free space manager with all space free
                self.free_space.write().initialize(self.device_size)?;

                let mut metadata = self._metadata.write();
                metadata.device_size = self.device_size;
                metadata.update();
            } else {
                // Existing file - just set device size, free space will be rebuilt during scan
                self.free_space.write().set_device_size(self.device_size);
            }

            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file_arc = Arc::new(file);
                let fd = file_arc.as_raw_fd();
                self.device_fd = Some(fd);
                // Store a clone of the file to keep it alive
                self.device_file = Some(file_arc.as_ref().try_clone().map_err(FeoxError::IoError)?);
                let disk_io = crate::storage::io::DiskIO::new(file_arc, use_direct_io)?;
                self.disk_io = Some(Arc::new(RwLock::new(disk_io)));
            }

            #[cfg(not(unix))]
            {
                // Store a clone of the file to keep it alive
                self.device_file = Some(file.try_clone().map_err(FeoxError::IoError)?);
                let disk_io = crate::storage::io::DiskIO::new_from_file(file)?;
                self.disk_io = Some(Arc::new(RwLock::new(disk_io)));
            }

            let disk_io = self.disk_io.as_ref().unwrap().read();

            // Read metadata from existing files (not newly created ones)
            if !was_newly_created {
                if let Ok(metadata_bytes) = disk_io.read_metadata() {
                    if let Some(loaded_metadata) =
                        crate::storage::metadata::Metadata::from_bytes(&metadata_bytes)
                    {
                        // Initialize stats from metadata
                        self.stats
                            .disk_usage
                            .store(loaded_metadata.total_size, Ordering::Relaxed);
                        *self._metadata.write() = loaded_metadata;
                    }
                }
            }
        }
        Ok(())
    }

    fn load_indexes(&mut self) -> Result<()> {
        if self.memory_only {
            return Ok(());
        }

        // Try to read metadata from sector 0
        if let Some(ref disk_io) = self.disk_io {
            let metadata_data = disk_io.read().read_metadata()?;

            // Check if metadata is valid (has our signature)
            if metadata_data.len() >= FEOX_SIGNATURE_SIZE {
                let signature = &metadata_data[..FEOX_SIGNATURE_SIZE];

                if signature == FEOX_SIGNATURE {
                    // Valid metadata found, scan the disk to rebuild indexes
                    self.scan_and_rebuild_indexes()?;
                }
            }
        }

        Ok(())
    }

    fn scan_and_rebuild_indexes(&mut self) -> Result<()> {
        if self.memory_only || self.device_size == 0 {
            return Ok(());
        }

        let disk_io = self.disk_io.as_ref().ok_or(FeoxError::NoDevice)?;

        let total_sectors = self.device_size / FEOX_BLOCK_SIZE as u64;
        let mut sector: u64 = 1;
        let mut _records_loaded = 0;
        let mut occupied_sectors = Vec::new();

        while sector < total_sectors {
            let data = match disk_io.read().read_sectors_sync(sector, 1) {
                Ok(d) => d,
                Err(_) => {
                    sector += 1;
                    continue;
                }
            };

            if data.len() < SECTOR_HEADER_SIZE {
                sector += 1;
                continue;
            }

            // Check for deletion marker first
            if data.len() >= 8 && &data[..8] == b"\0DELETED" {
                // This sector has been deleted, skip it
                sector += 1;
                continue;
            }

            let marker = u16::from_le_bytes([data[0], data[1]]);
            let seq_num = u16::from_le_bytes([data[2], data[3]]);

            if marker != SECTOR_MARKER || seq_num != 0 {
                sector += 1;
                continue;
            }

            if data.len() < SECTOR_HEADER_SIZE + 2 {
                sector += 1;
                continue;
            }

            let key_len =
                u16::from_le_bytes([data[SECTOR_HEADER_SIZE], data[SECTOR_HEADER_SIZE + 1]])
                    as usize;

            if key_len == 0 || key_len > MAX_KEY_SIZE {
                sector += 1;
                continue;
            }

            if data.len() < SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 {
                sector += 1;
                continue;
            }

            let mut offset = SECTOR_HEADER_SIZE + 2;
            let key = data[offset..offset + key_len].to_vec();
            offset += key_len;

            let value_len = u64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]) as usize;
            offset += 8;

            let timestamp = u64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);

            let total_size = SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 + value_len;
            let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);

            let mut record = Record::new(key.clone(), Vec::new(), timestamp);
            record.sector.store(sector, Ordering::Release);
            record.value_len = value_len;
            record.clear_value();

            let record_arc = Arc::new(record);
            self.hash_table.insert(key.clone(), Arc::clone(&record_arc));
            self.tree.insert(key, Arc::clone(&record_arc));

            self.stats.record_count.fetch_add(1, Ordering::AcqRel);
            let record_size = self.calculate_record_size(key_len, value_len);
            self.stats
                .memory_usage
                .fetch_add(record_size, Ordering::AcqRel);

            // Track disk usage
            self.stats
                .disk_usage
                .fetch_add((sectors_needed * FEOX_BLOCK_SIZE) as u64, Ordering::AcqRel);

            for i in 0..sectors_needed {
                occupied_sectors.push(sector + i as u64);
            }

            _records_loaded += 1;
            sector += sectors_needed as u64;
        }

        // Now rebuild free space from gaps between occupied sectors
        occupied_sectors.sort_unstable();

        // Start after metadata sectors (sectors 0-15 are reserved)
        let mut last_end = FEOX_DATA_START_BLOCK;

        for &occupied_start in &occupied_sectors {
            if occupied_start > last_end {
                self.free_space
                    .write()
                    .release_sectors(last_end, occupied_start - last_end)?;
            }
            last_end = occupied_start + 1;
        }

        if last_end < total_sectors {
            self.free_space
                .write()
                .release_sectors(last_end, total_sectors - last_end)?;
        }

        Ok(())
    }

    // ============ Utility Methods ============

    /// Check if a key exists
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.hash_table.contains_key(key)
    }

    /// Get the number of records in the store
    pub fn len(&self) -> usize {
        self.stats.record_count.load(Ordering::Acquire) as usize
    }

    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get memory usage statistics
    pub fn memory_usage(&self) -> usize {
        self.stats.memory_usage.load(Ordering::Acquire)
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

impl Drop for FeoxStore {
    fn drop(&mut self) {
        // Signal shutdown to write buffer workers
        if let Some(ref wb) = self.write_buffer {
            wb.initiate_shutdown();
        }

        // Write metadata directly without using the write buffer
        if !self.memory_only {
            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                let _ = disk_io.write().write_metadata(metadata.as_bytes());
                let _ = disk_io.write().flush();
            }
        }

        // Take ownership of write_buffer to properly shut it down
        if let Some(wb_arc) = self.write_buffer.take() {
            // Try to get mutable access if we're the only owner
            if let Ok(wb) = Arc::try_unwrap(wb_arc) {
                // We own it exclusively, can call complete_shutdown
                let mut wb_mut = wb;
                wb_mut.complete_shutdown();
            }
            // If we can't get exclusive access, workers are already shutting down via initiate_shutdown
        }

        // Now it's safe to shutdown disk I/O since workers have exited
        if let Some(ref disk_io) = self.disk_io {
            disk_io.write().shutdown();
        }
    }
}
