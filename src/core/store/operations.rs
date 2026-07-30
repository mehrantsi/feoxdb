use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};

use super::{FeoxStore, MemoryReservation};

impl FeoxStore {
    /// Insert or update a key-value pair.
    ///
    /// If the key already exists with a TTL, the TTL is removed (key becomes permanent).
    /// To preserve or set TTL, use `insert_with_ttl()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if a new key was inserted, `Ok(false)` if an existing key was updated.
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
    /// * Memory mode: ~600ns
    /// * Persistent mode: ~800ns (buffered write)
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        self.insert_with_timestamp(key, value, None)
    }

    /// Insert or update a key-value pair with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control for
    /// conflict resolution. Most users should use `insert()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store
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
    ) -> Result<bool> {
        self.insert_with_timestamp_and_ttl_internal(key, value, timestamp, 0)
    }

    /// Insert or update a key-value pair using zero-copy Bytes.
    ///
    /// This method avoids copying the value data by directly using the Bytes type,
    /// which provides reference-counted zero-copy semantics. Useful when inserting
    /// data that was already read from network or disk as Bytes.
    ///
    /// If the key already exists with a TTL, the TTL is removed (key becomes permanent).
    /// To preserve or set TTL, use `insert_bytes_with_ttl()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store as Bytes
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if a new key was inserted, `Ok(false)` if an existing key was updated.
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
    /// # use bytes::Bytes;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// let data = Bytes::from_static(b"{\"name\":\"Mehran\"}");
    /// store.insert_bytes(b"user:123", data)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~600ns (avoids value copy)
    /// * Persistent mode: ~800ns (buffered write, avoids value copy)
    pub fn insert_bytes(&self, key: &[u8], value: Bytes) -> Result<bool> {
        self.insert_bytes_with_timestamp(key, value, None)
    }

    /// Insert or update a key-value pair using zero-copy Bytes with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control for
    /// conflict resolution. Most users should use `insert_bytes()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store as Bytes
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn insert_bytes_with_timestamp(
        &self,
        key: &[u8],
        value: Bytes,
        timestamp: Option<u64>,
    ) -> Result<bool> {
        self.insert_bytes_with_timestamp_and_ttl_internal(key, value, timestamp, 0)
    }

    pub(super) fn insert_with_timestamp_and_ttl_internal(
        &self,
        key: &[u8],
        value: &[u8],
        timestamp: Option<u64>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        self.validate_key_value(key, value)?;
        let (timestamp, explicit_timestamp) = self.resolve_timestamp(key, timestamp);
        let ttl_expiry = if ttl_seconds > 0 && self.enable_ttl {
            timestamp.saturating_add(ttl_seconds.saturating_mul(1_000_000_000))
        } else {
            0
        };

        let record_size = self.calculate_record_size(key.len(), value.len());

        loop {
            let existing_record = self.hash_table.read(key, |_, v| v.clone());
            if let Some(existing_record) = existing_record {
                if timestamp <= existing_record.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }
                crate::test_hooks::pause_at(crate::test_hooks::AFTER_UPSERT_READ);

                match self.update_record_with_ttl(
                    &existing_record,
                    value,
                    timestamp,
                    explicit_timestamp,
                    ttl_expiry,
                ) {
                    Err(FeoxError::KeyNotFound) => continue,
                    result => return result,
                }
            }

            let reservation = self.reserve_memory(record_size)?;

            let record = if ttl_expiry > 0 && self.enable_ttl {
                Arc::new(Record::new_with_timestamp_ttl(
                    key.to_vec(),
                    value.to_vec(),
                    timestamp,
                    ttl_expiry,
                ))
            } else {
                Arc::new(Record::new(key.to_vec(), value.to_vec(), timestamp))
            };

            let key_vec = record.key.clone();

            let buffered_record = match self.hash_table.entry(key_vec.clone()) {
                scc::hash_map::Entry::Vacant(entry) => {
                    let buffered_record = self
                        .write_buffer
                        .as_ref()
                        .filter(|_| !self.memory_only)
                        .map(|_| Arc::clone(&record));
                    let _entry = entry.insert_entry(Arc::clone(&record));
                    self.insert_into_tree(key_vec, record);
                    self.observe_published_timestamp(key, timestamp, explicit_timestamp);
                    reservation.commit();
                    if ttl_expiry > 0 && self.enable_ttl {
                        self.stats.keys_with_ttl.fetch_add(1, Ordering::Relaxed);
                    }
                    self.stats.record_count.fetch_add(1, Ordering::Relaxed);
                    buffered_record
                }
                scc::hash_map::Entry::Occupied(_) => continue,
            };

            self.stats
                .record_insert(start.elapsed().as_nanos() as u64, false);

            if let (Some(wb), Some(record)) = (&self.write_buffer, buffered_record) {
                wb.add_write(Operation::Insert, record, 0)?;
            }

            return Ok(true);
        }
    }

    /// Internal method to insert a Bytes value with timestamp and TTL (zero-copy)
    pub(super) fn insert_bytes_with_timestamp_and_ttl_internal(
        &self,
        key: &[u8],
        value: Bytes,
        timestamp: Option<u64>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        self.validate_new_key(key)?;
        let value_len = value.len();
        if value_len == 0 || value_len > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }
        let (timestamp, explicit_timestamp) = self.resolve_timestamp(key, timestamp);

        let ttl_expiry = if ttl_seconds > 0 && self.enable_ttl {
            timestamp.saturating_add(ttl_seconds.saturating_mul(1_000_000_000))
        } else {
            0
        };
        self.insert_bytes_with_expiry(key, value, timestamp, explicit_timestamp, ttl_expiry, start)
    }

    pub(super) fn insert_migrated_bytes(
        &self,
        key: &[u8],
        value: Bytes,
        timestamp: u64,
        ttl_expiry: u64,
    ) -> Result<bool> {
        let start = std::time::Instant::now();
        self.validate_new_key(key)?;
        if value.is_empty() || value.len() > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }
        self.insert_bytes_with_expiry(key, value, timestamp, true, ttl_expiry, start)
    }

    #[inline]
    fn insert_bytes_with_expiry(
        &self,
        key: &[u8],
        value: Bytes,
        timestamp: u64,
        explicit_timestamp: bool,
        ttl_expiry: u64,
        start: std::time::Instant,
    ) -> Result<bool> {
        let new_size = self.calculate_record_size(key.len(), value.len());
        loop {
            let existing_record = self.hash_table.read(key, |_, v| v.clone());
            if let Some(existing_record) = existing_record {
                if timestamp <= existing_record.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }

                match self.update_record_with_ttl_bytes(
                    &existing_record,
                    value.clone(),
                    timestamp,
                    explicit_timestamp,
                    ttl_expiry,
                ) {
                    Err(FeoxError::KeyNotFound) => continue,
                    result => return result,
                }
            }

            let reservation = self.reserve_memory(new_size)?;

            let record = if ttl_expiry > 0 {
                Arc::new(Record::new_from_bytes_with_ttl(
                    key.to_vec(),
                    value.clone(),
                    timestamp,
                    ttl_expiry,
                ))
            } else {
                Arc::new(Record::new_from_bytes(
                    key.to_vec(),
                    value.clone(),
                    timestamp,
                ))
            };

            let key_vec = record.key.clone();

            let buffered_record = match self.hash_table.entry(key_vec.clone()) {
                scc::hash_map::Entry::Vacant(entry) => {
                    let buffered_record = self
                        .write_buffer
                        .as_ref()
                        .filter(|_| !self.memory_only)
                        .map(|_| Arc::clone(&record));
                    let _entry = entry.insert_entry(Arc::clone(&record));
                    self.insert_into_tree(key_vec, record);
                    self.observe_published_timestamp(key, timestamp, explicit_timestamp);
                    reservation.commit();
                    if ttl_expiry > 0 {
                        self.stats.keys_with_ttl.fetch_add(1, Ordering::Relaxed);
                    }
                    self.stats.record_count.fetch_add(1, Ordering::Relaxed);
                    buffered_record
                }
                scc::hash_map::Entry::Occupied(_) => continue,
            };

            self.stats
                .record_insert(start.elapsed().as_nanos() as u64, false);

            if let (Some(wb), Some(record)) = (&self.write_buffer, buffered_record) {
                wb.add_write(Operation::Insert, record, 0)?;
            }

            return Ok(true);
        }
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

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        let (value, cache_hit, source) = self.resolve_value(key, record)?;

        if !cache_hit {
            if let Some(ref cache) = self.cache {
                cache.insert_for_record(key.to_vec(), value.clone(), &source);
            }
        }

        self.stats
            .record_get(start.elapsed().as_nanos() as u64, cache_hit);
        Ok(value.to_vec())
    }

    /// Get a value by key without copying (zero-copy).
    ///
    /// Returns `Bytes` which avoids the memory copy that `get()` performs
    /// when converting to `Vec<u8>`.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up
    ///
    /// # Returns
    ///
    /// Returns the value as `Bytes` if found.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// # store.insert(b"key", b"value")?;
    /// let bytes = store.get_bytes(b"key")?;
    /// // Use bytes directly without copying
    /// assert_eq!(&bytes[..], b"value");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// Significantly faster than `get()` for large values:
    /// * 100 bytes: ~15% faster
    /// * 1KB: ~50% faster  
    /// * 10KB: ~90% faster
    /// * 100KB: ~95% faster
    pub fn get_bytes(&self, key: &[u8]) -> Result<Bytes> {
        let start = std::time::Instant::now();
        self.validate_key(key)?;

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        let (value, cache_hit, source) = self.resolve_value(key, record)?;

        if !cache_hit {
            if let Some(ref cache) = self.cache {
                cache.insert_for_record(key.to_vec(), value.clone(), &source);
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
        self.validate_key(key)?;
        let (timestamp, explicit_timestamp) = self.resolve_timestamp(key, timestamp);

        let (record, old_value_len) = match self.hash_table.entry(key.to_vec()) {
            scc::hash_map::Entry::Occupied(entry) => {
                let record = Arc::clone(entry.get());
                if timestamp <= record.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }
                let record_size = record.calculate_size();
                let old_value_len = record.value_len;
                record.retired_at.store(timestamp, Ordering::Release);
                record.refcount.store(0, Ordering::Release);
                // Ordered index first: a key vanishing early from a range scan is
                // benign, whereas a deleted key lingering there is a phantom.
                self.tree.remove(key);
                self.stats.record_count.fetch_sub(1, Ordering::Relaxed);
                self.stats
                    .memory_usage
                    .fetch_sub(record_size, Ordering::Relaxed);
                self.note_ttl_transition(record.ttl_expiry.load(Ordering::Acquire), 0);
                self.observe_published_timestamp(key, timestamp, explicit_timestamp);
                let _ = entry.remove();
                (record, old_value_len)
            }
            scc::hash_map::Entry::Vacant(_) => return Err(FeoxError::KeyNotFound),
        };

        self.remove_cached(key, &record);

        // Queue deletion for persistence if write buffer exists and not memory-only
        if !self.memory_only {
            if let Some(ref wb) = self.write_buffer {
                wb.add_write(Operation::Delete, record, old_value_len)?;
            }
        }

        self.stats.record_delete(start.elapsed().as_nanos() as u64);
        Ok(())
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

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        Ok(record.value_len)
    }

    // Internal helper methods

    pub(super) fn validate_key_value(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_new_key(key)?;

        if value.is_empty() || value.len() > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }

        Ok(())
    }

    /// Bound for a key that is about to create a record. A persistent store must
    /// refuse keys it could never rebuild an index from: the record would be written,
    /// silently dropped on restart, and its extent handed out again while the old
    /// bytes are still on disk to be misparsed as headers.
    pub(super) fn validate_new_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }
        if self.memory_only || key.len() <= MAX_RECOVERABLE_KEY_SIZE {
            return Ok(());
        }
        if self.format_version == 1 && key.len() <= MAX_RECOVERABLE_KEY_SIZE_V1 {
            return Ok(());
        }

        Err(FeoxError::InvalidKeySize)
    }

    pub(super) fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        Ok(())
    }

    #[inline]
    pub(super) fn reserve_memory(&self, amount: usize) -> Result<MemoryReservation<'_>> {
        let usage = &self.stats.memory_usage;
        if amount == 0 {
            return Ok(MemoryReservation { usage, amount });
        }
        let Some(limit) = self.max_memory else {
            usage.fetch_add(amount, Ordering::Relaxed);
            return Ok(MemoryReservation { usage, amount });
        };
        let mut current = usage.load(Ordering::Relaxed);
        loop {
            let next = current.checked_add(amount).ok_or(FeoxError::OutOfMemory)?;
            if next > limit {
                return Err(FeoxError::OutOfMemory);
            }
            match usage.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return Ok(MemoryReservation { usage, amount }),
                Err(observed) => current = observed,
            }
        }
    }

    #[inline]
    pub(super) fn release_memory(&self, amount: usize) {
        self.stats.memory_usage.fetch_sub(amount, Ordering::Relaxed);
    }

    pub(super) fn calculate_record_size(&self, key_len: usize, value_len: usize) -> usize {
        std::mem::size_of::<Record>() + key_len + value_len
    }

    /// Resolve a key's value from memory, cache, or disk.
    ///
    /// A disk read can come back rejected when the record was retired and its
    /// extent reused while this reader held it. That is a stale handle rather than
    /// a failure, so the current generation is fetched from the hash table and the
    /// read retried. Returns the record the value actually came from, so callers
    /// populate the cache against the right generation.
    pub(super) fn resolve_value(
        &self,
        key: &[u8],
        record: Arc<Record>,
    ) -> Result<(Bytes, bool, Arc<Record>)> {
        let mut record = record;
        for _ in 0..STALE_READ_RETRY_LIMIT {
            match self.resolve_record_value(key, &record)? {
                Some((value, cache_hit)) => return Ok((value, cache_hit, record)),
                None => {
                    record = self
                        .hash_table
                        .read(key, |_, v| v.clone())
                        .ok_or(FeoxError::KeyNotFound)?;
                }
            }
        }
        Err(FeoxError::StaleExtent)
    }

    pub(super) fn resolve_value_ref(&self, key: &[u8], record: &Arc<Record>) -> Result<Bytes> {
        if let Some((value, _)) = self.resolve_record_value(key, record)? {
            return Ok(value);
        }

        let record = self
            .hash_table
            .read(key, |_, record| Arc::clone(record))
            .ok_or(FeoxError::KeyNotFound)?;
        self.resolve_value(key, record).map(|(value, _, _)| value)
    }

    fn resolve_record_value(
        &self,
        key: &[u8],
        record: &Arc<Record>,
    ) -> Result<Option<(Bytes, bool)>> {
        if self.enable_ttl {
            let ttl_expiry = record.ttl_expiry.load(Ordering::Acquire);
            if ttl_expiry > 0 {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() as u64;
                if now > ttl_expiry {
                    self.stats.ttl_expired_lazy.fetch_add(1, Ordering::Relaxed);
                    return Err(FeoxError::KeyNotFound);
                }
            }
        }
        if let Some(value) = record.get_value() {
            return Ok(Some((value, true)));
        }
        if let Some(value) = self
            .cache
            .as_ref()
            .and_then(|cache| cache.get_for_record(key, record))
        {
            return Ok(Some((value, true)));
        }
        match self.load_value_from_disk(record) {
            Ok(value) => Ok(Some((value, false))),
            Err(FeoxError::StaleExtent) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Timestamps double as record version numbers and must increase for a key.
    #[inline]
    pub(super) fn get_timestamp(&self, key: &[u8]) -> u64 {
        self.version_clock.next(key, self.get_timestamp_pub())
    }

    #[inline]
    pub(super) fn resolve_timestamp(&self, key: &[u8], timestamp: Option<u64>) -> (u64, bool) {
        match timestamp {
            Some(timestamp) if timestamp != 0 => (timestamp, true),
            _ => (self.get_timestamp(key), false),
        }
    }

    #[inline]
    pub(super) fn observe_published_timestamp(&self, key: &[u8], timestamp: u64, explicit: bool) {
        if explicit {
            self.version_clock.observe(key, timestamp);
        }
    }

    #[cfg(test)]
    pub(crate) fn timestamp_shard_for_test(&self, key: &[u8]) -> usize {
        self.version_clock.shard_index(key)
    }
}
