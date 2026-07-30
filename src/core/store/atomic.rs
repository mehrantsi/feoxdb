use bytes::{BufMut, BytesMut};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::Operation;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

impl FeoxStore {
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
        self.atomic_increment_with_timestamp_and_ttl(key, delta, None, 0)
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
        self.atomic_increment_with_timestamp_and_ttl(key, delta, timestamp, 0)
    }

    /// Atomically increment/decrement with TTL support.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to increment/decrement
    /// * `delta` - Amount to add (negative to decrement)
    /// * `ttl_seconds` - Time-to-live in seconds (0 for no expiry)
    ///
    /// # Errors
    ///
    /// * `InvalidOperation` - Value is not a valid i64
    pub fn atomic_increment_with_ttl(
        &self,
        key: &[u8],
        delta: i64,
        ttl_seconds: u64,
    ) -> Result<i64> {
        self.atomic_increment_with_timestamp_and_ttl(key, delta, None, ttl_seconds)
    }

    /// Atomically increment/decrement with explicit timestamp and TTL.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to increment/decrement
    /// * `delta` - Amount to add (negative to decrement)
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    /// * `ttl_seconds` - Time-to-live in seconds (0 for no expiry)
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn atomic_increment_with_timestamp_and_ttl(
        &self,
        key: &[u8],
        delta: i64,
        timestamp: Option<u64>,
        ttl_seconds: u64,
    ) -> Result<i64> {
        if ttl_seconds > 0 {
            self.ensure_ttl_write_supported()?;
        }
        self.validate_new_key(key)?;

        let key_vec = key.to_vec();
        let explicit_timestamp = timestamp.filter(|timestamp| *timestamp != 0);
        let mut observed = None;

        loop {
            let Some(current) = self.hash_table.read(key, |_, record| Arc::clone(record)) else {
                let retired_at = observed
                    .as_ref()
                    .map_or(0, |record: &Arc<Record>| record.retirement_timestamp());
                let timestamp = match explicit_timestamp {
                    Some(timestamp) => timestamp,
                    None => {
                        let minimum = retired_at.checked_add(1).ok_or(FeoxError::OlderTimestamp)?;
                        self.get_timestamp(key).max(minimum)
                    }
                };
                if timestamp <= retired_at {
                    return Err(FeoxError::OlderTimestamp);
                }

                match self.hash_table.entry(key_vec.clone()) {
                    scc::hash_map::Entry::Occupied(_) => continue,
                    scc::hash_map::Entry::Vacant(entry) => {
                        let record_size = self.calculate_record_size(key.len(), 8);
                        let reservation = self.reserve_memory(record_size)?;
                        let new_record =
                            counter_record(key_vec.clone(), delta, timestamp, ttl_seconds);
                        let ttl_expiry = new_record.ttl_expiry.load(Ordering::Acquire);
                        let buffered_record = self
                            .write_buffer
                            .as_ref()
                            .filter(|_| !self.memory_only)
                            .map(|_| Arc::clone(&new_record));
                        let entry_guard = entry.insert_entry(Arc::clone(&new_record));
                        self.insert_into_tree(key_vec.clone(), new_record);
                        self.observe_published_timestamp(
                            key,
                            timestamp,
                            explicit_timestamp.is_some(),
                        );
                        reservation.commit();
                        self.note_ttl_transition(0, ttl_expiry);
                        self.stats.record_count.fetch_add(1, Ordering::Relaxed);
                        drop(entry_guard);

                        if let (Some(write_buffer), Some(record)) =
                            (&self.write_buffer, buffered_record)
                        {
                            write_buffer.add_write(Operation::Insert, record, 0)?;
                        }
                        return Ok(delta);
                    }
                }
            };

            let root = observed.get_or_insert_with(|| Arc::clone(&current));
            if explicit_timestamp.is_some_and(|timestamp| timestamp <= current.timestamp) {
                return Err(FeoxError::OlderTimestamp);
            }

            let expiry = current.ttl_expiry.load(Ordering::Acquire);
            if self.enable_ttl && expiry > 0 {
                let now = self.get_timestamp_pub();
                if now > expiry {
                    self.retire_expired_if_current(key, &current, now)?;
                    continue;
                }
            }

            let (value, _, source) = match self.resolve_value(key, Arc::clone(&current)) {
                Ok(resolved) => resolved,
                Err(FeoxError::KeyNotFound) => {
                    let now = self.get_timestamp_pub();
                    self.retire_expired_if_current(key, &current, now)?;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if value.len() != 8 {
                return Err(FeoxError::InvalidOperation);
            }
            let current_value = i64::from_le_bytes(
                value
                    .as_ref()
                    .try_into()
                    .map_err(|_| FeoxError::InvalidNumericValue)?,
            );
            let new_value = current_value.saturating_add(delta);
            let timestamp = explicit_timestamp.unwrap_or_else(|| self.get_timestamp(key));

            match self.hash_table.entry(key_vec.clone()) {
                scc::hash_map::Entry::Occupied(mut entry) => {
                    let old_record = entry.get();
                    if !Arc::ptr_eq(old_record, &source) {
                        if explicit_timestamp
                            .is_some_and(|timestamp| timestamp <= root.retirement_timestamp())
                        {
                            return Err(FeoxError::OlderTimestamp);
                        }
                        continue;
                    }
                    if timestamp <= old_record.timestamp {
                        return Err(FeoxError::OlderTimestamp);
                    }

                    let old_size = old_record.calculate_size();
                    let new_size = self.calculate_record_size(old_record.key.len(), 8);
                    let reservation = self.reserve_memory(new_size.saturating_sub(old_size))?;
                    let old_record = Arc::clone(old_record);
                    let record = counter_record(key_vec.clone(), new_value, timestamp, ttl_seconds);
                    let old_expiry = old_record.ttl_expiry.load(Ordering::Acquire);
                    let new_expiry = record.ttl_expiry.load(Ordering::Acquire);

                    old_record.link_successor(&record);
                    old_record.refcount.store(0, Ordering::Release);
                    entry.insert(Arc::clone(&record));
                    self.publish_to_tree(&key_vec, Arc::clone(&record));
                    self.observe_published_timestamp(key, timestamp, explicit_timestamp.is_some());
                    reservation.commit();
                    self.note_ttl_transition(old_expiry, new_expiry);
                    if old_size > new_size {
                        self.release_memory(old_size - new_size);
                    }
                    drop(entry);

                    if !self.memory_only {
                        if self.enable_caching {
                            if let Some(cache) = &self.cache {
                                cache.remove_for_record(&key_vec, &old_record);
                            }
                        }

                        if let Some(write_buffer) = &self.write_buffer {
                            write_buffer.add_replacement(record, old_record)?;
                        }
                    }
                    return Ok(new_value);
                }
                scc::hash_map::Entry::Vacant(_) => {
                    if explicit_timestamp
                        .is_some_and(|timestamp| timestamp <= root.retirement_timestamp())
                    {
                        return Err(FeoxError::OlderTimestamp);
                    }
                }
            }
        }
    }

    /// Insert a key only when it does not already exist.
    ///
    /// The existence check and insertion happen under the hash-table entry guard, so
    /// concurrent callers cannot both create the same key.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` when this call inserted the key and `Ok(false)` when another
    /// value already exists.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// let store = FeoxStore::new(None)?;
    /// assert!(store.insert_if_absent(b"job:1", b"first")?);
    /// assert!(!store.insert_if_absent(b"job:1", b"second")?);
    /// assert_eq!(store.get(b"job:1")?, b"first");
    /// # Ok(())
    /// # }
    /// ```
    pub fn insert_if_absent(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let start = std::time::Instant::now();
        self.validate_key_value(key, value)?;
        let key_vec = key.to_vec();

        match self.hash_table.entry(key_vec.clone()) {
            scc::hash_map::Entry::Occupied(_) => Ok(false),
            scc::hash_map::Entry::Vacant(entry) => {
                let record_size = self.calculate_record_size(key.len(), value.len());
                let reservation = self.reserve_memory(record_size)?;

                let timestamp = self.get_timestamp(key);
                let record = Arc::new(Record::new(key_vec.clone(), value.to_vec(), timestamp));
                let buffered_record = self
                    .write_buffer
                    .as_ref()
                    .filter(|_| !self.memory_only)
                    .map(|_| Arc::clone(&record));
                let entry_guard = entry.insert_entry(Arc::clone(&record));

                self.insert_into_tree(key_vec, record);
                reservation.commit();
                self.stats.record_count.fetch_add(1, Ordering::Relaxed);
                drop(entry_guard);
                self.stats
                    .record_insert(start.elapsed().as_nanos() as u64, false);

                if let (Some(write_buffer), Some(record)) = (&self.write_buffer, buffered_record) {
                    write_buffer.add_write(Operation::Insert, record, 0)?;
                }

                Ok(true)
            }
        }
    }

    /// Atomically compare and swap a value.
    ///
    /// Compares the current value of a key with an expected value, and if they match,
    /// atomically replaces it with a new value. This operation is atomic within the
    /// HashMap shard, preventing race conditions.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check and potentially update
    /// * `expected` - The expected current value
    /// * `new_value` - The new value to set if comparison succeeds
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` if the swap succeeded (current value matched expected).
    /// Returns `Ok(false)` if the current value didn't match or key doesn't exist.
    ///
    /// # Errors
    ///
    /// * `InvalidKeySize` - Key is invalid
    /// * `InvalidValueSize` - New value is too large
    /// * `OutOfMemory` - Memory limit exceeded
    /// * `IoError` - Failed to read value from disk
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::new(None)?;
    /// store.insert(b"config", b"v1")?;
    ///
    /// // Successful CAS - value matches
    /// let swapped = store.compare_and_swap(b"config", b"v1", b"v2")?;
    /// assert_eq!(swapped, true);
    ///
    /// // Failed CAS - value doesn't match
    /// let swapped = store.compare_and_swap(b"config", b"v1", b"v3")?;
    /// assert_eq!(swapped, false); // Value is now "v2", not "v1"
    ///
    /// // CAS on non-existent key
    /// let swapped = store.compare_and_swap(b"missing", b"any", b"new")?;
    /// assert_eq!(swapped, false);
    /// # Ok(())
    /// # }
    /// ```
    pub fn compare_and_swap(&self, key: &[u8], expected: &[u8], new_value: &[u8]) -> Result<bool> {
        self.compare_and_swap_with_timestamp_and_ttl(key, expected, new_value, None, 0)
    }

    /// Compare and swap with explicit timestamp.
    ///
    /// This is the advanced version that allows manual timestamp control for
    /// conflict resolution. Most users should use `compare_and_swap()` instead.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check and potentially update
    /// * `expected` - The expected current value
    /// * `new_value` - The new value to set if comparison succeeds
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn compare_and_swap_with_timestamp(
        &self,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
        timestamp: Option<u64>,
    ) -> Result<bool> {
        self.compare_and_swap_with_timestamp_and_ttl(key, expected, new_value, timestamp, 0)
    }

    /// Compare and swap with TTL support.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check and potentially update
    /// * `expected` - The expected current value
    /// * `new_value` - The new value to set if comparison succeeds
    /// * `ttl_seconds` - Time-to-live in seconds (0 for no expiry)
    ///
    /// # Errors
    ///
    /// * `InvalidKeySize` - Key is invalid
    /// * `InvalidValueSize` - New value is too large
    pub fn compare_and_swap_with_ttl(
        &self,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
        ttl_seconds: u64,
    ) -> Result<bool> {
        self.compare_and_swap_with_timestamp_and_ttl(key, expected, new_value, None, ttl_seconds)
    }

    /// Compare and swap with explicit timestamp and TTL.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check and potentially update
    /// * `expected` - The expected current value
    /// * `new_value` - The new value to set if comparison succeeds
    /// * `timestamp` - Optional timestamp. If `None`, uses current time.
    /// * `ttl_seconds` - Time-to-live in seconds (0 for no expiry)
    ///
    /// # Errors
    ///
    /// * `OlderTimestamp` - Timestamp is not newer than existing record
    pub fn compare_and_swap_with_timestamp_and_ttl(
        &self,
        key: &[u8],
        expected: &[u8],
        new_value: &[u8],
        timestamp: Option<u64>,
        ttl_seconds: u64,
    ) -> Result<bool> {
        if ttl_seconds > 0 {
            self.ensure_ttl_write_supported()?;
        }
        let start = std::time::Instant::now();
        self.validate_key_value(key, new_value)?;
        let key_vec = key.to_vec();

        let initial_record = {
            let record = match self
                .hash_table
                .read(&key_vec, |_, record| Arc::clone(record))
            {
                Some(record) => record,
                None => return Ok(false),
            };
            let (value, cache_hit, source) = match self.resolve_value(key, record) {
                Ok(resolved) => resolved,
                Err(FeoxError::KeyNotFound | FeoxError::StaleExtent) => return Ok(false),
                Err(error) => return Err(error),
            };

            if !cache_hit {
                if let Some(cache) = &self.cache {
                    cache.insert_for_record(key_vec.clone(), value.clone(), &source);
                }
            }
            if value.as_ref() != expected {
                return Ok(false);
            }
            source
        };

        let timestamp = self.resolve_timestamp(key, timestamp);
        self.replace_record_if_current(
            &key_vec,
            &initial_record,
            new_value,
            timestamp,
            ttl_seconds,
            start,
        )
    }

    pub(super) fn replace_record_if_current(
        &self,
        key: &[u8],
        expected: &Arc<Record>,
        new_value: &[u8],
        timestamp: (u64, bool),
        ttl_seconds: u64,
        start: std::time::Instant,
    ) -> Result<bool> {
        let (timestamp, explicit_timestamp) = timestamp;
        match self.hash_table.entry(key.to_vec()) {
            scc::hash_map::Entry::Occupied(mut entry) => {
                let old_record = entry.get();

                if !Arc::ptr_eq(old_record, expected) {
                    return Ok(false);
                }

                if timestamp <= old_record.timestamp {
                    return Err(FeoxError::OlderTimestamp);
                }

                let old_size = old_record.calculate_size();
                let new_size = self.calculate_record_size(key.len(), new_value.len());
                let reservation = self.reserve_memory(new_size.saturating_sub(old_size))?;
                let old_record_arc = Arc::clone(old_record);
                let old_expiry = old_record_arc.ttl_expiry.load(Ordering::Acquire);

                // Create new record with TTL if specified
                let new_record = if ttl_seconds > 0 {
                    let ttl_expiry =
                        timestamp.saturating_add(ttl_seconds.saturating_mul(1_000_000_000));
                    Arc::new(Record::new_with_timestamp_ttl(
                        key.to_vec(),
                        new_value.to_vec(),
                        timestamp,
                        ttl_expiry,
                    ))
                } else {
                    Arc::new(Record::new(key.to_vec(), new_value.to_vec(), timestamp))
                };

                old_record_arc.link_successor(&new_record);
                old_record_arc.refcount.store(0, Ordering::Release);
                entry.insert(Arc::clone(&new_record));
                self.publish_to_tree(key, Arc::clone(&new_record));
                self.observe_published_timestamp(key, timestamp, explicit_timestamp);
                reservation.commit();
                self.note_ttl_transition(old_expiry, new_record.ttl_expiry.load(Ordering::Acquire));
                if old_size > new_size {
                    self.release_memory(old_size - new_size);
                }
                drop(entry);

                self.stats
                    .record_insert(start.elapsed().as_nanos() as u64, true);

                if !self.memory_only {
                    if self.enable_caching {
                        if let Some(ref cache) = self.cache {
                            cache.remove_for_record(key, &old_record_arc);
                        }
                    }

                    if let Some(ref wb) = self.write_buffer {
                        wb.add_replacement(new_record, old_record_arc)?;
                    }
                }

                Ok(true)
            }
            scc::hash_map::Entry::Vacant(_) => Ok(false),
        }
    }
}

fn counter_record(key: Vec<u8>, value: i64, timestamp: u64, ttl_seconds: u64) -> Arc<Record> {
    let mut bytes = BytesMut::with_capacity(std::mem::size_of::<i64>());
    bytes.put_i64_le(value);
    let value = bytes.freeze();

    if ttl_seconds > 0 {
        Arc::new(Record::new_from_bytes_with_ttl(
            key,
            value,
            timestamp,
            timestamp.saturating_add(ttl_seconds.saturating_mul(1_000_000_000)),
        ))
    } else {
        Arc::new(Record::new_from_bytes(key, value, timestamp))
    }
}
