use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

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
    /// * Memory mode: ~600ns
    /// * Persistent mode: ~800ns (buffered write)
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
    ) -> Result<()> {
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
    pub fn insert_bytes(&self, key: &[u8], value: Bytes) -> Result<()> {
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
    ) -> Result<()> {
        self.insert_bytes_with_timestamp_and_ttl_internal(key, value, timestamp, 0)
    }

    pub(super) fn insert_with_timestamp_and_ttl_internal(
        &self,
        key: &[u8],
        value: &[u8],
        timestamp: Option<u64>,
        ttl_expiry: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };
        self.validate_key_value(key, value)?;

        // Check for existing record
        let is_update = self.hash_table.contains(key);
        let existing_record = self.hash_table.read(key, |_, v| v.clone());
        if let Some(existing_record) = existing_record {
            let existing_ts = existing_record.timestamp;
            let existing_clone = existing_record;

            if timestamp < existing_ts {
                return Err(FeoxError::OlderTimestamp);
            }

            // Update existing record
            return self.update_record_with_ttl(&existing_clone, value, timestamp, ttl_expiry);
        }

        let record_size = self.calculate_record_size(key.len(), value.len());
        if !self.check_memory_limit(record_size) {
            return Err(FeoxError::OutOfMemory);
        }

        // Create new record with TTL if specified and TTL is enabled
        let record = if ttl_expiry > 0 && self.enable_ttl {
            self.stats.keys_with_ttl.fetch_add(1, Ordering::Relaxed);
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

        // Insert into hash table
        self.hash_table.upsert(key_vec.clone(), Arc::clone(&record));

        // Insert into lock-free skip list for ordered access
        self.tree.insert(key_vec, Arc::clone(&record));

        // Update statistics
        self.stats.record_count.fetch_add(1, Ordering::AcqRel);
        self.stats
            .memory_usage
            .fetch_add(record_size, Ordering::AcqRel);
        self.stats
            .record_insert(start.elapsed().as_nanos() as u64, is_update);

        // Only do persistence if not in memory-only mode
        if !self.memory_only {
            // Queue for persistence if write buffer exists
            if let Some(ref wb) = self.write_buffer {
                if let Err(_e) = wb.add_write(Operation::Insert, record, 0) {
                    // Don't fail the insert - data is still in memory
                    // Return code already indicates success since data is in memory
                }
            }
        }

        Ok(())
    }

    /// Internal method to insert a Bytes value with timestamp and TTL (zero-copy)
    pub(super) fn insert_bytes_with_timestamp_and_ttl_internal(
        &self,
        key: &[u8],
        value: Bytes,
        timestamp: Option<u64>,
        ttl_seconds: u64,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        // Get timestamp before any operations
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };

        self.validate_key(key)?;
        let value_len = value.len();
        if value_len == 0 || value_len > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }

        // Check for existing record
        let existing_record = self.hash_table.read(key, |_, v| v.clone());
        if let Some(existing_record) = existing_record {
            let existing_ts = existing_record.timestamp;

            if timestamp < existing_ts {
                return Err(FeoxError::OlderTimestamp);
            }

            // Calculate TTL expiry
            let ttl_expiry = if ttl_seconds > 0 && self.enable_ttl {
                timestamp + (ttl_seconds * 1_000_000_000)
            } else {
                0
            };

            // Update existing record using the Bytes version
            return self.update_record_with_ttl_bytes(
                &existing_record,
                value,
                timestamp,
                ttl_expiry,
            );
        }

        // This point is only reached for new inserts (not updates)
        let new_size = self.calculate_record_size(key.len(), value_len);
        if !self.check_memory_limit(new_size) {
            return Err(FeoxError::OutOfMemory);
        }

        // Create new record with Bytes value
        let record = if ttl_seconds > 0 && self.enable_ttl {
            let ttl_expiry = timestamp + (ttl_seconds * 1_000_000_000);
            self.stats.keys_with_ttl.fetch_add(1, Ordering::Relaxed);
            Arc::new(Record::new_from_bytes_with_ttl(
                key.to_vec(),
                value,
                timestamp,
                ttl_expiry,
            ))
        } else {
            Arc::new(Record::new_from_bytes(key.to_vec(), value, timestamp))
        };

        let key_vec = record.key.clone();

        // Insert into hash table
        self.hash_table.upsert(key_vec.clone(), Arc::clone(&record));

        // Insert into skip list for ordered access
        self.tree.insert(key_vec, Arc::clone(&record));

        // Update statistics
        self.stats.record_count.fetch_add(1, Ordering::AcqRel);
        self.stats
            .memory_usage
            .fetch_add(new_size, Ordering::AcqRel);
        self.stats
            .record_insert(start.elapsed().as_nanos() as u64, false);

        // Only do persistence if not in memory-only mode
        if !self.memory_only {
            // Queue for persistence if write buffer exists
            if let Some(ref wb) = self.write_buffer {
                if let Err(_e) = wb.add_write(Operation::Insert, record, 0) {
                    // Don't fail the insert - data is still in memory
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

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        // Check TTL expiry if TTL is enabled
        if self.enable_ttl {
            let ttl_expiry = record.ttl_expiry.load(Ordering::Relaxed);
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

        if self.enable_caching {
            if let Some(ref cache) = self.cache {
                if let Some(value) = cache.get(key) {
                    self.stats
                        .record_get(start.elapsed().as_nanos() as u64, true);
                    return Ok(value);
                }
            }
        }

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        // Check TTL expiry if TTL is enabled
        if self.enable_ttl {
            let ttl_expiry = record.ttl_expiry.load(Ordering::Relaxed);
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

        let (value, cache_hit) = if let Some(val) = record.get_value() {
            (val, true)
        } else {
            (Bytes::from(self.load_value_from_disk(&record)?), false)
        };

        if self.enable_caching {
            if let Some(ref cache) = self.cache {
                cache.insert(key.to_vec(), value.clone());
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
        let record_pair = self.hash_table.remove(key).ok_or(FeoxError::KeyNotFound)?;
        let record = record_pair.1;

        if timestamp < record.timestamp {
            // Put it back if timestamp is older
            self.hash_table.upsert(key.to_vec(), record);
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
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        if value.is_empty() || value.len() > MAX_VALUE_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }

        Ok(())
    }

    pub(super) fn validate_key(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > MAX_KEY_SIZE {
            return Err(FeoxError::InvalidKeySize);
        }

        Ok(())
    }

    pub(super) fn check_memory_limit(&self, size: usize) -> bool {
        match self.max_memory {
            Some(limit) => {
                let current = self.stats.memory_usage.load(Ordering::Acquire);
                current + size <= limit
            }
            None => true,
        }
    }

    pub(super) fn calculate_record_size(&self, key_len: usize, value_len: usize) -> usize {
        std::mem::size_of::<Record>() + key_len + value_len
    }

    pub(super) fn get_timestamp(&self) -> u64 {
        self.get_timestamp_pub()
    }
}
