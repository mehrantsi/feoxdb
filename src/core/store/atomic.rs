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
}
