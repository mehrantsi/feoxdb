use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::record::Record;
use crate::core::ttl_sweep::TtlConfig;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

enum TtlReplacementValue {
    Resident(Bytes),
    Deferred(Arc<Record>),
}

impl FeoxStore {
    /// Insert or update a key-value pair with TTL (Time-To-Live).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store
    /// * `ttl_seconds` - Time-to-live in seconds
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::builder().enable_ttl(true).build()?;
    /// // Key expires after 60 seconds
    /// store.insert_with_ttl(b"session:123", b"data", 60)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~800ns
    /// * Persistent mode: ~1µs (buffered write)
    pub fn insert_with_ttl(&self, key: &[u8], value: &[u8], ttl_seconds: u64) -> Result<bool> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.insert_with_ttl_and_timestamp(key, value, ttl_seconds, None)
    }

    /// Insert or update a key-value pair with TTL and explicit timestamp.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store
    /// * `ttl_seconds` - Time-to-live in seconds
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
    pub fn insert_with_ttl_and_timestamp(
        &self,
        key: &[u8],
        value: &[u8],
        ttl_seconds: u64,
        timestamp: Option<u64>,
    ) -> Result<bool> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.ensure_ttl_write_supported()?;
        self.insert_with_timestamp_and_ttl_internal(key, value, timestamp, ttl_seconds)
    }

    /// Insert or update a key-value pair with TTL using zero-copy Bytes.
    ///
    /// This method avoids copying the value data by directly using the Bytes type,
    /// which provides reference-counted zero-copy semantics.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store as Bytes
    /// * `ttl_seconds` - Time-to-live in seconds
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # use bytes::Bytes;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::builder().enable_ttl(true).build()?;
    /// let data = Bytes::from_static(b"session_data");
    /// // Key expires after 60 seconds
    /// store.insert_bytes_with_ttl(b"session:123", data, 60)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Performance
    ///
    /// * Memory mode: ~800ns (avoids value copy)
    /// * Persistent mode: ~1µs (buffered write, avoids value copy)
    pub fn insert_bytes_with_ttl(
        &self,
        key: &[u8],
        value: Bytes,
        ttl_seconds: u64,
    ) -> Result<bool> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.insert_bytes_with_ttl_and_timestamp(key, value, ttl_seconds, None)
    }

    /// Insert or update a key-value pair with TTL and explicit timestamp using zero-copy Bytes.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert
    /// * `value` - The value to store as Bytes
    /// * `ttl_seconds` - Time-to-live in seconds
    /// * `timestamp` - Optional timestamp for conflict resolution. If `None`, uses current time.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
    pub fn insert_bytes_with_ttl_and_timestamp(
        &self,
        key: &[u8],
        value: Bytes,
        ttl_seconds: u64,
        timestamp: Option<u64>,
    ) -> Result<bool> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.ensure_ttl_write_supported()?;
        self.insert_bytes_with_timestamp_and_ttl_internal(key, value, timestamp, ttl_seconds)
    }

    /// Get the remaining TTL (Time-To-Live) for a key in seconds.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to check
    ///
    /// # Returns
    ///
    /// Returns `Some(seconds)` if the key has TTL set, `None` if no TTL or key not found.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// # let store = FeoxStore::builder().enable_ttl(true).build()?;
    /// store.insert_with_ttl(b"session", b"data", 3600)?;
    ///
    /// // Check remaining TTL
    /// if let Ok(Some(ttl)) = store.get_ttl(b"session") {
    ///     println!("Session expires in {} seconds", ttl);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_ttl(&self, key: &[u8]) -> Result<Option<u64>> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.validate_key(key)?;

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;
        let ttl_expiry = record.ttl_expiry.load(Ordering::Acquire);

        if ttl_expiry == 0 {
            return Ok(None); // No TTL set
        }

        let now = self.get_timestamp_pub();
        if now >= ttl_expiry {
            return Ok(Some(0)); // Already expired
        }

        // Return remaining seconds
        Ok(Some((ttl_expiry - now) / 1_000_000_000))
    }

    /// Update the TTL for an existing key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to update
    /// * `ttl_seconds` - New TTL in seconds (0 to remove TTL)
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
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
    /// # let store = FeoxStore::builder().enable_ttl(true).build()?;
    /// # store.insert(b"key", b"value")?;
    /// // Extend TTL to 1 hour
    /// store.update_ttl(b"key", 3600)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn update_ttl(&self, key: &[u8], ttl_seconds: u64) -> Result<()> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.ensure_ttl_write_supported()?;
        self.validate_key(key)?;

        let (new_record, old_record, cache_guarded) = self
            .hash_table
            .update(key, |stored_key, current| {
                let old_record = Arc::clone(current);
                let old_expiry = old_record.ttl_expiry.load(Ordering::Acquire);
                let now = self.get_timestamp_pub();
                if old_expiry > 0 && now > old_expiry {
                    return Err(FeoxError::KeyNotFound);
                }
                let timestamp = self.version_clock.next(stored_key, now).max(
                    old_record
                        .timestamp
                        .checked_add(1)
                        .ok_or(FeoxError::OlderTimestamp)?,
                );
                let expiry = ttl_expiry(now, ttl_seconds);
                let resident = old_record.get_value();
                let cache_entry = resident.is_none().then(|| {
                    self.cache
                        .as_ref()
                        .map(|cache| cache.record_entry(stored_key, &old_record))
                });
                let cache_entry = cache_entry.flatten();
                let cache_guarded = cache_entry.is_some();
                let value = Self::ttl_replacement_value(
                    &old_record,
                    resident.or_else(|| cache_entry.as_ref().and_then(|entry| entry.value())),
                );
                let new_record = match value {
                    TtlReplacementValue::Resident(value) if expiry == 0 => {
                        Arc::new(Record::new_from_bytes(stored_key.clone(), value, timestamp))
                    }
                    TtlReplacementValue::Resident(value) => {
                        Arc::new(Record::new_from_bytes_with_ttl(
                            stored_key.clone(),
                            value,
                            timestamp,
                            expiry,
                        ))
                    }
                    TtlReplacementValue::Deferred(predecessor) => Arc::new(
                        Record::new_deferred_with_ttl(&predecessor, timestamp, expiry),
                    ),
                };

                old_record.link_successor(&new_record);
                old_record.refcount.store(0, Ordering::Release);
                *current = Arc::clone(&new_record);
                if let Some(entry) = cache_entry {
                    entry.remove();
                }
                self.publish_to_tree(stored_key, Arc::clone(&new_record));
                self.note_ttl_transition(old_expiry, expiry);

                Ok((new_record, old_record, cache_guarded))
            })
            .ok_or(FeoxError::KeyNotFound)??;

        if !cache_guarded {
            self.remove_cached(key, &old_record);
        }

        if let Some(write_buffer) = self.write_buffer.as_ref() {
            write_buffer.add_replacement(new_record, old_record)?;
        }

        Ok(())
    }

    fn ttl_replacement_value(record: &Arc<Record>, value: Option<Bytes>) -> TtlReplacementValue {
        if let Some(value) = value {
            return TtlReplacementValue::Resident(value);
        }

        let predecessor = Arc::clone(record);
        #[cfg(test)]
        crate::test_hooks::pause_at(crate::test_hooks::AFTER_TTL_DEFERRED_SOURCE);
        TtlReplacementValue::Deferred(predecessor)
    }

    pub(super) fn note_ttl_transition(&self, previous: u64, current: u64) {
        match (previous > 0, current > 0) {
            (false, true) => {
                self.stats.keys_with_ttl.fetch_add(1, Ordering::Relaxed);
            }
            (true, false) => {
                let _ = self.stats.keys_with_ttl.fetch_update(
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                    |count| Some(count.saturating_sub(1)),
                );
            }
            _ => {}
        }
    }

    pub(super) fn ensure_ttl_write_supported(&self) -> Result<()> {
        if !self.memory_only && self.format_version == 1 {
            return Err(FeoxError::Unsupported);
        }
        Ok(())
    }

    /// Remove TTL from a key, making it persistent.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to persist
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful.
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
    /// # let store = FeoxStore::builder().enable_ttl(true).build()?;
    /// # store.insert_with_ttl(b"temp", b"data", 60)?;
    /// // Remove TTL, make permanent
    /// store.persist(b"temp")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn persist(&self, key: &[u8]) -> Result<()> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.update_ttl(key, 0)
    }

    /// Start the TTL sweeper if configured
    /// This must be called with an `Arc<Self>` after construction
    pub fn start_ttl_sweeper(self: &Arc<Self>, config: Option<TtlConfig>) {
        // Only start TTL sweeper if TTL is enabled
        if !self.enable_ttl {
            return;
        }

        let ttl_config = config.unwrap_or_else(|| {
            if self.memory_only {
                TtlConfig::default_memory()
            } else {
                TtlConfig::default_persistent()
            }
        });

        if ttl_config.enabled {
            let weak_store = Arc::downgrade(self);
            let mut sweeper = crate::core::ttl_sweep::TtlSweeper::new(weak_store, ttl_config);
            sweeper.start();

            // Store the sweeper
            *self.ttl_sweeper.write() = Some(sweeper);
        }
    }

    /// Get current timestamp (public for TTL cleaner)
    pub fn get_timestamp_pub(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}

#[inline]
fn ttl_expiry(timestamp: u64, ttl_seconds: u64) -> u64 {
    if ttl_seconds == 0 {
        0
    } else {
        timestamp.saturating_add(ttl_seconds.saturating_mul(1_000_000_000))
    }
}
