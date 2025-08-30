use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::core::ttl_sweep::TtlConfig;
use crate::error::{FeoxError, Result};

use super::FeoxStore;

impl FeoxStore {
    /// Insert or update a key-value pair with TTL (Time-To-Live).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (max 65KB)
    /// * `value` - The value to store (max 4GB)
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
    pub fn insert_with_ttl(&self, key: &[u8], value: &[u8], ttl_seconds: u64) -> Result<()> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        self.insert_with_ttl_and_timestamp(key, value, ttl_seconds, None)
    }

    /// Insert or update a key-value pair with TTL and explicit timestamp.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert (max 65KB)
    /// * `value` - The value to store (max 4GB)
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
    ) -> Result<()> {
        if !self.enable_ttl {
            return Err(FeoxError::TtlNotEnabled);
        }
        let timestamp = match timestamp {
            Some(0) | None => self.get_timestamp(),
            Some(ts) => ts,
        };

        // Calculate expiry timestamp
        let ttl_expiry = if ttl_seconds > 0 {
            timestamp + (ttl_seconds * 1_000_000_000) // Convert seconds to nanoseconds
        } else {
            0
        };

        self.insert_with_timestamp_and_ttl_internal(key, value, Some(timestamp), ttl_expiry)
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

        let now = self.get_timestamp();
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
        self.validate_key(key)?;

        let record = self
            .hash_table
            .read(key, |_, v| v.clone())
            .ok_or(FeoxError::KeyNotFound)?;

        let new_expiry = if ttl_seconds > 0 {
            self.get_timestamp() + (ttl_seconds * 1_000_000_000)
        } else {
            0
        };

        record.ttl_expiry.store(new_expiry, Ordering::Release);
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
