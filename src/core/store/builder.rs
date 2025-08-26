use std::time::Duration;

use crate::constants::*;
use crate::core::ttl_sweep::TtlConfig;
use crate::error::Result;

use super::FeoxStore;

/// Configuration options for FeoxStore.
///
/// Use `StoreBuilder` for a more ergonomic way to configure the store.
pub struct StoreConfig {
    pub hash_bits: u32,
    pub memory_only: bool,
    pub enable_caching: bool,
    pub device_path: Option<String>,
    pub max_memory: Option<usize>,
    pub enable_ttl: bool,
    pub ttl_config: Option<TtlConfig>,
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
///     .enable_ttl(true)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub struct StoreBuilder {
    hash_bits: u32,
    device_path: Option<String>,
    max_memory: Option<usize>,
    enable_caching: Option<bool>,
    enable_ttl: bool,
    ttl_config: Option<TtlConfig>,
}

impl StoreBuilder {
    pub fn new() -> Self {
        Self {
            hash_bits: DEFAULT_HASH_BITS,
            device_path: None,
            max_memory: Some(DEFAULT_MAX_MEMORY),
            enable_caching: None, // Disable caching for memory-only mode
            enable_ttl: false,
            ttl_config: None,
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

    /// Enable or disable TTL (Time-To-Live) functionality.
    ///
    /// When disabled (default), TTL operations will return errors and no background cleaner runs.
    /// When enabled, keys can have expiry times and a background cleaner removes expired keys.
    /// Default: false (disabled for optimal performance)
    pub fn enable_ttl(mut self, enable: bool) -> Self {
        self.enable_ttl = enable;
        if enable {
            let mut config = self.ttl_config.unwrap_or_default();
            config.enabled = true;
            self.ttl_config = Some(config);
        }
        self
    }

    /// Enable or disable TTL sweeper.
    ///
    /// When enabled, a background thread periodically removes expired keys.
    /// Note: This method is deprecated in favor of enable_ttl().
    pub fn enable_ttl_cleaner(mut self, enable: bool) -> Self {
        let mut config = self.ttl_config.unwrap_or_default();
        config.enabled = enable;
        self.ttl_config = Some(config);
        self.enable_ttl = enable; // Also enable TTL when cleaner is enabled
        self
    }

    /// Configure TTL sweeper with custom parameters.
    ///
    /// # Arguments
    ///
    /// * `sample_size` - Keys to check per batch
    /// * `threshold` - Continue if >threshold expired (0.0-1.0)
    /// * `max_time_ms` - Max milliseconds per cleaning run
    /// * `interval_ms` - Sleep between runs
    pub fn ttl_sweeper_config(
        mut self,
        sample_size: usize,
        threshold: f32,
        max_time_ms: u64,
        interval_ms: u64,
    ) -> Self {
        self.ttl_config = Some(TtlConfig {
            sample_size,
            expiry_threshold: threshold,
            max_iterations: 16,
            max_time_per_run: Duration::from_millis(max_time_ms),
            sleep_interval: Duration::from_millis(interval_ms),
            enabled: true,
        });
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
            enable_ttl: self.enable_ttl,
            ttl_config: self.ttl_config,
        };

        FeoxStore::with_config(config)
    }
}

impl Default for StoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}
