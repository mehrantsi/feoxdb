use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use rand::rngs::ThreadRng;
use rand::Rng;

use crate::constants::Operation;
use crate::core::store::FeoxStore;

/// Configuration for TTL cleaner background thread
#[derive(Clone, Debug)]
pub struct TtlConfig {
    /// Number of keys to sample per batch
    pub sample_size: usize,
    /// Continue sampling if expiry rate exceeds this threshold (0.0-1.0)
    pub expiry_threshold: f32,
    /// Maximum iterations per cleaning run
    pub max_iterations: usize,
    /// Maximum time to spend per cleaning run
    pub max_time_per_run: Duration,
    /// Sleep interval between cleaning runs
    pub sleep_interval: Duration,
    /// Whether TTL cleaner is enabled
    pub enabled: bool,
}

impl Default for TtlConfig {
    fn default() -> Self {
        Self {
            sample_size: 100,
            expiry_threshold: 0.25,
            max_iterations: 16,
            max_time_per_run: Duration::from_millis(1),
            sleep_interval: Duration::from_millis(1000),
            enabled: false,
        }
    }
}

impl TtlConfig {
    /// Create a default configuration for persistent stores
    pub fn default_persistent() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Create a default configuration for memory-only stores
    pub fn default_memory() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }
}

/// Background thread that periodically sweeps expired TTL keys
pub struct TtlSweeper {
    /// Weak reference to the store to avoid circular references
    store: Weak<FeoxStore>,
    /// Configuration
    config: TtlConfig,
    /// Shutdown flag
    shutdown: Arc<AtomicBool>,
    /// Thread handle
    handle: Option<JoinHandle<()>>,
    /// Statistics
    stats: TtlSweeperStats,
}

/// Statistics for TTL sweeper operations
pub struct TtlSweeperStats {
    /// Total keys sampled
    pub total_sampled: Arc<AtomicU64>,
    /// Total keys expired
    pub total_expired: Arc<AtomicU64>,
    /// Total cleaning runs
    pub total_runs: Arc<AtomicU64>,
    /// Last run timestamp (nanoseconds)
    pub last_run: Arc<AtomicU64>,
}

impl TtlSweeperStats {
    fn new() -> Self {
        Self {
            total_sampled: Arc::new(AtomicU64::new(0)),
            total_expired: Arc::new(AtomicU64::new(0)),
            total_runs: Arc::new(AtomicU64::new(0)),
            last_run: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TtlSweeper {
    /// Create a new TTL sweeper
    pub fn new(store: Weak<FeoxStore>, config: TtlConfig) -> Self {
        Self {
            store,
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            stats: TtlSweeperStats::new(),
        }
    }

    /// Start the background sweeper thread
    pub fn start(&mut self) {
        if !self.config.enabled {
            return;
        }

        let store = self.store.clone();
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        let stats = TtlSweeperStats {
            total_sampled: self.stats.total_sampled.clone(),
            total_expired: self.stats.total_expired.clone(),
            total_runs: self.stats.total_runs.clone(),
            last_run: self.stats.last_run.clone(),
        };

        let handle = thread::spawn(move || {
            run_sweeper_loop(store, config, shutdown, stats);
        });

        self.handle = Some(handle);
    }

    /// Stop the background sweeper thread
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Get sweeper statistics
    pub fn stats(&self) -> SweeperSnapshot {
        SweeperSnapshot {
            total_sampled: self.stats.total_sampled.load(Ordering::Relaxed),
            total_expired: self.stats.total_expired.load(Ordering::Relaxed),
            total_runs: self.stats.total_runs.load(Ordering::Relaxed),
            last_run: self.stats.last_run.load(Ordering::Relaxed),
        }
    }
}

impl Drop for TtlSweeper {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Snapshot of sweeper statistics
#[derive(Debug, Clone)]
pub struct SweeperSnapshot {
    pub total_sampled: u64,
    pub total_expired: u64,
    pub total_runs: u64,
    pub last_run: u64,
}

/// Main sweeper loop that runs in the background thread
fn run_sweeper_loop(
    store: Weak<FeoxStore>,
    config: TtlConfig,
    shutdown: Arc<AtomicBool>,
    stats: TtlSweeperStats,
) {
    while !shutdown.load(Ordering::Acquire) {
        // Sleep between runs
        thread::sleep(config.sleep_interval);

        // Try to get strong reference to store
        let Some(store) = store.upgrade() else {
            // Store has been dropped, exit
            break;
        };

        // Perform sweeping run
        let start = Instant::now();
        let mut iterations = 0;
        let mut total_sampled = 0;
        let mut total_expired = 0;

        loop {
            // Sample and expire a batch
            let (sampled, expired) = sample_and_expire_batch(&store, &config);
            total_sampled += sampled;
            total_expired += expired;
            iterations += 1;

            // Calculate expiry rate
            let expiry_rate = if sampled > 0 {
                expired as f32 / sampled as f32
            } else {
                0.0
            };

            // Check stop conditions
            if expiry_rate < config.expiry_threshold {
                break; // Few expired keys, we're done
            }
            if iterations >= config.max_iterations {
                break; // Bounded iterations
            }
            if start.elapsed() > config.max_time_per_run {
                break; // Bounded time
            }
        }

        // Update statistics
        if total_sampled > 0 {
            stats
                .total_sampled
                .fetch_add(total_sampled, Ordering::Relaxed);
            stats
                .total_expired
                .fetch_add(total_expired, Ordering::Relaxed);
            stats.total_runs.fetch_add(1, Ordering::Relaxed);
            stats.last_run.store(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64,
                Ordering::Relaxed,
            );
        }

        // Check shutdown flag again
        if shutdown.load(Ordering::Acquire) {
            break;
        }
    }
}

/// Sample keys and expire those that have exceeded their TTL
fn sample_and_expire_batch(store: &Arc<FeoxStore>, config: &TtlConfig) -> (u64, u64) {
    let now = store.get_timestamp_pub();
    let mut sampled = 0;
    let mut expired = 0;
    let mut rng = rand::rng();

    // Get access to the hash table
    let hash_table = store.get_hash_table();

    // Sample random entries directly from the hash table
    for _ in 0..config.sample_size {
        // Try to get a random entry with TTL
        if let Some((key, record)) = get_random_ttl_entry(hash_table, &mut rng) {
            sampled += 1;

            let ttl_expiry = record.ttl_expiry.load(Ordering::Relaxed);

            // Check if expired
            if ttl_expiry > 0 && ttl_expiry < now {
                // Remove expired entry
                hash_table.remove(&key);
                store.remove_from_tree(&key);

                expired += 1;

                // Queue disk cleanup if needed
                if record.sector.load(Ordering::Relaxed) > 0 {
                    // Add to write buffer for disk cleanup
                    if let Some(wb) = store.get_write_buffer() {
                        let _ = wb.add_write(Operation::Delete, record, 0);
                    }
                }
            }
        }
    }

    (sampled, expired)
}

/// Get a random entry with TTL using sampling
fn get_random_ttl_entry(
    hash_table: &scc::HashMap<Vec<u8>, Arc<crate::core::record::Record>>,
    rng: &mut ThreadRng,
) -> Option<(Vec<u8>, Arc<crate::core::record::Record>)> {
    // Sample up to 100 entries and pick one with TTL
    let mut candidates = Vec::new();
    let mut count = 0;

    hash_table.scan(|key: &Vec<u8>, value: &Arc<crate::core::record::Record>| {
        if count >= 100 {
            return; // Stop iteration
        }
        count += 1;

        if value.ttl_expiry.load(Ordering::Relaxed) > 0 {
            candidates.push((key.clone(), value.clone()));
        }
    });

    if candidates.is_empty() {
        None
    } else {
        // Pick a random candidate
        let idx = rng.random_range(0..candidates.len());
        Some(candidates.into_iter().nth(idx).unwrap())
    }
}
