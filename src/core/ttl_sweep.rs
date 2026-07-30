use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

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
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
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
    let mut expired = 0;
    let mut rng = rand::rng();

    // Get access to the hash table
    let hash_table = store.get_hash_table();

    let candidates = sample_ttl_entries(hash_table, config.sample_size, &mut rng);
    let sampled = candidates.len() as u64;

    for (key, record) in candidates {
        let ttl_expiry = record.ttl_expiry.load(Ordering::Relaxed);

        if ttl_expiry > 0 && ttl_expiry < now {
            #[cfg(test)]
            crate::test_hooks::pause_at(crate::test_hooks::TTL_AFTER_EXPIRED_SAMPLE);

            let old_value_len = record.value_len;
            let record_size = record.calculate_size();
            let retired = match hash_table.entry(key.clone()) {
                scc::hash_map::Entry::Occupied(entry) => {
                    let current_expiry = record.ttl_expiry.load(Ordering::Acquire);
                    if Arc::ptr_eq(entry.get(), &record)
                        && current_expiry > 0
                        && current_expiry < now
                    {
                        record.retired_at.store(now, Ordering::Release);
                        record.refcount.store(0, Ordering::Release);
                        store.remove_from_tree(&key);
                        let _ = entry.remove();
                        true
                    } else {
                        false
                    }
                }
                scc::hash_map::Entry::Vacant(_) => false,
            };

            if retired {
                store.remove_cached(&key, &record);
                store.note_expired_record(record_size);
                expired += 1;

                if let Some(wb) = store.get_write_buffer() {
                    let _ = wb.add_write(Operation::Delete, record, old_value_len);
                }
            }
        }
    }

    (sampled, expired)
}

#[cfg(test)]
pub(crate) fn sample_and_expire_for_test(store: &Arc<FeoxStore>) -> (u64, u64) {
    sample_and_expire_batch(
        store,
        &TtlConfig {
            sample_size: 1,
            ..TtlConfig::default()
        },
    )
}

fn sample_ttl_entries<R: Rng + ?Sized>(
    hash_table: &scc::HashMap<Vec<u8>, Arc<crate::core::record::Record>, ahash::RandomState>,
    sample_size: usize,
    rng: &mut R,
) -> Vec<(Vec<u8>, Arc<crate::core::record::Record>)> {
    if sample_size == 0 {
        return Vec::new();
    }

    let mut candidates = Vec::with_capacity(sample_size.min(hash_table.len()));
    let mut seen = 0usize;
    hash_table.scan(|key: &Vec<u8>, value: &Arc<crate::core::record::Record>| {
        if value.ttl_expiry.load(Ordering::Relaxed) > 0 {
            seen += 1;
            if candidates.len() < sample_size {
                candidates.push((key.clone(), Arc::clone(value)));
            } else {
                let index = rng.random_range(0..seen);
                if index < sample_size {
                    candidates[index] = (key.clone(), Arc::clone(value));
                }
            }
        }
    });

    candidates
}

#[cfg(test)]
pub(crate) fn sample_ttl_keys_for_test(store: &FeoxStore, sample_size: usize) -> Vec<Vec<u8>> {
    let mut rng = rand::rng();
    sample_ttl_entries(store.get_hash_table(), sample_size, &mut rng)
        .into_iter()
        .map(|(key, _)| key)
        .collect()
}

#[cfg(test)]
#[path = "../tests/ttl_sweep_safety_tests.rs"]
mod tests;
