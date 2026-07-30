use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};

/// Central statistics hub for FeoxStore
#[derive(Debug)]
pub struct Statistics {
    // Store metrics
    pub record_count: AtomicU32,
    pub memory_usage: AtomicUsize,
    pub disk_usage: AtomicU64,

    // Operation counters
    pub total_gets: AtomicU64,
    pub total_inserts: AtomicU64,
    pub total_updates: AtomicU64,
    pub total_deletes: AtomicU64,
    pub total_range_queries: AtomicU64,

    // Operation latencies (in nanoseconds)
    pub get_latency_ns: AtomicU64,
    pub insert_latency_ns: AtomicU64,
    pub delete_latency_ns: AtomicU64,

    // Cache metrics
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub cache_memory: AtomicUsize,

    // Write buffer metrics
    pub writes_buffered: AtomicU64,
    pub writes_flushed: AtomicU64,
    pub write_failures: AtomicU64,
    pub sector_release_failures: AtomicU64,
    pub write_entries_stuck: AtomicU64,
    pub flush_count: AtomicU64,

    // Disk I/O metrics
    pub disk_reads: AtomicU64,
    pub disk_writes: AtomicU64,
    pub disk_bytes_read: AtomicU64,
    pub disk_bytes_written: AtomicU64,

    // Error counters
    pub key_not_found_errors: AtomicU64,
    pub out_of_memory_errors: AtomicU64,
    pub io_errors: AtomicU64,

    // TTL metrics
    pub ttl_expired_lazy: AtomicU64,   // Keys expired during GET
    pub ttl_expired_active: AtomicU64, // Keys expired by cleaner
    pub ttl_cleaner_runs: AtomicU64,   // Cleaner iterations
    pub keys_with_ttl: AtomicU64,      // Approximate count
}

impl Statistics {
    pub fn new() -> Self {
        Self {
            // Store metrics
            record_count: AtomicU32::new(0),
            memory_usage: AtomicUsize::new(0),
            disk_usage: AtomicU64::new(0),

            // Operation counters
            total_gets: AtomicU64::new(0),
            total_inserts: AtomicU64::new(0),
            total_updates: AtomicU64::new(0),
            total_deletes: AtomicU64::new(0),
            total_range_queries: AtomicU64::new(0),

            // Operation latencies
            get_latency_ns: AtomicU64::new(0),
            insert_latency_ns: AtomicU64::new(0),
            delete_latency_ns: AtomicU64::new(0),

            // Cache metrics
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            cache_memory: AtomicUsize::new(0),

            // Write buffer metrics
            writes_buffered: AtomicU64::new(0),
            writes_flushed: AtomicU64::new(0),
            write_failures: AtomicU64::new(0),
            sector_release_failures: AtomicU64::new(0),
            write_entries_stuck: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),

            // Disk I/O metrics
            disk_reads: AtomicU64::new(0),
            disk_writes: AtomicU64::new(0),
            disk_bytes_read: AtomicU64::new(0),
            disk_bytes_written: AtomicU64::new(0),

            // Error counters
            key_not_found_errors: AtomicU64::new(0),
            out_of_memory_errors: AtomicU64::new(0),
            io_errors: AtomicU64::new(0),

            // TTL metrics
            ttl_expired_lazy: AtomicU64::new(0),
            ttl_expired_active: AtomicU64::new(0),
            ttl_cleaner_runs: AtomicU64::new(0),
            keys_with_ttl: AtomicU64::new(0),
        }
    }

    /// Record a get operation
    pub fn record_get(&self, latency_ns: u64, hit: bool) {
        self.total_gets.fetch_add(1, Ordering::Relaxed);
        self.get_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);

        if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an insert operation
    pub fn record_insert(&self, latency_ns: u64, is_update: bool) {
        if is_update {
            self.total_updates.fetch_add(1, Ordering::Relaxed);
        } else {
            self.total_inserts.fetch_add(1, Ordering::Relaxed);
        }
        self.insert_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record a delete operation
    pub fn record_delete(&self, latency_ns: u64) {
        self.total_deletes.fetch_add(1, Ordering::Relaxed);
        self.delete_latency_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
    }

    /// Record a range query
    pub fn record_range_query(&self) {
        self.total_range_queries.fetch_add(1, Ordering::Relaxed);
    }

    /// Record cache eviction
    pub fn record_eviction(&self, count: u64) {
        self.cache_evictions.fetch_add(count, Ordering::Relaxed);
    }

    /// Record write buffer operation
    pub fn record_write_buffered(&self) {
        self.record_writes_buffered(1);
    }

    pub(crate) fn record_writes_buffered(&self, count: u64) {
        self.writes_buffered.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_write_flushed(&self, count: u64) {
        self.writes_flushed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_write_failed(&self) {
        self.write_failures.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_write_entry_stuck(&self) {
        self.write_entries_stuck.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_sector_release_failure(&self) {
        self.sector_release_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record disk I/O
    pub fn record_disk_read(&self, bytes: u64) {
        self.disk_reads.fetch_add(1, Ordering::Relaxed);
        self.disk_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_disk_write(&self, bytes: u64) {
        self.disk_writes.fetch_add(1, Ordering::Relaxed);
        self.disk_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record errors
    pub fn record_error(&self, error: &crate::error::FeoxError) {
        use crate::error::FeoxError;
        match error {
            FeoxError::KeyNotFound => {
                self.key_not_found_errors.fetch_add(1, Ordering::Relaxed);
            }
            FeoxError::OutOfMemory => {
                self.out_of_memory_errors.fetch_add(1, Ordering::Relaxed);
            }
            FeoxError::IoError(_) | FeoxError::IndeterminateWrite(_) => {
                self.io_errors.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Get a snapshot of current statistics
    pub fn snapshot(&self) -> StatsSnapshot {
        let total_gets = self.total_gets.load(Ordering::Relaxed);
        let total_inserts = self.total_inserts.load(Ordering::Relaxed);
        let total_updates = self.total_updates.load(Ordering::Relaxed);
        let total_deletes = self.total_deletes.load(Ordering::Relaxed);
        let cache_hits = self.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.cache_misses.load(Ordering::Relaxed);
        let total_ops = total_gets
            .saturating_add(total_inserts)
            .saturating_add(total_updates)
            .saturating_add(total_deletes);
        let avg_get_latency = self
            .get_latency_ns
            .load(Ordering::Relaxed)
            .checked_div(total_gets)
            .unwrap_or(0);
        let avg_insert_latency = self
            .insert_latency_ns
            .load(Ordering::Relaxed)
            .checked_div(total_inserts.saturating_add(total_updates))
            .unwrap_or(0);
        let avg_delete_latency = self
            .delete_latency_ns
            .load(Ordering::Relaxed)
            .checked_div(total_deletes)
            .unwrap_or(0);
        let cache_hit_rate = match cache_hits.saturating_add(cache_misses) {
            0 => 0.0,
            total => (cache_hits as f64 / total as f64) * 100.0,
        };

        StatsSnapshot {
            record_count: self.record_count.load(Ordering::Relaxed),
            memory_usage: self.memory_usage.load(Ordering::Relaxed),
            total_operations: total_ops,
            total_gets,
            total_inserts,
            total_updates,
            total_deletes,
            total_range_queries: self.total_range_queries.load(Ordering::Relaxed),
            avg_get_latency_ns: avg_get_latency,
            avg_insert_latency_ns: avg_insert_latency,
            avg_delete_latency_ns: avg_delete_latency,
            cache_hits,
            cache_misses,
            cache_hit_rate,
            cache_evictions: self.cache_evictions.load(Ordering::Relaxed),
            cache_memory: self.cache_memory.load(Ordering::Relaxed),
            writes_buffered: self.writes_buffered.load(Ordering::Relaxed),
            writes_flushed: self.writes_flushed.load(Ordering::Relaxed),
            write_failures: self.write_failures.load(Ordering::Relaxed),
            sector_release_failures: self.sector_release_failures.load(Ordering::Relaxed),
            write_entries_stuck: self.write_entries_stuck.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            disk_reads: self.disk_reads.load(Ordering::Relaxed),
            disk_writes: self.disk_writes.load(Ordering::Relaxed),
            disk_bytes_read: self.disk_bytes_read.load(Ordering::Relaxed),
            disk_bytes_written: self.disk_bytes_written.load(Ordering::Relaxed),
            key_not_found_errors: self.key_not_found_errors.load(Ordering::Relaxed),
            out_of_memory_errors: self.out_of_memory_errors.load(Ordering::Relaxed),
            io_errors: self.io_errors.load(Ordering::Relaxed),
            ttl_expired_lazy: self.ttl_expired_lazy.load(Ordering::Relaxed),
            ttl_expired_active: self.ttl_expired_active.load(Ordering::Relaxed),
            ttl_cleaner_runs: self.ttl_cleaner_runs.load(Ordering::Relaxed),
            keys_with_ttl: self.keys_with_ttl.load(Ordering::Relaxed),
        }
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.total_gets.store(0, Ordering::Relaxed);
        self.total_inserts.store(0, Ordering::Relaxed);
        self.total_updates.store(0, Ordering::Relaxed);
        self.total_deletes.store(0, Ordering::Relaxed);
        self.total_range_queries.store(0, Ordering::Relaxed);
        self.get_latency_ns.store(0, Ordering::Relaxed);
        self.insert_latency_ns.store(0, Ordering::Relaxed);
        self.delete_latency_ns.store(0, Ordering::Relaxed);
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.cache_evictions.store(0, Ordering::Relaxed);
        self.writes_buffered.store(0, Ordering::Relaxed);
        self.writes_flushed.store(0, Ordering::Relaxed);
        self.write_failures.store(0, Ordering::Relaxed);
        self.sector_release_failures.store(0, Ordering::Relaxed);
        self.write_entries_stuck.store(0, Ordering::Relaxed);
        self.flush_count.store(0, Ordering::Relaxed);
        self.disk_reads.store(0, Ordering::Relaxed);
        self.disk_writes.store(0, Ordering::Relaxed);
        self.disk_bytes_read.store(0, Ordering::Relaxed);
        self.disk_bytes_written.store(0, Ordering::Relaxed);
        self.key_not_found_errors.store(0, Ordering::Relaxed);
        self.out_of_memory_errors.store(0, Ordering::Relaxed);
        self.io_errors.store(0, Ordering::Relaxed);
        self.ttl_expired_lazy.store(0, Ordering::Relaxed);
        self.ttl_expired_active.store(0, Ordering::Relaxed);
        self.ttl_cleaner_runs.store(0, Ordering::Relaxed);
    }
}

impl Default for Statistics {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of statistics at a point in time
#[derive(Debug, Clone)]
pub struct StatsSnapshot {
    // Store metrics
    pub record_count: u32,
    pub memory_usage: usize,

    // Operations
    pub total_operations: u64,
    pub total_gets: u64,
    pub total_inserts: u64,
    pub total_updates: u64,
    pub total_deletes: u64,
    pub total_range_queries: u64,

    // Latencies (nanoseconds)
    pub avg_get_latency_ns: u64,
    pub avg_insert_latency_ns: u64,
    pub avg_delete_latency_ns: u64,

    // Cache
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
    pub cache_evictions: u64,
    pub cache_memory: usize,

    // Write buffer
    pub writes_buffered: u64,
    pub writes_flushed: u64,
    pub write_failures: u64,
    pub sector_release_failures: u64,
    pub write_entries_stuck: u64,
    pub flush_count: u64,

    // Disk I/O
    pub disk_reads: u64,
    pub disk_writes: u64,
    pub disk_bytes_read: u64,
    pub disk_bytes_written: u64,

    // Errors
    pub key_not_found_errors: u64,
    pub out_of_memory_errors: u64,
    pub io_errors: u64,

    // TTL
    pub ttl_expired_lazy: u64,
    pub ttl_expired_active: u64,
    pub ttl_cleaner_runs: u64,
    pub keys_with_ttl: u64,
}

impl StatsSnapshot {
    /// Format statistics as a human-readable string
    pub fn format(&self) -> String {
        format!(
            "=== FeOxDB Statistics ===\n\
            Store:\n\
            - Records: {}\n\
            - Memory: {:.2} MB\n\n\
            Operations:\n\
            - Total: {}\n\
            - Gets: {} (avg latency: {:.2}μs)\n\
            - Inserts: {} (avg latency: {:.2}μs)\n\
            - Updates: {}\n\
            - Deletes: {} (avg latency: {:.2}μs)\n\
            - Range Queries: {}\n\n\
            Cache:\n\
            - Hit Rate: {:.1}%\n\
            - Hits: {}\n\
            - Misses: {}\n\
            - Evictions: {}\n\
            - Memory: {:.2} MB\n\n\
            Write Buffer:\n\
            - Buffered: {}\n\
            - Flushed: {}\n\
            - Failures: {}\n\
            - Flush Count: {}\n\n\
            Disk I/O:\n\
            - Reads: {} ({:.2} MB)\n\
            - Writes: {} ({:.2} MB)\n\n\
            Errors:\n\
            - Key Not Found: {}\n\
            - Out of Memory: {}\n\
            - I/O Errors: {}",
            self.record_count,
            self.memory_usage as f64 / 1_048_576.0,
            self.total_operations,
            self.total_gets,
            self.avg_get_latency_ns as f64 / 1000.0,
            self.total_inserts,
            self.avg_insert_latency_ns as f64 / 1000.0,
            self.total_updates,
            self.total_deletes,
            self.avg_delete_latency_ns as f64 / 1000.0,
            self.total_range_queries,
            self.cache_hit_rate,
            self.cache_hits,
            self.cache_misses,
            self.cache_evictions,
            self.cache_memory as f64 / 1_048_576.0,
            self.writes_buffered,
            self.writes_flushed,
            self.write_failures,
            self.flush_count,
            self.disk_reads,
            self.disk_bytes_read as f64 / 1_048_576.0,
            self.disk_writes,
            self.disk_bytes_written as f64 / 1_048_576.0,
            self.key_not_found_errors,
            self.out_of_memory_errors,
            self.io_errors
        )
    }
}
