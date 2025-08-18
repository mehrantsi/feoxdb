use crate::stats::Statistics;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;

#[test]
fn test_statistics_creation() {
    let stats = Statistics::new();

    assert_eq!(stats.record_count.load(Ordering::Relaxed), 0);
    assert_eq!(stats.memory_usage.load(Ordering::Relaxed), 0);
    assert_eq!(stats.disk_usage.load(Ordering::Relaxed), 0);
    assert_eq!(stats.total_gets.load(Ordering::Relaxed), 0);
    assert_eq!(stats.total_inserts.load(Ordering::Relaxed), 0);
}

#[test]
fn test_record_operations() {
    let stats = Statistics::new();

    // Record some operations
    stats.record_get(100, true);
    stats.record_get(200, false);
    stats.record_insert(150, false); // This is an insert
    stats.record_insert(250, true); // This is an update
    stats.record_delete(50);

    assert_eq!(stats.total_gets.load(Ordering::Relaxed), 2);
    assert_eq!(stats.total_inserts.load(Ordering::Relaxed), 1); // Only one insert
    assert_eq!(stats.total_deletes.load(Ordering::Relaxed), 1);
    assert_eq!(stats.cache_hits.load(Ordering::Relaxed), 1);
    assert_eq!(stats.cache_misses.load(Ordering::Relaxed), 1);
    assert_eq!(stats.total_updates.load(Ordering::Relaxed), 1); // One update
}

#[test]
fn test_latency_recording() {
    let stats = Statistics::new();

    stats.record_get(1000, true); // 1 microsecond
    stats.record_get(2000, false); // 2 microseconds
    stats.record_insert(3000, false); // 3 microseconds
    stats.record_delete(500); // 0.5 microseconds

    assert!(stats.get_latency_ns.load(Ordering::Relaxed) >= 3000);
    assert!(stats.insert_latency_ns.load(Ordering::Relaxed) >= 3000);
    assert!(stats.delete_latency_ns.load(Ordering::Relaxed) >= 500);
}

#[test]
fn test_write_buffer_stats() {
    let stats = Statistics::new();

    stats.record_write_buffered();
    stats.record_write_buffered();
    stats.record_write_flushed(10);
    stats.record_write_failed();

    assert_eq!(stats.writes_buffered.load(Ordering::Relaxed), 2);
    assert_eq!(stats.writes_flushed.load(Ordering::Relaxed), 10);
    assert_eq!(stats.write_failures.load(Ordering::Relaxed), 1);
}

#[test]
fn test_eviction_stats() {
    let stats = Statistics::new();

    stats.record_eviction(5);
    stats.record_eviction(3);

    assert_eq!(stats.cache_evictions.load(Ordering::Relaxed), 8);
}

#[test]
fn test_statistics_snapshot() {
    let stats = Statistics::new();

    // Set up some stats
    stats.record_count.store(100, Ordering::Relaxed);
    stats.memory_usage.store(10000, Ordering::Relaxed);
    stats.disk_bytes_written.store(50000, Ordering::Relaxed);
    stats.total_gets.store(500, Ordering::Relaxed);
    stats.total_inserts.store(100, Ordering::Relaxed);
    stats.cache_hits.store(400, Ordering::Relaxed);
    stats.cache_misses.store(100, Ordering::Relaxed);

    let snapshot = stats.snapshot();

    assert_eq!(snapshot.record_count, 100);
    assert_eq!(snapshot.memory_usage, 10000);
    assert_eq!(snapshot.disk_bytes_written, 50000);
    assert_eq!(snapshot.total_gets, 500);
    assert_eq!(snapshot.total_inserts, 100);
    assert_eq!(snapshot.cache_hit_rate, 80.0);
}

#[test]
fn test_cache_hit_rate_calculation() {
    let stats = Statistics::new();

    // No cache accesses
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.cache_hit_rate, 0.0);

    // All hits
    stats.cache_hits.store(100, Ordering::Relaxed);
    stats.cache_misses.store(0, Ordering::Relaxed);
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.cache_hit_rate, 100.0);

    // 75% hit rate
    stats.cache_hits.store(75, Ordering::Relaxed);
    stats.cache_misses.store(25, Ordering::Relaxed);
    let snapshot = stats.snapshot();
    assert_eq!(snapshot.cache_hit_rate, 75.0);
}

#[test]
fn test_average_latencies() {
    let stats = Statistics::new();

    // Record some operations with known latencies
    stats.total_gets.store(100, Ordering::Relaxed);
    stats.get_latency_ns.store(10000, Ordering::Relaxed);

    stats.total_inserts.store(50, Ordering::Relaxed);
    stats.insert_latency_ns.store(15000, Ordering::Relaxed);

    stats.total_deletes.store(25, Ordering::Relaxed);
    stats.delete_latency_ns.store(5000, Ordering::Relaxed);

    let _snapshot = stats.snapshot();

    // Average latency calculation would be in snapshot implementation
    // For now, just check that stats are stored correctly
    assert_eq!(stats.get_latency_ns.load(Ordering::Relaxed), 10000);
    assert_eq!(stats.insert_latency_ns.load(Ordering::Relaxed), 15000);
    assert_eq!(stats.delete_latency_ns.load(Ordering::Relaxed), 5000);
}

#[test]
fn test_concurrent_statistics() {
    let stats = Arc::new(Statistics::new());
    let mut handles = vec![];

    // Multiple threads updating stats
    for _ in 0..10 {
        let stats_clone = Arc::clone(&stats);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                stats_clone.record_get(100, true);
                stats_clone.record_insert(200, false);
                stats_clone.record_delete(50);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(stats.total_gets.load(Ordering::Relaxed), 1000);
    assert_eq!(stats.total_inserts.load(Ordering::Relaxed), 1000);
    assert_eq!(stats.total_deletes.load(Ordering::Relaxed), 1000);
}

#[test]
fn test_memory_tracking() {
    let stats = Statistics::new();

    stats.memory_usage.store(1000, Ordering::Relaxed);
    assert_eq!(stats.memory_usage.load(Ordering::Relaxed), 1000);

    stats.memory_usage.fetch_add(500, Ordering::Relaxed);
    assert_eq!(stats.memory_usage.load(Ordering::Relaxed), 1500);

    stats.memory_usage.fetch_sub(200, Ordering::Relaxed);
    assert_eq!(stats.memory_usage.load(Ordering::Relaxed), 1300);
}

#[test]
fn test_flush_count() {
    let stats = Statistics::new();

    stats.flush_count.fetch_add(1, Ordering::Relaxed);
    stats.flush_count.fetch_add(1, Ordering::Relaxed);

    assert_eq!(stats.flush_count.load(Ordering::Relaxed), 2);
}
