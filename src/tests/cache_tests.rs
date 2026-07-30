use crate::constants::*;
use crate::core::cache::ClockCache;
use crate::core::record::Record;
use crate::stats::Statistics;
use crate::test_hooks::{gate, AFTER_CACHE_BUCKET_CLEAR};
use crate::utils::hash::murmur3_32;
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn record(key: &[u8], value: &Bytes, timestamp: u64) -> Arc<Record> {
    Arc::new(Record::new(key.to_vec(), value.to_vec(), timestamp))
}

fn key_for_bucket(bucket: usize) -> Vec<u8> {
    (0_u64..)
        .map(|candidate| candidate.to_le_bytes().to_vec())
        .find(|key| murmur3_32(key, 0) as usize % CACHE_BUCKETS == bucket)
        .unwrap()
}

#[test]
fn test_basic_cache_operations() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats);

    let key = b"test_key".to_vec();
    let value = Bytes::from("test_value");

    // Insert and retrieve
    cache.insert(key.clone(), value.clone());
    let retrieved = cache.get(&key).unwrap();
    assert_eq!(retrieved, value);

    // Remove and verify gone
    cache.remove(&key);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_cache_eviction() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats.clone());

    // Set lower watermarks for testing
    cache.adjust_watermarks(1, 0); // 1MB high, 0 low

    // Insert many entries to trigger eviction
    for i in 0..10000 {
        let key = format!("key_{}", i).into_bytes();
        let value = Bytes::from(vec![0u8; KB]); // 1KB each
        cache.insert(key, value);
    }

    let cache_stats = cache.stats();
    assert!(cache_stats.memory_usage <= cache_stats.high_watermark);
}

#[test]
fn test_reference_bit_behavior() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats);

    for i in 0..100 {
        let key = format!("key_{}", i).into_bytes();
        let value = Bytes::from(format!("value_{}", i));
        cache.insert(key, value);
    }

    // Access some entries to set reference bits
    for i in 0..50 {
        let key = format!("key_{}", i).into_bytes();
        cache.get(&key);
    }

    // These entries should be less likely to be evicted
    // due to their reference bits being set
}

#[test]
fn test_cache_clear() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats.clone());

    for i in 0..10 {
        let key = format!("key_{}", i).into_bytes();
        let value = Bytes::from(format!("value_{}", i));
        cache.insert(key, value);
    }

    // Clear cache
    cache.clear();

    // Verify all entries are gone
    for i in 0..10 {
        let key = format!("key_{}", i).into_bytes();
        assert!(cache.get(&key).is_none());
    }

    // Verify memory usage is 0
    assert_eq!(cache.stats().memory_usage, 0);
}

#[test]
fn concurrent_insert_after_bucket_clear_remains_accounted() {
    let session = gate::session();
    let stats = Arc::new(Statistics::new());
    let cache = Arc::new(ClockCache::new(stats));
    let (start_tx, start_rx) = sync_channel(0);
    let clear_cache = Arc::clone(&cache);
    let clearer = thread::spawn(move || {
        start_rx.recv().unwrap();
        clear_cache.clear();
    });
    let armed = session.arm_for_thread(AFTER_CACHE_BUCKET_CLEAR, clearer.thread().id(), 1);

    start_tx.send(()).unwrap();
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));

    let key = key_for_bucket(0);
    let value = Bytes::from_static(b"survivor");
    cache.insert(key.clone(), value.clone());

    assert!(armed.release_and_drain(Duration::from_secs(5)));
    drop(armed);
    clearer.join().unwrap();

    assert_eq!(cache.get(&key), Some(value));
    assert!(cache.stats().memory_usage > 0);
    cache.remove(&key);
    assert_eq!(cache.stats().memory_usage, 0);
}

#[test]
fn test_cache_update_existing() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats);

    let key = b"test_key".to_vec();
    let value1 = Bytes::from("value1");
    let value2 = Bytes::from("much_longer_value2");
    let record1 = record(&key, &value1, 1);
    let record2 = record(&key, &value2, 2);

    // Insert initial value
    cache.insert_for_record(key.clone(), value1.clone(), &record1);
    assert_eq!(cache.get_for_record(&key, &record1).unwrap(), value1);
    assert!(cache.get_for_record(&key, &record2).is_none());

    // Update with new value
    cache.insert_for_record(key.clone(), value2.clone(), &record2);
    assert!(cache.get_for_record(&key, &record1).is_none());
    assert_eq!(cache.get_for_record(&key, &record2).unwrap(), value2);
}

#[test]
fn stale_generation_does_not_displace_live_cache_entry() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats);
    let key = b"generation".to_vec();
    let old_value = Bytes::from_static(b"old value with a different size");
    let new_value = Bytes::from_static(b"new");
    let old_record = record(&key, &old_value, 1);
    let new_record = record(&key, &new_value, 2);

    cache.insert_for_record(key.clone(), new_value.clone(), &new_record);
    let memory_usage = cache.stats().memory_usage;
    cache.insert_for_record(key.clone(), old_value, &old_record);

    assert_eq!(cache.stats().memory_usage, memory_usage);
    assert_eq!(cache.get_for_record(&key, &new_record), Some(new_value));
    assert!(cache.get_for_record(&key, &old_record).is_none());
}

#[test]
fn unversioned_cache_inserts_remain_authoritative() {
    let cache = ClockCache::new(Arc::new(Statistics::new()));
    let key = b"manual".to_vec();
    let versioned_value = Bytes::from_static(b"versioned");
    let manual_value = Bytes::from_static(b"manual");
    let original = record(&key, &versioned_value, 1);

    cache.insert_for_record(key.clone(), versioned_value, &original);
    cache.insert(key.clone(), manual_value.clone());
    assert_eq!(cache.get(&key), Some(manual_value));
    assert!(cache.get_for_record(&key, &original).is_none());

    let replacement_value = Bytes::from_static(b"replacement");
    let replacement = record(&key, &replacement_value, 2);
    cache.insert_for_record(key.clone(), replacement_value.clone(), &replacement);
    assert_eq!(
        cache.get_for_record(&key, &replacement),
        Some(replacement_value)
    );
}

#[test]
fn record_cache_does_not_keep_a_generation_alive() {
    let cache = ClockCache::new(Arc::new(Statistics::new()));
    let key = b"generation".to_vec();
    let value = Bytes::from_static(b"value");
    let original = record(&key, &value, 1);
    let lifetime = Arc::downgrade(&original);

    cache.insert_for_record(key.clone(), value, &original);
    assert_eq!(Arc::strong_count(&original), 1);
    drop(original);
    assert!(lifetime.upgrade().is_none());

    let replacement = record(&key, &Bytes::from_static(b"replacement"), 2);
    assert!(cache.get_for_record(&key, &replacement).is_none());
}

#[test]
fn stale_generation_removal_preserves_live_cache_entry() {
    let cache = ClockCache::new(Arc::new(Statistics::new()));
    let key = b"generation".to_vec();
    let old_record = record(&key, &Bytes::from_static(b"old"), 1);
    let new_record = record(&key, &Bytes::from_static(b"new"), 2);

    cache.insert_for_record(key.clone(), Bytes::from_static(b"new"), &new_record);
    cache.remove_for_record(&key, &old_record);

    assert_eq!(
        cache.get_for_record(&key, &new_record),
        Some(Bytes::from_static(b"new"))
    );
}

#[test]
fn record_entry_removes_the_matching_generation() {
    let cache = ClockCache::new(Arc::new(Statistics::new()));
    let key = b"generation".to_vec();
    let value = Bytes::from_static(b"value");
    let record = record(&key, &value, 1);

    cache.insert_for_record(key.clone(), value.clone(), &record);
    let entry = cache.record_entry(&key, &record);
    assert_eq!(entry.value(), Some(value));
    entry.remove();

    assert!(cache.get_for_record(&key, &record).is_none());
    assert_eq!(cache.stats().memory_usage, 0);
}

#[test]
fn retired_generation_cannot_displace_lower_timestamp_recreation() {
    let cache = ClockCache::new(Arc::new(Statistics::new()));
    let key = b"recreated".to_vec();
    let old_record = record(&key, &Bytes::from_static(b"old"), 200);
    let new_record = record(&key, &Bytes::from_static(b"new"), 150);

    cache.insert_for_record(key.clone(), Bytes::from_static(b"old"), &old_record);
    old_record.refcount.store(0, Ordering::Release);
    cache.insert_for_record(key.clone(), Bytes::from_static(b"new"), &new_record);
    cache.insert_for_record(key.clone(), Bytes::from_static(b"stale"), &old_record);

    assert_eq!(
        cache.get_for_record(&key, &new_record),
        Some(Bytes::from_static(b"new"))
    );
    assert!(cache.get_for_record(&key, &old_record).is_none());
}

#[test]
fn test_cache_large_value_rejection() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats.clone());

    // Try to insert a value larger than 1/4 of high watermark
    let key = b"huge_key".to_vec();
    let huge_value = Bytes::from(vec![0u8; CACHE_HIGH_WATERMARK_MB * MB / 3]);

    cache.insert(key.clone(), huge_value);

    // Should not be cached
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_cache_watermark_adjustment() {
    let stats = Arc::new(Statistics::new());
    let cache = ClockCache::new(stats.clone());

    // Adjust watermarks
    cache.adjust_watermarks(10, 5); // 10MB high, 5MB low

    let cache_stats = cache.stats();
    assert_eq!(cache_stats.high_watermark, 10 * MB);
    assert_eq!(cache_stats.low_watermark, 5 * MB);

    // Invalid adjustment should be ignored
    cache.adjust_watermarks(5, 10); // Invalid: low > high
    let cache_stats = cache.stats();
    assert_eq!(cache_stats.high_watermark, 10 * MB); // Unchanged
}

#[test]
fn test_concurrent_cache_access() {
    let stats = Arc::new(Statistics::new());
    let cache = Arc::new(ClockCache::new(stats));

    let mut handles = vec![];

    // Multiple threads inserting
    for i in 0..10 {
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let key = format!("thread{}:key{}", i, j).into_bytes();
                let value = Bytes::from(format!("value_{}_{}", i, j));
                cache_clone.insert(key, value);
            }
        }));
    }

    // Multiple threads reading
    for i in 0..10 {
        let cache_clone = Arc::clone(&cache);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let key = format!("thread{}:key{}", i, j).into_bytes();
                cache_clone.get(&key);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
