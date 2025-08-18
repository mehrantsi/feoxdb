use crate::core::record::{AtomicLink, Record};
use bytes::Bytes;
use crossbeam_epoch;
use std::sync::atomic::Ordering;

#[test]
fn test_record_creation() {
    let key = b"test_key".to_vec();
    let value = b"test_value".to_vec();
    let timestamp = 12345u64;

    let record = Record::new(key.clone(), value.clone(), timestamp);

    assert_eq!(record.key, key);
    assert_eq!(record.key_len, key.len() as u16);
    assert_eq!(record.value_len, value.len());
    assert_eq!(record.timestamp, timestamp);
    assert_eq!(record.sector.load(Ordering::Acquire), 0);
    assert_eq!(record.refcount.load(Ordering::Acquire), 1);
}

#[test]
fn test_record_value_operations() {
    let record = Record::new(b"key".to_vec(), b"value".to_vec(), 100);

    // Should have value initially
    assert!(record.get_value().is_some());
    assert_eq!(record.get_value().unwrap(), Bytes::from("value"));

    // Clear value
    record.clear_value();
    assert!(record.get_value().is_none());

    // Value length should remain
    assert_eq!(record.value_len, 5);
}

#[test]
fn test_record_size_calculation() {
    let key = b"key".to_vec();
    let value = b"value".to_vec();
    let record = Record::new(key.clone(), value.clone(), 100);

    let size = record.calculate_size();
    assert!(size > 0);
    assert!(size >= std::mem::size_of::<Record>() + key.len() + value.len());

    let disk_size = record.calculate_disk_size();
    assert!(disk_size > 0);
    assert_eq!(disk_size % 4096, 0); // Should be aligned to block size
}

#[test]
fn test_record_refcount() {
    let record = Record::new(b"key".to_vec(), b"value".to_vec(), 100);

    assert_eq!(record.ref_count(), 1);

    record.inc_ref();
    assert_eq!(record.ref_count(), 2);

    let remaining = record.dec_ref();
    assert_eq!(remaining, 1);
    assert_eq!(record.ref_count(), 1);

    let remaining = record.dec_ref();
    assert_eq!(remaining, 0);
    assert_eq!(record.ref_count(), 0);
}

#[test]
fn test_record_sector_storage() {
    let record = Record::new(b"key".to_vec(), b"value".to_vec(), 100);

    assert_eq!(record.sector.load(Ordering::Acquire), 0);

    record.sector.store(12345, Ordering::Release);
    assert_eq!(record.sector.load(Ordering::Acquire), 12345);
}

#[test]
fn test_atomic_link_operations() {
    let guard = &crossbeam_epoch::pin();
    let link = AtomicLink::new();

    // Initially null
    assert!(link.load(guard).is_none());

    // Store a record
    let record1 = Record::new(b"key1".to_vec(), b"val1".to_vec(), 100);
    let record1_shared = crossbeam_epoch::Owned::new(record1).into_shared(guard);
    link.store(Some(record1_shared), guard);

    // Load it back
    let loaded = link.load(guard);
    assert!(loaded.is_some());

    // Compare and exchange
    let record2 = Record::new(b"key2".to_vec(), b"val2".to_vec(), 200);
    let record2_shared = crossbeam_epoch::Owned::new(record2).into_shared(guard);

    let result = link.compare_exchange(record1_shared, record2_shared, guard);
    assert!(result.is_ok());

    // Verify new value is stored
    let loaded = link.load(guard);
    assert!(loaded.is_some());
}

#[test]
fn test_record_cache_metadata() {
    let record = Record::new(b"key".to_vec(), b"value".to_vec(), 100);

    assert_eq!(record.cache_ref_bit.load(Ordering::Acquire), 0);
    assert_eq!(record.cache_access_time.load(Ordering::Acquire), 0);

    record.cache_ref_bit.store(1, Ordering::Release);
    record.cache_access_time.store(999999, Ordering::Release);

    assert_eq!(record.cache_ref_bit.load(Ordering::Acquire), 1);
    assert_eq!(record.cache_access_time.load(Ordering::Acquire), 999999);
}

#[test]
fn test_record_with_timestamp() {
    let timestamp = 987654321u64;
    let record = Record::new_with_timestamp(b"key".to_vec(), b"value".to_vec(), timestamp);

    assert_eq!(record.timestamp, timestamp);
}

#[test]
fn test_record_thread_safety() {
    use std::sync::Arc;
    use std::thread;

    let record = Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 100));
    let mut handles = vec![];

    // Multiple threads incrementing refcount
    for _ in 0..10 {
        let record_clone = Arc::clone(&record);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                record_clone.inc_ref();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have 1 (initial) + 1000 (10 threads * 100 increments)
    assert_eq!(record.ref_count(), 1001);
}

#[test]
fn test_record_value_rwlock() {
    use std::sync::Arc;
    use std::thread;

    let record = Arc::new(Record::new(b"key".to_vec(), b"initial".to_vec(), 100));
    let mut handles = vec![];

    // Multiple readers
    for _ in 0..5 {
        let record_clone = Arc::clone(&record);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                let _ = record_clone.get_value();
            }
        }));
    }

    // Writer clearing value
    let record_clone = Arc::clone(&record);
    handles.push(thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        record_clone.clear_value();
    }));

    for handle in handles {
        handle.join().unwrap();
    }

    // Value should be cleared
    assert!(record.get_value().is_none());
}
