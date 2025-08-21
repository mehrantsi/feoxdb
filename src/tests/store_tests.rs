use crate::constants::MAX_KEY_SIZE;
use crate::core::store::{FeoxStore, StoreBuilder};
use crate::error::FeoxError;
use std::sync::Arc;
use std::thread;

// ============ Basic Operations Tests ============

#[test]
fn test_basic_operations() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"test_key";
    let value = b"test_value";

    // Insert
    store.insert(key, value).unwrap();

    // Get
    let retrieved = store.get(key).unwrap();
    assert_eq!(retrieved.as_slice(), value);

    // Delete
    store.delete(key).unwrap();

    // Get after delete should fail
    let result = store.get(key);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::KeyNotFound));
}

#[test]
fn test_update_existing_key() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"update_key";
    let value1 = b"value1";
    let value2 = b"value2_updated";

    store.insert(key, value1).unwrap();
    assert_eq!(store.get(key).unwrap(), value1);

    store.insert(key, value2).unwrap();
    assert_eq!(store.get(key).unwrap(), value2);
}

#[test]
fn test_empty_store() {
    let store = FeoxStore::new(None).unwrap();

    assert!(store.is_empty());
    assert_eq!(store.len(), 0);

    store.insert(b"key", b"value").unwrap();
    assert!(!store.is_empty());
    assert_eq!(store.len(), 1);
}

#[test]
fn test_contains_key() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"exists";
    assert!(!store.contains_key(key));

    store.insert(key, b"value").unwrap();
    assert!(store.contains_key(key));

    store.delete(key).unwrap();
    assert!(!store.contains_key(key));
}

// ============ Timestamp Tests ============

#[test]
fn test_timestamp_conflict_resolution() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"ts_key";

    // Insert with timestamp 100
    store.insert_with_timestamp(key, b"v1", Some(100)).unwrap();

    // Update with higher timestamp succeeds
    store.insert_with_timestamp(key, b"v2", Some(200)).unwrap();
    assert_eq!(store.get(key).unwrap(), b"v2");

    // Update with lower timestamp fails
    let result = store.insert_with_timestamp(key, b"v3", Some(150));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Value should still be v2
    assert_eq!(store.get(key).unwrap(), b"v2");
}

#[test]
fn test_delete_with_timestamp() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"del_ts_key";

    store
        .insert_with_timestamp(key, b"value", Some(100))
        .unwrap();

    // Delete with older timestamp fails
    let result = store.delete_with_timestamp(key, Some(50));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Key should still exist
    assert!(store.contains_key(key));

    // Delete with newer timestamp succeeds
    store.delete_with_timestamp(key, Some(200)).unwrap();
    assert!(!store.contains_key(key));
}

// ============ Memory Management Tests ============

#[test]
fn test_memory_limit_enforcement() {
    let store = StoreBuilder::new()
        .max_memory(1024) // Very small limit
        .build()
        .unwrap();

    // Insert small values should work
    store.insert(b"k1", b"v1").unwrap();

    // Try to insert a large value that exceeds limit
    let large_value = vec![0u8; 2048];
    let result = store.insert(b"k2", &large_value);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OutOfMemory));
}

#[test]
fn test_memory_usage_tracking() {
    let store = FeoxStore::new(None).unwrap();

    let initial_usage = store.memory_usage();

    // Insert some data
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        store.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let after_insert = store.memory_usage();
    assert!(after_insert > initial_usage);

    // Delete some data
    for i in 0..5 {
        let key = format!("key_{}", i);
        store.delete(key.as_bytes()).unwrap();
    }

    let after_delete = store.memory_usage();
    assert!(after_delete < after_insert);
}

// ============ Range Query Tests ============

#[test]
fn test_range_query_inclusive() {
    let store = FeoxStore::new(None).unwrap();

    // Insert sorted keys
    store.insert(b"key:001", b"val1").unwrap();
    store.insert(b"key:002", b"val2").unwrap();
    store.insert(b"key:003", b"val3").unwrap();
    store.insert(b"key:004", b"val4").unwrap();
    store.insert(b"key:005", b"val5").unwrap();

    // Query range 002-004 (inclusive)
    let results = store.range_query(b"key:002", b"key:004", 10).unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].0, b"key:002");
    assert_eq!(results[1].0, b"key:003");
    assert_eq!(results[2].0, b"key:004");
}

#[test]
fn test_range_query_with_limit() {
    let store = FeoxStore::new(None).unwrap();

    // Insert many keys
    for i in 0..100 {
        let key = format!("key:{:03}", i);
        let value = format!("val{}", i);
        store.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Query with limit
    let results = store.range_query(b"key:000", b"key:099", 5).unwrap();
    assert_eq!(results.len(), 5);
}

#[test]
fn test_range_query_empty_range() {
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"key:001", b"val1").unwrap();
    store.insert(b"key:005", b"val5").unwrap();

    // Query range with no matches
    let results = store.range_query(b"key:002", b"key:004", 10).unwrap();
    assert_eq!(results.len(), 0);
}

// ============ Atomic Operations Tests ============

#[test]
fn test_atomic_increment_basic() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"counter";

    // Initialize counter
    let zero: i64 = 0;
    store.insert(key, &zero.to_le_bytes()).unwrap();

    // Increment
    let val = store.atomic_increment(key, 1).unwrap();
    assert_eq!(val, 1);

    let val = store.atomic_increment(key, 5).unwrap();
    assert_eq!(val, 6);

    // Decrement
    let val = store.atomic_increment(key, -2).unwrap();
    assert_eq!(val, 4);
}

#[test]
fn test_atomic_increment_create_if_not_exists() {
    let store = FeoxStore::new(None).unwrap();

    // Increment non-existent key creates it
    let val = store.atomic_increment(b"new_counter", 100).unwrap();
    assert_eq!(val, 100);

    let val = store.atomic_increment(b"new_counter", 50).unwrap();
    assert_eq!(val, 150);
}

#[test]
fn test_atomic_increment_invalid_value() {
    let store = FeoxStore::new(None).unwrap();

    // Insert non-numeric value
    store.insert(b"not_a_counter", b"text").unwrap();

    // Try to increment
    let result = store.atomic_increment(b"not_a_counter", 1);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidOperation));
}

#[test]
fn test_atomic_increment_saturation() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"saturate";

    // Set to max value
    let max: i64 = i64::MAX - 1;
    store.insert(key, &max.to_le_bytes()).unwrap();

    // Increment should saturate, not overflow
    let val = store.atomic_increment(key, 10).unwrap();
    assert_eq!(val, i64::MAX);
}

// ============ JSON Patch Tests ============

#[test]
fn test_json_patch_basic() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"json_doc";
    let doc = br#"{"name":"Alice","age":30}"#;
    store.insert(key, doc).unwrap();

    let patch = br#"[{"op":"replace","path":"/age","value":31}]"#;
    store.json_patch(key, patch).unwrap();

    let updated = store.get(key).unwrap();
    let updated_str = String::from_utf8_lossy(&updated);

    // Verify the specific changes were applied
    assert!(
        updated_str.contains(r#""age":31"#),
        "Age should be updated to 31"
    );
    assert!(
        updated_str.contains(r#""name":"Alice""#),
        "Name should remain unchanged"
    );
    assert!(
        !updated_str.contains(r#""age":30"#),
        "Old age value should be gone"
    );
}

#[test]
fn test_json_patch_on_non_json() {
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"not_json", b"plain text").unwrap();

    let patch = br#"[{"op":"add","path":"/foo","value":"bar"}]"#;
    let result = store.json_patch(b"not_json", patch);
    assert!(result.is_err());
}

// ============ Concurrency Tests ============

#[test]
fn test_concurrent_inserts() {
    let store = Arc::new(FeoxStore::new(None).unwrap());
    let mut handles = vec![];

    for i in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for j in 0..100 {
                let key = format!("thread{}:key{}", i, j);
                let value = format!("value_{}_{}", i, j);
                store_clone
                    .insert(key.as_bytes(), value.as_bytes())
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(store.len(), 1000);
}

#[test]
fn test_concurrent_mixed_operations() {
    let store = Arc::new(FeoxStore::new(None).unwrap());

    // Pre-populate
    for i in 0..100 {
        let key = format!("key_{}", i);
        store.insert(key.as_bytes(), b"initial").unwrap();
    }

    let mut handles = vec![];

    // Readers
    for _ in 0..5 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key_{}", i);
                let _ = store_clone.get(key.as_bytes());
            }
        }));
    }

    // Writers
    for t in 0..5 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let key = format!("key_{}", i);
                let value = format!("updated_by_{}", t);
                let _ = store_clone.insert(key.as_bytes(), value.as_bytes());
            }
        }));
    }

    // Deleters
    for _ in 0..2 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 90..100 {
                let key = format!("key_{}", i);
                let _ = store_clone.delete(key.as_bytes());
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_atomic_increments() {
    let store = Arc::new(FeoxStore::new(None).unwrap());

    let key = b"shared_counter";
    let zero: i64 = 0;
    store.insert(key, &zero.to_le_bytes()).unwrap();

    let mut handles = vec![];

    for _ in 0..10 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                store_clone.atomic_increment(key, 1).unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Should have exactly 1000 increments
    let final_value = store.atomic_increment(key, 0).unwrap();
    assert_eq!(final_value, 1000);
}

// ============ Edge Cases ============

#[test]
fn test_empty_key_rejected() {
    let store = FeoxStore::new(None).unwrap();

    let result = store.insert(b"", b"value");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidKeySize));
}

#[test]
fn test_empty_value_rejected() {
    let store = FeoxStore::new(None).unwrap();

    let result = store.insert(b"key", b"");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidValueSize));
}

#[test]
fn test_max_key_size() {
    let store = FeoxStore::new(None).unwrap();

    // Large key should work
    let large_key = vec![b'k'; 10000];
    store.insert(&large_key, b"value").unwrap();
    assert_eq!(store.get(&large_key).unwrap(), b"value");

    // Over max size (100KB) should fail
    let oversized_key = vec![b'k'; MAX_KEY_SIZE + 1];
    let result = store.insert(&oversized_key, b"value");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidKeySize));
}

#[test]
fn test_get_size() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"sized_key";
    let value = vec![b'v'; 1000];
    store.insert(key, &value).unwrap();

    let size = store.get_size(key).unwrap();
    assert_eq!(size, 1000);
}

#[test]
fn test_builder_configuration() {
    let store = StoreBuilder::new()
        .max_memory(2_000_000_000)
        .hash_bits(20)
        .enable_caching(false)
        .build()
        .unwrap();

    // Should work with custom config
    store.insert(b"test", b"value").unwrap();
    assert_eq!(store.get(b"test").unwrap(), b"value");
}

#[test]
fn test_stats_snapshot() {
    let store = FeoxStore::new(None).unwrap();

    // Insert some data
    for i in 0..10 {
        let key = format!("key_{}", i);
        store.insert(key.as_bytes(), b"value").unwrap();
    }

    let stats = store.stats();
    assert_eq!(stats.record_count, 10);
    assert!(stats.memory_usage > 0);
    assert_eq!(stats.total_inserts, 10);
}
