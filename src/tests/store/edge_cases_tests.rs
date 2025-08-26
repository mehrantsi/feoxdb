use crate::constants::MAX_KEY_SIZE;
use crate::core::store::FeoxStore;
use crate::error::FeoxError;

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
