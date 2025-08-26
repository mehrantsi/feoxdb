use crate::core::store::FeoxStore;
use crate::error::FeoxError;

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
