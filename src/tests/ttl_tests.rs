use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use std::thread;
use std::time::Duration;

#[test]
fn test_insert_with_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with 1 second TTL
    store.insert_with_ttl(b"key1", b"value1", 1).unwrap();

    // Should be retrievable immediately
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value1");

    // Wait for expiry
    thread::sleep(Duration::from_millis(1100));

    // Should be expired now
    let result = store.get(b"key1");
    assert!(result.is_err());
}

#[test]
fn test_get_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with 10 second TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Check TTL
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());
    let ttl_seconds = ttl.unwrap();
    assert!(ttl_seconds > 8 && ttl_seconds <= 10);

    // Insert without TTL
    store.insert(b"key2", b"value2").unwrap();
    let ttl = store.get_ttl(b"key2").unwrap();
    assert!(ttl.is_none());
}

#[test]
fn test_update_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert without TTL
    store.insert(b"key1", b"value1").unwrap();

    // Add TTL
    store.update_ttl(b"key1", 5).unwrap();
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());

    // Remove TTL (persist)
    store.persist(b"key1").unwrap();
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_none());
}

#[test]
fn test_ttl_preserves_value() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store
        .insert_with_ttl(b"key1", b"original_value", 10)
        .unwrap();

    // Update TTL shouldn't change value
    store.update_ttl(b"key1", 20).unwrap();

    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"original_value");
}

#[test]
fn test_expired_key_not_found() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // TTL of 0 means no expiry
    store.insert_with_ttl(b"ephemeral", b"data", 0).unwrap();

    // Should still be there
    let result = store.get(b"ephemeral");
    assert!(result.is_ok());

    // Test with 1 second TTL
    store.insert_with_ttl(b"ephemeral2", b"data", 1).unwrap();

    // Should be retrievable immediately
    assert!(store.get(b"ephemeral2").is_ok());

    // Wait for expiry
    thread::sleep(Duration::from_secs(2));

    // Should be expired now
    let result = store.get(b"ephemeral2");
    assert!(result.is_err());
}

#[test]
fn test_update_resets_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Update with new TTL
    store.insert_with_ttl(b"key1", b"value2", 20).unwrap();

    // Check new TTL is applied
    let ttl = store.get_ttl(b"key1").unwrap().unwrap();
    assert!(ttl > 15 && ttl <= 20);

    // Check value is updated
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value2");
}

#[test]
fn test_regular_insert_removes_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Verify TTL is set
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());

    // Regular insert should remove TTL
    store.insert(b"key1", b"value2").unwrap();

    // Verify TTL is removed
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_none());

    // Value should be updated
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value2");

    // Wait to ensure it doesn't expire (since TTL was removed)
    thread::sleep(Duration::from_millis(100));
    assert!(store.get(b"key1").is_ok());
}

#[test]
fn test_ttl_operations_fail_when_disabled() {
    // Create store with TTL disabled (default)
    let store = FeoxStore::new(None).unwrap();

    // All TTL operations should return TtlNotEnabled error
    assert!(matches!(
        store.insert_with_ttl(b"key1", b"value1", 10),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.insert_with_ttl_and_timestamp(b"key2", b"value2", 10, None),
        Err(FeoxError::TtlNotEnabled)
    ));

    // Regular insert should work
    store.insert(b"key3", b"value3").unwrap();

    assert!(matches!(
        store.get_ttl(b"key3"),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.update_ttl(b"key3", 10),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.persist(b"key3"),
        Err(FeoxError::TtlNotEnabled)
    ));

    // Regular operations should still work
    assert_eq!(store.get(b"key3").unwrap(), b"value3");
    store.delete(b"key3").unwrap();
}

#[test]
fn test_ttl_with_builder_explicit_disable() {
    // Explicitly disable TTL via builder
    let store = FeoxStore::builder().enable_ttl(false).build().unwrap();

    // TTL operations should fail
    assert!(matches!(
        store.insert_with_ttl(b"key1", b"value1", 10),
        Err(FeoxError::TtlNotEnabled)
    ));
}
