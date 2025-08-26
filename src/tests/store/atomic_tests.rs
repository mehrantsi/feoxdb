use crate::core::store::FeoxStore;
use crate::error::FeoxError;

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
