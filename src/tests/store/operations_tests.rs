use crate::core::store::FeoxStore;
use crate::error::FeoxError;

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
fn test_get_bytes_basic() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"test_key";
    let value = b"test_value";

    store.insert(key, value).unwrap();

    let bytes_result = store.get_bytes(key).unwrap();
    assert_eq!(&bytes_result[..], value);

    assert!(!bytes_result.is_empty());
    assert_eq!(bytes_result.len(), value.len());
}

#[test]
fn test_get_bytes_large_value() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"large_key";
    let value = vec![b'x'; 100_000];

    store.insert(key, &value).unwrap();

    let bytes_result = store.get_bytes(key).unwrap();
    assert_eq!(&bytes_result[..], &value[..]);
    assert_eq!(bytes_result.len(), 100_000);
}

#[test]
fn test_get_bytes_zero_copy_semantics() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"zero_copy_key";
    let value = b"shared_value";

    store.insert(key, value).unwrap();

    let bytes1 = store.get_bytes(key).unwrap();
    let bytes2 = store.get_bytes(key).unwrap();

    assert_eq!(&bytes1[..], &bytes2[..]);
    assert_eq!(&bytes1[..], value);
}

#[test]
fn test_get_bytes_key_not_found() {
    let store = FeoxStore::new(None).unwrap();

    let result = store.get_bytes(b"nonexistent");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::KeyNotFound));
}

#[test]
fn test_get_bytes_after_update() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"update_bytes_key";
    let value1 = b"initial";
    let value2 = b"updated_value_longer";

    store.insert(key, value1).unwrap();
    let bytes1 = store.get_bytes(key).unwrap();
    assert_eq!(&bytes1[..], value1);

    store.insert(key, value2).unwrap();
    let bytes2 = store.get_bytes(key).unwrap();
    assert_eq!(&bytes2[..], value2);
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

#[test]
fn test_get_size() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"sized_key";
    let value = vec![b'x'; 12345];

    store.insert(key, &value).unwrap();

    let size = store.get_size(key).unwrap();
    assert_eq!(size, 12345);

    // Non-existent key should error
    assert!(store.get_size(b"nonexistent").is_err());
}
