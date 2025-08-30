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

#[test]
fn test_insert_bytes_basic() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    let key = b"bytes_key";
    let value = Bytes::from_static(b"test_value");

    // Insert using Bytes
    store.insert_bytes(key, value.clone()).unwrap();

    // Retrieve and verify
    let retrieved = store.get(key).unwrap();
    assert_eq!(retrieved.as_slice(), b"test_value");

    // Also check with get_bytes
    let bytes_result = store.get_bytes(key).unwrap();
    assert_eq!(&bytes_result[..], b"test_value");
}

#[test]
fn test_insert_bytes_with_timestamp() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    let key = b"timestamped_bytes";
    let value1 = Bytes::from(vec![1, 2, 3, 4]);
    let value2 = Bytes::from(vec![5, 6, 7, 8]);

    // Insert with explicit timestamp
    store
        .insert_bytes_with_timestamp(key, value1, Some(100))
        .unwrap();

    // Try to insert with older timestamp - should fail
    let result = store.insert_bytes_with_timestamp(key, value2.clone(), Some(50));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Insert with newer timestamp - should succeed
    store
        .insert_bytes_with_timestamp(key, value2.clone(), Some(200))
        .unwrap();

    let retrieved = store.get(key).unwrap();
    assert_eq!(retrieved.as_slice(), &[5, 6, 7, 8]);
}

#[test]
fn test_insert_bytes_zero_copy() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    // Create a large Bytes value
    let large_data = vec![42u8; 100_000];
    let bytes_value = Bytes::from(large_data.clone());

    let key = b"large_bytes_key";

    // Insert using Bytes (should avoid copying the 100KB)
    store.insert_bytes(key, bytes_value).unwrap();

    // Verify the data
    let retrieved = store.get_bytes(key).unwrap();
    assert_eq!(retrieved.len(), 100_000);
    assert!(retrieved.iter().all(|&b| b == 42));
}

#[test]
fn test_insert_bytes_update_existing() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    let key = b"update_bytes";

    // First insert with regular insert
    store.insert(key, b"initial").unwrap();

    // Update with insert_bytes
    let new_value = Bytes::from_static(b"updated_with_bytes");
    store.insert_bytes(key, new_value).unwrap();

    let retrieved = store.get(key).unwrap();
    assert_eq!(retrieved.as_slice(), b"updated_with_bytes");
}

#[test]
fn test_insert_bytes_from_static() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    // Using from_static avoids allocation entirely
    let static_bytes = Bytes::from_static(b"static string data");

    store.insert_bytes(b"static_key", static_bytes).unwrap();

    let retrieved = store.get(b"static_key").unwrap();
    assert_eq!(retrieved.as_slice(), b"static string data");
}

#[test]
fn test_insert_bytes_empty_value() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    let key = b"empty_bytes";
    let empty = Bytes::new();

    // Empty values should be rejected, consistent with regular insert
    let result = store.insert_bytes(key, empty);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::InvalidValueSize));
}

#[test]
fn test_insert_bytes_with_slice() {
    use bytes::Bytes;

    let store = FeoxStore::new(None).unwrap();

    // Create Bytes from a slice of another Bytes
    let original = Bytes::from(vec![1, 2, 3, 4, 5]);
    let slice = original.slice(1..4); // Creates a view [2, 3, 4]

    store.insert_bytes(b"slice_key", slice).unwrap();

    let retrieved = store.get(b"slice_key").unwrap();
    assert_eq!(retrieved.as_slice(), &[2, 3, 4]);
}
