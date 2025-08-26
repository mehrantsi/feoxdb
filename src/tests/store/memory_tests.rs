use crate::core::store::{FeoxStore, StoreBuilder};
use crate::error::FeoxError;

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
