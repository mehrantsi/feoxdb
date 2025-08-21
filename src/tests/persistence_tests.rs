use crate::core::store::FeoxStore;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

#[test]
fn test_basic_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Create store and insert data
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        store.insert(b"persist_key", b"persist_value").unwrap();
        store.insert(b"another_key", b"another_value").unwrap();

        store.flush();
    } // Store is dropped here

    // Reopen and verify data persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let value = store.get(b"persist_key").unwrap();
        assert_eq!(value, b"persist_value");

        let value2 = store.get(b"another_key").unwrap();
        assert_eq!(value2, b"another_value");
    }
}

#[test]
fn test_flush_all() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    let store = FeoxStore::new(Some(path)).unwrap();

    // Insert data
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        store.insert(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // Force flush
    store.flush_all();

    // Data should be on disk even without dropping store
    assert_eq!(store.len(), 100);
}

#[test]
fn test_graceful_shutdown() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Insert data and let Drop handle flushing
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        for i in 0..20 {
            let key = format!("shutdown_key_{}", i);
            let value = format!("shutdown_value_{}", i);
            store.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // Store drops here - Drop impl will flush
    }

    // Reopen and verify all data persisted (thanks to Drop)
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        // All keys should be present due to graceful shutdown
        for i in 0..20 {
            let key = format!("shutdown_key_{}", i);
            assert!(store.contains_key(key.as_bytes()));
        }
    }
}

#[test]
fn test_value_offloading() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    let store = FeoxStore::new(Some(path)).unwrap();

    // Insert large value
    let large_value = vec![0xAB; 100_000]; // 100KB
    store.insert(b"large_key", &large_value).unwrap();

    // Force flush to disk
    store.flush();

    // Wait for write buffer to process
    thread::sleep(Duration::from_millis(100));

    // Value should still be retrievable
    let retrieved = store.get(b"large_key").unwrap();
    assert_eq!(retrieved, large_value);
}

#[test]
fn test_metadata_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    let initial_count;

    // Create store and get initial stats
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        for i in 0..25 {
            let key = format!("meta_key_{}", i);
            store.insert(key.as_bytes(), b"value").unwrap();
        }

        initial_count = store.len();
        store.flush_all();
    }

    // Reopen and verify metadata
    {
        let store = FeoxStore::new(Some(path)).unwrap();
        assert_eq!(store.len(), initial_count);
    }
}

#[test]
fn test_concurrent_persistence() {
    use std::sync::Arc;

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    let store = Arc::new(FeoxStore::new(Some(path.clone())).unwrap());
    let mut handles = vec![];

    // Multiple threads writing
    for t in 0..5 {
        let store_clone = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..20 {
                let key = format!("thread{}:key{}", t, i);
                let value = format!("value_{}_{}", t, i);
                store_clone
                    .insert(key.as_bytes(), value.as_bytes())
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    store.flush_all();
    drop(store);

    // Verify all data persisted
    let store = FeoxStore::new(Some(path)).unwrap();
    assert_eq!(store.len(), 100); // 5 threads * 20 keys
}

#[test]
fn test_delete_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Insert and delete
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        store.insert(b"del_key1", b"value1").unwrap();
        store.insert(b"del_key2", b"value2").unwrap();
        store.insert(b"keep_key", b"keep_value").unwrap();

        store.delete(b"del_key1").unwrap();
        store.delete(b"del_key2").unwrap();

        store.flush_all();
    }

    // Verify deletes persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        assert!(!store.contains_key(b"del_key1"));
        assert!(!store.contains_key(b"del_key2"));
        assert!(store.contains_key(b"keep_key"));
        assert_eq!(store.len(), 1);
    }
}

#[test]
fn test_update_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Multiple updates
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        store.insert(b"update_key", b"value1").unwrap();
        store.insert(b"update_key", b"value2").unwrap();
        store.insert(b"update_key", b"value3").unwrap();
        store.insert(b"update_key", b"final_value").unwrap();

        store.flush_all();
    }

    // Verify only latest value persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let value = store.get(b"update_key").unwrap();
        assert_eq!(value, b"final_value");
    }
}

#[test]
fn test_atomic_increment_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Perform atomic operations
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        let zero: i64 = 0;
        store.insert(b"counter", &zero.to_le_bytes()).unwrap();

        for _ in 0..100 {
            store.atomic_increment(b"counter", 1).unwrap();
        }

        store.flush_all();
    }

    // Verify counter value persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let value = store.atomic_increment(b"counter", 0).unwrap();
        assert_eq!(value, 100);
    }
}

#[test]
fn test_range_query_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Insert sorted data
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        for i in 0..50 {
            let key = format!("item:{:03}", i);
            let value = format!("value_{}", i);
            store.insert(key.as_bytes(), value.as_bytes()).unwrap();
        }

        store.flush_all();
    }

    // Verify range queries work after restart
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let results = store.range_query(b"item:010", b"item:020", 100).unwrap();
        assert_eq!(results.len(), 11); // 010 through 020 inclusive

        assert_eq!(results[0].0, b"item:010");
        assert_eq!(results[10].0, b"item:020");
    }
}
