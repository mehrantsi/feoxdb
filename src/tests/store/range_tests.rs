use crate::core::store::FeoxStore;
use std::sync::{Arc, Barrier};
use std::thread;

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
fn range_query_crosses_epoch_repin_boundary() {
    let store = FeoxStore::new(None).unwrap();
    for index in 0..300 {
        let key = format!("key:{index:03}");
        store.insert(key.as_bytes(), b"value").unwrap();
    }

    let results = store.range_query(b"key:000", b"key:299", 300).unwrap();
    assert_eq!(results.len(), 300);
    for (index, (key, value)) in results.iter().enumerate() {
        assert_eq!(key, format!("key:{index:03}").as_bytes());
        assert_eq!(value, b"value");
    }
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

#[test]
fn test_range_query_during_updates() {
    let store = Arc::new(FeoxStore::new(None).unwrap());
    for index in 0..64 {
        let key = format!("key:{index:03}");
        store.insert(key.as_bytes(), b"old").unwrap();
    }

    let barrier = Arc::new(Barrier::new(2));
    let writer_store = Arc::clone(&store);
    let writer_barrier = Arc::clone(&barrier);
    let writer = thread::spawn(move || {
        writer_barrier.wait();
        for index in 0..2_000 {
            let key = format!("key:{:03}", index % 64);
            writer_store.insert(key.as_bytes(), b"new").unwrap();
        }
    });

    barrier.wait();
    for _ in 0..100 {
        let results = store.range_query(b"key:000", b"key:063", 64).unwrap();
        assert_eq!(results.len(), 64);
        for (index, (key, value)) in results.iter().enumerate() {
            assert_eq!(key, format!("key:{index:03}").as_bytes());
            assert!(value == b"old" || value == b"new");
        }
    }

    writer.join().unwrap();
}
