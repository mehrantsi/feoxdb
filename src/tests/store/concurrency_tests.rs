use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use crate::test_hooks::{gate, AFTER_UPSERT_READ};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

#[test]
fn newer_delete_wins_over_in_flight_upsert() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"key", b"old", Some(100))
        .unwrap();

    let (start_tx, start_rx) = sync_channel(0);
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        start_rx.recv().unwrap();
        writer_store.insert_with_timestamp(b"key", b"stale", Some(150))
    });
    let armed = session.arm_for_thread(AFTER_UPSERT_READ, writer.thread().id(), 1);

    start_tx.send(()).unwrap();
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));
    store.delete_with_timestamp(b"key", Some(200)).unwrap();
    armed.release();

    assert!(matches!(
        writer.join().unwrap(),
        Err(FeoxError::OlderTimestamp)
    ));
    assert!(!store.contains_key(b"key"));
}

#[test]
fn newer_delete_wins_across_an_intermediate_generation() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"key", b"old", Some(100))
        .unwrap();

    let (start_tx, start_rx) = sync_channel(0);
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        start_rx.recv().unwrap();
        writer_store.insert_with_timestamp(b"key", b"stale", Some(150))
    });
    let armed = session.arm_for_thread(AFTER_UPSERT_READ, writer.thread().id(), 1);

    start_tx.send(()).unwrap();
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));
    store
        .insert_with_timestamp(b"key", b"intermediate", Some(160))
        .unwrap();
    store.delete_with_timestamp(b"key", Some(200)).unwrap();
    armed.release();

    assert!(matches!(
        writer.join().unwrap(),
        Err(FeoxError::OlderTimestamp)
    ));
    assert!(!store.contains_key(b"key"));
}

#[test]
fn newer_delete_wins_after_a_fresh_recreation() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"key", b"old", Some(100))
        .unwrap();

    let (start_tx, start_rx) = sync_channel(0);
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        start_rx.recv().unwrap();
        writer_store.insert_with_timestamp(b"key", b"stale", Some(150))
    });
    let armed = session.arm_for_thread(AFTER_UPSERT_READ, writer.thread().id(), 1);

    start_tx.send(()).unwrap();
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));
    store.delete_with_timestamp(b"key", Some(200)).unwrap();
    store
        .insert_with_timestamp(b"key", b"fresh", Some(50))
        .unwrap();
    armed.release();

    assert!(matches!(
        writer.join().unwrap(),
        Err(FeoxError::OlderTimestamp)
    ));
    assert_eq!(store.get(b"key").unwrap(), b"fresh");
}

#[test]
fn newer_in_flight_upsert_recreates_after_delete() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"key", b"old", Some(100))
        .unwrap();

    let (start_tx, start_rx) = sync_channel(0);
    let writer_store = Arc::clone(&store);
    let writer = thread::spawn(move || {
        start_rx.recv().unwrap();
        writer_store.insert_with_timestamp(b"key", b"new", Some(300))
    });
    let armed = session.arm_for_thread(AFTER_UPSERT_READ, writer.thread().id(), 1);

    start_tx.send(()).unwrap();
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));
    store.delete_with_timestamp(b"key", Some(200)).unwrap();
    armed.release();

    assert!(writer.join().unwrap().unwrap());
    assert_eq!(store.get(b"key").unwrap(), b"new");
}
