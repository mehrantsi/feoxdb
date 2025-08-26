use crate::core::store::FeoxStore;
use std::sync::Arc;
use std::thread;

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
