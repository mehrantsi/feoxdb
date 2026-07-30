use crate::core::record::Record;
use crate::core::store::{FeoxStore, StoreBuilder};
use crate::error::{FeoxError, Result};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::NamedTempFile;

const CONCURRENT_WRITERS: usize = 16;
const LARGE_VALUE_SIZE: usize = 512 * 1024;

fn record_size(key_len: usize, value_len: usize) -> usize {
    std::mem::size_of::<Record>() + key_len + value_len
}

fn assert_concurrent_creates_respect_limit<F>(value_len: usize, create: F)
where
    F: Fn(&FeoxStore, &[u8]) -> Result<()> + Send + Sync + 'static,
{
    let key_len = std::mem::size_of::<u64>();
    let limit = record_size(key_len, value_len);
    let store = Arc::new(FeoxStore::builder().max_memory(limit).build().unwrap());
    let create = Arc::new(create);
    let start = Arc::new(Barrier::new(CONCURRENT_WRITERS));
    let mut handles = Vec::with_capacity(CONCURRENT_WRITERS);

    for index in 0..CONCURRENT_WRITERS {
        let store = Arc::clone(&store);
        let create = Arc::clone(&create);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let key = (index as u64).to_le_bytes();
            start.wait();
            create(&store, &key)
        }));
    }

    let mut successes = 0;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(()) => successes += 1,
            Err(FeoxError::OutOfMemory) => {}
            Err(error) => panic!("unexpected insert error: {error:?}"),
        }
    }

    assert_eq!(successes, 1);
    assert_eq!(store.len(), 1);
    assert_eq!(store.memory_usage(), limit);
}

fn assert_concurrent_growth_respects_limit<F>(replace: F)
where
    F: Fn(&FeoxStore, &[u8], &[u8]) -> Result<()> + Send + Sync + 'static,
{
    let key_len = std::mem::size_of::<u64>();
    let small_size = record_size(key_len, 1);
    let large_size = record_size(key_len, LARGE_VALUE_SIZE);
    let limit = CONCURRENT_WRITERS * small_size + large_size - small_size;
    let store = Arc::new(FeoxStore::builder().max_memory(limit).build().unwrap());
    for index in 0..CONCURRENT_WRITERS {
        store.insert(&(index as u64).to_le_bytes(), b"x").unwrap();
    }

    let value = Arc::new(vec![b'x'; LARGE_VALUE_SIZE]);
    let replace = Arc::new(replace);
    let start = Arc::new(Barrier::new(CONCURRENT_WRITERS));
    let mut handles = Vec::with_capacity(CONCURRENT_WRITERS);
    for index in 0..CONCURRENT_WRITERS {
        let store = Arc::clone(&store);
        let value = Arc::clone(&value);
        let replace = Arc::clone(&replace);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            let key = (index as u64).to_le_bytes();
            start.wait();
            replace(&store, &key, &value)
        }));
    }

    let mut successes = 0;
    for handle in handles {
        match handle.join().unwrap() {
            Ok(()) => successes += 1,
            Err(FeoxError::OutOfMemory) => {}
            Err(error) => panic!("unexpected replacement error: {error:?}"),
        }
    }

    assert_eq!(successes, 1);
    assert_eq!(store.len(), CONCURRENT_WRITERS);
    assert_eq!(store.memory_usage(), limit);
}

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

#[test]
fn concurrent_inserts_reserve_memory_atomically() {
    let value = Arc::new(vec![b'x'; LARGE_VALUE_SIZE]);
    assert_concurrent_creates_respect_limit(LARGE_VALUE_SIZE, move |store, key| {
        store.insert(key, &value).map(|_| ())
    });
}

#[test]
fn concurrent_insert_if_absent_reserves_memory_atomically() {
    let value = Arc::new(vec![b'x'; LARGE_VALUE_SIZE]);
    assert_concurrent_creates_respect_limit(LARGE_VALUE_SIZE, move |store, key| {
        store.insert_if_absent(key, &value).map(|_| ())
    });
}

#[test]
fn concurrent_atomic_increment_creates_reserve_memory_atomically() {
    assert_concurrent_creates_respect_limit(std::mem::size_of::<i64>(), |store, key| {
        store.atomic_increment(key, 1).map(|_| ())
    });
}

#[test]
fn concurrent_updates_reserve_growth_atomically() {
    assert_concurrent_growth_respects_limit(|store, key, value| {
        store.insert(key, value).map(|_| ())
    });
}

#[test]
fn concurrent_compare_and_swap_reserves_growth_atomically() {
    assert_concurrent_growth_respects_limit(|store, key, value| {
        match store.compare_and_swap(key, b"x", value)? {
            true => Ok(()),
            false => panic!("compare-and-swap lost its unchanged key"),
        }
    });
}

#[test]
fn shrinking_replacements_release_memory() {
    let key = 0_u64.to_le_bytes();
    let large = vec![b'x'; LARGE_VALUE_SIZE];
    let store = FeoxStore::new(None).unwrap();

    store.insert(&key, &large).unwrap();
    store.insert(&key, b"x").unwrap();
    assert_eq!(store.memory_usage(), record_size(key.len(), 1));

    assert!(store.compare_and_swap(&key, b"x", &large).unwrap());
    assert!(store.compare_and_swap(&key, &large, b"x").unwrap());
    assert_eq!(store.memory_usage(), record_size(key.len(), 1));
}

#[test]
fn recovery_preserves_usage_above_the_configured_limit() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_string_lossy().into_owned();
    let key = b"recovered";
    let value = vec![b'x'; LARGE_VALUE_SIZE];

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(2 * 1024 * 1024)
            .build()
            .unwrap();
        store.insert(key, &value).unwrap();
        store.flush().unwrap();
    }

    let store = FeoxStore::builder()
        .device_path(path)
        .max_memory(1)
        .build()
        .unwrap();
    assert!(store.memory_usage() > 1);
    store.insert(key, b"x").unwrap();
    assert_eq!(store.get(key).unwrap(), b"x");
    assert!(matches!(
        store.insert(b"new", b"value"),
        Err(FeoxError::OutOfMemory)
    ));
}

#[test]
fn concurrent_publish_and_delete_keep_accounting_balanced() {
    let store = Arc::new(FeoxStore::new(None).unwrap());
    let start = Arc::new(Barrier::new(CONCURRENT_WRITERS));
    let mut handles = Vec::with_capacity(CONCURRENT_WRITERS);

    for thread_index in 0..CONCURRENT_WRITERS {
        let store = Arc::clone(&store);
        let start = Arc::clone(&start);
        handles.push(thread::spawn(move || {
            start.wait();
            for iteration in 0..2_000 {
                if (thread_index + iteration) & 1 == 0 {
                    let _ = store.insert_if_absent(b"shared", b"value");
                } else {
                    let _ = store.delete(b"shared");
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
    let _ = store.delete(b"shared");

    assert!(store.is_empty());
    assert_eq!(store.memory_usage(), 0);
}
