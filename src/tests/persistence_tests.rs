use crate::constants::{FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK, SECTOR_MARKER};
use crate::core::record::Record;
use crate::core::store::FeoxStore;
use crate::storage::format::{FormatV2, RecordFormat};
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

const TEST_DEVICE_SIZE: u64 = 2 * 1024 * 1024;
const CRASH_HELPER: &str = "tests::persistence_tests::persistent_update_crash_helper";

#[test]
fn test_basic_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    // Create store and insert data
    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();

        store.insert(b"persist_key", b"persist_value").unwrap();
        store.insert(b"another_key", b"another_value").unwrap();

        store.flush().unwrap();
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
    store.flush_all().unwrap();

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
    store.flush().unwrap();

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
        store.flush_all().unwrap();
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

    store.flush_all().unwrap();
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

        store.flush_all().unwrap();
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

        store.flush_all().unwrap();
    }

    // Verify only latest value persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let value = store.get(b"update_key").unwrap();
        assert_eq!(value, b"final_value");
    }
}

#[test]
fn test_insert_if_absent_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();
        assert!(store.insert_if_absent(b"job", b"first").unwrap());
        assert!(!store.insert_if_absent(b"job", b"second").unwrap());
        store.flush().unwrap();
    }

    let reopened = FeoxStore::new(Some(path)).unwrap();
    assert_eq!(reopened.get(b"job").unwrap(), b"first");
}

#[test]
fn test_persistent_update_keeps_old_value_before_replacement_write() {
    assert_update_crash_recovery("before_replacement_write", b"old");
}

#[test]
fn test_persistent_update_keeps_new_value_after_replacement_write() {
    assert_update_crash_recovery("after_replacement_write", b"new");
}

#[test]
fn persistent_update_crash_helper() {
    let Ok(path) = std::env::var("FEOX_TEST_CRASH_PATH") else {
        return;
    };

    let store = FeoxStore::builder()
        .device_path(path)
        .file_size(TEST_DEVICE_SIZE)
        .build()
        .unwrap();
    store.insert(b"state", b"old").unwrap();
    store.flush().unwrap();
    store.insert(b"state", b"new").unwrap();
    store.flush().unwrap();
    panic!("crash point was not reached");
}

fn assert_update_crash_recovery(point: &str, expected: &[u8]) {
    let temp_file = NamedTempFile::new().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", CRASH_HELPER, "--nocapture"])
        .env("FEOX_TEST_CRASH_PATH", temp_file.path())
        .env("FEOX_TEST_CRASH_POINT", point)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(86));
    let reopened = FeoxStore::new(Some(temp_file.path().to_string_lossy().into_owned())).unwrap();
    assert_eq!(reopened.get(b"state").unwrap(), expected);
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

        store.flush_all().unwrap();
    }

    // Verify counter value persisted
    {
        let store = FeoxStore::new(Some(path)).unwrap();

        let value = store.atomic_increment(b"counter", 0).unwrap();
        assert_eq!(value, 100);
    }
}

#[test]
fn test_atomic_operations_reuse_versioned_cache() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = FeoxStore::new(Some(path)).unwrap();

    store.insert(b"control", b"stable:1").unwrap();
    store.flush_all().unwrap();

    assert_eq!(store.get(b"control").unwrap(), b"stable:1");
    let reads_after_get = store.stats().disk_reads;

    assert!(store
        .compare_and_swap(b"control", b"stable:1", b"applying:2")
        .unwrap());
    assert_eq!(store.stats().disk_reads, reads_after_get);
}

#[test]
fn test_superseded_buffered_record_does_not_reappear_after_reopen() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(TEST_DEVICE_SIZE)
            .build()
            .unwrap();
        store.insert(b"hole", b"value").unwrap();
        store.flush().unwrap();

        let zero = 0_i64.to_le_bytes();
        assert_eq!(store.atomic_increment(b"control", 0).unwrap(), 0);
        assert!(store
            .compare_and_swap(b"control", &zero, b"first-state")
            .unwrap());
        store.flush().unwrap();

        store.delete(b"hole").unwrap();
        store.flush().unwrap();
        assert!(store
            .compare_and_swap(b"control", b"first-state", b"current-state")
            .unwrap());
        store.flush().unwrap();
    }

    let reopened = FeoxStore::new(Some(path)).unwrap();
    assert_eq!(reopened.get(b"control").unwrap(), b"current-state");
}

#[test]
fn test_recovery_prefers_newest_timestamp_over_sector_order() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(TEST_DEVICE_SIZE)
            .build()
            .unwrap();
        store.flush().unwrap();
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    write_test_record(
        &mut file,
        FEOX_DATA_START_BLOCK,
        b"duplicate",
        b"newest",
        200,
    );
    write_test_record(
        &mut file,
        FEOX_DATA_START_BLOCK + 1,
        b"duplicate",
        b"stale",
        100,
    );
    file.sync_all().unwrap();
    drop(file);

    let reopened = FeoxStore::new(Some(path)).unwrap();
    assert_eq!(reopened.get(b"duplicate").unwrap(), b"newest");
    assert_eq!(reopened.len(), 1);
}

fn write_test_record(
    file: &mut std::fs::File,
    sector: u64,
    key: &[u8],
    value: &[u8],
    timestamp: u64,
) {
    let format = FormatV2;
    let record = Record::new(key.to_vec(), value.to_vec(), timestamp);
    let mut bytes = Vec::with_capacity(FEOX_BLOCK_SIZE);
    bytes.extend_from_slice(&SECTOR_MARKER.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&format.serialize_record(&record, true));
    assert!(bytes.len() <= FEOX_BLOCK_SIZE);
    bytes.resize(FEOX_BLOCK_SIZE, 0);
    file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
        .unwrap();
    file.write_all(&bytes).unwrap();
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

        store.flush_all().unwrap();
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
