use crate::constants::{
    FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK, FEOX_METADATA_BACKUP_BLOCK, MAX_DEVICE_SIZE,
    SECTOR_MARKER,
};
use crate::core::record::Record;
use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use crate::storage::format::{get_format_ref, FormatV1, FormatV2, RecordFormat};
use crate::storage::metadata::Metadata;
use crate::test_hooks::{fault, gate, RECORD_WRITE};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::Command;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

const TEST_DEVICE_SIZE: u64 = 2 * 1024 * 1024;
const CRASH_HELPER: &str = "tests::persistence_tests::persistent_update_crash_helper";

#[test]
fn invalid_new_device_sizes_are_rejected_before_resize() {
    let reserved_size = FEOX_DATA_START_BLOCK * FEOX_BLOCK_SIZE as u64;
    for size in [
        reserved_size,
        TEST_DEVICE_SIZE + 1,
        MAX_DEVICE_SIZE + FEOX_BLOCK_SIZE as u64,
    ] {
        let temp_file = NamedTempFile::new().unwrap();
        let result = FeoxStore::builder()
            .hash_bits(4)
            .device_path(temp_file.path().to_string_lossy().into_owned())
            .file_size(size)
            .build();
        assert!(matches!(result, Err(FeoxError::InvalidDevice)));
        assert_eq!(temp_file.as_file().metadata().unwrap().len(), 0);
    }
}

#[test]
fn invalid_existing_device_sizes_are_rejected() {
    let reserved_size = FEOX_DATA_START_BLOCK * FEOX_BLOCK_SIZE as u64;
    for size in [reserved_size, TEST_DEVICE_SIZE + 1] {
        let temp_file = NamedTempFile::new().unwrap();
        temp_file.as_file().set_len(size).unwrap();
        let result = FeoxStore::builder()
            .hash_bits(4)
            .device_path(temp_file.path().to_string_lossy().into_owned())
            .build();
        assert!(matches!(result, Err(FeoxError::InvalidDevice)));
    }
}

#[test]
fn sparse_minimum_device_is_initialized() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_string_lossy().into_owned();
    temp_file
        .as_file()
        .set_len((FEOX_DATA_START_BLOCK + 1) * FEOX_BLOCK_SIZE as u64)
        .unwrap();

    {
        let store = FeoxStore::builder()
            .hash_bits(4)
            .device_path(path.clone())
            .build()
            .unwrap();
        store.insert(b"key", b"value").unwrap();
    }

    let reopened = FeoxStore::builder()
        .hash_bits(4)
        .device_path(path)
        .build()
        .unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), b"value");
}

#[test]
fn record_serialization_appends_the_stable_disk_layout() {
    let record = Record::new_with_timestamp_ttl(b"key".to_vec(), b"value".to_vec(), 41, 73);

    for (format, has_ttl) in [
        (&FormatV1 as &dyn RecordFormat, false),
        (&FormatV2 as &dyn RecordFormat, true),
    ] {
        let mut data = vec![0xa5];
        format.serialize_record_into(&record, true, &mut data);

        let mut expected = vec![0xa5];
        expected.extend_from_slice(&3_u16.to_le_bytes());
        expected.extend_from_slice(b"key");
        expected.extend_from_slice(&5_u64.to_le_bytes());
        expected.extend_from_slice(&41_u64.to_le_bytes());
        if has_ttl {
            expected.extend_from_slice(&73_u64.to_le_bytes());
        }
        expected.extend_from_slice(b"value");

        assert_eq!(data, expected);
        assert_eq!(
            &data[1..],
            format.serialize_record(&record, true).as_slice()
        );
    }
}

#[test]
fn record_formats_are_shared() {
    assert!(std::ptr::eq(get_format_ref(1), get_format_ref(1)));
    assert!(std::ptr::eq(get_format_ref(3), get_format_ref(3)));
}

#[test]
fn invalid_metadata_is_rejected_without_overwriting_the_file() {
    let temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file().set_len(TEST_DEVICE_SIZE).unwrap();
    let mut file = temp_file.reopen().unwrap();
    file.write_all(b"NOTFEOX!").unwrap();
    file.sync_all().unwrap();

    let result = FeoxStore::builder()
        .device_path(temp_file.path().to_string_lossy().into_owned())
        .build();
    assert!(matches!(result, Err(FeoxError::InvalidMetadata)));

    file.seek(SeekFrom::Start(0)).unwrap();
    let mut signature = [0; 8];
    file.read_exact(&mut signature).unwrap();
    assert_eq!(&signature, b"NOTFEOX!");
}

#[test]
fn zero_metadata_does_not_hide_stale_reserved_blocks() {
    let temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file().set_len(TEST_DEVICE_SIZE).unwrap();
    let mut file = temp_file.reopen().unwrap();
    file.seek(SeekFrom::Start(FEOX_BLOCK_SIZE as u64)).unwrap();
    file.write_all(b"stale").unwrap();
    file.sync_all().unwrap();

    let result = FeoxStore::builder()
        .device_path(temp_file.path().to_string_lossy().into_owned())
        .build();
    assert!(matches!(result, Err(FeoxError::InvalidMetadata)));

    file.seek(SeekFrom::Start(FEOX_BLOCK_SIZE as u64)).unwrap();
    let mut stale = [0; 5];
    file.read_exact(&mut stale).unwrap();
    assert_eq!(&stale, b"stale");
}

#[test]
fn zero_metadata_does_not_hide_stale_data_blocks() {
    let temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file().set_len(TEST_DEVICE_SIZE).unwrap();
    let mut file = temp_file.reopen().unwrap();
    let offset = FEOX_DATA_START_BLOCK * FEOX_BLOCK_SIZE as u64;
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(b"stale").unwrap();
    file.sync_all().unwrap();

    let result = FeoxStore::builder()
        .device_path(temp_file.path().to_string_lossy().into_owned())
        .build();
    assert!(matches!(result, Err(FeoxError::InvalidMetadata)));

    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut stale = [0; 5];
    file.read_exact(&mut stale).unwrap();
    assert_eq!(&stale, b"stale");
}

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
fn drop_retries_a_transient_final_flush_failure() {
    let _session = gate::session();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_string_lossy().into_owned();
    let store = FeoxStore::builder()
        .hash_bits(4)
        .device_path(path.clone())
        .file_size(TEST_DEVICE_SIZE)
        .build()
        .unwrap();
    let fault = fault::fail_next(
        RECORD_WRITE,
        store.get_write_buffer().unwrap().fault_scope(),
        3,
    );

    store.insert(b"key", b"value").unwrap();
    drop(store);
    assert_eq!(fault.consumed(), 3);
    drop(fault);

    let reopened = FeoxStore::builder()
        .hash_bits(4)
        .device_path(path)
        .build()
        .unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), b"value");
}

#[test]
fn drop_metadata_matches_rebuilt_store_stats() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_string_lossy().into_owned();
    {
        let store = FeoxStore::builder()
            .hash_bits(4)
            .device_path(path.clone())
            .file_size(TEST_DEVICE_SIZE)
            .build()
            .unwrap();
        store.insert(b"key", b"value").unwrap();
    }

    let dropped = persisted_metadata(&path);
    assert_eq!(dropped.total_records, 1);
    assert_eq!(dropped.total_size, FEOX_BLOCK_SIZE as u64);

    {
        let reopened = FeoxStore::builder()
            .hash_bits(4)
            .device_path(path.clone())
            .build()
            .unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), b"value");
        reopened.flush_all().unwrap();
    }

    let rebuilt = persisted_metadata(&path);
    assert_eq!(dropped.total_records, rebuilt.total_records);
    assert_eq!(dropped.total_size, rebuilt.total_size);
    assert_eq!(dropped.fragmentation, rebuilt.fragmentation);
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
    crate::storage::seq_token::stamp_seq_token(&mut bytes, sector, &format);
    file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
        .unwrap();
    file.write_all(&bytes).unwrap();
}

fn persisted_metadata(path: &str) -> Metadata {
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    let mut primary = vec![0_u8; FEOX_BLOCK_SIZE];
    file.read_exact(&mut primary).unwrap();
    file.seek(SeekFrom::Start(
        FEOX_METADATA_BACKUP_BLOCK * FEOX_BLOCK_SIZE as u64,
    ))
    .unwrap();
    let mut backup = vec![0_u8; FEOX_BLOCK_SIZE];
    file.read_exact(&mut backup).unwrap();

    let primary = Metadata::from_bytes(&primary);
    let backup = Metadata::from_bytes(&backup);
    match (primary, backup) {
        (Some(primary), Some(backup)) if backup.generation() > primary.generation() => backup,
        (Some(primary), _) => primary,
        (None, Some(backup)) => backup,
        (None, None) => panic!("valid persisted metadata"),
    }
}

fn persisted_disk_usage(path: &str) -> u64 {
    persisted_metadata(path).total_size
}

#[test]
fn recovery_rebuilds_disk_usage_without_double_counting() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::new(Some(path.clone())).unwrap();
        store.insert(b"live", b"value").unwrap();
        store.flush_all().unwrap();
    }

    for _ in 0..3 {
        let store = FeoxStore::new(Some(path.clone())).unwrap();
        assert_eq!(store.get(b"live").unwrap(), b"value");
        store.flush_all().unwrap();
        assert_eq!(persisted_disk_usage(&path), FEOX_BLOCK_SIZE as u64);
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

#[test]
fn concurrent_same_key_updates_do_not_leak_extents() {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    const TOTAL: usize = 20_000;
    const ROUNDS: usize = 20;
    const SEED_TS: u64 = 1;
    const FIRST_WRITE_TS: u64 = SEED_TS + 1;

    fn run(path: &str, threads: usize) -> u64 {
        let store = Arc::new(
            FeoxStore::builder()
                .device_path(path.to_string())
                .file_size(64 * 1024 * 1024)
                .enable_caching(false)
                .build()
                .unwrap(),
        );
        store
            .insert_with_timestamp(b"hot", b"seed", Some(SEED_TS))
            .unwrap();

        let value = vec![b'v'; 100];
        let next_timestamp = Arc::new(AtomicU64::new(FIRST_WRITE_TS));
        let successful_updates = Arc::new(AtomicUsize::new(0));
        let unexpected_failures = Arc::new(AtomicUsize::new(0));
        let per_round = TOTAL / (threads * ROUNDS);
        let round_barrier = Arc::new(Barrier::new(threads + 1));
        let mut handles = Vec::new();

        for _ in 0..threads {
            let store = Arc::clone(&store);
            let value = value.clone();
            let next_timestamp = Arc::clone(&next_timestamp);
            let successful_updates = Arc::clone(&successful_updates);
            let unexpected_failures = Arc::clone(&unexpected_failures);
            let round_barrier = Arc::clone(&round_barrier);
            handles.push(thread::spawn(move || {
                for _ in 0..ROUNDS {
                    for _ in 0..per_round {
                        let timestamp = next_timestamp.fetch_add(1, Ordering::Relaxed);
                        match store.insert_with_timestamp(b"hot", &value, Some(timestamp)) {
                            Ok(_) => {
                                successful_updates.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(FeoxError::OlderTimestamp) => {}
                            Err(_) => {
                                unexpected_failures.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    round_barrier.wait();
                    round_barrier.wait();
                }
            }));
        }

        for _ in 0..ROUNDS {
            round_barrier.wait();
            store.flush().unwrap();
            round_barrier.wait();
        }
        for handle in handles {
            handle.join().unwrap();
        }
        store.flush().unwrap();

        assert!(
            successful_updates.load(Ordering::Relaxed) > 0,
            "no concurrent update succeeded"
        );
        assert_eq!(
            unexpected_failures.load(Ordering::Relaxed),
            0,
            "some writes failed unexpectedly"
        );
        assert_eq!(store.len(), 1, "one key must yield one record");
        assert!(
            store.stats().writes_flushed >= ROUNDS as u64,
            "test did not persist enough generations to exercise reclamation"
        );
        persisted_disk_usage(path)
    }

    let concurrent_file = NamedTempFile::new().unwrap();
    let disk_usage = run(concurrent_file.path().to_str().unwrap(), 4);

    assert_eq!(
        disk_usage, FEOX_BLOCK_SIZE as u64,
        "one live single-block record must consume exactly one extent"
    );

    {
        let reopened =
            FeoxStore::new(Some(concurrent_file.path().to_str().unwrap().to_string())).unwrap();
        assert_eq!(reopened.len(), 1);
        assert_eq!(reopened.get(b"hot").unwrap().len(), 100);
    }
}
