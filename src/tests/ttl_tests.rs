use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use crate::storage::metadata::Metadata;
use bytes::Bytes;
use std::collections::HashSet;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

const TTL_TEST_DEVICE_SIZE: u64 = 2 * 1024 * 1024;

#[test]
fn update_ttl_advances_conflict_versions_in_all_modes() {
    let temp_file = NamedTempFile::new().unwrap();
    let memory = FeoxStore::builder().enable_ttl(true).build().unwrap();
    let persistent = FeoxStore::builder()
        .device_path(temp_file.path().to_string_lossy().into_owned())
        .file_size(TTL_TEST_DEVICE_SIZE)
        .enable_ttl(true)
        .build()
        .unwrap();

    for store in [&memory, &persistent] {
        let timestamp = store.get_timestamp_pub();
        store
            .insert_with_ttl_and_timestamp(b"key", b"value", 60, Some(timestamp))
            .unwrap();
        store.update_ttl(b"key", 120).unwrap();
        assert!(matches!(
            store.insert_with_timestamp(b"key", b"stale", Some(timestamp.checked_add(1).unwrap())),
            Err(FeoxError::OlderTimestamp)
        ));
    }
}

#[test]
fn update_ttl_does_not_resurrect_expired_key() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();
    let key = b"expired";

    store
        .insert_with_ttl_and_timestamp(key, b"value", 1, Some(1))
        .unwrap();

    assert!(matches!(
        store.update_ttl(key, 60),
        Err(FeoxError::KeyNotFound)
    ));
    assert!(matches!(store.persist(key), Err(FeoxError::KeyNotFound)));
    assert!(matches!(store.get(key), Err(FeoxError::KeyNotFound)));
}

#[test]
fn test_legacy_v1_rejects_ttl_mutations() {
    let temp_file = NamedTempFile::new().unwrap();
    temp_file.as_file().set_len(TTL_TEST_DEVICE_SIZE).unwrap();

    let mut metadata = Metadata::new();
    metadata.version = 1;
    metadata.device_size = TTL_TEST_DEVICE_SIZE;
    metadata.update();

    let mut file = temp_file.reopen().unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&metadata.encode()).unwrap();
    file.sync_all().unwrap();

    let store = FeoxStore::builder()
        .device_path(temp_file.path().to_string_lossy().into_owned())
        .enable_ttl(true)
        .build()
        .unwrap();
    store.insert(b"existing", b"value").unwrap();
    store.flush().unwrap();

    let update_result = store.update_ttl(b"existing", 60);
    assert!(
        matches!(update_result, Err(FeoxError::Unsupported)),
        "{update_result:?}"
    );
    assert!(matches!(
        store.persist(b"existing"),
        Err(FeoxError::Unsupported)
    ));
    assert!(matches!(
        store.insert_with_ttl(b"new", b"value", 60),
        Err(FeoxError::Unsupported)
    ));
    assert_eq!(store.get(b"existing").unwrap(), b"value");
}

#[test]
fn test_insert_with_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with 1 second TTL
    store.insert_with_ttl(b"key1", b"value1", 1).unwrap();

    // Should be retrievable immediately
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value1");

    // Wait for expiry
    thread::sleep(Duration::from_millis(1100));

    // Should be expired now
    let result = store.get(b"key1");
    assert!(result.is_err());
}

#[test]
fn test_get_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with 10 second TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Check TTL
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());
    let ttl_seconds = ttl.unwrap();
    assert!(ttl_seconds > 8 && ttl_seconds <= 10);

    // Insert without TTL
    store.insert(b"key2", b"value2").unwrap();
    let ttl = store.get_ttl(b"key2").unwrap();
    assert!(ttl.is_none());
}

#[test]
fn test_update_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert without TTL
    store.insert(b"key1", b"value1").unwrap();

    // Add TTL
    store.update_ttl(b"key1", 5).unwrap();
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());

    // Remove TTL (persist)
    store.persist(b"key1").unwrap();
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_none());
}

#[test]
fn test_ttl_preserves_value() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store
        .insert_with_ttl(b"key1", b"original_value", 10)
        .unwrap();

    // Update TTL shouldn't change value
    store.update_ttl(b"key1", 20).unwrap();

    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"original_value");
}

#[test]
fn test_update_ttl_is_durable_after_value_offload() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let persisted_expiry;

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(TTL_TEST_DEVICE_SIZE)
            .enable_ttl(true)
            .build()
            .unwrap();
        store.insert(b"renewed", b"value").unwrap();
        store.flush().unwrap();

        let flushed = store
            .get_hash_table()
            .read(b"renewed".as_slice(), |_, record| Arc::clone(record))
            .unwrap();
        assert!(flushed.get_value().is_none());

        store.update_ttl(b"renewed", 60).unwrap();
        store.flush().unwrap();
        persisted_expiry = store
            .get_hash_table()
            .read(b"renewed".as_slice(), |_, record| {
                record.ttl_expiry.load(Ordering::Acquire)
            })
            .unwrap();
        assert_ne!(persisted_expiry, 0);
    }

    let reopened = FeoxStore::builder()
        .device_path(path)
        .enable_ttl(true)
        .build()
        .unwrap();
    assert_eq!(reopened.get(b"renewed").unwrap(), b"value");
    let recovered_expiry = reopened
        .get_hash_table()
        .read(b"renewed".as_slice(), |_, record| {
            record.ttl_expiry.load(Ordering::Acquire)
        })
        .unwrap();
    assert_eq!(recovered_expiry, persisted_expiry);
}

#[test]
fn update_ttl_defers_offloaded_value_read() {
    let _session = crate::test_hooks::gate::session();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = FeoxStore::builder()
        .device_path(path)
        .file_size(TTL_TEST_DEVICE_SIZE)
        .enable_ttl(true)
        .build()
        .unwrap();
    let value = vec![b'x'; 64 * 1024];

    store.insert(b"renewed", &value).unwrap();
    store.flush().unwrap();

    let gate = _session.arm_for_thread(
        crate::test_hooks::AFTER_SECTOR_LOAD,
        thread::current().id(),
        0,
    );
    store.update_ttl(b"renewed", 60).unwrap();
    assert_eq!(gate.arrivals(), 0);

    store.update_ttl(b"renewed", 120).unwrap();
    assert_eq!(gate.arrivals(), 0);
    store.flush().unwrap();
    assert_eq!(store.get(b"renewed").unwrap(), value);
}

#[test]
fn deferred_ttl_source_tracks_the_generation_flushed_concurrently() {
    let session = crate::test_hooks::gate::session();
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = Arc::new(
        FeoxStore::builder()
            .device_path(path.clone())
            .file_size(TTL_TEST_DEVICE_SIZE)
            .enable_caching(false)
            .enable_ttl(true)
            .build()
            .unwrap(),
    );
    let value = vec![b'x'; 64 * 1024];

    store.insert(b"renewed", &value).unwrap();
    store.flush().unwrap();
    store.update_ttl(b"renewed", 60).unwrap();

    let predecessor = store
        .get_hash_table()
        .read(b"renewed".as_slice(), |_, record| Arc::clone(record))
        .unwrap();
    assert_eq!(predecessor.sector.load(Ordering::Acquire), 0);

    let gate = Arc::new(session.arm_for_thread(
        crate::test_hooks::AFTER_TTL_DEFERRED_SOURCE,
        thread::current().id(),
        1,
    ));
    let flush_store = Arc::clone(&store);
    let flush_gate = Arc::clone(&gate);
    let flush = thread::spawn(move || {
        assert!(flush_gate.wait_for_arrivals(1, Duration::from_secs(5)));
        let result = flush_store.flush();
        assert!(flush_gate.release_and_drain(Duration::from_secs(5)));
        result
    });

    store.update_ttl(b"renewed", 120).unwrap();
    flush.join().unwrap().unwrap();
    assert_ne!(predecessor.sector.load(Ordering::Acquire), 0);

    store.flush().unwrap();
    assert_eq!(store.get(b"renewed").unwrap(), value);

    drop(predecessor);
    drop(store);
    let reopened = FeoxStore::builder()
        .device_path(path)
        .enable_caching(false)
        .enable_ttl(true)
        .build()
        .unwrap();
    assert_eq!(reopened.get(b"renewed").unwrap(), value);
}

#[test]
fn test_persist_is_durable_after_value_offload() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(TTL_TEST_DEVICE_SIZE)
            .enable_ttl(true)
            .build()
            .unwrap();
        store.insert_with_ttl(b"persistent", b"value", 60).unwrap();
        store.flush().unwrap();

        let flushed = store
            .get_hash_table()
            .read(b"persistent".as_slice(), |_, record| Arc::clone(record))
            .unwrap();
        assert!(flushed.get_value().is_none());

        store.persist(b"persistent").unwrap();
        store.flush().unwrap();
    }

    let reopened = FeoxStore::builder()
        .device_path(path)
        .enable_ttl(true)
        .build()
        .unwrap();
    assert_eq!(reopened.get(b"persistent").unwrap(), b"value");
    assert_eq!(reopened.get_ttl(b"persistent").unwrap(), None);
}

#[test]
fn test_active_sweeper_removes_expired_record_after_renewal_is_rejected() {
    let _session = crate::test_hooks::gate::session();
    let store = Arc::new(FeoxStore::builder().enable_ttl(true).build().unwrap());
    store
        .insert_with_ttl_and_timestamp(b"sweeper-renew", b"old", 1, Some(1))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let sweeper_store = Arc::clone(&store);
    let sweeper_start = Arc::clone(&start);
    let sweeper = thread::spawn(move || {
        sweeper_start.wait();
        crate::core::ttl_sweep::sample_and_expire_for_test(&sweeper_store)
    });
    let gate = _session.arm_for_thread(
        crate::test_hooks::TTL_AFTER_EXPIRED_SAMPLE,
        sweeper.thread().id(),
        1,
    );

    start.wait();
    assert!(gate.wait_for_arrivals(1, Duration::from_secs(5)));
    assert!(matches!(
        store.update_ttl(b"sweeper-renew", 60),
        Err(FeoxError::KeyNotFound)
    ));
    assert!(gate.release_and_drain(Duration::from_secs(5)));

    assert_eq!(sweeper.join().unwrap().1, 1);
    assert!(matches!(
        store.get(b"sweeper-renew"),
        Err(FeoxError::KeyNotFound)
    ));
}

#[test]
fn test_active_sweeper_does_not_delete_replacement_record() {
    let _session = crate::test_hooks::gate::session();
    let store = Arc::new(FeoxStore::builder().enable_ttl(true).build().unwrap());
    store
        .insert_with_ttl_and_timestamp(b"sweeper-replace", b"old", 1, Some(1))
        .unwrap();

    let start = Arc::new(Barrier::new(2));
    let sweeper_store = Arc::clone(&store);
    let sweeper_start = Arc::clone(&start);
    let sweeper = thread::spawn(move || {
        sweeper_start.wait();
        crate::core::ttl_sweep::sample_and_expire_for_test(&sweeper_store)
    });
    let gate = _session.arm_for_thread(
        crate::test_hooks::TTL_AFTER_EXPIRED_SAMPLE,
        sweeper.thread().id(),
        1,
    );

    start.wait();
    assert!(gate.wait_for_arrivals(1, Duration::from_secs(5)));
    store.insert(b"sweeper-replace", b"new").unwrap();
    assert!(gate.release_and_drain(Duration::from_secs(5)));

    assert_eq!(sweeper.join().unwrap().1, 0);
    assert_eq!(store.get(b"sweeper-replace").unwrap(), b"new");
    assert_eq!(store.get_ttl(b"sweeper-replace").unwrap(), None);
}

#[test]
fn test_active_sweeper_allows_recreation() {
    let store = Arc::new(FeoxStore::builder().enable_ttl(true).build().unwrap());
    store
        .insert_with_ttl_and_timestamp(b"sweeper-delete", b"old", 1, Some(1))
        .unwrap();

    assert_eq!(
        crate::core::ttl_sweep::sample_and_expire_for_test(&store).1,
        1
    );
    store
        .insert_with_timestamp(b"sweeper-delete", b"newer", Some(2))
        .unwrap();
    assert_eq!(store.get(b"sweeper-delete").unwrap(), b"newer");
}

#[test]
fn ttl_sampler_is_bounded_and_filters_non_ttl_records() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();
    for index in 0..32 {
        let key = format!("ttl-{index}");
        store.insert_with_ttl(key.as_bytes(), b"value", 60).unwrap();
    }
    for index in 0..32 {
        let key = format!("plain-{index}");
        store.insert(key.as_bytes(), b"value").unwrap();
    }

    let sample = crate::core::ttl_sweep::sample_ttl_keys_for_test(&store, 7);
    assert_eq!(sample.len(), 7);
    assert_eq!(sample.iter().collect::<HashSet<_>>().len(), 7);
    assert!(sample.iter().all(|key| key.starts_with(b"ttl-")));

    let all = crate::core::ttl_sweep::sample_ttl_keys_for_test(&store, usize::MAX);
    assert_eq!(all.len(), 32);
    assert_eq!(all.iter().collect::<HashSet<_>>().len(), 32);
    assert!(crate::core::ttl_sweep::sample_ttl_keys_for_test(&store, 0).is_empty());
}

#[test]
fn test_expired_key_not_found() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // TTL of 0 means no expiry
    store.insert_with_ttl(b"ephemeral", b"data", 0).unwrap();

    // Should still be there
    let result = store.get(b"ephemeral");
    assert!(result.is_ok());

    // Test with 1 second TTL
    store.insert_with_ttl(b"ephemeral2", b"data", 1).unwrap();

    // Should be retrievable immediately
    assert!(store.get(b"ephemeral2").is_ok());

    // Wait for expiry
    thread::sleep(Duration::from_secs(2));

    // Should be expired now
    let result = store.get(b"ephemeral2");
    assert!(result.is_err());
}

#[test]
fn test_update_resets_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Update with new TTL
    store.insert_with_ttl(b"key1", b"value2", 20).unwrap();

    // Check new TTL is applied
    let ttl = store.get_ttl(b"key1").unwrap().unwrap();
    assert!(ttl > 15 && ttl <= 20);

    // Check value is updated
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value2");
}

#[test]
fn test_regular_insert_removes_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    store.insert_with_ttl(b"key1", b"value1", 10).unwrap();

    // Verify TTL is set
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());

    // Regular insert should remove TTL
    store.insert(b"key1", b"value2").unwrap();

    // Verify TTL is removed
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_none());

    // Value should be updated
    let value = store.get(b"key1").unwrap();
    assert_eq!(value, b"value2");

    // Wait to ensure it doesn't expire (since TTL was removed)
    thread::sleep(Duration::from_millis(100));
    assert!(store.get(b"key1").is_ok());
}

#[test]
fn test_ttl_operations_fail_when_disabled() {
    // Create store with TTL disabled (default)
    let store = FeoxStore::new(None).unwrap();

    // All TTL operations should return TtlNotEnabled error
    assert!(matches!(
        store.insert_with_ttl(b"key1", b"value1", 10),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.insert_with_ttl_and_timestamp(b"key2", b"value2", 10, None),
        Err(FeoxError::TtlNotEnabled)
    ));

    // Regular insert should work
    store.insert(b"key3", b"value3").unwrap();

    assert!(matches!(
        store.get_ttl(b"key3"),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.update_ttl(b"key3", 10),
        Err(FeoxError::TtlNotEnabled)
    ));

    assert!(matches!(
        store.persist(b"key3"),
        Err(FeoxError::TtlNotEnabled)
    ));

    // Regular operations should still work
    assert_eq!(store.get(b"key3").unwrap(), b"value3");
    store.delete(b"key3").unwrap();
}

#[test]
fn test_ttl_with_builder_explicit_disable() {
    // Explicitly disable TTL via builder
    let store = FeoxStore::builder().enable_ttl(false).build().unwrap();

    // TTL operations should fail
    assert!(matches!(
        store.insert_with_ttl(b"key1", b"value1", 10),
        Err(FeoxError::TtlNotEnabled)
    ));
}

#[test]
fn test_insert_bytes_with_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with 1 second TTL using Bytes
    let value = Bytes::from_static(b"value1");
    store.insert_bytes_with_ttl(b"key1", value, 1).unwrap();

    // Should be retrievable immediately
    let retrieved = store.get_bytes(b"key1").unwrap();
    assert_eq!(&retrieved[..], b"value1");

    // Wait for expiry
    thread::sleep(Duration::from_millis(1100));

    // Should be expired now
    let result = store.get(b"key1");
    assert!(result.is_err());
}

#[test]
fn test_insert_bytes_with_ttl_and_timestamp() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Get a base timestamp
    let base_timestamp = store.get_timestamp_pub();

    // Insert with TTL and explicit timestamp
    let value1 = Bytes::from(vec![1, 2, 3, 4]);
    store
        .insert_bytes_with_ttl_and_timestamp(b"key1", value1, 10, Some(base_timestamp))
        .unwrap();

    // Try to update with older timestamp - should fail
    let value2 = Bytes::from(vec![5, 6, 7, 8]);
    let result = store.insert_bytes_with_ttl_and_timestamp(
        b"key1",
        value2.clone(),
        10,
        Some(base_timestamp - 1000),
    );
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Update with newer timestamp - should succeed
    store
        .insert_bytes_with_ttl_and_timestamp(b"key1", value2, 10, Some(base_timestamp + 1000))
        .unwrap();

    let retrieved = store.get(b"key1").unwrap();
    assert_eq!(retrieved.as_slice(), &[5, 6, 7, 8]);
}

#[test]
fn test_insert_bytes_ttl_not_enabled() {
    // Create store without TTL enabled
    let store = FeoxStore::builder().enable_ttl(false).build().unwrap();

    let value = Bytes::from_static(b"value1");
    let result = store.insert_bytes_with_ttl(b"key1", value, 10);

    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::TtlNotEnabled));
}

#[test]
fn test_insert_bytes_preserves_ttl() {
    let store = FeoxStore::builder().enable_ttl(true).build().unwrap();

    // Insert with TTL
    let value1 = Bytes::from_static(b"value1");
    store.insert_bytes_with_ttl(b"key1", value1, 10).unwrap();

    // Check TTL is set
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_some());
    let ttl_seconds = ttl.unwrap();
    assert!(ttl_seconds > 8 && ttl_seconds <= 10);

    // Update with regular insert_bytes (should remove TTL)
    let value2 = Bytes::from_static(b"value2");
    store.insert_bytes(b"key1", value2).unwrap();

    // TTL should be removed
    let ttl = store.get_ttl(b"key1").unwrap();
    assert!(ttl.is_none());
}
