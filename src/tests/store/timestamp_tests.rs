use crate::core::record::Record;
use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use bytes::Bytes;
use tempfile::NamedTempFile;

const FUTURE_OFFSET: u64 = 86_400_000_000_000;

fn record_timestamp(store: &FeoxStore, key: &[u8]) -> u64 {
    store
        .get_hash_table()
        .read(key, |_, record| record.timestamp)
        .unwrap()
}

fn assert_automatic_writes_remain_usable(store: &FeoxStore, key: &[u8]) {
    store.insert(key, b"a").unwrap();
    store.insert(key, b"b").unwrap();
    assert_ne!(record_timestamp(store, key), u64::MAX);
}

fn store_at_record_limit(key: &[u8]) -> FeoxStore {
    let limit = std::mem::size_of::<Record>() + key.len() + 1;
    let store = FeoxStore::builder().max_memory(limit).build().unwrap();
    store.insert(key, b"a").unwrap();
    store
}

#[test]
fn test_timestamp_conflict_resolution() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"ts_key";

    // Insert with timestamp 100
    store.insert_with_timestamp(key, b"v1", Some(100)).unwrap();

    // Update with higher timestamp succeeds
    store.insert_with_timestamp(key, b"v2", Some(200)).unwrap();
    assert_eq!(store.get(key).unwrap(), b"v2");

    // Update with lower timestamp fails
    let result = store.insert_with_timestamp(key, b"v3", Some(150));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Value should still be v2
    assert_eq!(store.get(key).unwrap(), b"v2");
}

#[test]
fn test_delete_with_timestamp() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"del_ts_key";

    store
        .insert_with_timestamp(key, b"value", Some(100))
        .unwrap();

    // Delete with older timestamp fails
    let result = store.delete_with_timestamp(key, Some(50));
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::OlderTimestamp));

    // Key should still exist
    assert!(store.contains_key(key));

    // Delete with newer timestamp succeeds
    store.delete_with_timestamp(key, Some(200)).unwrap();
    assert!(!store.contains_key(key));

    store
        .insert_with_timestamp(key, b"recreated", Some(150))
        .unwrap();
    store
        .insert_with_timestamp(key, b"newer", Some(300))
        .unwrap();
    assert_eq!(store.get(key).unwrap(), b"newer");
}

#[test]
fn test_automatic_timestamps_increase_for_the_same_key() {
    let store = FeoxStore::new(None).unwrap();
    let key: &[u8] = b"automatic_timestamp";

    store.insert(key, b"0").unwrap();
    let mut timestamp = store
        .get_hash_table()
        .read(key, |_, record| record.timestamp)
        .unwrap();

    for value in 1_u64..100 {
        store.insert(key, &value.to_le_bytes()).unwrap();
        let next = store
            .get_hash_table()
            .read(key, |_, record| record.timestamp)
            .unwrap();
        assert!(next > timestamp);
        timestamp = next;
    }
}

#[test]
fn automatic_timestamps_follow_explicit_publications() {
    let store = FeoxStore::new(None).unwrap();

    let create_key = b"explicit_create";
    let create_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .insert_with_timestamp(create_key, b"old", Some(create_timestamp))
        .unwrap();
    store.insert(create_key, b"new").unwrap();
    assert!(record_timestamp(&store, create_key) > create_timestamp);

    let insert_key = b"explicit_insert";
    store.insert(insert_key, b"old").unwrap();
    let insert_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .insert_with_timestamp(insert_key, b"middle", Some(insert_timestamp))
        .unwrap();
    store.insert(insert_key, b"new").unwrap();
    assert!(record_timestamp(&store, insert_key) > insert_timestamp);

    let counter_key = b"explicit_counter";
    store.insert(counter_key, &0_i64.to_le_bytes()).unwrap();
    let counter_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .atomic_increment_with_timestamp(counter_key, 1, Some(counter_timestamp))
        .unwrap();
    assert_eq!(store.atomic_increment(counter_key, 1).unwrap(), 2);
    assert!(record_timestamp(&store, counter_key) > counter_timestamp);

    let counter_create_key = b"explicit_counter_create";
    let counter_create_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .atomic_increment_with_timestamp(counter_create_key, 1, Some(counter_create_timestamp))
        .unwrap();
    assert_eq!(store.atomic_increment(counter_create_key, 1).unwrap(), 2);
    assert!(record_timestamp(&store, counter_create_key) > counter_create_timestamp);

    let cas_key = b"explicit_cas";
    store.insert(cas_key, b"old").unwrap();
    let cas_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    assert!(store
        .compare_and_swap_with_timestamp(cas_key, b"old", b"middle", Some(cas_timestamp))
        .unwrap());
    assert!(store.compare_and_swap(cas_key, b"middle", b"new").unwrap());
    assert!(record_timestamp(&store, cas_key) > cas_timestamp);

    let delete_key = b"explicit_delete";
    store.insert(delete_key, b"old").unwrap();
    let delete_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .delete_with_timestamp(delete_key, Some(delete_timestamp))
        .unwrap();
    store.insert(delete_key, b"new").unwrap();
    assert!(record_timestamp(&store, delete_key) > delete_timestamp);

    let bytes_key = b"explicit_bytes";
    store.insert(bytes_key, b"old").unwrap();
    let bytes_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .insert_bytes_with_timestamp(
            bytes_key,
            Bytes::from_static(b"middle"),
            Some(bytes_timestamp),
        )
        .unwrap();
    store
        .insert_bytes(bytes_key, Bytes::from_static(b"new"))
        .unwrap();
    assert!(record_timestamp(&store, bytes_key) > bytes_timestamp);

    let bytes_create_key = b"explicit_bytes_create";
    let bytes_create_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .insert_bytes_with_timestamp(
            bytes_create_key,
            Bytes::from_static(b"old"),
            Some(bytes_create_timestamp),
        )
        .unwrap();
    store
        .insert_bytes(bytes_create_key, Bytes::from_static(b"new"))
        .unwrap();
    assert!(record_timestamp(&store, bytes_create_key) > bytes_create_timestamp);

    let json_key = b"explicit_json";
    store.insert(json_key, br#"{"value":0}"#).unwrap();
    let json_timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
    store
        .json_patch_with_timestamp(
            json_key,
            br#"[{"op":"replace","path":"/value","value":1}]"#,
            Some(json_timestamp),
        )
        .unwrap();
    store
        .json_patch(json_key, br#"[{"op":"replace","path":"/value","value":2}]"#)
        .unwrap();
    assert!(record_timestamp(&store, json_key) > json_timestamp);
}

#[test]
fn failed_explicit_timestamps_do_not_advance_the_clock() {
    {
        let key = b"insert_failure";
        let store = store_at_record_limit(key);
        assert!(matches!(
            store.insert_with_timestamp(key, b"bb", Some(u64::MAX)),
            Err(FeoxError::OutOfMemory)
        ));
        assert_automatic_writes_remain_usable(&store, key);
    }

    {
        let key = b"bytes_failure";
        let store = store_at_record_limit(key);
        assert!(matches!(
            store.insert_bytes_with_timestamp(key, Bytes::from_static(b"bb"), Some(u64::MAX)),
            Err(FeoxError::OutOfMemory)
        ));
        assert_automatic_writes_remain_usable(&store, key);
    }

    {
        let store = FeoxStore::new(None).unwrap();
        assert!(matches!(
            store.delete_with_timestamp(b"delete_failure", Some(u64::MAX)),
            Err(FeoxError::KeyNotFound)
        ));
        assert_automatic_writes_remain_usable(&store, b"delete_failure");
    }

    {
        let store = FeoxStore::new(None).unwrap();
        store.insert(b"atomic_failure", b"x").unwrap();
        assert!(matches!(
            store.atomic_increment_with_timestamp(b"atomic_failure", 1, Some(u64::MAX)),
            Err(FeoxError::InvalidOperation)
        ));
        assert_automatic_writes_remain_usable(&store, b"atomic_failure");
    }

    {
        let key = b"cas_failure";
        let store = store_at_record_limit(key);
        assert!(matches!(
            store.compare_and_swap_with_timestamp(key, b"a", b"bb", Some(u64::MAX)),
            Err(FeoxError::OutOfMemory)
        ));
        assert_automatic_writes_remain_usable(&store, key);
    }

    {
        let store = FeoxStore::new(None).unwrap();
        assert!(matches!(
            store.json_patch_with_timestamp(
                b"json_failure",
                br#"[{"op":"replace","path":"/value","value":1}]"#,
                Some(u64::MAX),
            ),
            Err(FeoxError::KeyNotFound)
        ));
        assert_automatic_writes_remain_usable(&store, b"json_failure");
    }
}

#[test]
fn automatic_timestamps_follow_recovered_timestamps() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_string_lossy().into_owned();
    let key = b"recovered_timestamp";
    let timestamp;

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(2 * 1024 * 1024)
            .build()
            .unwrap();
        timestamp = store.get_timestamp_pub().saturating_add(FUTURE_OFFSET);
        store
            .insert_with_timestamp(key, b"old", Some(timestamp))
            .unwrap();
        store.flush().unwrap();
    }

    let store = FeoxStore::builder().device_path(path).build().unwrap();
    store.insert(key, b"new").unwrap();
    assert!(record_timestamp(&store, key) > timestamp);
}

#[test]
fn terminal_timestamp_does_not_exhaust_colliding_keys() {
    let store = FeoxStore::new(None).unwrap();
    let terminal_key = b"terminal";
    let terminal_shard = store.timestamp_shard_for_test(terminal_key);
    let colliding_key = (0_u64..)
        .map(|candidate| candidate.to_le_bytes())
        .find(|candidate| {
            candidate.as_slice() != terminal_key
                && store.timestamp_shard_for_test(candidate) == terminal_shard
        })
        .unwrap();

    store
        .insert_with_timestamp(terminal_key, b"terminal", Some(u64::MAX))
        .unwrap();
    assert!(matches!(
        store.insert(terminal_key, b"new"),
        Err(FeoxError::OlderTimestamp)
    ));

    store.insert(&colliding_key, b"first").unwrap();
    store.insert(&colliding_key, b"second").unwrap();
    assert_eq!(store.get(&colliding_key).unwrap(), b"second");
}
