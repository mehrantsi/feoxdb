use crate::constants::{
    DELETION_MARKER, FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK, RETIREMENT_COMPLETE, SECTOR_MARKER,
};
use crate::core::record::Record;
use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use crate::storage::allocation_journal::{
    decode as decode_allocation_journal, ALLOCATION_JOURNAL_BLOCKS, ALLOCATION_JOURNAL_START_BLOCK,
};
use crate::storage::format::{
    fill_retirement_extent, get_format_ref, pending_retirement_block, retirement_block, FormatV2,
    RecordFormat,
};
use crate::storage::metadata::Metadata;
use crate::storage::seq_token::stamp_seq_token;
use crate::test_hooks::{fault, gate, AFTER_SECTOR_LOAD, RECORD_WRITE};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::Command;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::NamedTempFile;

const DEVICE_SIZE: u64 = 8 * 1024 * 1024;
const VALUE_LEN: usize = 300;
const VICTIM_KEY: &[u8] = b"victim";
const ALLOCATION_INTENT_HELPER: &str =
    "tests::stale_extent_tests::multi_block_allocation_intent_crash_helper";
const MULTI_BLOCK_UPDATE_HELPER: &str =
    "tests::stale_extent_tests::multi_block_update_crash_helper";

fn persistent_store(path: &str) -> Arc<FeoxStore> {
    Arc::new(
        FeoxStore::builder()
            .device_path(path.to_string())
            .file_size(DEVICE_SIZE)
            .enable_caching(false)
            .build()
            .unwrap(),
    )
}

fn persistent_ttl_store(path: &str) -> Arc<FeoxStore> {
    Arc::new(
        FeoxStore::builder()
            .device_path(path.to_string())
            .file_size(DEVICE_SIZE)
            .enable_caching(false)
            .enable_ttl(true)
            .build()
            .unwrap(),
    )
}

fn parked_reader(
    store: &Arc<FeoxStore>,
    session: &gate::Session,
) -> (
    gate::Armed,
    thread::JoinHandle<crate::error::Result<Vec<u8>>>,
) {
    let (start_tx, start_rx) = sync_channel(0);
    let reader_store = Arc::clone(store);
    let reader = thread::spawn(move || {
        start_rx.recv().unwrap();
        reader_store.get(VICTIM_KEY)
    });
    let armed = session.arm_for_thread(AFTER_SECTOR_LOAD, reader.thread().id(), 1);
    start_tx.send(()).unwrap();
    (armed, reader)
}

fn background_flush(
    store: &Arc<FeoxStore>,
) -> (Receiver<crate::error::Result<()>>, thread::JoinHandle<()>) {
    let flush_store = Arc::clone(store);
    let (flush_tx, flush_rx) = sync_channel(1);
    let flush = thread::spawn(move || {
        flush_tx.send(flush_store.flush()).unwrap();
    });
    (flush_rx, flush)
}

fn assert_flush_pending(flush_rx: &Receiver<crate::error::Result<()>>) {
    assert!(matches!(
        flush_rx.recv_timeout(Duration::from_millis(100)),
        Err(RecvTimeoutError::Timeout)
    ));
}

fn finish_flush(flush_rx: Receiver<crate::error::Result<()>>, flush: thread::JoinHandle<()>) {
    flush_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("flush did not complete after the reader released its extent")
        .unwrap();
    flush.join().unwrap();
}

fn assert_old_or_new_generation(value: &[u8]) {
    assert_eq!(value.len(), VALUE_LEN);
    let old = value.iter().all(|byte| *byte == b'V');
    let new = value.iter().all(|byte| *byte == b'W');
    assert!(
        old || new,
        "reader returned foreign or mixed bytes: first={:?} len={}",
        value.first(),
        value.len()
    );
}

fn serialized_record_sector(
    version: u32,
    sector: u64,
    key: &[u8],
    value: &[u8],
    timestamp: u64,
    stamp: bool,
) -> Vec<u8> {
    let bytes = serialized_record_extent(version, sector, key, value, timestamp, 0, stamp);
    assert_eq!(bytes.len(), FEOX_BLOCK_SIZE);
    bytes
}

fn serialized_record_extent(
    version: u32,
    sector: u64,
    key: &[u8],
    value: &[u8],
    timestamp: u64,
    ttl_expiry: u64,
    stamp: bool,
) -> Vec<u8> {
    let format = get_format_ref(version);
    let record = Record::new(key.to_vec(), value.to_vec(), timestamp);
    record.ttl_expiry.store(ttl_expiry, Ordering::Release);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SECTOR_MARKER.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&format.serialize_record(&record, true));
    let extent_size = bytes.len().div_ceil(FEOX_BLOCK_SIZE) * FEOX_BLOCK_SIZE;
    bytes.resize(extent_size, 0);
    if stamp {
        stamp_seq_token(&mut bytes, sector, format);
    }
    bytes
}

fn multi_block_value_with_continuation(version: u32, continuation: &[u8]) -> Vec<u8> {
    let format = get_format_ref(version);
    let value_offset = format.value_offset(VICTIM_KEY.len());
    let continuation_offset = FEOX_BLOCK_SIZE - value_offset;
    let mut value = vec![b'V'; FEOX_BLOCK_SIZE * 2];
    value[continuation_offset..continuation_offset + continuation.len()]
        .copy_from_slice(continuation);
    value
}

fn initialize_device(path: &str, version: u32) {
    let mut metadata = Metadata::new();
    metadata.version = version;
    metadata.device_size = DEVICE_SIZE;
    metadata.update();

    let encoded = metadata.encode();
    let mut block = vec![0; FEOX_BLOCK_SIZE];
    block[..encoded.len()].copy_from_slice(&encoded);

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.set_len(DEVICE_SIZE).unwrap();
    file.write_all(&block).unwrap();
    file.sync_all().unwrap();
}

fn write_sector(path: &str, sector: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn read_sector(path: &str, sector: u64) -> Vec<u8> {
    read_sectors(path, sector, 1)
}

fn read_sectors(path: &str, sector: u64, sectors: u64) -> Vec<u8> {
    let mut file = OpenOptions::new().read(true).open(path).unwrap();
    let mut bytes = vec![0; sectors as usize * FEOX_BLOCK_SIZE];
    file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
        .unwrap();
    file.read_exact(&mut bytes).unwrap();
    bytes
}

#[test]
fn reader_parked_before_pread_returns_one_complete_generation() {
    let temp_file = NamedTempFile::new().unwrap();
    let store = persistent_store(temp_file.path().to_str().unwrap());

    store.insert(VICTIM_KEY, &vec![b'V'; VALUE_LEN]).unwrap();
    store.flush().unwrap();

    let session = gate::session();
    let (armed, reader) = parked_reader(&store, &session);

    assert!(
        armed.wait_for_arrivals(1, Duration::from_secs(10)),
        "reader never reached the sector-load hook"
    );

    store.insert(VICTIM_KEY, &vec![b'W'; VALUE_LEN]).unwrap();
    let (flush_rx, flush) = background_flush(&store);
    assert_flush_pending(&flush_rx);

    armed.release();
    let observed = reader.join().unwrap();
    finish_flush(flush_rx, flush);

    for i in 0..16u32 {
        let key = format!("filler{i:04}");
        store
            .insert(key.as_bytes(), &vec![b'F'; VALUE_LEN])
            .unwrap();
    }
    store.flush().unwrap();

    match observed {
        Ok(value) => assert_old_or_new_generation(&value),
        Err(error) => panic!("reader failed instead of re-resolving: {error}"),
    }
    assert_eq!(armed.timeouts(), 0, "reader escaped through hook timeout");

    assert_eq!(store.get(VICTIM_KEY).unwrap(), vec![b'W'; VALUE_LEN]);
}

#[test]
fn reader_parked_before_pread_never_returns_the_deletion_marker() {
    let temp_file = NamedTempFile::new().unwrap();
    let store = persistent_store(temp_file.path().to_str().unwrap());

    store.insert(VICTIM_KEY, &vec![b'V'; VALUE_LEN]).unwrap();
    store.flush().unwrap();

    let session = gate::session();
    let (armed, reader) = parked_reader(&store, &session);

    assert!(
        armed.wait_for_arrivals(1, Duration::from_secs(10)),
        "reader never reached the sector-load hook"
    );

    store.insert(VICTIM_KEY, &vec![b'W'; VALUE_LEN]).unwrap();
    let (flush_rx, flush) = background_flush(&store);
    assert_flush_pending(&flush_rx);

    armed.release();
    let observed = reader.join().unwrap();
    finish_flush(flush_rx, flush);

    let value = observed.expect("reader failed instead of re-resolving");
    assert_old_or_new_generation(&value);
    assert_eq!(armed.timeouts(), 0, "reader escaped through hook timeout");
}

#[test]
fn parked_multi_block_reader_never_observes_a_retired_continuation() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = persistent_store(&path);
    let value_len = FEOX_BLOCK_SIZE * 2;

    store.insert(VICTIM_KEY, &vec![b'V'; value_len]).unwrap();
    store.flush().unwrap();

    let session = gate::session();
    let (armed, reader) = parked_reader(&store, &session);
    assert!(
        armed.wait_for_arrivals(1, Duration::from_secs(10)),
        "reader never reached the sector-load hook"
    );

    store.insert(VICTIM_KEY, &vec![b'W'; value_len]).unwrap();
    let (flush_rx, flush) = background_flush(&store);
    assert_flush_pending(&flush_rx);
    assert_ne!(
        &read_sector(&path, FEOX_DATA_START_BLOCK + 1)[..DELETION_MARKER.len()],
        DELETION_MARKER.as_slice(),
        "retirement overwrote a continuation while a reader held the extent"
    );

    armed.release();
    let value = reader
        .join()
        .unwrap()
        .expect("reader failed instead of returning a complete generation");
    finish_flush(flush_rx, flush);
    assert_eq!(value.len(), value_len);
    assert!(
        value.iter().all(|byte| *byte == b'V') || value.iter().all(|byte| *byte == b'W'),
        "reader returned a torn generation"
    );
    assert_eq!(armed.timeouts(), 0, "reader escaped through hook timeout");
}

#[test]
fn flush_waits_for_parked_reader_before_acknowledging_delete() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = persistent_store(&path);

    store.insert(VICTIM_KEY, &vec![b'V'; VALUE_LEN]).unwrap();
    store.flush().unwrap();

    let session = gate::session();
    let (armed, reader) = parked_reader(&store, &session);
    assert!(
        armed.wait_for_arrivals(1, Duration::from_secs(10)),
        "reader never reached the sector-load hook"
    );

    store.delete(VICTIM_KEY).unwrap();
    let (flush_rx, flush) = background_flush(&store);
    assert_flush_pending(&flush_rx);

    armed.release();
    let _ = reader.join().unwrap();
    finish_flush(flush_rx, flush);

    assert_eq!(
        &read_sector(&path, FEOX_DATA_START_BLOCK)[..DELETION_MARKER.len()],
        DELETION_MARKER.as_slice()
    );

    drop(store);
    let reopened = persistent_store(&path);
    assert!(!reopened.contains_key(VICTIM_KEY));
}

#[test]
fn periodic_flush_retries_retirement_after_reader_releases() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let store = persistent_store(&path);

    store.insert(VICTIM_KEY, &vec![b'V'; VALUE_LEN]).unwrap();
    store.flush().unwrap();

    let session = gate::session();
    let (armed, reader) = parked_reader(&store, &session);
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(10)));

    store.delete(VICTIM_KEY).unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_ne!(
        &read_sector(&path, FEOX_DATA_START_BLOCK)[..DELETION_MARKER.len()],
        DELETION_MARKER.as_slice()
    );

    armed.release();
    let _ = reader.join().unwrap();

    let mut retired = false;
    for _ in 0..40 {
        if &read_sector(&path, FEOX_DATA_START_BLOCK)[..DELETION_MARKER.len()]
            == DELETION_MARKER.as_slice()
        {
            retired = true;
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(retired);
}

#[test]
fn recovery_skips_a_retired_multi_block_extent() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let ghost_sector = serialized_record_sector(
        3,
        FEOX_DATA_START_BLOCK + 1,
        b"ghost",
        b"ghost-value",
        100,
        true,
    );
    let victim_value = multi_block_value_with_continuation(3, &ghost_sector);

    {
        let store = persistent_store(&path);
        store.insert(VICTIM_KEY, &victim_value).unwrap();
        store.flush().unwrap();
        store.delete(VICTIM_KEY).unwrap();
        store.flush().unwrap();
    }

    write_sector(&path, FEOX_DATA_START_BLOCK + 1, &ghost_sector);

    {
        let reopened = persistent_store(&path);
        assert!(reopened.is_empty());
        assert!(!reopened.contains_key(b"ghost"));
        reopened.insert(b"filler", b"live").unwrap();
        reopened.flush().unwrap();
    }

    assert_eq!(
        &read_sector(&path, FEOX_DATA_START_BLOCK + 1)[..DELETION_MARKER.len()],
        DELETION_MARKER.as_slice()
    );
    let reopened = persistent_store(&path);
    assert_eq!(reopened.get(b"filler").unwrap(), b"live");
    assert!(!reopened.contains_key(b"ghost"));
    assert_eq!(reopened.len(), 1);
}

#[test]
fn recovery_completes_pending_retirement_before_reuse() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let pending = pending_retirement_block(FEOX_DATA_START_BLOCK, 3);
    let ghost = serialized_record_sector(
        3,
        FEOX_DATA_START_BLOCK + 1,
        b"ghost",
        b"ghost-value",
        100,
        true,
    );
    write_sector(&path, FEOX_DATA_START_BLOCK, &pending);
    write_sector(&path, FEOX_DATA_START_BLOCK + 1, &ghost);

    {
        let recovered = persistent_store(&path);
        assert!(recovered.is_empty());
        recovered.insert(b"filler", b"live").unwrap();
        recovered.flush().unwrap();

        let filler = get_format_ref(3)
            .parse_record(&read_sector(&path, FEOX_DATA_START_BLOCK))
            .expect("filler did not reuse the pending extent's head");
        assert_eq!(filler.0, b"filler");
        let tail = read_sector(&path, FEOX_DATA_START_BLOCK + 1);
        assert_eq!(&tail[..DELETION_MARKER.len()], DELETION_MARKER);
        assert_eq!(tail[18], RETIREMENT_COMPLETE);
    }

    let reopened = persistent_store(&path);
    assert_eq!(reopened.get(b"filler").unwrap(), b"live");
    assert!(!reopened.contains_key(b"ghost"));
    assert_eq!(reopened.len(), 1);
}

#[test]
fn recovery_loads_multi_block_records_in_all_formats() {
    let value = vec![b'W'; FEOX_BLOCK_SIZE * 2];

    for version in [1, 2, 3] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, version);
        let extent = serialized_record_extent(
            version,
            FEOX_DATA_START_BLOCK,
            b"wide",
            &value,
            100,
            0,
            version == 3,
        );
        write_sector(&path, FEOX_DATA_START_BLOCK, &extent);

        let recovered = persistent_store(&path);
        assert_eq!(recovered.get(b"wide").unwrap(), value, "version {version}");
    }
}

#[test]
fn recovery_loads_records_across_scan_buffer_boundaries() {
    const SCAN_BLOCKS: u64 = 256;

    let value = vec![b'W'; FEOX_BLOCK_SIZE * 2];
    let sector = FEOX_DATA_START_BLOCK + SCAN_BLOCKS - 1;

    for version in [1, 2, 3] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, version);

        let extent =
            serialized_record_extent(version, sector, b"wide", &value, 100, 0, version == 3);
        let next_sector = sector + (extent.len() / FEOX_BLOCK_SIZE) as u64;
        let next = serialized_record_sector(
            version,
            next_sector,
            b"next",
            b"next-value",
            101,
            version == 3,
        );
        write_sector(&path, sector, &extent);
        write_sector(&path, next_sector, &next);

        let recovered = persistent_store(&path);
        assert_eq!(recovered.get(b"wide").unwrap(), value, "version {version}");
        assert_eq!(
            recovered.get(b"next").unwrap(),
            b"next-value",
            "version {version}"
        );
    }
}

#[test]
fn recovery_rejects_a_corrupt_tail_across_a_scan_buffer_boundary() {
    const SCAN_BLOCKS: u64 = 256;

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let sector = FEOX_DATA_START_BLOCK + SCAN_BLOCKS - 1;
    let mut extent = serialized_record_extent(
        3,
        sector,
        b"wide",
        &vec![b'W'; FEOX_BLOCK_SIZE * 2],
        100,
        0,
        true,
    );
    extent[FEOX_BLOCK_SIZE + 73] ^= 1;
    write_sector(&path, sector, &extent);

    let result = FeoxStore::builder()
        .device_path(path)
        .file_size(DEVICE_SIZE)
        .enable_caching(false)
        .build();
    assert!(matches!(result, Err(FeoxError::CorruptedRecord)));
}

#[test]
fn recovery_repairs_a_large_retired_extent_past_the_first_read() {
    const RETIRED_BLOCKS: usize = 300;
    const CORRUPT_OFFSET: usize = 270;

    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let mut retired = vec![0; RETIRED_BLOCKS * FEOX_BLOCK_SIZE];
    fill_retirement_extent(&mut retired, FEOX_DATA_START_BLOCK, RETIRED_BLOCKS);
    write_sector(&path, FEOX_DATA_START_BLOCK, &retired);

    let corrupt_sector = FEOX_DATA_START_BLOCK + CORRUPT_OFFSET as u64;
    let ghost = serialized_record_sector(3, corrupt_sector, b"ghost", b"value", 100, true);
    write_sector(&path, corrupt_sector, &ghost);

    {
        let recovered = persistent_store(&path);
        assert!(recovered.is_empty());
        recovered.insert(b"filler", b"live").unwrap();
        recovered.flush().unwrap();
    }

    let repaired = read_sector(&path, corrupt_sector);
    assert_eq!(&repaired[..DELETION_MARKER.len()], DELETION_MARKER);
    assert_eq!(
        u64::from_le_bytes(repaired[8..16].try_into().unwrap()),
        (RETIRED_BLOCKS - CORRUPT_OFFSET) as u64
    );
    assert_eq!(repaired[18], RETIREMENT_COMPLETE);

    let reopened = persistent_store(&path);
    assert_eq!(reopened.get(b"filler").unwrap(), b"live");
    assert!(!reopened.contains_key(b"ghost"));
}

#[test]
fn multi_block_allocation_invalidates_a_shorter_retirement_head_first() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);
    write_sector(
        &path,
        FEOX_DATA_START_BLOCK,
        &retirement_block(FEOX_DATA_START_BLOCK, 1),
    );

    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", ALLOCATION_INTENT_HELPER, "--nocapture"])
        .env("FEOX_TEST_ALLOCATION_INTENT_PATH", &path)
        .env("FEOX_TEST_CRASH_POINT", "after_allocation_intent")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(86));

    let head = read_sector(&path, FEOX_DATA_START_BLOCK);
    assert_eq!(&head[..DELETION_MARKER.len()], DELETION_MARKER);
    assert_eq!(u64::from_le_bytes(head[8..16].try_into().unwrap()), 1);
    assert_eq!(head[18], RETIREMENT_COMPLETE);

    let journal = read_sectors(
        &path,
        ALLOCATION_JOURNAL_START_BLOCK,
        ALLOCATION_JOURNAL_BLOCKS,
    );
    assert_eq!(
        decode_allocation_journal(&journal, DEVICE_SIZE / FEOX_BLOCK_SIZE as u64)
            .unwrap()
            .extents,
        vec![(FEOX_DATA_START_BLOCK, 3)]
    );

    let ghost = serialized_record_sector(
        3,
        FEOX_DATA_START_BLOCK + 1,
        b"ghost",
        b"ghost-value",
        100,
        true,
    );
    write_sector(&path, FEOX_DATA_START_BLOCK + 1, &ghost);

    let reopened = persistent_store(&path);
    assert!(reopened.is_empty());
    assert!(!reopened.contains_key(b"ghost"));
}

#[test]
fn multi_block_allocation_intent_crash_helper() {
    let Ok(path) = std::env::var("FEOX_TEST_ALLOCATION_INTENT_PATH") else {
        return;
    };

    let store = persistent_store(&path);
    store
        .insert(b"wide", &vec![b'W'; FEOX_BLOCK_SIZE * 2])
        .unwrap();
    store.flush().unwrap();
    panic!("crash point was not reached");
}

#[test]
fn multi_block_updates_commit_after_the_journal_clears() {
    for version in [1, 2, 3] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, version);
        {
            let store = persistent_store(&path);
            store.insert(b"state", b"old").unwrap();
            store.flush().unwrap();
        }

        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", MULTI_BLOCK_UPDATE_HELPER, "--nocapture"])
            .env("FEOX_TEST_MULTI_BLOCK_UPDATE_PATH", &path)
            .env("FEOX_TEST_CRASH_POINT", "before_allocation_journal_clear")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(86), "version {version}");

        let reopened = persistent_store(&path);
        assert_eq!(reopened.get(b"state").unwrap(), b"old", "version {version}");
    }
}

#[test]
fn multi_block_update_crash_helper() {
    let Ok(path) = std::env::var("FEOX_TEST_MULTI_BLOCK_UPDATE_PATH") else {
        return;
    };

    let store = persistent_store(&path);
    store
        .insert(b"state", &vec![b'W'; FEOX_BLOCK_SIZE * 2])
        .unwrap();
    store.flush().unwrap();
    panic!("crash point was not reached");
}

#[test]
fn recovery_rejects_invalid_v3_heads() {
    let mut invalid_retirement = vec![0; FEOX_BLOCK_SIZE];
    invalid_retirement[..DELETION_MARKER.len()].copy_from_slice(DELETION_MARKER);
    invalid_retirement[8..16].copy_from_slice(&3_u64.to_le_bytes());

    let mut invalid_record = vec![0; FEOX_BLOCK_SIZE];
    invalid_record[..2].copy_from_slice(&SECTOR_MARKER.to_le_bytes());

    let mut stale_complete_state = pending_retirement_block(FEOX_DATA_START_BLOCK, 3);
    stale_complete_state[18] = RETIREMENT_COMPLETE;

    for head in [
        invalid_retirement,
        pending_retirement_block(FEOX_DATA_START_BLOCK, 0),
        invalid_record,
        stale_complete_state,
    ] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, 3);
        write_sector(&path, FEOX_DATA_START_BLOCK, &head);

        let result = FeoxStore::builder()
            .device_path(path)
            .file_size(DEVICE_SIZE)
            .enable_caching(false)
            .build();
        assert!(matches!(result, Err(FeoxError::CorruptedRecord)));
    }
}

#[test]
fn recovery_skips_legacy_continuations_after_head_reuse() {
    for version in [1, 2] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, version);

        let ghost_sector = serialized_record_sector(
            version,
            FEOX_DATA_START_BLOCK + 1,
            b"ghost",
            b"ghost-value",
            100,
            false,
        );
        let victim_value = multi_block_value_with_continuation(version, &ghost_sector);

        {
            let store = persistent_store(&path);
            store.insert(VICTIM_KEY, &victim_value).unwrap();
            store.flush().unwrap();
            store.insert(VICTIM_KEY, b"current").unwrap();
            store.flush().unwrap();
            store.insert(b"filler", b"live").unwrap();
            store.flush().unwrap();
        }

        let format = get_format_ref(version);
        let filler = format
            .parse_record(&read_sector(&path, FEOX_DATA_START_BLOCK))
            .expect("filler did not reuse the retired head sector");
        assert_eq!(filler.0, b"filler");
        assert_eq!(
            &read_sector(&path, FEOX_DATA_START_BLOCK + 1)[..DELETION_MARKER.len()],
            DELETION_MARKER.as_slice(),
            "version {version} continuation was not retired"
        );

        let reopened = persistent_store(&path);
        assert_eq!(reopened.get(VICTIM_KEY).unwrap(), b"current");
        assert_eq!(reopened.get(b"filler").unwrap(), b"live");
        assert!(!reopened.contains_key(b"ghost"));
        assert_eq!(reopened.len(), 2);
    }
}

#[test]
fn recovery_requires_opt_in_for_released_pre_token_retirement_markers() {
    for version in [1, 2] {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        initialize_device(&path, version);

        let mut legacy_marker = vec![0; FEOX_BLOCK_SIZE];
        legacy_marker[..DELETION_MARKER.len()].copy_from_slice(DELETION_MARKER);
        let live_sector = serialized_record_sector(
            version,
            FEOX_DATA_START_BLOCK + 1,
            b"live",
            b"live-value",
            100,
            false,
        );
        write_sector(&path, FEOX_DATA_START_BLOCK, &legacy_marker);
        write_sector(&path, FEOX_DATA_START_BLOCK + 1, &live_sector);

        let rejected = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(DEVICE_SIZE)
            .enable_caching(false)
            .build();
        assert!(matches!(rejected, Err(FeoxError::AmbiguousLegacyTombstone)));

        let store = FeoxStore::builder()
            .device_path(path)
            .file_size(DEVICE_SIZE)
            .enable_caching(false)
            .allow_ambiguous_legacy_recovery(true)
            .build()
            .unwrap();
        assert_eq!(store.get(b"live").unwrap(), b"live-value");
    }
}

#[test]
fn recovery_retires_stale_duplicate_extent_before_reuse() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let ghost_sector = serialized_record_sector(
        3,
        FEOX_DATA_START_BLOCK + 1,
        b"ghost",
        b"ghost-value",
        50,
        true,
    );
    let stale_value = multi_block_value_with_continuation(3, &ghost_sector);
    let stale_extent = serialized_record_extent(
        3,
        FEOX_DATA_START_BLOCK,
        VICTIM_KEY,
        &stale_value,
        100,
        0,
        true,
    );
    let newer_sector = FEOX_DATA_START_BLOCK + (stale_extent.len() / FEOX_BLOCK_SIZE) as u64;
    let newer = serialized_record_sector(3, newer_sector, VICTIM_KEY, b"current", 200, true);
    write_sector(&path, FEOX_DATA_START_BLOCK, &stale_extent);
    write_sector(&path, newer_sector, &newer);

    {
        let recovered = persistent_store(&path);
        assert_eq!(recovered.get(VICTIM_KEY).unwrap(), b"current");
        recovered.insert(b"filler", b"live").unwrap();
        recovered.flush().unwrap();

        let format = get_format_ref(3);
        let filler = format
            .parse_record(&read_sector(&path, FEOX_DATA_START_BLOCK))
            .expect("filler did not reuse the stale duplicate's head");
        assert_eq!(filler.0, b"filler");
        assert_eq!(
            &read_sector(&path, FEOX_DATA_START_BLOCK + 1)[..DELETION_MARKER.len()],
            DELETION_MARKER.as_slice()
        );
    }

    let reopened = persistent_store(&path);
    assert_eq!(reopened.get(VICTIM_KEY).unwrap(), b"current");
    assert_eq!(reopened.get(b"filler").unwrap(), b"live");
    assert!(!reopened.contains_key(b"ghost"));
    assert_eq!(reopened.len(), 2);
}

#[test]
fn recovery_rejects_a_newer_torn_multi_block_generation() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let old_value = vec![b'V'; FEOX_BLOCK_SIZE * 2];
    let old_extent = serialized_record_extent(
        3,
        FEOX_DATA_START_BLOCK,
        VICTIM_KEY,
        &old_value,
        100,
        0,
        true,
    );
    let newer_sector = FEOX_DATA_START_BLOCK + (old_extent.len() / FEOX_BLOCK_SIZE) as u64;
    let newer_extent = serialized_record_extent(
        3,
        newer_sector,
        VICTIM_KEY,
        &vec![b'W'; FEOX_BLOCK_SIZE * 2],
        200,
        0,
        true,
    );
    write_sector(&path, FEOX_DATA_START_BLOCK, &old_extent);
    write_sector(&path, newer_sector, &newer_extent[..FEOX_BLOCK_SIZE]);

    let result = FeoxStore::builder()
        .device_path(path)
        .file_size(DEVICE_SIZE)
        .enable_caching(false)
        .build();
    assert!(matches!(result, Err(FeoxError::CorruptedRecord)));
}

#[test]
fn expired_newest_generation_does_not_resurrect_an_older_value() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 2);

    let old = serialized_record_extent(2, FEOX_DATA_START_BLOCK, VICTIM_KEY, b"old", 100, 0, false);
    let new_sector = FEOX_DATA_START_BLOCK + (old.len() / FEOX_BLOCK_SIZE) as u64;
    let expired = serialized_record_extent(2, new_sector, VICTIM_KEY, b"expired", 200, 1, false);
    write_sector(&path, FEOX_DATA_START_BLOCK, &old);
    write_sector(&path, new_sector, &expired);

    let recovered = persistent_ttl_store(&path);
    assert!(!recovered.contains_key(VICTIM_KEY));
    assert!(recovered.is_empty());
}

#[test]
fn recovery_removes_expired_winners_across_batches() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 2);

    let mut sector = FEOX_DATA_START_BLOCK;
    let mut image = Vec::new();
    for index in 0..300 {
        let key = format!("expired-{index:03}");
        let extent = serialized_record_extent(2, sector, key.as_bytes(), b"value", index, 1, false);
        sector += (extent.len() / FEOX_BLOCK_SIZE) as u64;
        image.extend_from_slice(&extent);
    }
    write_sector(&path, FEOX_DATA_START_BLOCK, &image);

    let recovered = persistent_ttl_store(&path);
    assert!(recovered.is_empty());
}

#[test]
fn recovery_retires_expired_extent_before_reuse() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    initialize_device(&path, 3);

    let ghost_sector = serialized_record_sector(
        3,
        FEOX_DATA_START_BLOCK + 1,
        b"ghost",
        b"ghost-value",
        50,
        true,
    );
    let expired_value = multi_block_value_with_continuation(3, &ghost_sector);
    let expired_extent = serialized_record_extent(
        3,
        FEOX_DATA_START_BLOCK,
        VICTIM_KEY,
        &expired_value,
        100,
        1,
        true,
    );
    write_sector(&path, FEOX_DATA_START_BLOCK, &expired_extent);

    {
        let recovered = persistent_ttl_store(&path);
        assert!(!recovered.contains_key(VICTIM_KEY));
        recovered.insert(b"filler", b"live").unwrap();
        recovered.flush().unwrap();

        let format = get_format_ref(3);
        let filler = format
            .parse_record(&read_sector(&path, FEOX_DATA_START_BLOCK))
            .expect("filler did not reuse the expired extent's head");
        assert_eq!(filler.0, b"filler");
        assert_eq!(
            &read_sector(&path, FEOX_DATA_START_BLOCK + 1)[..DELETION_MARKER.len()],
            DELETION_MARKER.as_slice()
        );
    }

    let reopened = persistent_ttl_store(&path);
    assert_eq!(reopened.get(b"filler").unwrap(), b"live");
    assert!(!reopened.contains_key(VICTIM_KEY));
    assert!(!reopened.contains_key(b"ghost"));
    assert_eq!(reopened.len(), 1);
}

/// A replacement whose write fails must leave the old extent intact. Until the
/// successor reaches disk the old extent is the key's only durable copy, so
/// honouring the update's paired deletion on the failure path destroys the
/// record. The assertion reads the extent directly rather than reopening,
/// because a later successful retry would hide the loss behind a fresh
/// generation.
#[test]
fn failed_replacement_write_keeps_the_old_extent() {
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();
    let _session = gate::session();

    let store = persistent_store(&path);
    store.insert(VICTIM_KEY, b"original").unwrap();
    store.flush().unwrap();

    let head = read_sector(&path, FEOX_DATA_START_BLOCK);
    let (key, _, _, _) = FormatV2
        .parse_record(&head)
        .expect("the first record did not land on the first data sector");
    assert_eq!(key, VICTIM_KEY);

    let fault = fault::fail_next(
        RECORD_WRITE,
        store.get_write_buffer().unwrap().fault_scope(),
        1024,
    );
    store.insert(VICTIM_KEY, b"replacement").unwrap();
    assert!(
        store.flush().is_err(),
        "flush must surface the injected write failure"
    );
    assert!(
        fault.consumed() > 0,
        "the record write path never consulted the fault"
    );

    let head = read_sector(&path, FEOX_DATA_START_BLOCK);
    assert_ne!(
        &head[..DELETION_MARKER.len()],
        DELETION_MARKER.as_slice(),
        "old extent was retired while its successor was still unwritten"
    );
    let (key, value_len, _, _) = FormatV2
        .parse_record(&head)
        .expect("old generation no longer parses after the failed replacement");
    assert_eq!(key, VICTIM_KEY);
    let value_offset = FormatV2.value_offset(VICTIM_KEY.len());
    assert_eq!(
        &head[value_offset..value_offset + value_len],
        b"original",
        "old extent no longer holds the durable value"
    );

    drop(fault);
}

#[test]
fn record_write_fault_is_scoped_to_one_store() {
    let _session = gate::session();
    let failed_file = NamedTempFile::new().unwrap();
    let healthy_file = NamedTempFile::new().unwrap();
    let failed = persistent_store(failed_file.path().to_str().unwrap());
    let healthy = persistent_store(healthy_file.path().to_str().unwrap());

    failed.insert(b"failed", b"value").unwrap();
    healthy.insert(b"healthy", b"value").unwrap();

    let fault = fault::fail_next(
        RECORD_WRITE,
        failed.get_write_buffer().unwrap().fault_scope(),
        1024,
    );
    healthy
        .flush()
        .expect("another store consumed the scoped fault");
    assert!(failed.flush().is_err());
    assert!(fault.consumed() > 0);

    drop(fault);
    failed.flush().unwrap();
}

/// A replacement whose new generation cannot be allocated must report the
/// failure rather than reporting success, and must not lose the key.
#[test]
fn failed_allocation_surfaces_an_error_and_keeps_the_key() {
    const SECTORS: u64 = 20;
    let temp_file = NamedTempFile::new().unwrap();
    let path = temp_file.path().to_str().unwrap().to_string();

    {
        let store = FeoxStore::builder()
            .device_path(path.clone())
            .file_size(SECTORS * FEOX_BLOCK_SIZE as u64)
            .enable_caching(false)
            .build()
            .unwrap();

        store.insert(b"survivor", b"original").unwrap();
        store.flush().unwrap();
        assert_eq!(store.get(b"survivor").unwrap(), b"original");

        // Consume every remaining sector so the replacement cannot be allocated.
        let mut filler = 0;
        loop {
            let key = format!("filler{filler:04}");
            if store.insert(key.as_bytes(), b"x").is_err() {
                break;
            }
            if store.flush().is_err() {
                break;
            }
            filler += 1;
            assert!(filler < SECTORS * 4, "device never filled");
        }

        // The in-memory update lands, but its persistence must fail and must not
        // take the old extent with it.
        store.insert(b"survivor", b"replacement").unwrap();
        assert!(
            store.flush().is_err(),
            "flush must surface the allocation failure rather than reporting success"
        );
    }

    let reopened = FeoxStore::new(Some(path)).unwrap();
    let survived = reopened
        .get(b"survivor")
        .expect("the key must not be lost when its replacement could not be written");
    assert!(
        survived == b"original" || survived == b"replacement",
        "recovered an unexpected generation: {survived:?}"
    );
}
