use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::atomic::Ordering;

use tempfile::TempDir;

use crate::constants::{
    DELETION_MARKER, FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK, FEOX_METADATA_BACKUP_BLOCK,
    MAX_RECOVERABLE_KEY_SIZE, SECTOR_MARKER,
};
use crate::core::record::Record;
use crate::storage::allocation_journal::{encode_active, ALLOCATION_JOURNAL_START_BLOCK};
use crate::storage::format::{get_format_ref, pending_retirement_block};
use crate::storage::metadata::Metadata;
use crate::{migrate, MigrationError, MigrationOptions};

const DEVICE_SIZE: u64 = 2 * 1024 * 1024;

struct RawRecord {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
    ttl_expiry: u64,
}

#[test]
fn migration_preserves_v2_records_and_source_bytes() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 2, DEVICE_SIZE);

    let first = serialized_record(2, b"alpha", b"value", 0, 0);
    let second_sector = FEOX_DATA_START_BLOCK + sectors(&first);
    let second_value = vec![0x5a; FEOX_BLOCK_SIZE + 137];
    let second = serialized_record(2, b"ttl", &second_value, 1_000_000_007, 61_000_000_003);
    write_at(&source, FEOX_DATA_START_BLOCK, &first);
    write_at(&source, second_sector, &second);
    let before = fs::read(&source).unwrap();

    let report = migrate(
        MigrationOptions::new(&source, &destination)
            .allow_ambiguous_legacy_recovery(false)
            .hash_bits(4),
    )
    .unwrap();

    assert_eq!(report.source_version, 2);
    assert_eq!(report.destination_version, 3);
    assert_eq!(report.records, 2);
    assert_eq!(report.value_bytes, (5 + second_value.len()) as u64);
    assert_eq!(report.ambiguous_legacy_markers, 0);
    assert_eq!(fs::read(&source).unwrap(), before);
    assert_eq!(metadata_versions(&destination), [Some(3), Some(3)]);

    let records = raw_records(&destination, 3);
    let alpha = record(&records, b"alpha");
    assert_eq!(alpha.value, b"value");
    assert_eq!(alpha.timestamp, 0);
    assert_eq!(alpha.ttl_expiry, 0);
    let ttl = record(&records, b"ttl");
    assert_eq!(ttl.value, second_value);
    assert_eq!(ttl.timestamp, 1_000_000_007);
    assert_eq!(ttl.ttl_expiry, 61_000_000_003);
}

#[test]
fn ambiguous_legacy_migration_requires_explicit_opt_in() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let rejected_destination = temp.path().join("rejected.feox");
    let accepted_destination = temp.path().join("accepted.feox");
    initialize_device(&source, 2, DEVICE_SIZE);

    let mut marker = vec![0; FEOX_BLOCK_SIZE];
    marker[..DELETION_MARKER.len()].copy_from_slice(DELETION_MARKER);
    write_at(&source, FEOX_DATA_START_BLOCK, &marker);
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 1,
        &serialized_record(2, b"live", b"value", 41, 0),
    );
    let before = fs::read(&source).unwrap();

    let error = migrate(
        MigrationOptions::new(&source, &rejected_destination)
            .allow_ambiguous_legacy_recovery(false)
            .hash_bits(4),
    )
    .unwrap_err();
    assert!(matches!(error, MigrationError::AmbiguousLegacyRecovery));
    assert!(!rejected_destination.exists());
    assert_eq!(fs::read(&source).unwrap(), before);

    let report = migrate(
        MigrationOptions::new(&source, &accepted_destination)
            .allow_ambiguous_legacy_recovery(true)
            .hash_bits(4),
    )
    .unwrap();
    assert_eq!(report.records, 1);
    assert_eq!(report.ambiguous_legacy_markers, 1);
    assert_eq!(
        record(&raw_records(&accepted_destination, 3), b"live").value,
        b"value"
    );
    assert_eq!(fs::read(&source).unwrap(), before);
}

#[test]
fn migration_masks_active_allocation_journal_without_mutating_source() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 2, DEVICE_SIZE);
    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &serialized_record(2, b"ghost", b"discard", 9, 0),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 1,
        &serialized_record(2, b"live", b"keep", 10, 0),
    );
    write_at(
        &source,
        ALLOCATION_JOURNAL_START_BLOCK,
        &encode_active(1, &[(FEOX_DATA_START_BLOCK, 1)]).unwrap(),
    );
    let before = fs::read(&source).unwrap();

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(report.records, 1);
    let records = raw_records(&destination, 3);
    assert!(records.iter().all(|record| record.key != b"ghost"));
    assert_eq!(record(&records, b"live").value, b"keep");
    assert_eq!(fs::read(&source).unwrap(), before);
}

#[test]
fn migration_virtualizes_pending_retirement_without_mutating_source() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 2, DEVICE_SIZE);

    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &pending_retirement_block(FEOX_DATA_START_BLOCK, 3),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 1,
        &serialized_record(2, b"ghost", b"discard", 9, 0),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 3,
        &serialized_record(2, b"live", b"keep", 10, 0),
    );
    let before = fs::read(&source).unwrap();

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(report.records, 1);
    let records = raw_records(&destination, 3);
    assert!(records.iter().all(|record| record.key != b"ghost"));
    assert_eq!(record(&records, b"live").value, b"keep");
    assert_eq!(fs::read(&source).unwrap(), before);
}

#[test]
fn migration_copies_only_the_recovered_record_generation() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 2, DEVICE_SIZE);

    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &serialized_record(2, b"key", b"old", 10, 0),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 1,
        &serialized_record(2, b"key", b"current", 30, 0),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 2,
        &serialized_record(2, b"key", b"stale", 20, 0),
    );

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(report.records, 1);
    let records = raw_records(&destination, 3);
    let migrated = record(&records, b"key");
    assert_eq!(migrated.value, b"current");
    assert_eq!(migrated.timestamp, 30);
}

#[test]
fn migration_keeps_expired_winner_from_resurrecting_an_older_value() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 2, DEVICE_SIZE);

    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &serialized_record(2, b"key", b"old", 10, 0),
    );
    write_at(
        &source,
        FEOX_DATA_START_BLOCK + 1,
        &serialized_record(2, b"key", b"expired", 20, 1),
    );

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(report.records, 1);
    let records = raw_records(&destination, 3);
    let migrated = record(&records, b"key");
    assert_eq!(migrated.value, b"expired");
    assert_eq!(migrated.timestamp, 20);
    assert_eq!(migrated.ttl_expiry, 1);

    let reopened = crate::FeoxStore::builder()
        .hash_bits(4)
        .device_path(destination.to_str().unwrap())
        .enable_caching(false)
        .enable_ttl(true)
        .build()
        .unwrap();
    assert!(!reopened.contains_key(b"key"));
}

#[test]
fn migration_refuses_current_format_and_existing_destination() {
    let temp = TempDir::new().unwrap();
    let v3_source = temp.path().join("v3.feox");
    let v2_source = temp.path().join("v2.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&v3_source, 3, DEVICE_SIZE);
    initialize_device(&v2_source, 2, DEVICE_SIZE);

    let error = migrate(MigrationOptions::new(&v3_source, &destination).hash_bits(4)).unwrap_err();
    assert!(matches!(error, MigrationError::CurrentFormat(3)));
    assert!(!destination.exists());

    fs::write(&destination, b"sentinel").unwrap();
    let error = migrate(MigrationOptions::new(&v2_source, &destination).hash_bits(4)).unwrap_err();
    assert!(matches!(error, MigrationError::DestinationExists(_)));
    assert_eq!(fs::read(&destination).unwrap(), b"sentinel");
}

#[test]
fn v1_migration_rejects_keys_that_v3_cannot_recover() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_device(&source, 1, DEVICE_SIZE);
    let key = vec![b'k'; MAX_RECOVERABLE_KEY_SIZE + 1];
    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &serialized_record(1, &key, b"value", 1, 0),
    );

    let error = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap_err();

    assert!(matches!(
        error,
        MigrationError::KeyTooLarge {
            length,
            maximum: MAX_RECOVERABLE_KEY_SIZE
        } if length == key.len()
    ));
    assert!(!destination.exists());
}

#[test]
fn v1_migration_grows_destination_for_the_v3_header() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    let source_size = (FEOX_DATA_START_BLOCK + 1) * FEOX_BLOCK_SIZE as u64;
    initialize_device(&source, 1, source_size);
    let key = b"key";
    let value_len = FEOX_BLOCK_SIZE - get_format_ref(1).record_header_size(key.len());
    let value = vec![0x33; value_len];
    let extent = serialized_record(1, key, &value, 123, 0);
    assert_eq!(extent.len(), FEOX_BLOCK_SIZE);
    write_at(&source, FEOX_DATA_START_BLOCK, &extent);

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(
        report.destination_size,
        (FEOX_DATA_START_BLOCK + 2) * FEOX_BLOCK_SIZE as u64
    );
    assert_eq!(
        fs::metadata(&destination).unwrap().len(),
        report.destination_size
    );
    assert_eq!(record(&raw_records(&destination, 3), key).value, value);
}

#[cfg(target_os = "linux")]
#[test]
fn migration_accepts_non_utf8_paths() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let temp = TempDir::new().unwrap();
    let source = temp
        .path()
        .join(OsString::from_vec(b"source-\xff.feox".to_vec()));
    let destination = temp
        .path()
        .join(OsString::from_vec(b"destination-\xff.feox".to_vec()));
    initialize_device(&source, 2, DEVICE_SIZE);
    write_at(
        &source,
        FEOX_DATA_START_BLOCK,
        &serialized_record(2, b"key", b"value", 1, 0),
    );

    let report = migrate(MigrationOptions::new(&source, &destination).hash_bits(4)).unwrap();

    assert_eq!(report.records, 1);
    assert_eq!(
        record(&raw_records(&destination, 3), b"key").value,
        b"value"
    );
}

fn initialize_device(path: &std::path::Path, version: u32, device_size: u64) {
    let mut metadata = Metadata::new();
    metadata.version = version;
    metadata.device_size = device_size;
    metadata.update();
    let mut encoded = metadata.encode();
    if version < 3 {
        encoded[64..].fill(0);
    }

    let mut file = File::create(path).unwrap();
    file.set_len(device_size).unwrap();
    file.write_all(&encoded).unwrap();
    file.sync_all().unwrap();
}

fn serialized_record(
    version: u32,
    key: &[u8],
    value: &[u8],
    timestamp: u64,
    ttl_expiry: u64,
) -> Vec<u8> {
    let format = get_format_ref(version);
    let record = Record::new(key.to_vec(), value.to_vec(), timestamp);
    record.ttl_expiry.store(ttl_expiry, Ordering::Release);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&SECTOR_MARKER.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&format.serialize_record(&record, true));
    bytes.resize(bytes.len().div_ceil(FEOX_BLOCK_SIZE) * FEOX_BLOCK_SIZE, 0);
    bytes
}

fn write_at(path: &std::path::Path, sector: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn sectors(bytes: &[u8]) -> u64 {
    (bytes.len() / FEOX_BLOCK_SIZE) as u64
}

fn raw_records(path: &std::path::Path, version: u32) -> Vec<RawRecord> {
    let format = get_format_ref(version);
    let mut file = File::open(path).unwrap();
    let total_sectors = file.metadata().unwrap().len() / FEOX_BLOCK_SIZE as u64;
    let mut sector = FEOX_DATA_START_BLOCK;
    let mut records = Vec::new();

    while sector < total_sectors {
        let mut head = vec![0; FEOX_BLOCK_SIZE];
        file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
            .unwrap();
        file.read_exact(&mut head).unwrap();
        if u16::from_le_bytes([head[0], head[1]]) != SECTOR_MARKER {
            sector += 1;
            continue;
        }
        let Some((key, value_len, timestamp, ttl_expiry)) = format.parse_record(&head) else {
            sector += 1;
            continue;
        };
        let extent_size = format
            .total_size(key.len(), value_len)
            .div_ceil(FEOX_BLOCK_SIZE)
            * FEOX_BLOCK_SIZE;
        let mut extent = vec![0; extent_size];
        file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
            .unwrap();
        file.read_exact(&mut extent).unwrap();
        let value_offset = format.value_offset(key.len());
        records.push(RawRecord {
            key,
            value: extent[value_offset..value_offset + value_len].to_vec(),
            timestamp,
            ttl_expiry,
        });
        sector += (extent_size / FEOX_BLOCK_SIZE) as u64;
    }

    records
}

fn record<'a>(records: &'a [RawRecord], key: &[u8]) -> &'a RawRecord {
    records.iter().find(|record| record.key == key).unwrap()
}

fn metadata_versions(path: &std::path::Path) -> [Option<u32>; 2] {
    let mut file = File::open(path).unwrap();
    [0, FEOX_METADATA_BACKUP_BLOCK].map(|sector| {
        let mut block = vec![0; FEOX_BLOCK_SIZE];
        file.seek(SeekFrom::Start(sector * FEOX_BLOCK_SIZE as u64))
            .unwrap();
        file.read_exact(&mut block).unwrap();
        Metadata::from_bytes(&block).map(|metadata| metadata.version)
    })
}
