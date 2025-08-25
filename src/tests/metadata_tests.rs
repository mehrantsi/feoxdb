use crate::constants::*;
use crate::storage::metadata::Metadata;

#[test]
fn test_metadata_creation() {
    let metadata = Metadata::new();

    assert_eq!(metadata.version, 2);
    assert_eq!(metadata.total_records, 0);
    assert_eq!(metadata.total_size, 0);
    assert!(metadata.creation_time > 0);
    assert!(metadata.last_update_time > 0);
}

#[test]
fn test_metadata_update() {
    let mut metadata = Metadata::new();
    let initial_modified = metadata.last_update_time;

    // Sleep briefly to ensure time difference
    std::thread::sleep(std::time::Duration::from_millis(10));

    metadata.update();

    assert!(metadata.last_update_time >= initial_modified);
}

#[test]
fn test_metadata_serialization() {
    let mut metadata = Metadata::new();
    metadata.total_records = 12345;
    metadata.total_size = 67890;
    metadata.fragmentation = 42;
    metadata.device_size = DEFAULT_DEVICE_SIZE;

    let bytes = metadata.as_bytes();
    assert_eq!(bytes.len(), std::mem::size_of::<Metadata>());

    // Verify signature
    assert_eq!(&bytes[..FEOX_SIGNATURE_SIZE], FEOX_SIGNATURE);
}

#[test]
fn test_metadata_deserialization() {
    let mut original = Metadata::new();
    original.total_records = 999;
    original.total_size = 888777;
    original.fragmentation = 33;
    original.device_size = 1024 * 1024 * 1024;
    original.update();

    let bytes = original.as_bytes();
    let restored = Metadata::from_bytes(bytes).unwrap();

    assert_eq!(restored.version, original.version);
    assert_eq!(restored.total_records, original.total_records);
    assert_eq!(restored.total_size, original.total_size);
    assert_eq!(restored.fragmentation, original.fragmentation);
    assert_eq!(restored.device_size, original.device_size);
    assert_eq!(restored.creation_time, original.creation_time);
    assert_eq!(restored.last_update_time, original.last_update_time);
}

#[test]
fn test_metadata_invalid_signature() {
    let mut bytes = vec![0u8; FEOX_METADATA_SIZE];
    bytes[..4].copy_from_slice(b"FAKE");

    let result = Metadata::from_bytes(&bytes);
    assert!(result.is_none());
}

#[test]
fn test_metadata_invalid_size() {
    let bytes = vec![0u8; 100]; // Wrong size
    let result = Metadata::from_bytes(&bytes);
    assert!(result.is_none());
}

#[test]
fn test_metadata_field_updates() {
    let mut metadata = Metadata::new();

    metadata.total_records = 100;
    assert_eq!(metadata.total_records, 100);

    metadata.total_size = 1_000_000;
    assert_eq!(metadata.total_size, 1_000_000);

    metadata.fragmentation = 15;
    assert_eq!(metadata.fragmentation, 15);

    metadata.device_size = DEFAULT_DEVICE_SIZE * 2;
    assert_eq!(metadata.device_size, DEFAULT_DEVICE_SIZE * 2);
}

#[test]
fn test_metadata_time_fields() {
    let metadata = Metadata::new();

    // Times should be reasonable (after year 2020)
    let year_2020_secs = 1_577_836_800u64; // Jan 1, 2020 in seconds
    assert!(metadata.creation_time > year_2020_secs);
    assert!(metadata.last_update_time > year_2020_secs);
    assert!(metadata.last_update_time >= metadata.creation_time);
}

#[test]
fn test_metadata_roundtrip() {
    let mut metadata = Metadata::new();

    // Set various fields
    metadata.total_records = 42;
    metadata.total_size = 123456;
    metadata.fragmentation = 7;
    metadata.device_size = 999999;
    metadata.update();

    // Serialize and deserialize
    let bytes = metadata.as_bytes();
    let restored = Metadata::from_bytes(bytes).unwrap();

    // Everything should match
    assert_eq!(metadata.version, restored.version);
    assert_eq!(metadata.total_records, restored.total_records);
    assert_eq!(metadata.total_size, restored.total_size);
    assert_eq!(metadata.fragmentation, restored.fragmentation);
    assert_eq!(metadata.device_size, restored.device_size);
}
