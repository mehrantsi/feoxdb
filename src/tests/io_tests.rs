use crate::constants::*;
use crate::storage::format::retirement_marker_token;
use crate::storage::io::DiskIO;
use crate::storage::metadata::Metadata;
use bytes::Bytes;
#[cfg(unix)]
use std::sync::Arc;
use tempfile::NamedTempFile;

fn create_test_disk_io() -> (DiskIO, NamedTempFile) {
    let temp_file = NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    // Set file size
    file.set_len(DEFAULT_DEVICE_SIZE).unwrap();

    #[cfg(unix)]
    let disk_io = DiskIO::new(Arc::new(file), false).unwrap();

    #[cfg(not(unix))]
    let disk_io = DiskIO::new_from_file(file).unwrap();

    (disk_io, temp_file)
}

#[test]
fn test_disk_io_creation() {
    let (_disk_io, _temp) = create_test_disk_io();
    // Should create without panic
}

#[test]
fn test_read_write_sectors() {
    let (disk_io, _temp) = create_test_disk_io();

    let test_data = vec![42u8; FEOX_BLOCK_SIZE];
    let sector = 100;

    // Write data
    disk_io.write_sectors_sync(sector, &test_data).unwrap();

    // Read it back
    let read_data = disk_io.read_sectors_sync(sector, 1).unwrap();

    assert_eq!(read_data, test_data);
}

#[test]
fn test_multiple_sector_read_write() {
    let (disk_io, _temp) = create_test_disk_io();

    let sectors = 5;
    let test_data = vec![123u8; FEOX_BLOCK_SIZE * sectors];
    let start_sector = 200;

    // Write multiple sectors
    disk_io
        .write_sectors_sync(start_sector, &test_data)
        .unwrap();

    // Read them back
    let read_data = disk_io
        .read_sectors_sync(start_sector, sectors as u64)
        .unwrap();

    assert_eq!(read_data, test_data);
}

#[test]
fn test_metadata_read_write() {
    let (disk_io, _temp) = create_test_disk_io();
    let mut metadata = Metadata::new();
    metadata.device_size = DEFAULT_DEVICE_SIZE;
    metadata.update();

    disk_io.write_metadata(&metadata.encode()).unwrap();

    let read_metadata = disk_io.read_metadata().unwrap();
    let loaded = Metadata::from_bytes(&read_metadata).unwrap();
    assert_eq!(loaded.device_size, metadata.device_size);
}

#[test]
fn test_metadata_read_falls_back_to_previous_copy() {
    let (disk_io, _temp) = create_test_disk_io();
    let mut metadata = Metadata::new();
    metadata.device_size = DEFAULT_DEVICE_SIZE;
    metadata.update();
    disk_io.initialize_store_metadata(&mut metadata).unwrap();
    disk_io.flush().unwrap();

    metadata.total_records = 1;
    metadata.update();
    disk_io.write_store_metadata(&mut metadata).unwrap();
    assert_eq!(metadata.generation(), 2);

    disk_io
        .write_sectors_sync(FEOX_METADATA_BLOCK, &vec![0; FEOX_BLOCK_SIZE])
        .unwrap();

    let read_metadata = disk_io.read_metadata().unwrap();
    let loaded = Metadata::from_bytes(&read_metadata).unwrap();
    assert_eq!(loaded.generation(), 1);
    assert_eq!(loaded.total_records, 0);
}

#[test]
fn test_batch_write() {
    let (mut disk_io, _temp) = create_test_disk_io();

    let mut batch = Vec::new();
    for i in 0..10 {
        let sector = 100 + i * 10;
        let data = vec![(i + 1) as u8; FEOX_BLOCK_SIZE];
        batch.push((sector, data));
    }

    // Batch write
    disk_io.batch_write(batch.clone()).unwrap();

    // Verify all writes
    for (sector, expected_data) in batch {
        let read_data = disk_io.read_sectors_sync(sector, 1).unwrap();
        assert_eq!(read_data, expected_data);
    }
}

#[test]
fn test_batch_write_shared_buffers() {
    let (mut disk_io, _temp) = create_test_disk_io();
    let batch = vec![
        (300, Bytes::from(vec![0x31; FEOX_BLOCK_SIZE])),
        (301, Bytes::from(vec![0x32; FEOX_BLOCK_SIZE])),
    ];

    disk_io.batch_write_bytes(&batch).unwrap();

    for (sector, expected_data) in batch {
        assert_eq!(disk_io.read_sectors_sync(sector, 1).unwrap(), expected_data);
    }
}

#[test]
fn test_flush() {
    let (disk_io, _temp) = create_test_disk_io();

    let test_data = vec![99u8; FEOX_BLOCK_SIZE];
    disk_io.write_sectors_sync(50, &test_data).unwrap();

    // Flush should not panic
    disk_io.flush().unwrap();
}

#[test]
fn test_retire_extents_marks_every_block() {
    let (disk_io, _temp) = create_test_disk_io();
    let extents = [(100, 3), (200, 1)];

    disk_io.retire_extents(&extents).unwrap();

    for &(sector, sectors) in &extents {
        for offset in 0..sectors {
            let block = disk_io
                .read_sectors_sync(sector + offset as u64, 1)
                .unwrap();
            assert_eq!(&block[..8], DELETION_MARKER);
            assert_eq!(
                u64::from_le_bytes(block[8..16].try_into().unwrap()),
                (sectors - offset) as u64
            );
            assert_eq!(
                u16::from_le_bytes(block[16..18].try_into().unwrap()),
                retirement_marker_token(sector + offset as u64, &block)
            );
            assert_eq!(block[18], RETIREMENT_COMPLETE);
        }
    }
}

#[test]
fn test_retire_extent_streams_without_changing_marker_lengths() {
    let (disk_io, _temp) = create_test_disk_io();
    let sector = 1_000;
    let sectors = 300;

    disk_io
        .write_sectors_sync(sector, &vec![0xA5; sectors * FEOX_BLOCK_SIZE])
        .unwrap();
    disk_io.retire_extents(&[(sector, sectors)]).unwrap();

    for offset in [0, 255, 256, 299] {
        let block = disk_io
            .read_sectors_sync(sector + offset as u64, 1)
            .unwrap();
        assert_eq!(&block[..8], DELETION_MARKER);
        assert_eq!(
            u64::from_le_bytes(block[8..16].try_into().unwrap()),
            (sectors - offset) as u64
        );
        assert_eq!(
            u16::from_le_bytes(block[16..18].try_into().unwrap()),
            retirement_marker_token(sector + offset as u64, &block)
        );
        assert_eq!(block[18], RETIREMENT_COMPLETE);
        assert!(block[DELETION_MARKER_SIZE..].iter().all(|byte| *byte == 0));
    }
}

#[test]
fn test_sector_alignment() {
    let (disk_io, _temp) = create_test_disk_io();

    // Data should be properly aligned to sector boundaries
    let test_data = vec![0xABu8; FEOX_BLOCK_SIZE];
    let sector = 123;

    disk_io.write_sectors_sync(sector, &test_data).unwrap();

    // Read with proper alignment
    let read_data = disk_io.read_sectors_sync(sector, 1).unwrap();
    assert_eq!(read_data.len(), FEOX_BLOCK_SIZE);
    assert_eq!(read_data, test_data);
}

#[test]
fn test_edge_sectors() {
    let (disk_io, _temp) = create_test_disk_io();

    // Test writing to first data sector (after metadata)
    let first_data_sector = FEOX_DATA_START_BLOCK;
    let test_data = vec![0x11u8; FEOX_BLOCK_SIZE];

    disk_io
        .write_sectors_sync(first_data_sector, &test_data)
        .unwrap();
    let read_data = disk_io.read_sectors_sync(first_data_sector, 1).unwrap();
    assert_eq!(read_data, test_data);
}

#[test]
fn test_shutdown() {
    let (mut disk_io, _temp) = create_test_disk_io();

    // Shutdown should complete without panic
    disk_io.shutdown();

    // Operations after shutdown might fail, but shouldn't panic
    let _ = disk_io.flush();
}

#[test]
fn test_concurrent_io() {
    use std::sync::{Arc, Mutex};
    use std::thread;

    let (disk_io, _temp) = create_test_disk_io();
    let disk_io = Arc::new(Mutex::new(disk_io));

    let mut handles = vec![];

    for i in 0..10 {
        let disk_io_clone = Arc::clone(&disk_io);
        handles.push(thread::spawn(move || {
            let sector = 500 + i * 10;
            let data = vec![(i + 1) as u8; FEOX_BLOCK_SIZE];

            disk_io_clone
                .lock()
                .unwrap()
                .write_sectors_sync(sector, &data)
                .unwrap();

            let read_data = disk_io_clone
                .lock()
                .unwrap()
                .read_sectors_sync(sector, 1)
                .unwrap();

            assert_eq!(read_data, data);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
