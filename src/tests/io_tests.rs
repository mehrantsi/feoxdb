use crate::constants::*;
use crate::storage::io::DiskIO;
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

    let metadata = vec![0xFE, 0x0B, 0xDB, 0x00]; // Example metadata
    let padded_metadata = {
        let mut data = metadata.clone();
        data.resize(FEOX_METADATA_SIZE, 0);
        data
    };

    // Write metadata
    disk_io.write_metadata(&padded_metadata).unwrap();

    // Read it back
    let read_metadata = disk_io.read_metadata().unwrap();

    assert_eq!(read_metadata, padded_metadata);
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
fn test_flush() {
    let (disk_io, _temp) = create_test_disk_io();

    let test_data = vec![99u8; FEOX_BLOCK_SIZE];
    disk_io.write_sectors_sync(50, &test_data).unwrap();

    // Flush should not panic
    disk_io.flush().unwrap();
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
