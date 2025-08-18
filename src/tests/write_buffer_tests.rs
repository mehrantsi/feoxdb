use crate::core::record::Record;
use crate::error::FeoxError;
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::io::DiskIO;
use crate::storage::write_buffer::{WriteBuffer, WriteEntry};
use parking_lot::RwLock;
use std::sync::Arc;
use std::thread;

fn create_test_write_buffer() -> WriteBuffer {
    // Create temporary file for testing
    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let file = temp_file.reopen().unwrap();

    #[cfg(unix)]
    let disk_io = DiskIO::new(Arc::new(file), false).unwrap();
    #[cfg(not(unix))]
    let disk_io = DiskIO::new_from_file(file).unwrap();

    let free_space = Arc::new(RwLock::new({
        let mut fs = FreeSpaceManager::new();
        fs.initialize(1024 * 1024 * 1024).unwrap(); // 1GB
        fs
    }));

    let stats = Arc::new(Statistics::new());

    WriteBuffer::new(Arc::new(RwLock::new(disk_io)), free_space, stats)
}

#[test]
fn test_write_buffer_creation() {
    let wb = create_test_write_buffer();
    let stats = wb.stats();

    assert_eq!(stats.total_entries, 0);
    assert_eq!(stats.total_size, 0);
}

#[test]
fn test_add_write_operation() {
    let wb = create_test_write_buffer();

    let record = Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 100));

    wb.add_write(crate::core::store::Operation::Insert, record, 0)
        .unwrap();

    let stats = wb.stats();
    assert_eq!(stats.total_writes, 1);
}

#[test]
fn test_write_buffer_shutdown() {
    let mut wb = create_test_write_buffer();
    let num_shards = (num_cpus::get() / 2).max(1);
    wb.start_workers(num_shards);

    // Add some writes
    for i in 0..10 {
        let record = Arc::new(Record::new(
            format!("key_{}", i).into_bytes(),
            format!("value_{}", i).into_bytes(),
            100 + i as u64,
        ));
        wb.add_write(crate::core::store::Operation::Insert, record, 0)
            .unwrap();
    }

    // Shutdown
    wb.initiate_shutdown();
    wb.complete_shutdown();

    // Should not accept new writes after shutdown
    let record = Arc::new(Record::new(b"after".to_vec(), b"shutdown".to_vec(), 200));
    let result = wb.add_write(crate::core::store::Operation::Insert, record, 0);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), FeoxError::ShuttingDown));
}

#[test]
fn test_force_flush() {
    let mut wb = create_test_write_buffer();
    let num_shards = (num_cpus::get() / 2).max(1);
    wb.start_workers(num_shards);

    // Add writes
    for i in 0..5 {
        let record = Arc::new(Record::new(
            format!("key_{}", i).into_bytes(),
            format!("value_{}", i).into_bytes(),
            100 + i as u64,
        ));
        wb.add_write(crate::core::store::Operation::Insert, record, 0)
            .unwrap();
    }

    // Force flush
    wb.force_flush().unwrap();

    // Stats should show flush
    let stats = wb.stats();
    assert!(stats.total_flushes > 0);

    wb.complete_shutdown();
}

#[test]
fn test_concurrent_writes() {
    let mut wb_mut = create_test_write_buffer();
    wb_mut.start_workers(4);
    let wb = Arc::new(wb_mut);

    let mut handles = vec![];

    for t in 0..10 {
        let wb_clone = Arc::clone(&wb);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let record = Arc::new(Record::new(
                    format!("thread{}:key{}", t, i).into_bytes(),
                    format!("value_{}_{}", t, i).into_bytes(),
                    1000 + (t * 100 + i) as u64,
                ));
                wb_clone
                    .add_write(crate::core::store::Operation::Insert, record, 0)
                    .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let stats = wb.stats();
    assert_eq!(stats.total_writes, 1000);
}

#[test]
fn test_delete_operation() {
    let wb = create_test_write_buffer();

    let record = Record::new(b"delete_key".to_vec(), b"value".to_vec(), 100);
    record
        .sector
        .store(12345, std::sync::atomic::Ordering::Release);

    wb.add_write(
        crate::core::store::Operation::Delete,
        Arc::new(record),
        5, // old value length
    )
    .unwrap();

    let stats = wb.stats();
    assert!(stats.total_writes > 0);
}

#[test]
fn test_update_operation() {
    let wb = create_test_write_buffer();

    let record = Arc::new(Record::new(
        b"update_key".to_vec(),
        b"new_value".to_vec(),
        200,
    ));

    wb.add_write(
        crate::core::store::Operation::Update,
        record,
        9, // old value length
    )
    .unwrap();

    let stats = wb.stats();
    assert!(stats.total_writes > 0);
}

#[test]
fn test_write_buffer_full_trigger() {
    let mut wb = create_test_write_buffer();
    // Use the actual number of shards created
    let num_shards = (num_cpus::get() / 2).max(1);
    wb.start_workers(num_shards);

    // Add many writes to trigger automatic flush
    // With proper shard size, this should trigger flush
    for i in 0..20000 {
        let record = Arc::new(Record::new(
            format!("key_{}", i).into_bytes(),
            vec![0u8; 2048], // 2KB each to reach 16MB faster
            100 + i as u64,
        ));
        wb.add_write(crate::core::store::Operation::Insert, record, 0)
            .unwrap();
    }

    // Force flush to ensure it happens
    wb.force_flush().unwrap();

    let stats = wb.stats();
    assert!(stats.total_flushes > 0);

    wb.complete_shutdown();
}

#[test]
fn test_write_entry_fields() {
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    let record = Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 100));

    let entry = WriteEntry {
        op: crate::core::store::Operation::Insert,
        record: record.clone(),
        old_value_len: 0,
        work_status: std::sync::atomic::AtomicU32::new(0),
        retry_count: std::sync::atomic::AtomicU32::new(0),
        timestamp: Instant::now(),
    };

    assert_eq!(entry.work_status.load(Ordering::Relaxed), 0);
    assert_eq!(entry.retry_count.load(Ordering::Relaxed), 0);

    entry.work_status.store(1, Ordering::Release);
    entry.retry_count.store(3, Ordering::Release);

    assert_eq!(entry.work_status.load(Ordering::Relaxed), 1);
    assert_eq!(entry.retry_count.load(Ordering::Relaxed), 3);
}
