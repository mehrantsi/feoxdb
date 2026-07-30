use super::*;
#[cfg(unix)]
use crate::storage::allocation_journal::{
    decode as decode_allocation_journal, ALLOCATION_JOURNAL_BLOCKS, ALLOCATION_JOURNAL_START_BLOCK,
};
use crate::storage::metadata::Metadata;
#[cfg(unix)]
use std::fs::File;
#[cfg(unix)]
use tempfile::NamedTempFile;

#[test]
fn failed_allocation_release_retains_its_reservation() {
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space
        .write()
        .initialize(20 * FEOX_BLOCK_SIZE as u64)
        .unwrap();

    let stats = Statistics::new();
    stats
        .disk_usage
        .store(FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);
    let entry = WriteEntry {
        op: Operation::Insert,
        record: Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 1)),
        work_status: AtomicU32::new(FEOX_DATA_START_BLOCK as u32),
        retry_count: AtomicU32::new(0),
    };
    let allocation = PreparedWrite {
        data: Vec::new(),
        sectors_needed: 1,
        entry,
        sector: Some(FEOX_DATA_START_BLOCK),
    };

    assert!(release_allocations(&free_space, std::slice::from_ref(&allocation), &stats).is_err());
    assert_eq!(
        reserved_sector(&allocation.entry),
        Some(FEOX_DATA_START_BLOCK)
    );
    assert_eq!(
        stats.disk_usage.load(Ordering::Relaxed),
        FEOX_BLOCK_SIZE as u64
    );
}

#[cfg(unix)]
#[test]
fn indeterminate_allocations_remain_owned_and_cannot_be_released() {
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space
        .write()
        .initialize(20 * FEOX_BLOCK_SIZE as u64)
        .unwrap();
    let sector = free_space.write().allocate_sectors(1).unwrap();
    let free_before = free_space.read().get_total_free();
    let stats = Statistics::new();
    stats
        .disk_usage
        .store(FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);
    let allocation = PreparedWrite {
        data: Vec::new(),
        sectors_needed: 1,
        entry: WriteEntry {
            op: Operation::Insert,
            record: Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 1)),
            work_status: AtomicU32::new(sector as u32 | RESERVATION_DIRTY),
            retry_count: AtomicU32::new(0),
        },
        sector: Some(sector),
    };

    quarantine_allocations(std::slice::from_ref(&allocation));
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(20 * FEOX_BLOCK_SIZE as u64).unwrap();
    let disk_io = Arc::new(RwLock::new(
        DiskIO::new(Arc::new(file.reopen().unwrap()), false).unwrap(),
    ));
    cleanup_failed_allocations(
        &mut disk_io.write(),
        &free_space,
        std::slice::from_ref(&allocation),
        &stats,
        false,
    )
    .unwrap();

    assert_eq!(reserved_sector(&allocation.entry), Some(sector));
    assert!(reservation_is_quarantined(&allocation.entry));
    assert_eq!(free_space.read().get_total_free(), free_before);
    assert_eq!(
        stats.disk_usage.load(Ordering::Relaxed),
        FEOX_BLOCK_SIZE as u64
    );
    assert_ne!(free_space.write().allocate_sectors(1).unwrap(), sector);
}

#[cfg(unix)]
#[test]
fn one_worker_flushes_every_shard() {
    let shard_count = (num_cpus::get() / 2).max(1);
    let device_size = (shard_count as u64 + FEOX_DATA_START_BLOCK + 16) * FEOX_BLOCK_SIZE as u64;
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(device_size).unwrap();
    let disk_io = Arc::new(RwLock::new(
        DiskIO::new(Arc::new(file.reopen().unwrap()), false).unwrap(),
    ));
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space.write().initialize(device_size).unwrap();
    let stats = Arc::new(Statistics::new());
    let mut write_buffer = WriteBuffer::new(disk_io, free_space, stats, Metadata::new().version);
    let mut records = Vec::with_capacity(shard_count);
    let mut nonce = 0_u64;

    for target_shard in 0..shard_count {
        loop {
            let key = format!("shard-{nonce}").into_bytes();
            nonce += 1;
            if write_buffer.get_shard_id(&key) == target_shard {
                let record = Arc::new(Record::new(key, b"value".to_vec(), nonce));
                write_buffer
                    .add_write(Operation::Insert, Arc::clone(&record), 0)
                    .unwrap();
                records.push(record);
                break;
            }
        }
    }

    write_buffer.start_workers(0);
    write_buffer.force_flush().unwrap();
    assert!(records
        .iter()
        .all(|record| record.sector.load(Ordering::Acquire) != 0));
    write_buffer.complete_shutdown();
}

#[cfg(unix)]
#[test]
fn force_flush_retires_all_shards_in_one_transaction() {
    let shard_count = (num_cpus::get() / 2).max(1);
    let device_size = (shard_count as u64 + FEOX_DATA_START_BLOCK + 16) * FEOX_BLOCK_SIZE as u64;
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(device_size).unwrap();
    let disk_io = Arc::new(RwLock::new(
        DiskIO::new(Arc::new(file.reopen().unwrap()), false).unwrap(),
    ));
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space.write().initialize(device_size).unwrap();
    let free_before = free_space.read().get_total_free();
    let stats = Arc::new(Statistics::new());
    let mut write_buffer = WriteBuffer::new(
        Arc::clone(&disk_io),
        Arc::clone(&free_space),
        Arc::clone(&stats),
        Metadata::new().version,
    );
    let mut records = Vec::with_capacity(shard_count);
    let mut nonce = 0_u64;

    for target_shard in 0..shard_count {
        loop {
            let key = format!("delete-shard-{nonce}").into_bytes();
            nonce += 1;
            if write_buffer.get_shard_id(&key) != target_shard {
                continue;
            }
            let sector = free_space.write().allocate_sectors(1).unwrap();
            let record = Arc::new(Record::new(key, b"value".to_vec(), nonce));
            record.sector.store(sector, Ordering::Release);
            write_buffer
                .add_write(Operation::Delete, Arc::clone(&record), record.value_len)
                .unwrap();
            records.push(record);
            break;
        }
    }
    stats
        .disk_usage
        .store((records.len() * FEOX_BLOCK_SIZE) as u64, Ordering::Relaxed);

    write_buffer.start_workers(shard_count);
    write_buffer.force_flush().unwrap();

    assert_eq!(free_space.read().get_total_free(), free_before);
    let journal = disk_io
        .read()
        .read_sectors_sync(ALLOCATION_JOURNAL_START_BLOCK, ALLOCATION_JOURNAL_BLOCKS)
        .unwrap();
    let state = decode_allocation_journal(&journal, device_size / FEOX_BLOCK_SIZE as u64).unwrap();
    assert_eq!(state.generation, 2);
    assert!(state.extents.is_empty());
    write_buffer.complete_shutdown();
}

#[cfg(unix)]
#[test]
fn failed_retirement_retains_every_reservation() {
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space
        .write()
        .initialize(24 * FEOX_BLOCK_SIZE as u64)
        .unwrap();
    let sectors = {
        let mut free_space = free_space.write();
        [
            free_space.allocate_sectors(1).unwrap(),
            free_space.allocate_sectors(1).unwrap(),
        ]
    };
    let free_before = free_space.read().get_total_free();

    let allocations = sectors
        .iter()
        .map(|&sector| PreparedWrite {
            data: Vec::new(),
            sectors_needed: 1,
            entry: WriteEntry {
                op: Operation::Insert,
                record: Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 1)),
                work_status: AtomicU32::new(sector as u32 | RESERVATION_DIRTY),
                retry_count: AtomicU32::new(0),
            },
            sector: Some(sector),
        })
        .collect::<Vec<_>>();

    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(24 * FEOX_BLOCK_SIZE as u64).unwrap();
    let read_only = File::open(file.path()).unwrap();
    let disk_io = Arc::new(RwLock::new(
        DiskIO::new(Arc::new(read_only), false).unwrap(),
    ));
    let stats = Statistics::new();
    stats.disk_usage.store(
        (allocations.len() * FEOX_BLOCK_SIZE) as u64,
        Ordering::Relaxed,
    );

    assert!(cleanup_failed_allocations(
        &mut disk_io.write(),
        &free_space,
        &allocations,
        &stats,
        false,
    )
    .is_err());
    for (allocation, sector) in allocations.iter().zip(sectors) {
        assert_eq!(reserved_sector(&allocation.entry), Some(sector));
        assert!(reservation_is_dirty(&allocation.entry));
    }
    assert_eq!(free_space.read().get_total_free(), free_before);
    assert_eq!(
        stats.disk_usage.load(Ordering::Relaxed),
        (allocations.len() * FEOX_BLOCK_SIZE) as u64
    );
}

#[cfg(unix)]
#[test]
fn failed_adjacent_cleanup_keeps_the_tail_reserved() {
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space
        .write()
        .initialize(24 * FEOX_BLOCK_SIZE as u64)
        .unwrap();
    let (first, second) = {
        let mut free_space = free_space.write();
        let first = free_space.allocate_sectors(1).unwrap();
        let second = free_space.allocate_sectors(1).unwrap();
        assert_eq!(second, first + 1);
        free_space.release_sectors(first, 1).unwrap();
        (first, second)
    };
    let free_before = free_space.read().get_total_free();

    let allocations = [first, second].map(|sector| PreparedWrite {
        data: Vec::new(),
        sectors_needed: 1,
        entry: WriteEntry {
            op: Operation::Insert,
            record: Arc::new(Record::new(b"key".to_vec(), b"value".to_vec(), 1)),
            work_status: AtomicU32::new(sector as u32 | RESERVATION_DIRTY),
            retry_count: AtomicU32::new(0),
        },
        sector: Some(sector),
    });

    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(24 * FEOX_BLOCK_SIZE as u64).unwrap();
    let mut disk_io = DiskIO::new(Arc::new(file.reopen().unwrap()), false).unwrap();
    let stats = Statistics::new();
    stats
        .disk_usage
        .store(2 * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);

    assert!(
        cleanup_failed_allocations(&mut disk_io, &free_space, &allocations, &stats, false,)
            .is_err()
    );

    for (allocation, sector) in allocations.iter().zip([first, second]) {
        assert_eq!(reserved_sector(&allocation.entry), Some(sector));
        assert!(reservation_is_dirty(&allocation.entry));
    }
    assert_eq!(free_space.read().get_total_free(), free_before);
    assert_eq!(
        stats.disk_usage.load(Ordering::Relaxed),
        2 * FEOX_BLOCK_SIZE as u64
    );

    let mut free_space = free_space.write();
    assert_eq!(free_space.allocate_sectors(1).unwrap(), first);
    assert_ne!(free_space.allocate_sectors(1).unwrap(), second);
}

#[test]
fn adjacent_retirements_are_published_to_free_space_as_one_unit() {
    let mut free_space = FreeSpaceManager::new();
    free_space.initialize(24 * FEOX_BLOCK_SIZE as u64).unwrap();
    let first = free_space.allocate_sectors(1).unwrap();
    let second = free_space.allocate_sectors(1).unwrap();
    assert_eq!(second, first + 1);
    free_space.release_sectors(first, 1).unwrap();
    let free_before = free_space.get_total_free();

    let mut group = vec![
        WriteEntry {
            op: Operation::Delete,
            record: Arc::new(Record::new(b"first".to_vec(), b"value".to_vec(), 1)),
            work_status: AtomicU32::new(DELETE_MARKER_DURABLE),
            retry_count: AtomicU32::new(0),
        },
        WriteEntry {
            op: Operation::Delete,
            record: Arc::new(Record::new(b"second".to_vec(), b"value".to_vec(), 2)),
            work_status: AtomicU32::new(DELETE_MARKER_DURABLE),
            retry_count: AtomicU32::new(0),
        },
    ];
    group[0].record.sector.store(first, Ordering::Release);
    group[1].record.sector.store(second, Ordering::Release);

    let stats = Statistics::new();
    let mut retries = Vec::new();
    let mut error = None;
    let mut released_sectors = 0;
    release_retirement_group(
        &mut group,
        &mut free_space,
        &stats,
        get_format_ref(Metadata::new().version),
        &mut retries,
        &mut error,
        &mut released_sectors,
    );

    assert!(error.is_some());
    assert_eq!(retries.len(), 2);
    assert_eq!(released_sectors, 0);
    assert_eq!(free_space.get_total_free(), free_before);
}

#[cfg(unix)]
fn tiny_write_buffer() -> (WriteBuffer, Arc<RwLock<FreeSpaceManager>>, Arc<Statistics>) {
    let device_size = (FEOX_DATA_START_BLOCK + 2) * FEOX_BLOCK_SIZE as u64;
    let file = NamedTempFile::new().unwrap();
    file.as_file().set_len(device_size).unwrap();
    let disk_io = Arc::new(RwLock::new(
        DiskIO::new(Arc::new(file.reopen().unwrap()), false).unwrap(),
    ));
    let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
    free_space.write().initialize(device_size).unwrap();
    let stats = Arc::new(Statistics::new());
    let write_buffer = WriteBuffer::new(
        disk_io,
        Arc::clone(&free_space),
        Arc::clone(&stats),
        Metadata::new().version,
    );
    (write_buffer, free_space, stats)
}

#[cfg(unix)]
fn worker_context(write_buffer: &WriteBuffer) -> WorkerContext {
    WorkerContext {
        worker_id: 0,
        worker_count: 1,
        disk_io: Arc::clone(&write_buffer.disk_io),
        free_space: Arc::clone(&write_buffer.free_space),
        sharded_buffers: Arc::clone(&write_buffer.sharded_buffers),
        shutdown: Arc::clone(&write_buffer.shutdown),
        stats: Arc::clone(&write_buffer.stats),
        retirement_queue: Arc::clone(&write_buffer.retirement_queue),
        format_version: write_buffer.format_version,
        fault_scope: write_buffer.fault_scope,
    }
}

#[cfg(unix)]
#[test]
fn out_of_space_retires_a_durable_predecessor_and_retries_the_write() {
    let (write_buffer, free_space, stats) = tiny_write_buffer();
    let (old_sector, successor_sector) = {
        let mut free_space = free_space.write();
        (
            free_space.allocate_sectors(1).unwrap(),
            free_space.allocate_sectors(1).unwrap(),
        )
    };
    stats
        .disk_usage
        .store(2 * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);

    let old = Arc::new(Record::new(b"key".to_vec(), b"old".to_vec(), 1));
    old.sector.store(old_sector, Ordering::Release);
    let successor = Arc::new(Record::new(b"key".to_vec(), b"new".to_vec(), 2));
    successor.sector.store(successor_sector, Ordering::Release);
    old.link_successor(&successor);
    write_buffer
        .retirement_queue
        .pending
        .lock()
        .push(WriteEntry::new(Operation::Delete, old));

    let pending = Arc::new(Record::new(b"pending".to_vec(), b"value".to_vec(), 3));
    write_buffer
        .add_write(Operation::Insert, Arc::clone(&pending), 0)
        .unwrap();
    let ctx = worker_context(&write_buffer);
    let format = get_format_ref(write_buffer.format_version);

    assert!(flush_worker_shards(&ctx, format, true).unwrap());
    assert_eq!(free_space.read().get_total_free(), FEOX_BLOCK_SIZE as u64);
    assert_eq!(pending.sector.load(Ordering::Acquire), 0);

    assert!(!flush_worker_shards(&ctx, format, true).unwrap());
    assert_eq!(pending.sector.load(Ordering::Acquire), old_sector);
    assert_eq!(free_space.read().get_total_free(), 0);
}

#[cfg(unix)]
#[test]
fn out_of_space_retires_a_delete_from_the_same_drained_batch() {
    let (write_buffer, free_space, stats) = tiny_write_buffer();
    let (deleted_sector, _) = {
        let mut free_space = free_space.write();
        (
            free_space.allocate_sectors(1).unwrap(),
            free_space.allocate_sectors(1).unwrap(),
        )
    };
    stats
        .disk_usage
        .store(2 * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);

    let deleted = Arc::new(Record::new(b"deleted".to_vec(), b"old".to_vec(), 1));
    deleted.sector.store(deleted_sector, Ordering::Release);
    deleted.refcount.store(0, Ordering::Release);
    let shard = write_buffer.get_shard_id(&deleted.key);
    let pending_key = (0_u64..)
        .map(|candidate| format!("pending-{candidate}").into_bytes())
        .find(|key| write_buffer.get_shard_id(key) == shard)
        .unwrap();
    let pending = Arc::new(Record::new(pending_key, b"value".to_vec(), 2));

    write_buffer
        .add_write(Operation::Delete, deleted, 0)
        .unwrap();
    write_buffer
        .add_write(Operation::Insert, Arc::clone(&pending), 0)
        .unwrap();
    let ctx = worker_context(&write_buffer);
    let format = get_format_ref(write_buffer.format_version);

    assert!(flush_worker_shards(&ctx, format, true).unwrap());
    assert_eq!(pending.sector.load(Ordering::Acquire), 0);
    assert_eq!(free_space.read().get_total_free(), FEOX_BLOCK_SIZE as u64);

    assert!(!flush_worker_shards(&ctx, format, true).unwrap());
    assert_eq!(pending.sector.load(Ordering::Acquire), deleted_sector);
    assert_eq!(free_space.read().get_total_free(), 0);
}

#[cfg(unix)]
#[test]
fn out_of_space_keeps_an_undurable_predecessor_and_returns_the_error() {
    let (write_buffer, free_space, stats) = tiny_write_buffer();
    let (old_sector, _) = {
        let mut free_space = free_space.write();
        (
            free_space.allocate_sectors(1).unwrap(),
            free_space.allocate_sectors(1).unwrap(),
        )
    };
    stats
        .disk_usage
        .store(2 * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);

    let old = Arc::new(Record::new(b"key".to_vec(), b"old".to_vec(), 1));
    old.sector.store(old_sector, Ordering::Release);
    let successor = Arc::new(Record::new(b"key".to_vec(), b"new".to_vec(), 2));
    old.link_successor(&successor);
    write_buffer
        .retirement_queue
        .pending
        .lock()
        .push(WriteEntry::new(Operation::Delete, Arc::clone(&old)));

    let pending = Arc::new(Record::new(b"pending".to_vec(), b"value".to_vec(), 3));
    write_buffer
        .add_write(Operation::Insert, Arc::clone(&pending), 0)
        .unwrap();
    let ctx = worker_context(&write_buffer);
    let error =
        flush_worker_shards(&ctx, get_format_ref(write_buffer.format_version), true).unwrap_err();

    assert!(matches!(error, FeoxError::OutOfSpace));
    assert_eq!(free_space.read().get_total_free(), 0);
    assert_eq!(pending.sector.load(Ordering::Acquire), 0);
    assert!(old.acquire_extent().is_some());
    assert_eq!(write_buffer.retirement_queue.pending.lock().len(), 1);
    assert!(!final_flush_error_is_retryable(&FeoxError::OutOfSpace));
    assert!(!final_flush_error_is_retryable(
        &FeoxError::AllocationFailed
    ));
}
