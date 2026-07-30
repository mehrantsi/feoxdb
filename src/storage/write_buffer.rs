use ahash::RandomState;
use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver, Sender};
use crossbeam_utils::CachePadded;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::stats::Statistics;
use crate::storage::allocation_journal::ALLOCATION_JOURNAL_MAX_ENTRIES;
use crate::storage::format::{get_format_ref, sector_holds_record, RecordFormat};
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::io::DiskIO;
use crate::storage::seq_token::{stamp_seq_token, SEQ_TOKEN_MIN_VERSION};
use crate::test_hooks::{fail_at, RECORD_WRITE};

/// Sharded write buffer for reducing contention
/// Each thread consistently uses the same shard to improve cache locality
#[repr(align(64))] // Cache line alignment
pub struct ShardedWriteBuffer {
    /// Buffered writes pending flush
    buffer: Mutex<VecDeque<WriteEntry>>,

    /// Number of entries in buffer
    count: AtomicUsize,

    /// Total size of buffered data
    size: AtomicUsize,
}

/// Write entry for buffered operations
pub struct WriteEntry {
    pub op: Operation,
    pub record: Arc<Record>,
    pub work_status: AtomicU32,
    pub retry_count: AtomicU32,
}

impl WriteEntry {
    fn new(op: Operation, record: Arc<Record>) -> Self {
        Self {
            op,
            record,
            work_status: AtomicU32::new(0),
            retry_count: AtomicU32::new(0),
        }
    }
}

const DELETE_MARKER_DURABLE: u32 = 1;
const RESERVATION_DIRTY: u32 = 1 << 31;
const RESERVATION_QUARANTINED: u32 = 1 << 30;
const RESERVATION_FLAGS: u32 = RESERVATION_DIRTY | RESERVATION_QUARANTINED;
const FINAL_FLUSH_RETRY_LIMIT: usize = 1_024;

#[inline]
fn reserved_sector(entry: &WriteEntry) -> Option<u64> {
    let sector = entry.work_status.load(Ordering::Acquire) & !RESERVATION_FLAGS;
    (sector != 0).then_some(sector as u64)
}

#[inline]
fn reserve_sector(entry: &WriteEntry, sector: u64) {
    let sector = u32::try_from(sector).expect("device sector exceeds supported range");
    debug_assert_eq!(sector & RESERVATION_FLAGS, 0);
    entry.work_status.store(sector, Ordering::Release);
}

#[inline]
fn mark_reservation_dirty(entry: &WriteEntry) {
    entry
        .work_status
        .fetch_or(RESERVATION_DIRTY, Ordering::AcqRel);
}

#[inline]
fn mark_reservation_clean(entry: &WriteEntry) {
    entry
        .work_status
        .fetch_and(!RESERVATION_DIRTY, Ordering::AcqRel);
}

#[inline]
fn reservation_is_dirty(entry: &WriteEntry) -> bool {
    entry.work_status.load(Ordering::Acquire) & RESERVATION_DIRTY != 0
}

#[inline]
fn quarantine_reservation(entry: &WriteEntry) {
    entry
        .work_status
        .fetch_or(RESERVATION_QUARANTINED, Ordering::AcqRel);
}

#[inline]
fn reservation_is_quarantined(entry: &WriteEntry) -> bool {
    entry.work_status.load(Ordering::Acquire) & RESERVATION_QUARANTINED != 0
}

#[inline]
fn clear_reserved_sector(entry: &WriteEntry) {
    entry.work_status.store(0, Ordering::Release);
}

struct PreparedWrite {
    data: Vec<u8>,
    sectors_needed: usize,
    entry: WriteEntry,
    sector: Option<u64>,
}

struct BatchOutcome {
    result: Result<()>,
    retries: Vec<WriteEntry>,
}

struct BatchFailure {
    error: FeoxError,
    clear_journal: bool,
    indeterminate: bool,
}

struct RetirementQueue {
    pending: Mutex<Vec<WriteEntry>>,
    flush: Mutex<()>,
    released_sectors: AtomicU64,
}

impl RetirementQueue {
    fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::new()),
            flush: Mutex::new(()),
            released_sectors: AtomicU64::new(0),
        }
    }
}

/// Main write buffer coordinator
pub struct WriteBuffer {
    /// Sharded buffers to reduce contention between threads
    sharded_buffers: Arc<Vec<CachePadded<ShardedWriteBuffer>>>,

    /// Shared disk I/O handle
    disk_io: Arc<RwLock<DiskIO>>,

    /// Free space manager for sector allocation
    free_space: Arc<RwLock<FreeSpaceManager>>,

    /// Per-worker channels for targeted flush requests
    worker_channels: Vec<Sender<FlushRequest>>,

    /// Background worker handles
    worker_handles: Mutex<Vec<JoinHandle<()>>>,

    /// Periodic flush thread handle
    periodic_flush_handle: Mutex<Option<JoinHandle<()>>>,

    /// Shutdown flag
    shutdown: Arc<AtomicBool>,

    /// Shared statistics
    stats: Arc<Statistics>,

    /// Stable per-store key hasher for preserving per-key write order
    shard_hasher: RandomState,

    retirement_queue: Arc<RetirementQueue>,

    /// Format version for record serialization
    format_version: u32,

    fault_scope: usize,
}

#[derive(Debug)]
struct FlushRequest {
    response: Option<Sender<Result<bool>>>,
    defer_retirements: bool,
}

struct WorkerContext {
    worker_id: usize,
    worker_count: usize,
    disk_io: Arc<RwLock<DiskIO>>,
    free_space: Arc<RwLock<FreeSpaceManager>>,
    sharded_buffers: Arc<Vec<CachePadded<ShardedWriteBuffer>>>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<Statistics>,
    retirement_queue: Arc<RetirementQueue>,
    format_version: u32,
    fault_scope: usize,
}

impl ShardedWriteBuffer {
    fn new(_shard_id: usize) -> Self {
        Self {
            buffer: Mutex::new(VecDeque::new()),
            count: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
        }
    }

    fn add_entries<const N: usize>(
        &self,
        entries: [WriteEntry; N],
        shutdown: &AtomicBool,
    ) -> Result<()> {
        let mut buffer = self.buffer.lock();
        if shutdown.load(Ordering::Acquire) {
            return Err(FeoxError::ShuttingDown);
        }

        let entry_size = entries
            .iter()
            .map(|entry| entry.record.calculate_size())
            .sum::<usize>();
        buffer.extend(entries);

        self.count.fetch_add(N, Ordering::Relaxed);
        self.size.fetch_add(entry_size, Ordering::Relaxed);
        Ok(())
    }

    fn drain_entries(&self) -> Vec<WriteEntry> {
        let mut buffer = self.buffer.lock();
        let entries: Vec<_> = buffer.drain(..).collect();

        self.count.store(0, Ordering::Relaxed);
        self.size.store(0, Ordering::Relaxed);

        entries
    }

    fn requeue_entries(&self, entries: Vec<WriteEntry>, stats: &Arc<Statistics>, failed: bool) {
        if entries.is_empty() {
            return;
        }

        let count = entries.len();
        let size = entries
            .iter()
            .map(|entry| entry.record.calculate_size())
            .sum();
        let mut buffer = self.buffer.lock();
        for entry in entries.into_iter().rev() {
            if failed {
                let retries = entry.retry_count.fetch_add(1, Ordering::Relaxed) + 1;
                if retries == WRITE_ENTRY_RETRY_ALARM {
                    stats.record_write_entry_stuck();
                    eprintln!(
                        "feox: write entry for a {} byte key has been retried {} times",
                        entry.record.key.len(),
                        retries
                    );
                }
            }
            buffer.push_front(entry);
        }
        self.count.fetch_add(count, Ordering::Relaxed);
        self.size.fetch_add(size, Ordering::Relaxed);
    }

    fn is_full(&self) -> bool {
        self.count.load(Ordering::Relaxed) >= WRITE_BUFFER_SIZE
            || self.size.load(Ordering::Relaxed) >= FEOX_WRITE_BUFFER_SIZE
    }
}

impl WriteBuffer {
    pub fn new(
        disk_io: Arc<RwLock<DiskIO>>,
        free_space: Arc<RwLock<FreeSpaceManager>>,
        stats: Arc<Statistics>,
        format_version: u32,
    ) -> Self {
        // Use half CPU count for both shards and workers
        let num_shards = (num_cpus::get() / 2).max(1);

        let sharded_buffers = Arc::new(
            (0..num_shards)
                .map(|shard_id| CachePadded::new(ShardedWriteBuffer::new(shard_id)))
                .collect(),
        );

        Self {
            sharded_buffers,
            disk_io,
            free_space,
            worker_channels: Vec::new(),
            worker_handles: Mutex::new(Vec::new()),
            periodic_flush_handle: Mutex::new(None),
            shutdown: Arc::new(AtomicBool::new(false)),
            stats,
            shard_hasher: RandomState::new(),
            retirement_queue: Arc::new(RetirementQueue::new()),
            format_version,
            fault_scope: crate::test_hooks::new_fault_scope(),
        }
    }

    #[cfg(test)]
    pub(crate) fn fault_scope(&self) -> usize {
        self.fault_scope
    }

    /// Add write operation to buffer (lock-free fast path)
    pub fn add_write(
        &self,
        op: Operation,
        record: Arc<Record>,
        _old_value_len: usize,
    ) -> Result<()> {
        let shard_id = self.get_shard_id(&record.key);
        let buffer = &self.sharded_buffers[shard_id];
        buffer.add_entries([WriteEntry::new(op, record)], &self.shutdown)?;
        self.stats.record_write_buffered();
        self.trigger_flush(shard_id, buffer);
        Ok(())
    }

    pub(crate) fn add_replacement(&self, record: Arc<Record>, replaced: Arc<Record>) -> Result<()> {
        debug_assert_eq!(record.key, replaced.key);
        let shard_id = self.get_shard_id(&record.key);
        let buffer = &self.sharded_buffers[shard_id];
        buffer.add_entries(
            [
                WriteEntry::new(Operation::Update, record),
                WriteEntry::new(Operation::Delete, replaced),
            ],
            &self.shutdown,
        )?;
        self.stats.record_writes_buffered(2);
        self.trigger_flush(shard_id, buffer);
        Ok(())
    }

    fn trigger_flush(&self, shard_id: usize, buffer: &ShardedWriteBuffer) {
        if buffer.is_full() && !self.worker_channels.is_empty() {
            let worker_id = shard_id % self.worker_channels.len();
            let req = FlushRequest {
                response: None,
                defer_retirements: false,
            };
            let _ = self.worker_channels[worker_id].try_send(req);
        }
    }

    /// Start background worker threads
    pub fn start_workers(&mut self, num_workers: usize) {
        // Ensure we have the right number of workers for shards
        let num_shards = self.sharded_buffers.len();
        let actual_workers = num_workers.clamp(1, num_shards);

        // Create per-worker channels
        let mut receivers = Vec::new();
        for _ in 0..actual_workers {
            let (tx, rx) = bounded(2);
            self.worker_channels.push(tx);
            receivers.push(rx);
        }

        // Start workers, each owning one shard
        for (worker_id, flush_rx) in receivers.into_iter().enumerate() {
            let ctx = WorkerContext {
                worker_id,
                worker_count: actual_workers,
                disk_io: self.disk_io.clone(),
                free_space: self.free_space.clone(),
                sharded_buffers: self.sharded_buffers.clone(),
                shutdown: self.shutdown.clone(),
                stats: self.stats.clone(),
                retirement_queue: self.retirement_queue.clone(),
                format_version: self.format_version,
                fault_scope: self.fault_scope,
            };

            let handle = thread::spawn(move || {
                write_buffer_worker(ctx, flush_rx);
            });

            self.worker_handles.get_mut().push(handle);
        }

        // Start periodic flush coordinator
        let worker_channels = self.worker_channels.clone();
        let shutdown = self.shutdown.clone();
        let sharded_buffers = self.sharded_buffers.clone();
        let retirement_queue = Arc::clone(&self.retirement_queue);

        let periodic_handle = thread::spawn(move || {
            let interval = WRITE_BUFFER_FLUSH_INTERVAL;

            while !shutdown.load(Ordering::Acquire) {
                thread::sleep(interval);

                let retirements_pending = !retirement_queue.pending.lock().is_empty();
                for (worker_id, channel) in worker_channels.iter().enumerate() {
                    let pending = (worker_id..sharded_buffers.len())
                        .step_by(worker_channels.len())
                        .any(|shard_id| {
                            sharded_buffers[shard_id].count.load(Ordering::Relaxed) > 0
                        });
                    if pending || (worker_id == 0 && retirements_pending) {
                        let _ = channel.try_send(FlushRequest {
                            response: None,
                            defer_retirements: false,
                        });
                    }
                }
            }
        });

        *self.periodic_flush_handle.get_mut() = Some(periodic_handle);
    }

    /// Force flush and wait for completion
    pub fn force_flush(&self) -> Result<()> {
        let mut pending_workers: Vec<_> = (0..self.worker_channels.len()).collect();
        let mut retry_delay_us = 50;
        let format = get_format_ref(self.format_version);

        loop {
            let mut responses = Vec::with_capacity(pending_workers.len());
            for worker_id in pending_workers.drain(..) {
                let (tx, rx) = bounded(1);
                self.worker_channels[worker_id]
                    .send(FlushRequest {
                        response: Some(tx),
                        defer_retirements: true,
                    })
                    .map_err(|_| FeoxError::ChannelError)?;
                responses.push((worker_id, rx));
            }

            let mut first_error = None;
            for (worker_id, rx) in responses {
                match rx.recv() {
                    Ok(Ok(true)) => pending_workers.push(worker_id),
                    Ok(Ok(false)) => {}
                    Ok(Err(error)) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(_) => {
                        if first_error.is_none() {
                            first_error = Some(FeoxError::ChannelError);
                        }
                    }
                }
            }

            if let Some(error) = first_error {
                return Err(error);
            }
            if flush_pending_deletions(
                &self.retirement_queue,
                &self.disk_io,
                &self.free_space,
                &self.stats,
                format,
            )? {
                pending_workers = (0..self.worker_channels.len()).collect();
            }
            if pending_workers.is_empty() {
                return Ok(());
            }
            if !pending_workers.is_empty() {
                thread::sleep(Duration::from_micros(retry_delay_us));
                retry_delay_us = (retry_delay_us * 2).min(1_000);
            }
        }
    }

    /// Shutdown write buffer
    pub fn initiate_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);

        // Don't call force_flush here as it can block
        // Workers will see the shutdown flag and exit gracefully
    }

    /// Complete shutdown - must be called after initiate_shutdown
    pub fn complete_shutdown(&mut self) {
        self.finish_shutdown();
    }

    pub(crate) fn finish_shutdown(&self) {
        // Ensure shutdown flag is set
        self.shutdown.store(true, Ordering::Release);

        if let Some(handle) = self.periodic_flush_handle.lock().take() {
            let _ = handle.join();
        }

        // Signal workers to stop and wait
        let handles = std::mem::take(&mut *self.worker_handles.lock());
        for handle in handles {
            let _ = handle.join();
        }

        // Note: disk_io shutdown is handled by the Store's Drop implementation
        // to ensure proper ordering
    }

    /// Legacy shutdown for compatibility
    pub fn shutdown(&mut self) {
        self.complete_shutdown();
    }

    #[inline]
    fn get_shard_id(&self, key: &[u8]) -> usize {
        self.shard_hasher.hash_one(key) as usize % self.sharded_buffers.len()
    }
}

/// Background worker for processing write buffer flushes
fn write_buffer_worker(ctx: WorkerContext, flush_rx: Receiver<FlushRequest>) {
    let format = get_format_ref(ctx.format_version);

    loop {
        if ctx.shutdown.load(Ordering::Acquire) {
            break;
        }

        // Wait for flush request with timeout to check shutdown periodically
        let req = match flush_rx.recv_timeout(Duration::from_millis(500)) {
            Ok(req) => req,
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                continue;
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                break;
            }
        };

        let result = flush_worker_shards(&ctx, format, !req.defer_retirements);
        if let Some(tx) = req.response {
            let _ = tx.send(result);
        }
    }

    if ctx.shutdown.load(Ordering::Acquire) {
        let mut retry_delay_us = 50;
        let mut retries = 0;
        loop {
            match flush_worker_shards(&ctx, format, true) {
                Ok(false) => break,
                Ok(true) => {
                    retries += 1;
                    if retries == FINAL_FLUSH_RETRY_LIMIT {
                        eprintln!("feox: final write-buffer flush left pending retirements");
                        break;
                    }
                    thread::sleep(Duration::from_micros(retry_delay_us));
                    retry_delay_us = (retry_delay_us * 2).min(1_000);
                }
                Err(error @ FeoxError::IndeterminateWrite(_)) => {
                    eprintln!("feox: final write-buffer flush failed: {error}");
                    break;
                }
                Err(error) => {
                    if !final_flush_error_is_retryable(&error) {
                        eprintln!("feox: final write-buffer flush failed: {error}");
                        break;
                    }
                    retries += 1;
                    if retries == FINAL_FLUSH_RETRY_LIMIT {
                        eprintln!(
                            "feox: final write-buffer flush failed after {retries} attempts: {error}"
                        );
                        break;
                    }
                    thread::sleep(Duration::from_micros(retry_delay_us));
                    retry_delay_us = (retry_delay_us * 2).min(1_000);
                }
            }
        }
    }
}

fn final_flush_error_is_retryable(error: &FeoxError) -> bool {
    matches!(error, FeoxError::IoError(_) | FeoxError::StaleExtent)
}

fn flush_worker_shards(
    ctx: &WorkerContext,
    format: &dyn RecordFormat,
    flush_retirements: bool,
) -> Result<bool> {
    let released_sectors = ctx
        .retirement_queue
        .released_sectors
        .load(Ordering::Relaxed);
    let mut has_retries = false;
    let mut first_error = None;

    for shard_id in (ctx.worker_id..ctx.sharded_buffers.len()).step_by(ctx.worker_count) {
        let buffer = &ctx.sharded_buffers[shard_id];
        let entries = buffer.drain_entries();
        if entries.is_empty() {
            continue;
        }

        let mut entries = entries.into_iter();
        let mut shard_retries = Vec::new();
        let mut shard_failed = false;

        loop {
            let batch = entries
                .by_ref()
                .take(ALLOCATION_JOURNAL_MAX_ENTRIES)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }

            let BatchOutcome { result, retries } = process_write_batch(ctx, batch, format);
            shard_retries.extend(retries);

            if let Err(error) = result {
                shard_failed = true;
                shard_retries.extend(entries);
                if first_error.is_none() {
                    first_error = Some(error);
                }
                break;
            }
        }

        has_retries |= !shard_retries.is_empty();
        buffer.requeue_entries(shard_retries, &ctx.stats, shard_failed);
        ctx.stats.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    if first_error
        .as_ref()
        .is_some_and(|error| matches!(error, FeoxError::OutOfSpace | FeoxError::AllocationFailed))
    {
        flush_pending_deletions(
            &ctx.retirement_queue,
            &ctx.disk_io,
            &ctx.free_space,
            &ctx.stats,
            format,
        )?;
        if ctx
            .retirement_queue
            .released_sectors
            .load(Ordering::Relaxed)
            != released_sectors
        {
            return Ok(true);
        }
    } else if first_error.is_none() && flush_retirements {
        match flush_pending_deletions(
            &ctx.retirement_queue,
            &ctx.disk_io,
            &ctx.free_space,
            &ctx.stats,
            format,
        ) {
            Ok(retries) => has_retries |= retries,
            Err(error) => first_error = Some(error),
        }
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(has_retries),
    }
}

fn flush_pending_deletions(
    retirement_queue: &RetirementQueue,
    disk_io: &Arc<RwLock<DiskIO>>,
    free_space: &Arc<RwLock<FreeSpaceManager>>,
    stats: &Arc<Statistics>,
    format: &dyn RecordFormat,
) -> Result<bool> {
    let _flush_guard = retirement_queue.flush.lock();
    let delete_operations = {
        let mut pending = retirement_queue.pending.lock();
        if pending.is_empty() {
            return Ok(false);
        }
        std::mem::take(&mut *pending)
    };

    let mut retries = Vec::new();
    let mut released_sectors = 0;
    let result = process_deletions(
        disk_io,
        free_space,
        stats,
        format,
        delete_operations,
        &mut retries,
        &mut released_sectors,
    );
    if released_sectors != 0 {
        retirement_queue
            .released_sectors
            .fetch_add(released_sectors, Ordering::Relaxed);
    }
    let has_retries = !retries.is_empty();
    if has_retries {
        retirement_queue.pending.lock().extend(retries);
    }
    result.map(|_| has_retries)
}

fn process_deletions(
    disk_io: &Arc<RwLock<DiskIO>>,
    free_space: &Arc<RwLock<FreeSpaceManager>>,
    stats: &Arc<Statistics>,
    format: &dyn RecordFormat,
    delete_operations: Vec<WriteEntry>,
    retries: &mut Vec<WriteEntry>,
    released_sectors: &mut u64,
) -> Result<()> {
    let capacity = delete_operations.len();
    let mut first_error = None;
    let mut marker_writes = Vec::with_capacity(capacity);
    let mut marker_extents = Vec::with_capacity(capacity);
    let mut release_operations = Vec::with_capacity(capacity);

    for entry in delete_operations {
        let sector = entry.record.sector.load(Ordering::Acquire);
        if sector == 0 {
            continue;
        }
        if entry.work_status.load(Ordering::Acquire) == DELETE_MARKER_DURABLE {
            release_operations.push(entry);
            continue;
        }
        if !entry.record.successor_is_durable_or_deleted() {
            retries.push(entry);
            continue;
        }

        entry.record.retire_extent();
        if entry.record.extent_has_readers() {
            retries.push(entry);
            continue;
        }
        let sectors_needed = format_extent_size(&entry, format);
        marker_extents.push((sector, sectors_needed));
        marker_writes.push(entry);
    }

    if !marker_writes.is_empty() {
        match disk_io.write().retire_extents(&marker_extents) {
            Ok(()) => {
                for entry in &marker_writes {
                    entry
                        .work_status
                        .store(DELETE_MARKER_DURABLE, Ordering::Release);
                }
                release_operations.append(&mut marker_writes);
            }
            Err(error) => {
                stats.record_sector_release_failure();
                eprintln!("feox: extent retirement failed: {error}");
                retries.append(&mut release_operations);
                retries.append(&mut marker_writes);
                return Err(error);
            }
        }
    }

    let mut releasable = Vec::with_capacity(release_operations.len());
    for entry in release_operations {
        if entry.record.extent_has_readers() {
            retries.push(entry);
            continue;
        }
        releasable.push(entry);
    }

    releasable.sort_unstable_by_key(|entry| entry.record.sector.load(Ordering::Acquire));
    let mut free_space_guard = free_space.write();
    let mut group = Vec::with_capacity(releasable.len());
    let mut group_end = 0;
    for entry in releasable {
        let sector = entry.record.sector.load(Ordering::Acquire);
        let sectors_needed = format_extent_size(&entry, format) as u64;
        if !group.is_empty() && sector != group_end {
            release_retirement_group(
                &mut group,
                &mut free_space_guard,
                stats,
                format,
                retries,
                &mut first_error,
                released_sectors,
            );
        }
        group_end = sector + sectors_needed;
        group.push(entry);
    }
    release_retirement_group(
        &mut group,
        &mut free_space_guard,
        stats,
        format,
        retries,
        &mut first_error,
        released_sectors,
    );

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn release_retirement_group(
    group: &mut Vec<WriteEntry>,
    free_space: &mut FreeSpaceManager,
    stats: &Statistics,
    format: &dyn RecordFormat,
    retries: &mut Vec<WriteEntry>,
    first_error: &mut Option<FeoxError>,
    released_sectors: &mut u64,
) {
    let Some(first) = group.first() else {
        return;
    };
    let sector = first.record.sector.load(Ordering::Acquire);
    let sectors_needed = group
        .iter()
        .map(|entry| format_extent_size(entry, format) as u64)
        .sum::<u64>();

    match free_space.release_sectors(sector, sectors_needed) {
        Ok(()) => {
            *released_sectors += sectors_needed;
            stats
                .disk_usage
                .fetch_sub(sectors_needed * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);
            group.clear();
        }
        Err(error) => {
            stats.record_sector_release_failure();
            eprintln!("feox: sector release failed for {sector}+{sectors_needed}: {error}");
            if first_error.is_none() {
                *first_error = Some(error);
            }
            retries.append(group);
        }
    }
}

fn format_extent_size(entry: &WriteEntry, format: &dyn RecordFormat) -> usize {
    format
        .total_size(entry.record.key.len(), entry.record.value_len)
        .div_ceil(FEOX_BLOCK_SIZE)
}

fn process_write_batch(
    ctx: &WorkerContext,
    entries: Vec<WriteEntry>,
    format: &dyn RecordFormat,
) -> BatchOutcome {
    let disk_io = &ctx.disk_io;
    let free_space = &ctx.free_space;
    let stats = &ctx.stats;
    let format_version = ctx.format_version;
    let fault_scope = ctx.fault_scope;
    let retirement_queue = &ctx.retirement_queue;
    let mut prepared_writes = Vec::new();
    let mut batch_writes = Vec::new();
    let mut delete_operations = Vec::new();
    let mut retry_entries = Vec::new();
    let mut first_error = None;

    for entry in entries {
        match entry.op {
            Operation::Insert | Operation::Update => {
                let sector = reserved_sector(&entry);
                if entry.record.sector.load(Ordering::Acquire) == 0
                    && (entry.record.refcount.load(Ordering::Acquire) > 0 || sector.is_some())
                {
                    match prepare_record_data(&entry.record, format, disk_io) {
                        Ok(data) => {
                            let sectors_needed = data.len().div_ceil(FEOX_BLOCK_SIZE);
                            prepared_writes.push(PreparedWrite {
                                data,
                                sectors_needed,
                                entry,
                                sector,
                            });
                        }
                        Err(error) => {
                            if first_error.is_none() {
                                first_error = Some(error);
                            }
                            retry_entries.push(entry);
                        }
                    }
                }
            }
            Operation::Delete => {
                delete_operations.push(entry);
            }
            _ => {}
        }
    }

    let stamp = format_version >= SEQ_TOKEN_MIN_VERSION;
    let has_deletions = !delete_operations.is_empty();
    if has_deletions {
        retirement_queue
            .pending
            .lock()
            .extend(delete_operations.drain(..));
    }

    if !prepared_writes.is_empty() {
        let mut free_space_guard = free_space.write();
        for index in 0..prepared_writes.len() {
            let sectors_needed = prepared_writes[index].sectors_needed;
            let sector = match prepared_writes[index].sector {
                Some(sector) => sector,
                None => match free_space_guard.allocate_sectors(sectors_needed as u64) {
                    Ok(sector) => {
                        reserve_sector(&prepared_writes[index].entry, sector);
                        stats.disk_usage.fetch_add(
                            (sectors_needed * FEOX_BLOCK_SIZE) as u64,
                            Ordering::Relaxed,
                        );
                        prepared_writes[index].sector = Some(sector);
                        sector
                    }
                    Err(error) => {
                        drop(free_space_guard);
                        let _ = release_allocations(free_space, &prepared_writes, stats);
                        retry_entries.extend(prepared_writes.drain(..).map(|write| write.entry));
                        retry_entries.extend(delete_operations);
                        return BatchOutcome {
                            result: Err(error),
                            retries: retry_entries,
                        };
                    }
                },
            };
            let write = &mut prepared_writes[index];
            if stamp {
                stamp_seq_token(&mut write.data, sector, format);
            }
            batch_writes.push((sector, Bytes::from(std::mem::take(&mut write.data))));
        }
    }

    if !batch_writes.is_empty() {
        let mut disk_guard = disk_io.write();
        for write in &prepared_writes {
            mark_reservation_dirty(&write.entry);
        }

        let journal_extents = prepared_writes
            .iter()
            .map(|write| {
                (
                    write.sector.expect("prepared write has an allocation"),
                    write.sectors_needed,
                )
            })
            .collect::<Vec<_>>();
        let journal_active = !journal_extents.is_empty();

        if journal_active {
            match disk_guard.write_allocation_journal(&journal_extents) {
                Ok(()) => crash_at("after_allocation_intent"),
                Err(error @ FeoxError::IndeterminateWrite(_)) => {
                    return failed_batch_outcome(
                        &mut disk_guard,
                        free_space,
                        &mut prepared_writes,
                        delete_operations,
                        retry_entries,
                        stats,
                        BatchFailure {
                            error,
                            clear_journal: true,
                            indeterminate: true,
                        },
                    );
                }
                Err(error) => {
                    return failed_batch_outcome(
                        &mut disk_guard,
                        free_space,
                        &mut prepared_writes,
                        delete_operations,
                        retry_entries,
                        stats,
                        BatchFailure {
                            error,
                            clear_journal: true,
                            indeterminate: false,
                        },
                    );
                }
            }
        }

        if has_deletions {
            crash_at("before_replacement_write");
        }

        let mut attempts = 3;
        let mut delay_us = 100;

        while attempts > 0 {
            let result = if fail_at(RECORD_WRITE, fault_scope) {
                Err(FeoxError::IoError(std::io::Error::other(
                    "injected record write failure",
                )))
            } else {
                disk_guard.batch_write_bytes(&batch_writes)
            };

            match result {
                Ok(()) => {
                    break;
                }
                Err(error @ FeoxError::IndeterminateWrite(_)) => {
                    return failed_batch_outcome(
                        &mut disk_guard,
                        free_space,
                        &mut prepared_writes,
                        delete_operations,
                        retry_entries,
                        stats,
                        BatchFailure {
                            error,
                            clear_journal: journal_active,
                            indeterminate: true,
                        },
                    );
                }
                Err(e) => {
                    attempts -= 1;
                    if attempts > 0 {
                        // Exponential backoff with jitter ±10%
                        let jitter = {
                            use rand::Rng;
                            let mut rng = rand::rng();
                            (delay_us * rng.random_range(-10..=10)) / 100
                        };
                        let actual_delay = (delay_us + jitter).max(1);
                        thread::sleep(Duration::from_micros(actual_delay as u64));
                        delay_us *= 2;
                    } else {
                        return failed_batch_outcome(
                            &mut disk_guard,
                            free_space,
                            &mut prepared_writes,
                            delete_operations,
                            retry_entries,
                            stats,
                            BatchFailure {
                                error: e,
                                clear_journal: journal_active,
                                indeterminate: false,
                            },
                        );
                    }
                }
            }
        }

        if journal_active {
            crash_at("before_allocation_journal_clear");
            if let Err(error) = disk_guard.clear_allocation_journal() {
                let indeterminate = matches!(error, FeoxError::IndeterminateWrite(_));
                return failed_batch_outcome(
                    &mut disk_guard,
                    free_space,
                    &mut prepared_writes,
                    delete_operations,
                    retry_entries,
                    stats,
                    BatchFailure {
                        error,
                        clear_journal: true,
                        indeterminate,
                    },
                );
            }
        }

        if has_deletions {
            crash_at("after_replacement_write");
        }
        for write in &prepared_writes {
            write
                .entry
                .record
                .sector
                .store(write.sector.unwrap(), Ordering::Release);
            std::sync::atomic::fence(Ordering::Release);
            write.entry.record.clear_value();
        }
        stats.record_write_flushed(prepared_writes.len() as u64);
    }

    let result = match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    };

    BatchOutcome {
        result,
        retries: retry_entries,
    }
}

#[cfg(test)]
fn crash_at(point: &str) {
    if std::env::var("FEOX_TEST_CRASH_POINT").as_deref() == Ok(point) {
        std::process::exit(86);
    }
}

#[cfg(not(test))]
#[inline]
fn crash_at(_: &str) {}

fn failed_batch_outcome(
    disk_io: &mut DiskIO,
    free_space: &Arc<RwLock<FreeSpaceManager>>,
    prepared_writes: &mut Vec<PreparedWrite>,
    delete_operations: Vec<WriteEntry>,
    mut retry_entries: Vec<WriteEntry>,
    stats: &Statistics,
    failure: BatchFailure,
) -> BatchOutcome {
    stats.record_write_failed();

    let error = if failure.indeterminate {
        quarantine_allocations(prepared_writes);
        failure.error
    } else {
        match cleanup_failed_allocations(
            disk_io,
            free_space,
            prepared_writes,
            stats,
            failure.clear_journal,
        ) {
            Ok(()) => failure.error,
            Err(cleanup_error) => {
                quarantine_allocations(prepared_writes);
                disk_io.poison_writes(cleanup_error)
            }
        }
    };

    retry_entries.extend(prepared_writes.drain(..).map(|write| write.entry));
    retry_entries.extend(delete_operations);
    BatchOutcome {
        result: Err(error),
        retries: retry_entries,
    }
}

fn release_allocations(
    free_space: &Arc<RwLock<FreeSpaceManager>>,
    allocations: &[PreparedWrite],
    stats: &Statistics,
) -> Result<()> {
    let mut first_error = None;
    let mut free_space_guard = free_space.write();
    for allocation in allocations {
        let Some(sector) = allocation.sector else {
            continue;
        };
        if reservation_is_dirty(&allocation.entry) {
            continue;
        }
        match free_space_guard.release_sectors(sector, allocation.sectors_needed as u64) {
            Ok(()) => {
                stats.disk_usage.fetch_sub(
                    (allocation.sectors_needed * FEOX_BLOCK_SIZE) as u64,
                    Ordering::Relaxed,
                );
                clear_reserved_sector(&allocation.entry);
            }
            Err(error) => {
                stats.record_sector_release_failure();
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
    }
    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn quarantine_allocations(allocations: &[PreparedWrite]) {
    for allocation in allocations {
        if allocation.sector.is_some() {
            quarantine_reservation(&allocation.entry);
        }
    }
}

fn cleanup_failed_allocations(
    disk_io: &mut DiskIO,
    free_space: &Arc<RwLock<FreeSpaceManager>>,
    allocations: &[PreparedWrite],
    stats: &Statistics,
    clear_journal: bool,
) -> Result<()> {
    let extents = allocations
        .iter()
        .filter(|allocation| !reservation_is_quarantined(&allocation.entry))
        .filter_map(|allocation| {
            allocation
                .sector
                .map(|sector| (sector, allocation.sectors_needed))
        })
        .collect::<Vec<_>>();

    if extents.is_empty() {
        if clear_journal {
            disk_io.clear_allocation_journal()?;
        }
    } else if let Err(error) = disk_io.retire_extents(&extents) {
        stats.record_sector_release_failure();
        eprintln!("feox: failed-write scrub failed: {error}");
        return Err(error);
    }

    let mut free_space = free_space.write();
    release_scrubbed_allocations(&mut free_space, allocations, stats)
}

fn release_scrubbed_allocations(
    free_space: &mut FreeSpaceManager,
    allocations: &[PreparedWrite],
    stats: &Statistics,
) -> Result<()> {
    let mut ordered = allocations
        .iter()
        .filter(|allocation| !reservation_is_quarantined(&allocation.entry))
        .filter_map(|allocation| allocation.sector.map(|sector| (sector, allocation)))
        .collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|(sector, _)| *sector);

    let mut first_error = None;
    let mut group_start = 0;
    while group_start < ordered.len() {
        let sector = ordered[group_start].0;
        let mut group_end = group_start + 1;
        let mut end_sector = sector + ordered[group_start].1.sectors_needed as u64;
        while group_end < ordered.len() && ordered[group_end].0 == end_sector {
            end_sector += ordered[group_end].1.sectors_needed as u64;
            group_end += 1;
        }
        let sectors_needed = end_sector - sector;

        match free_space.release_sectors(sector, sectors_needed) {
            Ok(()) => {
                stats
                    .disk_usage
                    .fetch_sub(sectors_needed * FEOX_BLOCK_SIZE as u64, Ordering::Relaxed);
                for (_, allocation) in &ordered[group_start..group_end] {
                    mark_reservation_clean(&allocation.entry);
                    clear_reserved_sector(&allocation.entry);
                }
            }
            Err(error) => {
                stats.record_sector_release_failure();
                eprintln!(
                    "feox: failed-write sector release failed for {sector}+{sectors_needed}: {error}"
                );
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        group_start = group_end;
    }

    match first_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn prepare_record_data(
    record: &Record,
    format: &dyn RecordFormat,
    disk_io: &Arc<RwLock<DiskIO>>,
) -> Result<Vec<u8>> {
    let total_size = format.total_size(record.key.len(), record.value_len);
    let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);
    let padded_size = sectors_needed * FEOX_BLOCK_SIZE;

    match record.get_value() {
        Some(value) => Ok(serialize_record_data(record, format, &value, padded_size)),
        None => prepare_deferred_record_data(record, format, disk_io, padded_size),
    }
}

fn serialize_record_data(
    record: &Record,
    format: &dyn RecordFormat,
    value: &[u8],
    padded_size: usize,
) -> Vec<u8> {
    let mut data = Vec::with_capacity(padded_size);
    data.extend_from_slice(&SECTOR_MARKER.to_le_bytes());
    data.extend_from_slice(&0u16.to_le_bytes());
    format.serialize_record_into(record, false, &mut data);
    data.extend_from_slice(value);
    data.resize(padded_size, 0);

    data
}

fn prepare_deferred_record_data(
    record: &Record,
    format: &dyn RecordFormat,
    disk_io: &Arc<RwLock<DiskIO>>,
    padded_size: usize,
) -> Result<Vec<u8>> {
    let mut source = record.value_source().ok_or(FeoxError::StaleExtent)?;
    loop {
        if let Some(value) = source.get_value() {
            return Ok(serialize_record_data(record, format, &value, padded_size));
        }
        if source.sector.load(Ordering::Acquire) != 0 {
            break;
        }
        source = source.value_source().ok_or(FeoxError::StaleExtent)?;
    }

    let extent = source.acquire_extent().ok_or(FeoxError::StaleExtent)?;
    let sector = source.sector.load(Ordering::Acquire);
    if sector == 0 {
        return Err(FeoxError::StaleExtent);
    }
    if source.value_len != record.value_len || source.key != record.key {
        return Err(FeoxError::InvalidRecord);
    }
    let total_size = format.total_size(source.key.len(), source.value_len);
    let sectors = total_size.div_ceil(FEOX_BLOCK_SIZE);
    let mut data = disk_io.read().read_sectors_sync(sector, sectors as u64)?;
    drop(extent);
    if !sector_holds_record(&data, &source) {
        return Err(FeoxError::StaleExtent);
    }

    let value_offset = format.value_offset(record.key.len());
    if data.len() != padded_size || value_offset > data.len() {
        return Err(FeoxError::InvalidRecord);
    }
    let mut header = Vec::with_capacity(value_offset);
    header.extend_from_slice(&SECTOR_MARKER.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes());
    format.serialize_record_into(record, false, &mut header);
    if header.len() != value_offset {
        return Err(FeoxError::InvalidRecord);
    }
    data[..value_offset].copy_from_slice(&header);
    Ok(data)
}

impl Drop for WriteBuffer {
    fn drop(&mut self) {
        self.complete_shutdown();
    }
}

#[cfg(test)]
#[path = "../tests/write_buffer_safety_tests.rs"]
mod tests;
