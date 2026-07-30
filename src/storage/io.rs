use bytes::Bytes;
#[cfg(target_os = "linux")]
use io_uring::{opcode, types, IoUring, Probe};
#[cfg(any(target_os = "linux", test))]
use std::collections::HashMap;
use std::fs::File;
#[cfg(any(unix, target_os = "windows", test))]
use std::io;
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
#[cfg(any(target_os = "linux", test))]
use std::sync::{Mutex, OnceLock};

use crate::constants::*;
use crate::error::{FeoxError, Result};
use crate::storage::allocation_journal::{
    decode as decode_allocation_journal, encode_active as encode_active_allocation_journal,
    encode_clear as encode_clear_allocation_journal, ALLOCATION_JOURNAL_BLOCKS,
    ALLOCATION_JOURNAL_MAX_ENTRIES, ALLOCATION_JOURNAL_SLOTS, ALLOCATION_JOURNAL_SLOT_BLOCKS,
    ALLOCATION_JOURNAL_START_BLOCK,
};
use crate::storage::format::fill_retirement_markers;
use crate::storage::metadata::Metadata;
#[cfg(unix)]
use crate::utils::allocator::AlignedBuffer;

#[cfg(target_os = "linux")]
enum PendingWriteBuffer {
    Aligned(AlignedBuffer),
    Shared(Bytes),
}

#[cfg(target_os = "linux")]
impl PendingWriteBuffer {
    fn as_ptr(&self) -> *const u8 {
        match self {
            Self::Aligned(buffer) => buffer.as_ptr(),
            Self::Shared(buffer) => buffer.as_ptr(),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Aligned(buffer) => buffer.len(),
            Self::Shared(buffer) => buffer.len(),
        }
    }
}

trait BatchWriteData {
    fn as_slice(&self) -> &[u8];

    #[cfg(target_os = "linux")]
    fn retain_for_write(&self) -> Bytes;
}

impl BatchWriteData for Vec<u8> {
    fn as_slice(&self) -> &[u8] {
        self
    }

    #[cfg(target_os = "linux")]
    fn retain_for_write(&self) -> Bytes {
        Bytes::copy_from_slice(self)
    }
}

impl BatchWriteData for Bytes {
    fn as_slice(&self) -> &[u8] {
        self
    }

    #[cfg(target_os = "linux")]
    fn retain_for_write(&self) -> Bytes {
        self.clone()
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

// Keep poisoned inodes alive so their identities cannot be reused before restart.
#[cfg(any(target_os = "linux", test))]
static INDETERMINATE_FILES: OnceLock<Mutex<HashMap<FileIdentity, Arc<File>>>> = OnceLock::new();

#[cfg(any(target_os = "linux", test))]
fn indeterminate_files() -> &'static Mutex<HashMap<FileIdentity, Arc<File>>> {
    INDETERMINATE_FILES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(any(target_os = "linux", test))]
fn file_is_indeterminate(identity: FileIdentity) -> bool {
    indeterminate_files()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains_key(&identity)
}

#[cfg(any(target_os = "linux", test))]
fn mark_file_indeterminate(identity: FileIdentity, file: &Arc<File>) {
    indeterminate_files()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .entry(identity)
        .or_insert_with(|| Arc::clone(file));
}

#[cfg(target_os = "linux")]
fn file_identity(file: &File) -> Result<FileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(FeoxError::IoError)?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(any(target_os = "linux", test))]
struct InFlightBuffers<T> {
    buffers: Vec<Option<T>>,
    in_flight: u128,
}

#[cfg(any(target_os = "linux", test))]
impl<T> InFlightBuffers<T> {
    fn with_capacity(capacity: usize) -> Self {
        assert!(capacity <= u128::BITS as usize);
        Self {
            buffers: Vec::with_capacity(capacity),
            in_flight: 0,
        }
    }

    fn push(&mut self, buffer: T) {
        assert!(self.buffers.len() < u128::BITS as usize);
        self.buffers.push(Some(buffer));
    }

    #[cfg(test)]
    fn new(buffers: Vec<T>) -> Self {
        let mut in_flight = Self::with_capacity(buffers.len());
        for buffer in buffers {
            in_flight.push(buffer);
        }
        in_flight
    }

    fn get(&self, index: usize) -> &T {
        self.buffers[index]
            .as_ref()
            .expect("in-flight buffer is owned")
    }

    fn mark_in_flight(&mut self, index: usize) {
        self.in_flight |= 1 << index;
    }

    fn mark_unqueued(&mut self, index: usize) {
        self.in_flight &= !(1 << index);
    }

    fn mark_complete(&mut self, index: usize) -> bool {
        let mask = 1 << index;
        let was_in_flight = self.in_flight & mask != 0;
        self.in_flight &= !mask;
        was_in_flight
    }
}

#[cfg(any(target_os = "linux", test))]
impl<T> Drop for InFlightBuffers<T> {
    fn drop(&mut self) {
        for (index, buffer) in self.buffers.iter_mut().enumerate() {
            if self.in_flight & (1 << index) != 0 {
                // A failed io_uring_enter does not prove that the kernel released this pointer.
                std::mem::forget(buffer.take());
            }
        }
    }
}

#[cfg(any(target_os = "linux", test))]
fn validate_write_completion(result: i32, expected: usize) -> io::Result<()> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }
    if result as usize != expected {
        return Err(io::Error::new(
            io::ErrorKind::WriteZero,
            format!("Wrote {result} bytes, expected {expected}"),
        ));
    }
    Ok(())
}

fn coalesce_extents(extents: &[(u64, usize)]) -> Result<Vec<(u64, usize)>> {
    let mut ordered = extents.to_vec();
    ordered.sort_unstable_by_key(|extent| extent.0);

    let mut coalesced: Vec<(u64, usize)> = Vec::with_capacity(ordered.len());
    for (sector, sectors) in ordered {
        if sectors == 0 {
            return Err(FeoxError::InvalidArgument);
        }
        let end = sector
            .checked_add(sectors as u64)
            .ok_or(FeoxError::InvalidArgument)?;

        let Some(previous) = coalesced.last_mut() else {
            coalesced.push((sector, sectors));
            continue;
        };
        let previous_end = previous
            .0
            .checked_add(previous.1 as u64)
            .ok_or(FeoxError::InvalidArgument)?;
        if sector < previous_end {
            return Err(FeoxError::InvalidArgument);
        }
        if sector == previous_end {
            previous.1 =
                usize::try_from(end - previous.0).map_err(|_| FeoxError::InvalidArgument)?;
        } else {
            coalesced.push((sector, sectors));
        }
    }
    Ok(coalesced)
}

const RETIREMENT_WRITE_BLOCKS: usize = 256;

pub struct DiskIO {
    #[cfg(target_os = "linux")]
    ring: Option<IoUring>,
    #[cfg(target_os = "linux")]
    next_user_data: u64,
    write_indeterminate: AtomicBool,
    journal_generation: AtomicU64,
    journal_slot: AtomicUsize,
    #[cfg(target_os = "linux")]
    file_identity: FileIdentity,
    _file: Arc<File>,
    #[cfg(unix)]
    fd: RawFd,
    _use_direct_io: bool,
}

impl DiskIO {
    #[cfg(unix)]
    pub fn new(file: Arc<File>, use_direct_io: bool) -> Result<Self> {
        use std::os::unix::io::AsRawFd;
        let fd = file.as_raw_fd();
        #[cfg(target_os = "linux")]
        {
            let file_identity = file_identity(file.as_ref())?;
            if file_is_indeterminate(file_identity) {
                return Err(FeoxError::IndeterminateWrite(io::Error::other(
                    "io_uring write outcome for this file is indeterminate until process restart",
                )));
            }

            // Create io_uring instance
            let ring: Option<IoUring> = IoUring::builder()
                .setup_sqpoll(IOURING_SQPOLL_IDLE_MS)
                .build(IOURING_QUEUE_SIZE)
                .ok();

            if let Some(ref r) = ring {
                let mut probe = Probe::new();
                if r.submitter().register_probe(&mut probe).is_ok()
                    && probe.is_supported(opcode::Read::CODE)
                    && probe.is_supported(opcode::Write::CODE)
                {
                    return Ok(Self {
                        ring,
                        next_user_data: 0,
                        write_indeterminate: AtomicBool::new(false),
                        journal_generation: AtomicU64::new(0),
                        journal_slot: AtomicUsize::new(ALLOCATION_JOURNAL_SLOTS - 1),
                        file_identity,
                        _file: file.clone(),
                        fd,
                        _use_direct_io: use_direct_io,
                    });
                }
            }

            Ok(Self {
                ring: None,
                next_user_data: 0,
                write_indeterminate: AtomicBool::new(false),
                journal_generation: AtomicU64::new(0),
                journal_slot: AtomicUsize::new(ALLOCATION_JOURNAL_SLOTS - 1),
                file_identity,
                _file: file,
                fd,
                _use_direct_io: use_direct_io,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = use_direct_io; // Suppress unused warning
            Ok(Self {
                write_indeterminate: AtomicBool::new(false),
                journal_generation: AtomicU64::new(0),
                journal_slot: AtomicUsize::new(ALLOCATION_JOURNAL_SLOTS - 1),
                _file: file,
                fd,
                _use_direct_io: false, // O_DIRECT not supported on this platform
            })
        }
    }

    #[cfg(not(unix))]
    pub fn new_from_file(file: File) -> Result<Self> {
        Ok(Self {
            write_indeterminate: AtomicBool::new(false),
            journal_generation: AtomicU64::new(0),
            journal_slot: AtomicUsize::new(ALLOCATION_JOURNAL_SLOTS - 1),
            _file: Arc::new(file),
            _use_direct_io: false,
        })
    }

    pub fn read_sectors_sync(&self, sector: u64, count: u64) -> Result<Vec<u8>> {
        let size = (count * FEOX_BLOCK_SIZE as u64) as usize;
        let offset = sector * FEOX_BLOCK_SIZE as u64;

        #[cfg(unix)]
        {
            // Only use aligned buffer for O_DIRECT
            if self._use_direct_io {
                let mut buffer = AlignedBuffer::new(size)?;
                buffer.set_len(size);

                let read = unsafe {
                    libc::pread(
                        self.fd,
                        buffer.as_mut_ptr() as *mut libc::c_void,
                        size,
                        offset as libc::off_t,
                    )
                };

                if read < 0 {
                    let err = io::Error::last_os_error();
                    return Err(FeoxError::IoError(err));
                }

                if read as usize != size {
                    return Err(FeoxError::IoError(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("Read {} bytes, expected {}", read, size),
                    )));
                }

                // Return the buffer's data directly (avoids extra copy)
                Ok(buffer.as_slice().to_vec())
            } else {
                // Non-O_DIRECT path: use regular Vec
                let mut buffer = vec![0u8; size];

                let read = unsafe {
                    libc::pread(
                        self.fd,
                        buffer.as_mut_ptr() as *mut libc::c_void,
                        size,
                        offset as libc::off_t,
                    )
                };

                if read < 0 {
                    let err = io::Error::last_os_error();
                    return Err(FeoxError::IoError(err));
                }

                if read as usize != size {
                    return Err(FeoxError::IoError(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("Read {} bytes, expected {}", read, size),
                    )));
                }

                buffer.truncate(read as usize);
                Ok(buffer)
            }
        }

        #[cfg(not(unix))]
        {
            // For non-Unix, no O_DIRECT, use regular Vec
            let mut buffer = vec![0u8; size];

            // For non-Unix, we need platform-specific implementations
            #[cfg(target_os = "windows")]
            {
                use std::os::windows::fs::FileExt;
                let read = self
                    ._file
                    .seek_read(&mut buffer, offset)
                    .map_err(FeoxError::IoError)?;
                if read != size {
                    return Err(FeoxError::IoError(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("Read {read} bytes, expected {size}"),
                    )));
                }
            }

            #[cfg(not(any(unix, target_os = "windows")))]
            {
                // Fallback for other platforms using standard file operations
                use std::io::{Read, Seek, SeekFrom};

                // Clone the Arc<File> to get a mutable handle for seeking
                let mut file = self
                    ._file
                    .as_ref()
                    .try_clone()
                    .map_err(FeoxError::IoError)?;

                file.seek(SeekFrom::Start(offset))
                    .map_err(FeoxError::IoError)?;

                file.read_exact(&mut buffer).map_err(FeoxError::IoError)?;
            }

            Ok(buffer)
        }
    }

    pub fn write_sectors_sync(&self, sector: u64, data: &[u8]) -> Result<()> {
        self.ensure_writable()?;
        let offset = sector * FEOX_BLOCK_SIZE as u64;

        #[cfg(unix)]
        {
            let written = if self._use_direct_io {
                // O_DIRECT path: need aligned buffer
                let mut aligned_buffer = AlignedBuffer::new(data.len())?;
                aligned_buffer.set_len(data.len());
                aligned_buffer.as_mut_slice().copy_from_slice(data);

                unsafe {
                    libc::pwrite(
                        self.fd,
                        aligned_buffer.as_ptr() as *const libc::c_void,
                        aligned_buffer.len(),
                        offset as libc::off_t,
                    )
                }
            } else {
                // Non-O_DIRECT path: write directly from input buffer
                unsafe {
                    libc::pwrite(
                        self.fd,
                        data.as_ptr() as *const libc::c_void,
                        data.len(),
                        offset as libc::off_t,
                    )
                }
            };

            if written < 0 {
                return Err(FeoxError::IoError(io::Error::last_os_error()));
            }

            if written as usize != data.len() {
                return Err(FeoxError::IoError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Partial write",
                )));
            }
        }

        #[cfg(not(unix))]
        {
            #[cfg(target_os = "windows")]
            {
                use std::os::windows::fs::FileExt;
                let written = self
                    ._file
                    .seek_write(data, offset)
                    .map_err(FeoxError::IoError)?;
                if written != data.len() {
                    return Err(FeoxError::IoError(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!("Wrote {written} bytes, expected {}", data.len()),
                    )));
                }
            }

            #[cfg(not(any(unix, target_os = "windows")))]
            {
                // Fallback for other platforms using standard file operations
                use std::io::{Seek, SeekFrom, Write};

                // Clone the Arc<File> to get a mutable handle for seeking
                let mut file = self
                    ._file
                    .as_ref()
                    .try_clone()
                    .map_err(FeoxError::IoError)?;

                file.seek(SeekFrom::Start(offset))
                    .map_err(FeoxError::IoError)?;

                file.write_all(data).map_err(FeoxError::IoError)?;

                // Ensure data is written to disk
                file.sync_data().map_err(FeoxError::IoError)?;
            }
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.ensure_writable()?;
        #[cfg(unix)]
        unsafe {
            if libc::fsync(self.fd) == -1 {
                return Err(FeoxError::IoError(io::Error::last_os_error()));
            }
        }

        #[cfg(not(unix))]
        {
            self._file.sync_all().map_err(FeoxError::IoError)?;
        }

        Ok(())
    }

    pub(crate) fn read_allocation_journal(&self, total_sectors: u64) -> Result<Vec<(u64, usize)>> {
        let data =
            self.read_sectors_sync(ALLOCATION_JOURNAL_START_BLOCK, ALLOCATION_JOURNAL_BLOCKS)?;
        let state = decode_allocation_journal(&data, total_sectors)?;
        self.journal_generation
            .store(state.generation, Ordering::Release);
        self.journal_slot.store(state.slot, Ordering::Release);
        Ok(state.extents)
    }

    pub(crate) fn write_allocation_journal(&self, extents: &[(u64, usize)]) -> Result<()> {
        let (generation, slot) = self.next_journal_position()?;
        let journal = encode_active_allocation_journal(generation, extents)?;
        self.write_sectors_sync(self.journal_sector(slot), &journal)?;
        self.flush()?;
        self.journal_generation.store(generation, Ordering::Release);
        self.journal_slot.store(slot, Ordering::Release);
        Ok(())
    }

    pub(crate) fn clear_allocation_journal(&self) -> Result<()> {
        let (generation, slot) = self.next_journal_position()?;
        let journal = encode_clear_allocation_journal(generation)?;
        self.write_sectors_sync(self.journal_sector(slot), &journal)?;
        self.flush()?;
        self.journal_generation.store(generation, Ordering::Release);
        self.journal_slot.store(slot, Ordering::Release);
        Ok(())
    }

    pub(crate) fn poison_writes(&self, error: FeoxError) -> FeoxError {
        self.write_indeterminate.store(true, Ordering::Release);
        #[cfg(target_os = "linux")]
        mark_file_indeterminate(self.file_identity, &self._file);
        FeoxError::IndeterminateWrite(std::io::Error::other(error.to_string()))
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.write_indeterminate.load(Ordering::Acquire) {
            return Err(FeoxError::IndeterminateWrite(std::io::Error::other(
                "write outcome is indeterminate until restart",
            )));
        }
        Ok(())
    }

    fn next_journal_position(&self) -> Result<(u64, usize)> {
        let generation = self
            .journal_generation
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or(FeoxError::InvalidMetadata)?;
        let slot = (self.journal_slot.load(Ordering::Acquire) + 1) % ALLOCATION_JOURNAL_SLOTS;
        Ok((generation, slot))
    }

    fn journal_sector(&self, slot: usize) -> u64 {
        ALLOCATION_JOURNAL_START_BLOCK + slot as u64 * ALLOCATION_JOURNAL_SLOT_BLOCKS
    }

    pub(crate) fn retire_extents(&self, extents: &[(u64, usize)]) -> Result<()> {
        if extents.is_empty() {
            return Ok(());
        }

        let coalesced = coalesce_extents(extents)?;
        for chunk in coalesced.chunks(ALLOCATION_JOURNAL_MAX_ENTRIES) {
            if let Err(error) = self.write_allocation_journal(chunk) {
                return Err(self.poison_writes(error));
            }
            if let Err(error) = self.retire_extents_unjournaled(chunk) {
                return Err(self.poison_writes(error));
            }
            if let Err(error) = self.clear_allocation_journal() {
                return Err(self.poison_writes(error));
            }
        }

        Ok(())
    }

    pub(crate) fn replay_allocation_journal(&self, extents: &[(u64, usize)]) -> Result<()> {
        if extents.is_empty() {
            return Ok(());
        }

        let coalesced = coalesce_extents(extents)?;
        self.retire_extents_unjournaled(&coalesced)?;
        self.clear_allocation_journal()
    }

    fn retire_extents_unjournaled(&self, extents: &[(u64, usize)]) -> Result<()> {
        if extents.iter().any(|(_, sectors)| *sectors == 0) {
            return Err(FeoxError::InvalidArgument);
        }

        let scratch_blocks = extents
            .iter()
            .map(|(_, sectors)| (*sectors).min(RETIREMENT_WRITE_BLOCKS))
            .max()
            .ok_or(FeoxError::InvalidArgument)?;
        let scratch_size = scratch_blocks
            .checked_mul(FEOX_BLOCK_SIZE)
            .ok_or(FeoxError::InvalidArgument)?;

        #[cfg(unix)]
        if self._use_direct_io {
            self.ensure_writable()?;
            let mut scratch = AlignedBuffer::new(scratch_size)?;
            scratch.set_len(scratch_size);
            scratch.as_mut_slice().fill(0);
            for &(sector, sectors) in extents {
                self.write_retirement_extent_direct(sector, sectors, &mut scratch)?;
            }
            return self.flush();
        }

        let mut scratch = vec![0; scratch_size];
        for &(sector, sectors) in extents {
            self.write_retirement_extent_buffered(sector, sectors, &mut scratch)?;
        }
        self.flush()
    }

    fn write_retirement_extent_buffered(
        &self,
        sector: u64,
        sectors: usize,
        scratch: &mut [u8],
    ) -> Result<()> {
        let mut offset = 0;
        while offset < sectors {
            let blocks = (sectors - offset).min(RETIREMENT_WRITE_BLOCKS);
            let size = blocks
                .checked_mul(FEOX_BLOCK_SIZE)
                .ok_or(FeoxError::InvalidArgument)?;
            let block_sector = sector + offset as u64;
            let remaining = sectors - offset;
            let retired = &mut scratch[..size];
            fill_retirement_markers(retired, block_sector, remaining);
            self.write_sectors_sync(block_sector, retired)?;

            offset += blocks;
        }

        Ok(())
    }

    #[cfg(unix)]
    fn write_retirement_extent_direct(
        &self,
        sector: u64,
        sectors: usize,
        scratch: &mut AlignedBuffer,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < sectors {
            let blocks = (sectors - offset).min(RETIREMENT_WRITE_BLOCKS);
            let size = blocks
                .checked_mul(FEOX_BLOCK_SIZE)
                .ok_or(FeoxError::InvalidArgument)?;
            let block_sector = sector + offset as u64;
            let remaining = sectors - offset;
            scratch.set_len(size);
            fill_retirement_markers(scratch.as_mut_slice(), block_sector, remaining);

            let written = unsafe {
                libc::pwrite(
                    self.fd,
                    scratch.as_ptr() as *const libc::c_void,
                    scratch.len(),
                    (block_sector * FEOX_BLOCK_SIZE as u64) as libc::off_t,
                )
            };
            if written < 0 {
                return Err(FeoxError::IoError(io::Error::last_os_error()));
            }
            if written as usize != size {
                return Err(FeoxError::IoError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Partial write",
                )));
            }

            offset += blocks;
        }

        Ok(())
    }

    /// Shutdown io_uring to stop SQPOLL kernel thread
    pub fn shutdown(&mut self) {
        #[cfg(target_os = "linux")]
        {
            if let Some(ref mut ring) = self.ring {
                // First, wait for any pending submissions to complete
                // This ensures all in-flight I/O operations finish
                if ring.submit_and_wait(0).is_ok() {
                    // Now drain all completions to acknowledge them
                    while ring.completion().next().is_some() {
                        // Consume all completion events
                    }
                }
            }
            self.ring = None;
        }
    }

    /// Batch write with io_uring for better throughput
    /// Operations complete synchronously before returning
    #[cfg(target_os = "linux")]
    pub fn batch_write(&mut self, writes: Vec<(u64, Vec<u8>)>) -> Result<()> {
        self.batch_write_inner(&writes)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn batch_write_bytes(&mut self, writes: &[(u64, Bytes)]) -> Result<()> {
        self.batch_write_inner(writes)
    }

    #[cfg(target_os = "linux")]
    fn batch_write_inner<T: BatchWriteData>(&mut self, writes: &[(u64, T)]) -> Result<()> {
        self.ensure_writable()?;

        if self.ring.is_none() {
            for (sector, data) in writes {
                self.write_sectors_sync(*sector, data.as_slice())?;
            }
            self.flush()?;
            return Ok(());
        }

        for chunk in writes.chunks(IOURING_MAX_BATCH) {
            let mut buffers = InFlightBuffers::with_capacity(chunk.len());
            for (_sector, data) in chunk {
                if self._use_direct_io {
                    let data = data.as_slice();
                    let mut aligned = AlignedBuffer::new(data.len())?;
                    aligned.set_len(data.len());
                    aligned.as_mut_slice().copy_from_slice(data);
                    buffers.push(PendingWriteBuffer::Aligned(aligned));
                } else {
                    buffers.push(PendingWriteBuffer::Shared(data.retain_for_write()));
                }
            }

            let user_data_base = self.next_user_data;
            self.next_user_data = self.next_user_data.wrapping_add(chunk.len() as u64);

            let queued = {
                let ring = self.ring.as_mut().expect("io_uring checked above");
                let mut sq = ring.submission();
                let mut queued = 0;

                for (i, (sector, _)) in chunk.iter().enumerate() {
                    let offset = sector * FEOX_BLOCK_SIZE as u64;
                    let buffer = buffers.get(i);
                    let write_e = opcode::Write::new(
                        types::Fd(self.fd),
                        buffer.as_ptr(),
                        buffer.len() as u32,
                    )
                    .offset(offset)
                    .build()
                    .user_data(user_data_base.wrapping_add(i as u64));

                    buffers.mark_in_flight(i);
                    if unsafe { sq.push(&write_e) }.is_err() {
                        buffers.mark_unqueued(i);
                        break;
                    }
                    queued += 1;
                }

                queued
            };

            let mut first_error =
                (queued != chunk.len()).then(|| FeoxError::IoError(io::Error::other("SQ full")));
            let mut completed_count = 0;

            while completed_count < queued {
                let wait_result = self
                    .ring
                    .as_mut()
                    .expect("io_uring checked above")
                    .submit_and_wait(queued - completed_count);

                if let Err(error) = wait_result {
                    if error.kind() == io::ErrorKind::Interrupted {
                        continue;
                    }

                    let submit_error = FeoxError::IndeterminateWrite(error);
                    self.write_indeterminate.store(true, Ordering::Release);
                    mark_file_indeterminate(self.file_identity, &self._file);
                    let ring = self.ring.as_mut().expect("io_uring checked above");
                    process_completions(
                        ring,
                        user_data_base,
                        queued,
                        &mut buffers,
                        &mut completed_count,
                        &mut first_error,
                    );
                    drop(self.ring.take());
                    return Err(submit_error);
                }

                let ring = self.ring.as_mut().expect("io_uring checked above");
                process_completions(
                    ring,
                    user_data_base,
                    queued,
                    &mut buffers,
                    &mut completed_count,
                    &mut first_error,
                );
            }

            if let Some(error) = first_error {
                return Err(error);
            }
        }

        self.flush()
    }

    pub fn read_metadata(&self) -> Result<Vec<u8>> {
        let blocks = self.read_sectors_sync(FEOX_METADATA_BLOCK, FEOX_METADATA_BACKUP_BLOCK + 1)?;
        let primary = &blocks[..FEOX_BLOCK_SIZE];
        let backup_start = FEOX_METADATA_BACKUP_BLOCK as usize * FEOX_BLOCK_SIZE;
        let backup = &blocks[backup_start..backup_start + FEOX_BLOCK_SIZE];

        match (Metadata::from_bytes(primary), Metadata::from_bytes(backup)) {
            (Some(primary_metadata), Some(backup_metadata))
                if backup_metadata.generation() > primary_metadata.generation() =>
            {
                Ok(backup.to_vec())
            }
            (Some(_), _) => Ok(primary.to_vec()),
            (None, Some(_)) => Ok(backup.to_vec()),
            (None, None) => Ok(primary.to_vec()),
        }
    }

    pub fn write_metadata(&self, metadata: &[u8]) -> Result<()> {
        let block = metadata_block(metadata)?;
        self.write_sectors_sync(FEOX_METADATA_BLOCK, &block)?;
        self.flush()
    }

    pub(crate) fn write_store_metadata(&self, metadata: &mut Metadata) -> Result<()> {
        let mut next = *metadata;
        next.advance_generation()?;
        let encoded = next.encode();
        let block = metadata_block(&encoded)?;
        let sector = if next.generation() & 1 == 0 {
            FEOX_METADATA_BLOCK
        } else {
            FEOX_METADATA_BACKUP_BLOCK
        };
        self.write_sectors_sync(sector, &block)?;
        self.flush()?;
        *metadata = next;
        Ok(())
    }

    pub(crate) fn initialize_store_metadata(&self, metadata: &mut Metadata) -> Result<()> {
        let mut next = *metadata;
        next.advance_generation()?;
        let encoded = next.encode();
        let block = metadata_block(&encoded)?;
        self.write_sectors_sync(FEOX_METADATA_BLOCK, &block)?;
        self.write_sectors_sync(FEOX_METADATA_BACKUP_BLOCK, &block)?;
        *metadata = next;
        Ok(())
    }

    /// Non-Linux fallback implementation
    #[cfg(not(target_os = "linux"))]
    pub fn batch_write(&mut self, writes: Vec<(u64, Vec<u8>)>) -> Result<()> {
        self.batch_write_inner(&writes)
    }

    #[cfg(not(target_os = "linux"))]
    pub(crate) fn batch_write_bytes(&mut self, writes: &[(u64, Bytes)]) -> Result<()> {
        self.batch_write_inner(writes)
    }

    #[cfg(not(target_os = "linux"))]
    fn batch_write_inner<T: BatchWriteData>(&mut self, writes: &[(u64, T)]) -> Result<()> {
        self.ensure_writable()?;
        for (sector, data) in writes {
            self.write_sectors_sync(*sector, data.as_slice())?;
        }
        self.flush()?;
        Ok(())
    }
}

fn metadata_block(metadata: &[u8]) -> Result<Vec<u8>> {
    if metadata.len() > FEOX_BLOCK_SIZE {
        return Err(FeoxError::InvalidValueSize);
    }

    let mut block = vec![0; FEOX_BLOCK_SIZE];
    block[..metadata.len()].copy_from_slice(metadata);
    Ok(block)
}

#[cfg(target_os = "linux")]
fn process_completions(
    ring: &mut IoUring,
    user_data_base: u64,
    queued: usize,
    buffers: &mut InFlightBuffers<PendingWriteBuffer>,
    completed_count: &mut usize,
    first_error: &mut Option<FeoxError>,
) {
    for cqe in ring.completion() {
        let index = cqe.user_data().wrapping_sub(user_data_base);
        if index >= queued as u64 {
            continue;
        }
        let index = index as usize;
        if !buffers.mark_complete(index) {
            continue;
        }

        *completed_count += 1;
        if first_error.is_none() {
            if let Err(error) = validate_write_completion(cqe.result(), buffers.get(index).len()) {
                *first_error = Some(FeoxError::IoError(error));
            }
        }
    }
}

#[cfg(test)]
#[path = "../tests/io_safety_tests.rs"]
mod tests;
