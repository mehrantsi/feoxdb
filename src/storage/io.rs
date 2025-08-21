#[cfg(target_os = "linux")]
use io_uring::{opcode, types, IoUring, Probe};
use std::fs::File;
#[cfg(unix)]
use std::io;
#[cfg(unix)]
use std::os::unix::io::RawFd;
use std::sync::Arc;

use crate::constants::*;
use crate::error::{FeoxError, Result};
use crate::utils::allocator::AlignedBuffer;

pub struct DiskIO {
    #[cfg(target_os = "linux")]
    ring: Option<IoUring>,
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
                        _file: file.clone(),
                        fd,
                        _use_direct_io: use_direct_io,
                    });
                }
            }

            Ok(Self {
                ring,
                _file: file,
                fd,
                _use_direct_io: false, // io_uring not available, can't use O_DIRECT efficiently
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = use_direct_io; // Suppress unused warning
            Ok(Self {
                _file: file,
                fd,
                _use_direct_io: false, // O_DIRECT not supported on this platform
            })
        }
    }

    #[cfg(not(unix))]
    pub fn new_from_file(file: File) -> Result<Self> {
        Ok(Self {
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
                    eprintln!("pread failed at offset {}: {}", offset, err);
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
                    eprintln!("pread failed at offset {}: {}", offset, err);
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
                self._file
                    .seek_read(&mut buffer, offset)
                    .map_err(FeoxError::IoError)?;
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
                self._file
                    .seek_write(data, offset)
                    .map_err(FeoxError::IoError)?;
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
        if let Some(ref mut ring) = self.ring {
            // Process in chunks to avoid overwhelming the submission queue
            for chunk in writes.chunks(IOURING_MAX_BATCH) {
                let mut aligned_buffers = Vec::new();

                // Create aligned buffers for this chunk
                for (_sector, data) in chunk {
                    let mut aligned = AlignedBuffer::new(data.len())?;
                    aligned.set_len(data.len());
                    aligned.as_mut_slice().copy_from_slice(data);
                    aligned_buffers.push(aligned);
                }

                // Submit operations for this chunk
                unsafe {
                    let mut sq = ring.submission();

                    for (i, (sector, _)) in chunk.iter().enumerate() {
                        let offset = sector * FEOX_BLOCK_SIZE as u64;
                        let buffer = &aligned_buffers[i];

                        let write_e = opcode::Write::new(
                            types::Fd(self.fd),
                            buffer.as_ptr(),
                            buffer.len() as u32,
                        )
                        .offset(offset)
                        .build()
                        .user_data(i as u64);

                        sq.push(&write_e)
                            .map_err(|_| FeoxError::IoError(io::Error::other("SQ full")))?;
                    }
                }

                // Submit and wait for this chunk to complete
                let submitted = ring
                    .submit_and_wait(chunk.len())
                    .map_err(FeoxError::IoError)?;

                // Process completions for this chunk
                let mut completed = 0;
                for cqe in ring.completion() {
                    if cqe.result() < 0 {
                        return Err(FeoxError::IoError(io::Error::from_raw_os_error(
                            -cqe.result(),
                        )));
                    }
                    completed += 1;
                    if completed >= submitted {
                        break;
                    }
                }
            }

            // Sync to ensure durability
            self.flush()?;

            Ok(())
        } else {
            // Fallback: do sync writes
            for (sector, data) in writes {
                self.write_sectors_sync(sector, &data)?;
            }
            Ok(())
        }
    }

    pub fn read_metadata(&self) -> Result<Vec<u8>> {
        self.read_sectors_sync(FEOX_METADATA_BLOCK, 1)
    }

    pub fn write_metadata(&self, metadata: &[u8]) -> Result<()> {
        if metadata.len() > FEOX_BLOCK_SIZE {
            return Err(FeoxError::InvalidValueSize);
        }

        // Prepare a full block (metadata may be smaller)
        let mut block_data = vec![0u8; FEOX_BLOCK_SIZE];
        block_data[..metadata.len()].copy_from_slice(metadata);

        // write_sectors_sync will handle alignment if needed
        self.write_sectors_sync(FEOX_METADATA_BLOCK, &block_data)?;
        self.flush()
    }

    /// Non-Linux fallback implementation
    #[cfg(not(target_os = "linux"))]
    pub fn batch_write(&mut self, writes: Vec<(u64, Vec<u8>)>) -> Result<()> {
        // Fallback: do sync writes
        for (sector, data) in writes {
            self.write_sectors_sync(sector, &data)?;
        }
        Ok(())
    }
}
