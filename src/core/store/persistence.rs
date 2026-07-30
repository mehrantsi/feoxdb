use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::storage::format::{get_format_ref, sector_holds_record};

use super::FeoxStore;

const ZERO_SCAN_BLOCKS: usize = 256;

fn validate_device_size(size: u64) -> Result<()> {
    let reserved_size = FEOX_DATA_START_BLOCK * FEOX_BLOCK_SIZE as u64;
    if size <= reserved_size
        || size > MAX_DEVICE_SIZE
        || !size.is_multiple_of(FEOX_BLOCK_SIZE as u64)
    {
        return Err(FeoxError::InvalidDevice);
    }
    Ok(())
}

fn file_is_all_zero(_file: &std::fs::File, path: &str, size: u64) -> Result<bool> {
    #[cfg(target_os = "linux")]
    if sparse_file_has_no_data(_file)? {
        return Ok(true);
    }

    let mut contents = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(FeoxError::IoError)?;
    let mut buffer = vec![0; ZERO_SCAN_BLOCKS * FEOX_BLOCK_SIZE];
    let mut remaining = size;
    while remaining > 0 {
        let read_len = remaining.min(buffer.len() as u64) as usize;
        contents
            .read_exact(&mut buffer[..read_len])
            .map_err(FeoxError::IoError)?;
        if buffer[..read_len].iter().any(|byte| *byte != 0) {
            return Ok(false);
        }
        remaining -= read_len as u64;
    }
    Ok(true)
}

#[cfg(target_os = "linux")]
fn sparse_file_has_no_data(file: &std::fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let offset = unsafe { libc::lseek(file.as_raw_fd(), 0, libc::SEEK_DATA) };
    if offset >= 0 {
        return Ok(false);
    }

    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ENXIO) => Ok(true),
        Some(libc::EINVAL) => Ok(false),
        _ => Err(FeoxError::IoError(error)),
    }
}

impl FeoxStore {
    /// Force flush all pending writes to disk.
    ///
    /// In persistent mode, ensures all buffered writes are flushed to disk.
    /// In memory-only mode, this is a no-op.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use feoxdb::FeoxStore;
    /// # fn main() -> feoxdb::Result<()> {
    /// let store = FeoxStore::new(Some("/path/to/data.feox".to_string()))?;
    /// store.insert(b"important", b"data")?;
    /// store.flush_all()?;  // Ensure data is persisted
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_all(&self) -> Result<()> {
        if self.initialized && !self.memory_only {
            // First flush the write buffer to ensure all data is written
            if let Some(ref wb) = self.write_buffer {
                wb.force_flush()?;
            }

            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                disk_io.write().write_store_metadata(&mut metadata)?;
            }
        }
        Ok(())
    }

    pub(super) fn load_value_from_disk(&self, record: &Arc<Record>) -> Result<Bytes> {
        let mut source = Arc::clone(record);
        loop {
            if let Some(value) = source.get_value() {
                return Ok(value);
            }
            if source.sector.load(Ordering::Acquire) != 0 {
                break;
            }
            source = source.value_source().ok_or(FeoxError::StaleExtent)?;
        }
        let extent = source.acquire_extent().ok_or(FeoxError::StaleExtent)?;
        let sector = source.sector.load(Ordering::Acquire);
        if self.memory_only || sector == 0 {
            return Err(FeoxError::StaleExtent);
        }
        crate::test_hooks::pause_at(crate::test_hooks::AFTER_SECTOR_LOAD);

        // Get the appropriate format handler
        let format = get_format_ref(self.format_version);

        // Calculate how many sectors we need to read
        let total_size = format.total_size(source.key.len(), source.value_len);
        let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);

        // Read the sectors
        let disk_io = self
            .disk_io
            .as_ref()
            .ok_or_else(|| {
                FeoxError::IoError(io::Error::new(
                    io::ErrorKind::NotFound,
                    "No disk IO available",
                ))
            })?
            .read();

        let data = disk_io.read_sectors_sync(sector, sectors_needed as u64)?;
        drop(extent);

        if !sector_holds_record(&data, &source) {
            return Err(FeoxError::StaleExtent);
        }

        let offset = format.value_offset(source.key.len());
        let end = offset
            .checked_add(source.value_len)
            .filter(|end| *end <= data.len())
            .ok_or(FeoxError::InvalidRecord)?;
        if source.value_len <= data.len() / 2 {
            Ok(Bytes::copy_from_slice(&data[offset..end]))
        } else {
            Ok(Bytes::from(data).slice(offset..end))
        }
    }

    pub(super) fn open_device(
        &mut self,
        device_path: &Option<String>,
        file_size: Option<u64>,
    ) -> Result<()> {
        if let Some(path) = device_path {
            // Open the device/file
            #[cfg(target_os = "linux")]
            use std::os::unix::fs::OpenOptionsExt;

            #[cfg(unix)]
            let (file, use_direct_io) = if std::path::Path::new("/.dockerenv").exists() {
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(path)
                    .map_err(FeoxError::IoError)?;
                (file, false) // Don't use O_DIRECT in Docker
            } else {
                // Try with O_DIRECT on Linux, fall back without it on other Unix systems
                #[cfg(target_os = "linux")]
                {
                    // Try to open with O_DIRECT first
                    match OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .custom_flags(libc::O_DIRECT)
                        .open(path)
                    {
                        Ok(file) => (file, true), // Successfully opened with O_DIRECT
                        Err(_) => {
                            // Fallback to regular open
                            let file = OpenOptions::new()
                                .read(true)
                                .write(true)
                                .create(true)
                                .truncate(false)
                                .open(path)
                                .map_err(FeoxError::IoError)?;
                            (file, false)
                        }
                    }
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(path)
                        .map_err(FeoxError::IoError)?;
                    (file, false) // O_DIRECT not supported on this platform
                }
            };

            #[cfg(not(unix))]
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map_err(FeoxError::IoError)?;

            // Get file size
            let metadata = file.metadata().map_err(FeoxError::IoError)?;
            self.device_size = metadata.len();

            // Track whether this is a newly created file
            self.fresh_device = self.device_size == 0;

            if self.fresh_device {
                self.initialize_fresh_device(&file, file_size)?;
            } else {
                validate_device_size(self.device_size)?;
                let is_empty_file = file_is_all_zero(&file, path, self.device_size)?;

                if is_empty_file {
                    self.free_space.write().initialize(self.device_size)?;
                    self.fresh_device = true;
                    let mut metadata = self._metadata.write();
                    metadata.device_size = self.device_size;
                    metadata.update();
                } else {
                    self.free_space.write().set_device_size(self.device_size);
                }
            }

            #[cfg(not(unix))]
            let use_direct_io = false;
            self.attach_device_file(file, use_direct_io)?;
        }
        Ok(())
    }

    pub(super) fn open_device_read_only(&mut self, file: File) -> Result<()> {
        self.device_size = file.metadata().map_err(FeoxError::IoError)?.len();
        validate_device_size(self.device_size)?;
        self.free_space.write().set_device_size(self.device_size);
        self.attach_device_file(file, false)
    }

    pub(super) fn open_fresh_device(&mut self, file: File, file_size: Option<u64>) -> Result<()> {
        if file.metadata().map_err(FeoxError::IoError)?.len() != 0 {
            return Err(FeoxError::InvalidDevice);
        }
        self.fresh_device = true;
        self.initialize_fresh_device(&file, file_size)?;
        self.attach_device_file(file, false)
    }

    fn initialize_fresh_device(&mut self, file: &File, file_size: Option<u64>) -> Result<()> {
        let target_size = file_size.unwrap_or(DEFAULT_DEVICE_SIZE);
        validate_device_size(target_size)?;
        file.set_len(target_size).map_err(FeoxError::IoError)?;
        self.device_size = target_size;
        self.free_space.write().initialize(self.device_size)?;

        let mut metadata = self._metadata.write();
        metadata.device_size = self.device_size;
        metadata.update();
        Ok(())
    }

    fn attach_device_file(&mut self, file: File, use_direct_io: bool) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let file = Arc::new(file);
            self.device_fd = Some(file.as_raw_fd());
            self.device_file = Some(file.as_ref().try_clone().map_err(FeoxError::IoError)?);
            let disk_io = crate::storage::io::DiskIO::new(file, use_direct_io)?;
            self.disk_io = Some(Arc::new(parking_lot::RwLock::new(disk_io)));
        }

        #[cfg(not(unix))]
        {
            let _ = use_direct_io;
            self.device_file = Some(file.try_clone().map_err(FeoxError::IoError)?);
            let disk_io = crate::storage::io::DiskIO::new_from_file(file)?;
            self.disk_io = Some(Arc::new(parking_lot::RwLock::new(disk_io)));
        }

        Ok(())
    }
}

impl Drop for FeoxStore {
    fn drop(&mut self) {
        // Stop TTL sweeper if running
        if let Some(mut sweeper) = self.ttl_sweeper.write().take() {
            sweeper.stop();
        }

        // Signal shutdown to write buffer workers
        if let Some(ref wb) = self.write_buffer {
            wb.initiate_shutdown();
        }

        if let Some(write_buffer) = self.write_buffer.take() {
            write_buffer.finish_shutdown();
        }

        // Write metadata directly without using the write buffer
        if self.initialized && !self.memory_only && !self.read_only {
            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                let _ = disk_io.write().write_store_metadata(&mut metadata);
            }
        }

        // Now it's safe to shutdown disk I/O since workers have exited
        if let Some(ref disk_io) = self.disk_io {
            disk_io.write().shutdown();
        }
    }
}
