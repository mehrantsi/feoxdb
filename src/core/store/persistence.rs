use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::storage::format::get_format;

use super::FeoxStore;

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
    /// store.flush_all();  // Ensure data is persisted
    /// # Ok(())
    /// # }
    /// ```
    pub fn flush_all(&self) {
        if !self.memory_only {
            // First flush the write buffer to ensure all data is written
            if let Some(ref wb) = self.write_buffer {
                let _ = wb.force_flush();
            }

            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                let _ = disk_io.write().write_metadata(metadata.as_bytes());
                let _ = disk_io.write().flush();
            }
        }
    }

    pub(super) fn load_value_from_disk(&self, record: &Record) -> Result<Vec<u8>> {
        let sector = record.sector.load(Ordering::Acquire);
        if self.memory_only || sector == 0 {
            return Err(FeoxError::InvalidRecord);
        }

        // Get the appropriate format handler
        let metadata_version = self._metadata.read().version;
        let format = get_format(metadata_version);

        // Calculate how many sectors we need to read
        let total_size = format.total_size(record.key.len(), record.value_len);
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

        // Use format to get the value offset
        let offset = format.value_offset(record.key.len());
        if offset + record.value_len > data.len() {
            return Err(FeoxError::InvalidRecord);
        }

        Ok(data[offset..offset + record.value_len].to_vec())
    }

    pub(super) fn open_device(&mut self, device_path: &Option<String>) -> Result<()> {
        if let Some(path) = device_path {
            // Open the device/file
            use std::fs::OpenOptions;
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
            let was_newly_created = self.device_size == 0;

            if was_newly_created {
                // New empty file - set default size and initialize free space
                file.set_len(DEFAULT_DEVICE_SIZE)
                    .map_err(FeoxError::IoError)?;
                self.device_size = DEFAULT_DEVICE_SIZE;

                // Initialize free space manager with all space free
                self.free_space.write().initialize(self.device_size)?;

                let mut metadata = self._metadata.write();
                metadata.device_size = self.device_size;
                metadata.update();
            } else {
                // Existing file - just set device size, free space will be rebuilt during scan
                self.free_space.write().set_device_size(self.device_size);
            }

            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let file_arc = Arc::new(file);
                let fd = file_arc.as_raw_fd();
                self.device_fd = Some(fd);
                // Store a clone of the file to keep it alive
                self.device_file = Some(file_arc.as_ref().try_clone().map_err(FeoxError::IoError)?);
                let disk_io = crate::storage::io::DiskIO::new(file_arc, use_direct_io)?;
                self.disk_io = Some(Arc::new(parking_lot::RwLock::new(disk_io)));
            }

            #[cfg(not(unix))]
            {
                // Store a clone of the file to keep it alive
                self.device_file = Some(file.try_clone().map_err(FeoxError::IoError)?);
                let disk_io = crate::storage::io::DiskIO::new_from_file(file)?;
                self.disk_io = Some(Arc::new(parking_lot::RwLock::new(disk_io)));
            }

            let disk_io = self.disk_io.as_ref().unwrap().read();

            // Read metadata from existing files (not newly created ones)
            if !was_newly_created {
                if let Ok(metadata_bytes) = disk_io.read_metadata() {
                    if let Some(loaded_metadata) =
                        crate::storage::metadata::Metadata::from_bytes(&metadata_bytes)
                    {
                        // Initialize stats from metadata
                        self.stats
                            .disk_usage
                            .store(loaded_metadata.total_size, Ordering::Relaxed);
                        *self._metadata.write() = loaded_metadata;
                    }
                }
            }
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

        // Write metadata directly without using the write buffer
        if !self.memory_only {
            if let Some(ref disk_io) = self.disk_io {
                // Update metadata with current stats
                let mut metadata = self._metadata.write();
                metadata.total_records = self.stats.record_count.load(Ordering::Relaxed) as u64;
                metadata.total_size = self.stats.disk_usage.load(Ordering::Relaxed);
                metadata.fragmentation = self.free_space.read().get_fragmentation();
                metadata.update();

                // Write metadata
                let _ = disk_io.write().write_metadata(metadata.as_bytes());
                let _ = disk_io.write().flush();
            }
        }

        // Take ownership of write_buffer to properly shut it down
        if let Some(wb_arc) = self.write_buffer.take() {
            // Try to get mutable access if we're the only owner
            if let Ok(wb) = Arc::try_unwrap(wb_arc) {
                // We own it exclusively, can call complete_shutdown
                let mut wb_mut = wb;
                wb_mut.complete_shutdown();
            }
            // If we can't get exclusive access, workers are already shutting down via initiate_shutdown
        }

        // Now it's safe to shutdown disk I/O since workers have exited
        if let Some(ref disk_io) = self.disk_io {
            disk_io.write().shutdown();
        }
    }
}
