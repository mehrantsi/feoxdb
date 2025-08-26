use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::*;
use crate::core::record::Record;
use crate::error::{FeoxError, Result};
use crate::storage::format::get_format;
use crate::storage::metadata::Metadata;

use super::FeoxStore;

impl FeoxStore {
    pub(super) fn load_indexes(&mut self) -> Result<()> {
        if self.memory_only {
            return Ok(());
        }

        // Try to read metadata from sector 0
        if let Some(ref disk_io) = self.disk_io {
            let metadata_data = disk_io.read().read_metadata()?;

            // Check if metadata is valid (has our signature)
            if metadata_data.len() >= FEOX_SIGNATURE_SIZE {
                let signature = &metadata_data[..FEOX_SIGNATURE_SIZE];

                if signature == FEOX_SIGNATURE {
                    // Parse and store metadata
                    if let Some(metadata) = Metadata::from_bytes(&metadata_data) {
                        *self._metadata.write() = metadata;
                    }

                    // Valid metadata found, scan the disk to rebuild indexes
                    self.scan_and_rebuild_indexes()?;
                }
            }
        }

        Ok(())
    }

    pub(super) fn scan_and_rebuild_indexes(&mut self) -> Result<()> {
        if self.memory_only || self.device_size == 0 {
            return Ok(());
        }

        let disk_io = self.disk_io.as_ref().ok_or(FeoxError::NoDevice)?;

        // Get the appropriate format handler
        let metadata_version = self._metadata.read().version;
        let format = get_format(metadata_version);

        let total_sectors = self.device_size / FEOX_BLOCK_SIZE as u64;
        let mut sector: u64 = 1;
        let mut _records_loaded = 0;
        let mut occupied_sectors = Vec::new();

        while sector < total_sectors {
            let data = match disk_io.read().read_sectors_sync(sector, 1) {
                Ok(d) => d,
                Err(_) => {
                    sector += 1;
                    continue;
                }
            };

            if data.len() < SECTOR_HEADER_SIZE {
                sector += 1;
                continue;
            }

            // Check for deletion marker first
            if data.len() >= 8 && &data[..8] == b"\0DELETED" {
                // This sector has been deleted, skip it
                sector += 1;
                continue;
            }

            let marker = u16::from_le_bytes([data[0], data[1]]);
            let seq_num = u16::from_le_bytes([data[2], data[3]]);

            if marker != SECTOR_MARKER || seq_num != 0 {
                sector += 1;
                continue;
            }

            if data.len() < SECTOR_HEADER_SIZE + 2 {
                sector += 1;
                continue;
            }

            // Parse the record using format trait
            let (key, value_len, timestamp, ttl_expiry) = match format.parse_record(&data) {
                Some(parsed) => parsed,
                None => {
                    sector += 1;
                    continue;
                }
            };

            if key.is_empty() || key.len() > MAX_KEY_SIZE {
                sector += 1;
                continue;
            }

            // Calculate total size using format trait
            let total_size = format.total_size(key.len(), value_len);
            let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);

            let mut record = Record::new(key.clone(), Vec::new(), timestamp);
            record.sector.store(sector, Ordering::Release);
            record.value_len = value_len;
            record.ttl_expiry.store(ttl_expiry, Ordering::Release);
            record.clear_value();

            // Skip expired records during load if TTL is enabled
            if self.enable_ttl && ttl_expiry > 0 && self.get_timestamp() > ttl_expiry {
                sector += sectors_needed as u64;
                continue;
            }

            let record_arc = Arc::new(record);
            let key_len = key.len();
            self.hash_table.insert(key.clone(), Arc::clone(&record_arc));
            self.tree.insert(key, Arc::clone(&record_arc));

            self.stats.record_count.fetch_add(1, Ordering::AcqRel);
            let record_size = self.calculate_record_size(key_len, value_len);
            self.stats
                .memory_usage
                .fetch_add(record_size, Ordering::AcqRel);

            // Track disk usage
            self.stats
                .disk_usage
                .fetch_add((sectors_needed * FEOX_BLOCK_SIZE) as u64, Ordering::AcqRel);

            for i in 0..sectors_needed {
                occupied_sectors.push(sector + i as u64);
            }

            _records_loaded += 1;
            sector += sectors_needed as u64;
        }

        // Now rebuild free space from gaps between occupied sectors
        occupied_sectors.sort_unstable();

        // Start after metadata sectors (sectors 0-15 are reserved)
        let mut last_end = FEOX_DATA_START_BLOCK;

        for &occupied_start in &occupied_sectors {
            if occupied_start > last_end {
                self.free_space
                    .write()
                    .release_sectors(last_end, occupied_start - last_end)?;
            }
            last_end = occupied_start + 1;
        }

        if last_end < total_sectors {
            self.free_space
                .write()
                .release_sectors(last_end, total_sectors - last_end)?;
        }

        Ok(())
    }
}
