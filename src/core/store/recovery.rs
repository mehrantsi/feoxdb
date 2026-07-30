use std::ops::Bound;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::constants::*;
use crate::core::record::{Record, TreeSlot};
use crate::error::{FeoxError, Result};
use crate::storage::format::{get_format_ref, retirement_marker_token, RecordFormat};
use crate::storage::io::DiskIO;
use crate::storage::metadata::Metadata;
use crate::storage::seq_token::{crc32c, header_range, SEQ_TOKEN_MIN_VERSION};

use super::FeoxStore;

const RECOVERY_SCAN_BLOCKS: u64 = 256;
const RECOVERY_EXPIRED_BATCH: usize = 256;

struct RecoveryScanner<'a> {
    disk_io: &'a DiskIO,
    total_sectors: u64,
    buffer_start: u64,
    buffer: Vec<u8>,
}

impl<'a> RecoveryScanner<'a> {
    fn new(disk_io: &'a DiskIO, total_sectors: u64) -> Self {
        Self {
            disk_io,
            total_sectors,
            buffer_start: total_sectors,
            buffer: Vec::new(),
        }
    }

    fn block(&mut self, sector: u64) -> Result<&[u8]> {
        self.fill_at(sector)?;
        let offset = usize::try_from(sector - self.buffer_start)
            .map_err(|_| FeoxError::InvalidDevice)?
            * FEOX_BLOCK_SIZE;
        Ok(&self.buffer[offset..offset + FEOX_BLOCK_SIZE])
    }

    fn visit_blocks(
        &mut self,
        start: u64,
        blocks: u64,
        mut visit: impl FnMut(u64, &[u8]) -> bool,
    ) -> Result<bool> {
        let end = start
            .checked_add(blocks)
            .filter(|end| *end <= self.total_sectors)
            .ok_or(FeoxError::CorruptedRecord)?;
        let mut sector = start;

        while sector < end {
            self.fill_at(sector)?;
            let buffered_blocks = (self.buffer.len() / FEOX_BLOCK_SIZE) as u64;
            let buffer_end = self.buffer_start + buffered_blocks;
            let chunk_end = end.min(buffer_end);
            let offset = usize::try_from(sector - self.buffer_start)
                .map_err(|_| FeoxError::InvalidDevice)?
                * FEOX_BLOCK_SIZE;
            let len = usize::try_from(chunk_end - sector).map_err(|_| FeoxError::InvalidDevice)?
                * FEOX_BLOCK_SIZE;
            if !visit(sector, &self.buffer[offset..offset + len]) {
                return Ok(false);
            }
            sector = chunk_end;
        }

        Ok(true)
    }

    fn fill_at(&mut self, sector: u64) -> Result<()> {
        let buffered_blocks = (self.buffer.len() / FEOX_BLOCK_SIZE) as u64;
        if sector >= self.buffer_start && sector < self.buffer_start + buffered_blocks {
            return Ok(());
        }
        if sector >= self.total_sectors {
            return Err(FeoxError::CorruptedRecord);
        }

        let blocks = (self.total_sectors - sector).min(RECOVERY_SCAN_BLOCKS);
        self.buffer = self.disk_io.read_sectors_sync(sector, blocks)?;
        self.buffer_start = sector;
        Ok(())
    }
}

impl FeoxStore {
    pub(super) fn load_indexes(&mut self) -> Result<()> {
        if self.memory_only {
            return Ok(());
        }

        // A fresh device has nothing to scan, but its signature must be on disk
        // before the first record is written: without it a process that crashed
        // before its first flush_all recovers zero records and re-marks the whole
        // device free over live data.
        if self.fresh_device {
            if let Some(ref disk_io) = self.disk_io {
                let mut metadata = self._metadata.write();
                disk_io.read().initialize_store_metadata(&mut metadata)?;
            }
            return Ok(());
        }

        if let Some(ref disk_io) = self.disk_io {
            let metadata_data = disk_io.read().read_metadata()?;
            if metadata_data.len() < FEOX_SIGNATURE_SIZE
                || &metadata_data[..FEOX_SIGNATURE_SIZE] != FEOX_SIGNATURE
            {
                return Err(FeoxError::InvalidMetadata);
            }

            let metadata =
                Metadata::from_bytes(&metadata_data).ok_or(FeoxError::InvalidMetadata)?;
            self.format_version = metadata.version;
            *self._metadata.write() = metadata;
            self.scan_and_rebuild_indexes()?;
        }

        Ok(())
    }

    pub(super) fn scan_and_rebuild_indexes(&mut self) -> Result<()> {
        if self.memory_only || self.device_size == 0 {
            return Ok(());
        }

        let disk_io = self.disk_io.as_ref().ok_or(FeoxError::NoDevice)?;

        // Get the appropriate format handler
        let metadata_version = self.format_version;
        let format = get_format_ref(metadata_version);

        let total_sectors = self.device_size / FEOX_BLOCK_SIZE as u64;
        let disk = disk_io.read();
        let mut allocation_journal = disk.read_allocation_journal(total_sectors)?;
        if self.read_only {
            allocation_journal.sort_unstable_by_key(|extent| extent.0);
        } else if !allocation_journal.is_empty() {
            disk.replay_allocation_journal(&allocation_journal)?;
        }

        let mut scanner = RecoveryScanner::new(&disk, total_sectors);
        let mut sector = FEOX_DATA_START_BLOCK;
        let mut journal_index = 0;
        let mut last_end = FEOX_DATA_START_BLOCK;
        let mut retired_extents = Vec::new();
        let recovery_time = self.enable_ttl.then(|| self.get_timestamp_pub());
        self.stats.disk_usage.store(0, Ordering::Relaxed);

        'scan: while sector < total_sectors {
            if self.read_only {
                while let Some(&(start, sectors)) = allocation_journal.get(journal_index) {
                    let end = start
                        .checked_add(sectors as u64)
                        .ok_or(FeoxError::CorruptedRecord)?;
                    if sector < start {
                        break;
                    }
                    journal_index += 1;
                    if sector < end {
                        sector = end;
                        continue 'scan;
                    }
                }
            }

            let data = scanner.block(sector)?;

            if data.len() < SECTOR_HEADER_SIZE {
                sector += 1;
                continue;
            }

            // Check for deletion marker first
            if data.len() >= DELETION_MARKER.len() && &data[..8] == DELETION_MARKER {
                if metadata_version < SEQ_TOKEN_MIN_VERSION
                    && data[8..].iter().all(|byte| *byte == 0)
                {
                    if !self.allow_ambiguous_legacy_recovery {
                        return Err(FeoxError::AmbiguousLegacyTombstone);
                    }
                    self.ambiguous_legacy_markers += 1;
                    sector += 1;
                    continue;
                }

                if data.len() < DELETION_MARKER_SIZE {
                    return Err(FeoxError::CorruptedRecord);
                }

                let expected = retirement_marker_token(sector, data);
                let found = u16::from_le_bytes([data[16], data[17]]);
                if expected != found {
                    return Err(FeoxError::CorruptedRecord);
                }

                let extent = u64::from_le_bytes(data[8..16].try_into().unwrap());
                let Some(extent_end) = sector.checked_add(extent) else {
                    return Err(FeoxError::CorruptedRecord);
                };
                if extent == 0 || extent_end > total_sectors {
                    return Err(FeoxError::CorruptedRecord);
                }
                let mut needs_repair = data[18] != RETIREMENT_COMPLETE;
                if !needs_repair && extent > 1 {
                    needs_repair =
                        !scanner.visit_blocks(sector + 1, extent - 1, |chunk_sector, tails| {
                            tails
                                .chunks_exact(FEOX_BLOCK_SIZE)
                                .enumerate()
                                .all(|(index, tail)| {
                                    let tail_sector = chunk_sector + index as u64;
                                    is_complete_retirement_block(
                                        tail,
                                        tail_sector,
                                        extent - (tail_sector - sector),
                                    )
                                })
                        })?;
                }
                if needs_repair && !self.read_only {
                    retired_extents.push((sector, extent as usize));
                }
                sector = extent_end;
                continue;
            }

            let marker = u16::from_le_bytes([data[0], data[1]]);

            if marker != SECTOR_MARKER {
                sector += 1;
                continue;
            }

            if data.len() < SECTOR_HEADER_SIZE + 2 {
                if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                    return Err(FeoxError::CorruptedRecord);
                }
                sector += 1;
                continue;
            }

            if header_range(format, data).is_none() {
                if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                    return Err(FeoxError::CorruptedRecord);
                }
                sector += 1;
                continue;
            }

            let seq_num = u16::from_le_bytes([data[2], data[3]]);

            if (metadata_version < SEQ_TOKEN_MIN_VERSION && seq_num != 0)
                || (metadata_version >= SEQ_TOKEN_MIN_VERSION && seq_num == 0)
            {
                return Err(FeoxError::CorruptedRecord);
            }

            // Parse the record using format trait
            let (key, value_len, timestamp, ttl_expiry) = match format.parse_record(data) {
                Some(parsed) => parsed,
                None => {
                    if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                        return Err(FeoxError::CorruptedRecord);
                    }
                    sector += 1;
                    continue;
                }
            };

            if key.len() > MAX_KEY_SIZE || value_len == 0 || value_len > MAX_VALUE_SIZE {
                if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                    return Err(FeoxError::CorruptedRecord);
                }
                sector += 1;
                continue;
            }

            // Calculate total size using format trait
            let total_size = format.total_size(key.len(), value_len);
            let sectors_needed = total_size.div_ceil(FEOX_BLOCK_SIZE);
            let head_crc =
                (metadata_version >= SEQ_TOKEN_MIN_VERSION).then(|| record_crc_head(sector, data));

            // A record whose extent leaves the device is rejected, never propagated:
            // one such header used to make every subsequent open fail.
            let extent_in_bounds = sector
                .checked_add(sectors_needed as u64)
                .is_some_and(|end| end <= total_sectors);
            if sectors_needed == 0 || !extent_in_bounds {
                if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                    return Err(FeoxError::CorruptedRecord);
                }
                sector += 1;
                continue;
            }
            let extent_end = sector + sectors_needed as u64;
            if self.read_only && journal_overlaps(&allocation_journal, journal_index, extent_end) {
                return Err(FeoxError::CorruptedRecord);
            }

            if metadata_version >= SEQ_TOKEN_MIN_VERSION {
                let mut crc = head_crc.unwrap();
                if sectors_needed > 1 {
                    scanner.visit_blocks(sector + 1, sectors_needed as u64 - 1, |_, tail| {
                        crc = crc32c(crc, tail);
                        true
                    })?;
                }
                let actual = record_token(crc);
                if seq_num != actual {
                    return Err(FeoxError::CorruptedRecord);
                }
            }

            self.version_clock.observe(&key, timestamp);
            let mut record = Record::new(key.clone(), Vec::new(), timestamp);
            record.sector.store(sector, Ordering::Release);
            record.value_len = value_len;
            record.ttl_expiry.store(ttl_expiry, Ordering::Release);
            record.clear_value();

            let record_arc = Arc::new(record);
            let key_len = key.len();

            let existing = self.hash_table.read(&key, |_, record| Arc::clone(record));
            if existing
                .as_ref()
                .is_some_and(|record| record.timestamp > timestamp)
            {
                if !self.read_only {
                    retired_extents.push((sector, sectors_needed));
                }
                sector += sectors_needed as u64;
                continue;
            }

            if let Some(existing) = existing {
                let existing_sectors = format
                    .total_size(existing.key.len(), existing.value_len)
                    .div_ceil(FEOX_BLOCK_SIZE);
                let existing_sector = existing.sector.load(Ordering::Acquire);
                self.free_space
                    .write()
                    .release_sectors(existing_sector, existing_sectors as u64)?;
                self.stats.memory_usage.fetch_sub(
                    self.calculate_record_size(existing.key.len(), existing.value_len),
                    Ordering::Relaxed,
                );
                self.stats.disk_usage.fetch_sub(
                    (existing_sectors * FEOX_BLOCK_SIZE) as u64,
                    Ordering::Relaxed,
                );
                self.note_ttl_transition(existing.ttl_expiry.load(Ordering::Acquire), 0);
                if !self.read_only {
                    retired_extents.push((existing_sector, existing_sectors));
                }
            } else {
                self.stats.record_count.fetch_add(1, Ordering::Relaxed);
            }

            if sector > last_end {
                self.free_space
                    .write()
                    .release_sectors(last_end, sector - last_end)?;
            }
            last_end = sector + sectors_needed as u64;

            self.hash_table.upsert(key.clone(), Arc::clone(&record_arc));
            self.tree
                .insert(key, TreeSlot::new(Arc::clone(&record_arc)));

            let record_size = self.calculate_record_size(key_len, value_len);
            self.stats
                .memory_usage
                .fetch_add(record_size, Ordering::Relaxed);
            self.note_ttl_transition(0, ttl_expiry);

            // Track disk usage
            self.stats
                .disk_usage
                .fetch_add((sectors_needed * FEOX_BLOCK_SIZE) as u64, Ordering::Relaxed);

            sector += sectors_needed as u64;
        }

        if let Some(now) = recovery_time {
            self.remove_expired_recovery_winners(now, format, &mut retired_extents)?;
        }

        if !self.read_only {
            disk.retire_extents(&retired_extents)?;
        }

        if last_end < total_sectors {
            self.free_space
                .write()
                .release_sectors(last_end, total_sectors - last_end)?;
        }

        Ok(())
    }

    fn remove_expired_recovery_winners(
        &self,
        now: u64,
        format: &dyn RecordFormat,
        retired_extents: &mut Vec<(u64, usize)>,
    ) -> Result<()> {
        let mut after = None;

        loop {
            let (last, expired) = {
                let guard = &crossbeam_epoch::pin();
                let mut cursor = match after.as_deref() {
                    Some(key) => self.tree.lower_bound(Bound::Excluded(key)),
                    None => self.tree.front(),
                };
                let mut last = None;
                let mut expired = Vec::new();
                let mut visited = 0;

                while visited < RECOVERY_EXPIRED_BATCH {
                    let Some(entry) = cursor else {
                        break;
                    };
                    let record = Arc::clone(entry.value().load(guard));
                    last = Some(entry.key().clone());
                    cursor = entry.next();
                    let expiry = record.ttl_expiry.load(Ordering::Acquire);
                    if expiry > 0 && now > expiry {
                        expired.push((record.key.clone(), record));
                    }
                    visited += 1;
                }

                (last, expired)
            };

            let Some(last) = last else {
                break;
            };
            after = Some(last);

            for (key, record) in expired {
                let removed = match self.hash_table.entry(key.clone()) {
                    scc::hash_map::Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), &record) => {
                        self.tree.remove(&key);
                        let _ = entry.remove();
                        true
                    }
                    _ => false,
                };
                if !removed {
                    continue;
                }

                let sectors = format
                    .total_size(record.key.len(), record.value_len)
                    .div_ceil(FEOX_BLOCK_SIZE);
                let sector = record.sector.load(Ordering::Acquire);
                record.refcount.store(0, Ordering::Release);
                self.free_space
                    .write()
                    .release_sectors(sector, sectors as u64)?;
                self.stats.record_count.fetch_sub(1, Ordering::Relaxed);
                self.stats.memory_usage.fetch_sub(
                    self.calculate_record_size(record.key.len(), record.value_len),
                    Ordering::Relaxed,
                );
                self.stats
                    .disk_usage
                    .fetch_sub((sectors * FEOX_BLOCK_SIZE) as u64, Ordering::Relaxed);
                self.note_ttl_transition(record.ttl_expiry.load(Ordering::Acquire), 0);
                if !self.read_only {
                    retired_extents.push((sector, sectors));
                }
            }
        }

        Ok(())
    }
}

fn journal_overlaps(journal: &[(u64, usize)], index: usize, extent_end: u64) -> bool {
    journal
        .get(index)
        .is_some_and(|(start, _)| *start < extent_end)
}

fn record_crc_head(sector: u64, head: &[u8]) -> u32 {
    let mut crc = crc32c(0, &sector.to_le_bytes());
    crc = crc32c(crc, &head[..2]);
    crc = crc32c(crc, &[0, 0]);
    crc32c(crc, &head[SECTOR_HEADER_SIZE..])
}

fn record_token(crc: u32) -> u16 {
    match ((crc >> 16) ^ (crc & 0xFFFF)) as u16 {
        0 => 1,
        token => token,
    }
}

fn is_complete_retirement_block(data: &[u8], sector: u64, remaining: u64) -> bool {
    data.len() >= DELETION_MARKER_SIZE
        && &data[..8] == DELETION_MARKER
        && u64::from_le_bytes(data[8..16].try_into().unwrap()) == remaining
        && data[18] == RETIREMENT_COMPLETE
        && u16::from_le_bytes([data[16], data[17]]) == retirement_marker_token(sector, data)
}
