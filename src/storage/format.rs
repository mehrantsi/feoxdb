use crate::constants::*;
use crate::core::record::Record;
use crate::storage::seq_token::seq_token;
use std::sync::atomic::Ordering;

/// Trait for handling different record format versions
pub trait RecordFormat: Send + Sync {
    /// Calculate the size of a record on disk (excluding value)
    fn record_header_size(&self, key_len: usize) -> usize;

    /// Calculate total size including value
    fn total_size(&self, key_len: usize, value_len: usize) -> usize;

    /// Serialize a record to bytes for disk storage
    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8>;

    /// Append a serialized record without its sector header.
    fn serialize_record_into(&self, record: &Record, include_value: bool, data: &mut Vec<u8>) {
        data.extend_from_slice(&self.serialize_record(record, include_value));
    }

    /// Parse a record from disk bytes (returns key, value_len, timestamp, ttl_expiry)
    fn parse_record(&self, data: &[u8]) -> Option<(Vec<u8>, usize, u64, u64)>;

    /// Get the offset where value data starts in the serialized format
    fn value_offset(&self, key_len: usize) -> usize;
}

fn serialization_buffer(
    format: &dyn RecordFormat,
    record: &Record,
    include_value: bool,
) -> Vec<u8> {
    let value_len = if include_value { record.value_len } else { 0 };
    Vec::with_capacity(format.total_size(record.key.len(), value_len) - SECTOR_HEADER_SIZE)
}

/// Version 1 format (no TTL support)
pub struct FormatV1;
static FORMAT_V1: FormatV1 = FormatV1;

impl RecordFormat for FormatV1 {
    fn record_header_size(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 // header + key_len(2) + key + value_len(8) + timestamp(8)
    }

    fn total_size(&self, key_len: usize, value_len: usize) -> usize {
        self.record_header_size(key_len) + value_len
    }

    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8> {
        let mut data = serialization_buffer(self, record, include_value);
        self.serialize_record_into(record, include_value, &mut data);
        data
    }

    fn serialize_record_into(&self, record: &Record, include_value: bool, data: &mut Vec<u8>) {
        data.extend_from_slice(&(record.key.len() as u16).to_le_bytes());
        data.extend_from_slice(&record.key);
        data.extend_from_slice(&(record.value_len as u64).to_le_bytes());
        data.extend_from_slice(&record.timestamp.to_le_bytes());

        if include_value {
            if let Some(value) = record.value.read().as_ref() {
                data.extend_from_slice(value);
            }
        }
    }

    fn parse_record(&self, data: &[u8]) -> Option<(Vec<u8>, usize, u64, u64)> {
        if data.len() < SECTOR_HEADER_SIZE + 2 {
            return None;
        }

        let mut offset = SECTOR_HEADER_SIZE + 2;
        let key_len = u16::from_le_bytes(
            data[SECTOR_HEADER_SIZE..SECTOR_HEADER_SIZE + 2]
                .try_into()
                .ok()?,
        ) as usize;

        if offset + key_len + 16 > data.len() {
            return None;
        }

        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;

        let value_len = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?) as usize;
        offset += 8;

        let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);

        Some((key, value_len, timestamp, 0)) // No TTL in v1
    }

    fn value_offset(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8
    }
}

/// Version 2 format (with TTL support)
pub struct FormatV2;
static FORMAT_V2: FormatV2 = FormatV2;

impl RecordFormat for FormatV2 {
    fn record_header_size(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 + 8 // header + key_len(2) + key + value_len(8) + timestamp(8) + ttl(8)
    }

    fn total_size(&self, key_len: usize, value_len: usize) -> usize {
        self.record_header_size(key_len) + value_len
    }

    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8> {
        let mut data = serialization_buffer(self, record, include_value);
        self.serialize_record_into(record, include_value, &mut data);
        data
    }

    fn serialize_record_into(&self, record: &Record, include_value: bool, data: &mut Vec<u8>) {
        data.extend_from_slice(&(record.key.len() as u16).to_le_bytes());
        data.extend_from_slice(&record.key);
        data.extend_from_slice(&(record.value_len as u64).to_le_bytes());
        data.extend_from_slice(&record.timestamp.to_le_bytes());
        data.extend_from_slice(&record.ttl_expiry.load(Ordering::Acquire).to_le_bytes());

        if include_value {
            if let Some(value) = record.value.read().as_ref() {
                data.extend_from_slice(value);
            }
        }
    }

    fn parse_record(&self, data: &[u8]) -> Option<(Vec<u8>, usize, u64, u64)> {
        if data.len() < SECTOR_HEADER_SIZE + 2 {
            return None;
        }

        let mut offset = SECTOR_HEADER_SIZE + 2;
        let key_len = u16::from_le_bytes(
            data[SECTOR_HEADER_SIZE..SECTOR_HEADER_SIZE + 2]
                .try_into()
                .ok()?,
        ) as usize;

        if offset + key_len + 24 > data.len() {
            // 24 = value_len(8) + timestamp(8) + ttl(8)
            return None;
        }

        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;

        let value_len = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?) as usize;
        offset += 8;

        let timestamp = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);
        offset += 8;

        let ttl_expiry = u64::from_le_bytes(data[offset..offset + 8].try_into().ok()?);

        Some((key, value_len, timestamp, ttl_expiry))
    }

    fn value_offset(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 + 8
    }
}

/// Check that a head sector still stores the record it was read for.
///
/// A retired extent can be released and handed to another record while a reader
/// still holds the old `Record` and its stale sector number, so the bytes coming
/// back from disk must be proven to belong to this record before they are used.
/// Everything needed is already in the buffer the reader just filled, so this
/// costs no extra I/O. The layout prefix is identical in every format version:
/// marker(2) seq(2) key_len(2) key value_len(8) timestamp(8).
pub fn sector_holds_record(data: &[u8], record: &Record) -> bool {
    if data.len() < SECTOR_HEADER_SIZE + 2 {
        return false;
    }
    if u16::from_le_bytes([data[0], data[1]]) != SECTOR_MARKER {
        return false;
    }
    let key_len =
        u16::from_le_bytes([data[SECTOR_HEADER_SIZE], data[SECTOR_HEADER_SIZE + 1]]) as usize;
    if key_len != record.key.len() {
        return false;
    }
    let key_at = SECTOR_HEADER_SIZE + 2;
    let value_len_at = key_at + key_len;
    if value_len_at + 16 > data.len() {
        return false;
    }
    if data[key_at..value_len_at] != record.key[..] {
        return false;
    }
    let Ok(value_len) = data[value_len_at..value_len_at + 8].try_into() else {
        return false;
    };
    if u64::from_le_bytes(value_len) as usize != record.value_len {
        return false;
    }
    let Ok(timestamp) = data[value_len_at + 8..value_len_at + 16].try_into() else {
        return false;
    };
    u64::from_le_bytes(timestamp) == record.timestamp
}

#[cfg(test)]
pub(crate) fn fill_retirement_extent(retired: &mut [u8], sector: u64, sectors: usize) {
    debug_assert_eq!(retired.len(), sectors * FEOX_BLOCK_SIZE);
    retired.fill(0);
    fill_retirement_markers(retired, sector, sectors);
}

pub(crate) fn fill_retirement_markers(retired: &mut [u8], sector: u64, remaining: usize) {
    debug_assert_eq!(retired.len() % FEOX_BLOCK_SIZE, 0);
    let blocks = retired.len() / FEOX_BLOCK_SIZE;
    debug_assert!(blocks <= remaining);
    for offset in 0..blocks {
        let start = offset * FEOX_BLOCK_SIZE;
        fill_retirement_marker(
            &mut retired[start..start + DELETION_MARKER_SIZE],
            sector + offset as u64,
            remaining - offset,
        );
    }
}

pub(crate) fn fill_retirement_marker(marker: &mut [u8], sector: u64, remaining: usize) {
    write_retirement_marker(marker, sector, remaining, RETIREMENT_COMPLETE);
}

#[cfg(test)]
pub(crate) fn retirement_block(sector: u64, remaining: usize) -> Vec<u8> {
    let mut retired = vec![0u8; FEOX_BLOCK_SIZE];
    fill_retirement_marker(&mut retired[..DELETION_MARKER_SIZE], sector, remaining);
    retired
}

#[cfg(test)]
pub(crate) fn pending_retirement_block(sector: u64, remaining: usize) -> Vec<u8> {
    let mut retired = vec![0u8; FEOX_BLOCK_SIZE];
    write_retirement_marker(
        &mut retired[..DELETION_MARKER_SIZE],
        sector,
        remaining,
        RETIREMENT_PENDING,
    );
    retired
}

pub(crate) fn retirement_marker_token(sector: u64, marker: &[u8]) -> u16 {
    let mut protected = [0; 17];
    protected[..16].copy_from_slice(&marker[..16]);
    protected[16] = marker[18];
    seq_token(sector, &protected)
}

fn write_retirement_marker(marker: &mut [u8], sector: u64, remaining: usize, state: u8) {
    marker[..8].copy_from_slice(DELETION_MARKER);
    marker[8..16].copy_from_slice(&(remaining as u64).to_le_bytes());
    marker[18] = state;
    let token = retirement_marker_token(sector, marker);
    marker[16..18].copy_from_slice(&token.to_le_bytes());
}

pub fn get_format(version: u32) -> Box<dyn RecordFormat> {
    match version {
        1 => Box::new(FormatV1),
        2 | 3 => Box::new(FormatV2),
        _ => Box::new(FormatV2),
    }
}

pub(crate) fn get_format_ref(version: u32) -> &'static dyn RecordFormat {
    match version {
        1 => &FORMAT_V1,
        2 | 3 => &FORMAT_V2,
        _ => &FORMAT_V2,
    }
}
