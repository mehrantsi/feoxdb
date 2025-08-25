use crate::constants::*;
use crate::core::record::Record;
use std::sync::atomic::Ordering;

/// Trait for handling different record format versions
pub trait RecordFormat: Send + Sync {
    /// Calculate the size of a record on disk (excluding value)
    fn record_header_size(&self, key_len: usize) -> usize;

    /// Calculate total size including value
    fn total_size(&self, key_len: usize, value_len: usize) -> usize;

    /// Serialize a record to bytes for disk storage
    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8>;

    /// Parse a record from disk bytes (returns key, value_len, timestamp, ttl_expiry)
    fn parse_record(&self, data: &[u8]) -> Option<(Vec<u8>, usize, u64, u64)>;

    /// Get the offset where value data starts in the serialized format
    fn value_offset(&self, key_len: usize) -> usize;
}

/// Version 1 format (no TTL support)
pub struct FormatV1;

impl RecordFormat for FormatV1 {
    fn record_header_size(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 // header + key_len(2) + key + value_len(8) + timestamp(8)
    }

    fn total_size(&self, key_len: usize, value_len: usize) -> usize {
        self.record_header_size(key_len) + value_len
    }

    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.total_size(record.key.len(), record.value_len));

        // Key length (2 bytes)
        data.extend_from_slice(&(record.key.len() as u16).to_le_bytes());

        // Key
        data.extend_from_slice(&record.key);

        // Value length (8 bytes)
        data.extend_from_slice(&(record.value_len as u64).to_le_bytes());

        // Timestamp (8 bytes)
        data.extend_from_slice(&record.timestamp.to_le_bytes());

        // Value (if requested)
        if include_value {
            if let Some(value) = record.value.read().as_ref() {
                data.extend_from_slice(value);
            }
        }

        data
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

impl RecordFormat for FormatV2 {
    fn record_header_size(&self, key_len: usize) -> usize {
        SECTOR_HEADER_SIZE + 2 + key_len + 8 + 8 + 8 // header + key_len(2) + key + value_len(8) + timestamp(8) + ttl(8)
    }

    fn total_size(&self, key_len: usize, value_len: usize) -> usize {
        self.record_header_size(key_len) + value_len
    }

    fn serialize_record(&self, record: &Record, include_value: bool) -> Vec<u8> {
        let mut data = Vec::with_capacity(self.total_size(record.key.len(), record.value_len));

        // Key length (2 bytes)
        data.extend_from_slice(&(record.key.len() as u16).to_le_bytes());

        // Key
        data.extend_from_slice(&record.key);

        // Value length (8 bytes)
        data.extend_from_slice(&(record.value_len as u64).to_le_bytes());

        // Timestamp (8 bytes)
        data.extend_from_slice(&record.timestamp.to_le_bytes());

        // TTL expiry (8 bytes) - always included in v2
        data.extend_from_slice(&record.ttl_expiry.load(Ordering::Acquire).to_le_bytes());

        // Value (if requested)
        if include_value {
            if let Some(value) = record.value.read().as_ref() {
                data.extend_from_slice(value);
            }
        }

        data
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

/// Factory function to get the appropriate format handler based on version
pub fn get_format(version: u32) -> Box<dyn RecordFormat> {
    match version {
        1 => Box::new(FormatV1),
        2 => Box::new(FormatV2),
        _ => Box::new(FormatV2), // Default to latest version
    }
}
