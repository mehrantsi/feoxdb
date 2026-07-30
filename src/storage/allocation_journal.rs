use crate::constants::{FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK};
use crate::error::{FeoxError, Result};
use crate::storage::seq_token::crc32c;

pub(crate) const ALLOCATION_JOURNAL_START_BLOCK: u64 = 1;
pub(crate) const ALLOCATION_JOURNAL_SLOT_BLOCKS: u64 = 3;
pub(crate) const ALLOCATION_JOURNAL_SLOTS: usize = 2;
pub(crate) const ALLOCATION_JOURNAL_MAX_ENTRIES: usize = 1024;

const JOURNAL_MAGIC: &[u8; 8] = b"\0FEOXAJ1";
const JOURNAL_VERSION: u32 = 2;
const FULL_SLOT_CHECKSUM_VERSION: u32 = 1;
const JOURNAL_CLEAR: u32 = 0;
const JOURNAL_ACTIVE: u32 = 1;
const JOURNAL_HEADER_SIZE: usize = 40;
const JOURNAL_ENTRY_SIZE: usize = 8;
const JOURNAL_SLOT_SIZE: usize = ALLOCATION_JOURNAL_SLOT_BLOCKS as usize * FEOX_BLOCK_SIZE;
pub(crate) const ALLOCATION_JOURNAL_BLOCKS: u64 =
    ALLOCATION_JOURNAL_SLOT_BLOCKS * ALLOCATION_JOURNAL_SLOTS as u64;

pub(crate) struct JournalState {
    pub(crate) generation: u64,
    pub(crate) slot: usize,
    pub(crate) extents: Vec<(u64, usize)>,
}

pub(crate) fn encode_active(generation: u64, extents: &[(u64, usize)]) -> Result<Vec<u8>> {
    if generation == 0 || extents.is_empty() || extents.len() > ALLOCATION_JOURNAL_MAX_ENTRIES {
        return Err(FeoxError::InvalidArgument);
    }

    let mut journal = journal_header(generation, JOURNAL_ACTIVE, extents.len());
    for (index, &(sector, sectors)) in extents.iter().enumerate() {
        let sector = u32::try_from(sector).map_err(|_| FeoxError::InvalidArgument)?;
        let sectors = u32::try_from(sectors).map_err(|_| FeoxError::InvalidArgument)?;
        if sector < FEOX_DATA_START_BLOCK as u32 || sectors == 0 {
            return Err(FeoxError::InvalidArgument);
        }
        let offset = JOURNAL_HEADER_SIZE + index * JOURNAL_ENTRY_SIZE;
        journal[offset..offset + 4].copy_from_slice(&sector.to_le_bytes());
        journal[offset + 4..offset + 8].copy_from_slice(&sectors.to_le_bytes());
    }
    stamp_checksum(&mut journal);
    Ok(journal)
}

pub(crate) fn encode_clear(generation: u64) -> Result<Vec<u8>> {
    if generation == 0 {
        return Err(FeoxError::InvalidArgument);
    }
    let mut journal = journal_header(generation, JOURNAL_CLEAR, 0);
    stamp_checksum(&mut journal);
    Ok(journal)
}

pub(crate) fn decode(data: &[u8], total_sectors: u64) -> Result<JournalState> {
    if data.len() != ALLOCATION_JOURNAL_BLOCKS as usize * FEOX_BLOCK_SIZE {
        return Err(FeoxError::CorruptedRecord);
    }

    let mut valid = Vec::with_capacity(ALLOCATION_JOURNAL_SLOTS);
    let mut missing = Vec::with_capacity(ALLOCATION_JOURNAL_SLOTS);
    for slot in 0..ALLOCATION_JOURNAL_SLOTS {
        let start = slot * JOURNAL_SLOT_SIZE;
        let bytes = &data[start..start + JOURNAL_SLOT_SIZE];
        if bytes.iter().all(|byte| *byte == 0) {
            missing.push(slot);
        } else if let Ok(state) = decode_slot(bytes, total_sectors, slot) {
            valid.push(state);
        }
    }

    if let Some(state) = valid.into_iter().max_by_key(|state| state.generation) {
        return Ok(state);
    }
    if let Some(slot) = missing.into_iter().next_back() {
        return Ok(JournalState {
            generation: 0,
            slot,
            extents: Vec::new(),
        });
    }
    Err(FeoxError::CorruptedRecord)
}

fn journal_header(generation: u64, state: u32, count: usize) -> Vec<u8> {
    let mut journal = vec![0; journal_image_size(count)];
    journal[..8].copy_from_slice(JOURNAL_MAGIC);
    journal[8..12].copy_from_slice(&JOURNAL_VERSION.to_le_bytes());
    journal[16..24].copy_from_slice(&generation.to_le_bytes());
    journal[24..28].copy_from_slice(&state.to_le_bytes());
    journal[28..32].copy_from_slice(&(count as u32).to_le_bytes());
    journal
}

fn decode_slot(data: &[u8], total_sectors: u64, slot: usize) -> Result<JournalState> {
    if &data[..8] != JOURNAL_MAGIC {
        return Err(FeoxError::CorruptedRecord);
    }

    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if !matches!(version, FULL_SLOT_CHECKSUM_VERSION | JOURNAL_VERSION) {
        return Err(FeoxError::CorruptedRecord);
    }

    let generation = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let state = u32::from_le_bytes(data[24..28].try_into().unwrap());
    let count = u32::from_le_bytes(data[28..32].try_into().unwrap()) as usize;
    if generation == 0
        || count > ALLOCATION_JOURNAL_MAX_ENTRIES
        || !matches!(state, JOURNAL_CLEAR | JOURNAL_ACTIVE)
        || (state == JOURNAL_CLEAR && count != 0)
        || (state == JOURNAL_ACTIVE && count == 0)
    {
        return Err(FeoxError::CorruptedRecord);
    }

    let checksum_len = if version == FULL_SLOT_CHECKSUM_VERSION {
        JOURNAL_SLOT_SIZE
    } else {
        journal_image_size(count)
    };
    let checksum = u32::from_le_bytes(data[12..16].try_into().unwrap());
    let complement = u32::from_le_bytes(data[32..36].try_into().unwrap());
    if complement != !checksum || journal_checksum(&data[..checksum_len]) != checksum {
        return Err(FeoxError::CorruptedRecord);
    }

    let mut extents = Vec::with_capacity(count);
    for index in 0..count {
        let offset = JOURNAL_HEADER_SIZE + index * JOURNAL_ENTRY_SIZE;
        let sector = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as u64;
        let sectors = u32::from_le_bytes(data[offset + 4..offset + 8].try_into().unwrap()) as u64;
        let end = sector
            .checked_add(sectors)
            .filter(|end| *end <= total_sectors)
            .ok_or(FeoxError::CorruptedRecord)?;
        if sector < FEOX_DATA_START_BLOCK || sectors == 0 || end <= sector {
            return Err(FeoxError::CorruptedRecord);
        }
        extents.push((sector, sectors as usize));
    }

    let mut ordered = extents.clone();
    ordered.sort_unstable_by_key(|extent| extent.0);
    for pair in ordered.windows(2) {
        let previous_end = pair[0]
            .0
            .checked_add(pair[0].1 as u64)
            .ok_or(FeoxError::CorruptedRecord)?;
        if previous_end > pair[1].0 {
            return Err(FeoxError::CorruptedRecord);
        }
    }

    Ok(JournalState {
        generation,
        slot,
        extents,
    })
}

fn stamp_checksum(data: &mut [u8]) {
    let checksum = journal_checksum(data);
    data[12..16].copy_from_slice(&checksum.to_le_bytes());
    data[32..36].copy_from_slice(&(!checksum).to_le_bytes());
}

fn journal_image_size(count: usize) -> usize {
    (JOURNAL_HEADER_SIZE + count * JOURNAL_ENTRY_SIZE).div_ceil(FEOX_BLOCK_SIZE) * FEOX_BLOCK_SIZE
}

fn journal_checksum(data: &[u8]) -> u32 {
    let mut checksum = crc32c(0, &data[..12]);
    checksum = crc32c(checksum, &[0; 4]);
    checksum = crc32c(checksum, &data[16..32]);
    checksum = crc32c(checksum, &[0; 4]);
    crc32c(checksum, &data[36..])
}

#[cfg(test)]
#[path = "../tests/allocation_journal_tests.rs"]
mod tests;
