use crate::constants::*;
use crate::storage::format::RecordFormat;
use std::sync::OnceLock;

/// Metadata version from which the record header's seq_number field carries a
/// content-and-location bound token instead of a constant zero.
pub const SEQ_TOKEN_MIN_VERSION: u32 = 3;

const CRC32C_POLY: u32 = 0x82F6_3B78;

const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut crc = index as u32;
        let mut bit = 0;
        while bit < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY
            } else {
                crc >> 1
            };
            bit += 1;
        }
        table[index] = crc;
        index += 1;
    }
    table
};

fn crc32c_sw(seed: u32, data: &[u8]) -> u32 {
    let mut crc = !seed;
    for &byte in data {
        crc = CRC32C_TABLE[((crc ^ byte as u32) & 0xFF) as usize] ^ (crc >> 8);
    }
    !crc
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc32c_arm(seed: u32, data: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd, __crc32cw};

    let mut crc = !seed;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        crc = __crc32cd(crc, u64::from_le_bytes(chunk.try_into().unwrap()));
    }
    let rest = chunks.remainder();
    let mut i = 0;
    if rest.len() - i >= 4 {
        crc = __crc32cw(crc, u32::from_le_bytes(rest[i..i + 4].try_into().unwrap()));
        i += 4;
    }
    while i < rest.len() {
        crc = __crc32cb(crc, rest[i]);
        i += 1;
    }
    !crc
}

#[cfg(target_arch = "aarch64")]
fn crc32c_arm_dispatch(seed: u32, data: &[u8]) -> u32 {
    unsafe { crc32c_arm(seed, data) }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_x86(seed: u32, data: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u32, _mm_crc32_u64, _mm_crc32_u8};

    let mut crc = !seed;
    let mut chunks = data.chunks_exact(8);
    for chunk in &mut chunks {
        crc = _mm_crc32_u64(crc as u64, u64::from_le_bytes(chunk.try_into().unwrap())) as u32;
    }
    let rest = chunks.remainder();
    let mut i = 0;
    if rest.len() - i >= 4 {
        crc = _mm_crc32_u32(crc, u32::from_le_bytes(rest[i..i + 4].try_into().unwrap()));
        i += 4;
    }
    while i < rest.len() {
        crc = _mm_crc32_u8(crc, rest[i]);
        i += 1;
    }
    !crc
}

#[cfg(target_arch = "x86_64")]
fn crc32c_x86_dispatch(seed: u32, data: &[u8]) -> u32 {
    unsafe { crc32c_x86(seed, data) }
}

type Crc32c = fn(u32, &[u8]) -> u32;

static CRC32C_IMPL: OnceLock<Crc32c> = OnceLock::new();

fn select_crc32c() -> Crc32c {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        return crc32c_arm_dispatch;
    }

    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("sse4.2") {
        return crc32c_x86_dispatch;
    }

    crc32c_sw
}

#[inline]
fn crc32c_impl() -> Crc32c {
    *CRC32C_IMPL.get_or_init(select_crc32c)
}

pub(crate) fn crc32c(seed: u32, data: &[u8]) -> u32 {
    crc32c_impl()(seed, data)
}

/// Token bound to both the record header bytes and the absolute sector the extent
/// was allocated at. Never zero, so a token can never be confused with the legacy
/// constant-zero seq_number.
#[inline]
pub fn seq_token(sector: u64, header: &[u8]) -> u16 {
    let crc32c = crc32c_impl();
    let crc = crc32c(crc32c(0, &sector.to_le_bytes()), header);
    nonzero_token(crc)
}

/// Token bound to a record's landing sector and complete padded extent. The
/// seq_number bytes are treated as zero so stamping is idempotent.
#[inline]
pub fn record_seq_token(sector: u64, data: &[u8]) -> u16 {
    let crc32c = crc32c_impl();
    let mut crc = crc32c(0, &sector.to_le_bytes());
    if data.len() >= SECTOR_HEADER_SIZE {
        crc = crc32c(crc, &data[..2]);
        crc = crc32c(crc, &[0, 0]);
        crc = crc32c(crc, &data[SECTOR_HEADER_SIZE..]);
    } else {
        crc = crc32c(crc, data);
    }
    nonzero_token(crc)
}

#[inline]
fn nonzero_token(crc: u32) -> u16 {
    match ((crc >> 16) ^ (crc & 0xFFFF)) as u16 {
        0 => 1,
        token => token,
    }
}

/// Byte range of the record header inside the head sector, or None if the declared
/// key length does not fit in one sector.
#[inline]
pub fn header_range(format: &dyn RecordFormat, data: &[u8]) -> Option<std::ops::Range<usize>> {
    if data.len() < SECTOR_HEADER_SIZE + 2 {
        return None;
    }
    let key_len =
        u16::from_le_bytes([data[SECTOR_HEADER_SIZE], data[SECTOR_HEADER_SIZE + 1]]) as usize;
    if key_len == 0 {
        return None;
    }
    let end = format.record_header_size(key_len);
    if end > FEOX_BLOCK_SIZE || end > data.len() {
        return None;
    }
    Some(SECTOR_HEADER_SIZE..end)
}

/// Overwrite the seq_number of an already serialized head sector with its token.
#[inline]
pub fn stamp_seq_token(data: &mut [u8], sector: u64, format: &dyn RecordFormat) {
    if header_range(format, data).is_some() {
        let token = record_seq_token(sector, data);
        data[2..4].copy_from_slice(&token.to_le_bytes());
    }
}
