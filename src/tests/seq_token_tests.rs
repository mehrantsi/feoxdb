use crate::constants::{FEOX_BLOCK_SIZE, SECTOR_MARKER};
use crate::storage::seq_token::{crc32c, record_seq_token, seq_token};

fn reference_crc32c(seed: u32, data: &[u8]) -> u32 {
    let mut crc = !seed;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F6_3B78
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[test]
fn crc32c_matches_reference_implementation() {
    for len in 0..200usize {
        let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
        assert_eq!(crc32c(0, &data), reference_crc32c(0, &data));
        assert_eq!(
            crc32c(0xDEAD_BEEF, &data),
            reference_crc32c(0xDEAD_BEEF, &data)
        );
    }
}

#[test]
fn crc32c_matches_known_vector() {
    assert_eq!(crc32c(0, b"123456789"), 0xE306_9283);
}

#[test]
fn token_is_never_zero_and_is_sector_bound() {
    let header = b"\x04\x00realABCDEFGHIJKLMNOPQRSTUVWX";
    let a = seq_token(16, header);
    let b = seq_token(17, header);
    assert_ne!(a, 0);
    assert_ne!(b, 0);
    assert_ne!(a, b);
}

#[test]
fn record_token_covers_continuation_bytes() {
    let mut data = vec![0; FEOX_BLOCK_SIZE * 2];
    data[..2].copy_from_slice(&SECTOR_MARKER.to_le_bytes());
    let original = record_seq_token(16, &data);
    data[FEOX_BLOCK_SIZE + 17] = 1;
    assert_ne!(record_seq_token(16, &data), original);
}
