use super::*;

fn journal_with(slot: usize, image: &[u8]) -> Vec<u8> {
    let mut journal = vec![0; ALLOCATION_JOURNAL_BLOCKS as usize * FEOX_BLOCK_SIZE];
    let start = slot * JOURNAL_SLOT_SIZE;
    journal[start..start + image.len()].copy_from_slice(image);
    journal
}

fn write_slot(journal: &mut [u8], slot: usize, image: &[u8]) {
    let start = slot * JOURNAL_SLOT_SIZE;
    journal[start..start + image.len()].copy_from_slice(image);
}

#[test]
fn journal_selects_the_latest_generation() {
    let active = encode_active(1, &[(16, 1), (40, 9)]).unwrap();
    let clear = encode_clear(2).unwrap();
    let mut journal = journal_with(0, &active);
    write_slot(&mut journal, 1, &clear);

    let state = decode(&journal, 128).unwrap();
    assert_eq!(state.generation, 2);
    assert_eq!(state.slot, 1);
    assert!(state.extents.is_empty());
}

#[test]
fn journal_uses_only_the_blocks_occupied_by_entries() {
    assert_eq!(encode_clear(1).unwrap().len(), FEOX_BLOCK_SIZE);
    assert_eq!(encode_active(1, &[(16, 1)]).unwrap().len(), FEOX_BLOCK_SIZE);
    assert_eq!(
        encode_active(1, &vec![(16, 1); 600]).unwrap().len(),
        2 * FEOX_BLOCK_SIZE
    );
}

#[test]
fn journal_ignores_bytes_after_the_compact_image() {
    let active = encode_active(1, &[(16, 1)]).unwrap();
    let mut journal = journal_with(0, &active);
    journal[FEOX_BLOCK_SIZE] = 1;

    let state = decode(&journal, 128).unwrap();
    assert_eq!(state.extents, vec![(16, 1)]);
}

#[test]
fn journal_reads_full_slot_checksum_images() {
    let mut active = vec![0; JOURNAL_SLOT_SIZE];
    active[..8].copy_from_slice(JOURNAL_MAGIC);
    active[8..12].copy_from_slice(&FULL_SLOT_CHECKSUM_VERSION.to_le_bytes());
    active[16..24].copy_from_slice(&1_u64.to_le_bytes());
    active[24..28].copy_from_slice(&JOURNAL_ACTIVE.to_le_bytes());
    active[28..32].copy_from_slice(&1_u32.to_le_bytes());
    active[JOURNAL_HEADER_SIZE..JOURNAL_HEADER_SIZE + 4].copy_from_slice(&16_u32.to_le_bytes());
    active[JOURNAL_HEADER_SIZE + 4..JOURNAL_HEADER_SIZE + 8].copy_from_slice(&1_u32.to_le_bytes());
    stamp_checksum(&mut active);

    let state = decode(&journal_with(0, &active), 128).unwrap();
    assert_eq!(state.generation, 1);
    assert_eq!(state.extents, vec![(16, 1)]);
}

#[test]
fn journal_falls_back_from_a_torn_newer_slot() {
    let clear = encode_clear(2).unwrap();
    let mut active = encode_active(3, &[(16, 3)]).unwrap();
    active[JOURNAL_HEADER_SIZE + 1] ^= 1;
    let mut journal = journal_with(0, &active);
    write_slot(&mut journal, 1, &clear);

    let state = decode(&journal, 128).unwrap();
    assert_eq!(state.generation, 2);
    assert!(state.extents.is_empty());
}

#[test]
fn journal_falls_back_to_active_from_a_torn_clear() {
    let active = encode_active(3, &[(16, 3)]).unwrap();
    let mut clear = encode_clear(4).unwrap();
    clear[8] ^= 1;
    let mut journal = journal_with(0, &active);
    write_slot(&mut journal, 1, &clear);

    let state = decode(&journal, 128).unwrap();
    assert_eq!(state.generation, 3);
    assert_eq!(state.extents, vec![(16, 3)]);
}

#[test]
fn journal_rejects_invalid_extents() {
    for extents in [vec![(15, 3)], vec![(16, 0)]] {
        assert!(matches!(
            encode_active(1, &extents),
            Err(FeoxError::InvalidArgument)
        ));
    }

    let mut overlapping = encode_active(1, &[(16, 3), (20, 3)]).unwrap();
    overlapping[JOURNAL_HEADER_SIZE + JOURNAL_ENTRY_SIZE..JOURNAL_HEADER_SIZE + 12]
        .copy_from_slice(&18_u32.to_le_bytes());
    stamp_checksum(&mut overlapping);
    let journal = journal_with(0, &overlapping);
    assert!(decode(&journal, 128).unwrap().extents.is_empty());

    let out_of_bounds = encode_active(1, &[(126, 3)]).unwrap();
    let journal = journal_with(0, &out_of_bounds);
    assert!(decode(&journal, 128).unwrap().extents.is_empty());
}
