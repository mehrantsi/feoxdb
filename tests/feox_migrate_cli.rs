use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::process::Command;

use tempfile::TempDir;

const BLOCK_SIZE: usize = 4096;
const DATA_START_BLOCK: u64 = 16;

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_feox-migrate"))
}

#[test]
fn help_describes_offline_copy_migration() {
    let output = command().arg("--help").output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--source <PATH>"));
    assert!(stdout.contains("--destination <PATH>"));
    assert!(stdout.contains("--allow-ambiguous-legacy-recovery"));
    assert!(stdout.contains("must not be open by another process"));
}

#[test]
fn version_matches_the_package() {
    let output = command().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("feox-migrate {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn invalid_arguments_use_the_usage_exit_code() {
    let missing = command().output().unwrap();
    assert_eq!(missing.status.code(), Some(2));
    assert!(String::from_utf8(missing.stderr)
        .unwrap()
        .contains("--source is required"));

    let unknown = command().arg("--unknown").output().unwrap();
    assert_eq!(unknown.status.code(), Some(2));
    assert!(String::from_utf8(unknown.stderr)
        .unwrap()
        .contains("unknown option"));
}

#[test]
fn migrates_a_v2_record_end_to_end_without_changing_the_source() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_v2_store(&source);
    write_at(
        &source,
        DATA_START_BLOCK,
        &v2_record(b"key", b"value", 42, 1),
    );
    let source_before = fs::read(&source).unwrap();

    let output = command()
        .arg("--source")
        .arg(&source)
        .arg("--destination")
        .arg(&destination)
        .output()
        .unwrap();

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(destination.exists());
    assert!(String::from_utf8(output.stdout)
        .unwrap()
        .contains("source_format=v2 destination_format=v3 records=1"));
    assert_eq!(fs::read(&source).unwrap(), source_before);
    assert_eq!(
        read_v3_record(&destination),
        (b"key".to_vec(), b"value".to_vec(), 42, 1)
    );
    assert!(fs::read_dir(temp.path()).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains("feox-migrate")
    }));
}

#[test]
fn ambiguous_recovery_requires_the_cli_flag() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let rejected = temp.path().join("rejected.feox");
    let accepted = temp.path().join("accepted.feox");
    initialize_v2_store(&source);

    let mut marker = vec![0; BLOCK_SIZE];
    marker[..8].copy_from_slice(b"\0DELETED");
    write_at(&source, DATA_START_BLOCK, &marker);
    let source_before = fs::read(&source).unwrap();

    let rejected_output = command()
        .arg("--source")
        .arg(&source)
        .arg("--destination")
        .arg(&rejected)
        .output()
        .unwrap();
    assert_eq!(rejected_output.status.code(), Some(1));
    assert!(String::from_utf8(rejected_output.stderr)
        .unwrap()
        .contains("--allow-ambiguous-legacy-recovery"));
    assert!(!rejected.exists());

    let accepted_output = command()
        .arg("--source")
        .arg(&source)
        .arg("--destination")
        .arg(&accepted)
        .arg("--allow-ambiguous-legacy-recovery")
        .output()
        .unwrap();
    assert!(
        accepted_output.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted_output.stderr)
    );
    assert!(String::from_utf8(accepted_output.stderr)
        .unwrap()
        .contains("warning: ambiguous legacy recovery"));
    assert!(String::from_utf8(accepted_output.stdout)
        .unwrap()
        .contains("ambiguous_markers=1"));
    assert_eq!(fs::read(&source).unwrap(), source_before);
}

#[test]
fn existing_destination_is_not_overwritten() {
    let temp = TempDir::new().unwrap();
    let source = temp.path().join("source.feox");
    let destination = temp.path().join("destination.feox");
    initialize_v2_store(&source);
    fs::write(&destination, b"sentinel").unwrap();

    let output = command()
        .arg("--source")
        .arg(&source)
        .arg("--destination")
        .arg(&destination)
        .output()
        .unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("destination already exists"));
    assert_eq!(fs::read(&destination).unwrap(), b"sentinel");
}

fn initialize_v2_store(path: &std::path::Path) {
    let device_size = (DATA_START_BLOCK + 1) * BLOCK_SIZE as u64;
    let mut metadata = [0; 136];
    metadata[..8].copy_from_slice(b"FEOX_SIG");
    metadata[8..12].copy_from_slice(&2_u32.to_le_bytes());
    metadata[32..40].copy_from_slice(&device_size.to_le_bytes());
    metadata[40..44].copy_from_slice(&(BLOCK_SIZE as u32).to_le_bytes());

    let mut file = File::create(path).unwrap();
    file.set_len(device_size).unwrap();
    file.write_all(&metadata).unwrap();
    file.sync_all().unwrap();
}

fn v2_record(key: &[u8], value: &[u8], timestamp: u64, ttl_expiry: u64) -> Vec<u8> {
    let mut record = Vec::new();
    record.extend_from_slice(&0xABCD_u16.to_le_bytes());
    record.extend_from_slice(&0_u16.to_le_bytes());
    record.extend_from_slice(&(key.len() as u16).to_le_bytes());
    record.extend_from_slice(key);
    record.extend_from_slice(&(value.len() as u64).to_le_bytes());
    record.extend_from_slice(&timestamp.to_le_bytes());
    record.extend_from_slice(&ttl_expiry.to_le_bytes());
    record.extend_from_slice(value);
    record.resize(record.len().div_ceil(BLOCK_SIZE) * BLOCK_SIZE, 0);
    record
}

fn write_at(path: &std::path::Path, sector: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(sector * BLOCK_SIZE as u64))
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn read_v3_record(path: &std::path::Path) -> (Vec<u8>, Vec<u8>, u64, u64) {
    let bytes = fs::read(path).unwrap();
    let head = &bytes[DATA_START_BLOCK as usize * BLOCK_SIZE..];
    assert_eq!(u16::from_le_bytes([head[0], head[1]]), 0xABCD);

    let key_len = u16::from_le_bytes([head[4], head[5]]) as usize;
    let key_start = 6;
    let key_end = key_start + key_len;
    let value_len = u64::from_le_bytes(head[key_end..key_end + 8].try_into().unwrap()) as usize;
    let timestamp = u64::from_le_bytes(head[key_end + 8..key_end + 16].try_into().unwrap());
    let ttl_expiry = u64::from_le_bytes(head[key_end + 16..key_end + 24].try_into().unwrap());
    let value_start = key_end + 24;
    (
        head[key_start..key_end].to_vec(),
        head[value_start..value_start + value_len].to_vec(),
        timestamp,
        ttl_expiry,
    )
}
