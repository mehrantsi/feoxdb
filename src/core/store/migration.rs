//! Offline migration from legacy store formats.

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;

use crossbeam_epoch as epoch;
use thiserror::Error;

use crate::constants::{
    FEOX_BLOCK_SIZE, FEOX_DATA_START_BLOCK, MAX_DEVICE_SIZE, MAX_RECOVERABLE_KEY_SIZE,
};
use crate::core::record::Record;
use crate::error::FeoxError;
use crate::storage::format::get_format_ref;

use super::{FeoxStore, StoreConfig};

const MIGRATION_HASH_BITS: u32 = 18;
const MIGRATION_SCAN_RECORDS: usize = 256;
const MIGRATION_FLUSH_RECORDS: u64 = 4_096;
const MIGRATION_FLUSH_BYTES: u64 = 64 * 1024 * 1024;
const TEMP_CREATE_ATTEMPTS: usize = 16;

/// Options for copying a v1 or v2 store into a new v3 store.
///
/// The source must be offline for the entire migration. FeOxDB opens it
/// read-only but does not lock it against another process.
#[derive(Clone, Debug)]
pub struct MigrationOptions {
    source: PathBuf,
    destination: PathBuf,
    allow_ambiguous_legacy_recovery: bool,
    hash_bits: u32,
}

impl MigrationOptions {
    /// Create migration options for an offline source and a new destination.
    ///
    /// The destination must not exist and is never overwritten.
    pub fn new(source: impl Into<PathBuf>, destination: impl Into<PathBuf>) -> Self {
        Self {
            source: source.into(),
            destination: destination.into(),
            allow_ambiguous_legacy_recovery: false,
            hash_bits: MIGRATION_HASH_BITS,
        }
    }

    /// Permit the historical skip-one interpretation of legacy deletion markers.
    ///
    /// A v1/v2 marker does not encode the retired extent length. Enabling this can
    /// expose continuation bytes from a deleted multi-sector record as a record.
    pub fn allow_ambiguous_legacy_recovery(mut self, allow: bool) -> Self {
        self.allow_ambiguous_legacy_recovery = allow;
        self
    }

    #[cfg(test)]
    pub(crate) fn hash_bits(mut self, hash_bits: u32) -> Self {
        self.hash_bits = hash_bits;
        self
    }
}

/// Summary of a completed and verified migration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationReport {
    /// Legacy source format version.
    pub source_version: u32,
    /// Format version written to the destination.
    pub destination_version: u32,
    /// Number of recovered records copied.
    pub records: u64,
    /// Total logical value bytes copied.
    pub value_bytes: u64,
    /// Allocated destination file size.
    pub destination_size: u64,
    /// Number of ambiguous legacy markers accepted by explicit opt-in.
    pub ambiguous_legacy_markers: u64,
}

/// Error returned by an offline migration.
#[derive(Error, Debug)]
pub enum MigrationError {
    #[error("destination path has no file name: {0:?}")]
    InvalidDestination(PathBuf),

    #[error("destination already exists: {0:?}")]
    DestinationExists(PathBuf),

    #[error("source store already uses format v{0}")]
    CurrentFormat(u32),

    #[error("source key is {length} bytes; format v3 supports at most {maximum}")]
    KeyTooLarge { length: usize, maximum: usize },

    #[error("format v3 destination would exceed the maximum device size")]
    DestinationTooLarge,

    #[error("source changed while migration was running")]
    SourceChanged,

    #[error("temporary destination changed before publication")]
    DestinationChanged,

    #[error("migration verification failed at record {0}")]
    VerificationFailed(u64),

    #[error("ambiguous legacy deletion marker requires explicit unsafe migration opt-in")]
    AmbiguousLegacyRecovery,

    #[error("{operation} {path:?}: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error(transparent)]
    Store(#[from] FeoxError),
}

/// Result type returned by migration operations.
pub type MigrationResult<T> = std::result::Result<T, MigrationError>;

/// Copy an offline format v1 or v2 store into a new format v3 store.
///
/// The source is opened read-only and remains unchanged. Migration resolves the
/// newest record generation for each key with TTL filtering disabled, then
/// preserves each winning record's timestamp and absolute TTL expiry exactly.
/// Consequently, an expired newest generation remains the winner and an older
/// value cannot reappear when the migrated store is opened with TTL enabled.
///
/// Data is written to a sibling temporary file, flushed, reopened, and compared
/// record-for-record before it is published at the destination without
/// overwriting an existing path. Verification proves that the logical copy
/// matches recovery; legacy v1/v2 data has no v3 record checksums, so it cannot
/// prove that the source itself was uncorrupted.
///
/// No process may write the source or manipulate the destination paths during
/// this operation. The implementation performs best-effort identity and metadata
/// checks, not cooperative locking.
pub fn migrate(options: MigrationOptions) -> MigrationResult<MigrationReport> {
    let source_file = open_read_only_file(&options.source)?;
    let source_stamp = FileStamp::read_file(&source_file, &options.source)?;
    if FileStamp::read(&options.source)? != source_stamp {
        return Err(MigrationError::SourceChanged);
    }
    let source = build_read_only(
        source_file,
        options.allow_ambiguous_legacy_recovery,
        options.hash_bits,
    )?;
    if FileStamp::read_store_file(&source, &options.source)? != source_stamp
        || FileStamp::read(&options.source)? != source_stamp
    {
        return Err(MigrationError::SourceChanged);
    }
    let source_version = source.format_version;
    if source_version >= 3 {
        return Err(MigrationError::CurrentFormat(source_version));
    }

    let layout = source_layout(&source)?;
    let destination_size = source.device_size.max(layout.required_size);
    if destination_size > MAX_DEVICE_SIZE {
        return Err(MigrationError::DestinationTooLarge);
    }

    let mut destination_guard = DestinationGuard::create(&options.destination)?;
    let destination = FeoxStore::with_config_for_migration_destination(
        migration_config(options.hash_bits, Some(destination_size)),
        destination_guard.take_file(),
    )?;

    copy_records(&source, &destination)?;
    destination.flush()?;
    let verification_file = destination
        .device_file
        .as_ref()
        .ok_or(FeoxError::NoDevice)?
        .try_clone()
        .map_err(|source| MigrationError::Io {
            operation: "cloning temporary destination",
            path: destination_guard.temporary.clone(),
            source,
        })?;
    drop(destination);

    let verified = build_read_only(verification_file, false, options.hash_bits)?;
    if verified.format_version != 3 {
        return Err(MigrationError::VerificationFailed(0));
    }
    let records = verify_records(&source, &verified)?;
    let destination_stamp =
        FileStamp::read_store_file(&verified, destination_guard.temporary_path())?;
    drop(verified);

    if FileStamp::read_store_file(&source, &options.source)? != source_stamp
        || FileStamp::read(&options.source)? != source_stamp
    {
        return Err(MigrationError::SourceChanged);
    }

    destination_guard.publish(&destination_stamp)?;
    Ok(MigrationReport {
        source_version,
        destination_version: 3,
        records,
        value_bytes: layout.value_bytes,
        destination_size,
        ambiguous_legacy_markers: source.ambiguous_legacy_markers,
    })
}

struct SourceLayout {
    required_size: u64,
    value_bytes: u64,
}

fn source_layout(store: &FeoxStore) -> MigrationResult<SourceLayout> {
    let format = get_format_ref(3);
    let mut after = None;
    let mut sectors = FEOX_DATA_START_BLOCK;
    let mut value_bytes = 0_u64;

    loop {
        let records = record_batch(store, after.as_deref());
        let Some(last) = records.last() else {
            break;
        };
        after = Some(last.key.clone());

        for record in records {
            if record.key.len() > MAX_RECOVERABLE_KEY_SIZE {
                return Err(MigrationError::KeyTooLarge {
                    length: record.key.len(),
                    maximum: MAX_RECOVERABLE_KEY_SIZE,
                });
            }
            let record_sectors = format
                .total_size(record.key.len(), record.value_len)
                .div_ceil(FEOX_BLOCK_SIZE) as u64;
            sectors = sectors
                .checked_add(record_sectors)
                .ok_or(MigrationError::DestinationTooLarge)?;
            value_bytes = value_bytes
                .checked_add(record.value_len as u64)
                .ok_or(MigrationError::DestinationTooLarge)?;
        }
    }

    let required_size = sectors
        .checked_mul(FEOX_BLOCK_SIZE as u64)
        .ok_or(MigrationError::DestinationTooLarge)?;
    Ok(SourceLayout {
        required_size,
        value_bytes,
    })
}

fn copy_records(source: &FeoxStore, destination: &FeoxStore) -> MigrationResult<()> {
    let mut after = None;
    let mut visited = 0_u64;
    let mut pending_records = 0_u64;
    let mut pending_bytes = 0_u64;

    loop {
        let records = record_batch(source, after.as_deref());
        let Some(last) = records.last() else {
            break;
        };
        after = Some(last.key.clone());

        for record in records {
            let value = source.resolve_value_ref(&record.key, &record)?;
            let value_len = value.len() as u64;
            let inserted = destination.insert_migrated_bytes(
                &record.key,
                value,
                record.timestamp,
                record.ttl_expiry.load(Ordering::Acquire),
            )?;
            if !inserted {
                return Err(MigrationError::VerificationFailed(visited));
            }
            visited += 1;
            pending_records += 1;
            pending_bytes += value_len;
            if pending_records >= MIGRATION_FLUSH_RECORDS || pending_bytes >= MIGRATION_FLUSH_BYTES
            {
                destination.flush()?;
                pending_records = 0;
                pending_bytes = 0;
            }
        }
    }

    Ok(())
}

fn verify_records(source: &FeoxStore, destination: &FeoxStore) -> MigrationResult<u64> {
    let mut source_after = None;
    let mut destination_after = None;
    let mut records = 0;

    loop {
        let source_records = record_batch(source, source_after.as_deref());
        let destination_records = record_batch(destination, destination_after.as_deref());
        if source_records.is_empty() && destination_records.is_empty() {
            return Ok(records);
        }
        if source_records.len() != destination_records.len() {
            return Err(MigrationError::VerificationFailed(records));
        }

        source_after = source_records.last().map(|record| record.key.clone());
        destination_after = destination_records.last().map(|record| record.key.clone());

        for (source_record, destination_record) in
            source_records.into_iter().zip(destination_records)
        {
            let same_metadata = source_record.key == destination_record.key
                && source_record.timestamp == destination_record.timestamp
                && source_record.ttl_expiry.load(Ordering::Acquire)
                    == destination_record.ttl_expiry.load(Ordering::Acquire);
            if !same_metadata
                || source.resolve_value_ref(&source_record.key, &source_record)?
                    != destination
                        .resolve_value_ref(&destination_record.key, &destination_record)?
            {
                return Err(MigrationError::VerificationFailed(records));
            }
            records += 1;
        }
    }
}

fn record_batch(store: &FeoxStore, after: Option<&[u8]>) -> Vec<Arc<Record>> {
    let guard = &epoch::pin();
    let mut cursor = match after {
        Some(key) => store.tree.lower_bound(Bound::Excluded(key)),
        None => store.tree.front(),
    };
    let mut records = Vec::with_capacity(MIGRATION_SCAN_RECORDS);

    while records.len() < MIGRATION_SCAN_RECORDS {
        let Some(entry) = cursor else {
            break;
        };
        records.push(Arc::clone(entry.value().load(guard)));
        cursor = entry.next();
    }

    records
}

fn open_read_only_file(path: &Path) -> MigrationResult<File> {
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|source| MigrationError::Io {
            operation: "opening",
            path: path.to_path_buf(),
            source,
        })
}

fn build_read_only(
    file: File,
    allow_ambiguous_legacy_recovery: bool,
    hash_bits: u32,
) -> MigrationResult<FeoxStore> {
    FeoxStore::with_config_for_migration_source(
        migration_config(hash_bits, None),
        allow_ambiguous_legacy_recovery,
        file,
    )
    .map_err(|error| match error {
        FeoxError::AmbiguousLegacyTombstone => MigrationError::AmbiguousLegacyRecovery,
        error => MigrationError::Store(error),
    })
}

fn migration_config(hash_bits: u32, file_size: Option<u64>) -> StoreConfig {
    StoreConfig {
        hash_bits,
        memory_only: false,
        enable_caching: false,
        device_path: None,
        file_size,
        max_memory: None,
        enable_ttl: false,
        ttl_config: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

impl FileStamp {
    fn read(path: &Path) -> MigrationResult<Self> {
        let metadata = fs::metadata(path).map_err(|source| MigrationError::Io {
            operation: "reading",
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self::from_metadata(metadata))
    }

    fn read_store_file(store: &FeoxStore, path: &Path) -> MigrationResult<Self> {
        let file = store.device_file.as_ref().ok_or(FeoxError::NoDevice)?;
        Self::read_file(file, path)
    }

    fn read_regular(path: &Path) -> MigrationResult<Option<Self>> {
        let metadata = fs::symlink_metadata(path).map_err(|source| MigrationError::Io {
            operation: "reading",
            path: path.to_path_buf(),
            source,
        })?;
        Ok(metadata
            .file_type()
            .is_file()
            .then(|| Self::from_metadata(metadata)))
    }

    fn read_file(file: &File, path: &Path) -> MigrationResult<Self> {
        let metadata = file.metadata().map_err(|source| MigrationError::Io {
            operation: "reading metadata for",
            path: path.to_path_buf(),
            source,
        })?;
        Ok(Self::from_metadata(metadata))
    }

    fn from_metadata(metadata: fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;

        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
        }
    }
}

struct DestinationGuard {
    destination: PathBuf,
    temporary: PathBuf,
    file: Option<File>,
}

impl DestinationGuard {
    fn create(destination: &Path) -> MigrationResult<Self> {
        match fs::symlink_metadata(destination) {
            Ok(_) => {
                return Err(MigrationError::DestinationExists(destination.to_path_buf()));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(MigrationError::Io {
                    operation: "checking",
                    path: destination.to_path_buf(),
                    source,
                });
            }
        }

        let file_name = destination
            .file_name()
            .ok_or_else(|| MigrationError::InvalidDestination(destination.to_path_buf()))?;
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        let mut last_collision = None;

        for _ in 0..TEMP_CREATE_ATTEMPTS {
            let mut temporary_name = OsString::from(".");
            temporary_name.push(file_name);
            temporary_name.push(format!(
                ".feox-migrate-{}-{:016x}.tmp",
                process::id(),
                rand::random::<u64>()
            ));
            let temporary = parent.join(temporary_name);
            match OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(file) => {
                    return Ok(Self {
                        destination: destination.to_path_buf(),
                        temporary,
                        file: Some(file),
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                    last_collision = Some(error);
                }
                Err(source) => {
                    return Err(MigrationError::Io {
                        operation: "creating temporary destination beside",
                        path: destination.to_path_buf(),
                        source,
                    });
                }
            }
        }

        Err(MigrationError::Io {
            operation: "creating temporary destination beside",
            path: destination.to_path_buf(),
            source: last_collision.unwrap_or_else(|| {
                io::Error::new(io::ErrorKind::AlreadyExists, "temporary name collision")
            }),
        })
    }

    fn take_file(&mut self) -> File {
        self.file
            .take()
            .expect("temporary destination file missing")
    }

    fn temporary_path(&self) -> &Path {
        &self.temporary
    }

    fn publish(&mut self, expected: &FileStamp) -> MigrationResult<()> {
        if FileStamp::read_regular(&self.temporary)?.as_ref() != Some(expected) {
            return Err(MigrationError::DestinationChanged);
        }
        fs::hard_link(&self.temporary, &self.destination).map_err(|source| {
            if source.kind() == io::ErrorKind::AlreadyExists {
                MigrationError::DestinationExists(self.destination.clone())
            } else {
                MigrationError::Io {
                    operation: "publishing",
                    path: self.destination.clone(),
                    source,
                }
            }
        })?;

        match FileStamp::read_regular(&self.destination) {
            Ok(stamp) if stamp.as_ref() == Some(expected) => {}
            Ok(_) => {
                self.rollback_publication();
                return Err(MigrationError::DestinationChanged);
            }
            Err(error) => {
                self.rollback_publication();
                return Err(error);
            }
        }

        if let Err(source) = sync_parent_directory(&self.destination) {
            self.rollback_publication();
            return Err(MigrationError::Io {
                operation: "syncing destination directory for",
                path: self.destination.clone(),
                source,
            });
        }

        if let Err(source) = fs::remove_file(&self.temporary) {
            self.rollback_publication();
            return Err(MigrationError::Io {
                operation: "removing temporary destination for",
                path: self.destination.clone(),
                source,
            });
        }
        let _ = sync_parent_directory(&self.destination);
        Ok(())
    }

    fn rollback_publication(&self) {
        let _ = fs::remove_file(&self.destination);
        let _ = sync_parent_directory(&self.destination);
    }
}

impl Drop for DestinationGuard {
    fn drop(&mut self) {
        drop(self.file.take());
        let _ = fs::remove_file(&self.temporary);
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty());
    File::open(parent.unwrap_or_else(|| Path::new(".")))?.sync_all()
}

#[cfg(not(unix))]
fn sync_parent_directory(_: &Path) -> io::Result<()> {
    Ok(())
}
