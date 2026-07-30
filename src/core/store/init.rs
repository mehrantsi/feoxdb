use ahash::RandomState;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use scc::HashMap;
use std::fs::File;
use std::sync::Arc;

use crate::constants::*;
use crate::error::{FeoxError, Result};
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::metadata::Metadata;
use crate::storage::write_buffer::WriteBuffer;

use super::{FeoxStore, StoreConfig, VersionClock};

enum OpenMode {
    ReadWrite,
    ReadOnly(File),
    Fresh(File),
}

impl FeoxStore {
    /// Create a new FeoxStore with default configuration
    pub fn new(device_path: Option<String>) -> Result<Self> {
        let memory_only = device_path.is_none();
        let config = StoreConfig {
            hash_bits: DEFAULT_HASH_BITS,
            memory_only,
            enable_caching: !memory_only, // Disable caching for memory-only mode
            device_path,
            file_size: None,
            max_memory: Some(DEFAULT_MAX_MEMORY),
            enable_ttl: false,
            ttl_config: None,
        };
        Self::with_config_and_legacy_recovery(config, false)
    }

    /// Create a new FeoxStore with custom configuration
    pub fn with_config(config: StoreConfig) -> Result<Self> {
        Self::with_config_and_legacy_recovery(config, false)
    }

    pub(super) fn with_config_and_legacy_recovery(
        config: StoreConfig,
        allow_ambiguous_legacy_recovery: bool,
    ) -> Result<Self> {
        Self::with_config_and_open_mode(
            config,
            allow_ambiguous_legacy_recovery,
            OpenMode::ReadWrite,
        )
    }

    pub(super) fn with_config_for_migration_source(
        config: StoreConfig,
        allow_ambiguous_legacy_recovery: bool,
        file: File,
    ) -> Result<Self> {
        Self::with_config_and_open_mode(
            config,
            allow_ambiguous_legacy_recovery,
            OpenMode::ReadOnly(file),
        )
    }

    pub(super) fn with_config_for_migration_destination(
        config: StoreConfig,
        file: File,
    ) -> Result<Self> {
        Self::with_config_and_open_mode(config, false, OpenMode::Fresh(file))
    }

    fn with_config_and_open_mode(
        config: StoreConfig,
        allow_ambiguous_legacy_recovery: bool,
        open_mode: OpenMode,
    ) -> Result<Self> {
        let read_only = matches!(&open_mode, OpenMode::ReadOnly(_));
        // Initialize hash table with configured capacity
        let hasher = RandomState::new();
        let hash_table = HashMap::with_capacity_and_hasher(1 << config.hash_bits, hasher.clone());

        let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
        let metadata = Metadata::new();
        let format_version = metadata.version;
        let metadata = Arc::new(RwLock::new(metadata));
        let stats = Arc::new(Statistics::new());

        let cache = if config.enable_caching {
            Some(Arc::new(crate::core::cache::ClockCache::new(stats.clone())))
        } else {
            None
        };

        let mut store = Self {
            hash_table,
            tree: Arc::new(SkipMap::new()),
            stats: stats.clone(),
            version_clock: VersionClock::new(hasher),
            write_buffer: None,
            free_space: free_space.clone(),
            _metadata: metadata,
            format_version,
            fresh_device: false,
            allow_ambiguous_legacy_recovery,
            ambiguous_legacy_markers: 0,
            read_only,
            initialized: config.memory_only,
            memory_only: config.memory_only,
            enable_caching: config.enable_caching,
            max_memory: config.max_memory,
            cache,
            #[cfg(unix)]
            device_fd: None,
            device_size: 0,
            device_file: None,
            disk_io: None,
            ttl_sweeper: Arc::new(RwLock::new(None)),
            enable_ttl: config.enable_ttl,
        };

        if !config.memory_only {
            match open_mode {
                OpenMode::ReadWrite => {
                    store.open_device(&config.device_path, config.file_size)?;
                }
                OpenMode::ReadOnly(file) => {
                    store.open_device_read_only(file)?;
                }
                OpenMode::Fresh(file) => {
                    store.open_fresh_device(file, config.file_size)?;
                }
            }
            store.load_indexes()?;

            // Initialize write buffer for persistent mode
            if !read_only {
                let disk_io = store.disk_io.as_ref().ok_or(FeoxError::NoDevice)?;
                let mut write_buffer = WriteBuffer::new(
                    disk_io.clone(),
                    free_space,
                    stats.clone(),
                    store.format_version,
                );
                let num_workers = (num_cpus::get() / 2).max(1);
                write_buffer.start_workers(num_workers);
                store.write_buffer = Some(Arc::new(write_buffer));
            }
            store.initialized = true;
        }

        Ok(store)
    }
}
