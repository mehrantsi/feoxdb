use ahash::RandomState;
use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use scc::HashMap;
use std::sync::Arc;

use crate::constants::*;
use crate::error::Result;
use crate::stats::Statistics;
use crate::storage::free_space::FreeSpaceManager;
use crate::storage::metadata::Metadata;
use crate::storage::write_buffer::WriteBuffer;

use super::{FeoxStore, StoreConfig};

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
        let hash_table =
            HashMap::with_capacity_and_hasher(1 << config.hash_bits, RandomState::new());

        let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
        let metadata = Arc::new(RwLock::new(Metadata::new()));
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
            write_buffer: None,
            free_space: free_space.clone(),
            _metadata: metadata,
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
            store.open_device(&config.device_path, config.file_size)?;
            store.load_indexes()?;

            // Initialize write buffer for persistent mode
            if let Some(ref disk_io) = store.disk_io {
                let metadata_version = store._metadata.read().version;
                let mut write_buffer =
                    WriteBuffer::new(disk_io.clone(), free_space, stats.clone(), metadata_version);
                let num_workers = (num_cpus::get() / 2).max(1);
                write_buffer.start_workers(num_workers);
                store.write_buffer = Some(Arc::new(write_buffer));
            }
        }

        Ok(store)
    }

    /// Create a new FeoxStore with custom configuration
    pub fn with_config(config: StoreConfig) -> Result<Self> {
        // Initialize hash table with configured capacity
        let hash_table =
            HashMap::with_capacity_and_hasher(1 << config.hash_bits, RandomState::new());

        let free_space = Arc::new(RwLock::new(FreeSpaceManager::new()));
        let metadata = Arc::new(RwLock::new(Metadata::new()));
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
            write_buffer: None,
            free_space: free_space.clone(),
            _metadata: metadata,
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
            store.open_device(&config.device_path, config.file_size)?;
            store.load_indexes()?;

            // Initialize write buffer for persistent mode
            if let Some(ref disk_io) = store.disk_io {
                let metadata_version = store._metadata.read().version;
                let mut write_buffer =
                    WriteBuffer::new(disk_io.clone(), free_space, stats.clone(), metadata_version);
                let num_workers = (num_cpus::get() / 2).max(1);
                write_buffer.start_workers(num_workers);
                store.write_buffer = Some(Arc::new(write_buffer));
            }
        }

        Ok(store)
    }
}
