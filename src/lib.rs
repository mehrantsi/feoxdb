//! # FeOxDB - High-Performance Embedded Key-Value Store
//!
// Copyright 2025 Mehran Toosi
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! FeOxDB is an ultra-fast embedded key-value database designed for sub-microsecond latency.
//!
//! ## Performance
//!
//! View the latest benchmark results at: <https://feoxdb.com/benchmarks.html>
//!
//! ## Features
//!
//! - **Ultra-Low Latency**: <200ns GET operations, <700ns INSERT operations
//! - **Lock-Free Operations**: Uses SCC HashMap and SkipList for concurrent access
//! - **io_uring Support** (Linux): Kernel-bypass I/O for maximum throughput with minimal syscalls
//! - **Flexible Storage**: Memory-only or persistent modes with async I/O
//! - **JSON Patch Support**: RFC 6902 compliant partial updates for JSON values
//! - **Atomic Operations**: Compare-and-swap (CAS) and atomic counters
//! - **CLOCK Cache**: Efficient cache eviction algorithm
//! - **Write Buffering**: Batched writes with sharded buffers to reduce contention
//! - **Statistics**: Real-time performance metrics and monitoring
//!
//! ## ACID Properties and Durability
//!
//! FeOxDB provides ACI properties with relaxed durability:
//!
//! - **Atomicity**: ✅ Individual operations are atomic via Arc-wrapped records
//! - **Consistency**: ✅ Timestamp-based conflict resolution ensures consistency  
//! - **Isolation**: ✅ Lock-free reads and sharded writes provide operation isolation
//! - **Durability**: ⚠️ Write-behind logging with bounded data loss window
//!
//! ### Durability Trade-offs
//!
//! FeOxDB trades full durability for extreme performance:
//! - **Write-behind buffering**: Flushes every 100ms or when buffers fill (1024 entries or 16MB per shard)
//! - **Worst-case data loss**:
//!   - **Time window**: `100ms + 16MB / 4KB_random_write_QD1_throughput`
//!   - **Data at risk**: `16MB × num_shards (num_shards = num_cpus / 2)` (e.g., 64MB for 4 shards, 128MB for 8 shards)
//!   - Workers write in parallel, so time doesn't multiply with shards
//!   - Example (50MB/s 4KB random QD1): 420ms window, up to 64MB at risk (4 shards)
//!   - Example (200MB/s 4KB random QD1): 180ms window, up to 64MB at risk (4 shards)
//! - **Memory-only mode**: No durability, maximum performance
//! - **Explicit flush**: Call `store.flush()` to synchronously write all buffered data (blocks until fsync completes)
//!
//! ## Quick Start
//!
//! ### Memory-Only Mode (Fastest)
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! // Create an in-memory store
//! let store = FeoxStore::new(None)?;
//!
//! // Insert a key-value pair
//! store.insert(b"key", b"value")?;
//!
//! // Retrieve the value
//! let value = store.get(b"key")?;
//! assert_eq!(value, b"value");
//!
//! // Delete the key
//! store.delete(b"key")?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Persistent Mode
//! ```no_run
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! // Create a persistent store
//! let store = FeoxStore::new(Some("/path/to/data.feox".to_string()))?;
//!
//! // Operations are automatically persisted
//! store.insert(b"persistent_key", b"persistent_value")?;
//!
//! // Flush to disk
//! store.flush();
//! # Ok(())
//! # }
//! ```
//!
//! ### Using the Builder Pattern
//! ```no_run
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::builder()
//!     .device_path("/path/to/data.feox")
//!     .file_size(5 * 1024 * 1024 * 1024)  // 5GB initial file size (default: 1GB)
//!     .max_memory(1024 * 1024 * 1024)  // 1GB limit
//!     .hash_bits(20)  // 1M hash buckets
//!     .enable_ttl(true)  // Enable TTL support (default: false)
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Range Queries and Store Operations
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::new(None)?;
//!
//! // Insert sorted keys
//! store.insert(b"user:001", b"Mehran")?;
//! store.insert(b"user:002", b"Bob")?;
//! store.insert(b"user:003", b"Charlie")?;
//!
//! // Range query (both start and end are inclusive)
//! let results = store.range_query(b"user:001", b"user:003", 10)?;
//! assert_eq!(results.len(), 3);  // Returns user:001, user:002, user:003
//!
//! // Check existence
//! assert!(store.contains_key(b"user:001"));
//!
//! // Get store metrics
//! assert_eq!(store.len(), 3);
//! println!("Memory usage: {} bytes", store.memory_usage());
//! # Ok(())
//! # }
//! ```
//!
//! ### JSON Patch Operations (RFC 6902)
//! ```no_run
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::new(None)?;
//!
//! // Insert a JSON document
//! let initial_json = br#"{
//!     "name": "Mehran",
//!     "age": 30,
//!     "scores": [85, 90, 95]
//! }"#;
//! store.insert(b"user:123", initial_json)?;
//!
//! // Apply a JSON Patch to modify specific fields
//! let patch = br#"[
//!     {"op": "replace", "path": "/age", "value": 31},
//!     {"op": "add", "path": "/email", "value": "mehran@example.com"},
//!     {"op": "add", "path": "/scores/-", "value": 100}
//! ]"#;
//!
//! store.json_patch(b"user:123", patch)?;
//!
//! // The document is now updated with the patches applied
//! let updated = store.get(b"user:123")?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Time-To-Live (TTL) Support
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! // Enable TTL feature via builder
//! let store = FeoxStore::builder()
//!     .enable_ttl(true)
//!     .build()?;
//!
//! // Set key to expire after 60 seconds
//! store.insert_with_ttl(b"session:123", b"user_session", 60)?;
//!
//! // Check remaining TTL
//! if let Some(ttl) = store.get_ttl(b"session:123")? {
//!     println!("Session expires in {} seconds", ttl);
//! }
//!
//! // Extend TTL to 120 seconds
//! store.update_ttl(b"session:123", 120)?;
//!
//! // Remove TTL (make permanent)
//! store.persist(b"session:123")?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Atomic Counter Operations
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::new(None)?;
//!
//! // Initialize a counter (must be 8-byte i64 value)
//! let zero: i64 = 0;
//! store.insert(b"counter:visits", &zero.to_le_bytes())?;
//!
//! // Increment atomically
//! let new_value = store.atomic_increment(b"counter:visits", 1)?;
//! assert_eq!(new_value, 1);
//!
//! // Increment by 5
//! let new_value = store.atomic_increment(b"counter:visits", 5)?;
//! assert_eq!(new_value, 6);
//!
//! // Decrement by 2  
//! let new_value = store.atomic_increment(b"counter:visits", -2)?;
//! assert_eq!(new_value, 4);
//! # Ok(())
//! # }
//! ```
//!
//! ### Compare-and-Swap (CAS) Operations
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::new(None)?;
//!
//! // Insert initial configuration
//! store.insert(b"config:version", b"v1.0")?;
//!
//! // Atomically update only if value matches expected
//! let swapped = store.compare_and_swap(b"config:version", b"v1.0", b"v2.0")?;
//! assert!(swapped); // Returns true if swap succeeded
//!
//! // This CAS will fail because current value is now "v2.0"
//! let swapped = store.compare_and_swap(b"config:version", b"v1.0", b"v3.0")?;
//! assert!(!swapped); // Returns false if current value didn't match
//!
//! // CAS enables optimistic concurrency control for safe updates
//! // Multiple threads can attempt CAS - only one will succeed
//! # Ok(())
//! # }
//! ```
//!
//! ## Timestamps and Consistency
//!
//! FeOxDB uses timestamps for conflict resolution and consistency:
//!
//! ```rust
//! # use feoxdb::FeoxStore;
//! # fn main() -> feoxdb::Result<()> {
//! # let store = FeoxStore::new(None)?;
//! // Insert with explicit timestamp
//! store.insert_with_timestamp(b"key", b"value_v1", Some(100))?;
//!
//! // Update with higher timestamp succeeds
//! store.insert_with_timestamp(b"key", b"value_v2", Some(200))?;
//!
//! // Update with lower timestamp fails
//! let result = store.insert_with_timestamp(b"key", b"value_v3", Some(150));
//! assert!(result.is_err());  // OlderTimestamp error
//! # Ok(())
//! # }
//! ```
//!
//! ## Performance and Benchmarking
//!
//! Run benchmarks to verify performance on your hardware:
//!
//! ```bash
//! # Criterion benchmarks for detailed latency analysis
//! cargo bench
//!
//! # Deterministic test for reproducible results
//! cargo run --release --example deterministic_test 100000 100
//! ```
//!
//! Typical results on M3 Max:
//!
//! | Operation | Latency (Memory Mode) | Throughput |
//! |-----------|----------------------|------------|
//! | GET       | ~180-205ns | 11-14M ops/sec |
//! | INSERT    | ~630-970ns | 1.0-1.8M ops/sec |
//! | DELETE    | ~250ns | 4M ops/sec |
//! | Mixed (80/20) | ~280ns | 3.8M ops/sec |
//!
//! ## Architecture Overview
//!
//! FeOxDB uses a lock-free, multi-tier architecture optimized for modern multi-core CPUs:
//!
//! ### Lock-Free Data Structures
//! - **Hash Table**: SCC HashMap provides fine-grained locking optimized for high concurrency
//! - **Ordered Index**: Crossbeam SkipList enables lock-free sorted traversal
//! - **Atomic Operations**: All metadata updates use atomic primitives in hot path
//!
//! ### Async Write-Behind Logging
//! - **Sharded Write Buffers**: Multiple write buffers with thread-consistent assignment to reduce contention
//! - **Batched Writes**: Writes are buffered and flushed asynchronously in batches
//! - **io_uring Integration**: On Linux, uses kernel-bypass I/O for minimal syscall overhead
//! - **Write Coalescing**: Multiple updates to the same key are automatically coalesced
//!
//! ### Storage Tiers
//! 1. **In-Memory Layer**: Hot data in SCC HashMap with O(1) access
//! 2. **Write Buffer**: Sharded buffers with thread-local affinity for write batching
//! 3. **Persistent Storage**: Sector-aligned async I/O with write-ahead logging
//! 4. **Cache Layer**: CLOCK algorithm keeps frequently accessed data in memory
//!
//! ### Free Space Management
//!
//! FeOxDB uses a dual RB-tree structure for managing disk space:
//! - One tree sorted by size for best-fit allocation
//! - One tree sorted by address for efficient merging
//! - Automatic coalescing of adjacent free blocks
//! - O(log n) allocation and deallocation
//! - Zero external fragmentation through immediate merging
//!
//! ## Thread Safety
//!
//! All operations are thread-safe and can be called concurrently:
//!
//! ```rust
//! # use feoxdb::FeoxStore;
//! # use std::sync::Arc;
//! # use std::thread;
//! # fn main() -> feoxdb::Result<()> {
//! let store = Arc::new(FeoxStore::new(None)?);
//! let mut handles = vec![];
//!
//! for i in 0..10 {
//!     let store_clone = Arc::clone(&store);
//!     handles.push(thread::spawn(move || {
//!         let key = format!("key_{}", i);
//!         store_clone.insert(key.as_bytes(), b"value").unwrap();
//!     }));
//! }
//!
//! for handle in handles {
//!     handle.join().unwrap();
//! }
//! # Ok(())
//! # }
//! ```

// Configure global allocator
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub mod constants;
pub mod core;
pub mod error;
pub mod stats;
pub mod storage;
pub mod utils;

pub use bytes::Bytes;
pub use core::store::{FeoxStore, StoreBuilder, StoreConfig};
pub use error::{FeoxError, Result};
pub use stats::Statistics;

#[cfg(test)]
mod tests;
