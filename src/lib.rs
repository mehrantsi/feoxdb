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
//! View the latest benchmark results at: <https://mehrantsi.github.io/FeOxDB/benchmarks.html>
//!
//! ## Features
//!
//! - **Ultra-Low Latency**: <300ns GET operations, <600ns INSERT operations
//! - **Lock-Free Operations**: Uses DashMap and SkipList for concurrent access
//! - **io_uring Support** (Linux): Kernel-bypass I/O for maximum throughput with minimal syscalls
//! - **Flexible Storage**: Memory-only or persistent modes with async I/O
//! - **JSON Patch Support**: RFC 6902 compliant partial updates for JSON values
//! - **Atomic Operations**: Increment/decrement counters atomically
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
//! - **Worst-case data loss**: `100ms + 16MB / 4KB_random_write_QD1_throughput`
//!   - Example (50MB/s SATA SSD): 100ms + 16MB / 50MB/s = 420ms
//!   - Example (200MB/s NVMe): 100ms + 16MB / 200MB/s = 180ms
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
//! ```rust
//! use feoxdb::FeoxStore;
//!
//! # fn main() -> feoxdb::Result<()> {
//! let store = FeoxStore::builder()
//!     .max_memory(1024 * 1024 * 1024)  // 1GB limit
//!     .hash_bits(20)  // 1M hash buckets
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
//!
//! // Verify the patch was applied (without using serde_json in doc test)
//! assert!(updated.len() > initial_json.len()); // Should be bigger after adding email
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
//! # Throughput test (100k operations, 100 byte values)
//! cargo run --release --example performance_test 100000 100
//!
//! # Deterministic test for reproducible results
//! cargo run --release --example deterministic_test 100000 100
//! ```
//!
//! Typical results on M3 Max:
//!
//! | Operation | Latency (Memory Mode) | Throughput |
//! |-----------|----------------------|------------|
//! | GET       | ~200-260ns | 2.1M ops/sec |
//! | INSERT    | ~700ns | 850K ops/sec |
//! | DELETE    | ~290ns | 1.1M ops/sec |
//! | Mixed (80/20) | ~290ns | 3.1M ops/sec |
//!
//! ## Architecture Overview
//!
//! FeOxDB uses a lock-free, multi-tier architecture optimized for modern multi-core CPUs:
//!
//! ### Lock-Free Data Structures
//! - **Hash Table**: DashMap provides lock-free concurrent hash map with sharded locks
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
//! 1. **In-Memory Layer**: Hot data in DashMap with O(1) access
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

pub mod constants;
pub mod core;
pub mod error;
pub mod stats;
pub mod storage;
pub mod utils;

pub use core::store::{FeoxStore, StoreBuilder, StoreConfig};
pub use error::{FeoxError, Result};
pub use stats::Statistics;

#[cfg(test)]
mod tests;
