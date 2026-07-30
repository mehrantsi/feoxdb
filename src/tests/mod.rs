// Test modules for FeOxDB

#[cfg(test)]
pub mod store;

#[cfg(test)]
pub mod cache_tests;

#[cfg(test)]
pub mod record_tests;

#[cfg(test)]
pub mod free_space_tests;

#[cfg(test)]
pub mod write_buffer_tests;

#[cfg(test)]
pub mod io_tests;

#[cfg(test)]
pub mod metadata_tests;

#[cfg(test)]
pub mod migration_tests;

#[cfg(test)]
pub mod json_patch_tests;

#[cfg(test)]
pub mod hash_tests;

#[cfg(test)]
pub mod stats_tests;

#[cfg(test)]
pub mod allocator_tests;

#[cfg(test)]
pub mod persistence_tests;

#[cfg(test)]
pub mod ttl_tests;

#[cfg(test)]
pub mod stale_extent_tests;

#[cfg(test)]
pub mod seq_token_tests;
