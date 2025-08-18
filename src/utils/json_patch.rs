use crate::error::{FeoxError, Result};
use json_patch::Patch;
use serde_json::Value;

pub fn apply_json_patch(document: &[u8], patch: &[u8]) -> Result<Vec<u8>> {
    let mut doc: Value = serde_json::from_slice(document)
        .map_err(|e| FeoxError::JsonPatchError(format!("Invalid JSON document: {}", e)))?;

    let patch: Patch = serde_json::from_slice(patch)
        .map_err(|e| FeoxError::JsonPatchError(format!("Invalid patch: {}", e)))?;

    json_patch::patch(&mut doc, &patch)
        .map_err(|e| FeoxError::JsonPatchError(format!("Patch failed: {}", e)))?;

    serde_json::to_vec(&doc)
        .map_err(|e| FeoxError::JsonPatchError(format!("Serialization failed: {}", e)))
}
