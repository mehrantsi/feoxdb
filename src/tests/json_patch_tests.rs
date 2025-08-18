use crate::utils::json_patch::apply_json_patch;
use serde_json::json;

#[test]
fn test_rfc6902_json_patch() {
    let doc = json!({
        "name": "Alice",
        "age": 30,
        "scores": [85, 90],
        "address": {
            "city": "SF",
            "zip": "94105"
        }
    });

    let patch = json!([
        {"op": "replace", "path": "/age", "value": 31},
        {"op": "add", "path": "/email", "value": "alice@example.com"},
        {"op": "add", "path": "/scores/-", "value": 95},
        {"op": "replace", "path": "/address/city", "value": "Seattle"},
        {"op": "remove", "path": "/address/zip"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();

    assert_eq!(result_json["age"], 31);
    assert_eq!(result_json["email"], "alice@example.com");
    assert_eq!(result_json["scores"], json!([85, 90, 95]));
    assert_eq!(result_json["address"]["city"], "Seattle");
    assert!(result_json["address"].get("zip").is_none());
}

#[test]
fn test_json_patch_invalid_json() {
    let invalid_doc = b"not json";
    let patch = json!([{"op": "replace", "path": "/foo", "value": "bar"}]);

    let result = apply_json_patch(invalid_doc, &serde_json::to_vec(&patch).unwrap());
    assert!(result.is_err());
}

#[test]
fn test_json_patch_invalid_patch() {
    let doc = json!({"foo": "bar"});
    let invalid_patch = b"not a patch";

    let result = apply_json_patch(&serde_json::to_vec(&doc).unwrap(), invalid_patch);
    assert!(result.is_err());
}

#[test]
fn test_json_patch_failed_test_operation() {
    let doc = json!({"foo": "bar"});
    let patch = json!([
        {"op": "test", "path": "/foo", "value": "baz"},
        {"op": "replace", "path": "/foo", "value": "new"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    );

    // Should fail because test operation fails (foo != baz)
    assert!(result.is_err());
}

#[test]
fn test_json_patch_move_operation() {
    let doc = json!({
        "foo": "bar",
        "baz": "qux"
    });

    let patch = json!([
        {"op": "move", "from": "/foo", "path": "/moved"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert!(result_json.get("foo").is_none());
    assert_eq!(result_json["moved"], "bar");
    assert_eq!(result_json["baz"], "qux");
}

#[test]
fn test_json_patch_copy_operation() {
    let doc = json!({
        "foo": {"bar": "baz"},
        "qux": "value"
    });

    let patch = json!([
        {"op": "copy", "from": "/foo", "path": "/copied"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json["foo"], json!({"bar": "baz"}));
    assert_eq!(result_json["copied"], json!({"bar": "baz"}));
    assert_eq!(result_json["qux"], "value");
}

#[test]
fn test_json_patch_add_to_array() {
    let doc = json!({
        "items": [1, 2, 3]
    });

    let patch = json!([
        {"op": "add", "path": "/items/1", "value": 99}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json["items"], json!([1, 99, 2, 3]));
}

#[test]
fn test_json_patch_remove_from_array() {
    let doc = json!({
        "items": [1, 2, 3, 4]
    });

    let patch = json!([
        {"op": "remove", "path": "/items/1"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json["items"], json!([1, 3, 4]));
}

#[test]
fn test_json_patch_nested_operations() {
    let doc = json!({
        "a": {
            "b": {
                "c": {
                    "d": "deep_value"
                }
            }
        }
    });

    let patch = json!([
        {"op": "replace", "path": "/a/b/c/d", "value": "new_deep_value"},
        {"op": "add", "path": "/a/b/c/e", "value": "another_value"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json["a"]["b"]["c"]["d"], "new_deep_value");
    assert_eq!(result_json["a"]["b"]["c"]["e"], "another_value");
}

#[test]
fn test_json_patch_nonexistent_path() {
    let doc = json!({"foo": "bar"});
    let patch = json!([
        {"op": "replace", "path": "/nonexistent", "value": "value"}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    );

    assert!(result.is_err());
}

#[test]
fn test_json_patch_empty_patch() {
    let doc = json!({"foo": "bar"});
    let patch = json!([]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json, doc); // Should be unchanged
}

#[test]
fn test_json_patch_successful_test_operation() {
    let doc = json!({"foo": "bar", "baz": 42});
    let patch = json!([
        {"op": "test", "path": "/foo", "value": "bar"},
        {"op": "replace", "path": "/baz", "value": 43}
    ]);

    let result = apply_json_patch(
        &serde_json::to_vec(&doc).unwrap(),
        &serde_json::to_vec(&patch).unwrap(),
    )
    .unwrap();

    let result_json: serde_json::Value = serde_json::from_slice(&result).unwrap();
    assert_eq!(result_json["foo"], "bar");
    assert_eq!(result_json["baz"], 43);
}
