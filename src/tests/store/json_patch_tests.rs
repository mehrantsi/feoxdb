use crate::core::store::FeoxStore;

#[test]
fn test_json_patch_basic() {
    let store = FeoxStore::new(None).unwrap();

    let key = b"json_doc";
    let doc = br#"{"name":"Alice","age":30}"#;
    store.insert(key, doc).unwrap();

    let patch = br#"[{"op":"replace","path":"/age","value":31}]"#;
    store.json_patch(key, patch).unwrap();

    let updated = store.get(key).unwrap();
    let updated_str = String::from_utf8_lossy(&updated);

    // Verify the specific changes were applied
    assert!(
        updated_str.contains(r#""age":31"#),
        "Age should be updated to 31"
    );
    assert!(
        updated_str.contains(r#""name":"Alice""#),
        "Name should remain unchanged"
    );
    assert!(
        !updated_str.contains(r#""age":30"#),
        "Old age value should be gone"
    );
}

#[test]
fn test_json_patch_on_non_json() {
    let store = FeoxStore::new(None).unwrap();

    store.insert(b"not_json", b"plain text").unwrap();

    let patch = br#"[{"op":"add","path":"/foo","value":"bar"}]"#;
    let result = store.json_patch(b"not_json", patch);
    assert!(result.is_err());
}
