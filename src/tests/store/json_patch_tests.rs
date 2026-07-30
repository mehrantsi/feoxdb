use crate::core::store::FeoxStore;
use crate::error::FeoxError;
use crate::test_hooks::{gate, AFTER_JSON_PATCH_READ};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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

#[test]
fn json_patch_retries_against_the_current_generation() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"json", br#"{"a":0,"b":0}"#, Some(100))
        .unwrap();

    let patch_store = Arc::clone(&store);
    let patcher = thread::spawn(move || {
        patch_store.json_patch_with_timestamp(
            b"json",
            br#"[{"op":"replace","path":"/a","value":1}]"#,
            Some(300),
        )
    });
    let armed = session.arm_for_thread(AFTER_JSON_PATCH_READ, patcher.thread().id(), 1);
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));

    store
        .insert_with_timestamp(b"json", br#"{"a":0,"b":2}"#, Some(200))
        .unwrap();
    armed.release();
    patcher.join().unwrap().unwrap();

    let value: serde_json::Value = serde_json::from_slice(&store.get(b"json").unwrap()).unwrap();
    assert_eq!(value, serde_json::json!({"a": 1, "b": 2}));
}

#[test]
fn newer_delete_wins_over_an_in_flight_json_patch() {
    let session = gate::session();
    let store = Arc::new(FeoxStore::new(None).unwrap());
    store
        .insert_with_timestamp(b"json", br#"{"a":0}"#, Some(100))
        .unwrap();

    let patch_store = Arc::clone(&store);
    let patcher = thread::spawn(move || {
        patch_store.json_patch_with_timestamp(
            b"json",
            br#"[{"op":"replace","path":"/a","value":1}]"#,
            Some(150),
        )
    });
    let armed = session.arm_for_thread(AFTER_JSON_PATCH_READ, patcher.thread().id(), 1);
    assert!(armed.wait_for_arrivals(1, Duration::from_secs(5)));

    store.delete_with_timestamp(b"json", Some(200)).unwrap();
    store
        .insert_with_timestamp(b"json", br#"{"a":0}"#, Some(50))
        .unwrap();
    armed.release();

    assert!(matches!(
        patcher.join().unwrap(),
        Err(FeoxError::OlderTimestamp)
    ));
    assert_eq!(store.get(b"json").unwrap(), br#"{"a":0}"#);
}
