use crate::core::store::StoreBuilder;

#[test]
fn test_builder_configuration() {
    let store = StoreBuilder::new()
        .max_memory(2_000_000_000)
        .hash_bits(20)
        .enable_caching(false)
        .build()
        .unwrap();

    // Should work with custom config
    store.insert(b"test", b"value").unwrap();
    assert_eq!(store.get(b"test").unwrap(), b"value");
}

#[test]
fn test_builder_defaults() {
    let store = StoreBuilder::new().build().unwrap();

    // Should work with default config
    store.insert(b"key", b"value").unwrap();
    assert_eq!(store.get(b"key").unwrap(), b"value");
}
