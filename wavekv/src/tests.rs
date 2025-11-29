use crate::node::Node;
use crate::sync::SyncManager;
use crate::types::{compare_entries, Entry, Metadata};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_dynamic_membership() {
    let store = Node::new(1, vec![2, 3]);

    // Initial peers (excluding self)
    let peers = store.read().get_peers();
    assert_eq!(peers.len(), 2);
    assert!(peers.contains(&2));
    assert!(peers.contains(&3));

    // All nodes should include self
    let all_nodes = store.read().get_all_nodes();
    assert_eq!(all_nodes.len(), 3); // self + 2 peers
    assert!(all_nodes.contains(&1));

    // Check peer state
    let peer2_state = store.read().get_peer_state(2).unwrap();
    assert_eq!(peer2_state.local_ack, 0);
    assert_eq!(peer2_state.peer_ack, 0);

    // Add a new peer
    assert!(store.write().add_peer(4).unwrap());
    let peers = store.read().get_peers();
    assert_eq!(peers.len(), 3);
    assert!(peers.contains(&4));

    // Try to add duplicate peer (should fail)
    assert!(!store.write().add_peer(4).unwrap());
    assert_eq!(store.read().get_peers().len(), 3);

    // Try to add self as peer (should fail)
    assert!(!store.write().add_peer(1).unwrap());
    assert_eq!(store.read().get_peers().len(), 3);

    // Remove a peer
    assert!(store.write().remove_peer(3).unwrap());
    let peers = store.read().get_peers();
    assert_eq!(peers.len(), 2);
    assert!(!peers.contains(&3));

    // Try to remove self (should fail)
    assert!(!store.write().remove_peer(1).unwrap());

    // Try to remove non-existent peer (should fail)
    assert!(!store.write().remove_peer(99).unwrap());

    // Get all nodes (should include self)
    let all_nodes = store.read().get_all_nodes();
    assert_eq!(all_nodes.len(), 3); // node 1, 2, 4
    assert!(all_nodes.contains(&1));
    assert!(all_nodes.contains(&2));
    assert!(all_nodes.contains(&4));
}

#[tokio::test]
async fn test_prefix_scan() {
    let store = Node::new(1, vec![]);

    // Insert test data with different prefixes
    store
        .write()
        .put("user:1001:name".to_string(), b"Alice".to_vec())
        .unwrap();
    store
        .write()
        .put("user:1001:age".to_string(), b"25".to_vec())
        .unwrap();
    store
        .write()
        .put("user:1002:name".to_string(), b"Bob".to_vec())
        .unwrap();
    store
        .write()
        .put("user:1002:age".to_string(), b"30".to_vec())
        .unwrap();
    store
        .write()
        .put("product:2001:name".to_string(), b"Laptop".to_vec())
        .unwrap();
    store
        .write()
        .put("product:2001:price".to_string(), b"1000".to_vec())
        .unwrap();

    // Test prefix scan for user:1001
    let user_1001 = store.read().get_by_prefix("user:1001:");
    assert_eq!(user_1001.len(), 2);
    assert_eq!(
        user_1001.get("user:1001:name").unwrap().value,
        Some(b"Alice".to_vec())
    );

    // Test prefix scan for all users
    let all_users = store.read().get_by_prefix("user:");
    assert_eq!(all_users.len(), 4);

    // Test prefix scan for products
    let products = store.read().get_by_prefix("product:");
    assert_eq!(products.len(), 2);

    // Test non-existent prefix
    let empty = store.read().get_by_prefix("order:");
    assert_eq!(empty.len(), 0);
}

#[tokio::test]
async fn test_basic_put_get() {
    let store = Node::new(1, vec![]);

    // Test put
    let item = store
        .write()
        .put("key1".to_string(), b"value1".to_vec())
        .unwrap();
    assert_eq!(item.key, "key1");
    assert_eq!(item.value, Some(b"value1".to_vec()));

    // Test get
    let retrieved = store.read().get("key1");
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.key, "key1");
    assert_eq!(retrieved.value, Some(b"value1".to_vec()));

    // Test non-existent key
    let missing = store.read().get("nonexistent");
    assert!(missing.is_none());
}

#[tokio::test]
async fn test_multiple_puts_same_key() {
    let store = Node::new(1, vec![]);

    // First put
    store
        .write()
        .put("key1".to_string(), b"value1".to_vec())
        .unwrap();

    // Second put (should overwrite)
    store
        .write()
        .put("key1".to_string(), b"value2".to_vec())
        .unwrap();

    let retrieved = store.read().get("key1").unwrap();
    assert_eq!(retrieved.value, Some(b"value2".to_vec()));
}

#[tokio::test]
async fn test_delete() {
    let store = Node::new(1, vec![]);

    // Put then delete
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store.write().delete("key1".to_string()).unwrap();

    // Should return None after delete
    let result = store.read().get("key1");
    assert!(result.is_none());
}

#[tokio::test]
async fn test_item_comparison() {
    let entry1 = Entry::new_put(
        Metadata::new(1, 1, 1000),
        "key1".to_string(),
        b"value1".to_vec(),
    );
    let entry2 = Entry::new_put(
        Metadata::new(2, 2, 2000),
        "key1".to_string(),
        b"value2".to_vec(),
    );

    // entry2 has later timestamp, should be Greater
    assert_eq!(compare_entries(&entry1, &entry2), std::cmp::Ordering::Less);
    assert_eq!(
        compare_entries(&entry2, &entry1),
        std::cmp::Ordering::Greater
    );
}

#[tokio::test]
async fn test_sync_between_stores() {
    let store1 = Arc::new(Node::new(1, vec![2]));
    let store2 = Arc::new(Node::new(2, vec![1]));

    // Store1 creates an item
    let item1 = store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();

    // Sync to store2
    let updated = store2.write().sync(item1.clone()).unwrap();
    assert!(updated);

    // Verify store2 has the item
    let retrieved = store2.read().get("key1");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_delete_propagation() {
    let store1 = Arc::new(Node::new(1, vec![2]));
    let store2 = Arc::new(Node::new(2, vec![1]));

    // Both stores have the same key initially
    store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store2
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();

    // Small delay to ensure different timestamp
    sleep(Duration::from_millis(2)).await;

    // Store1 deletes the key
    store1.write().delete("key1".to_string()).unwrap();

    // Get the tombstone from store1
    let tombstone = store1.read().get_including_tombstones("key1").unwrap();
    assert!(tombstone.value.is_none());

    // Sync tombstone to store2
    store2.write().sync(tombstone).unwrap();

    // Verify store2 also shows the key as deleted
    let result = store2.read().get("key1");
    assert!(result.is_none());
}

#[tokio::test]
async fn test_concurrent_writes_resolution() {
    let store1 = Arc::new(Node::new(1, vec![]));
    let store2 = Arc::new(Node::new(2, vec![]));

    // Simulate concurrent writes with different timestamps
    sleep(Duration::from_millis(10)).await;
    let _item1 = store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();

    sleep(Duration::from_millis(10)).await;
    let item2 = store2
        .write()
        .put("key1".to_string(), "value2".to_string())
        .unwrap();

    // item2 has later timestamp, should win
    store1.write().sync(item2.clone()).unwrap();

    let final_value = store1.read().get("key1").unwrap();
    assert_eq!(final_value.value, Some(b"value2".to_vec()));
}

#[tokio::test]
async fn test_lww_equal_timestamp_tombstone_wins_compare() {
    // Two entries with the same timestamp; tombstone should win
    let put = Entry::new_put(Metadata::new(1, 1, 1234), "k".into(), b"v".to_vec());
    let del = Entry::new_delete(Metadata::new(2, 2, 1234), "k".into());
    assert_eq!(compare_entries(&put, &del), std::cmp::Ordering::Less);
    assert_eq!(compare_entries(&del, &put), std::cmp::Ordering::Greater);
}

#[tokio::test]
async fn test_lww_equal_timestamp_node_id_tie_non_tombstone() {
    // Two non-tombstone entries with equal timestamp; higher node_id wins
    let a = Entry::new_put(Metadata::new(1, 1, 2000), "k".into(), b"a".to_vec());
    let b = Entry::new_put(Metadata::new(2, 2, 2000), "k".into(), b"b".to_vec());
    assert_eq!(compare_entries(&a, &b), std::cmp::Ordering::Less);
    assert_eq!(compare_entries(&b, &a), std::cmp::Ordering::Greater);
}

#[tokio::test]
async fn test_sync_equal_timestamp_tombstone_wins() {
    let store = Node::new(1, vec![]);

    // Start with a put
    let put = Entry::new_put(Metadata::new(1, 1, 1000), "k".into(), b"v".to_vec());
    store.write().sync(put).unwrap();

    // Sync a tombstone with the same timestamp but higher node_id
    let del = Entry::new_delete(Metadata::new(2, 2, 1000), "k".into());
    let updated = store.write().sync(del).unwrap();
    assert!(updated);

    // The tombstone should win
    assert!(store.read().get("k").is_none());
}

#[tokio::test]
async fn test_wal_recovery_equal_timestamp_tombstone_wins() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path();

    // Create store with WAL
    let store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

    // Write some data
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    drop(store);

    // Recover from WAL
    let recovered = Node::new_with_persistence(1, vec![], wal_path).unwrap();
    let item = recovered.read().get("key1").unwrap();
    assert_eq!(item.value, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn test_tombstone_cleanup() {
    let store = Node::new(1, vec![]);

    // Put and delete a key to create a tombstone
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store.write().delete("key1".to_string()).unwrap();

    // Verify tombstone exists
    assert!(store.read().get_including_tombstones("key1").is_some());
    assert!(store
        .read()
        .get_including_tombstones("key1")
        .unwrap()
        .value
        .is_none());

    // Sleep longer than tombstone TTL
    sleep(Duration::from_secs(2)).await;

    // Clean up expired tombstones
    let removed = store
        .write()
        .cleanup_expired_tombstones(Duration::from_secs(100))
        .unwrap();
    assert_eq!(removed, 0);
    let removed = store
        .write()
        .cleanup_expired_tombstones(Duration::from_secs(0))
        .unwrap();
    assert_eq!(removed, 1);
}

#[tokio::test]
async fn test_log_exchange() {
    use crate::sync::{ExchangeInterface, SyncMessage, SyncResponse};
    use anyhow::Result;

    // Create a simple mock network handler
    struct TestNetworkHandler {
        target_store: Arc<Node>,
    }

    impl ExchangeInterface for TestNetworkHandler {
        async fn sync_to(
            &self,
            _node: Arc<Node>,
            _peer: u32,
            msg: SyncMessage,
        ) -> Result<SyncResponse> {
            let store = self.target_store.clone();
            store.write().apply_pushed_entries(msg.clone())?;
            // Determine what to send back
            let (entries, is_snapshot) = match store.read().get_peer_missing_logs(&msg.sender_ack) {
                Some(entries) => (entries, false),
                None => (store.read().kv_to_log_entries(), true),
            };
            let progress = store.read().get_local_ack();
            let peer_id = store.read().id;
            Ok(SyncResponse {
                entries,
                is_snapshot,
                progress,
                peer_id,
            })
        }
    }

    let store1 = Arc::new(Node::new(1, vec![2]));
    let store2 = Arc::new(Node::new(2, vec![1]));

    // Store1 writes some data
    store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store1
        .write()
        .put("key2".to_string(), b"value2".to_vec())
        .unwrap();

    // Create sync managers
    let network1 = Arc::new(TestNetworkHandler {
        target_store: store2.clone(),
    });
    let sync1 = Arc::new(SyncManager::new(store1.clone(), network1));

    // Manually trigger log exchange
    let msg = SyncMessage {
        sender_id: store1.read().id,
        sender_ack: store1.read().get_local_ack(),
        entries: vec![], // Store1 has entries but we're testing the response
    };
    let response = sync1.handle_sync(msg).unwrap();

    // Verify response contains entries
    match response {
        SyncResponse {
            entries,
            is_snapshot,
            progress,
            peer_id: _,
        } => {
            // Store1 has progress (itself), so this should be incremental
            assert!(!is_snapshot);
            assert_eq!(entries.len(), 0); // Store2 has no data to send back
            assert!(progress.contains_key(&2)); // Store2 should report its progress
        }
    }
}

#[tokio::test]
async fn test_wal_persistence() {
    use tempfile::TempDir;

    // Create a temporary directory for WAL files
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path();

    // Create store with persistence
    {
        let store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

        // Write some data
        store
            .write()
            .put("key1".to_string(), "value1".to_string())
            .unwrap();
        store
            .write()
            .put("key2".to_string(), b"value2".to_vec())
            .unwrap();
        store.write().delete("key1".to_string()).unwrap();
    } // Drop store to close WAL

    // Recreate store from same WAL path
    {
        let recovered_store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

        // Verify data
        let key1 = recovered_store.read().get("key1");
        assert!(key1.is_none()); // Was deleted

        let key2 = recovered_store.read().get("key2");
        assert!(key2.is_some());
        assert_eq!(key2.unwrap().value, Some(b"value2".to_vec()));
    }
}

#[tokio::test]
async fn test_origin_seq_preservation() {
    // Test that seq and node_id are preserved end-to-end through sync
    let store1 = Arc::new(Node::new(1, vec![2]));
    let store2 = Arc::new(Node::new(2, vec![1]));

    // Store1 creates entries
    let entry1 = store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    assert_eq!(entry1.meta.seq, 1);

    let entry2 = store1
        .write()
        .put("key2".to_string(), "value2".to_string())
        .unwrap();
    assert_eq!(entry2.meta.seq, 2);

    // Sync to store2
    store2.write().sync(entry1.clone()).unwrap();
    store2.write().sync(entry2.clone()).unwrap();

    // Verify seq and node_id preserved
    let retrieved1 = store2.read().get_including_tombstones("key1").unwrap();
    assert_eq!(retrieved1.meta.seq, 1);
    assert_eq!(retrieved1.meta.node, 1);

    let retrieved2 = store2.read().get_including_tombstones("key2").unwrap();
    assert_eq!(retrieved2.meta.seq, 2);
    assert_eq!(retrieved2.meta.node, 1);
}

#[tokio::test]
async fn test_wal_recovery_lww() {
    use tempfile::TempDir;

    // Create a temporary directory for WAL files
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path();

    {
        let store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

        // Simply write two values - second write should persist
        store
            .write()
            .put("key1".to_string(), "value1".to_string())
            .unwrap();
        sleep(Duration::from_millis(10)).await; // Ensure different timestamps
        store
            .write()
            .put("key1".to_string(), "value2".to_string())
            .unwrap();
    }

    // Recover - should get the last written value
    let recovered_store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

    let item = recovered_store.read().get("key1").unwrap();
    assert_eq!(item.value, Some(b"value2".to_vec()));
}

#[tokio::test]
async fn test_get_missing_log_entries() {
    let store = Node::new(1, vec![2]);

    // Add some entries
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store
        .write()
        .put("key2".to_string(), "value2".to_string())
        .unwrap();
    store
        .write()
        .put("key3".to_string(), b"value3".to_vec())
        .unwrap();

    // Simulate peer progress
    let mut peer_progress = std::collections::HashMap::new();
    peer_progress.insert(1, 1_u64); // Peer has up to seq 1 from node 1

    // Get missing entries
    let missing = store.read().get_peer_missing_logs(&peer_progress).unwrap();

    // Should have 2 missing entries (seq 2 and 3)
    assert_eq!(missing.len(), 2);
}

#[tokio::test]
async fn test_kv_to_log_entries() {
    let store = Node::new(1, vec![]);

    // Add some data
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store
        .write()
        .put("key2".to_string(), "value2".to_string())
        .unwrap();
    store.write().delete("key3".to_string()).unwrap();

    // Convert to log entries
    let entries = store.read().kv_to_log_entries();

    // Should have 3 entries
    assert_eq!(entries.len(), 3);
}
