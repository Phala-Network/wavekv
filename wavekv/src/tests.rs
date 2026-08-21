use crate::node::Node;
use crate::sync::SyncManager;
use crate::types::{compare_entries, Entry, Metadata, NodeId};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

/// A stable per-node identity, so a transport can implement `query_uuid` the way a real
/// embedder does. Tests that leave `query_uuid` at its default `None` disable the check
/// entirely and cannot observe an unstamped envelope.
fn uuid_for(node: NodeId) -> Vec<u8> {
    format!("uuid-of-node-{node}").into_bytes()
}

#[test]
fn a_panic_under_the_write_lock_does_not_wedge_the_node() {
    let store = Node::new(1, vec![]);
    store
        .write()
        .put("key".to_string(), b"value".to_vec())
        .unwrap();

    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = store.write();
        panic!("boom");
    }));
    assert!(panicked.is_err());
    assert_eq!(
        store.read().get("key").unwrap().value.as_deref(),
        Some(b"value".as_slice())
    );
    store
        .write()
        .put("key2".to_string(), b"value2".to_vec())
        .unwrap();
}

/// The peer's *server side*, entered where a real request enters it.
///
/// `SyncManager` is the receive-side boundary: `check_uuid` runs there, and so will
/// anything added to it later. A double that reaches past it into `NodeState` cannot
/// observe those guards — which is exactly how an envelope that was never stamped with
/// a `sender_uuid` went unnoticed. Transports in these tests therefore hold a
/// `PeerEndpoint` rather than a bare `Node`.
#[derive(Clone)]
pub(crate) struct PeerEndpoint {
    manager: Arc<SyncManager<Answering>>,
    node: Node,
}

/// The endpoint answers; it never initiates. Its `query_uuid` is populated so the
/// identity check is live, as it is for any real embedder that implements it.
#[derive(Clone)]
struct Answering(NodeId);

impl crate::sync::ExchangeInterface for Answering {
    fn uuid(&self) -> Vec<u8> {
        uuid_for(self.0)
    }

    fn query_uuid(&self, node_id: NodeId) -> Option<Vec<u8>> {
        Some(uuid_for(node_id))
    }

    async fn sync_to(
        &self,
        _node: &Node,
        _peer: NodeId,
        _msg: crate::sync::SyncMessage,
    ) -> anyhow::Result<crate::sync::SyncResponse> {
        anyhow::bail!("a peer endpoint answers requests; it never sends them")
    }
}

impl PeerEndpoint {
    pub(crate) fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        let node = Node::new(id, peers);
        Self {
            manager: Arc::new(SyncManager::new(node.clone(), Answering(id))),
            node,
        }
    }

    /// The peer's store, for seeding fixtures and asserting outcomes.
    pub(crate) fn node(&self) -> &Node {
        &self.node
    }

    pub(crate) fn handle_sync_v1(
        &self,
        msg: crate::sync::SyncMessage,
    ) -> anyhow::Result<crate::sync::SyncResponse> {
        self.manager.handle_sync(msg)
    }

    pub(crate) fn handle_envelope(
        &self,
        env: crate::sync::SyncEnvelope,
    ) -> anyhow::Result<crate::sync::SyncEnvelope> {
        self.manager.handle_envelope(env)
    }

    pub(crate) fn handle_push(&self, env: crate::sync::SyncEnvelope) -> anyhow::Result<()> {
        self.manager.handle_push(env)
    }
}

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

    // A known but never-heard-from peer covers nothing yet.
    let peer2 = store
        .read()
        .status()
        .peers
        .into_iter()
        .find(|p| p.id == 2)
        .unwrap();
    assert_eq!(peer2.ack, 0);
    assert_eq!(peer2.peer_ack, 0);
    assert!(!peer2.heard_from);

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
async fn equal_timestamps_are_broken_by_node_id_not_by_tombstone() {
    // The LWW order is `(timestamp, node, seq)` (RFC 3.5) and is frozen for the
    // mixed-version window: v1 resolves the same way, so changing it would split a
    // mixed cluster. Deletes get no special treatment — asserting that in both
    // directions is what stops "the tombstone wins" from being read into the rule.
    let put = Entry::new_put(Metadata::new(1, 1, 1234), "k".into(), b"v".to_vec());
    let del = Entry::new_delete(Metadata::new(2, 2, 1234), "k".into());
    assert_eq!(compare_entries(&put, &del), std::cmp::Ordering::Less);
    assert_eq!(compare_entries(&del, &put), std::cmp::Ordering::Greater);

    // Same shape, tombstone on the *lower* node id: now the put wins. A fixture that
    // only covers the case above cannot tell the two rules apart.
    let del_low = Entry::new_delete(Metadata::new(1, 1, 1234), "k".into());
    let put_high = Entry::new_put(Metadata::new(2, 2, 1234), "k".into(), b"v".to_vec());
    assert_eq!(
        compare_entries(&del_low, &put_high),
        std::cmp::Ordering::Less
    );
    assert_eq!(
        compare_entries(&put_high, &del_low),
        std::cmp::Ordering::Greater
    );
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
async fn sync_breaks_an_equal_timestamp_tie_by_node_id() {
    let store = Node::new(1, vec![]);

    // A tombstone on the higher node id displaces the put.
    let put = Entry::new_put(Metadata::new(1, 1, 1000), "k".into(), b"v".to_vec());
    store.write().sync(put).unwrap();
    let del = Entry::new_delete(Metadata::new(2, 2, 1000), "k".into());
    assert!(store.write().sync(del).unwrap());
    assert!(store.read().get("k").is_none());

    // ...and the reverse: a tombstone on the lower node id is itself displaced. The
    // merge path must agree with `compare_entries`, including where that is not the
    // delete-friendly answer.
    let other = Node::new(1, vec![]);
    let del_low = Entry::new_delete(Metadata::new(1, 1, 1000), "j".into());
    other.write().sync(del_low).unwrap();
    let put_high = Entry::new_put(Metadata::new(2, 2, 1000), "j".into(), b"v".to_vec());
    assert!(other.write().sync(put_high).unwrap());
    assert_eq!(
        other.read().get("j").and_then(|e| e.value),
        Some(b"v".to_vec()),
        "the higher node id wins even when the loser is a tombstone"
    );
}

#[tokio::test]
async fn wal_replay_preserves_the_equal_timestamp_tiebreak() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path();

    let store = Node::new_with_persistence(1, vec![], wal_path).unwrap();

    // `k`: the tombstone holds the higher node id and wins.
    store
        .write()
        .sync(Entry::new_put(
            Metadata::new(1, 1, 1000),
            "k".into(),
            b"v".to_vec(),
        ))
        .unwrap();
    store
        .write()
        .sync(Entry::new_delete(Metadata::new(2, 2, 1000), "k".into()))
        .unwrap();

    // `j`: the tombstone holds the lower node id and loses. Both directions matter,
    // because replay re-applies the ops in log order — a resolver that silently
    // depended on arrival order would diverge here and nowhere else.
    store
        .write()
        .sync(Entry::new_delete(Metadata::new(1, 1, 1000), "j".into()))
        .unwrap();
    store
        .write()
        .sync(Entry::new_put(
            Metadata::new(2, 2, 1000),
            "j".into(),
            b"v".to_vec(),
        ))
        .unwrap();

    assert!(store.read().get("k").is_none());
    assert!(store.read().get("j").is_some());
    drop(store);

    let recovered = Node::new_with_persistence(1, vec![], wal_path).unwrap();
    assert!(
        recovered.read().get("k").is_none(),
        "the winning tombstone must still win after WAL replay"
    );
    let tombstone = recovered
        .read()
        .get_including_tombstones("k")
        .expect("the tombstone itself must survive replay, not merely the missing value");
    assert!(tombstone.value.is_none());
    assert_eq!((tombstone.meta.node, tombstone.meta.timestamp), (2, 1000));
    assert_eq!(
        recovered.read().get("j").and_then(|e| e.value),
        Some(b"v".to_vec()),
        "and the losing tombstone must still lose"
    );
}

/// Tombstone GC is gated on replication, not on a local clock.
///
/// v1 expired tombstones on a local TTL, which under any state-shipping scheme lets a
/// lagging replica resurrect a deleted key: it never saw the tombstone, so it re-offers
/// the original write as live data. The watermark rule makes that impossible.
#[tokio::test]
async fn tombstone_gc_waits_for_every_peer_to_cover_the_delete() {
    use crate::sync::SyncEnvelope;

    let store = Node::new(1, vec![2]);
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store.write().delete("key1".to_string()).unwrap();

    let tombstone = store.read().get_including_tombstones("key1").unwrap();
    assert!(tombstone.value.is_none());

    // Peer 2 has never reported coverage, so the tombstone is pinned no matter how
    // much local time passes.
    assert_eq!(
        store.write().collect_tombstone_garbage().unwrap(),
        0,
        "collecting here would let peer 2 resurrect the key on its next sync"
    );

    // Peer 2 now reports covering our writes only up to just before the delete.
    let mut behind = SyncEnvelope::new(2, vec![]);
    behind.acks.insert(1, tombstone.meta.seq - 1);
    store.write().handle_envelope(behind, vec![]).unwrap();
    assert_eq!(store.write().collect_tombstone_garbage().unwrap(), 0);

    // ... and finally past it.
    let mut caught_up = SyncEnvelope::new(2, vec![]);
    caught_up.acks.insert(1, tombstone.meta.seq);
    store.write().handle_envelope(caught_up, vec![]).unwrap();
    assert_eq!(store.write().collect_tombstone_garbage().unwrap(), 1);
    assert!(store.read().get_including_tombstones("key1").is_none());
}

/// A lone node has nobody to resurrect from, so it may collect immediately.
#[tokio::test]
async fn a_single_node_cluster_collects_tombstones_immediately() {
    let store = Node::new(1, vec![]);
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store.write().delete("key1".to_string()).unwrap();
    assert_eq!(store.write().collect_tombstone_garbage().unwrap(), 1);
}

/// In-process transport that hands envelopes to the peer's receive boundary. Used by
/// the tests below to drive real exchanges without a network.
#[derive(Clone)]
pub(crate) struct DirectLink {
    pub(crate) me: NodeId,
    pub(crate) target: PeerEndpoint,
}

impl crate::sync::ExchangeInterface for DirectLink {
    fn uuid(&self) -> Vec<u8> {
        uuid_for(self.me)
    }

    fn query_uuid(&self, node_id: NodeId) -> Option<Vec<u8>> {
        Some(uuid_for(node_id))
    }

    async fn sync_to(
        &self,
        _node: &Node,
        peer: u32,
        msg: crate::sync::SyncMessage,
    ) -> anyhow::Result<crate::sync::SyncResponse> {
        self.check_addressed_to(peer)?;
        self.target.handle_sync_v1(msg)
    }

    async fn sync_v2_to(
        &self,
        _node: &Node,
        peer: u32,
        env: crate::sync::SyncEnvelope,
    ) -> anyhow::Result<Option<crate::sync::SyncEnvelope>> {
        self.check_addressed_to(peer)?;
        Ok(Some(self.target.handle_envelope(env)?))
    }
}

impl DirectLink {
    /// A real transport dials the peer it is given. This double holds exactly one, so a
    /// fixture whose member list names a peer it never wired up would otherwise have
    /// every such round answered by the wrong node — and a per-peer mechanism could
    /// then be "confirmed" by a link that does not exist. That is not hypothetical: a
    /// third-origin divergence fixture converged here through a phantom peer before
    /// this check existed.
    fn check_addressed_to(&self, peer: NodeId) -> anyhow::Result<()> {
        let target = self.target.node().read().id;
        anyhow::ensure!(
            peer == target,
            "fixture routed a round for peer {peer} to node {target};              wire the peer up or drop it from the member list"
        );
        Ok(())
    }
}

#[tokio::test]
async fn a_v2_round_ships_the_delta_and_advances_coverage() {
    use crate::sync::SyncEnvelope;

    let store1 = Node::new(1, vec![2]);
    let store2 = Node::new(2, vec![1]);

    store1
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store1
        .write()
        .put("key2".to_string(), b"value2".to_vec())
        .unwrap();

    // Node 1 asks node 2 for anything it is missing, and offers its own delta.
    let request = store1.read().prepare_sync(2, vec![]);
    assert_eq!(request.entries.len(), 2, "node 2 has covered nothing yet");

    let response = store2.write().handle_envelope(request, vec![]).unwrap();
    assert!(response.entries.is_empty(), "node 2 has nothing to offer");

    // Node 2 merged both entries and adopted node 1's coverage claim.
    assert_eq!(
        store2.read().get("key1").unwrap().value,
        Some(b"value1".to_vec())
    );
    assert_eq!(store2.read().acks_snapshot().get(&1).copied(), Some(2));

    let outcome = store1.write().apply_envelope(response).unwrap();
    assert_eq!(outcome.merged, 0);
    assert_eq!(
        outcome.digest_match,
        Some(true),
        "both replicas hold the same state, so their digests must agree"
    );

    // A second round is a no-op: the delta filter now excludes everything.
    let followup = store1.read().prepare_sync(2, vec![]);
    assert!(followup.entries.is_empty());

    // An envelope carrying no acks at all is just a bootstrap request.
    let bootstrap = SyncEnvelope::new(3, vec![]);
    let full = store2.write().handle_envelope(bootstrap, vec![]).unwrap();
    assert_eq!(full.entries.len(), 2, "bootstrap is a delta against zero");
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

/// The delta owed to a partially-covered peer excludes what it already has.
#[tokio::test]
async fn a_delta_carries_only_what_the_peer_is_missing() {
    use crate::sync::SyncEnvelope;

    let store = Node::new(1, vec![2]);
    for key in ["key1", "key2", "key3"] {
        store.write().put(key.to_string(), b"v".to_vec()).unwrap();
    }

    // Peer 2 reports covering our writes up to seq 1.
    let mut reported = SyncEnvelope::new(2, vec![]);
    reported.acks.insert(1, 1);
    let response = store.write().handle_envelope(reported, vec![]).unwrap();

    assert_eq!(response.entries.len(), 2, "seq 2 and 3 remain outstanding");
    assert!(response.entries.iter().all(|e| e.meta.seq > 1));
}

/// Superseded writes are never shipped: v1 sent every intermediate version of a key,
/// v2 sends only the survivor.
#[tokio::test]
async fn overwrites_collapse_instead_of_replaying_every_version() {
    use crate::sync::SyncEnvelope;

    let store = Node::new(1, vec![2]);
    for i in 0..10 {
        store
            .write()
            .put("hot".to_string(), format!("v{i}").into_bytes())
            .unwrap();
    }

    let response = store
        .write()
        .handle_envelope(SyncEnvelope::new(2, vec![]), vec![])
        .unwrap();
    assert_eq!(
        response.entries.len(),
        1,
        "ten writes to one key collapse to the single winning entry"
    );
    assert_eq!(response.entries[0].value, Some(b"v9".to_vec()));
}

/// A full dump is the same code path as an incremental delta, with an empty ack map.
#[tokio::test]
async fn a_bootstrap_delta_includes_tombstones() {
    use crate::sync::SyncEnvelope;

    let store = Node::new(1, vec![]);
    store
        .write()
        .put("key1".to_string(), "value1".to_string())
        .unwrap();
    store
        .write()
        .put("key2".to_string(), "value2".to_string())
        .unwrap();
    store.write().delete("key3".to_string()).unwrap();

    let response = store
        .write()
        .handle_envelope(SyncEnvelope::new(9, vec![]), vec![])
        .unwrap();
    assert_eq!(response.entries.len(), 3);
    assert_eq!(
        response.entries.iter().filter(|e| e.is_deleted()).count(),
        1,
        "a tombstone the new node never learns about is a resurrection waiting to happen"
    );
}

#[tokio::test]
async fn test_sync_config_defaults() {
    use crate::sync::SyncConfig;

    let config = SyncConfig::default();
    assert_eq!(config.interval, Duration::from_secs(30));
    assert_eq!(config.timeout, Duration::from_secs(10));
}

#[tokio::test]
async fn a_sync_manager_round_converges_two_nodes() {
    use crate::sync::SyncConfig;

    let store1 = Node::new(1, vec![2]);
    let store2 = PeerEndpoint::new(2, vec![1]);

    let sync = SyncManager::with_config(
        store1.clone(),
        DirectLink {
            me: 1,
            target: store2.clone(),
        },
        SyncConfig {
            interval: Duration::from_millis(100),
            timeout: Duration::from_secs(5),
            ..Default::default()
        },
    );

    store1
        .write()
        .put("key".to_string(), "value".to_string())
        .unwrap();
    store2
        .node()
        .write()
        .put("other".to_string(), "value".to_string())
        .unwrap();

    sync.bootstrap().await.unwrap();

    assert_eq!(store1.state_digest(), store2.node().state_digest());
    assert!(store1.read().get("other").is_some());
    assert!(store2.node().read().get("key").is_some());
    let status = sync
        .link_status()
        .into_iter()
        .find(|s| s.id == 2)
        .expect("peer 2 is known and must be reported");
    assert_eq!(status.protocol, "v2");
    assert_eq!(
        status.consecutive_failures, 0,
        "a healthy round must clear the streak, not merely stop incrementing it"
    );
}

#[tokio::test]
async fn a_slow_peer_times_out_without_panicking() {
    use crate::sync::{ExchangeInterface, SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use anyhow::Result;
    use std::sync::Arc;

    #[derive(Clone)]
    struct SlowNetwork {
        delay: Duration,
        target: PeerEndpoint,
    }

    impl ExchangeInterface for SlowNetwork {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }

        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            msg: SyncMessage,
        ) -> Result<SyncResponse> {
            sleep(self.delay).await;
            self.target.handle_sync_v1(msg)
        }

        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            env: SyncEnvelope,
        ) -> Result<Option<SyncEnvelope>> {
            sleep(self.delay).await;
            Ok(Some(self.target.handle_envelope(env)?))
        }
    }

    let store1 = Node::new(1, vec![2]);
    let store2 = PeerEndpoint::new(2, vec![1]);

    let sync = Arc::new(SyncManager::with_config(
        store1.clone(),
        SlowNetwork {
            delay: Duration::from_millis(200),
            target: store2.clone(),
        },
        SyncConfig {
            interval: Duration::from_secs(30),
            timeout: Duration::from_millis(50),
            ..Default::default()
        },
    ));

    store1
        .write()
        .put("key".to_string(), "value".to_string())
        .unwrap();

    // Bootstrap logs per-peer failures and continues rather than propagating them.
    assert!(sync.bootstrap().await.is_ok());
}

// ---------------------------------------------------------------------------
// Snapshot recovery
// ---------------------------------------------------------------------------

/// Local persistence damage must be a quarantine-and-resync event, never a refusal to
/// start: the state is fully replicated, so a node that will not boot is strictly worse
/// than one that boots slightly stale.
#[tokio::test]
async fn a_corrupt_snapshot_falls_back_to_the_previous_generation() {
    let dir = tempfile::tempdir().unwrap();

    {
        let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        node.write()
            .put("gen".to_string(), b"one".to_vec())
            .unwrap();
        node.persist().unwrap();
        node.write()
            .put("gen".to_string(), b"two".to_vec())
            .unwrap();
        node.persist().unwrap(); // rotates generation one into .snapshot.bak
    }

    let snapshot = dir.path().join("node_1.snapshot");
    let backup = dir.path().join("node_1.snapshot.bak");
    assert!(backup.exists(), "a previous generation must be retained");
    fs_err::write(&snapshot, b"shredded").unwrap();

    let node = Node::new_with_persistence(1, vec![2], dir.path())
        .expect("a damaged snapshot must not prevent startup");
    assert_eq!(
        node.read().get("gen").map(|e| e.value.unwrap()),
        Some(b"one".to_vec()),
        "the node should come up on the retained generation and re-sync the rest"
    );
}

#[tokio::test]
async fn an_unreadable_snapshot_and_backup_still_start_empty() {
    let dir = tempfile::tempdir().unwrap();
    {
        let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        node.write().put("k".to_string(), b"v".to_vec()).unwrap();
        node.persist().unwrap();
        node.write().put("k2".to_string(), b"v".to_vec()).unwrap();
        node.persist().unwrap();
    }
    fs_err::write(dir.path().join("node_1.snapshot"), b"x").unwrap();
    fs_err::write(dir.path().join("node_1.snapshot.bak"), b"x").unwrap();

    let node = Node::new_with_persistence(1, vec![2], dir.path())
        .expect("both generations unusable must still start");
    assert_eq!(node.read().get_all_including_tombstones().len(), 0);
}

// ---------------------------------------------------------------------------
// Protocol negotiation
// ---------------------------------------------------------------------------

/// A peer that has not been upgraded answers the v2 route with "no such route", which
/// the transport reports as `Ok(None)`. The manager must fall back rather than treating
/// it as a failed round, and must remember the verdict.
#[tokio::test]
async fn a_peer_without_the_v2_route_falls_back_to_v1() {
    use crate::sync::{ExchangeInterface, SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct V1Only {
        target: PeerEndpoint,
        v2_probes: Arc<AtomicUsize>,
        v1_rounds: Arc<AtomicUsize>,
    }

    impl ExchangeInterface for V1Only {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }

        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            self.v1_rounds.fetch_add(1, Ordering::SeqCst);
            self.target.handle_sync_v1(msg)
        }

        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            _env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            self.v2_probes.fetch_add(1, Ordering::SeqCst);
            Ok(None) // 404: this peer speaks only v1
        }
    }

    let local = Node::new(1, vec![2]);
    let remote = PeerEndpoint::new(2, vec![1]);
    remote
        .node()
        .write()
        .put("remote".to_string(), b"v".to_vec())
        .unwrap();

    let v2_probes = Arc::new(AtomicUsize::new(0));
    let v1_rounds = Arc::new(AtomicUsize::new(0));
    let sync = SyncManager::with_config(
        local.clone(),
        V1Only {
            target: remote.clone(),
            v2_probes: v2_probes.clone(),
            v1_rounds: v1_rounds.clone(),
        },
        SyncConfig {
            // Long enough that the second round must reuse the cached verdict.
            protocol_reprobe: Duration::from_secs(3600),
            ..Default::default()
        },
    );

    sync.bootstrap().await.unwrap();
    assert_eq!(v2_probes.load(Ordering::SeqCst), 1);
    assert_eq!(v1_rounds.load(Ordering::SeqCst), 1, "must have fallen back");
    assert!(
        local.read().get("remote").is_some(),
        "data still flows over v1"
    );
    assert_eq!(
        sync.link_status()
            .iter()
            .find(|s| s.id == 2)
            .map(|s| s.protocol),
        Some("v1")
    );

    // The cached verdict spares the peer a probe on every subsequent round.
    sync.bootstrap().await.unwrap();
    assert_eq!(
        v2_probes.load(Ordering::SeqCst),
        1,
        "a peer known to be v1 must not be re-probed until the reprobe window elapses"
    );
    assert_eq!(v1_rounds.load(Ordering::SeqCst), 2);
}

/// A *failing* v2 route is not a v1 route.
///
/// Only 404/405 — surfaced as `Ok(None)` — means "not upgraded yet". A 5xx or a timeout
/// is a transport failure: the round fails and is retried, and the cached protocol must
/// not move. Demoting on any error instead would silently pin a healthy v2 peer to the
/// v1 path for a whole reprobe window every time it hiccuped.
#[tokio::test]
async fn a_failing_v2_route_is_retried_rather_than_mistaken_for_v1() {
    use crate::sync::{ExchangeInterface, SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct V2Broken {
        v2_attempts: Arc<AtomicUsize>,
        v1_rounds: Arc<AtomicUsize>,
    }

    impl ExchangeInterface for V2Broken {
        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            _msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            self.v1_rounds.fetch_add(1, Ordering::SeqCst);
            anyhow::bail!("the v1 leg must not be reached for a 5xx on the v2 route")
        }

        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            _env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            self.v2_attempts.fetch_add(1, Ordering::SeqCst);
            // HTTP 500, a gzip failure, a refused connection: anything that is not
            // "no such route".
            anyhow::bail!("request failed: 500 Internal Server Error")
        }
    }

    let v2_attempts = Arc::new(AtomicUsize::new(0));
    let v1_rounds = Arc::new(AtomicUsize::new(0));
    let sync = SyncManager::with_config(
        Node::new(1, vec![2]),
        V2Broken {
            v2_attempts: v2_attempts.clone(),
            v1_rounds: v1_rounds.clone(),
        },
        SyncConfig {
            protocol_reprobe: Duration::from_secs(3600),
            ..Default::default()
        },
    );

    // Per-peer failures are logged, not propagated (same contract as a timeout).
    sync.bootstrap().await.unwrap();
    sync.bootstrap().await.unwrap();

    assert_eq!(
        v2_attempts.load(Ordering::SeqCst),
        2,
        "every round must retry v2; the reprobe window governs demoted peers only"
    );
    assert_eq!(
        v1_rounds.load(Ordering::SeqCst),
        0,
        "a 5xx must not fall back to v1"
    );
    let status = sync
        .link_status()
        .into_iter()
        .find(|s| s.id == 2)
        .expect("a peer that only ever failed must still be reported, not omitted");
    assert_eq!(
        status.protocol, "v2",
        "the cached protocol must survive a transport failure"
    );
    assert_eq!(
        status.consecutive_failures, 2,
        "the failure streak is the only signal that distinguishes this from a healthy peer"
    );
}

/// ...but the verdict must expire, or a peer upgraded mid-rollout would stay on the v1
/// path until the whole cluster restarted.
#[tokio::test]
async fn an_upgraded_peer_is_picked_up_after_the_reprobe_window() {
    use crate::sync::{ExchangeInterface, SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct Upgradable {
        target: PeerEndpoint,
        speaks_v2: Arc<AtomicBool>,
    }

    impl ExchangeInterface for Upgradable {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }

        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            self.target.handle_sync_v1(msg)
        }

        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            if !self.speaks_v2.load(Ordering::SeqCst) {
                return Ok(None);
            }
            Ok(Some(self.target.handle_envelope(env)?))
        }
    }

    let local = Node::new(1, vec![2]);
    let remote = PeerEndpoint::new(2, vec![1]);
    let speaks_v2 = Arc::new(AtomicBool::new(false));

    let sync = SyncManager::with_config(
        local.clone(),
        Upgradable {
            target: remote.clone(),
            speaks_v2: speaks_v2.clone(),
        },
        SyncConfig {
            protocol_reprobe: Duration::from_millis(1),
            ..Default::default()
        },
    );

    sync.bootstrap().await.unwrap();
    assert_eq!(
        sync.link_status()
            .iter()
            .find(|s| s.id == 2)
            .map(|s| s.protocol),
        Some("v1")
    );

    speaks_v2.store(true, Ordering::SeqCst);
    sleep(Duration::from_millis(5)).await;
    sync.bootstrap().await.unwrap();

    assert_eq!(
        sync.link_status()
            .iter()
            .find(|s| s.id == 2)
            .map(|s| s.protocol),
        Some("v2"),
        "the upgraded peer must be promoted without a restart"
    );
}

// ---------------------------------------------------------------------------
// Divergence detection and automatic repair
// ---------------------------------------------------------------------------

/// The failure v1 could not even see: two replicas whose deltas are empty — so both
/// believe they are in sync — but whose states differ. The digest detects it and the
/// manager repairs it by lowering coverage, which is only safe because the data map is
/// never truncated.
#[tokio::test]
async fn persistently_mismatched_digests_trigger_an_automatic_repair() {
    use crate::sync::{SyncConfig, SyncEnvelope};

    let a = Node::new(1, vec![2]);
    let b = PeerEndpoint::new(2, vec![1]);
    a.write()
        .put("only-on-a".to_string(), b"1".to_vec())
        .unwrap();
    b.node()
        .write()
        .put("only-on-b".to_string(), b"2".to_vec())
        .unwrap();

    // Manufacture the pathological state: each side claims full coverage of the other
    // without ever having received its data.
    let full_coverage = || {
        let mut env = SyncEnvelope::new(0, vec![]);
        env.acks.insert(1, 1);
        env.acks.insert(2, 1);
        env
    };
    let mut from_b = full_coverage();
    from_b.sender_id = 2;
    a.write().handle_envelope(from_b, vec![]).unwrap();
    let mut from_a = full_coverage();
    from_a.sender_id = 1;
    b.node().write().handle_envelope(from_a, vec![]).unwrap();

    assert_ne!(
        a.state_digest(),
        b.node().state_digest(),
        "the two have diverged"
    );
    assert!(
        a.read().prepare_sync(2, vec![]).entries.is_empty(),
        "and neither has anything to send, so v1 would never notice"
    );

    let sync = SyncManager::with_config(
        a.clone(),
        DirectLink {
            me: 1,
            target: b.clone(),
        },
        SyncConfig {
            digest_check_rounds: 2,
            ..Default::default()
        },
    );

    for _ in 0..4 {
        sync.bootstrap().await.unwrap();
    }

    assert_eq!(
        a.state_digest(),
        b.node().state_digest(),
        "the digest mismatch must escalate to a full re-exchange and converge"
    );
    assert!(a.read().get("only-on-b").is_some());
    assert!(b.node().read().get("only-on-a").is_some());
    assert_eq!(
        sync.link_status()
            .iter()
            .find(|s| s.id == 2)
            .map(|s| s.digest_mismatches),
        Some(0),
        "the counter resets once the pair agrees again"
    );
}

/// The two-node fixture above cannot distinguish "repairs the pair" from "repairs the
/// entries the peer itself authored", because there every entry is origin==peer.
///
/// Lowering `acks[peer]` only asks the peer to resend what *it* wrote. A third node's
/// entries are filtered by `acks[C]`, which the repair never touches — so the responder
/// stays silent about exactly the data that is missing, round after round.
#[tokio::test]
async fn repair_recovers_an_entry_authored_by_a_third_node() {
    use crate::sync::{SyncConfig, SyncEnvelope};

    // Only B is a member: `DirectLink` answers for whatever peer it is handed, so a
    // third member would give the repair a second, uncontrolled link to converge on.
    let a = Node::new(1, vec![2]);
    let b = PeerEndpoint::new(2, vec![1]);

    // C's entry reaches B by an ordinary sync with C.
    let from_c = |entries: Vec<Entry>| {
        let mut env = SyncEnvelope::new(3, uuid_for(3));
        env.acks.insert(3, 1);
        env.entries = entries;
        env
    };
    let c_entry = Entry::new_put(Metadata::new(3, 1, 1000), "only-on-b".into(), b"c".to_vec());
    b.node()
        .write()
        .handle_envelope(from_c(vec![c_entry]), uuid_for(2))
        .unwrap();

    // A adopts C's coverage without ever receiving the entry — the divergence.
    a.write()
        .handle_envelope(from_c(Vec::new()), uuid_for(1))
        .unwrap();

    assert_ne!(
        a.state_digest(),
        b.node().state_digest(),
        "A is missing a C-authored entry it claims to cover"
    );

    let sync = SyncManager::with_config(
        a.clone(),
        DirectLink {
            me: 1,
            target: b.clone(),
        },
        SyncConfig {
            digest_check_rounds: 2,
            ..Default::default()
        },
    );

    for _ in 0..6 {
        sync.bootstrap().await.unwrap();
    }

    assert!(
        a.read().get("only-on-b").is_some(),
        "the repair must recover entries of every origin, not just the peer's own"
    );
    assert_eq!(a.state_digest(), b.node().state_digest());
}

/// R1 says acks may be adopted only after a *complete* merge. Across a paginated round
/// that has to mean every page, but completeness was evaluated per envelope and the
/// page loop carried no state between iterations.
///
/// So a round whose first page is partly refused and whose last page merges cleanly
/// ends with the initiator adopting the peer's full ack map — claiming coverage of the
/// very entry it just refused. Unpaged, the same refusal correctly parks the acks and
/// the entry is retransmitted next round; pagination turned a visible retransmit into a
/// silent, permanent hole.
#[tokio::test]
async fn a_refusal_on_an_early_page_blocks_adoption_at_the_end_of_the_round() {
    use crate::delta::PageInfo;
    use crate::sync::{SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn epoch_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_millis() as i64
    }

    #[derive(Clone)]
    struct Paginates {
        calls: Arc<AtomicUsize>,
    }

    impl crate::sync::ExchangeInterface for Paginates {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }
        fn query_uuid(&self, node: NodeId) -> Option<Vec<u8>> {
            Some(uuid_for(node))
        }
        async fn sync_to(
            &self,
            _n: &Node,
            _p: NodeId,
            _m: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            anyhow::bail!("v2 only")
        }
        async fn sync_v2_to(
            &self,
            _n: &Node,
            _p: NodeId,
            _env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            let mut env = SyncEnvelope::new(2, uuid_for(2));
            env.acks.insert(2, 5);
            // Three pages, not two. The refusal is on the first, and the accumulator
            // that has to remember it is only *read* by a page after the one that
            // follows — so a two-page round cannot tell `&=` from `|=`.
            match self.calls.fetch_add(1, Ordering::SeqCst) {
                0 => {
                    // Page one: a single entry stamped far enough ahead to be refused.
                    env.entries = vec![Entry::new_put(
                        Metadata::new(2, 1, epoch_millis() + 86_400_000),
                        "refused".into(),
                        b"v".to_vec(),
                    )];
                    env.page = Some(PageInfo {
                        cursor: (2, 1),
                        last: false,
                    });
                }
                1 => {
                    // Page two: clean, and not the last.
                    env.page = Some(PageInfo {
                        cursor: (2, 2),
                        last: false,
                    });
                }
                // Page three: clean and final (production signals "final" with `None`).
                _ => {}
            }
            Ok(Some(env))
        }
    }

    let a = Node::new(1, vec![2]);
    let calls = Arc::new(AtomicUsize::new(0));
    let sync = SyncManager::with_config(
        a.clone(),
        Paginates {
            calls: calls.clone(),
        },
        SyncConfig::default(),
    );
    sync.bootstrap().await.unwrap();

    assert!(
        calls.load(Ordering::SeqCst) >= 2,
        "the round must have paginated"
    );
    assert!(
        a.read().get("refused").is_none(),
        "the fixture depends on that entry being refused"
    );
    assert_eq!(
        a.read().acks_snapshot().get(&2).copied().unwrap_or(0),
        0,
        "adopting here claims coverage of an entry this node refused, so no peer will \
         ever send it again"
    );
}

/// The divergence check compares our digest against the one the responder volunteers.
/// Putting our digest in the *request* therefore tells the responder the exact value it
/// needs to appear healthy: echo it, and `digest_match` is `Some(true)` every round, so
/// the counter that drives repair is reset before it can ever reach the threshold.
///
/// A responder has no use for it — `handle_envelope` never reads the field — so the
/// only thing sending it can do is disclose.
#[tokio::test]
async fn a_sync_request_does_not_disclose_our_own_digest() {
    let a = Node::new(1, vec![2]);
    a.write().put("k".to_string(), b"v".to_vec()).unwrap();

    let request = a.read().prepare_sync(2, uuid_for(1));
    assert!(
        request.digest.is_none(),
        "a request carrying our digest lets any responder forge agreement with it"
    );

    // The responder still volunteers its own, which is what detection actually reads.
    let b = PeerEndpoint::new(2, vec![1]);
    let response = b.handle_envelope(request).unwrap();
    assert!(
        response.digest.is_some(),
        "removing it from requests must not disarm the check itself"
    );
}

// ---------------------------------------------------------------------------
// Opportunistic push
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_push_queue_drains_and_is_bounded() {
    use crate::NodeConfig;

    let node = Node::with_config(
        1,
        vec![2],
        NodeConfig {
            max_delta_entries: 4,
            ..Default::default()
        },
    );

    assert!(
        node.write().take_push_envelope().is_none(),
        "nothing pending means no envelope and no allocation"
    );

    node.write().put("a".to_string(), b"1".to_vec()).unwrap();
    node.write().put("b".to_string(), b"2".to_vec()).unwrap();
    let env = node
        .write()
        .take_push_envelope()
        .expect("two writes pending");
    assert!(env.push_only, "R3: a push carries no ack authority");
    assert!(env.acks.is_empty());
    assert_eq!(env.entries.len(), 2);

    assert!(
        node.write().take_push_envelope().is_none(),
        "taking the envelope must drain the queue, not copy it"
    );

    // Nothing draining: the backlog must stay bounded rather than growing forever.
    for i in 0..20 {
        node.write().put(format!("k{i}"), b"v".to_vec()).unwrap();
    }
    let env = node.write().take_push_envelope().expect("pending");
    assert!(
        env.entries.len() <= 4,
        "the backlog must be bounded by max_delta_entries, got {}",
        env.entries.len()
    );
}

/// End to end: a local write reaches the peer via the push channel, well inside one
/// sync interval.
#[tokio::test]
async fn a_local_write_reaches_the_peer_through_the_push_channel() {
    use crate::sync::{ExchangeInterface, SyncConfig, SyncEnvelope, SyncMessage, SyncResponse};
    use std::sync::Arc;

    // The receiver is driven through `SyncManager::handle_push`, not `merge_push`:
    // `check_uuid` lives on the manager, so a fake that merges straight into the
    // `NodeState` would skip the only step that can reject a push. Both sides also
    // implement `query_uuid`, because the check is opt-in and a transport that leaves
    // it at the default `None` cannot fail it either.
    #[derive(Clone)]
    struct PushLink {
        target: PeerEndpoint,
    }

    // Both sync legs are dead, so anything that reaches the peer can only have
    // arrived over the push channel. (`tokio::time::interval` fires its first tick
    // immediately, so leaving the periodic round working would deliver the write at
    // startup and prove nothing.)
    impl ExchangeInterface for PushLink {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }
        fn query_uuid(&self, node_id: NodeId) -> Option<Vec<u8>> {
            Some(uuid_for(node_id))
        }
        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            _msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            anyhow::bail!("the periodic round is disabled for this test")
        }
        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            _env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            anyhow::bail!("the periodic round is disabled for this test")
        }
        async fn push_to(&self, _node: &Node, _peer: u32, env: SyncEnvelope) -> anyhow::Result<()> {
            self.target.handle_push(env)
        }
    }

    let local = Node::new(1, vec![2]);
    let remote = PeerEndpoint::new(2, vec![1]);

    let sync = Arc::new(SyncManager::with_config(
        local.clone(),
        PushLink {
            target: remote.clone(),
        },
        SyncConfig {
            // A periodic round this slow would never explain the propagation below.
            interval: Duration::from_secs(3600),
            coalesce_window: Some(Duration::from_millis(20)),
            ..Default::default()
        },
    ));
    sync.clone().start_sync_tasks().await;

    local
        .write()
        .put("fresh".to_string(), b"v".to_vec())
        .unwrap();
    sync.notify_local_write();

    for _ in 0..50 {
        if remote.node().read().get("fresh").is_some() {
            break;
        }
        sleep(Duration::from_millis(10)).await;
    }
    assert!(
        remote.node().read().get("fresh").is_some(),
        "the write should arrive via push, not wait for the hourly round"
    );
    assert!(
        remote
            .node()
            .read()
            .get_including_tombstones("fresh")
            .is_some(),
        "sanity: the entry is really in the peer's map"
    );
    assert_eq!(
        remote
            .node()
            .read()
            .acks_snapshot()
            .get(&1)
            .copied()
            .unwrap_or(0),
        0,
        "R3: a push moves data but never coverage"
    );
}

// ---------------------------------------------------------------------------
// Remaining ingest guards
// ---------------------------------------------------------------------------

/// A runaway-clock peer does not merely lose one round — it stalls the pair.
///
/// Rejecting an entry sets `complete = false` for the whole batch (R1), so no acks move,
/// so our coverage for that origin never advances, so the same entry is re-offered next
/// round. The stall therefore persists for as long as the timestamp stays out of bounds,
/// and it takes down origins that had nothing to do with the offending entry. Pinning
/// both the persistence and the self-heal is what keeps this a bounded, understood cost
/// rather than a mystery outage.
#[tokio::test]
async fn a_future_stamped_entry_stalls_every_round_until_it_falls_in_range() {
    use crate::sync::SyncEnvelope;
    use crate::types::{Entry, Metadata};
    use crate::{Limits, NodeConfig};

    let node = Node::with_config(
        1,
        vec![2],
        NodeConfig {
            limits: Limits {
                max_clock_drift: Duration::from_secs(60),
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let now = || {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock is after the epoch")
            .as_millis() as i64
    };

    // One poisoned entry from origin 2, one innocent entry from origin 3, one ack map
    // covering both. The innocent origin is the point: R1 is batch-wide.
    let envelope = |poison_ts: i64| {
        let mut env = SyncEnvelope::new(2, vec![]);
        env.entries.push(Entry::new(
            "poisoned".to_string(),
            Some(b"v".to_vec()),
            Metadata::new(2, 1, poison_ts),
        ));
        env.entries.push(Entry::new(
            "innocent".to_string(),
            Some(b"v".to_vec()),
            Metadata::new(3, 1, now()),
        ));
        env.acks.insert(2, 1);
        env.acks.insert(3, 1);
        env
    };

    for round in 1..=3 {
        let outcome = node
            .write()
            .apply_envelope(envelope(now() + 3_600_000))
            .unwrap();
        assert_eq!(outcome.rejected, 1, "round {round}");
        assert!(
            !outcome.acks_adopted,
            "round {round}: one out-of-range entry parks the whole batch"
        );
        assert!(
            node.read().get("poisoned").is_none(),
            "round {round}: the poisoned value must never land"
        );
        assert_eq!(
            node.read().acks_snapshot().get(&3).copied().unwrap_or(0),
            0,
            "round {round}: an unrelated origin is stalled too — this is the real cost"
        );
    }

    // Wall time catches up (modelled by a timestamp that is now in range). Nothing had
    // to be reset by hand: the peer re-offers the same entry and the round completes.
    let outcome = node
        .write()
        .apply_envelope(envelope(now() + 1_000))
        .unwrap();
    assert_eq!(outcome.rejected, 0);
    assert!(
        outcome.acks_adopted,
        "the pair must heal without intervention"
    );
    assert!(node.read().get("poisoned").is_some());
    assert_eq!(node.read().acks_snapshot().get(&3).copied().unwrap_or(0), 1);
}

#[tokio::test]
async fn capacity_limits_refuse_new_keys_but_still_allow_updates() {
    use crate::sync::SyncEnvelope;
    use crate::types::{Entry, Metadata};
    use crate::{Limits, NodeConfig};

    let node = Node::with_config(
        1,
        vec![2],
        NodeConfig {
            limits: Limits {
                max_keys: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    );

    let merge = |key: &str, seq: u64, value: &[u8]| {
        let mut env = SyncEnvelope::new(2, vec![]);
        env.entries.push(Entry::new(
            key.to_string(),
            Some(value.to_vec()),
            Metadata::new(2, seq, seq as i64),
        ));
        env
    };

    let first = node.write().apply_envelope(merge("k1", 1, b"a")).unwrap();
    assert_eq!(first.rejected, 0);

    let second = node.write().apply_envelope(merge("k2", 2, b"b")).unwrap();
    assert_eq!(second.rejected, 1, "a second key exceeds max_keys");
    assert!(node.read().get("k2").is_none());

    // Updating an existing key creates no new key, so capacity must not block it.
    let update = node.write().apply_envelope(merge("k1", 3, b"c")).unwrap();
    assert_eq!(update.rejected, 0);
    assert_eq!(node.read().get("k1").unwrap().value, Some(b"c".to_vec()));
}

/// Only a v1 *full dump* is a complete delta, so only `is_snapshot` may move acks.
///
/// v1's incremental path silently skips origins whose logs were truncated below our
/// ack, so adopting after one would claim coverage of writes we never received — the
/// hole INV forbids. Both halves of the condition need a fixture that isolates them:
/// with only the happy case, `&&` and `||` are indistinguishable.
#[tokio::test]
async fn a_v1_response_moves_acks_only_when_it_is_a_full_dump() {
    use crate::sync::SyncResponse;
    use crate::types::{Entry, Metadata};

    let response = |is_snapshot: bool, entries: Vec<Entry>| SyncResponse {
        peer_id: 2,
        entries,
        progress: [(2, 7)].into_iter().collect(),
        is_snapshot,
    };
    let good = || {
        vec![Entry::new(
            "k".to_string(),
            Some(b"v".to_vec()),
            Metadata::new(2, 1, 1),
        )]
    };

    // Complete, but incremental: the merge succeeds and the data lands, yet coverage
    // must stay put.
    let node = Node::new(1, vec![2]);
    node.write()
        .apply_v1_response(response(false, good()))
        .unwrap();
    assert!(node.read().get("k").is_some(), "the data is still merged");
    assert_eq!(
        node.read().acks_snapshot().get(&2).copied().unwrap_or(0),
        0,
        "an incremental v1 response must not move acks"
    );

    // A snapshot whose merge was incomplete: R1 blocks adoption just the same. The
    // entry is refused by the clock guard, which is enough to fail the batch.
    let node = Node::new(1, vec![2]);
    let poisoned = vec![Entry::new(
        "bad".to_string(),
        Some(b"v".to_vec()),
        Metadata::new(2, 1, i64::MAX),
    )];
    node.write()
        .apply_v1_response(response(true, poisoned))
        .unwrap();
    assert_eq!(
        node.read().acks_snapshot().get(&2).copied().unwrap_or(0),
        0,
        "a failed merge blocks adoption even from a full dump"
    );

    // Both conditions met: now coverage advances.
    let node = Node::new(1, vec![2]);
    node.write()
        .apply_v1_response(response(true, good()))
        .unwrap();
    assert_eq!(
        node.read().acks_snapshot().get(&2).copied().unwrap_or(0),
        7,
        "a complete full dump is the one case that may move acks"
    );
}

/// After a restart the next write must not reuse a sequence number.
///
/// `next_seq` is rebuilt from the highest own seq in the recovered state. Getting the
/// arithmetic wrong hands out a number that is already in use, and a reused `(node,
/// seq)` silently collides in every peer's coverage map — the entry is filtered out as
/// "already seen" and the write is lost with no error anywhere.
#[tokio::test]
async fn recovery_resumes_after_the_highest_used_seq() {
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
    for i in 0..5 {
        store.write().put(format!("k{i}"), b"v".to_vec()).unwrap();
    }
    let highest = store
        .read()
        .get_all_including_tombstones()
        .values()
        .filter(|e| e.meta.node == 1)
        .map(|e| e.meta.seq)
        .max()
        .expect("five writes");
    assert_eq!(highest, 5);
    drop(store);

    let recovered = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
    let next = recovered
        .write()
        .put("after".to_string(), b"v".to_vec())
        .unwrap();
    assert_eq!(
        next.meta.seq,
        highest + 1,
        "the first post-recovery write must take the very next seq, not reuse or skip"
    );
}

/// The same hazard, from the direction the fixture above cannot reach: `max_own_seq`
/// scans the *live* index, and a peer's LWW win evicts our `(id, seq)` from it. The
/// seq stays covered by every peer's ack map regardless — coverage is about what was
/// authored, not about what still wins — so rebuilding `next_seq` from live entries
/// alone hands the number straight back out.
#[tokio::test]
async fn recovery_does_not_reuse_a_seq_whose_entry_lost_lww() {
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    let mine = store
        .write()
        .put("k".to_string(), b"mine".to_vec())
        .unwrap();
    assert_eq!(mine.meta.seq, 1);

    // Node 2 wins the key. Our own entry is evicted from the live index; our ack for
    // ourselves still records that seq 1 was authored.
    store
        .write()
        .sync(Entry::new_put(
            Metadata::new(2, 1, mine.meta.timestamp + 1_000),
            "k".into(),
            b"theirs".to_vec(),
        ))
        .unwrap();
    drop(store);

    let recovered = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    recovered.write().recover_next_seq();
    let next = recovered
        .write()
        .put("after".to_string(), b"v".to_vec())
        .unwrap();
    assert!(
        next.meta.seq > mine.meta.seq,
        "seq {} was already handed out; peers filter it as already seen and the write \
         is lost with no error anywhere",
        next.meta.seq
    );
}

/// A node whose data directory is lost keeps its id, so its old seqs are still spent —
/// and the only surviving record of how many is the peers' coverage of it.
///
/// `bootstrap` syncs before rebuilding `next_seq` precisely so this is available.
/// Without consulting our own ack the rebuilt node restarts from 1, and every write it
/// makes until it passes its old high-water mark is filtered out by every peer as
/// already seen — the node looks healthy and publishes nothing.
#[tokio::test]
async fn a_rebuilt_node_relearns_its_spent_seqs_from_its_peers() {
    use crate::sync::SyncConfig;

    let rebuilt = Node::new(1, vec![2]);
    let peer = PeerEndpoint::new(2, vec![1]);

    // The peer remembers covering node 1 up to seq 7 — told to it by the node as it was
    // before the loss. The rebuilt node itself holds nothing.
    let mut before_the_loss = crate::sync::SyncEnvelope::new(1, uuid_for(1));
    before_the_loss.acks.insert(1, 7);
    peer.node()
        .write()
        .handle_envelope(before_the_loss, uuid_for(2))
        .unwrap();
    assert!(rebuilt.read().get_all_including_tombstones().is_empty());

    let sync = SyncManager::with_config(
        rebuilt.clone(),
        DirectLink {
            me: 1,
            target: peer.clone(),
        },
        SyncConfig::default(),
    );
    sync.bootstrap().await.unwrap();

    let next = rebuilt
        .write()
        .put("after-rebuild".to_string(), b"v".to_vec())
        .unwrap();
    assert!(
        next.meta.seq > 7,
        "seq {} is still covered by the peer, which will filter the write out",
        next.meta.seq
    );
}

/// A peer whose uuid was regenerated must still be able to converge.
///
/// The node id is configuration; the uuid is derived from local state, so a node whose
/// data directory is recreated returns with the same id and a different uuid. From the
/// other side that is indistinguishable from two machines sharing an id, and the
/// inbound check rejects its requests. The only way its fresh identity record reaches
/// us is inside a *response* to a round we initiated — so responses must not be
/// subjected to the same check, or the pair is wedged for good.
#[tokio::test]
async fn a_peer_that_regenerated_its_uuid_can_still_be_pulled_from() {
    use crate::sync::{ExchangeInterface, SyncEnvelope, SyncMessage, SyncResponse};
    use crate::types::{Entry, Metadata};

    #[derive(Clone)]
    struct Rotated;

    impl ExchangeInterface for Rotated {
        fn uuid(&self) -> Vec<u8> {
            uuid_for(1)
        }

        // What we have on record for peer 2 — stale, because its data dir was recreated.
        fn query_uuid(&self, node_id: NodeId) -> Option<Vec<u8>> {
            Some(uuid_for(node_id))
        }

        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            _msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            anyhow::bail!("the v1 leg is not under test here")
        }

        async fn sync_v2_to(
            &self,
            _node: &Node,
            _peer: u32,
            _env: SyncEnvelope,
        ) -> anyhow::Result<Option<SyncEnvelope>> {
            // Same node id, new identity — and carrying the record that would teach us
            // the new uuid.
            let mut response = SyncEnvelope::new(2, b"regenerated-after-a-rebuild".to_vec());
            response.entries.push(Entry::new(
                "node/2".to_string(),
                Some(b"regenerated-after-a-rebuild".to_vec()),
                Metadata::new(2, 1, 1),
            ));
            Ok(Some(response))
        }
    }

    let local = Node::new(1, vec![2]);
    let sync = SyncManager::new(local.clone(), Rotated);
    sync.bootstrap().await.unwrap();

    assert!(
        local.read().get("node/2").is_some(),
        "rejecting this response would close the only path by which the peer's new \
         identity can reach us, and the pair would never converge again"
    );
}

/// Node ids are the addressing scheme, so reusing one across two machines silently
/// merges their sequence spaces. The UUID check is what catches that, and it must work
/// on *every* inbound entry point — v1, v2 and push. Covering only two of the three is
/// how a sender that never stamped its push envelopes went unnoticed.
#[tokio::test]
async fn a_reused_node_id_is_rejected_on_every_entry_point() {
    use crate::sync::{ExchangeInterface, SyncEnvelope, SyncMessage, SyncResponse};

    #[derive(Clone)]
    struct KnownPeers;

    impl ExchangeInterface for KnownPeers {
        fn uuid(&self) -> Vec<u8> {
            b"me".to_vec()
        }
        fn query_uuid(&self, node_id: u32) -> Option<Vec<u8>> {
            (node_id == 2).then(|| b"the-real-node-2".to_vec())
        }
        async fn sync_to(
            &self,
            _node: &Node,
            _peer: u32,
            _msg: SyncMessage,
        ) -> anyhow::Result<SyncResponse> {
            anyhow::bail!("not used")
        }
    }

    let sync = SyncManager::new(Node::new(1, vec![2]), KnownPeers);

    let mut impostor = SyncEnvelope::new(2, b"someone-else".to_vec());
    impostor.acks.insert(2, 1);
    let err = sync.handle_envelope(impostor).unwrap_err();
    assert!(err.to_string().contains("UUID mismatch"), "{err}");

    let v1_impostor = SyncMessage {
        sender_id: 2,
        sender_uuid: b"someone-else".to_vec(),
        sender_ack: Default::default(),
        entries: vec![],
    };
    let err = sync.handle_sync(v1_impostor).unwrap_err();
    assert!(err.to_string().contains("UUID mismatch"), "{err}");

    // The push channel is the third entry point and is checked exactly like the other
    // two. An envelope that reaches it *unstamped* is what a sender that forgot to fill
    // `sender_uuid` produces, so this also pins the failure mode down.
    let mut push_impostor = SyncEnvelope::new(2, b"someone-else".to_vec());
    push_impostor.push_only = true;
    let err = sync.handle_push(push_impostor).unwrap_err();
    assert!(err.to_string().contains("UUID mismatch"), "{err}");

    let mut unstamped = SyncEnvelope::new(2, Vec::new());
    unstamped.push_only = true;
    let err = sync.handle_push(unstamped).unwrap_err();
    assert!(
        err.to_string().contains("UUID mismatch"),
        "an unstamped push must be rejected, not silently merged: {err}"
    );

    // The genuine peer is still served on every entry point.
    let genuine = SyncEnvelope::new(2, b"the-real-node-2".to_vec());
    assert!(sync.handle_envelope(genuine).is_ok());

    let mut genuine_push = SyncEnvelope::new(2, b"the-real-node-2".to_vec());
    genuine_push.push_only = true;
    assert!(sync.handle_push(genuine_push).is_ok());
}

/// `remove_peer` must survive a restart, which constrains what else it may keep.
///
/// The snapshot container is byte-identical to v1 (RFC 8.3) and carries one `peers` map
/// that does double duty: membership, and the origins we hold coverage for. The
/// serializer folds every acked origin into it and the deserializer turns every key back
/// into a member. So an ack kept past the membership resurrects the peer on the next
/// load — which is why `RemovePeer` drops the ack even though that strands the
/// tombstones the departed node authored. This pins the direction of that trade, since
/// it is not visible from either side alone.
#[tokio::test]
async fn retiring_a_peer_survives_a_restart() {
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![2, 3], dir.path()).unwrap();

    // Give origin 2 real coverage, as any live cluster would.
    let mut from_2 = crate::sync::SyncEnvelope::new(2, uuid_for(2));
    from_2.acks.insert(2, 4);
    store.write().handle_envelope(from_2, uuid_for(1)).unwrap();

    store.write().remove_peer(2).unwrap();
    assert_eq!(store.read().get_peers(), vec![3]);
    store.write().persist_to_disk().unwrap();
    drop(store);

    let reloaded = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
    assert_eq!(
        reloaded.read().get_peers(),
        vec![3],
        "a retired peer that comes back on restart makes remove_peer advisory, and \
         pins the GC watermark on a node that is gone"
    );
}

/// Membership must survive a crash, because the tombstone GC watermark is a minimum
/// over *known* peers — and a minimum over a smaller set is larger.
///
/// A peer added but not yet captured in a snapshot used to vanish on restart. The node
/// would then compute the watermark without it and collect tombstones that peer had
/// never covered, so the peer's next round would resurrect every key it still held as a
/// live value. Deletion is the one operation this store cannot afford to lose.
#[tokio::test]
async fn a_peer_added_before_a_crash_is_still_a_peer_after_it() {
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    store.write().add_peer(3).unwrap();
    // Crash: no snapshot, only whatever reached the WAL.
    drop(store);

    let recovered = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    assert!(
        recovered.read().get_peers().contains(&3),
        "losing a peer silently widens the GC watermark and resurrects its deletes"
    );
}

/// A node rebuilt from an empty data directory must not take a requester's word for
/// what it holds.
///
/// The requester computes its delta against a *cached* view of our coverage. That cache
/// is stale in the dangerous direction here — it still records the coverage we had
/// before the rebuild — so the delta arrives nearly empty. Merging it is "complete" by
/// every local test, and adopting the acks that came with it makes us claim coverage of
/// state we do not hold. Those claims then propagate into every peer's `peer_acks` and
/// feed the tombstone GC watermark, where they can retire a tombstone nobody has seen.
#[tokio::test]
async fn a_node_mid_bootstrap_does_not_adopt_a_requesters_coverage() {
    use crate::sync::SyncEnvelope;

    let rebuilt = Node::new(1, vec![2]);
    rebuilt.write().begin_bootstrap();

    // A peer that believes we are fully caught up sends us almost nothing.
    let mut request = SyncEnvelope::new(2, uuid_for(2));
    request.acks.insert(2, 99);
    request.acks.insert(3, 99);
    rebuilt
        .write()
        .handle_envelope(request, uuid_for(1))
        .unwrap();

    assert!(
        rebuilt.read().acks_snapshot().is_empty(),
        "claiming coverage while holding nothing is the hole-INV violation itself: {:?}",
        rebuilt.read().acks_snapshot()
    );
}

/// The guard is a window, not a mode: once bootstrap has run, ordinary adoption resumes.
/// Leaving it armed would stop coverage advancing for the life of the process.
#[tokio::test]
async fn adoption_resumes_once_bootstrap_has_finished() {
    use crate::sync::{SyncConfig, SyncEnvelope};

    let node = Node::new(1, vec![2]);
    let peer = PeerEndpoint::new(2, vec![1]);
    let sync = SyncManager::with_config(
        node.clone(),
        DirectLink {
            me: 1,
            target: peer.clone(),
        },
        SyncConfig::default(),
    );
    sync.bootstrap().await.unwrap();

    let mut request = SyncEnvelope::new(2, uuid_for(2));
    request.acks.insert(2, 5);
    node.write().handle_envelope(request, uuid_for(1)).unwrap();

    assert_eq!(
        node.read().acks_snapshot().get(&2).copied(),
        Some(5),
        "bootstrap must disarm the guard it armed"
    );
}

/// Losing a snapshot generation must not hand a sequence number back out.
///
/// `persist_to_disk` rotates the primary snapshot to `.bak` and then resets the WAL, so
/// if the primary later turns out to be unreadable the node falls back to `.bak` — one
/// whole generation of its own writes older, with no WAL left to replay the difference.
/// Rebuilding `next_seq` from that state alone under-counts.
///
/// The recovery is the peers': they still cover the seqs this node has forgotten, which
/// is why `bootstrap` syncs before it rebuilds the counter. This pins that path, since
/// it is the only thing standing between a lost generation and silent write loss.
#[tokio::test]
async fn a_lost_snapshot_generation_does_not_reuse_seqs_a_peer_still_covers() {
    use crate::sync::SyncConfig;
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();

    // Generation one, captured in a snapshot.
    store.write().put("k1".to_string(), b"v".to_vec()).unwrap();
    store.write().persist_to_disk().unwrap();
    // Generation two: rotates the first to .bak, then resets the WAL.
    let latest = store.write().put("k2".to_string(), b"v".to_vec()).unwrap();
    store.write().persist_to_disk().unwrap();
    assert_eq!(latest.meta.seq, 2);
    drop(store);

    // The primary generation is unreadable; only .bak survives, and it predates seq 2.
    let primary = dir.path().join("node_1.snapshot");
    fs_err::write(&primary, b"not a snapshot").unwrap();

    let recovered = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    assert!(
        recovered.read().get("k2").is_none(),
        "the fixture depends on the newer generation actually being lost"
    );

    // The peer still remembers covering us up to seq 2.
    let peer = PeerEndpoint::new(2, vec![1]);
    let mut ours_before_the_loss = crate::sync::SyncEnvelope::new(1, uuid_for(1));
    ours_before_the_loss.acks.insert(1, 2);
    peer.node()
        .write()
        .handle_envelope(ours_before_the_loss, uuid_for(2))
        .unwrap();

    let sync = SyncManager::with_config(
        recovered.clone(),
        DirectLink {
            me: 1,
            target: peer.clone(),
        },
        SyncConfig::default(),
    );
    sync.bootstrap().await.unwrap();

    let next = recovered
        .write()
        .put("after".to_string(), b"v".to_vec())
        .unwrap();
    assert!(
        next.meta.seq > 2,
        "seq {} is still covered by the peer, which will filter the write out",
        next.meta.seq
    );
}

/// The third way an own write leaves the live index: its tombstone gets collected.
///
/// Coverage of that seq survives at every peer, so the counter must not step back over
/// it. Two independent things prevent that, and neither was pinned: the snapshot carries
/// `next_seq` explicitly, and it also carries our own ack, which `recover_next_seq`
/// takes the maximum with. Removing either alone leaves this green; removing both turns
/// it red, which is what says the test constrains something rather than nothing.
#[tokio::test]
async fn recovery_does_not_reuse_a_seq_whose_tombstone_was_collected() {
    use tempfile::TempDir;

    let dir = TempDir::new().unwrap();
    let store = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
    store
        .write()
        .put("gone".to_string(), b"v".to_vec())
        .unwrap();
    store.write().delete("gone".to_string()).unwrap();
    let tombstone_seq = store
        .read()
        .get_all_including_tombstones()
        .get("gone")
        .expect("tombstone")
        .meta
        .seq;
    assert_eq!(tombstone_seq, 2);

    // No peers, so the watermark is trivially satisfied and the tombstone is collected.
    assert_eq!(store.write().collect_tombstone_garbage().unwrap(), 1);
    assert!(store.read().get_all_including_tombstones().is_empty());
    store.write().persist_to_disk().unwrap();
    drop(store);

    let recovered = Node::new_with_persistence(1, vec![], dir.path()).unwrap();
    recovered.write().recover_next_seq();
    let next = recovered
        .write()
        .put("after".to_string(), b"v".to_vec())
        .unwrap();
    assert!(
        next.meta.seq > 2,
        "seq {} was spent by a write whose tombstone has since been collected",
        next.meta.seq
    );
}

/// `digest_check_rounds` must be a threshold, not decoration.
///
/// Repair is expensive — it forces a full re-exchange — and a single mismatched round
/// is not evidence of divergence: the two sides can simply have observed each other
/// mid-update. The existing repair tests loop until convergence, so they pass whether
/// the threshold is three rounds or one, and mutation testing duly found `>=` could
/// become `<` unnoticed.
#[tokio::test]
async fn repair_waits_for_the_configured_number_of_quiescent_rounds() {
    use crate::sync::{SyncConfig, SyncEnvelope};

    let a = Node::new(1, vec![2]);
    let b = PeerEndpoint::new(2, vec![1]);
    a.write()
        .put("only-on-a".to_string(), b"1".to_vec())
        .unwrap();
    b.node()
        .write()
        .put("only-on-b".to_string(), b"2".to_vec())
        .unwrap();

    // Each side claims coverage of the other without holding its data, so both deltas
    // are empty and every round is quiescent-but-mismatched.
    let full_coverage = |sender: NodeId| {
        let mut env = SyncEnvelope::new(sender, vec![]);
        env.acks.insert(1, 1);
        env.acks.insert(2, 1);
        env
    };
    a.write().handle_envelope(full_coverage(2), vec![]).unwrap();
    b.node()
        .write()
        .handle_envelope(full_coverage(1), vec![])
        .unwrap();

    let sync = SyncManager::with_config(
        a.clone(),
        DirectLink {
            me: 1,
            target: b.clone(),
        },
        SyncConfig {
            digest_check_rounds: 3,
            ..Default::default()
        },
    );

    // Two quiescent mismatched rounds are below the threshold: no repair, and the
    // counter is still climbing.
    for expected in 1..3u32 {
        sync.bootstrap().await.unwrap();
        assert_eq!(
            sync.link_status()
                .iter()
                .find(|s| s.id == 2)
                .map(|s| s.digest_mismatches),
            Some(expected),
            "the counter must climb, not trigger, below the threshold"
        );
        assert!(
            a.read().get("only-on-b").is_none(),
            "repair fired after {expected} round(s); the threshold is 3"
        );
    }

    // The third crosses it, which arms the reset; the re-exchange it asks for arrives
    // on the following round.
    sync.bootstrap().await.unwrap();
    assert!(
        a.read().get("only-on-b").is_none(),
        "crossing the threshold arms the repair, it does not carry the data itself"
    );
    sync.bootstrap().await.unwrap();
    assert!(a.read().get("only-on-b").is_some());
}

/// Divergence is only meaningful when *both* sides had nothing to send, and the two
/// halves of that test are not interchangeable.
///
/// The discriminating case is a round where the peer *did* send entries but none of them
/// changed our state — a retransmission of things we already hold. `merged == 0` is
/// true, `peer_delta_empty` is false. Under `&&` that is not quiescence and the counter
/// stays put; under `||` it counts, and a link that merely retransmits can drive an
/// unnecessary full re-exchange. A fixture where both halves are false cannot tell the
/// two apart, which is why this one arranges for exactly one to hold.
#[tokio::test]
async fn a_round_that_only_retransmitted_is_not_counted_as_quiescent() {
    use crate::sync::{SyncConfig, SyncEnvelope};

    let a = Node::new(1, vec![2]);
    let b = PeerEndpoint::new(2, vec![1]);

    // A holds something of its own that B will not learn about this round.
    a.write()
        .put("only-on-a".to_string(), b"1".to_vec())
        .unwrap();
    // Both hold the same B-authored entry, so B resending it changes nothing on A.
    let shared = b
        .node()
        .write()
        .put("shared".to_string(), b"v".to_vec())
        .unwrap();
    a.write().sync(shared).unwrap();

    // Tell A that B already covers A's own origin, so A's request carries no entries
    // for it and the two stay diverged for the round.
    let mut b_claims_our_origin = SyncEnvelope::new(2, vec![]);
    b_claims_our_origin.acks.insert(1, 1);
    a.write()
        .handle_envelope(b_claims_our_origin, vec![])
        .unwrap();

    let sync = SyncManager::with_config(
        a.clone(),
        DirectLink {
            me: 1,
            target: b.clone(),
        },
        SyncConfig {
            // Above 1 deliberately: at 1 the repair fires the moment the counter moves
            // and resets it, so the counter cannot witness whether it moved at all.
            digest_check_rounds: 3,
            ..Default::default()
        },
    );
    sync.bootstrap().await.unwrap();

    assert_ne!(
        a.state_digest(),
        b.node().state_digest(),
        "the fixture depends on the round ending diverged"
    );
    assert_eq!(
        sync.link_status()
            .iter()
            .find(|s| s.id == 2)
            .map(|s| s.digest_mismatches),
        Some(0),
        "the peer retransmitted an entry we already held; that is not a quiescent round"
    );
}

/// A merge round appends once.
///
/// `write_ops` fsyncs, and an fsync is four to five orders of magnitude dearer
/// than the append it follows: 91 us against 1 us on an NVMe ext4 host, and
/// milliseconds on the virtio disk a CVM actually gets. Appending per entry made
/// a catch-up of N entries cost N fsyncs, serially, under this node's write
/// lock — so a node rejoining a cluster stalled every reader for as long as it
/// took to write the backlog one record at a time.
#[tokio::test]
async fn a_merged_batch_is_appended_to_the_wal_once() {
    use crate::sync::SyncEnvelope;

    let dir = tempfile::tempdir().unwrap();
    let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();

    let before = node.read().wal_sync_count();
    let mut env = SyncEnvelope::new(2, uuid_for(2));
    env.push_only = true;
    env.entries = (0..200)
        .map(|i| {
            Entry::new_put(
                Metadata::new(2, i + 1, 1_000),
                format!("key-{i}"),
                b"value".to_vec(),
            )
        })
        .collect();
    node.write().merge_push(env).unwrap();

    assert_eq!(
        node.read().wal_sync_count() - before,
        1,
        "200 merged entries must cost one fsync, not one each"
    );
    assert_eq!(node.read().get("key-199").unwrap().value.unwrap(), b"value");
}

/// The batch defers the append; it must not lose it. Everything the round
/// merged has to survive a restart that has only the log to read.
#[tokio::test]
async fn a_batch_deferred_append_still_reaches_the_log() {
    use crate::sync::SyncEnvelope;

    let dir = tempfile::tempdir().unwrap();
    {
        let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
        let mut env = SyncEnvelope::new(2, uuid_for(2));
        env.push_only = true;
        env.entries = (0..50)
            .map(|i| {
                Entry::new_put(
                    Metadata::new(2, i + 1, 1_000),
                    format!("key-{i}"),
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect();
        node.write().merge_push(env).unwrap();
        // No snapshot: the WAL is the only record of the round.
    }

    let recovered = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();
    for i in 0..50 {
        assert_eq!(
            recovered
                .read()
                .get(&format!("key-{i}"))
                .and_then(|entry| entry.value),
            Some(format!("value-{i}").into_bytes()),
            "entry {i} was merged but never made it to the log"
        );
    }
}

/// Two entries for the same key in one batch still resolve by LWW.
///
/// This is why the batch defers only the append. `StateOp::Set` applies
/// unconditionally — the comparison lives in `sync`, against the live map — so a
/// batch that decided every entry up front and applied them afterwards would let
/// whichever entry came last win, regardless of which one LWW picks.
#[tokio::test]
async fn a_batch_holding_two_versions_of_a_key_keeps_the_later_one() {
    use crate::sync::SyncEnvelope;

    let dir = tempfile::tempdir().unwrap();
    let node = Node::new_with_persistence(1, vec![2, 3], dir.path()).unwrap();

    let mut env = SyncEnvelope::new(2, uuid_for(2));
    env.push_only = true;
    env.entries = vec![
        Entry::new_put(Metadata::new(2, 1, 2_000), "k".into(), b"newer".to_vec()),
        Entry::new_put(Metadata::new(3, 1, 1_000), "k".into(), b"older".to_vec()),
    ];
    node.write().merge_push(env).unwrap();

    assert_eq!(
        node.read().get("k").unwrap().value.unwrap(),
        b"newer",
        "the batch applied the loser after the winner"
    );

    // And the same holds across a restart, so the log agrees with memory.
    drop(node);
    let recovered = Node::new_with_persistence(1, vec![2, 3], dir.path()).unwrap();
    assert_eq!(recovered.read().get("k").unwrap().value.unwrap(), b"newer");
}

/// A single `sync` outside a batch keeps appending immediately: the deferral is
/// scoped to a merge round, not a new default for every write.
#[tokio::test]
async fn a_write_outside_a_batch_is_appended_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let node = Node::new_with_persistence(1, vec![2], dir.path()).unwrap();

    let before = node.read().wal_sync_count();
    node.write().put("a".to_string(), b"1".to_vec()).unwrap();
    node.write().put("b".to_string(), b"2".to_vec()).unwrap();
    node.write()
        .sync(Entry::new_put(
            Metadata::new(2, 1, 1_000),
            "c".into(),
            b"3".to_vec(),
        ))
        .unwrap();

    assert_eq!(
        node.read().wal_sync_count() - before,
        3,
        "writes outside a merge round must each be durable when they return"
    );
}
