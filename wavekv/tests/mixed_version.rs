//! Mixed-version compatibility suite (RFC 0001 section 8.5).
//!
//! Every node here is real: v2 nodes are this crate, v1 nodes are the **unmodified
//! `wavekv` 1.0 crate from crates.io**, pulled in as a dev-dependency. Nothing is
//! stubbed, so these tests exercise the actual v1 server and client logic that a
//! rolling upgrade will meet in production.
//!
//! Every cross-version message crosses a **msgpack round-trip** rather than being
//! passed as a Rust value. The two crates define structurally identical but distinct
//! types, so the round-trip is what proves wire compatibility: change a field on either
//! side and these tests fail rather than silently passing.

use std::collections::BTreeMap;

use wavekv::digest::StateDigest;
use wavekv::sync::{SyncEnvelope, SyncMessage, SyncResponse};
use wavekv::types::Entry;
use wavekv::Node as V2Node;

use wavekv_v1::sync::{
    ExchangeInterface as V1Exchange, SyncManager as V1SyncManager, SyncMessage as V1SyncMessage,
    SyncResponse as V1SyncResponse,
};
use wavekv_v1::Node as V1Node;

// ---------------------------------------------------------------------------
// Wire bridge
// ---------------------------------------------------------------------------

/// Re-encode a value as msgpack and decode it as the peer crate's type.
///
/// This is the whole point of the suite: it succeeds only while the two versions agree
/// on the positional encoding of the shared structs.
fn over_the_wire<A: serde::Serialize, B: serde::de::DeserializeOwned>(value: &A) -> B {
    let bytes = rmp_serde::to_vec(value).expect("encode");
    rmp_serde::from_slice(&bytes).expect("the v1 and v2 wire formats must stay identical")
}

/// A v1 node plus the real v1 `SyncManager` used to serve inbound requests.
struct V1Peer {
    node: V1Node,
    manager: V1SyncManager<NoopNet>,
    id: u32,
}

#[derive(Clone)]
struct NoopNet;

impl V1Exchange for NoopNet {
    async fn sync_to(
        &self,
        _node: &V1Node,
        _peer: u32,
        _msg: V1SyncMessage,
    ) -> anyhow::Result<V1SyncResponse> {
        anyhow::bail!("outbound v1 sync is driven explicitly by the harness")
    }
}

impl V1Peer {
    fn new(id: u32, peers: Vec<u32>) -> Self {
        let node = V1Node::new(id, peers);
        Self {
            manager: V1SyncManager::new(node.clone(), NoopNet),
            node,
            id,
        }
    }

    fn with_dir(id: u32, peers: Vec<u32>, dir: &std::path::Path) -> Self {
        let node = V1Node::new_with_persistence(id, peers, dir).expect("v1 persistence");
        Self {
            manager: V1SyncManager::new(node.clone(), NoopNet),
            node,
            id,
        }
    }

    fn put(&self, key: &str, value: &str) {
        self.node
            .write()
            .put(key.to_string(), value.as_bytes().to_vec())
            .expect("v1 put");
    }

    fn delete(&self, key: &str) {
        self.node
            .write()
            .delete(key.to_string())
            .expect("v1 delete");
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.node.read().get(key).and_then(|e| e.value)
    }

    /// Build the request a real v1 node would send, mirroring `SyncManager::sync_to`.
    fn build_request(&self, peer: u32) -> V1SyncMessage {
        let state = self.node.read();
        let peer_ack_for_us = state.get_peer_state(peer).map_or(0, |p| p.peer_ack);
        let entries = state
            .get_peer_logs_since(self.id, peer_ack_for_us)
            .unwrap_or_default();
        V1SyncMessage {
            sender_id: self.id,
            sender_uuid: Vec::new(),
            sender_ack: state.get_local_ack(),
            entries,
        }
    }

    fn digest(&self) -> StateDigest {
        let data: BTreeMap<String, Entry> = self
            .node
            .read()
            .get_all_including_tombstones()
            .iter()
            .map(|(k, v)| (k.clone(), over_the_wire::<_, Entry>(v)))
            .collect();
        StateDigest::compute(&data)
    }
}

fn v2_digest(node: &V2Node) -> StateDigest {
    node.state_digest()
}

// ---------------------------------------------------------------------------
// Round drivers — one per direction in the RFC 8.2.2 behaviour matrix
// ---------------------------------------------------------------------------

/// v1 initiates against a v2 responder (the shim path).
fn round_v1_to_v2(v1: &V1Peer, v2: &V2Node) {
    let request = v1.build_request(v2.read().id);
    let as_v2: SyncMessage = over_the_wire(&request);
    let response = v2.write().handle_sync_v1(as_v2).expect("shim response");
    let as_v1: V1SyncResponse = over_the_wire(&response);
    v1.node
        .write()
        .apply_pulled_entries(as_v1)
        .expect("v1 client consumes the shim response");
}

/// v2 initiates against a v1 responder. Per RFC 8.2.1 the push is deliberately empty.
fn round_v2_to_v1(v2: &V2Node, v1: &V1Peer) {
    let request = {
        let state = v2.read();
        SyncMessage {
            sender_id: state.id,
            sender_uuid: Vec::new(),
            sender_ack: state.acks_snapshot(),
            entries: Vec::new(),
        }
    };
    let as_v1: V1SyncMessage = over_the_wire(&request);
    let response = v1.manager.handle_sync(as_v1).expect("v1 server response");
    let as_v2: SyncResponse = over_the_wire(&response);
    v2.write()
        .apply_v1_response(as_v2)
        .expect("v2 consumes a v1 response");
}

fn round_v1_to_v1(a: &V1Peer, b: &V1Peer) {
    let request = a.build_request(b.id);
    let response = b.manager.handle_sync(request).expect("v1 server response");
    a.node
        .write()
        .apply_pulled_entries(response)
        .expect("v1 client");
}

/// v2 <-> v2, exercising the envelope codec (named msgpack) on both hops.
fn round_v2_to_v2(a: &V2Node, b: &V2Node) {
    let peer = b.read().id;
    let request = a.read().prepare_sync(peer, Vec::new());
    let encoded = request.encode().expect("encode request");
    let decoded = SyncEnvelope::decode(&encoded).expect("decode request");

    let response = b.write().handle_envelope(decoded, Vec::new()).unwrap();
    let encoded = response.encode().expect("encode response");
    let decoded = SyncEnvelope::decode(&encoded).expect("decode response");

    let mut outcome = a.write().apply_envelope(decoded).unwrap();
    // Drain pagination the way SyncManager does.
    while let Some(cursor) = outcome.resume_from {
        let mut request = a.read().prepare_sync(peer, Vec::new());
        request.resume_from = Some(cursor);
        let response = b.write().handle_envelope(request, Vec::new()).unwrap();
        outcome = a.write().apply_envelope(response).unwrap();
    }
}

// ---------------------------------------------------------------------------
// 8.5.1 — mixed clusters converge
// ---------------------------------------------------------------------------

/// Run every ordered pair for a few rounds, whichever versions they are.
fn gossip(v1s: &[&V1Peer], v2s: &[&V2Node], rounds: usize) {
    for _ in 0..rounds {
        for a in v1s {
            for b in v1s {
                if a.id != b.id {
                    round_v1_to_v1(a, b);
                }
            }
            for b in v2s {
                round_v1_to_v2(a, b);
            }
        }
        for a in v2s {
            for b in v1s {
                round_v2_to_v1(a, b);
            }
            for b in v2s {
                if a.read().id != b.read().id {
                    round_v2_to_v2(a, b);
                }
            }
        }
    }
}

#[test]
fn a_cluster_of_two_v1_and_one_v2_converges() {
    let a = V1Peer::new(1, vec![2, 3]);
    let b = V1Peer::new(2, vec![1, 3]);
    let c = V2Node::new(3, vec![1, 2]);

    a.put("from-a", "1");
    b.put("from-b", "2");
    c.write().put("from-c".into(), b"3".to_vec()).unwrap();

    gossip(&[&a, &b], &[&c], 4);

    for key in ["from-a", "from-b", "from-c"] {
        assert!(a.get(key).is_some(), "v1 node 1 missing {key}");
        assert!(b.get(key).is_some(), "v1 node 2 missing {key}");
        assert!(c.read().get(key).is_some(), "v2 node 3 missing {key}");
    }
    assert_eq!(a.digest(), v2_digest(&c));
    assert_eq!(b.digest(), v2_digest(&c));
}

#[test]
fn a_cluster_of_one_v1_and_two_v2_converges() {
    let a = V1Peer::new(1, vec![2, 3]);
    let b = V2Node::new(2, vec![1, 3]);
    let c = V2Node::new(3, vec![1, 2]);

    a.put("from-a", "1");
    b.write().put("from-b".into(), b"2".to_vec()).unwrap();
    c.write().put("from-c".into(), b"3".to_vec()).unwrap();

    gossip(&[&a], &[&b, &c], 4);

    assert_eq!(a.digest(), v2_digest(&b));
    assert_eq!(v2_digest(&b), v2_digest(&c));
    assert_eq!(a.get("from-c").as_deref(), Some(b"3".as_ref()));
}

#[test]
fn concurrent_writes_to_one_key_resolve_identically_on_both_versions() {
    let a = V1Peer::new(1, vec![2]);
    let b = V2Node::new(2, vec![1]);

    a.put("contended", "from-v1");
    std::thread::sleep(std::time::Duration::from_millis(5));
    b.write()
        .put("contended".into(), b"from-v2".to_vec())
        .unwrap();

    gossip(&[&a], &[&b], 3);

    assert_eq!(a.digest(), v2_digest(&b), "LWW must pick the same winner");
    assert_eq!(a.get("contended").as_deref(), Some(b"from-v2".as_ref()));
}

// ---------------------------------------------------------------------------
// 8.5.2 — the P3 pivot: a v1 client adopts a v2 shim response correctly
// ---------------------------------------------------------------------------

#[test]
fn a_v1_client_adopts_coverage_from_the_shim_response() {
    let v1 = V1Peer::new(1, vec![2]);
    let v2 = V2Node::new(2, vec![1]);

    for i in 0..5 {
        v2.write()
            .put(format!("k{i}"), format!("v{i}").into_bytes())
            .unwrap();
    }

    // Before the round the v1 node knows nothing about node 2's writes.
    assert_eq!(
        v1.node.read().get_local_ack().get(&2).copied().unwrap_or(0),
        0
    );

    round_v1_to_v2(&v1, &v2);

    // `is_snapshot = true` made v1 adopt the responder's progress map and then merge —
    // exactly delta-state adoption semantics, with no change to v1's code.
    assert_eq!(
        v1.node.read().get_local_ack().get(&2).copied(),
        Some(5),
        "v1 must have adopted node 2's coverage claim"
    );
    for i in 0..5 {
        assert_eq!(
            v1.get(&format!("k{i}")).as_deref(),
            Some(format!("v{i}").as_bytes())
        );
    }
    assert_eq!(v1.digest(), v2_digest(&v2));

    // The next round is empty in both directions: adoption actually took effect.
    let request = v1.build_request(2);
    let as_v2: SyncMessage = over_the_wire(&request);
    let response = v2.write().handle_sync_v1(as_v2).unwrap();
    assert!(
        response.entries.is_empty(),
        "the delta filter must now exclude everything v1 already covers"
    );
}

#[test]
fn the_shim_never_paginates() {
    // A partial delta paired with a full progress claim is exactly the hole INV
    // forbids, and v1 has no way to signal "more pages follow". The shim must
    // therefore ignore the delta size caps.
    let cfg = wavekv::NodeConfig {
        max_delta_entries: 2,
        max_delta_bytes: 16,
        ..Default::default()
    };
    let v2 = V2Node::with_config(2, vec![1], cfg);
    for i in 0..25 {
        v2.write().put(format!("k{i}"), vec![b'x'; 64]).unwrap();
    }

    let v1 = V1Peer::new(1, vec![2]);
    let request = v1.build_request(2);
    let response = v2.write().handle_sync_v1(over_the_wire(&request)).unwrap();

    assert_eq!(
        response.entries.len(),
        25,
        "the v1 shim must send the complete delta regardless of page caps"
    );

    round_v1_to_v2(&v1, &v2);
    assert_eq!(v1.digest(), v2_digest(&v2));
}

// ---------------------------------------------------------------------------
// 8.5.5 — tombstones cross versions in both directions
// ---------------------------------------------------------------------------

#[test]
fn a_delete_on_v2_removes_the_key_on_v1() {
    let v1 = V1Peer::new(1, vec![2]);
    let v2 = V2Node::new(2, vec![1]);

    v2.write().put("doomed".into(), b"x".to_vec()).unwrap();
    gossip(&[&v1], &[&v2], 2);
    assert!(v1.get("doomed").is_some());

    v2.write().delete("doomed".into()).unwrap();
    gossip(&[&v1], &[&v2], 2);

    assert!(
        v1.get("doomed").is_none(),
        "a deregistered entry reappearing on a peer is the security-sensitive failure"
    );
    assert_eq!(v1.digest(), v2_digest(&v2));
}

#[test]
fn a_delete_on_v1_removes_the_key_on_v2() {
    let v1 = V1Peer::new(1, vec![2]);
    let v2 = V2Node::new(2, vec![1]);

    v1.put("doomed", "x");
    gossip(&[&v1], &[&v2], 2);
    assert!(v2.read().get("doomed").is_some());

    v1.delete("doomed");
    gossip(&[&v1], &[&v2], 2);

    assert!(v2.read().get("doomed").is_none());
    assert_eq!(v1.digest(), v2_digest(&v2));
}

// ---------------------------------------------------------------------------
// 8.5.3 / 8.5.4 — bootstrap and rollback across the version boundary
// ---------------------------------------------------------------------------

#[test]
fn a_v2_node_cold_starts_against_a_v1_peer() {
    let v1 = V1Peer::new(1, vec![2]);
    for i in 0..10 {
        v1.put(&format!("k{i}"), &format!("v{i}"));
    }

    let fresh = V2Node::new(2, vec![1]);
    round_v2_to_v1(&fresh, &v1);

    assert_eq!(fresh.read().get_all_including_tombstones().len(), 10);
    assert_eq!(v1.digest(), v2_digest(&fresh));
}

#[test]
fn a_v1_node_cold_starts_against_a_v2_peer() {
    let v2 = V2Node::new(2, vec![1]);
    for i in 0..10 {
        v2.write()
            .put(format!("k{i}"), format!("v{i}").into_bytes())
            .unwrap();
    }

    let fresh = V1Peer::new(1, vec![2]);
    round_v1_to_v2(&fresh, &v2);

    assert_eq!(fresh.node.read().get_all_including_tombstones().len(), 10);
    assert_eq!(fresh.digest(), v2_digest(&v2));
}

#[test]
fn a_node_written_by_v2_can_be_rolled_back_to_the_v1_binary() {
    let dir = tempfile::tempdir().unwrap();

    // Run as v2: write data, snapshot, then write more so the WAL is non-empty too.
    let expected = {
        let v2 = V2Node::new_with_persistence(7, vec![8], dir.path()).unwrap();
        for i in 0..5 {
            v2.write()
                .put(format!("k{i}"), format!("v{i}").into_bytes())
                .unwrap();
        }
        v2.persist().unwrap();
        v2.write()
            .put("after-snapshot".into(), b"z".to_vec())
            .unwrap();
        v2.write().delete("k0".into()).unwrap();
        v2_digest(&v2)
    };

    // Roll back: the v1 binary opens the same directory.
    let v1 = V1Peer::with_dir(7, vec![8], dir.path());

    assert_eq!(
        v1.digest(),
        expected,
        "v1 must recover the full v2 state from the shared snapshot + WAL"
    );
    assert_eq!(v1.get("after-snapshot").as_deref(), Some(b"z".as_ref()));
    assert!(
        v1.get("k0").is_none(),
        "the tombstone must survive rollback"
    );

    // And it must keep allocating fresh sequence numbers rather than reusing them.
    v1.put("post-rollback", "1");
    let seq = v1
        .node
        .read()
        .get_including_tombstones("post-rollback")
        .unwrap()
        .meta
        .seq;
    assert!(seq > 5, "v1 reused a sequence number after rollback: {seq}");
}

#[test]
fn a_node_written_by_v1_is_readable_after_upgrading_to_v2() {
    let dir = tempfile::tempdir().unwrap();

    let expected = {
        let v1 = V1Peer::with_dir(7, vec![8], dir.path());
        for i in 0..5 {
            v1.put(&format!("k{i}"), &format!("v{i}"));
        }
        v1.node.persist().unwrap();
        v1.put("after-snapshot", "z");
        v1.delete("k0");
        v1.digest()
    };

    let v2 = V2Node::new_with_persistence(7, vec![8], dir.path()).unwrap();

    assert_eq!(
        v2_digest(&v2),
        expected,
        "v2 must recover the full v1 state, including the log-manipulating WAL ops"
    );
    v2.write()
        .put("post-upgrade".into(), b"1".to_vec())
        .unwrap();
    let seq = v2
        .read()
        .get_including_tombstones("post-upgrade")
        .unwrap()
        .meta
        .seq;
    assert!(seq > 5, "v2 reused a sequence number after upgrade: {seq}");
}

// ---------------------------------------------------------------------------
// 8.5.6 — fault injection on the v2 path
// ---------------------------------------------------------------------------

#[test]
fn dropped_duplicated_and_reordered_envelopes_still_converge() {
    let a = V2Node::new(1, vec![2]);
    let b = V2Node::new(2, vec![1]);

    for i in 0..20 {
        a.write()
            .put(format!("a{i}"), format!("{i}").into_bytes())
            .unwrap();
        b.write()
            .put(format!("b{i}"), format!("{i}").into_bytes())
            .unwrap();
    }

    // Collect a batch of envelopes, then deliver them out of order, twice over, with
    // some dropped entirely. Merge is idempotent, commutative and associative, so none
    // of this may affect the outcome.
    let mut envelopes: Vec<SyncEnvelope> = (0..6)
        .map(|i| {
            let mut env = a.read().prepare_sync(2, Vec::new());
            env.push_only = true; // R3: data only, no ack authority
            if i % 2 == 0 {
                env.entries.reverse();
            }
            env
        })
        .collect();
    envelopes.reverse();
    for env in envelopes.iter().skip(1) {
        b.write().merge_push(env.clone()).unwrap();
        b.write().merge_push(env.clone()).unwrap(); // duplicate delivery
    }

    // The periodic round is the anti-entropy backstop and the only ack authority.
    round_v2_to_v2(&a, &b);
    round_v2_to_v2(&b, &a);

    assert_eq!(v2_digest(&a), v2_digest(&b));
}

#[test]
fn an_interrupted_pagination_loses_nothing() {
    let cfg = wavekv::NodeConfig {
        max_delta_entries: 3,
        ..Default::default()
    };
    let big = V2Node::with_config(1, vec![2], cfg);
    for i in 0..20 {
        big.write().put(format!("k{i:02}"), b"v".to_vec()).unwrap();
    }
    let small = V2Node::new(2, vec![1]);

    // Deliver exactly one page, then abandon the exchange.
    let request = small.read().prepare_sync(1, Vec::new());
    let page = big.write().handle_envelope(request, Vec::new()).unwrap();
    assert!(page.page.is_some_and(|p| !p.last));
    let outcome = small.write().apply_envelope(page).unwrap();
    assert!(
        !outcome.acks_adopted,
        "R2: acks must not move on a non-final page"
    );

    // A fresh exchange restarts the filter and completes.
    for _ in 0..12 {
        round_v2_to_v2(&small, &big);
    }
    assert_eq!(v2_digest(&big), v2_digest(&small));
}

#[test]
fn a_rejected_entry_parks_acks_instead_of_losing_data() {
    use std::sync::Arc;
    use wavekv::{Admission, NodeConfig};

    // Node 2 refuses anything outside `allowed/`.
    let picky = NodeConfig {
        admission: Some(Arc::new(|entry: &Entry| {
            if entry.key.starts_with("allowed/") {
                Admission::Accept
            } else {
                Admission::Reject {
                    reason: "outside the permitted prefix",
                }
            }
        })),
        ..Default::default()
    };

    let sender = V2Node::new(1, vec![2]);
    let receiver = V2Node::with_config(2, vec![1], picky);

    sender
        .write()
        .put("allowed/ok".into(), b"1".to_vec())
        .unwrap();
    sender
        .write()
        .put("denied/no".into(), b"2".to_vec())
        .unwrap();

    round_v2_to_v2(&sender, &receiver);

    assert!(receiver.read().get("allowed/ok").is_some());
    assert!(receiver.read().get("denied/no").is_none());
    assert_eq!(
        receiver
            .read()
            .acks_snapshot()
            .get(&1)
            .copied()
            .unwrap_or(0),
        0,
        "R1: a per-entry rejection must block ack adoption for the whole round, so the \
         peer keeps re-offering rather than silently losing the entry"
    );
    assert_eq!(receiver.read().status().entries_rejected, 1);
}

#[test]
fn lowering_an_ack_repairs_divergence_instead_of_losing_data() {
    let a = V2Node::new(1, vec![2]);
    let b = V2Node::new(2, vec![1]);

    a.write().put("k".into(), b"v".to_vec()).unwrap();
    round_v2_to_v2(&a, &b);
    assert_eq!(v2_digest(&a), v2_digest(&b));

    // Simulate the repair path: b forgets what it covers of a.
    b.write().reset_peer_coverage(1);
    assert_eq!(b.read().acks_snapshot().get(&1).copied().unwrap_or(0), 0);

    // Because the data map is never truncated, the next round simply re-ships. In v1
    // the equivalent state (an ack behind a truncated log) forced the full-dump path.
    round_v2_to_v2(&b, &a);
    assert_eq!(v2_digest(&a), v2_digest(&b));
    assert_eq!(b.read().acks_snapshot().get(&1).copied(), Some(1));
}

// ---------------------------------------------------------------------------
// 8.5.7 — clock skew
// ---------------------------------------------------------------------------

#[test]
fn a_runaway_clock_cannot_poison_a_key_permanently() {
    use chrono::Utc;
    use wavekv::types::Metadata;

    let victim = V2Node::new(1, vec![2]);
    victim.write().put("k".into(), b"honest".to_vec()).unwrap();

    // A peer one hour into the future would win LWW against every honest write until
    // real time caught up.
    let far_future = Entry::new(
        "k".to_string(),
        Some(b"poison".to_vec()),
        Metadata::new(2, 1, Utc::now().timestamp_millis() + 3_600_000),
    );
    let mut env = SyncEnvelope::new(2, Vec::new());
    env.entries.push(far_future);

    let outcome = victim.write().apply_envelope(env).unwrap();
    assert_eq!(outcome.rejected, 1, "clamping must refuse the entry");
    assert!(
        !outcome.acks_adopted,
        "R1: the rejection also parks acks for the round"
    );
    assert_eq!(
        victim.read().get("k").unwrap().value,
        Some(b"honest".to_vec())
    );
}

#[test]
fn force_put_recovers_a_key_poisoned_before_clamping_existed() {
    use chrono::Utc;
    use wavekv::types::Metadata;

    // Admit the poisoned entry the way a pre-clamping node would have.
    let node = V2Node::with_config(
        1,
        vec![2],
        wavekv::NodeConfig {
            limits: wavekv::Limits {
                max_clock_drift: std::time::Duration::from_secs(86_400 * 365),
                ..Default::default()
            },
            ..Default::default()
        },
    );
    let poison_ts = Utc::now().timestamp_millis() + 3_600_000;
    let poison = || {
        let mut env = SyncEnvelope::new(2, Vec::new());
        env.entries.push(Entry::new(
            "k".to_string(),
            Some(b"poison".to_vec()),
            Metadata::new(2, 1, poison_ts),
        ));
        env
    };
    node.write().apply_envelope(poison()).unwrap();
    assert_eq!(
        node.read().get("k").unwrap().value,
        Some(b"poison".to_vec())
    );

    // A plain put is applied locally without an LWW check, so it *looks* like it
    // worked — but the poisoned entry is still live on the peer, and the next round
    // re-merges it and silently reverts the repair. That is the real failure mode.
    node.write().put("k".into(), b"repair".to_vec()).unwrap();
    assert_eq!(
        node.read().get("k").unwrap().value,
        Some(b"repair".to_vec())
    );
    node.write().apply_envelope(poison()).unwrap();
    assert_eq!(
        node.read().get("k").unwrap().value,
        Some(b"poison".to_vec()),
        "an honest timestamp loses to the future-stamped entry on every re-delivery"
    );

    // force_put stamps above the current winner, so the repair survives re-delivery.
    node.write()
        .force_put("k".into(), b"repair".to_vec())
        .unwrap();
    node.write().apply_envelope(poison()).unwrap();
    assert_eq!(
        node.read().get("k").unwrap().value,
        Some(b"repair".to_vec())
    );
    assert!(
        node.read()
            .get_including_tombstones("k")
            .unwrap()
            .meta
            .timestamp
            > poison_ts
    );
}
