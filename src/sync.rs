use crate::node::Node;
use crate::types::{Entry, NodeId};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, warn};

/// Bidirectional sync: sender includes their local_ack AND their new entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncMessage {
    pub sender_id: NodeId,
    /// How far the sender has synced each node's logs (local_ack)
    pub sender_ack: HashMap<NodeId, u64>,
    /// Sender's new log entries (incremental or full dump)
    pub entries: Vec<Entry>,
}

/// Unified log exchange response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub peer_id: NodeId,
    pub entries: Vec<Entry>,
    pub progress: HashMap<NodeId, u64>, // Responder's local_ack for each node
    pub is_snapshot: bool,              // Indicates if this is a full KV->log conversion
}

pub trait ExchangeInterface: Send + Sync + 'static {
    fn sync_to(
        &self,
        node: Arc<Node>,
        peer: NodeId,
        msg: SyncMessage,
    ) -> impl Future<Output = Result<SyncResponse>> + Send;
}

/// Simplified sync manager
pub struct SyncManager<Net: ExchangeInterface> {
    store: Arc<Node>,
    network: Arc<Net>,
}

impl<Net: ExchangeInterface> SyncManager<Net> {
    pub fn new(store: Arc<Node>, network: Arc<Net>) -> Self {
        Self { store, network }
    }

    /// Bootstrap: Sync from all peers and recover next_seq before starting local operations
    /// This is critical after data loss to avoid sequence number reuse
    pub async fn bootstrap(&self) -> Result<()> {
        let my_id = self.store.read().id;
        let peers = self.store.read().get_peers();

        if peers.is_empty() {
            info!("No peers to bootstrap from, starting fresh");
            return Ok(());
        }

        info!("Bootstrapping from {} peers...", peers.len());
        let mut success_count = 0;
        let mut max_seq_found = 0u64;

        // Sync from all peers
        for peer in &peers {
            match sync_to(self.store.clone(), self.network.clone(), *peer).await {
                Ok(_) => {
                    success_count += 1;
                    info!("Successfully bootstrapped from peer {}", peer);
                }
                Err(e) => {
                    warn!("Failed to bootstrap from peer {}: {}", peer, e);
                }
            }
        }

        // Scan all received entries to find our highest seq
        let store = self.store.read();
        for peer_state in store.get_all_peer_states().values() {
            for entry in &peer_state.log {
                if entry.meta.node == my_id && entry.meta.seq > max_seq_found {
                    max_seq_found = entry.meta.seq;
                }
            }
        }

        // Also check the main data store
        for entry in store.get_all_including_tombstones().values() {
            if entry.meta.node == my_id && entry.meta.seq > max_seq_found {
                max_seq_found = entry.meta.seq;
            }
        }
        drop(store);

        // Update next_seq if we found any of our own entries
        if max_seq_found > 0 {
            let new_next_seq = max_seq_found + 1;
            self.store.write().ensure_next_seq(new_next_seq);
        }

        if success_count == 0 && !peers.is_empty() {
            warn!("Bootstrap: Failed to sync from any peer, proceeding anyway");
        } else {
            info!(
                "Bootstrap: Successfully synced from {}/{} peers",
                success_count,
                peers.len()
            );
            let status = self.store.read().status();
            info!("Node status after bootstrap: {:#?}", status);
        }

        Ok(())
    }

    /// Start periodic log exchange with peers
    pub async fn start_sync_tasks(self: Arc<Self>) {
        let sync_manager = self.clone();
        tokio::spawn(async move {
            sync_manager.periodic_log_exchange().await;
        });
    }

    /// Periodic log exchange: send our logs to peers and request their logs
    async fn periodic_log_exchange(&self) {
        let mut ticker = interval(Duration::from_secs(5));

        loop {
            ticker.tick().await;

            for peer in self.store.read().get_peers() {
                let store = self.store.clone();
                let network = self.network.clone();

                tokio::spawn(async move {
                    if let Err(e) = sync_to(store, network, peer).await {
                        warn!("Failed to exchange logs with peer {peer}: {e}");
                    }
                });
            }
        }
    }

    /// Handle incoming sync message (bidirectional sync)
    ///
    /// Protocol:
    /// - Request contains only sender's own logs (entries from sender_id)
    /// - sender_ack serves dual purpose: progress report + "since" parameter
    /// - Response contains logs from ALL nodes based on sender_ack
    #[tracing::instrument(skip(self, msg), fields(from = msg.sender_id))]
    pub fn handle_sync(&self, msg: SyncMessage) -> Result<SyncResponse> {
        let peer_progress = msg.sender_ack.clone();
        let peer_id = msg.sender_id;
        // Step 1: Apply sender's entries (only contains sender_id's logs)
        self.store.write().apply_pushed_entries(msg)?;
        // Step 2: Use sender_ack to determine what to send back
        // Response can include logs from ALL nodes
        let store = self.store.read();
        let (entries, is_snapshot) = match store.get_peer_missing_logs(&peer_progress) {
            Some(entries) => {
                info!(
                    "Returning {} incremental log entries to node {peer_id}",
                    entries.len(),
                );
                (entries, false)
            }
            None => {
                let entries = store.kv_to_log_entries();
                info!(
                    "Returning snapshot ({} entries) to node {peer_id}",
                    entries.len(),
                );
                (entries, true)
            }
        };

        // Step 3: Include our progress so sender can update their peer_ack for us
        let my_progress = store.get_local_ack();

        // Step 4: Update our peer_ack assuming the peer will accept our logs
        let my_id = store.id;
        let peer_ack = *my_progress.get(&my_id).unwrap_or(&0);

        drop(store);

        let _ = self.store.write().update_peer_ack(peer_id, peer_ack);

        Ok(SyncResponse {
            peer_id: my_id,
            progress: my_progress,
            entries,
            is_snapshot,
        })
    }
}

/// Perform log exchange with a peer (bidirectional)
///
/// Protocol:
/// - Send only OUR node's logs (entries from store.id)
/// - Include our sender_ack (progress on all nodes)
/// - Peer responds with logs from ALL nodes they have
#[tracing::instrument(skip(store, network))]
async fn sync_to<Net: ExchangeInterface>(
    store: Arc<Node>,
    network: Arc<Net>,
    peer: NodeId,
) -> Result<()> {
    // Prepare our local_ack to send (tells peer what we've synced)
    let sender_ack = store.read().get_local_ack();

    // Get only OUR node's log entries that peer hasn't ack'd yet
    let peer_ack_for_us = store.read().get_peer_state(peer).map_or(0, |p| p.peer_ack);

    let sender_id = store.read().id;
    // Collect our entries with seq > peer_ack
    let entries = store
        .read()
        .get_peer_logs_since(sender_id, peer_ack_for_us)
        .unwrap_or_default();

    info!("Sending {} log entries to peer {peer}", entries.len());
    // Send bidirectional sync message
    let msg = SyncMessage {
        sender_id,
        sender_ack,
        entries,
    };

    match network.sync_to(store.clone(), peer, msg).await {
        Ok(response) => store.write().apply_pulled_entries(response),
        Err(e) => {
            warn!("Log exchange with peer {peer} failed: {e}");
            Err(e)
        }
    }
}
