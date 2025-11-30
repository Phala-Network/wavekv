use anyhow::{Context, Result};
use axum::{
    extract::State as AxumState,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::{Parser, Subcommand};
use fs_err as fs;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use serde::Serialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    sync::mpsc,
    task::spawn_blocking,
    time::{interval, Duration},
};
use tracing::{error, info, warn};
use wavekv::{
    node::Node,
    sync::{ExchangeInterface, SyncManager, SyncMessage, SyncResponse},
    types::NodeId,
};

const FILE_KEY_PREFIX: &str = "__file/";
const PEER_ADDR_PREFIX: &str = "__peer_addr/";

#[derive(Parser, Debug)]
#[command(name = "filesync")]
#[command(about = "File synchronization using WaveKV", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a filesync node
    Run {
        /// Node ID (1, 2, 3, ...)
        #[arg(short, long)]
        id: NodeId,

        /// Directory to watch
        #[arg(short, long)]
        watch_dir: PathBuf,

        /// Data directory for persistence
        #[arg(short, long)]
        data_dir: PathBuf,

        /// HTTP listen address (e.g., 127.0.0.1:8001)
        #[arg(short, long)]
        addr: String,

        /// Peer addresses (e.g., "2=http://127.0.0.1:8002,3=http://127.0.0.1:8003")
        #[arg(short, long, default_value = "")]
        peers: String,
    },
    /// Check consistency across monitored directories
    Check {
        /// Directories to check (comma-separated)
        #[arg(short, long)]
        dirs: String,
    },
}

struct SyncManagerState {
    sync_manager: Arc<SyncManager<HttpNetwork>>,
    node: Node,
    watch_dir: PathBuf,
}

fn encode_file_key(name: &str) -> String {
    format!("{FILE_KEY_PREFIX}{name}")
}

fn decode_file_key(key: &str) -> Option<&str> {
    key.strip_prefix(FILE_KEY_PREFIX)
}

#[derive(Clone)]
struct HttpNetwork {
    client: reqwest::Client,
}

impl HttpNetwork {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Get peer address from DB using key prefix __peer_addr/{node_id}
    fn get_peer_addr(node: &Node, peer_id: NodeId) -> Option<String> {
        let key = format!("{PEER_ADDR_PREFIX}{peer_id}");
        node.read()
            .get(&key)
            .and_then(|entry| entry.value.clone())
            .and_then(|bytes| String::from_utf8(bytes).ok())
    }

    /// Register this node's address in DB
    fn register_node_addr(node: &Node, my_id: NodeId, my_addr: String) -> Result<()> {
        let key = format!("{PEER_ADDR_PREFIX}{my_id}");
        node.write().put(key, my_addr.into_bytes())?;
        info!("Registered node {} address in DB", my_id);
        Ok(())
    }
}

impl ExchangeInterface for HttpNetwork {
    async fn sync_to(&self, node: &Node, peer: NodeId, msg: SyncMessage) -> Result<SyncResponse> {
        // Dynamically read peer address from DB
        let addr = Self::get_peer_addr(node, peer)
            .ok_or_else(|| anyhow::anyhow!("Peer {} address not found in DB", peer))?;

        let url = format!("{}/sync", addr);
        let resp = self
            .client
            .post(&url)
            .json(&msg)
            .send()
            .await
            .context("Failed to send sync request")?;

        let sync_resp = resp
            .json::<SyncResponse>()
            .await
            .context("Failed to parse sync response")?;

        Ok(sync_resp)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            id,
            watch_dir,
            data_dir,
            addr,
            peers,
        } => {
            run_node(id, watch_dir, data_dir, addr, peers).await?;
        }
        Commands::Check { dirs } => {
            check_consistency(dirs)?;
        }
    }

    Ok(())
}

async fn run_node(
    id: NodeId,
    watch_dir: PathBuf,
    data_dir: PathBuf,
    addr: String,
    peers_str: String,
) -> Result<()> {
    // Parse initial peers (only IDs and addresses for bootstrapping)
    let mut peer_ids = Vec::new();
    let mut initial_peer_addrs = HashMap::new();

    if !peers_str.is_empty() {
        for peer in peers_str.split(',') {
            let parts: Vec<&str> = peer.split('=').collect();
            if parts.len() == 2 {
                let peer_id: NodeId = parts[0].parse()?;
                let peer_addr = parts[1].to_string();
                peer_ids.push(peer_id);
                initial_peer_addrs.insert(peer_id, peer_addr);
            }
        }
    }

    // Create watch directory if it doesn't exist
    fs::create_dir_all(&watch_dir)?;
    fs::create_dir_all(&data_dir)?;

    info!("Starting filesync node {} watching {:?}", id, watch_dir);

    // Initialize wavekv node
    let node = Node::new_with_persistence(id, peer_ids.clone(), &data_dir)?;

    // Register this node's address in DB
    let my_addr = format!("http://{}", addr);
    HttpNetwork::register_node_addr(&node, id, my_addr)?;

    // Register initial peer addresses in DB (for bootstrapping)
    for (peer_id, peer_addr) in &initial_peer_addrs {
        HttpNetwork::register_node_addr(&node, *peer_id, peer_addr.clone())?;
    }

    // Load existing files from watch_dir into wavekv
    load_initial_files(&node, &watch_dir).await?;

    // Set up file watcher
    let (tx, mut rx) = mpsc::channel(100);
    let mut watcher: RecommendedWatcher =
        notify::recommended_watcher(move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        })?;

    watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

    // Create SyncManager (network layer reads addresses from DB)
    let sync_manager = Arc::new(SyncManager::new(node.clone(), HttpNetwork::new()));

    let state = Arc::new(SyncManagerState {
        sync_manager: sync_manager.clone(),
        node: node.clone(),
        watch_dir: watch_dir.clone(),
    });

    // Start HTTP server
    let app = Router::new()
        .route("/sync", post(handle_sync))
        .route("/debug/state", get(handle_debug_state))
        .route("/debug/files", get(handle_debug_files))
        .route("/debug/raw", get(handle_debug_raw))
        .with_state(state.clone());

    let socket_addr: SocketAddr = addr.parse()?;
    info!("HTTP server listening on {}", socket_addr);

    let server = axum::serve(tokio::net::TcpListener::bind(socket_addr).await?, app);

    // Bootstrap: Sync from peers to recover next_seq and avoid sequence number reuse
    info!("Bootstrapping from peers...");
    if let Err(e) = sync_manager.bootstrap().await {
        error!("Bootstrap failed: {}", e);
        error!("Proceeding anyway, but sequence number conflicts may occur");
    }

    // Start periodic sync using SyncManager
    sync_manager.clone().start_sync_tasks().await;

    // Periodic snapshot task: persist WaveKV every 10s if there were changes
    let node_for_persist = node.clone();
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            let node_clone = node_for_persist.clone();
            match spawn_blocking(move || node_clone.persist_if_dirty()).await {
                Ok(Ok(true)) => info!("Periodic persist: wrote snapshot to disk"),
                Ok(Ok(false)) => {}
                Ok(Err(err)) => error!("Periodic persist failed: {err}"),
                Err(join_err) => error!("Periodic persist task panicked: {join_err}"),
            }
        }
    });

    // Start file watcher task (local changes -> WaveKV)
    let node_for_watcher = node.clone();
    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Err(e) = handle_file_event(&node_for_watcher, event).await {
                error!("Error handling file event: {}", e);
            }
        }
    });

    // Start WaveKV watcher task (WaveKV changes -> filesystem)
    let watch_dir_clone = watch_dir.clone();
    let node_for_watch = node.clone();
    tokio::spawn(async move {
        // Initial sync: WaveKV -> filesystem
        if let Err(e) = sync_wavekv_to_filesystem(&node_for_watch, &watch_dir_clone).await {
            error!("Initial filesystem sync failed: {}", e);
        }

        let mut watcher = node_for_watch.write().watch_prefix(FILE_KEY_PREFIX);
        info!("Started WaveKV watcher for filesystem sync");

        loop {
            if watcher.changed().await.is_err() {
                warn!("WaveKV watcher channel closed");
                break;
            }

            // When watcher triggers, sync ALL files (not just the one that changed)
            // This is because tokio::sync::watch only keeps the latest value,
            // so rapid updates might be missed if we only sync the current key
            if let Err(e) = sync_wavekv_to_filesystem(&node_for_watch, &watch_dir_clone).await {
                error!("Watcher-triggered filesystem sync failed: {e:?}");
            }
        }
    });

    // Keep watcher alive
    let _watcher = watcher;

    server.await?;

    Ok(())
}

async fn load_initial_files(node: &Node, watch_dir: &Path) -> Result<()> {
    let entries = fs::read_dir(watch_dir)?;

    for entry in entries {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(filename) = path.file_name() {
                let filename = filename.to_string_lossy().to_string();
                let content = fs::read(&path)?;

                node.write().put(encode_file_key(&filename), content)?;
                info!("Loaded initial file: {}", filename);
            }
        }
    }

    Ok(())
}

async fn handle_file_event(node: &Node, event: Event) -> Result<()> {
    match event.kind {
        EventKind::Create(_) | EventKind::Modify(_) => {
            for path in event.paths {
                if path.is_file() {
                    if let Some(filename) = path.file_name() {
                        let filename = filename.to_string_lossy().to_string();
                        let content = fs::read(&path)?;

                        node.write().put(encode_file_key(&filename), content)?;
                        info!("File created/modified: {} (local)", filename);
                    }
                }
            }
        }
        EventKind::Remove(_) => {
            for path in event.paths {
                if let Some(filename) = path.file_name() {
                    let filename = filename.to_string_lossy().to_string();

                    node.write().delete(encode_file_key(&filename))?;
                    info!("File deleted: {} (local)", filename);
                }
            }
        }
        _ => {}
    }

    Ok(())
}

/// Sync all entries from WaveKV to filesystem
async fn sync_wavekv_to_filesystem(node: &Node, watch_dir: &Path) -> Result<()> {
    let all_entries = node.read().get_all_including_tombstones();

    for (key, entry) in all_entries {
        let Some(filename) = decode_file_key(&key) else {
            continue;
        };
        let file_path = watch_dir.join(filename);

        if let Some(content) = &entry.value {
            // Write file if it doesn't exist or content differs
            match fs::read(&file_path) {
                Ok(existing) if existing == *content => {
                    // Content already matches, skip
                }
                _ => {
                    fs::write(&file_path, content)?;
                    info!("Synced {} to disk (initial)", filename);
                }
            }
        } else {
            // Remove file if it's a tombstone
            if file_path.exists() {
                fs::remove_file(&file_path)?;
                info!("Removed {} from disk (initial tombstone)", filename);
            }
        }
    }

    Ok(())
}

async fn handle_sync(
    AxumState(state): AxumState<Arc<SyncManagerState>>,
    Json(msg): Json<SyncMessage>,
) -> impl IntoResponse {
    info!("Received sync from node {}", msg.sender_id);

    match state.sync_manager.handle_sync(msg) {
        Ok(response) => (StatusCode::OK, Json(response)),
        Err(e) => {
            error!("Sync failed: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(SyncResponse {
                    peer_id: 0,
                    entries: vec![],
                    is_snapshot: false,
                    progress: HashMap::new(),
                }),
            )
        }
    }
}

#[derive(Serialize)]
struct DebugStateResponse {
    node_id: NodeId,
    peers: Vec<NodeId>,
    kv_entries: HashMap<String, DebugEntry>,
    local_ack: HashMap<NodeId, u64>,
    peer_states: HashMap<NodeId, DebugPeerState>,
}

#[derive(Serialize)]
struct DebugEntry {
    value_size: Option<usize>,
    is_tombstone: bool,
    timestamp: u64,
    seq: u64,
    origin_node: NodeId,
}

#[derive(Serialize)]
struct DebugPeerState {
    log_size: usize,
    peer_ack: u64,
}

#[derive(Serialize)]
struct DebugFilesResponse {
    watch_dir: String,
    files: Vec<DebugFileInfo>,
}

#[derive(Serialize)]
struct DebugFileInfo {
    name: String,
    size: u64,
    in_wavekv: bool,
    wavekv_size: Option<usize>,
    match_status: String,
}

async fn handle_debug_state(
    AxumState(state): AxumState<Arc<SyncManagerState>>,
) -> impl IntoResponse {
    let node = state.node.read();

    let node_id = node.id;
    let peers = node.get_peers();
    let local_ack = node.get_local_ack();

    // Get all KV entries
    let all_entries = node.get_all_including_tombstones();
    let mut kv_entries = HashMap::new();
    for (key, entry) in all_entries {
        if let Some(filename) = decode_file_key(&key) {
            kv_entries.insert(
                filename.to_string(),
                DebugEntry {
                    value_size: entry.value.as_ref().map(|v| v.len()),
                    is_tombstone: entry.value.is_none(),
                    timestamp: entry.meta.timestamp as u64,
                    seq: entry.meta.seq,
                    origin_node: entry.meta.node,
                },
            );
        }
    }

    // Get peer states
    let mut peer_states = HashMap::new();
    for peer_id in &peers {
        if let Some(peer_state) = node.get_peer_state(*peer_id) {
            peer_states.insert(
                *peer_id,
                DebugPeerState {
                    log_size: peer_state.log.len(),
                    peer_ack: peer_state.peer_ack,
                },
            );
        }
    }

    let response = DebugStateResponse {
        node_id,
        peers,
        kv_entries,
        local_ack,
        peer_states,
    };

    (StatusCode::OK, Json(response))
}

async fn handle_debug_raw(AxumState(state): AxumState<Arc<SyncManagerState>>) -> impl IntoResponse {
    let node = state.node.read();
    let all_entries = node.get_all_including_tombstones();

    let mut raw_entries = HashMap::new();
    for (key, entry) in all_entries {
        raw_entries.insert(
            key.clone(),
            serde_json::json!({
                "value_size": entry.value.as_ref().map(|v| v.len()),
                "value_preview": entry.value.as_ref().and_then(|v| {
                    String::from_utf8(v.clone()).ok().map(|s| {
                        if s.len() > 100 { format!("{}...", &s[..100]) } else { s }
                    })
                }),
                "is_tombstone": entry.value.is_none(),
                "timestamp": entry.meta.timestamp,
                "seq": entry.meta.seq,
                "origin_node": entry.meta.node,
            }),
        );
    }

    (StatusCode::OK, Json(raw_entries))
}

async fn handle_debug_files(
    AxumState(state): AxumState<Arc<SyncManagerState>>,
) -> impl IntoResponse {
    let node = state.node.read();
    let all_entries = node.get_all_including_tombstones();
    drop(node);

    let mut files = Vec::new();

    // Scan filesystem
    if let Ok(entries) = fs::read_dir(&state.watch_dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    let filekey = encode_file_key(&name);
                    let size = metadata.len();

                    let (in_wavekv, wavekv_size, match_status) =
                        if let Some(kv_entry) = all_entries.get(&filekey) {
                            if let Some(content) = &kv_entry.value {
                                let fs_content = fs::read(entry.path()).unwrap_or_default();
                                let matches = fs_content == *content;
                                (
                                    true,
                                    Some(content.len()),
                                    if matches {
                                        "✓ Match".to_string()
                                    } else {
                                        "✗ Mismatch".to_string()
                                    },
                                )
                            } else {
                                (true, None, "✗ Tombstone in WaveKV".to_string())
                            }
                        } else {
                            (false, None, "✗ Not in WaveKV".to_string())
                        };

                    files.push(DebugFileInfo {
                        name,
                        size,
                        in_wavekv,
                        wavekv_size,
                        match_status,
                    });
                }
            }
        }
    }

    // Check for entries in WaveKV but not on filesystem
    for (key, entry) in &all_entries {
        let Some(filename) = decode_file_key(key) else {
            continue;
        };
        let file_path = state.watch_dir.join(filename);
        if !file_path.exists() {
            if let Some(content) = &entry.value {
                files.push(DebugFileInfo {
                    name: filename.to_string(),
                    size: 0,
                    in_wavekv: true,
                    wavekv_size: Some(content.len()),
                    match_status: "✗ Missing on filesystem".to_string(),
                });
            }
        }
    }

    files.sort_by(|a, b| a.name.cmp(&b.name));

    let response = DebugFilesResponse {
        watch_dir: state.watch_dir.to_string_lossy().to_string(),
        files,
    };

    (StatusCode::OK, Json(response))
}

fn check_consistency(dirs_str: String) -> Result<()> {
    let dirs: Vec<PathBuf> = dirs_str.split(',').map(PathBuf::from).collect();

    if dirs.len() < 2 {
        println!("Need at least 2 directories to check consistency");
        return Ok(());
    }

    let mut file_sets: Vec<HashMap<String, Vec<u8>>> = Vec::new();

    for dir in &dirs {
        let mut files = HashMap::new();
        if dir.exists() {
            let entries = fs::read_dir(dir)?;
            for entry in entries {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() {
                    if let Some(filename) = path.file_name() {
                        let filename = filename.to_string_lossy().to_string();
                        let content = fs::read(&path)?;
                        files.insert(filename, content);
                    }
                }
            }
        }
        file_sets.push(files);
    }

    // Check consistency
    let mut all_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    for file_set in &file_sets {
        for key in file_set.keys() {
            all_keys.insert(key.clone());
        }
    }

    let mut consistent = true;
    for key in &all_keys {
        let values: Vec<Option<&Vec<u8>>> = file_sets.iter().map(|fs| fs.get(key)).collect();

        // Check if all values are the same
        let first = values[0];
        if !values.iter().all(|v| v == &first) {
            consistent = false;
            println!("❌ Inconsistent file: {}", key);
            for (i, value) in values.iter().enumerate() {
                match value {
                    Some(content) => println!(
                        "   dir[{}]: {} bytes (hash: {:x})",
                        i,
                        content.len(),
                        md5::compute(content)
                    ),
                    None => println!("   dir[{}]: missing", i),
                }
            }
        } else {
            println!("✓ Consistent file: {}", key);
        }
    }

    if consistent {
        println!("\n✅ All files are consistent across directories");
    } else {
        println!("\n❌ Inconsistencies detected");
    }

    Ok(())
}
