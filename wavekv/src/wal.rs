use crate::ops::StateOp;
use crate::types::NodeId;
use anyhow::{anyhow, bail, Context as _, Result};
use fs_err::{self as fs, File, OpenOptions};
use serde::{Deserialize, Serialize};
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::{error, info, trace, warn};

/// WAL file header to identify format and version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalHeader {
    pub magic: [u8; 4],  // "WKVL"
    pub version: u32,    // Format version
    pub node_id: NodeId, // Node that created this WAL
    pub created_at: i64, // Timestamp when WAL was created
}

impl WalHeader {
    const MAGIC: [u8; 4] = *b"WKVL";
    const VERSION: u32 = 1;

    pub fn new(node_id: NodeId) -> Self {
        Self {
            magic: Self::MAGIC,
            version: Self::VERSION,
            node_id,
            created_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == Self::MAGIC && self.version == Self::VERSION
    }
}

/// Entry in the WAL file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub sequence: u64,     // Sequence number within this WAL file
    pub state_op: StateOp, // The actual state operation
    pub checksum: u32,     // CRC32 checksum for integrity
}

impl WalEntry {
    pub fn new(sequence: u64, state_op: StateOp) -> Self {
        let serialized = bincode::serde::encode_to_vec(&state_op, bincode::config::standard())
            .unwrap_or_default();
        let checksum = crc32fast::hash(&serialized);

        Self {
            sequence,
            state_op,
            checksum,
        }
    }

    pub fn verify_checksum(&self) -> bool {
        let serialized = bincode::serde::encode_to_vec(&self.state_op, bincode::config::standard())
            .unwrap_or_default();
        let computed = crc32fast::hash(&serialized);
        computed == self.checksum
    }
}

/// Write-Ahead Log implementation
pub struct WriteAheadLog {
    file_path: PathBuf,
    writer: Option<BufWriter<File>>,
    sequence: u64,
    node_id: NodeId,
}

impl WriteAheadLog {
    /// Create or open a WAL file
    pub fn new<P: AsRef<Path>>(file_path: P, node_id: NodeId) -> Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        let mut wal = Self {
            file_path,
            writer: None,
            sequence: 0,
            node_id,
        };

        wal.open_for_writing()?;
        Ok(wal)
    }

    fn open_for_writing(&mut self) -> Result<()> {
        let file_exists = self.file_path.exists();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.file_path)?;

        if !file_exists {
            // New file - write header
            let mut writer = BufWriter::new(file);
            let header = WalHeader::new(self.node_id);
            let header_bytes = bincode::serde::encode_to_vec(&header, bincode::config::standard())?;
            let header_len = header_bytes.len() as u32;

            writer.write_all(&header_len.to_le_bytes())?;
            writer.write_all(&header_bytes)?;
            writer.flush()?;

            // fsync to ensure header is durable
            writer.get_ref().sync_all()?;

            info!("Created new WAL file: {:?}", self.file_path);
            self.writer = Some(writer);
        } else {
            // Existing file - verify header and find last sequence
            let mut reader = BufReader::new(&file);
            let header = self.read_header(&mut reader)?;

            if !header.is_valid() {
                return Err(anyhow!("Invalid WAL file header"));
            }

            if header.node_id != self.node_id {
                warn!(
                    "WAL file node_id mismatch: expected {}, found {}",
                    self.node_id, header.node_id
                );
            }

            // Find the last sequence number
            self.sequence = self.find_last_sequence(&mut reader)?;

            let writer = BufWriter::new(file);
            self.writer = Some(writer);

            info!(
                "Opened existing WAL file: {:?}, last sequence: {}",
                self.file_path, self.sequence
            );
        }

        Ok(())
    }

    fn read_header<R: Read>(&self, reader: &mut R) -> Result<WalHeader> {
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let header_len = u32::from_le_bytes(len_bytes);

        let mut header_bytes = vec![0u8; header_len as usize];
        reader.read_exact(&mut header_bytes)?;

        let (header, _): (WalHeader, _) =
            bincode::serde::decode_from_slice(&header_bytes, bincode::config::standard())?;
        Ok(header)
    }

    fn find_last_sequence<R: Read + Seek>(&self, reader: &mut R) -> Result<u64> {
        let mut last_sequence = 0;

        // Skip header by seeking past it
        reader.seek(SeekFrom::Start(0))?;
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let header_len = u32::from_le_bytes(len_bytes) as u64;
        reader.seek(SeekFrom::Current(header_len as i64))?;

        // Read entries to find the last sequence
        loop {
            let mut entry_len_bytes = [0u8; 4];
            match reader.read_exact(&mut entry_len_bytes) {
                Ok(_) => {
                    let entry_len = u32::from_le_bytes(entry_len_bytes);
                    let mut entry_bytes = vec![0u8; entry_len as usize];

                    match reader.read_exact(&mut entry_bytes) {
                        Ok(_) => {
                            if let Ok((entry, _)) = bincode::serde::decode_from_slice::<WalEntry, _>(
                                &entry_bytes,
                                bincode::config::standard(),
                            ) {
                                if entry.verify_checksum() {
                                    last_sequence = entry.sequence;
                                } else {
                                    warn!(
                                        "Corrupted WAL entry found, sequence: {}",
                                        entry.sequence
                                    );
                                    break;
                                }
                            } else {
                                warn!("Failed to deserialize WAL entry");
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }

        Ok(last_sequence)
    }

    /// Write a state operation to the WAL
    pub fn write_op(&mut self, state_op: &StateOp) -> Result<()> {
        self.write_ops(std::slice::from_ref(state_op))
    }

    /// Write multiple state operations to the WAL in a single fsync
    pub fn write_ops(&mut self, state_ops: &[StateOp]) -> Result<()> {
        if state_ops.is_empty() {
            return Ok(());
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("WAL writer not initialized"))?;

        for state_op in state_ops {
            self.sequence += 1;
            let wal_entry = WalEntry::new(self.sequence, state_op.clone());

            let entry_bytes =
                bincode::serde::encode_to_vec(&wal_entry, bincode::config::standard())?;
            let entry_len = entry_bytes.len() as u32;

            writer.write_all(&entry_len.to_le_bytes())?;
            writer.write_all(&entry_bytes)?;

            trace!("Wrote WAL op: sequence={}, op={state_op:?}", self.sequence);
        }

        writer.flush()?;
        writer.get_ref().sync_all()?;

        Ok(())
    }

    /// Read all state operations from the WAL for recovery
    pub fn read_all_ops(&self) -> Result<Vec<StateOp>> {
        let file = File::open(&self.file_path)?;
        let mut reader = BufReader::new(file);

        // Skip header
        let header = self.read_header(&mut reader)?;
        if header.node_id != self.node_id {
            bail!(
                "WAL node_id mismatch: expected {}, found {}",
                self.node_id,
                header.node_id
            );
        }
        if !header.is_valid() {
            bail!("WAL header is invalid");
        }

        let mut entries = Vec::new();

        loop {
            let mut entry_len_bytes = [0u8; 4];
            let entry_len = match reader.read_exact(&mut entry_len_bytes) {
                Ok(_) => u32::from_le_bytes(entry_len_bytes),
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        break;
                    }
                    Err(err).context("Failed to read WAL entry length")?
                }
            };

            let mut entry_bytes = vec![0u8; entry_len as usize];
            match reader.read_exact(&mut entry_bytes) {
                Ok(_) => {}
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        warn!(
                            "Encountered truncated WAL entry (expected {} bytes); stopping replay",
                            entry_len
                        );
                        break;
                    }
                    Err(err).context("Failed to read WAL entry")?;
                }
            }
            let (wal_entry, _) = bincode::serde::decode_from_slice::<WalEntry, _>(
                &entry_bytes,
                bincode::config::standard(),
            )
            .context("Failed to deserialize WAL entry")?;
            if !wal_entry.verify_checksum() {
                bail!("WAL entry corrupted");
            }
            entries.push(wal_entry.state_op);
        }

        info!(
            "Read {} log ops from WAL: {:?}",
            entries.len(),
            self.file_path
        );
        Ok(entries)
    }

    /// Close the WAL and ensure all data is flushed
    pub fn close(&mut self) -> Result<()> {
        if let Some(mut writer) = self.writer.take() {
            writer.flush()?;
            writer.get_ref().sync_all()?;
            info!("Closed WAL: {:?}", self.file_path);
        }
        Ok(())
    }

    /// Truncate WAL by deleting existing file and reopening a fresh one
    pub fn reset(&mut self) -> Result<()> {
        self.close()?;
        if let Err(err) = fs::remove_file(&self.file_path) {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }
        self.sequence = 0;
        self.open_for_writing()
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

impl Drop for WriteAheadLog {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("Error closing WAL in drop: {e}");
        }
    }
}
