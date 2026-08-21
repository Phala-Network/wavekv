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
    /// How many times [`Self::sync`] has forced the log to disk.
    ///
    /// An fsync costs four to five orders of magnitude more than the append it
    /// follows, so how many of them a write path performs is a property worth
    /// asserting rather than timing: a test can pin "one batch, one fsync"
    /// directly, and it stays pinned when someone reshapes the call path.
    syncs: u64,
    /// Whether an append has landed in the page cache but not yet on disk.
    unsynced: bool,
}

impl WriteAheadLog {
    /// Create or open a WAL file
    pub fn new<P: AsRef<Path>>(file_path: P, node_id: NodeId) -> Result<Self> {
        let file_path = file_path.as_ref().to_path_buf();

        let mut wal = Self {
            file_path,
            writer: None,
            syncs: 0,
            unsynced: false,
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
                bail!(
                    "WAL node_id mismatch: expected {}, found {}",
                    self.node_id,
                    header.node_id
                );
            }

            // Find the last sequence number and where the undamaged prefix ends.
            let file_len = file.metadata()?.len();
            let (last_sequence, valid_end) = self.scan_valid_prefix(&mut reader, file_len)?;
            self.sequence = last_sequence;

            // Drop a torn tail before appending. Replay stops at the first damaged
            // record, so anything written past it would be unreachable forever — an
            // fsynced write silently lost on the next restart.
            if valid_end < file_len {
                warn!(
                    "discarding {} damaged byte(s) at the end of {:?}",
                    file_len - valid_end,
                    self.file_path
                );
                file.set_len(valid_end)?;
                file.sync_all()?;
            }

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

    /// Scan the record region and report the last intact sequence number together with
    /// the byte offset at which the undamaged prefix ends.
    ///
    /// The stopping rules mirror `read_all_ops` exactly: whatever replay refuses to
    /// cross is what the writer must overwrite. If the two ever disagreed, records
    /// would land on the far side of a wall replay never passes.
    fn scan_valid_prefix<R: Read + Seek>(
        &self,
        reader: &mut R,
        file_len: u64,
    ) -> Result<(u64, u64)> {
        let mut last_sequence = 0;

        // Skip header by seeking past it
        reader.seek(SeekFrom::Start(0))?;
        let mut len_bytes = [0u8; 4];
        reader.read_exact(&mut len_bytes)?;
        let header_len = u32::from_le_bytes(len_bytes) as u64;
        reader.seek(SeekFrom::Current(header_len as i64))?;
        let mut valid_end = 4 + header_len;

        loop {
            let mut entry_len_bytes = [0u8; 4];
            if let Err(err) = reader.read_exact(&mut entry_len_bytes) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(err).context("Failed to scan WAL entry length");
            }
            let entry_len = u32::from_le_bytes(entry_len_bytes);

            // A corrupt length field must not turn into a multi-gigabyte allocation.
            if entry_len as u64 > file_len.saturating_sub(valid_end + 4) {
                warn!("WAL entry claims {entry_len} bytes but the file is shorter; stopping scan");
                break;
            }

            let mut entry_bytes = vec![0u8; entry_len as usize];
            if let Err(err) = reader.read_exact(&mut entry_bytes) {
                if err.kind() == ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(err).context("Failed to scan WAL entry body");
            }

            let entry = match bincode::serde::decode_from_slice::<WalEntry, _>(
                &entry_bytes,
                bincode::config::standard(),
            ) {
                Ok((entry, _)) => entry,
                Err(_) => {
                    warn!("Failed to deserialize WAL entry");
                    break;
                }
            };
            if !entry.verify_checksum() {
                warn!("Corrupted WAL entry found, sequence: {}", entry.sequence);
                break;
            }

            last_sequence = entry.sequence;
            valid_end += 4 + entry_len as u64;
        }

        Ok((last_sequence, valid_end))
    }

    /// Write a state operation to the WAL
    pub fn write_op(&mut self, state_op: &StateOp) -> Result<()> {
        self.write_ops(std::slice::from_ref(state_op))
    }

    /// Write multiple state operations to the WAL in a single fsync
    pub fn write_ops(&mut self, state_ops: &[StateOp]) -> Result<()> {
        self.append_ops(state_ops)?;
        self.sync()
    }

    /// Hand the ops to the kernel without forcing them to the platter.
    ///
    /// The distinction is the whole point of group commit. `write` reaches the
    /// page cache, which already survives everything that kills only this
    /// process — a panic, an OOM kill, a restart — and costs about a
    /// microsecond. `fsync` survives losing the machine, and costs about a
    /// hundred. Buffering in *user* space would give up the cheap half of that
    /// guarantee, so the BufWriter is flushed on every append; only the fsync is
    /// deferred.
    pub fn append_ops(&mut self, state_ops: &[StateOp]) -> Result<()> {
        if state_ops.is_empty() {
            return Ok(());
        }

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("WAL writer not initialized"))?;

        // Marked before the first byte moves, not after the flush succeeds: a
        // partial write leaves bytes in the file that a later sync still has to
        // cover. Claiming a debt that turns out not to exist costs one fsync;
        // missing one loses the bytes.
        self.unsynced = true;

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

        Ok(())
    }

    /// Force everything appended so far to disk.
    ///
    /// A no-op when nothing has been appended since the last one, so a periodic
    /// caller costs nothing on an idle node.
    pub fn sync(&mut self) -> Result<()> {
        if !self.unsynced {
            return Ok(());
        }
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow!("WAL writer not initialized"))?;
        writer.get_ref().sync_all()?;
        self.unsynced = false;
        self.syncs += 1;
        Ok(())
    }

    /// Whether the log holds appends that are not yet on disk.
    pub fn has_unsynced_appends(&self) -> bool {
        self.unsynced
    }

    /// How many times this log has been forced to disk since it was opened.
    ///
    /// Appends and closes only. A rotation ([`Self::replace_with_ops`]) forces
    /// the disk too — for the replacement file and its directory entry — and is
    /// not counted here, because what this number exists to explain is the cost
    /// a write path pays, and a rotation is not on one.
    pub fn sync_count(&self) -> u64 {
        self.syncs
    }

    /// Read all state operations from the WAL for recovery
    /// Replay every op in the WAL.
    ///
    /// A torn or corrupt tail is treated as truncation — warn and stop — rather than as
    /// a fatal error. WaveKV state is fully replicated, so the correct response to local
    /// persistence damage is to quarantine the damaged suffix and re-sync from peers,
    /// never a permanent refusal to start (RFC 0001 section 3.10). `scan_valid_prefix`
    /// stops on exactly the same conditions, so the writer truncates precisely what
    /// replay refuses to cross.
    pub fn read_all_ops(&self) -> Result<Vec<StateOp>> {
        let file = File::open(&self.file_path)?;
        let file_len = file.metadata()?.len();
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
        let mut consumed = reader.stream_position()?;

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

            consumed += 4;
            // A corrupt length field must not turn into a multi-gigabyte allocation.
            let remaining = file_len.saturating_sub(consumed);
            if entry_len as u64 > remaining {
                warn!(
                    "WAL entry claims {entry_len} bytes but only {remaining} remain; \
                     treating the tail as truncated and stopping replay"
                );
                break;
            }

            let mut entry_bytes = vec![0u8; entry_len as usize];
            match reader.read_exact(&mut entry_bytes) {
                Ok(_) => {}
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        warn!(
                            "encountered truncated WAL entry (expected {entry_len} bytes); \
                             stopping replay"
                        );
                        break;
                    }
                    Err(err).context("failed to read WAL entry")?;
                }
            }
            consumed += entry_len as u64;

            let wal_entry = match bincode::serde::decode_from_slice::<WalEntry, _>(
                &entry_bytes,
                bincode::config::standard(),
            ) {
                Ok((entry, _)) => entry,
                Err(err) => {
                    warn!("undecodable WAL entry ({err}); treating the tail as truncated");
                    break;
                }
            };
            if !wal_entry.verify_checksum() {
                // The checksum covers the canonical encoding of `state_op`, so this
                // also catches a format drift that happened to decode into a different
                // op — not just bit rot.
                warn!("WAL entry failed its checksum; treating the tail as truncated");
                break;
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
            // Closing is a sync point: whatever a deferred policy was still
            // holding is on disk when this returns. Nothing owed means nothing
            // to force — a rotation closes a log it has just written and
            // fsynced, and paying for that twice buys nothing.
            if self.unsynced {
                writer.get_ref().sync_all()?;
                self.unsynced = false;
                self.syncs += 1;
            }
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

    /// Atomically replace the WAL with a fresh header followed by `ops`.
    ///
    /// The existing WAL remains authoritative until the replacement has been fully
    /// written and fsynced. This is used after snapshotting: an ENOSPC/EIO while
    /// preserving writes that raced the snapshot must not delete their old WAL copy.
    pub fn replace_with_ops(&mut self, ops: &[StateOp]) -> Result<()> {
        let tmp_path = self.file_path.with_extension("wal.tmp");
        if let Err(err) = fs::remove_file(&tmp_path) {
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        }

        let mut replacement = WriteAheadLog::new(&tmp_path, self.node_id)?;
        if let Err(err) = replacement
            .write_ops(ops)
            .and_then(|()| replacement.close())
        {
            let _ = fs::remove_file(&tmp_path);
            return Err(err);
        }

        self.close()?;
        if let Err(err) = fs::rename(&tmp_path, &self.file_path) {
            // The rename did not happen, so reopen the still-authoritative old WAL.
            let _ = self.open_for_writing();
            return Err(err.into());
        }
        if let Some(parent) = self.file_path.parent() {
            File::open(parent)?.sync_all()?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Entry, Metadata};

    fn op(key: &str, seq: u64) -> StateOp {
        StateOp::Set(Entry::new(
            key.to_string(),
            Some(b"v".to_vec()),
            Metadata::new(1, seq, seq as i64),
        ))
    }

    fn wal_with(dir: &std::path::Path, ops: &[StateOp]) -> std::path::PathBuf {
        let path = dir.join("node_1.wal");
        let mut wal = WriteAheadLog::new(&path, 1).expect("create wal");
        wal.write_ops(ops).expect("write ops");
        drop(wal);
        path
    }

    fn keys_in(path: &std::path::Path) -> Vec<String> {
        let wal = WriteAheadLog::new(path, 1).expect("reopen wal");
        wal.read_all_ops()
            .expect("replay must not fail on a damaged tail")
            .into_iter()
            .filter_map(|op| match op {
                StateOp::Set(entry) => Some(entry.key),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_clean_wal_replays_in_full() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1), op("b", 2), op("c", 3)]);
        assert_eq!(keys_in(&path), vec!["a", "b", "c"]);
    }

    /// A crash mid-write leaves a partial record. WaveKV state is fully replicated, so
    /// the right response is to drop the torn suffix and re-sync — never to refuse to
    /// start, which would strand the node on a recoverable fault.
    #[test]
    fn a_torn_tail_truncates_instead_of_failing() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1), op("b", 2), op("c", 3)]);

        let full_len = fs_err::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_len - 3).unwrap();
        drop(file);

        let keys = keys_in(&path);
        assert_eq!(
            keys,
            vec!["a", "b"],
            "the intact prefix must survive; only the torn record is dropped"
        );
    }

    /// Recovering from a torn tail must also *remove* it. Appending after damage the
    /// replay path stops at would put every subsequent record beyond a wall that replay
    /// never crosses — silently losing writes that were acknowledged and fsynced.
    #[test]
    fn writes_after_a_torn_tail_recovery_survive_the_next_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1), op("b", 2)]);

        let full_len = fs_err::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full_len - 3).unwrap();
        drop(file);

        // First restart: replay drops the torn record, then the node takes a new write.
        let mut wal = WriteAheadLog::new(&path, 1).expect("reopen after damage");
        assert_eq!(
            wal.read_all_ops().unwrap().len(),
            1,
            "the torn record must not replay"
        );
        wal.write_ops(&[op("c", 3)]).expect("write after recovery");
        drop(wal);

        // Second restart: `c` was fsynced, so it must still be there.
        assert_eq!(keys_in(&path), vec!["a", "c"]);
    }

    /// A corrupt length prefix must not become a multi-gigabyte allocation.
    #[test]
    fn an_absurd_record_length_is_bounds_checked() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1)]);

        // Append a length header claiming far more than the file can hold.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&u32::MAX.to_le_bytes()).unwrap();
        file.write_all(b"junk").unwrap();
        drop(file);

        assert_eq!(keys_in(&path), vec!["a"]);
    }

    /// Bit rot inside a record — or a format drift that happens to decode into a
    /// different op — is caught by the CRC over the canonical encoding.
    #[test]
    fn a_checksum_failure_stops_replay_without_erroring() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1), op("bbbb", 2)]);

        // Flip a byte inside the last record's payload.
        let mut bytes = fs_err::read(&path).unwrap();
        let last = bytes.len() - 6;
        bytes[last] ^= 0xff;
        fs_err::write(&path, &bytes).unwrap();

        let keys = keys_in(&path);
        assert!(
            !keys.contains(&"bbbb".to_string()),
            "a record failing its checksum must not be replayed: {keys:?}"
        );
        assert_eq!(keys, vec!["a"]);
    }

    /// The relaxed handling above applies to a damaged *tail* only. A WAL belonging to
    /// another node is a misconfiguration, not corruption, so replay refuses it outright
    /// rather than silently adopting another node's history.
    #[test]
    fn a_wal_from_another_node_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = wal_with(dir.path(), &[op("a", 1)]);

        let before = fs_err::read(&path).unwrap();
        let err = match WriteAheadLog::new(&path, 2) {
            Ok(_) => panic!("opening another node's WAL must fail before mutating it"),
            Err(err) => err,
        };

        assert!(err.to_string().contains("node_id mismatch"), "{err}");
        assert_eq!(fs_err::read(&path).unwrap(), before);
    }

    /// A header the current build does not understand is likewise fatal, not skippable.
    #[test]
    fn an_unrecognised_header_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node_1.wal");
        fs_err::write(&path, b"not a wal file at all").unwrap();
        assert!(
            WriteAheadLog::new(&path, 1).is_err(),
            "a file without a valid WAL header must not be opened for appending"
        );
    }
}
