// Now only offers log persistent

use crate::raft::{LogEntry, LogsMap, Options, RaftMeta, SnapshotEntity, Storage};
use async_std::sync::*;
use serde::{Deserialize, Serialize};

use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound::*;
use std::path::{Path, PathBuf};
use tokio::fs::*;
use tokio::io::*;

// const MAX_LOG_CAPACITY: usize = 10;

#[derive(Clone)]
pub struct DiskOptions {
    pub path: String,
    pub take_snapshots: bool,
    pub append_logs: bool,
    pub trim_logs: bool,
    // Snapshot configuration
    pub snapshot_log_threshold: u64,  // Trigger snapshot after N logs
    pub log_compaction_threshold: u64, // Compact when logs exceed this
}

impl DiskOptions {
    pub fn new(path: String) -> Self {
        Self {
            path,
            take_snapshots: true,
            append_logs: true,
            trim_logs: true,
            snapshot_log_threshold: 1000,
            log_compaction_threshold: 2000,
        }
    }
}

pub struct StorageEntity {
    pub logs: Option<File>,
    pub snapshot: Option<File>,
    pub last_term: u64,
    pub base_path: PathBuf,
}

pub struct DiskLogEntry {
    pub term: u64,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log: LogEntry,
}

impl DiskLogEntry {
    /// Encode to deterministic binary format with CRC32 checksum.
    ///
    /// On-disk record layout (written by `append_logs`):
    ///   [8 bytes]  record length  (= 4 + payload length, does NOT include these 8 bytes)
    ///   [4 bytes]  CRC32 of payload
    ///   [N bytes]  payload:
    ///     [8 bytes] term
    ///     [8 bytes] commit_index
    ///     [8 bytes] last_applied
    ///     [8 bytes] log.id
    ///     [8 bytes] log.term
    ///     [8 bytes] log.sm_id
    ///     [8 bytes] log.fn_id
    ///     [8 bytes] log.data.len()
    ///     [M bytes] log.data
    ///
    /// `encode()` returns only the payload (the CRC and length prefix are added by the caller).
    pub fn encode(&self) -> Vec<u8> {
        let data_len = self.log.data.len();
        let total_size = 8 * 8 + data_len;
        let mut buf = Vec::with_capacity(total_size);
        buf.extend_from_slice(&self.term.to_le_bytes());
        buf.extend_from_slice(&self.commit_index.to_le_bytes());
        buf.extend_from_slice(&self.last_applied.to_le_bytes());
        buf.extend_from_slice(&self.log.id.to_le_bytes());
        buf.extend_from_slice(&self.log.term.to_le_bytes());
        buf.extend_from_slice(&self.log.sm_id.to_le_bytes());
        buf.extend_from_slice(&self.log.fn_id.to_le_bytes());
        buf.extend_from_slice(&(data_len as u64).to_le_bytes());
        buf.extend_from_slice(&self.log.data);
        buf
    }

    /// Decode payload bytes (without the length prefix or CRC — the caller strips those).
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("DiskLogEntry too short: {} bytes", data.len()),
            ));
        }
        let term          = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let commit_index  = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let last_applied  = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let log_id        = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let log_term      = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let log_sm_id     = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let log_fn_id     = u64::from_le_bytes(data[48..56].try_into().unwrap());
        let data_len      = u64::from_le_bytes(data[56..64].try_into().unwrap()) as usize;
        if data.len() < 64 + data_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("DiskLogEntry data truncated: expected {}, got {}", 64 + data_len, data.len()),
            ));
        }
        let log_data = data[64..64 + data_len].to_vec();
        Ok(DiskLogEntry {
            term,
            commit_index,
            last_applied,
            log: LogEntry {
                id: log_id,
                term: log_term,
                sm_id: log_sm_id,
                fn_id: log_fn_id,
                data: log_data,
            },
        })
    }
}

impl StorageEntity {
    pub fn new_with_options(
        opts: &Options,
        term: &mut u64,
        commit_index: &mut u64,
        last_applied: &mut u64,
        logs: &mut LogsMap,
    ) -> io::Result<Option<Self>> {
        Ok(match &opts.storage {
            &Storage::DISK(ref options) => {
                let base_path = Path::new(&options.path);
                let _ = std::fs::create_dir_all(base_path);
                let log_path = base_path.join("log.dat");
                let snapshot_path = base_path.join("snapshot.dat");
                let mut open_opts = OpenOptions::new();
                open_opts
                    .write(true)
                    .create(true)
                    .read(true)
                    .truncate(false);
                let mut storage = Self {
                    logs: if options.append_logs {
                        let mut log_file = open_opts.open(log_path.as_path())?;
                        let mut len_buf = [0u8; 8];
                        let mut crc_buf = [0u8; 4];
                        let mut counter = 0;
                        let mut last_valid_pos: u64 = 0;
                        loop {
                            let pos_before = log_file.seek(SeekFrom::Current(0))
                                .unwrap_or(last_valid_pos);
                            if log_file.read_exact(&mut len_buf).is_err() {
                                break;
                            }
                            let record_len = u64::from_le_bytes(len_buf);
                            // record_len = 4 (CRC) + payload_len
                            if record_len < 4 {
                                warn!("WAL corrupt: invalid record length {} at pos {}, truncating", record_len, pos_before);
                                break;
                            }
                            let payload_len = record_len - 4;
                            if log_file.read_exact(&mut crc_buf).is_err() {
                                warn!("WAL truncated: missing CRC at pos {}, truncating", pos_before);
                                break;
                            }
                            let expected_crc = u32::from_le_bytes(crc_buf);
                            let mut data_buf = vec![0u8; payload_len as usize];
                            if log_file.read_exact(&mut data_buf).is_err() {
                                warn!("WAL truncated: missing payload at pos {}, truncating", pos_before);
                                break;
                            }
                            let actual_crc = crc32fast::hash(&data_buf);
                            if actual_crc != expected_crc {
                                warn!(
                                    "WAL CRC mismatch at pos {}: expected {:#010x}, got {:#010x}, truncating",
                                    pos_before, expected_crc, actual_crc
                                );
                                break;
                            }
                            match DiskLogEntry::decode(&data_buf) {
                                Ok(entry) => {
                                    *term = entry.term;
                                    // Do not trust commit/last_applied embedded in WAL for SM reconstruction
                                    // We'll derive commit_index from commit.idx and force replay from last_applied=0
                                    logs.insert(entry.log.id, entry.log);
                                    counter += 1;
                                    last_valid_pos = log_file.seek(SeekFrom::Current(0))
                                        .unwrap_or(last_valid_pos);
                                }
                                Err(e) => {
                                    warn!("WAL decode error at pos {}: {:?}, truncating", pos_before, e);
                                    break;
                                }
                            }
                        }
                        // Truncate WAL at last valid entry to remove any corrupt tail,
                        // then seek to end so appends start at the correct position
                        let current_len = log_file.seek(SeekFrom::End(0)).unwrap_or(last_valid_pos);
                        if current_len > last_valid_pos {
                            info!("WAL has corrupt tail ({} extra bytes), truncating to {}", current_len - last_valid_pos, last_valid_pos);
                            if let Err(e) = log_file.set_len(last_valid_pos) {
                                warn!("Failed to truncate WAL to {} bytes: {:?}", last_valid_pos, e);
                            }
                        }
                        let _ = log_file.seek(SeekFrom::End(0));
                        debug!("Recovered {} raft logs", counter);
                        Some(File::from_std(log_file))
                    } else {
                        None
                    },
                    snapshot: if options.take_snapshots {
                        Some(File::from_std(open_opts.open(snapshot_path.as_path())?))
                    } else {
                        None
                    },
                    last_term: 0,
                    base_path: base_path.to_path_buf(),
                };

                // If commit progress side file exists, load it to ensure accurate indices
                // Force full replay by resetting last_applied to 0 on startup
                *last_applied = 0;
                if let Ok(Some((ci, _la))) = futures::executor::block_on(storage.read_commit_progress()) {
                    *commit_index = ci;
                    debug!("Recovered commit progress: commit_index={} (will replay to rebuild state)", ci);
                } else {
                    // If no commit progress found, default to 0 to avoid partial state
                    *commit_index = 0;
                }

                Some(storage)
            }
            _ => None,
        })
    }

    pub async fn append_logs<'a>(
        &mut self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        logs: &'a RwLockWriteGuard<'a, LogsMap>,
    ) -> io::Result<()> {
        if let Some(f) = &mut self.logs {
            let was_last_term = self.last_term;
            let mut counter = 0;
            let mut terms_appended = vec![];
            let master = meta.state_machine.read().await;
            for (term, log) in logs.range((Excluded(self.last_term), Unbounded)) {
                // Skip non-recoverable state machines
                if !master.is_recoverable(log.sm_id) {
                    continue;
                }
                let entry = DiskLogEntry {
                    term: *term,
                    commit_index: meta.commit_index,
                    last_applied: meta.last_applied,
                    log: log.clone(),
                };
                let entry_data = entry.encode();
                let checksum = crc32fast::hash(&entry_data);
                // Write: [8 bytes record_len = 4+payload_len][4 bytes CRC32][N bytes payload]
                let record_len = 4u64 + entry_data.len() as u64;
                f.write_all(&record_len.to_le_bytes()).await?;
                f.write_all(&checksum.to_le_bytes()).await?;
                f.write_all(entry_data.as_slice()).await?;
                self.last_term = *term;
                terms_appended.push(self.last_term);
                counter += 1;
            }
            if counter > 0 {
                f.sync_all().await?;
                debug!(
                    "Appended and persisted {} logs, was {}, appended {:?}",
                    counter, was_last_term, terms_appended
                );
            }
        }
        Ok(())
    }

    pub async fn post_processing<'a>(
        &mut self,
        meta: &RwLockWriteGuard<'a, RaftMeta>,
        logs: RwLockWriteGuard<'a, LogsMap>,
    ) -> io::Result<()> {
        // TODO: trim logs in memory
        // TODO: trim logs on disk
        self.append_logs(meta, &logs).await?;

        Ok(())

        // let (last_log_id, _) = get_last_log_info!(self, logs);
        // let expecting_oldest_log = if last_log_id > MAX_LOG_CAPACITY as u64 {
        //     last_log_id - MAX_LOG_CAPACITY as u64
        // } else {
        //     0
        // };
        // let double_cap = MAX_LOG_CAPACITY << 1;
        // if logs.len() > double_cap && meta.last_applied > expecting_oldest_log {
        //     debug!("trim logs");
        //     while logs.len() > MAX_LOG_CAPACITY {
        //         let first_key = *logs.iter().next().unwrap().0;
        //         logs.remove(&first_key).unwrap();
        //     }
        //     if let Some(ref storage) = meta.storage {
        //         let mut storage = storage.write().await;
        //         let snapshot = SnapshotEntity {
        //             term: meta.term,
        //             commit_index: meta.commit_index,
        //             last_applied: meta.last_applied,
        //             snapshot: meta.state_machine.read().await.snapshot().unwrap(),
        //         };
        //         storage
        //             .snapshot
        //             .write_all(crate::utils::serde::serialize(&snapshot).as_slice())?;
        //         storage.snapshot.sync_all().unwrap();
        //     }
        // }
        // if let Some(ref storage) = meta.storage {
        //     let mut storage = storage.write().await;
        //     let logs_data = crate::utils::serde::serialize(&*meta.logs.read().await);
        //     // TODO: async file system calls
        //     storage.logs.write_all(logs_data.as_slice())?;
        //     storage.logs.sync_all().unwrap();
        // }
    }

    /// Ensure WAL file is fully synced to disk.
    pub async fn flush_wal(&mut self) -> io::Result<()> {
        if let Some(f) = &mut self.logs {
            info!("WAL fsync: syncing log.dat to disk");
            f.sync_all().await?;
            info!("WAL fsync: completed");
        }
        Ok(())
    }

    /// Persist commit progress atomically to a side file (commit.idx)
    pub async fn write_commit_progress(&mut self, commit_index: u64, last_applied: u64) -> io::Result<()> {
        let commit_path = self.base_path.join("commit.idx");
        let temp_path = self.base_path.join("commit.idx.tmp");
        let mut f = File::create(&temp_path).await?;
        f.write_all(&commit_index.to_le_bytes()).await?;
        f.write_all(&last_applied.to_le_bytes()).await?;
        f.sync_all().await?;
        drop(f);
        std::fs::rename(&temp_path, &commit_path)?;
        Ok(())
    }

    /// Read commit progress if available
    pub async fn read_commit_progress(&self) -> io::Result<Option<(u64, u64)>> {
        let commit_path = self.base_path.join("commit.idx");
        if !commit_path.exists() { return Ok(None); }
        let mut f = File::open(&commit_path).await?;
        let mut buf = [0u8; 16];
        if f.read_exact(&mut buf).await.is_err() { return Ok(None); }
        let commit_index = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let last_applied = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        Ok(Some((commit_index, last_applied)))
    }

    /// Write snapshot to disk using atomic write pattern (temp file + rename)
    pub async fn write_snapshot(&mut self, snapshot: &SnapshotEntity) -> io::Result<()> {
        let snapshot_path = self.base_path.join("snapshot.dat");
        let temp_path = self.base_path.join("snapshot.dat.tmp");
        
        // Serialize snapshot
        let snapshot_data = crate::utils::serde::serialize(snapshot);
        
        // Calculate CRC32 checksum
        let checksum = crc32fast::hash(&snapshot_data);
        
        // Write to temp file
        let mut temp_file = File::create(&temp_path).await?;
        
        // Write checksum first
        temp_file.write_all(&checksum.to_le_bytes()).await?;
        
        // Write length
        temp_file.write_all(&(snapshot_data.len() as u64).to_le_bytes()).await?;
        
        // Write data
        temp_file.write_all(&snapshot_data).await?;
        
        // Sync to disk
        temp_file.sync_all().await?;
        drop(temp_file);
        
        // Atomic rename
        std::fs::rename(&temp_path, &snapshot_path)?;
        
        info!(
            "Snapshot persisted to disk: index={}, term={}, size={} bytes",
            snapshot.last_included_index,
            snapshot.last_included_term,
            snapshot_data.len()
        );
        
        Ok(())
    }

    /// Read and validate snapshot from disk
    pub async fn read_snapshot(&self) -> io::Result<Option<SnapshotEntity>> {
        let snapshot_path = self.base_path.join("snapshot.dat");
        
        // Check if snapshot file exists
        if !snapshot_path.exists() {
            debug!("No snapshot file found at {:?}", snapshot_path);
            return Ok(None);
        }
        
        let mut file = File::open(&snapshot_path).await?;
        
        // Read checksum
        let mut checksum_buf = [0u8; 4];
        if file.read_exact(&mut checksum_buf).await.is_err() {
            warn!("Failed to read snapshot checksum, file may be corrupted");
            return Ok(None);
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);
        
        // Read length
        let mut len_buf = [0u8; 8];
        if file.read_exact(&mut len_buf).await.is_err() {
            warn!("Failed to read snapshot length, file may be corrupted");
            return Ok(None);
        }
        let len = u64::from_le_bytes(len_buf);
        
        // Read data
        let mut data_buf = vec![0u8; len as usize];
        if file.read_exact(&mut data_buf).await.is_err() {
            warn!("Failed to read snapshot data, file may be corrupted");
            return Ok(None);
        }
        
        // Verify checksum
        let actual_checksum = crc32fast::hash(&data_buf);
        if actual_checksum != expected_checksum {
            error!(
                "Snapshot checksum mismatch! Expected: {}, Got: {}. File is corrupted.",
                expected_checksum, actual_checksum
            );
            return Ok(None);
        }
        
        // Deserialize
        let snapshot = crate::utils::serde::deserialize::<SnapshotEntity>(&data_buf).unwrap();
        
        info!(
            "Snapshot loaded from disk: index={}, term={}, size={} bytes",
            snapshot.last_included_index,
            snapshot.last_included_term,
            data_buf.len()
        );
        Ok(Some(snapshot))
    }
}
