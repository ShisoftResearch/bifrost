// Now only offers log persistent

use crate::raft::{LogEntry, LogsMap, Options, PlaneId, RaftMeta, SnapshotEntity, Storage};
use async_std::sync::*;

use std::collections::VecDeque;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Bound::*;
use std::path::{Path, PathBuf};
use tokio::fs::*;
use tokio::io::*;

// const MAX_LOG_CAPACITY: usize = 10;

/// Test-only fault injection for the persistence paths: a test arms a point
/// for one storage directory, and the I/O at that point fails with an
/// `io::Error` exactly as a failing disk would. Release and non-test builds
/// compile every check to `Ok(())`.
#[cfg(test)]
pub(crate) mod failpoints {
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Fail {
        /// Writing a WAL record.
        WalAppend,
        /// fsync of `log.dat`.
        WalSync,
        /// Writing `commit.idx`.
        CommitProgress,
        /// Writing the hard state: before the temp file is written.
        HardStateWrite,
        /// Writing the hard state: temp file written, not renamed.
        HardStateRename,
        /// Writing the hard state: renamed, directory not yet fsynced.
        HardStateDirSync,
    }

    static ARMED: Mutex<Vec<(PathBuf, Fail)>> = Mutex::new(Vec::new());

    pub fn arm(dir: &Path, point: Fail) {
        ARMED.lock().unwrap().push((dir.to_path_buf(), point));
    }

    pub fn disarm(dir: &Path, point: Fail) {
        ARMED
            .lock()
            .unwrap()
            .retain(|(armed_dir, armed)| !(armed_dir == dir && *armed == point));
    }

    pub fn check(dir: &Path, point: Fail) -> io::Result<()> {
        let armed = ARMED
            .lock()
            .unwrap()
            .iter()
            .any(|(armed_dir, armed)| armed_dir == dir && *armed == point);
        if armed {
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("injected {:?} failure in {}", point, dir.display()),
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
use failpoints::{check as failpoint, Fail};

#[derive(Clone)]
pub struct DiskOptions {
    pub path: String,
    pub take_snapshots: bool,
    pub append_logs: bool,
    pub trim_logs: bool,
    // Snapshot configuration
    pub snapshot_log_threshold: u64, // Trigger snapshot after N logs
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
    /// Despite the name: the id of the last log entry appended to `log.dat`.
    /// `append_logs` writes only the entries above it. That is sound because
    /// a leader's ids only grow -- including across snapshot and compaction,
    /// where the next id continues from the snapshot's last included index
    /// (`last_log_position`) instead of restarting at 1 -- and because a
    /// follower that discards a conflicting suffix moves it back below that
    /// suffix (`truncate_from`), so the replacing entries are written.
    pub last_term: u64,
    pub base_path: PathBuf,
    pub plane_id: PlaneId,
    pub wal_index: WalIndex,
    /// Set when a failed WAL write could not be undone: the file's state is
    /// unknown, so every later WAL operation refuses until a restart replays
    /// what is really on disk.
    pub poisoned: Option<String>,
}

/// Where each record of `log.dat` starts, so a discarded log suffix can be cut
/// off the file instead of resurrecting on the next replay.
///
/// Only records a follower may still have to discard are indexed: committed
/// entries never conflict, so whatever a snapshot covers is forgotten.
#[derive(Default)]
pub struct WalIndex {
    /// (log id, byte offset of its record), both strictly increasing.
    records: VecDeque<(u64, u64)>,
    /// Length of the valid prefix of the file: where the next record starts.
    len: u64,
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
        let term = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let commit_index = u64::from_le_bytes(data[8..16].try_into().unwrap());
        let last_applied = u64::from_le_bytes(data[16..24].try_into().unwrap());
        let log_id = u64::from_le_bytes(data[24..32].try_into().unwrap());
        let log_term = u64::from_le_bytes(data[32..40].try_into().unwrap());
        let log_sm_id = u64::from_le_bytes(data[40..48].try_into().unwrap());
        let log_fn_id = u64::from_le_bytes(data[48..56].try_into().unwrap());
        let data_len = u64::from_le_bytes(data[56..64].try_into().unwrap()) as usize;
        if data.len() < 64 + data_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "DiskLogEntry data truncated: expected {}, got {}",
                    64 + data_len,
                    data.len()
                ),
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
        Self::new_with_options_on_plane(
            PlaneId::type1(),
            opts,
            term,
            commit_index,
            last_applied,
            logs,
        )
    }

    pub fn new_with_options_on_plane(
        plane_id: PlaneId,
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
                let mut last_log_id = 0;
                let mut wal_index = WalIndex::default();
                let mut storage = Self {
                    logs: if options.append_logs {
                        let mut log_file = open_opts.open(log_path.as_path())?;
                        let mut len_buf = [0u8; 8];
                        let mut crc_buf = [0u8; 4];
                        let mut counter = 0;
                        let mut last_valid_pos: u64 = 0;
                        loop {
                            let pos_before = log_file
                                .seek(SeekFrom::Current(0))
                                .unwrap_or(last_valid_pos);
                            if log_file.read_exact(&mut len_buf).is_err() {
                                break;
                            }
                            let record_len = u64::from_le_bytes(len_buf);
                            // record_len = 4 (CRC) + payload_len
                            if record_len < 4 {
                                warn!(
                                    "WAL corrupt: invalid record length {} at pos {}, truncating",
                                    record_len, pos_before
                                );
                                break;
                            }
                            let payload_len = record_len - 4;
                            if log_file.read_exact(&mut crc_buf).is_err() {
                                warn!(
                                    "WAL truncated: missing CRC at pos {}, truncating",
                                    pos_before
                                );
                                break;
                            }
                            let expected_crc = u32::from_le_bytes(crc_buf);
                            let mut data_buf = vec![0u8; payload_len as usize];
                            if log_file.read_exact(&mut data_buf).is_err() {
                                warn!(
                                    "WAL truncated: missing payload at pos {}, truncating",
                                    pos_before
                                );
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
                                    // The record header's `term` is the log ID
                                    // (append_logs writes the map key there),
                                    // not a term. Restoring the term from it
                                    // usually lands far ABOVE the plane's term:
                                    // the member's first answer then deposes the
                                    // leader (an election per restart), but it
                                    // also keeps the member out of any term it
                                    // may have voted in, as neither the term nor
                                    // the vote is persisted. Alone, though, it
                                    // can sit BELOW the entries' own terms (more
                                    // elections than entries), and the next
                                    // leader term would rank older entries above
                                    // newer ones (section 5.4.1). So the term
                                    // never drops below the log's.
                                    *term = (*term).max(entry.term).max(entry.log.term);
                                    // Do not trust commit/last_applied embedded in WAL for SM reconstruction
                                    // We'll derive commit_index from commit.idx and force replay from last_applied=0
                                    let id = entry.log.id;
                                    if counter > 0 && id <= last_log_id {
                                        // Records are appended in log order, and a
                                        // discarded suffix is cut off the file
                                        // (`truncate_from`) before its replacement
                                        // is written. A record at or below its
                                        // predecessor is a replacement whose cut
                                        // did not reach the disk: it supersedes
                                        // the record it repeats and all after it,
                                        // exactly as it did in memory.
                                        warn!(
                                            "WAL record {} follows record {} on plane {}: replacing the suffix from {} on",
                                            id,
                                            last_log_id,
                                            plane_id.raw(),
                                            id
                                        );
                                        let _ = logs.split_off(&id);
                                        let kept = wal_index
                                            .records
                                            .partition_point(|(indexed, _)| *indexed < id);
                                        wal_index.records.truncate(kept);
                                    }
                                    last_log_id = id;
                                    wal_index.records.push_back((id, pos_before));
                                    logs.insert(entry.log.id, entry.log);
                                    counter += 1;
                                    last_valid_pos = log_file
                                        .seek(SeekFrom::Current(0))
                                        .unwrap_or(last_valid_pos);
                                }
                                Err(e) => {
                                    warn!(
                                        "WAL decode error at pos {}: {:?}, truncating",
                                        pos_before, e
                                    );
                                    break;
                                }
                            }
                        }
                        // Truncate WAL at last valid entry to remove any corrupt tail,
                        // then seek to end so appends start at the correct position
                        let current_len = log_file.seek(SeekFrom::End(0)).unwrap_or(last_valid_pos);
                        if current_len > last_valid_pos {
                            info!(
                                "WAL has corrupt tail ({} extra bytes), truncating to {}",
                                current_len - last_valid_pos,
                                last_valid_pos
                            );
                            // Appends must start right after the last valid
                            // record: written after a torn tail, they would sit
                            // beyond bytes the next replay stops at.
                            if let Err(e) = log_file.set_len(last_valid_pos) {
                                return Err(io::Error::new(
                                    e.kind(),
                                    format!(
                                        "cannot cut the torn WAL tail of plane {} at {} bytes: {}",
                                        plane_id.raw(),
                                        last_valid_pos,
                                        e
                                    ),
                                ));
                            }
                        }
                        let _ = log_file.seek(SeekFrom::End(0));
                        wal_index.len = last_valid_pos;
                        debug!(
                            "Recovered {} raft logs for plane {}",
                            counter,
                            plane_id.raw()
                        );
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
                    plane_id,
                    wal_index,
                    poisoned: None,
                };
                storage.last_term = last_log_id;

                // If commit progress side file exists, load it to ensure accurate indices
                // Force full replay by resetting last_applied to 0 on startup
                *last_applied = 0;
                // A damaged commit.idx is an error, not a 0: replay stops at
                // the commit index, and 0 would hide acknowledged commits.
                if let Some((ci, _la)) =
                    futures::executor::block_on(storage.read_commit_progress())?
                {
                    *commit_index = ci;
                    debug!(
                        "Recovered commit progress for plane {}: commit_index={} (will replay to rebuild state)",
                        plane_id.raw(),
                        ci
                    );
                } else {
                    // No commit progress yet: nothing was committed here.
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
        self.refuse_if_poisoned()?;
        let batch_start = (self.last_term, self.wal_index.len, self.wal_index.records.len());
        let written = self.write_batch(meta, logs).await;
        if let Err(e) = written {
            // Nothing of a failed batch may stay: a record written but not
            // fsynced can surface after a crash (and after a failed fsync
            // Linux may already have dropped the dirty pages, so a later
            // fsync would not save it either). Cut the file back to where the
            // batch began, so the WAL holds exactly what was acknowledged.
            self.rollback_batch(batch_start).await;
            return Err(e);
        }
        Ok(())
    }

    async fn rollback_batch(&mut self, (cursor, len, records): (u64, u64, usize)) {
        self.last_term = cursor;
        self.wal_index.records.truncate(records);
        self.wal_index.len = len;
        let Some(f) = &mut self.logs else {
            return;
        };
        let cut = async {
            f.set_len(len).await?;
            f.seek(SeekFrom::Start(len)).await?;
            f.sync_all().await
        }
        .await;
        if let Err(e) = cut {
            let reason = format!(
                "a failed WAL batch could not be cut back to {} bytes: {}",
                len, e
            );
            error!("Plane {}: {}; refusing WAL writes until restart", self.plane_id.raw(), reason);
            self.poisoned = Some(reason);
        }
    }

    fn refuse_if_poisoned(&self) -> io::Result<()> {
        match &self.poisoned {
            Some(reason) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("WAL of plane {} is unusable: {}", self.plane_id.raw(), reason),
            )),
            None => Ok(()),
        }
    }

    async fn write_batch<'a>(
        &mut self,
        meta: &'a RwLockWriteGuard<'a, RaftMeta>,
        logs: &'a RwLockWriteGuard<'a, LogsMap>,
    ) -> io::Result<()> {
        #[cfg(test)]
        let base_path = self.base_path.clone();
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
                let written = async {
                    #[cfg(test)]
                    failpoint(&base_path, Fail::WalAppend)?;
                    f.write_all(&record_len.to_le_bytes()).await?;
                    f.write_all(&checksum.to_le_bytes()).await?;
                    f.write_all(entry_data.as_slice()).await
                }
                .await;
                written?;
                self.wal_index.records.push_back((*term, self.wal_index.len));
                self.wal_index.len += 8 + record_len;
                self.last_term = *term;
                terms_appended.push(self.last_term);
                counter += 1;
            }
            if counter > 0 {
                #[cfg(test)]
                failpoint(&base_path, Fail::WalSync)?;
                f.sync_all().await?;
                debug!(
                    "Appended and persisted {} logs for plane {}, was {}, appended {:?}",
                    counter,
                    self.plane_id.raw(),
                    was_last_term,
                    terms_appended
                );
            }
        }
        Ok(())
    }

    /// Discard every record with log id `from_id` or above: the conflicting
    /// suffix a follower deletes (Raft Figure 2, AppendEntries step 3), or the
    /// log an InstallSnapshot replaces. The append cursor always moves below
    /// `from_id`, so the replacement entries are written even when the cut
    /// itself fails -- replay then lets them supersede the stale records.
    pub async fn truncate_from(&mut self, from_id: u64) -> io::Result<()> {
        self.refuse_if_poisoned()?;
        self.last_term = self.last_term.min(from_id.saturating_sub(1));
        let kept = self
            .wal_index
            .records
            .partition_point(|(id, _)| *id < from_id);
        let Some(&(first_cut, offset)) = self.wal_index.records.get(kept) else {
            return Ok(());
        };
        let cut = self.wal_index.records.len() - kept;
        // Forget the cut records even if the cut fails: the index must stay
        // ordered for the replacement records appended next.
        self.wal_index.records.truncate(kept);
        let Some(f) = &mut self.logs else {
            return Ok(());
        };
        f.set_len(offset).await?;
        self.wal_index.len = offset;
        // `set_len` leaves the cursor past the new end; writing there would
        // leave a zero-filled gap that replay reads as a torn tail. With the
        // file already cut, a failure here leaves its position unknown.
        let settled = async {
            f.seek(SeekFrom::Start(offset)).await?;
            f.sync_all().await
        }
        .await;
        if let Err(e) = settled {
            self.poisoned = Some(format!(
                "the WAL was cut to {} bytes but not settled: {}",
                offset, e
            ));
            return Err(e);
        }
        info!(
            "Cut {} WAL records from log id {} on plane {} (file now {} bytes)",
            cut,
            first_cut,
            self.plane_id.raw(),
            offset
        );
        Ok(())
    }

    /// A snapshot at `index` covers every entry up to it: those records are
    /// committed and never discarded, so their offsets are not needed.
    pub fn forget_records_through(&mut self, index: u64) {
        while matches!(self.wal_index.records.front(), Some((id, _)) if *id <= index) {
            self.wal_index.records.pop_front();
        }
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
        self.refuse_if_poisoned()?;
        if let Some(f) = &mut self.logs {
            info!(
                "WAL fsync for plane {}: syncing log.dat to disk",
                self.plane_id.raw()
            );
            #[cfg(test)]
            failpoint(&self.base_path, Fail::WalSync)?;
            f.sync_all().await?;
            info!("WAL fsync for plane {}: completed", self.plane_id.raw());
        }
        Ok(())
    }

    /// Persist commit progress atomically to a side file (commit.idx)
    pub async fn write_commit_progress(
        &mut self,
        commit_index: u64,
        last_applied: u64,
    ) -> io::Result<()> {
        let commit_path = self.base_path.join("commit.idx");
        let temp_path = self.base_path.join("commit.idx.tmp");
        #[cfg(test)]
        failpoint(&self.base_path, Fail::CommitProgress)?;
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
        if !commit_path.exists() {
            return Ok(None);
        }
        // Written by rename, so it is whole or absent; anything else is
        // damage, and guessing 0 would hide acknowledged commits from replay.
        let bytes = std::fs::read(&commit_path)?;
        let buf: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "commit.idx of plane {} holds {} bytes, expected 16",
                    self.plane_id.raw(),
                    bytes.len()
                ),
            )
        })?;
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
        temp_file
            .write_all(&(snapshot_data.len() as u64).to_le_bytes())
            .await?;

        // Write data
        temp_file.write_all(&snapshot_data).await?;

        // Sync to disk
        temp_file.sync_all().await?;
        drop(temp_file);

        // Atomic rename
        std::fs::rename(&temp_path, &snapshot_path)?;

        info!(
            "Snapshot persisted to disk for plane {}: index={}, term={}, size={} bytes",
            self.plane_id.raw(),
            snapshot.last_included_index,
            snapshot.last_included_term,
            snapshot_data.len()
        );

        Ok(())
    }

    /// Read and validate snapshot from disk
    pub async fn read_snapshot(&self) -> io::Result<Option<SnapshotEntity>> {
        Self::read_snapshot_at(&self.base_path, self.plane_id).await
    }

    /// Read the snapshot persisted under `base_path` without the storage
    /// lock. `write_snapshot` replaces the file by rename, so a concurrent
    /// read sees one complete snapshot or the other, never a mix.
    pub async fn read_snapshot_at(
        base_path: &Path,
        plane_id: PlaneId,
    ) -> io::Result<Option<SnapshotEntity>> {
        let snapshot_path = base_path.join("snapshot.dat");

        // Check if snapshot file exists
        if !snapshot_path.exists() {
            debug!(
                "No snapshot file found for plane {} at {:?}",
                plane_id.raw(),
                snapshot_path
            );
            return Ok(None);
        }

        let bytes = tokio::fs::read(&snapshot_path).await?;
        // Storage creates an empty snapshot.dat up front; empty is "none yet".
        if bytes.is_empty() {
            return Ok(None);
        }
        // Anything else was written whole and renamed into place, so a short
        // or mismatched file is damage. Starting without it would silently
        // drop state the log no longer holds (an installed snapshot), so it
        // is an error.
        let corrupt = |what: String| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("snapshot of plane {} is corrupt: {}", plane_id.raw(), what),
            )
        };
        if bytes.len() < 12 {
            return Err(corrupt(format!("{} bytes, shorter than its header", bytes.len())));
        }
        let expected_checksum = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let len = u64::from_le_bytes(bytes[4..12].try_into().unwrap()) as usize;
        let data_buf = &bytes[12..];
        if data_buf.len() != len {
            return Err(corrupt(format!(
                "header says {} bytes of data, file holds {}",
                len,
                data_buf.len()
            )));
        }
        let actual_checksum = crc32fast::hash(data_buf);
        if actual_checksum != expected_checksum {
            return Err(corrupt(format!(
                "checksum {:#010x}, expected {:#010x}",
                actual_checksum, expected_checksum
            )));
        }
        let snapshot = crate::utils::serde::deserialize::<SnapshotEntity>(data_buf)
            .ok_or_else(|| corrupt("its contents do not decode".to_string()))?;

        info!(
            "Snapshot loaded from disk for plane {}: index={}, term={}, size={} bytes",
            plane_id.raw(),
            snapshot.last_included_index,
            snapshot.last_included_term,
            data_buf.len()
        );
        Ok(Some(snapshot))
    }
}
