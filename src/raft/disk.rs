// Now only offers log persistent

use std::fs::OpenOptions;
use crate::raft::{Options, Storage, RaftMeta, LogsMap, LogEntry};
use std::path::Path;
use tokio::fs::*;
use tokio::io::*;
use async_std::sync::*;
use std::io;
use std::ops::Bound::*;
use serde::{Serialize, Deserialize};
use std::io::Read;
use std::collections::BTreeMap;

const MAX_LOG_CAPACITY: usize = 10;

#[derive(Clone)]
pub struct DiskOptions {
    pub path: String,
    pub take_snapshots: bool,
    pub append_logs: bool,
    pub trim_logs: bool
}

pub struct StorageEntity {
    logs: Option<File>,
    snapshot: Option<File>,
    last_term: u64
}

#[derive(Serialize, Deserialize)]
struct DiskLogEntry {
    term: u64,
    commit_index: u64,
    last_applied: u64,
    log: LogEntry
}

impl StorageEntity {
    pub fn new_with_options(
        opts: &Options,
        term: &mut u64,
        commit_index: &mut u64,
        last_applied: &mut u64,
        logs: &mut LogsMap
    ) -> io::Result<Option<Self>> {
        Ok(match &opts.storage {
            &Storage::DISK(ref options) => {
                let base_path = Path::new(&options.path);
                let _ = std::fs::create_dir_all(base_path);
                let log_path = base_path.with_file_name("log.dat");
                let snapshot_path = base_path.with_file_name("snapshot.dat");
                let mut open_opts = OpenOptions::new();
                open_opts
                    .write(true)
                    .create(true)
                    .read(true)
                    .truncate(false);
                Some(Self {
                    logs: if options.append_logs {
                        let mut log_file = open_opts.open(log_path.as_path())?;
                        let mut len_buf = [0u8; 8];
                        let mut counter = 0;
                        loop {
                            if log_file.read_exact(&mut len_buf).is_err() {
                                break;
                            }
                            let len = u64::from_le_bytes(len_buf);
                            let mut data_buf = vec![0u8; len as usize];
                            if log_file.read_exact(&mut data_buf).is_err() {
                                break;
                            }
                            let entry = crate::utils::serde::deserialize::<DiskLogEntry>(data_buf.as_slice()).unwrap();
                            *term = entry.term;
                            *commit_index = entry.commit_index;
                            *last_applied = entry.last_applied;
                            logs.insert(entry.term, entry.log);
                            counter += 1;
                        }
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
                    last_term: 0
                })
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
            debug!("Append logs to disk");
            let was_last_term = self.last_term;
            let mut counter = 0;
            let mut terms_appended = vec![];
            for (term, log) in logs.range((Excluded(self.last_term), Unbounded)) {
                let entry = DiskLogEntry {
                    term: *term,
                    commit_index: meta.commit_index,
                    last_applied: meta.last_applied,
                    log: log.clone()
                };
                let entry_data = crate::utils::serde::serialize(&entry);
                f.write(&(entry_data.len() as u64).to_le_bytes()).await?;
                f.write(entry_data.as_slice()).await?;
                self.last_term = *term;
                terms_appended.push(self.last_term);
                counter += 1;
            }
            if counter > 0 {
                f.sync_all().await?;
                debug!("Appended and persisted {} logs, was {}, appended {:?}", counter, was_last_term, terms_appended);q
            }
        }
        Ok(())
    }

    pub async fn post_processing<'a>(
        &mut self,
        meta: &RwLockWriteGuard<'a, RaftMeta>,
        mut logs: RwLockWriteGuard<'a, LogsMap>
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
}