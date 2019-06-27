use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::cell::RefCell;
use std::collections::linked_list::LinkedList;
use std::fs::{create_dir_all, read_dir, File};
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicUsize};

// segmented log structured durable_queue for state machines

type QueueRef<T> = Rc<RefCell<PartialQueue<T>>>;

#[derive(Serialize, Deserialize)]
pub struct Header {
    id: u64,
    length: u32,
    checksum: u32,
}

pub enum UpdatePolicy {
    Immediate,
    Delayed(u32),
}

pub struct PartialQueue<T>
where
    T: Serialize + DeserializeOwned,
{
    id: u64,
    list: LinkedList<T>,
    file: File,
    pending_ops: u32,
}

pub struct Storage<T>
where
    T: Serialize + DeserializeOwned,
{
    base_path: String,
    counter: u64,
    seg_cap: u32,
    push_policy: UpdatePolicy,
    pop_policy: UpdatePolicy,
    head: QueueRef<T>,
    tail: QueueRef<T>,
    head_id: u64,
}

impl<T> Storage<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(
        storage_path: &String,
        capacity: u32,
        push_policy: UpdatePolicy,
        pop_policy: UpdatePolicy,
    ) -> io::Result<Self> {
        let path = Path::new(storage_path);
        let heading_queue;
        let tailing_queue;
        let mut head_id = 0;
        let mut tail_id = 0;
        let mut last_seg_id = 0;
        if !path.exists() {
            create_dir_all(path);
        }
        let starting_point = 0;
        if read_dir(path)?.count() == 0 {
            heading_queue = wrap_queue(PartialQueue::new_from_path(path, starting_point)?);
            tailing_queue = heading_queue.clone();
        } else {
            let dir = read_dir(path)?;
            let mut files: Vec<String> = dir
                .map(|f| f.unwrap().file_name().to_str().unwrap().to_string())
                .collect();
            files.sort();
            let last_file_name = files.last().unwrap();
            let last_file_path = path.with_file_name(last_file_name);
            let last_file_num: u64 = file_name_to_num(last_file_name);
            let first_file_name = &files[0];
            let first_file_num: u64 = file_name_to_num(first_file_name);
            heading_queue = PartialQueue::from_file_path(path.with_file_name(first_file_name).as_path())?;
            tailing_queue = if files.len() > 1 {
                PartialQueue::from_file_path(last_file_path.as_path())?
            } else {
                heading_queue.clone()
            };
            last_seg_id = last_file_num;
            head_id = first_file_num;
            debug_assert_eq!(last_file_num, tailing_queue.borrow().id);
            debug_assert_eq!(first_file_num, heading_queue.borrow().id);
        }
        Ok(Storage {
            base_path: storage_path.clone(),
            counter: last_seg_id,
            seg_cap: 0,
            push_policy,
            pop_policy,
            head: heading_queue,
            tail: tailing_queue,
            head_id,
        })
    }

    pub fn push(&mut self, data: T) -> io::Result<()> {
        let mut tail = self.tail.borrow_mut();
        if tail.count() >= self.seg_cap as usize {
            // segment is full, need to persist current and get a new one
            tail.persist()?;
            self.counter += 1;
            *tail = PartialQueue::new_from_path(Path::new(&self.base_path), self.counter)?;
        }
        tail.push(data, &self.push_policy)
    }
}

impl<T> PartialQueue<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new_from_path(path: &Path, id: u64) -> io::Result<Self> {
        let file = File::create(path.with_file_name(file_name(id)).as_path())?;
        let mut queue = PartialQueue {
            id,
            list: LinkedList::<T>::new(),
            pending_ops: 0,
            file,
        };
        queue.persist()?;
        Ok(queue)
    }
    pub fn from_file_path(path: &Path) -> io::Result<QueueRef<T>> {
        Self::from_file(File::open(path)?)
    }
    pub fn from_file(file: File) -> io::Result<QueueRef<T>> {
        let mut read_buffer = BufReader::new(file);
        read_buffer.seek(SeekFrom::Start(0))?;
        let header = Header {
            id: read_buffer.read_u64::<LittleEndian>()?,
            length: read_buffer.read_u32::<LittleEndian>()?,
            checksum: read_buffer.read_u32::<LittleEndian>()?,
        };
        let mut buffer = vec![];
        read_buffer.read_to_end(&mut buffer)?;
        if buffer.len() != header.length as usize {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Invalid data length, expect {} got {}",
                    header.length,
                    buffer.len()
                ),
            ));
        }
        let checksum = crc32(buffer.as_slice());
        if checksum != header.checksum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!(
                    "Checksum failed, expect {} got {}",
                    header.checksum, checksum
                ),
            ));
        }
        let queue = PartialQueue {
            id: header.id,
            list: bincode::deserialize(buffer.as_slice()).unwrap(),
            file: read_buffer.into_inner(),
            pending_ops: 0
        };
        Ok(wrap_queue(queue))
    }

    pub fn persist(&mut self) -> io::Result<()> {
        let mut data = bincode::serialize(&self.list).unwrap();
        let len = data.len();
        let checksum = crc32(data.as_slice());
        let mut write_buffer = BufWriter::new(&self.file);
        write_buffer.seek(SeekFrom::Start(0))?;
        write_buffer.write_u64::<LittleEndian>(self.id)?;
        write_buffer.write_u32::<LittleEndian>(len as u32)?;
        write_buffer.write_u32::<LittleEndian>(checksum)?;
        write_buffer.write_all(data.as_mut_slice())?;
        write_buffer.flush()?;
        self.file.sync_all()?;
        self.pending_ops = 0;
        Ok(())
    }

    pub fn push(&mut self, item: T, policy: &UpdatePolicy) -> io::Result<()> {
        self.list.push_back(item);
        self.persist_for_policy(policy)
    }

    pub fn pop(&mut self, policy: &UpdatePolicy) -> io::Result<Option<T>> {
        let item = self.list.pop_front();
        if item.is_some() {
            self.persist_for_policy(policy);
        }
        Ok(item)
    }

    pub fn persist_for_policy(&mut self, policy: &UpdatePolicy) -> io::Result<()> {
        match policy {
            &UpdatePolicy::Immediate => self.persist()?,
            &UpdatePolicy::Delayed(d) if self.pending_ops >= d => self.persist()?,
            &UpdatePolicy::Delayed(_) => self.pending_ops += 1
        }
        Ok(())
    }

    pub fn count(&self) -> usize {
        self.list.len()
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

fn file_name(id: u64) -> String {
    format!("{:0>20}.dat", id)
}

fn file_name_to_num(name: &String) -> u64 {
    name.split('.').next().unwrap().parse().unwrap()
}

fn wrap_queue<T>(queue: PartialQueue<T>) -> QueueRef<T> where
    T: Serialize + DeserializeOwned
{
    Rc::new(RefCell::new(queue))
}