use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::io::{BufWriter, Cursor, BufReader, Error, ErrorKind, BufRead, Seek, SeekFrom};
use std::fs::{File, create_dir_all, read_dir};
use std::path::Path;
use std::io;
use byteorder::{LittleEndian, ReadBytesExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use crc32fast::Hasher;
use std::collections::linked_list::LinkedList;
use std::rc::Rc;
use std::cell::RefCell;

// segmented log structured storage for state machines and queues

type QueueRef<T> = Rc<RefCell<PartialQueue<T>>>;

#[derive(Serialize, Deserialize)]
pub struct Header {
    id: u64,
    length: u32,
    checksum: u32,
}

pub enum UpdatePolicy {
    Immediate,
    Delayed(u32)
}

pub struct PartialQueue<T> where T: Serialize + DeserializeOwned {
    id: u64,
    list: LinkedList<T>,
    file: File,
}

pub struct Storage<T> where T: Serialize + DeserializeOwned {
    base_path: String,
    counter: u64,
    seg_cap: u32,
    pending_push: u32,
    pending_pop: u32,
    push_policy: UpdatePolicy,
    pop_policy: UpdatePolicy,
    head: QueueRef<T>,
    tail: QueueRef<T>,
    head_id: u64,
    tail_id: u64
}

impl <T>Storage<T> where T: Serialize + DeserializeOwned {
    pub fn new(storage_path: &String, capacity: usize) -> io::Result<Self> {
        unimplemented!()
//        let path = Path::new(storage_path);
//        let mut last_data_id = 0;
//        let last_file;
//        let mut last_file_pos = 0;
//        if !path.exists() {
//            create_dir_all(path);
//        }
//        let starting_point = 0;
//        if read_dir(path)?.count() == 0 {
//            let first_file_path = path.with_file_name(format!("{:0>20}.dat", starting_point));
//            last_file = File::create(first_file_path.as_path())?;
//        } else {
//            let dir = read_dir(path)?;
//            let mut files: Vec<String> = dir.map(|f| f.unwrap().file_name().to_str().unwrap().to_string()).collect();
//            files.sort();
//            let last_file_name = files.last().unwrap();
//            let last_file_path = path.with_file_name(last_file_name);
//            let last_file_num: u64 = last_file_name.split('.').next().unwrap().parse().unwrap();
//            let mut read_buffer = BufReader::new(File::open(last_file_path)?);
//            last_data_id = last_file_num;
//            while read_buffer.fill_buf()?.len() > 0 {
//                let data_id = read_buffer.read_u64::<LittleEndian>()?;
//                let length = read_buffer.read_u32::<LittleEndian>()?;
//                if last_data_id != data_id {
//                    return Err(Error::from(ErrorKind::InvalidData));
//                }
//                last_data_id += 1;
//                last_file_pos = read_buffer.seek(SeekFrom::Current(length as i64))?;
//            }
//            last_file = read_buffer.into_inner();
//        }
//        Ok(Storage {
//            base_path: storage_path.clone(),
//            counter: last_data_id,
//            seg_pos: last_file_pos,
//            seg_file: last_file,
//            capacity
//        })
    }

    pub fn push(data: T) -> io::Result<()> {
        unimplemented!()
    }
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}
