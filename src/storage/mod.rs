use std::sync::atomic::{AtomicUsize, AtomicBool};
use std::io::{BufWriter, Cursor, BufReader, Error, ErrorKind, BufRead, Seek, SeekFrom};
use std::fs::{File, create_dir_all, read_dir};
use std::path::Path;
use std::io;
use byteorder::{LittleEndian, ReadBytesExt};

// segmented log structured storage for state machines and queues

pub struct Storage {
    base_path: String,
    counter: u64,
    seg_pos: u64,
    seg_file: File
}

impl Storage {
    pub fn new(storage_path: &String) -> io::Result<Self> {
        let path = Path::new(storage_path);
        let mut last_data_id = 0;
        let last_file;
        let mut last_file_pos = 0;
        if !path.exists() {
            create_dir_all(path);
        }
        let starting_point = 0;
        if read_dir(path)?.count() == 0 {
            let first_file_path = path.with_file_name(format!("{:0>20}.dat", starting_point));
            last_file = File::create(first_file_path.as_path())?;
        } else {
            let dir = read_dir(path)?;
            let mut files: Vec<String> = dir.map(|f| f.unwrap().file_name().to_str().unwrap().to_string()).collect();
            files.sort();
            let last_file_name = files.last().unwrap();
            let last_file_path = path.with_file_name(last_file_name);
            let last_file_num: u64 = last_file_name.split('.').next().unwrap().parse().unwrap();
            let mut read_buffer = BufReader::new(File::open(last_file_path)?);
            last_data_id = last_file_num;
            while read_buffer.fill_buf()?.len() > 0 {
                let data_id = read_buffer.read_u64::<LittleEndian>()?;
                let length = read_buffer.read_u32::<LittleEndian>()?;
                if last_data_id != data_id {
                    return Err(Error::from(ErrorKind::InvalidData));
                }
                last_data_id += 1;
                last_file_pos = read_buffer.seek(SeekFrom::Current(length as i64))?;
            }
            last_file = read_buffer.into_inner();
        }
        Ok(Storage {
            base_path: storage_path.clone(),
            counter: last_data_id,
            seg_pos: last_file_pos,
            seg_file: last_file
        })
    }


}
