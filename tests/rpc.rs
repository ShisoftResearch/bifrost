use bifrost::rpc;
use std::sync::Arc;
use std::sync::mpsc::channel;
use byteorder::{ByteOrder, BigEndian};
use std::thread;
use std::num;

#[test]
fn service () {
    //let (tx, rx) = channel();
    let (server, clients) = rpc::new(1156, move |data, conn| {
        let num = BigEndian::read_u64(data.as_ref());
        let mut buf = Vec::<u8>::with_capacity(8);
        BigEndian::write_u64(&mut buf, num + 1);
        conn.send_message(buf).unwrap();
    }, move |data| {
        let num = BigEndian::read_u64(data.as_ref());
        //tx.send(num + 2).unwrap();
    });
    let NTHREAD = 10;
    let mut threads = Vec::new();
    for i in 0..NTHREAD {
        let clients = clients.clone();
        threads.push(thread::spawn(move||{
            let mut buf = Vec::<u8>::with_capacity(8);
            BigEndian::write_u64(&mut buf, (i as u64).pow(10));
            clients.lock().unwrap().send_message(String::from("127.0.0.1:1156"), buf);
        }));
    }
}