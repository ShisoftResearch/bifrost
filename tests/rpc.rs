use bifrost::rpc::*;
use std::sync::Arc;
use std::sync::mpsc::channel;
use byteorder::{ByteOrder, BigEndian};
use std::thread;
use std::num;
use std::sync::Mutex;

#[test]
fn service () {
    let server_addr = String::from("127.0.0.1:1156");
    let (tx, rx) = channel();
    let clients = Arc::new(Mutex::new(Clients::<_>::new(
        move |data| {
            let num = BigEndian::read_u64(data.as_ref());
            println!("client received: {}", num);
            tx.send(num + 2).unwrap();
        }
    )));
    {
        let server_addr = server_addr.clone();
        thread::spawn(move ||{
            let mut server = Server::new(
                server_addr,
                move |data, conn| {
                    println!("SERVER RECEIVED");
                    let num = BigEndian::read_u64(data.as_ref());
                    let mut buf = Vec::<u8>::with_capacity(8);
                    BigEndian::write_u64(&mut buf, num + 1);
                    conn.send_message(buf).unwrap();
                    println!("server received: {}", num);
                }
            );
            server.start();
        });
    }
    let NTHREAD = 1;
    let mut threads = Vec::new();
    for i in 0..NTHREAD {
        let clients = clients.clone();
        let server_addr = server_addr.clone();
        threads.push(thread::spawn(move||{
            let mut buf = [0u8; 8];
            BigEndian::write_u64(&mut buf, (i as u64).pow(10));
            clients.lock().unwrap().send_message(server_addr, buf.to_vec());
        }));
    }
    for thread in threads {
        thread.join();
    }
}