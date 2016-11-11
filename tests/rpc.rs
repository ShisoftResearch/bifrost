use bifrost::rpc::*;
use std::sync::Arc;
use std::sync::mpsc::channel;
use byteorder::{ByteOrder, BigEndian};
use std::thread;
use std::num;
use std::sync::Mutex;
use std::time::Duration;

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
                    println!("server received: {}", num);
                    let mut buf = vec![0u8; 8];
                    BigEndian::write_u64(&mut buf, num + 1);
                    conn.send_message(buf).unwrap();
                }
            );
            server.start();
        });
    }
    thread::sleep(Duration::from_millis(1000));
    let NTHREAD = 2;
    let mut threads = Vec::new();
    for i in 1..(NTHREAD + 1) {
        let clients = clients.clone();
        let server_addr = server_addr.clone();
        threads.push(thread::spawn(move||{
            let mut buf = [0u8; 8];
            BigEndian::write_u64(&mut buf, 10u64.pow(i));
            clients.lock().unwrap().send_message(server_addr, buf.to_vec());
        }));
    }
    for thread in threads {
        thread.join();
    }
    for _ in 0..NTHREAD{
        let num = rx.recv().unwrap();
        print!("CHECK NUM: {}", num);
        assert_eq!(num % 10, 3);
    }
}