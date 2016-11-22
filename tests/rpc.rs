use bifrost::rpc::*;
use std::sync::Arc;
use std::sync::mpsc::channel;
use byteorder::{ByteOrder, LittleEndian};
use std::thread;
use std::sync::Mutex;
use std::time::Duration;

#[test]
fn tcp_transmission () {
    let server_addr = String::from("127.0.0.1:1156");
    let (tx, rx) = channel();
    let clients = Arc::new(Mutex::new(Clients::new(
        Box::new(
            move |data| {
                let num = LittleEndian::read_u64(data.as_ref());
                println!("client received: {}", num);
                tx.send(num + 2).unwrap();
            }
        )
    )));
    {
        let server_addr = server_addr.clone();
        thread::spawn(move ||{
            let mut server = Server::new(
                &server_addr,
                Box::new(
                    move |data, conn| {
                        println!("SERVER RECEIVED");
                        let num = LittleEndian::read_u64(data.as_ref());
                        println!("server received: {}", num);
                        let mut buf = vec![0u8; 8];
                        LittleEndian::write_u64(&mut buf, num + 1);
                        conn.send_message(buf).unwrap();
                    }
                )
            );
            server.start();
        });
    }
    thread::sleep(Duration::from_millis(1000));
    let nthread = 4;
    let mut threads = Vec::new();
    for i in 1..(nthread + 1) {
        let clients = clients.clone();
        let server_addr = server_addr.clone();
        threads.push(thread::spawn(move||{
            let mut buf = [0u8; 8];
            LittleEndian::write_u64(&mut buf, 10u64.pow(i));
            clients.lock().unwrap().send_message(&server_addr, buf.to_vec());
        }));
    }
    for thread in threads {
        let _ = thread.join();
    }
    for _ in 0..nthread {
        let num = rx.recv().unwrap();
        print!("CHECK NUM: {}", num);
        assert_eq!(num % 10, 3);
    }
}

mod simple_service {

    use std::thread;
    use std::time::Duration;

    service! {
        rpc hello(name: String) -> String;
        rpc error(message: String) | String;
    }
    #[derive(Clone)]
    struct HelloServer;

    impl Server for HelloServer {
        fn hello(&self, name: String) -> Result<String, ()> {
            Ok(format!("Hello, {}!", name))
        }
        fn error(&self, message: String) -> Result<(), String> {
            Err(message)
        }
    }
    #[test]
    fn simple_rpc () {
        let addr = String::from("127.0.0.1:1300");
        {
            let addr = addr.clone();
            thread::spawn(move|| {
                HelloServer.listen(&addr);
            });
        }
        thread::sleep(Duration::from_millis(1000));
        let mut client = SyncClient::new(&addr);
        let response = client.hello(String::from("Jack"));
        let greeting_str = response.unwrap();
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack!"));
        let expected_err_msg = String::from("This error is a good one");
        let response = client.error(expected_err_msg.clone());
        let error_msg = response.err().unwrap();
        assert_eq!(error_msg, expected_err_msg);
    }
}

mod struct_service {
    use std::thread;
    use std::time::Duration;

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Greeting {
        name: String,
        time: u32
    }

    #[derive(Serialize, Deserialize, Debug)]
    pub struct Respond {
        text: String,
        owner: u32
    }

    service! {
        rpc hello(gret: Greeting) -> Respond;
    }
    #[derive(Clone)]
    struct HelloServer;

    impl Server for HelloServer {
        fn hello(&self, gret: Greeting) -> Result<Respond, ()> {
            Ok(Respond {
                text: format!("Hello, {}. It is {} now!", gret.name, gret.time),
                owner: 42
            })
        }
    }
    #[test]
    fn struct_rpc () {
        let addr = String::from("127.0.0.1:1400");
        {
            let addr = addr.clone();
            thread::spawn(move|| {
                HelloServer.listen(&addr);
            });
        }
        thread::sleep(Duration::from_millis(1000));
        let mut client = SyncClient::new(&addr);
        let response = client.hello(Greeting {
            name: String::from("Jack"),
            time: 12
        });
        let res = response.unwrap();
        let greeting_str = res.text;
        println!("SERVER RESPONDED: {}", greeting_str);
        assert_eq!(greeting_str, String::from("Hello, Jack. It is 12 now!"));
        assert_eq!(42, res.owner);
    }
}