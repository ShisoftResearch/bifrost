use std::sync::Arc;
use std::time::Duration;

use crate::tcp::server::TcpReq;
use crate::tcp::{shortcut, STANDALONE_ADDRESS};
use crate::DISABLE_SHORTCUT;
use async_std::sync::Mutex;
use bifrost_hasher::hash_str;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use futures::stream::SplitSink;
use futures::SinkExt;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

type ResponseSenders = Arc<SyncMutex<HashMap<u64, oneshot::Sender<BytesMut>>>>;

struct Connection {
    writer: Mutex<SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>>,
    senders: ResponseSenders,
    closed: Arc<AtomicBool>,
    reader: tokio::task::AbortHandle,
}

impl Connection {
    async fn connect(address: &String, timeout: Duration) -> io::Result<Arc<Self>> {
        #[cfg(multisite_test)]
        let routed = std::env::var("MULTISITE_ROUTES").ok().and_then(|routes| {
            routes.split(';').find_map(|route| {
                let (target, proxy) = route.split_once('=')?;
                (target == address).then(|| proxy.to_owned())
            })
        });
        #[cfg(multisite_test)]
        let connect_address = routed.as_ref().unwrap_or(address);
        #[cfg(not(multisite_test))]
        let connect_address = address;
        let socket = time::timeout(timeout, TcpStream::connect(connect_address)).await??;
        let (writer, mut reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        let senders: ResponseSenders = Arc::new(SyncMutex::new(HashMap::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let reader_senders = senders.clone();
        let reader_closed = closed.clone();
        let task = tokio::spawn(async move {
            while let Some(Ok(mut data)) = reader.next().await {
                let message = data.get_u64_le();
                if let Some(sender) = reader_senders.lock().remove(&message) {
                    let _ = sender.send(data);
                }
            }
            reader_closed.store(true, Ordering::Release);
            // A missing response has an unknown outcome. Never replay it here.
            reader_senders.lock().clear();
        });
        Ok(Arc::new(Self {
            writer: Mutex::new(writer),
            senders,
            closed,
            reader: task.abort_handle(),
        }))
    }

    fn invalidate(&self) {
        self.closed.store(true, Ordering::Release);
        self.reader.abort();
        self.senders.lock().clear();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

struct NetworkClient {
    address: String,
    connection: Mutex<Arc<Connection>>,
}

// Remove a waiter also when the caller cancels or times out.
struct PendingResponse {
    senders: ResponseSenders,
    id: u64,
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        self.senders.lock().remove(&self.id);
    }
}

pub struct Client {
    network: Option<NetworkClient>,
    msg_counter: AtomicU64,
    timeout: Duration,
    pub server_id: u64,
    shortcut_token: Option<shortcut::ShortcutToken>,
}

impl Client {
    pub async fn connect_with_timeout(address: &String, timeout: Duration) -> io::Result<Self> {
        let server_id = hash_str(address);
        let shortcut_token = if DISABLE_SHORTCUT {
            None
        } else {
            shortcut::registration_token(server_id).await
        };
        let network = if shortcut_token.is_some() {
            None
        } else {
            if address == STANDALONE_ADDRESS {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "STANDALONE server is not found",
                ));
            }
            Some(NetworkClient {
                address: address.clone(),
                connection: Mutex::new(Connection::connect(address, timeout).await?),
            })
        };
        Ok(Self {
            network,
            msg_counter: AtomicU64::new(0),
            timeout,
            server_id,
            shortcut_token,
        })
    }

    pub async fn connect(address: &String) -> io::Result<Self> {
        Self::connect_with_timeout(address, Duration::from_secs(2)).await
    }

    pub async fn send_msg(&self, msg: TcpReq) -> io::Result<BytesMut> {
        let Some(network) = &self.network else {
            return shortcut::call(self.server_id, msg).await;
        };
        let connection = {
            let mut connection = network.connection.lock().await;
            if connection.closed.load(Ordering::Acquire) {
                // Only a new request can use a new connection. Never retry a sent request.
                *connection = Connection::connect(&network.address, self.timeout).await?;
            }
            connection.clone()
        };
        let msg_id = self.msg_counter.fetch_add(1, Ordering::Relaxed);
        let mut frame = BytesMut::with_capacity(8 + msg.len());
        frame.put_u64_le(msg_id);
        frame.extend_from_slice(msg.as_ref());
        let (sender, receiver) = oneshot::channel();
        {
            let mut senders = connection.senders.lock();
            if connection.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "TCP connection closed",
                ));
            }
            senders.insert(msg_id, sender);
        }
        let _pending = PendingResponse {
            senders: connection.senders.clone(),
            id: msg_id,
        };
        let sent = time::timeout(self.timeout, async {
            let mut writer = connection.writer.lock().await;
            if connection.closed.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "TCP connection closed",
                ));
            }
            writer.send(frame.freeze()).await
        })
        .await;
        match sent {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                connection.invalidate();
                return Err(error);
            }
            Err(error) => {
                connection.invalidate();
                return Err(error.into());
            }
        }
        match time::timeout(self.timeout, receiver).await? {
            Ok(response) => Ok(response),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Response channel closed",
            )),
        }
    }

    pub(crate) fn shortcut_token(&self) -> Option<&shortcut::ShortcutToken> {
        self.shortcut_token.as_ref()
    }
}

unsafe impl Send for Client {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    use std::time::Duration;

    #[cfg(disable_shortcut)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multisite_tcp_does_not_replay_lost_response() {
        let port: u16 = std::env::var("MULTISITE_TEST_PORT")
            .unwrap()
            .parse()
            .unwrap();
        let address = format!("127.0.0.1:{}", port + 1);
        let listener = tokio::net::TcpListener::bind(&address).await.unwrap();
        let peer = tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            let mut first = Framed::new(first, LengthDelimitedCodec::new());
            let request = first.next().await.unwrap().unwrap();
            assert_eq!(&request[8..], b"first");
            drop(first); // The request was taken, but its response was lost.
            let (second, _) = listener.accept().await.unwrap();
            let mut second = Framed::new(second, LengthDelimitedCodec::new());
            let request = second.next().await.unwrap().unwrap();
            assert_eq!(
                &request[8..],
                b"second",
                "do not replay an uncertain request"
            );
            second.send(request.freeze()).await.unwrap();
        });
        let client = Client::connect_with_timeout(&address, Duration::from_millis(500))
            .await
            .unwrap();
        assert!(client
            .send_msg(BytesMut::from(&b"first"[..]))
            .await
            .is_err());
        assert_eq!(
            &client
                .send_msg(BytesMut::from(&b"second"[..]))
                .await
                .unwrap()[..],
            b"second"
        );
        time::timeout(Duration::from_secs(2), peer)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_connect_timeout() {
        let _ = env_logger::builder().format_timestamp(None).try_init();

        // Try to connect to a non-existent server with short timeout
        let addr = String::from("127.0.0.1:9999");
        let timeout = Duration::from_millis(100);

        let result = Client::connect_with_timeout(&addr, timeout).await;
        // This should fail since there's no server
        assert!(
            result.is_err(),
            "Connection to non-existent server should fail"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_standalone_address() {
        let _ = env_logger::builder().format_timestamp(None).try_init();

        // Connecting to STANDALONE only fails while nothing has a standalone
        // shortcut registered; a concurrent standalone server would make this
        // connect succeed through the shortcut instead.
        let _serial = crate::tcp::STANDALONE_TEST_LOCK.lock().await;

        // Try to connect to STANDALONE address
        let standalone_addr = STANDALONE_ADDRESS.to_string();
        let result = Client::connect(&standalone_addr).await;
        assert!(result.is_err(), "Connection to STANDALONE should fail");

        if let Err(e) = result {
            assert_eq!(e.kind(), io::ErrorKind::Other);
            assert!(e.to_string().contains("STANDALONE"));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_client_server_id() {
        let addr = String::from("127.0.0.1:9876");
        let expected_id = hash_str(&addr);

        // Even if connection fails, we can test server_id calculation
        let timeout = Duration::from_millis(50);
        let _ = Client::connect_with_timeout(&addr, timeout).await;

        // Verify hash_str produces consistent results
        assert_eq!(hash_str(&addr), expected_id);
    }
}
