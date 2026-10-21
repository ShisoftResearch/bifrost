use std::sync::Arc;
use std::time::Duration;

use crate::tcp::server::TcpReq;
use crate::tcp::{shortcut, STANDALONE_ADDRESS};
use crate::DISABLE_SHORTCUT;
use async_std::sync::Mutex;
use bifrost_hasher::hash_str;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::prelude::*;
use futures::SinkExt;
use parking_lot::Mutex as SyncMutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use tokio::io;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tokio_util::sync::CancellationToken;

// This marker proves that this request did not reach the socket or callback.
#[derive(Debug)]
struct RequestNotSent(io::Error);

impl std::fmt::Display for RequestNotSent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RequestNotSent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

fn not_sent(error: io::Error) -> io::Error {
    io::Error::new(error.kind(), RequestNotSent(error))
}

pub(crate) fn request_was_not_sent(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|cause| cause.is::<RequestNotSent>())
}

type ResponseSenders = Arc<SyncMutex<HashMap<u64, oneshot::Sender<BytesMut>>>>;

const WRITE_QUEUED: u8 = 0;
const WRITE_STARTED: u8 = 1;
const WRITE_CANCELLED: u8 = 2;

struct WriteRequest {
    frame: Bytes,
    state: Arc<AtomicU8>,
    completed: oneshot::Sender<io::Result<()>>,
}

struct PendingWrite(Arc<AtomicU8>);

impl Drop for PendingWrite {
    fn drop(&mut self) {
        let _ = self.0.compare_exchange(
            WRITE_QUEUED,
            WRITE_CANCELLED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

// Only this task owns the sink. Queue order defines frame order on the socket.
async fn write_frames<S>(
    mut sink: S,
    mut requests: mpsc::Receiver<WriteRequest>,
    closed: Arc<AtomicBool>,
    stop: CancellationToken,
    #[cfg(test)] gate: Arc<tokio::sync::Semaphore>,
) where
    S: futures::Sink<Bytes, Error = io::Error> + Unpin,
{
    loop {
        let request = tokio::select! {
            _ = stop.cancelled() => break,
            request = requests.recv() => match request { Some(request) => request, None => break },
        };
        #[cfg(test)]
        let _permit = tokio::select! {
            _ = stop.cancelled() => break,
            permit = gate.acquire() => permit.unwrap(),
        };
        if closed.load(Ordering::Acquire) {
            break;
        }
        if request.completed.is_closed()
            || request
                .state
                .compare_exchange(
                    WRITE_QUEUED,
                    WRITE_STARTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
        {
            continue;
        }
        let result = tokio::select! {
            _ = stop.cancelled() => break,
            result = sink.send(request.frame) => result,
        };
        let failed = result.is_err();
        let _ = request.completed.send(result);
        if failed {
            break;
        }
    }
}

struct Connection {
    writer: mpsc::Sender<WriteRequest>,
    writer_task: tokio::task::AbortHandle,
    stop: CancellationToken,
    #[cfg(test)]
    writer_gate: Arc<tokio::sync::Semaphore>,
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
        let stop = CancellationToken::new();
        let reader_stop = stop.clone();
        let reader_senders = senders.clone();
        let reader_closed = closed.clone();
        let task = tokio::spawn(async move {
            loop {
                let data = tokio::select! {
                    _ = reader_stop.cancelled() => break,
                    data = reader.next() => data,
                };
                let Some(Ok(mut data)) = data else { break };
                let message = data.get_u64_le();
                if let Some(sender) = reader_senders.lock().remove(&message) {
                    let _ = sender.send(data);
                }
            }
            reader_closed.store(true, Ordering::Release);
            // A missing response has an unknown outcome. Never replay it here.
            reader_senders.lock().clear();
            reader_stop.cancel();
        });
        let (requests, receiver) = mpsc::channel(64);
        let writer_closed = closed.clone();
        let writer_stop = stop.clone();
        let writer_senders = senders.clone();
        #[cfg(test)]
        let writer_gate = Arc::new(tokio::sync::Semaphore::new(1));
        #[cfg(test)]
        let gate = writer_gate.clone();
        let writer_task = tokio::spawn(async move {
            write_frames(
                writer,
                receiver,
                writer_closed.clone(),
                writer_stop.clone(),
                #[cfg(test)]
                gate,
            )
            .await;
            writer_closed.store(true, Ordering::Release);
            writer_senders.lock().clear();
            writer_stop.cancel();
        });
        Ok(Arc::new(Self {
            writer: requests,
            writer_task: writer_task.abort_handle(),
            stop,
            #[cfg(test)]
            writer_gate,
            senders,
            closed,
            reader: task.abort_handle(),
        }))
    }

    fn invalidate(&self) {
        self.closed.store(true, Ordering::Release);
        self.stop.cancel();
        self.reader.abort();
        self.writer_task.abort();
        self.senders.lock().clear();
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.stop.cancel();
        self.reader.abort();
        self.writer_task.abort();
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
            return shortcut::call(self.server_id, msg).await.map_err(not_sent);
        };
        let connection = {
            let mut connection = network.connection.lock().await;
            if connection.closed.load(Ordering::Acquire) {
                // Only a new request can use a new connection. Never retry a sent request.
                *connection = Connection::connect(&network.address, self.timeout)
                    .await
                    .map_err(not_sent)?;
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
                return Err(not_sent(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "TCP connection closed",
                )));
            }
            senders.insert(msg_id, sender);
        }
        let _pending = PendingResponse {
            senders: connection.senders.clone(),
            id: msg_id,
        };
        let state = Arc::new(AtomicU8::new(WRITE_QUEUED));
        let pending_write = PendingWrite(state.clone());
        let (completed, sent) = oneshot::channel();
        let request = WriteRequest {
            frame: frame.freeze(),
            state: state.clone(),
            completed,
        };
        let result = time::timeout(self.timeout, async {
            connection
                .writer
                .send(request)
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "TCP writer closed"))?;
            sent.await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "TCP writer stopped"))?
        })
        .await;
        // Cancel a queued request before classifying the result. The writer
        // claims STARTED atomically, so a reported unsent request cannot start.
        drop(pending_write);
        let error = match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(error),
            Err(error) => Some(error.into()),
        };
        if let Some(error) = error {
            connection.invalidate();
            return Err(if state.load(Ordering::Acquire) == WRITE_STARTED {
                error
            } else {
                not_sent(error)
            });
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
    use std::convert::TryInto;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bflock_socket_backpressure_keeps_other_connection_live() {
        let listener = tokio::net::TcpSocket::new_v4().unwrap();
        listener.set_recv_buffer_size(1024).unwrap();
        listener.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let listener = listener.listen(128).unwrap();
        let address = listener.local_addr().unwrap();
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_send_buffer_size(1024).unwrap();
        let socket = socket.connect(address).await.unwrap();
        let (_blocked_peer, _) = listener.accept().await.unwrap();
        let (sink, _reader) = Framed::new(socket, LengthDelimitedCodec::new()).split();
        let (requests, receiver) = mpsc::channel(2);
        let stop = CancellationToken::new();
        let writer = tokio::spawn(write_frames(
            sink,
            receiver,
            Arc::new(AtomicBool::new(false)),
            stop.clone(),
            Arc::new(tokio::sync::Semaphore::new(1)),
        ));
        let state = Arc::new(AtomicU8::new(WRITE_QUEUED));
        let (completed, mut sent) = oneshot::channel();
        requests
            .send(WriteRequest {
                frame: Bytes::from(vec![7; 4 * 1024 * 1024]),
                state: state.clone(),
                completed,
            })
            .await
            .unwrap();
        // The small kernel buffers cannot flush this frame without a reader.
        assert!(time::timeout(Duration::from_millis(20), &mut sent)
            .await
            .is_err());
        assert_eq!(state.load(Ordering::Acquire), WRITE_STARTED);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
            let frame = frames.next().await.unwrap().unwrap();
            frames.send(frame.freeze()).await.unwrap();
        });
        let client = Client::connect_with_timeout(&address, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            &client
                .send_msg(BytesMut::from(&b"independent"[..]))
                .await
                .unwrap()[..],
            b"independent"
        );
        assert!(matches!(
            sent.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        stop.cancel();
        writer.await.unwrap();
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn bflock_cancelled_queued_frame_never_reaches_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let client = Client::connect_with_timeout(&address, Duration::from_secs(1))
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
        let connection = client
            .network
            .as_ref()
            .unwrap()
            .connection
            .lock()
            .await
            .clone();
        let gate = connection.writer_gate.acquire().await.unwrap();
        let cancelled = Arc::new(AtomicU8::new(WRITE_QUEUED));
        let (completed, skipped) = oneshot::channel();
        connection
            .writer
            .send(WriteRequest {
                frame: Bytes::from_static(b"cancelled"),
                state: cancelled.clone(),
                completed,
            })
            .await
            .unwrap();
        drop(PendingWrite(cancelled.clone()));
        let (completed, sent) = oneshot::channel();
        connection
            .writer
            .send(WriteRequest {
                frame: Bytes::from_static(b"whole-frame"),
                state: Arc::new(AtomicU8::new(WRITE_QUEUED)),
                completed,
            })
            .await
            .unwrap();
        drop(gate);
        assert_eq!(&frames.next().await.unwrap().unwrap()[..], b"whole-frame");
        sent.await.unwrap().unwrap();
        assert!(skipped.await.is_err());
        assert_eq!(cancelled.load(Ordering::Acquire), WRITE_CANCELLED);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bflock_concurrent_frames_keep_integrity_and_sender_order() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
            let mut next = [0_u64; 16];
            for _ in 0..512 {
                let frame = frames.next().await.unwrap().unwrap();
                assert_eq!(frame.len(), 8 + 16 + 4096);
                let sender = u64::from_le_bytes(frame[8..16].try_into().unwrap()) as usize;
                let sequence = u64::from_le_bytes(frame[16..24].try_into().unwrap());
                assert_eq!(sequence, next[sender]);
                next[sender] += 1;
                assert!(frame[24..].iter().all(|byte| *byte == sender as u8));
                frames.send(frame.freeze()).await.unwrap();
            }
            assert_eq!(next, [32; 16]);
        });
        let client = Arc::new(
            Client::connect_with_timeout(&address, Duration::from_secs(5))
                .await
                .unwrap(),
        );
        let mut tasks = Vec::new();
        for sender in 0..16_u64 {
            let client = client.clone();
            tasks.push(tokio::spawn(async move {
                for sequence in 0..32_u64 {
                    let mut frame = BytesMut::new();
                    frame.put_u64_le(sender);
                    frame.put_u64_le(sequence);
                    frame.extend_from_slice(&vec![sender as u8; 4096]);
                    assert_eq!(client.send_msg(frame.clone()).await.unwrap(), frame);
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        peer.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn bflock_blocked_writer_does_not_block_other_connection() {
        let blocked_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blocked_address = blocked_listener.local_addr().unwrap().to_string();
        let blocked = Arc::new(
            Client::connect_with_timeout(&blocked_address, Duration::from_secs(5))
                .await
                .unwrap(),
        );
        let (_blocked_socket, _) = blocked_listener.accept().await.unwrap();
        let connection = blocked
            .network
            .as_ref()
            .unwrap()
            .connection
            .lock()
            .await
            .clone();
        // Hold this connection's sink before its first socket poll.
        let writer = connection.writer_gate.acquire().await.unwrap();
        let sender = {
            let blocked = blocked.clone();
            tokio::spawn(async move { blocked.send_msg(BytesMut::from(&b"blocked"[..])).await })
        };
        while connection.senders.lock().is_empty() {
            tokio::task::yield_now().await;
        }
        assert!(!sender.is_finished());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
            let frame = frames.next().await.unwrap().unwrap();
            frames.send(frame.freeze()).await.unwrap();
        });
        let client = Client::connect_with_timeout(&address, Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            &client
                .send_msg(BytesMut::from(&b"independent"[..]))
                .await
                .unwrap()[..],
            b"independent"
        );
        assert!(!sender.is_finished());
        sender.abort();
        let _ = sender.await;
        drop(writer);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn commit_transport_classifies_timeouts_before_and_after_send() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let client = Client::connect_with_timeout(&address, Duration::from_millis(50))
            .await
            .unwrap();
        let (_peer, _) = listener.accept().await.unwrap();
        let connection = client
            .network
            .as_ref()
            .unwrap()
            .connection
            .lock()
            .await
            .clone();
        let writer = connection.writer_gate.acquire().await.unwrap();
        let error = client
            .send_msg(BytesMut::from(&b"unsent"[..]))
            .await
            .unwrap_err();
        assert!(request_was_not_sent(&error), "{error:?}");
        drop(writer);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let client = Client::connect_with_timeout(&address, Duration::from_millis(50))
            .await
            .unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut frames = Framed::new(socket, LengthDelimitedCodec::new());
            let request = frames.next().await.unwrap().unwrap();
            assert_eq!(&request[8..], b"sent");
            tokio::time::sleep(Duration::from_millis(100)).await;
        });
        let error = client
            .send_msg(BytesMut::from(&b"sent"[..]))
            .await
            .unwrap_err();
        assert!(
            !request_was_not_sent(&error),
            "a lost reply is uncertain: {error:?}"
        );
        peer.await.unwrap();
    }

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
