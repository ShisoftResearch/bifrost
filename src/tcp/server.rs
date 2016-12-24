use std::io::{self, ErrorKind, Write};

use futures::{future, Future, BoxFuture};
use tokio_core::io::{Io, Codec, Framed, EasyBuf};
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::Service;

use tcp::framed::BytesCodec;

