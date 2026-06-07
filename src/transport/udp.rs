use std::io;
use std::net::SocketAddr;

use anyhow::Context;
use async_trait::async_trait;
use tracing::warn;

use crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE;
use crate::message::Message;
use crate::serialize::{Deserializable, Serializable};
use crate::transport::{Socket, Transport};

pub struct UdpTransport;

#[async_trait]
impl Transport for UdpTransport {
    async fn open(&self, bind_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>> {
        let udp_socket = UdpSocket::open(bind_addr).await?;
        Ok(Box::new(udp_socket))
    }
}

pub struct UdpSocket {
    buf_send: Vec<u8>,
    buf_recv: Box<[u8; MAX_UDP_DATAGRAM_PAYLOAD_SIZE]>,
    socket: tokio::net::UdpSocket,
}

impl UdpSocket {
    pub async fn open(bind_addr: SocketAddr) -> anyhow::Result<UdpSocket> {
        let socket = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .with_context(|| format!("failed to bind to {bind_addr}/UDP for gossip"))?;
        Ok(UdpSocket {
            buf_send: Vec::with_capacity(MAX_UDP_DATAGRAM_PAYLOAD_SIZE),
            buf_recv: Box::new([0u8; MAX_UDP_DATAGRAM_PAYLOAD_SIZE]),
            socket,
        })
    }
}

fn is_transient_io_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::OutOfMemory
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionRefused
    )
}

#[async_trait]
impl Socket for UdpSocket {
    async fn send(&mut self, to_addr: SocketAddr, message: Message) -> anyhow::Result<()> {
        self.buf_send.clear();
        message.serialize(&mut self.buf_send);
        self.send_bytes(to_addr, &self.buf_send).await?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, Message)> {
        loop {
            if let Some(message) = self.receive_one().await? {
                return Ok(message);
            }
        }
    }
}

impl UdpSocket {
    async fn receive_one(&mut self) -> anyhow::Result<Option<(SocketAddr, Message)>> {
        let (len, from_addr) = match self.socket.recv_from(&mut self.buf_recv[..]).await {
            Ok(result) => result,
            Err(err) if is_transient_io_error(&err) => {
                warn!(error=%err, "transient UDP recv error");
                return Ok(None);
            }
            Err(err) => {
                return Err(err).context("fatal UDP recv error");
            }
        };
        let mut buf = &self.buf_recv[..len];
        match Message::deserialize(&mut buf) {
            Ok(msg) => Ok(Some((from_addr, msg))),
            Err(err) => {
                warn!(payload_len=len, from=%from_addr, err=%err, "invalid tchatche payload");
                Ok(None)
            }
        }
    }

    pub(crate) async fn send_bytes(
        &self,
        to_addr: SocketAddr,
        payload: &[u8],
    ) -> anyhow::Result<()> {
        self.socket
            .send_to(payload, to_addr)
            .await
            .context("failed to send tchatche message to peer")?;
        Ok(())
    }
}
