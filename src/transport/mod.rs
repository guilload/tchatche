use std::net::SocketAddr;

use async_trait::async_trait;

use crate::message::Message;

mod channel;
mod udp;

pub use channel::{ChannelTransport, Statistics};
pub use udp::{UdpSocket, UdpTransport};

#[async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn open(&self, listen_addr: SocketAddr) -> anyhow::Result<Box<dyn Socket>>;
}

#[async_trait]
pub trait Socket: Send + Sync + 'static {
    /// Returns an error only if the transport is broken and may not send in the future.
    async fn send(&mut self, to: SocketAddr, msg: Message) -> anyhow::Result<()>;
    /// Returns an error only if the transport is broken and may not receive in the future.
    async fn recv(&mut self) -> anyhow::Result<(SocketAddr, Message)>;
}
