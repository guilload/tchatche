use std::io::BufRead;

use anyhow::{Context, bail};

use crate::delta::Delta;
use crate::digest::Digest;
use crate::serialize::{Deserializable, Serializable};

/// `tchatche`'s own magic number (distinct from chitchat's — the protocols are
/// not wire-compatible).
const MAGIC_NUMBER: u16 = 0xC4A7;

/// A gossip datagram: a `cluster_id` (validated once, in the common header) plus
/// one of the three handshake messages.
#[derive(Debug, Eq, PartialEq)]
pub struct Message {
    pub cluster_id: String,
    pub kind: MessageKind,
}

/// The three steps of the gossip handshake. The 1-byte tag (see [`MessageType`])
/// is the handshake's entire state machine.
#[derive(Debug, Eq, PartialEq)]
pub enum MessageKind {
    /// Step 1: the initiator sends its digest.
    Syn { digest: Digest },
    /// Step 2: the peer replies with its own digest and a delta for the initiator.
    SynAck { digest: Digest, delta: Delta },
    /// Step 3: the initiator sends a delta back. Terminates the handshake.
    Ack { delta: Delta },
    /// A test-only message used to trigger a panic in the server.
    #[cfg(test)]
    PanicForTest,
}

impl Message {
    pub fn new(cluster_id: impl Into<String>, kind: MessageKind) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            kind,
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum ProtocolVersion {
    V0 = 0,
}

impl ProtocolVersion {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::V0),
            _ => None,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum MessageType {
    Syn = 0,
    SynAck = 1,
    Ack = 2,
    #[cfg(test)]
    PanicForTest = 255,
}

impl MessageType {
    fn from_code(code: u8) -> Option<Self> {
        match code {
            0 => Some(Self::Syn),
            1 => Some(Self::SynAck),
            2 => Some(Self::Ack),
            #[cfg(test)]
            255 => Some(Self::PanicForTest),
            _ => None,
        }
    }

    fn to_code(self) -> u8 {
        self as u8
    }
}

impl Serializable for Message {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend(MAGIC_NUMBER.to_le_bytes());
        buf.push(ProtocolVersion::V0 as u8);
        // Common header: cluster_id is validated once per datagram, before the tag.
        self.cluster_id.serialize(buf);
        match &self.kind {
            MessageKind::Syn { digest } => {
                buf.push(MessageType::Syn.to_code());
                digest.serialize(buf);
            }
            MessageKind::SynAck { digest, delta } => {
                buf.push(MessageType::SynAck.to_code());
                digest.serialize(buf);
                delta.serialize(buf);
            }
            MessageKind::Ack { delta } => {
                buf.push(MessageType::Ack.to_code());
                delta.serialize(buf);
            }
            #[cfg(test)]
            MessageKind::PanicForTest => {
                buf.push(MessageType::PanicForTest.to_code());
            }
        }
    }

    fn serialized_len(&self) -> usize {
        2 + 1
            + self.cluster_id.serialized_len()
            + 1
            + match &self.kind {
                MessageKind::Syn { digest } => digest.serialized_len(),
                MessageKind::SynAck { digest, delta } => {
                    digest.serialized_len() + delta.serialized_len()
                }
                MessageKind::Ack { delta } => delta.serialized_len(),
                #[cfg(test)]
                MessageKind::PanicForTest => 0,
            }
    }
}

impl Deserializable for Message {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < 3 {
            bail!("buffer too small for the magic number and protocol version");
        }
        let magic_number = u16::from_le_bytes(buf[0..2].try_into().unwrap());
        if magic_number != MAGIC_NUMBER {
            bail!("invalid tchatche magic number");
        }
        let protocol_version =
            ProtocolVersion::from_code(buf[2]).context("invalid protocol version")?;
        if protocol_version != ProtocolVersion::V0 {
            bail!("unsupported protocol version `{}`", buf[2]);
        }
        buf.consume(3);

        let cluster_id = String::deserialize(buf)?;

        let message_type = buf
            .first()
            .copied()
            .and_then(MessageType::from_code)
            .context("invalid message type")?;
        buf.consume(1);

        let kind = match message_type {
            MessageType::Syn => {
                let digest = Digest::deserialize(buf)?;
                MessageKind::Syn { digest }
            }
            MessageType::SynAck => {
                let digest = Digest::deserialize(buf)?;
                let delta = Delta::deserialize(buf)?;
                MessageKind::SynAck { digest, delta }
            }
            MessageType::Ack => {
                let delta = Delta::deserialize(buf)?;
                MessageKind::Ack { delta }
            }
            #[cfg(test)]
            MessageType::PanicForTest => MessageKind::PanicForTest,
        };
        Ok(Message { cluster_id, kind })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(message: &Message) {
        let mut buf = Vec::new();
        message.serialize(&mut buf);
        assert_eq!(buf.len(), message.serialized_len());
        let deser = Message::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(message, &deser);
    }

    #[test]
    fn test_syn_roundtrip() {
        roundtrip(&Message::new(
            "cluster-a",
            MessageKind::Syn {
                digest: Digest::default(),
            },
        ));
    }

    #[test]
    fn test_ack_roundtrip() {
        roundtrip(&Message::new(
            "cluster-a",
            MessageKind::Ack {
                delta: Delta::default(),
            },
        ));
    }
}
