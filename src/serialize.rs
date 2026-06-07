use std::io::BufRead;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use anyhow::{Context, bail};
use bytestring::ByteString;

use crate::{Heartbeat, MemberId};

/// Trait to serialize messages.
///
/// `tchatche` uses a custom binary serialization format whose point is to make it
/// possible to truncate a delta payload to a given mtu.
pub trait Serializable {
    fn serialize(&self, buf: &mut Vec<u8>);

    fn serialize_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.serialize(&mut buf);
        buf
    }

    fn serialized_len(&self) -> usize;
}

/// Trait to deserialize messages.
pub trait Deserializable: Sized {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self>;
}

impl Serializable for u8 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self)
    }

    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for u8 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let byte: [u8; 1] = Deserializable::deserialize(buf)?;
        Ok(byte[0])
    }
}

impl Serializable for u16 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        2
    }
}

impl Deserializable for u16 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u16_bytes: [u8; 2] = Deserializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u16_bytes))
    }
}

impl Serializable for u64 {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.to_le_bytes().serialize(buf);
    }
    fn serialized_len(&self) -> usize {
        8
    }
}

impl Deserializable for u64 {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let u64_bytes: [u8; 8] = Deserializable::deserialize(buf)?;
        Ok(Self::from_le_bytes(u64_bytes))
    }
}

impl Serializable for bool {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }
    fn serialized_len(&self) -> usize {
        1
    }
}

impl Deserializable for bool {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let bool_byte: [u8; 1] = Deserializable::deserialize(buf)?;
        Ok(bool_byte[0] != 0)
    }
}

#[repr(u8)]
enum IpVersion {
    V4 = 4u8,
    V6 = 6u8,
}

impl TryFrom<u8> for IpVersion {
    type Error = anyhow::Error;

    fn try_from(ip_type_byte: u8) -> anyhow::Result<Self> {
        if ip_type_byte == IpVersion::V4 as u8 {
            Ok(IpVersion::V4)
        } else if ip_type_byte == IpVersion::V6 as u8 {
            Ok(IpVersion::V6)
        } else {
            bail!("invalid IP version byte: expected `4` or `6`, got `{ip_type_byte}`");
        }
    }
}

impl Serializable for IpAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        match self {
            IpAddr::V4(ip_v4) => {
                buf.push(IpVersion::V4 as u8);
                buf.extend_from_slice(&ip_v4.octets());
            }
            IpAddr::V6(ip_v6) => {
                buf.push(IpVersion::V6 as u8);
                buf.extend_from_slice(&ip_v6.octets());
            }
        }
    }

    fn serialized_len(&self) -> usize {
        1 + match self {
            IpAddr::V4(_) => 4,
            IpAddr::V6(_) => 16,
        }
    }
}

impl Deserializable for IpAddr {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_version_byte: [u8; 1] = Deserializable::deserialize(buf)?;
        let ip_version = IpVersion::try_from(ip_version_byte[0])?;
        match ip_version {
            IpVersion::V4 => {
                let bytes: [u8; 4] = Deserializable::deserialize(buf)?;
                Ok(Ipv4Addr::from(bytes).into())
            }
            IpVersion::V6 => {
                let bytes: [u8; 16] = Deserializable::deserialize(buf)?;
                Ok(Ipv6Addr::from(bytes).into())
            }
        }
    }
}

impl Serializable for str {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.len() as u16).serialize(buf);
        buf.extend(self.as_bytes())
    }

    fn serialized_len(&self) -> usize {
        2 + self.len()
    }
}

impl Serializable for String {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_str().serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.as_str().serialized_len()
    }
}

impl Serializable for ByteString {
    fn serialize(&self, buf: &mut Vec<u8>) {
        let s: &str = self;
        s.serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        let s: &str = self;
        s.serialized_len()
    }
}

fn deserialize_str<'a>(buf: &mut &'a [u8]) -> anyhow::Result<&'a str> {
    let len: usize = u16::deserialize(buf)? as usize;
    let str_bytes = buf.get(..len).with_context(|| {
        format!(
            "failed to deserialize string, buffer too short (str_len={len}, buf_len={})",
            buf.len()
        )
    })?;
    let s = std::str::from_utf8(str_bytes)?;
    buf.consume(len);
    Ok(s)
}

impl Deserializable for String {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        Ok(deserialize_str(buf)?.to_owned())
    }
}

impl Deserializable for ByteString {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        // Compact on store: copy out of the (transient) decompressed buffer
        // rather than retaining a slice of it for the value's whole lifetime.
        Ok(ByteString::from(deserialize_str(buf)?.to_owned()))
    }
}

impl Deserializable for Arc<str> {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        Ok(Arc::from(deserialize_str(buf)?))
    }
}

impl<const N: usize> Serializable for [u8; N] {
    fn serialize(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self[..]);
    }
    fn serialized_len(&self) -> usize {
        N
    }
}

impl<const N: usize> Deserializable for [u8; N] {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        if buf.len() < N {
            bail!("buffer too short");
        }
        let val_bytes: [u8; N] = buf[..N].try_into()?;
        buf.consume(N);
        Ok(val_bytes)
    }
}

impl Serializable for SocketAddr {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.ip().serialize(buf);
        self.port().serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.ip().serialized_len() + self.port().serialized_len()
    }
}

impl Deserializable for SocketAddr {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let ip_addr = IpAddr::deserialize(buf)?;
        let port = u16::deserialize(buf)?;
        Ok(SocketAddr::new(ip_addr, port))
    }
}

impl Serializable for MemberId {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.id.serialize(buf);
        self.incarnation.serialize(buf);
        self.gossip_addr.serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.id.serialized_len()
            + self.incarnation.serialized_len()
            + self.gossip_addr.serialized_len()
    }
}

impl Serializable for Arc<str> {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.as_ref().serialize(buf)
    }

    fn serialized_len(&self) -> usize {
        self.as_ref().serialized_len()
    }
}

impl Deserializable for MemberId {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let id: Arc<str> = Arc::<str>::deserialize(buf)?;
        let incarnation = u64::deserialize(buf)?;
        let gossip_addr = SocketAddr::deserialize(buf)?;
        Ok(Self {
            id,
            incarnation,
            gossip_addr,
        })
    }
}

impl Serializable for Heartbeat {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.0.serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.0.serialized_len()
    }
}

impl Deserializable for Heartbeat {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = u64::deserialize(buf)?;
        Ok(Self(heartbeat))
    }
}

#[cfg(test)]
#[track_caller]
pub fn test_serdeser_aux<T: Serializable + Deserializable + PartialEq + std::fmt::Debug>(
    obj: &T,
    num_bytes: usize,
) {
    let mut buf = Vec::new();
    obj.serialize(&mut buf);
    assert_eq!(buf.len(), obj.serialized_len());
    assert_eq!(buf.len(), num_bytes);
    let obj_serdeser = T::deserialize(&mut &buf[..]).unwrap();
    assert_eq!(obj, &obj_serdeser);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_bool() {
        test_serdeser_aux(&true, 1);
    }

    #[test]
    fn test_serialize_member_id() {
        test_serdeser_aux(
            &MemberId::new(
                "member-id".to_string(),
                1,
                "127.0.0.1:7280".parse().unwrap(),
            ),
            // 2 (len) + 9 ("member-id") + 8 (incarnation) + 5 (ipv4) + 2 (port)
            26,
        );
    }

    #[test]
    fn test_serialize_heartbeat() {
        test_serdeser_aux(&Heartbeat(1), 8);
    }

    #[test]
    fn test_serialize_ip() {
        let ipv4 = IpAddr::from(Ipv4Addr::new(127, 1, 3, 9));
        test_serdeser_aux(&ipv4, 5);

        let ipv6 = IpAddr::from(Ipv6Addr::from([
            0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        ]));
        test_serdeser_aux(&ipv6, 17);
    }
}
