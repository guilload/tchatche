use anyhow::Context as _;
use bytestring::ByteString;

use crate::serialize::*;
use crate::{MemberId, Version};

/// A single key-value carried by a delta. Keys are immutable, so a value is just
/// `(key, value, version)` — no deletion status.
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct KeyValueDelta {
    pub key: ByteString,
    pub value: ByteString,
    pub version: Version,
}

/// A delta is the payload sent to bring a peer up to date.
///
/// On the wire it is a flat stream of tagged ops — a member header
/// ([`TAG_MEMBER`]) followed by its key-values ([`TAG_KEY_VALUE`]), repeated.
/// The per-op tagging is what lets [`DeltaSerializer`] truncate a delta to an mtu
/// mid-member (sending a member's first *k* key-values now, the rest next round).
/// The stream runs to the end of the buffer — a delta is always the last field
/// of a message.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Delta {
    pub(crate) member_deltas: Vec<MemberDelta>,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct MemberDelta {
    pub member_id: MemberId,
    /// All key-values with `version > from_version_excluded` (up to the max
    /// version present here) are included. Because key-values are immutable and
    /// densely versioned, this is always a contiguous prefix continuation.
    pub from_version_excluded: Version,
    pub key_values: Vec<KeyValueDelta>,
}

impl MemberDelta {
    /// The maximum version carried for this member (0 if no key-values).
    pub fn max_version(&self) -> Version {
        self.key_values
            .last()
            .map(|kv| kv.version)
            .unwrap_or(self.from_version_excluded)
    }

    fn serialized_len(&self) -> usize {
        let header =
            1 + self.member_id.serialized_len() + self.from_version_excluded.serialized_len();
        let key_values: usize = self.key_values.iter().map(key_value_serialized_len).sum();
        header + key_values
    }
}

const TAG_MEMBER: u8 = 0;
const TAG_KEY_VALUE: u8 = 1;

fn key_value_serialized_len(kv: &KeyValueDelta) -> usize {
    1 + kv.key.serialized_len() + kv.value.serialized_len() + kv.version.serialized_len()
}

impl Serializable for Delta {
    fn serialize(&self, buf: &mut Vec<u8>) {
        for member_delta in &self.member_deltas {
            buf.push(TAG_MEMBER);
            member_delta.member_id.serialize(buf);
            member_delta.from_version_excluded.serialize(buf);
            for kv in &member_delta.key_values {
                buf.push(TAG_KEY_VALUE);
                kv.key.serialize(buf);
                kv.value.serialize(buf);
                kv.version.serialize(buf);
            }
        }
    }

    fn serialized_len(&self) -> usize {
        self.member_deltas
            .iter()
            .map(MemberDelta::serialized_len)
            .sum()
    }
}

impl Deserializable for Delta {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let mut member_deltas: Vec<MemberDelta> = Vec::new();
        // The delta runs to the end of the (message) buffer.
        while !buf.is_empty() {
            match u8::deserialize(buf)? {
                TAG_MEMBER => member_deltas.push(MemberDelta {
                    member_id: MemberId::deserialize(buf)?,
                    from_version_excluded: Version::deserialize(buf)?,
                    key_values: Vec::new(),
                }),
                TAG_KEY_VALUE => {
                    let kv = KeyValueDelta {
                        key: ByteString::deserialize(buf)?,
                        value: ByteString::deserialize(buf)?,
                        version: Version::deserialize(buf)?,
                    };
                    let member_delta = member_deltas
                        .last_mut()
                        .context("received a key-value op without a preceding member op")?;
                    anyhow::ensure!(
                        member_delta.max_version() < kv.version,
                        "kv version should be strictly increasing"
                    );
                    member_delta.key_values.push(kv);
                }
                other => anyhow::bail!("unknown delta op tag: {other}"),
            }
            // Note: we don't reject a member appearing twice. ClusterState::apply_delta
            // already tolerates duplicate/out-of-order member deltas (a too-far
            // from_version_excluded is rejected, already-held versions are skipped).
        }
        Ok(Delta { member_deltas })
    }
}

/// Builds a delta member-by-member, key-value-by-key-value, stopping as soon as
/// the next op would exceed `mtu`. Sizes are exact (no compression).
///
/// Call `try_add_member`, then `try_add_kv`, stopping when one returns `false`.
pub(crate) struct DeltaSerializer {
    mtu: usize,
    serialized_len: usize,
    member_deltas: Vec<MemberDelta>,
}

impl DeltaSerializer {
    pub fn with_mtu(mtu: usize) -> Self {
        DeltaSerializer {
            mtu,
            serialized_len: 0,
            member_deltas: Vec::new(),
        }
    }

    #[must_use]
    pub fn try_add_member(&mut self, member_id: MemberId, from_version_excluded: Version) -> bool {
        let op_len = 1 + member_id.serialized_len() + from_version_excluded.serialized_len();
        if self.serialized_len + op_len > self.mtu {
            return false;
        }
        self.serialized_len += op_len;
        self.member_deltas.push(MemberDelta {
            member_id,
            from_version_excluded,
            key_values: Vec::new(),
        });
        true
    }

    #[must_use]
    pub fn try_add_kv(&mut self, key: &str, value: &ByteString, version: Version) -> bool {
        let op_len = 1 + key.serialized_len() + value.serialized_len() + version.serialized_len();
        if self.serialized_len + op_len > self.mtu {
            return false;
        }
        self.serialized_len += op_len;
        self.member_deltas
            .last_mut()
            .expect("try_add_kv called before try_add_member")
            .key_values
            .push(KeyValueDelta {
                key: ByteString::from(key.to_owned()),
                value: value.clone(),
                version,
            });
        true
    }

    pub fn finish(self) -> Delta {
        Delta {
            member_deltas: self.member_deltas,
        }
    }
}

#[cfg(test)]
impl Delta {
    pub(crate) fn add_member(&mut self, member_id: MemberId, from_version_excluded: Version) {
        assert!(
            !self
                .member_deltas
                .iter()
                .any(|member_delta| member_delta.member_id == member_id)
        );
        self.member_deltas.push(MemberDelta {
            member_id,
            from_version_excluded,
            key_values: Vec::new(),
        });
    }

    /// `add_kv` must be called in order of increasing versions.
    pub(crate) fn add_kv(
        &mut self,
        member_id: &MemberId,
        key: &str,
        value: &str,
        version: Version,
    ) {
        assert_ne!(version, 0, "0 version for a kv is forbidden");
        let member_delta = self
            .member_deltas
            .iter_mut()
            .find(|member_delta| &member_delta.member_id == member_id)
            .unwrap();
        assert!(member_delta.max_version() < version);
        member_delta.key_values.push(KeyValueDelta {
            key: ByteString::from(key.to_owned()),
            value: ByteString::from(value.to_owned()),
            version,
        });
    }

    pub(crate) fn get(&self, member_id: &MemberId) -> Option<&MemberDelta> {
        self.member_deltas
            .iter()
            .find(|member_delta| &member_delta.member_id == member_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_serialization_roundtrip() {
        let mut delta = Delta::default();
        let member1 = MemberId::for_local_test(10_001);
        delta.add_member(member1.clone(), 0);
        delta.add_kv(&member1, "key_a", "val_a", 1);
        delta.add_kv(&member1, "key_b", "val_b", 2);

        let mut buf = Vec::new();
        delta.serialize(&mut buf);
        assert_eq!(buf.len(), delta.serialized_len());
        let deser = Delta::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(delta.member_deltas, deser.member_deltas);
    }

    #[test]
    fn test_delta_mtu_truncation() {
        let member1 = MemberId::for_local_test(10_001);
        let value = ByteString::from("x".repeat(50));
        let mut serializer = DeltaSerializer::with_mtu(500);
        assert!(serializer.try_add_member(member1.clone(), 0));
        let mut added = 0;
        for version in 1..=100 {
            if !serializer.try_add_kv("k", &value, version) {
                break;
            }
            added += 1;
        }
        let delta = serializer.finish();
        let mut buf = Vec::new();
        delta.serialize(&mut buf);
        assert_eq!(buf.len(), delta.serialized_len());
        assert!(buf.len() <= 500);
        assert!(
            (1..100).contains(&added),
            "mtu should truncate, but add some"
        );
    }
}
