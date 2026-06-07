use std::collections::BTreeMap;

use crate::serialize::*;
use crate::{Heartbeat, MemberId, Version};

/// A per-member digest entry: the member's heartbeat and how many of its key-values
/// the sender holds (`max_version`, a contiguous prefix count).
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct MemberDigest {
    pub(crate) heartbeat: Heartbeat,
    pub(crate) max_version: Version,
}

impl Serializable for MemberDigest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        self.heartbeat.serialize(buf);
        self.max_version.serialize(buf);
    }

    fn serialized_len(&self) -> usize {
        self.heartbeat.serialized_len() + self.max_version.serialized_len()
    }
}

impl Deserializable for MemberDigest {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let heartbeat = Heartbeat::deserialize(buf)?;
        let max_version = Version::deserialize(buf)?;
        Ok(MemberDigest {
            heartbeat,
            max_version,
        })
    }
}

/// A digest summarizes, for each member the sender knows about, its heartbeat and
/// the version up to which the sender holds that member's key-values. It is both
/// the membership view and the version vector driving reconciliation.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct Digest {
    pub(crate) member_digests: BTreeMap<MemberId, MemberDigest>,
}

impl Digest {
    #[cfg(test)]
    pub(crate) fn add_member(
        &mut self,
        member: MemberId,
        heartbeat: Heartbeat,
        max_version: Version,
    ) {
        let member_digest = MemberDigest {
            heartbeat,
            max_version,
        };
        self.member_digests.insert(member, member_digest);
    }
}

impl Serializable for Digest {
    fn serialize(&self, buf: &mut Vec<u8>) {
        (self.member_digests.len() as u16).serialize(buf);
        for (member_id, member_digest) in &self.member_digests {
            member_id.serialize(buf);
            member_digest.serialize(buf);
        }
    }
    fn serialized_len(&self) -> usize {
        let mut len = (self.member_digests.len() as u16).serialized_len();
        for (member_id, member_digest) in &self.member_digests {
            len += member_id.serialized_len();
            len += member_digest.serialized_len();
        }
        len
    }
}

impl Deserializable for Digest {
    fn deserialize(buf: &mut &[u8]) -> anyhow::Result<Self> {
        let num_members = u16::deserialize(buf)?;
        let mut member_digests: BTreeMap<MemberId, MemberDigest> = Default::default();
        for _ in 0..num_members {
            let member_id = MemberId::deserialize(buf)?;
            let member_digest = MemberDigest::deserialize(buf)?;
            member_digests.insert(member_id, member_digest);
        }
        Ok(Digest { member_digests })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialize::test_serdeser_aux;

    #[test]
    fn test_member_digest_serialization() {
        let member_digest = MemberDigest {
            heartbeat: Heartbeat(100u64),
            max_version: 3,
        };
        // 8 (heartbeat) + 8 (max_version)
        test_serdeser_aux(&member_digest, 16);
    }

    #[test]
    fn test_digest_serialization() {
        let mut digest = Digest::default();
        let member1 = MemberId::for_local_test(10_001);
        let member2 = MemberId::for_local_test(10_002);
        digest.add_member(member1, Heartbeat(101), 11);
        digest.add_member(member2, Heartbeat(102), 12);
        let mut buf = Vec::new();
        digest.serialize(&mut buf);
        assert_eq!(buf.len(), digest.serialized_len());
        let digest_deser = Digest::deserialize(&mut &buf[..]).unwrap();
        assert_eq!(digest, digest_deser);
    }
}
