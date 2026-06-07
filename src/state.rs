use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::fmt::{Debug, Formatter};
use std::net::SocketAddr;

use bytestring::ByteString;
use itertools::Itertools;
use rand::Rng;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::delta::{Delta, DeltaSerializer, MemberDelta};
use crate::digest::{Digest, MemberDigest};
use crate::{Heartbeat, MemberId, Version, VersionedValue};

/// A member's replicated state: a frozen set of key-values (versioned `1..N`) plus
/// a monotonically increasing heartbeat.
///
/// For the local ("self") member the key-values are complete. For a remote member we
/// hold the contiguous prefix `1..=max_version` received so far.
#[derive(Clone, Serialize, Deserialize)]
pub struct MemberState {
    member_id: MemberId,
    heartbeat: Heartbeat,
    key_values: BTreeMap<ByteString, VersionedValue>,
    /// Number of key-values held — and, because versions are dense and
    /// immutable, exactly the highest version held: we have `1..=max_version`.
    max_version: Version,
}

impl Debug for MemberState {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("MemberState")
            .field("heartbeat", &self.heartbeat)
            .field("key_values", &self.key_values)
            .field("max_version", &self.max_version)
            .finish()
    }
}

impl MemberState {
    fn new(member_id: MemberId) -> MemberState {
        MemberState {
            member_id,
            heartbeat: Heartbeat(0),
            key_values: BTreeMap::new(),
            max_version: 0,
        }
    }

    /// Builds the local member's state from its immutable key-values, assigning
    /// dense versions `1..N` in (sorted) key order.
    pub(crate) fn with_key_values(
        member_id: MemberId,
        key_values: Vec<(String, String)>,
    ) -> MemberState {
        // De-duplicate (last value wins) and sort by key.
        let sorted: BTreeMap<ByteString, ByteString> = key_values
            .into_iter()
            .map(|(key, value)| (ByteString::from(key), ByteString::from(value)))
            .collect();
        let mut member_state = MemberState::new(member_id);
        for (version, (key, value)) in (1..).zip(sorted) {
            member_state
                .key_values
                .insert(key, VersionedValue::new(value, version));
            member_state.max_version = version;
        }
        member_state
    }

    pub fn member_id(&self) -> &MemberId {
        &self.member_id
    }

    pub fn heartbeat(&self) -> Heartbeat {
        self.heartbeat
    }

    pub fn max_version(&self) -> Version {
        self.max_version
    }

    /// Returns the value associated to `key`, if any.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.key_values
            .get(key)
            .map(|versioned_value| versioned_value.value.as_ref())
    }

    /// Iterates over all `(key, value)` pairs.
    pub fn key_values(&self) -> impl Iterator<Item = (&str, &str)> {
        self.key_values
            .iter()
            .map(|(key, versioned_value)| (key.as_ref(), versioned_value.value.as_ref()))
    }

    pub fn num_key_values(&self) -> usize {
        self.key_values.len()
    }

    pub(crate) fn inc_heartbeat(&mut self) {
        self.heartbeat.inc();
    }

    /// Attempts to set the heartbeat of another member. The first heartbeat seen is
    /// recorded but not treated as an update. Returns `true` only on an update.
    pub(crate) fn try_set_heartbeat(&mut self, heartbeat: Heartbeat) -> bool {
        if self.heartbeat.0 == 0 {
            self.heartbeat = heartbeat;
            return false;
        }
        if heartbeat > self.heartbeat {
            self.heartbeat = heartbeat;
            true
        } else {
            false
        }
    }

    fn digest(&self) -> MemberDigest {
        MemberDigest {
            heartbeat: self.heartbeat,
            max_version: self.max_version,
        }
    }

    /// Iterates over key-values whose version is strictly greater than
    /// `floor_version`.
    fn stale_key_values(
        &self,
        floor_version: Version,
    ) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.key_values
            .iter()
            .filter(move |(_, versioned_value)| versioned_value.version > floor_version)
            .map(|(key, versioned_value)| (key.as_ref(), versioned_value))
    }

    /// Applies a member delta, extending our prefix. Returns `true` if any new
    /// key-value was applied.
    fn apply_delta(&mut self, member_delta: MemberDelta) -> bool {
        if member_delta.from_version_excluded > self.max_version {
            // The delta starts past our prefix: applying it would leave a gap.
            // This can happen with out-of-order/duplicate datagrams; ignore it.
            return false;
        }
        let mut applied = false;
        for key_value in member_delta.key_values {
            if key_value.version <= self.max_version {
                // Already held.
                continue;
            }
            self.key_values.insert(
                key_value.key,
                VersionedValue::new(key_value.value, key_value.version),
            );
            self.max_version = key_value.version;
            applied = true;
        }
        applied
    }
}

pub(crate) struct ClusterState {
    member_states: BTreeMap<MemberId, MemberState>,
    seed_addrs: watch::Receiver<HashSet<SocketAddr>>,
}

impl Debug for ClusterState {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ClusterState")
            .field("seed_addrs", &self.seed_addrs.borrow())
            .field("member_states", &self.member_states)
            .finish()
    }
}

impl ClusterState {
    pub fn with_seed_addrs(seed_addrs: watch::Receiver<HashSet<SocketAddr>>) -> ClusterState {
        ClusterState {
            member_states: BTreeMap::new(),
            seed_addrs,
        }
    }

    pub fn member_states(&self) -> &BTreeMap<MemberId, MemberState> {
        &self.member_states
    }

    pub fn member_state(&self, member_id: &MemberId) -> Option<&MemberState> {
        self.member_states.get(member_id)
    }

    pub fn member_state_mut_or_init(&mut self, member_id: &MemberId) -> &mut MemberState {
        self.member_states
            .entry(member_id.clone())
            .or_insert_with(|| MemberState::new(member_id.clone()))
    }

    /// Inserts the local member's pre-built state.
    pub fn insert_self_member_state(&mut self, member_state: MemberState) {
        self.member_states
            .insert(member_state.member_id.clone(), member_state);
    }

    pub fn members(&self) -> impl Iterator<Item = &MemberId> {
        self.member_states.keys()
    }

    pub fn seed_addrs(&self) -> HashSet<SocketAddr> {
        self.seed_addrs.borrow().clone()
    }

    pub fn remove_member(&mut self, member_id: &MemberId) {
        self.member_states.remove(member_id);
    }

    pub fn apply_delta(&mut self, delta: Delta) {
        for member_delta in delta.member_deltas {
            if let Some(member_state) = self.member_states.get_mut(&member_delta.member_id) {
                member_state.apply_delta(member_delta);
            }
            // Unknown members are learned via the digest, not the delta; ignore.
        }
    }

    pub fn compute_digest(&self, exclude: &HashSet<&MemberId>) -> Digest {
        Digest {
            member_digests: self
                .member_states
                .iter()
                .filter(|(member_id, _)| !exclude.contains(member_id))
                .map(|(member_id, member_state)| (member_id.clone(), member_state.digest()))
                .collect(),
        }
    }

    /// Scuttlebutt reconciliation with scuttle-depth ordering, bounded by `mtu`.
    /// Members in `exclude` (dead) are not shared.
    pub fn compute_partial_delta_respecting_mtu(
        &self,
        digest: &Digest,
        mtu: usize,
        exclude: &HashSet<&MemberId>,
    ) -> Delta {
        let mut stale_members = SortedStaleMembers::default();
        for (member_id, member_state) in &self.member_states {
            if exclude.contains(member_id) {
                continue;
            }
            let digest_max_version = digest
                .member_digests
                .get(member_id)
                .map(|member_digest| member_digest.max_version)
                .unwrap_or(0);
            if member_state.max_version <= digest_max_version {
                // We have nothing fresher to offer.
                continue;
            }
            stale_members.offer(member_id, member_state, digest_max_version);
        }
        let mut delta_serializer = DeltaSerializer::with_mtu(mtu);
        for stale_member in stale_members.into_iter() {
            if !delta_serializer.try_add_member(
                stale_member.member_id.clone(),
                stale_member.from_version_excluded,
            ) {
                break;
            }
            for (key, versioned_value) in stale_member.stale_key_values() {
                if !delta_serializer.try_add_kv(
                    key,
                    &versioned_value.value,
                    versioned_value.version,
                ) {
                    return delta_serializer.finish();
                }
            }
        }
        delta_serializer.finish()
    }
}

/// Score deciding which member to gossip first. Unknown members go first (lowest
/// `max_version` among them first); then known members with more stale key-values.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
struct Staleness {
    is_unknown: bool,
    max_version: Version,
    num_stale_key_values: usize,
}

impl Ord for Staleness {
    fn cmp(&self, other: &Self) -> Ordering {
        self.is_unknown.cmp(&other.is_unknown).then_with(|| {
            if self.is_unknown {
                self.max_version.cmp(&other.max_version).reverse()
            } else {
                self.num_stale_key_values.cmp(&other.num_stale_key_values)
            }
        })
    }
}

impl PartialOrd for Staleness {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
struct SortedStaleMembers<'a> {
    stale_members: BTreeMap<Staleness, Vec<StaleMember<'a>>>,
}

fn staleness_score(member_state: &MemberState, floor_version: Version) -> Option<Staleness> {
    if member_state.max_version() <= floor_version {
        return None;
    }
    let is_unknown = floor_version == 0;
    let num_stale_key_values = if is_unknown {
        member_state.num_key_values()
    } else {
        member_state.stale_key_values(floor_version).count()
    };
    Some(Staleness {
        is_unknown,
        max_version: member_state.max_version(),
        num_stale_key_values,
    })
}

impl<'a> SortedStaleMembers<'a> {
    fn offer(
        &mut self,
        member_id: &'a MemberId,
        member_state: &'a MemberState,
        from_version_excluded: Version,
    ) {
        let Some(staleness) = staleness_score(member_state, from_version_excluded) else {
            return;
        };
        let stale_member = StaleMember {
            member_id,
            member_state,
            from_version_excluded,
        };
        self.stale_members
            .entry(staleness)
            .or_default()
            .push(stale_member);
    }

    fn into_iter(self) -> impl Iterator<Item = StaleMember<'a>> {
        let mut rng = random_generator();
        self.stale_members
            .into_values()
            .rev()
            .flat_map(move |mut stale_members| {
                stale_members.shuffle(&mut rng);
                stale_members.into_iter()
            })
    }
}

struct StaleMember<'a> {
    member_id: &'a MemberId,
    member_state: &'a MemberState,
    from_version_excluded: Version,
}

impl StaleMember<'_> {
    /// Stale key-values in increasing version order.
    fn stale_key_values(&self) -> impl Iterator<Item = (&str, &VersionedValue)> {
        self.member_state
            .stale_key_values(self.from_version_excluded)
            .sorted_unstable_by_key(|(_, versioned_value)| versioned_value.version)
    }
}

#[cfg(not(test))]
fn random_generator() -> impl Rng {
    rand::rng()
}

// Deterministic generator in tests.
#[cfg(test)]
fn random_generator() -> impl Rng {
    use rand::SeedableRng;
    use rand::rngs::StdRng;
    StdRng::seed_from_u64(9u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cluster_state() -> ClusterState {
        let (_tx, rx) = watch::channel(HashSet::new());
        ClusterState::with_seed_addrs(rx)
    }

    #[test]
    fn test_with_key_values_assigns_dense_versions() {
        let member = MemberState::with_key_values(
            MemberId::for_local_test(10_001),
            vec![
                ("key_b".to_string(), "2".to_string()),
                ("key_a".to_string(), "1".to_string()),
                ("key_c".to_string(), "3".to_string()),
            ],
        );
        assert_eq!(member.max_version(), 3);
        // Versions assigned in sorted key order.
        assert_eq!(member.key_values.get("key_a").unwrap().version, 1);
        assert_eq!(member.key_values.get("key_b").unwrap().version, 2);
        assert_eq!(member.key_values.get("key_c").unwrap().version, 3);
        assert_eq!(member.get("key_a"), Some("1"));
    }

    #[test]
    fn test_apply_delta_extends_prefix() {
        let member1 = MemberId::for_local_test(10_001);
        let mut cluster_state = test_cluster_state();
        // Learn member1 exists (as the digest would).
        cluster_state.member_state_mut_or_init(&member1);

        let mut delta = Delta::default();
        delta.add_member(member1.clone(), 0);
        delta.add_kv(&member1, "key_a", "1", 1);
        delta.add_kv(&member1, "key_b", "2", 2);
        cluster_state.apply_delta(delta);

        let member_state = cluster_state.member_state(&member1).unwrap();
        assert_eq!(member_state.max_version(), 2);
        assert_eq!(member_state.get("key_a"), Some("1"));
        assert_eq!(member_state.get("key_b"), Some("2"));

        // A stale delta (already-held versions) is a no-op.
        let mut stale = Delta::default();
        stale.add_member(member1.clone(), 0);
        stale.add_kv(&member1, "key_a", "1", 1);
        cluster_state.apply_delta(stale);
        assert_eq!(
            cluster_state.member_state(&member1).unwrap().max_version(),
            2
        );
    }

    #[test]
    fn test_compute_delta_is_contiguous_prefix() {
        let member1 = MemberId::for_local_test(10_001);
        let mut cluster_state = test_cluster_state();
        let self_state = MemberState::with_key_values(
            member1.clone(),
            vec![
                ("key_a".to_string(), "1".to_string()),
                ("key_b".to_string(), "2".to_string()),
                ("key_c".to_string(), "3".to_string()),
            ],
        );
        cluster_state.insert_self_member_state(self_state);

        // A peer that has nothing for member1 (max_version 0) should get all 3.
        let digest = Digest::default();
        let exclude = HashSet::new();
        let delta =
            cluster_state.compute_partial_delta_respecting_mtu(&digest, usize::MAX, &exclude);
        let member_delta = delta.get(&member1).unwrap();
        assert_eq!(member_delta.from_version_excluded, 0);
        assert_eq!(member_delta.key_values.len(), 3);
        // Increasing version order.
        assert_eq!(member_delta.key_values[0].version, 1);
        assert_eq!(member_delta.key_values[2].version, 3);
    }
}
