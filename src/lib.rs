mod configuration;
mod delta;
mod digest;
mod failure_detector;
mod message;
pub(crate) mod serialize;
mod server;
mod state;
pub mod transport;
mod types;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;

use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{error, warn};

use crate::digest::Digest;
use crate::failure_detector::FailureDetector;
use crate::message::{Message, MessageKind};
use crate::state::ClusterState;

pub use crate::configuration::Config;
pub use crate::failure_detector::FailureDetectorConfig;
pub use crate::message::Message as TchatcheMessage;
pub use crate::serialize::{Deserializable, Serializable};
pub use crate::server::{TchatcheHandle, spawn};
pub use crate::state::MemberState;
pub use crate::types::{Heartbeat, MemberId, Version, VersionedValue};

/// Maximum UDP datagram payload size (in bytes). We send the self digest "in
/// full", so we pick a large payload that may span several IP fragments.
pub(crate) const MAX_UDP_DATAGRAM_PAYLOAD_SIZE: usize = 65_507;

pub struct Tchatche {
    config: Config,
    cluster_state: ClusterState,
    failure_detector: FailureDetector,
    /// Tracks the live set (id -> max_version) to notify the watcher on change.
    previous_live_members: HashMap<MemberId, Version>,
    live_members_watcher_tx: watch::Sender<BTreeMap<MemberId, MemberState>>,
    live_members_watcher_rx: watch::Receiver<BTreeMap<MemberId, MemberState>>,
}

impl Tchatche {
    pub(crate) fn new(
        config: Config,
        seed_addrs: watch::Receiver<HashSet<std::net::SocketAddr>>,
        key_values: Vec<(String, String)>,
    ) -> Self {
        let failure_detector = FailureDetector::new(config.failure_detector.clone());
        let (live_members_watcher_tx, live_members_watcher_rx) = watch::channel(BTreeMap::new());

        let mut cluster_state = ClusterState::with_seed_addrs(seed_addrs);
        // Build the self member's immutable key-values, then bump its heartbeat
        // once so it answers SYNs as alive.
        let mut self_member_state =
            MemberState::with_key_values(config.member_id.clone(), key_values);
        self_member_state.inc_heartbeat();
        cluster_state.insert_self_member_state(self_member_state);

        Tchatche {
            config,
            cluster_state,
            failure_detector,
            previous_live_members: HashMap::new(),
            live_members_watcher_tx,
            live_members_watcher_rx,
        }
    }

    pub(crate) fn create_syn_message(&self) -> Message {
        let dead = self.dead_members_set();
        let dead_refs: HashSet<&MemberId> = dead.iter().collect();
        let digest = self.cluster_state.compute_digest(&dead_refs);
        Message::new(self.config.cluster_id.clone(), MessageKind::Syn { digest })
    }

    pub(crate) fn process_message(&mut self, message: Message) -> Option<Message> {
        if message.cluster_id != self.config.cluster_id {
            warn!(
                our_cluster_id = %self.config.cluster_id,
                their_cluster_id = %message.cluster_id,
                "received a message for a different cluster; dropping"
            );
            return None;
        }
        // Every received message is a sign of life; bump our own heartbeat.
        self.update_self_heartbeat();

        match message.kind {
            MessageKind::Syn { digest } => {
                self.report_heartbeats_in_digest(&digest);
                let dead = self.dead_members_set();
                let dead_refs: HashSet<&MemberId> = dead.iter().collect();
                let self_digest = self.cluster_state.compute_digest(&dead_refs);
                let delta_mtu = MAX_UDP_DATAGRAM_PAYLOAD_SIZE
                    .saturating_sub(self.header_overhead() + self_digest.serialized_len());
                let delta = self
                    .cluster_state
                    .compute_partial_delta_respecting_mtu(&digest, delta_mtu, &dead_refs);
                Some(Message::new(
                    self.config.cluster_id.clone(),
                    MessageKind::SynAck {
                        digest: self_digest,
                        delta,
                    },
                ))
            }
            MessageKind::SynAck { digest, delta } => {
                self.report_heartbeats_in_digest(&digest);
                self.cluster_state.apply_delta(delta);
                let dead = self.dead_members_set();
                let dead_refs: HashSet<&MemberId> = dead.iter().collect();
                let delta_mtu =
                    MAX_UDP_DATAGRAM_PAYLOAD_SIZE.saturating_sub(self.header_overhead());
                let delta = self
                    .cluster_state
                    .compute_partial_delta_respecting_mtu(&digest, delta_mtu, &dead_refs);
                Some(Message::new(
                    self.config.cluster_id.clone(),
                    MessageKind::Ack { delta },
                ))
            }
            MessageKind::Ack { delta } => {
                self.cluster_state.apply_delta(delta);
                None
            }
            #[cfg(test)]
            MessageKind::PanicForTest => panic!("panic message received"),
        }
    }

    /// Bytes the message framing adds before the delta (magic + version +
    /// cluster_id + message tag).
    fn header_overhead(&self) -> usize {
        2 + 1 + self.config.cluster_id.serialized_len() + 1
    }

    fn report_heartbeats_in_digest(&mut self, digest: &Digest) {
        for (member_id, member_digest) in &digest.member_digests {
            self.report_heartbeat(member_id, member_digest.heartbeat);
        }
    }

    fn report_heartbeat(&mut self, member_id: &MemberId, heartbeat: Heartbeat) {
        if member_id == self.self_id() {
            return;
        }
        // Learn the member if we don't know it yet (membership discovery), then
        // feed its heartbeat to the failure detector.
        let member_state = self.cluster_state.member_state_mut_or_init(member_id);
        if member_state.try_set_heartbeat(heartbeat) {
            self.failure_detector.report_heartbeat(member_id);
        }
    }

    pub(crate) fn update_self_heartbeat(&mut self) {
        self.self_member_state_mut().inc_heartbeat();
    }

    fn self_member_state_mut(&mut self) -> &mut MemberState {
        self.cluster_state
            .member_state_mut_or_init(&self.config.member_id)
    }

    /// Updates each member's liveness, notifies the watcher on change, and garbage
    /// collects members that have been dead longer than `quarantine_period`.
    pub(crate) fn update_members_liveness(&mut self) {
        let self_id = self.config.member_id.clone();
        let member_ids: Vec<MemberId> = self
            .cluster_state
            .members()
            .filter(|member_id| **member_id != self_id)
            .cloned()
            .collect();
        for member_id in &member_ids {
            self.failure_detector.update_member_liveness(member_id);
        }

        let current_live_members: HashMap<MemberId, Version> = self
            .live_members()
            .filter_map(|member_id| {
                let member_state = self.member_state(member_id)?;
                Some((member_id.clone(), member_state.max_version()))
            })
            .collect();

        if self.previous_live_members != current_live_members {
            let live_members: BTreeMap<MemberId, MemberState> = current_live_members
                .keys()
                .filter_map(|member_id| {
                    let member_state = self.member_state(member_id)?;
                    Some((member_id.clone(), member_state.clone()))
                })
                .collect();
            self.previous_live_members = current_live_members;
            if self.live_members_watcher_tx.send(live_members).is_err() {
                error!("failed to broadcast membership change");
            }
        }

        let garbage_collected = self
            .failure_detector
            .garbage_collect(self.config.quarantine_period);
        for member_id in garbage_collected {
            if member_id != self_id {
                self.cluster_state.remove_member(&member_id);
            } else {
                error!("self member was marked dead; please report");
            }
        }
    }

    fn dead_members_set(&self) -> HashSet<MemberId> {
        self.failure_detector.dead_members().cloned().collect()
    }

    // --- Public API ---

    /// This member's identity.
    pub fn self_id(&self) -> &MemberId {
        &self.config.member_id
    }

    pub fn cluster_id(&self) -> &str {
        &self.config.cluster_id
    }

    pub fn member_state(&self, member_id: &MemberId) -> Option<&MemberState> {
        self.cluster_state.member_state(member_id)
    }

    pub fn member_states(&self) -> &BTreeMap<MemberId, MemberState> {
        self.cluster_state.member_states()
    }

    /// Members considered live, always including this member.
    pub fn live_members(&self) -> impl Iterator<Item = &MemberId> {
        once(self.self_id()).chain(self.failure_detector.live_members())
    }

    /// Members considered dead by the failure detector.
    pub fn dead_members(&self) -> impl Iterator<Item = &MemberId> {
        self.failure_detector.dead_members()
    }

    pub fn seed_members(&self) -> HashSet<std::net::SocketAddr> {
        self.cluster_state.seed_addrs()
    }

    /// A watch stream emitting the live member set on every membership change.
    pub fn live_members_watch_stream(&self) -> WatchStream<BTreeMap<MemberId, MemberState>> {
        WatchStream::new(self.live_members_watcher_rx.clone())
    }

    pub fn live_members_watcher(&self) -> watch::Receiver<BTreeMap<MemberId, MemberState>> {
        self.live_members_watcher_rx.clone()
    }

    pub(crate) fn cluster_state(&self) -> &ClusterState {
        &self.cluster_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_seeds() -> watch::Receiver<HashSet<std::net::SocketAddr>> {
        watch::channel(HashSet::new()).1
    }

    fn run_handshake(initiator: &mut Tchatche, peer: &mut Tchatche) {
        let syn = initiator.create_syn_message();
        let syn_ack = peer.process_message(syn).unwrap();
        let ack = initiator.process_message(syn_ack).unwrap();
        assert!(peer.process_message(ack).is_none());
    }

    fn assert_kv_in_sync(lhs: &MemberState, rhs: &MemberState) {
        assert_eq!(lhs.num_key_values(), rhs.num_key_values());
        for (key, value) in lhs.key_values() {
            assert_eq!(rhs.get(key), Some(value));
        }
    }

    #[test]
    fn test_handshake_syncs_immutable_kvs() {
        let mut member1 = Tchatche::new(
            Config::for_test(10_001),
            empty_seeds(),
            vec![
                ("key1a".to_string(), "1".to_string()),
                ("key2a".to_string(), "2".to_string()),
            ],
        );
        let mut member2 = Tchatche::new(
            Config::for_test(10_002),
            empty_seeds(),
            vec![
                ("key1b".to_string(), "1".to_string()),
                ("key2b".to_string(), "2".to_string()),
            ],
        );
        run_handshake(&mut member1, &mut member2);

        let id1 = member1.self_id().clone();
        let id2 = member2.self_id().clone();
        // Each member now knows both members' immutable KVs.
        assert_kv_in_sync(
            member1.member_state(&id1).unwrap(),
            member2.member_state(&id1).unwrap(),
        );
        assert_kv_in_sync(
            member1.member_state(&id2).unwrap(),
            member2.member_state(&id2).unwrap(),
        );
        assert_eq!(member2.member_state(&id1).unwrap().get("key1a"), Some("1"));
        assert_eq!(member1.member_state(&id2).unwrap().get("key2b"), Some("2"));

        // A second handshake is a no-op (already converged).
        run_handshake(&mut member1, &mut member2);
        assert_eq!(member1.member_state(&id2).unwrap().max_version(), 2);
    }

    #[test]
    fn test_wrong_cluster_id_is_dropped() {
        let mut member1 = Tchatche::new(Config::for_test(10_001), empty_seeds(), vec![]);
        let foreign = Message::new(
            "other-cluster",
            MessageKind::Syn {
                digest: Digest::default(),
            },
        );
        // A datagram from a foreign cluster is dropped (no reply).
        assert!(member1.process_message(foreign).is_none());
    }
}
