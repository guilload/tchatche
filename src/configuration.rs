use std::net::SocketAddr;
use std::time::Duration;

use crate::{FailureDetectorConfig, MemberId};

/// Configuration for a [`crate::Tchatche`] instance.
pub struct Config {
    /// This member's identity.
    pub member_id: MemberId,
    /// The cluster this member belongs to. Datagrams with a different `cluster_id`
    /// are logged and dropped.
    pub cluster_id: String,
    /// How often this member initiates a gossip round.
    pub gossip_interval: Duration,
    /// The address to bind the gossip socket to.
    pub listen_addr: SocketAddr,
    /// Seed peers, as `ip:port` or `host:port` (the latter is DNS-refreshed).
    pub seed_members: Vec<String>,
    /// Phi-accrual failure detector configuration.
    pub failure_detector: FailureDetectorConfig,
    /// How long a dead member is retained (un-shared) before being forgotten. Must
    /// exceed the cluster-wide spread in death-detection times. Doubles as the
    /// resurrection guard (see `DESIGN.md`).
    pub quarantine_period: Duration,
}

impl Config {
    #[cfg(test)]
    pub fn for_test(port: u16) -> Self {
        let member_id = MemberId::for_local_test(port);
        let listen_addr = member_id.gossip_addr;
        Self {
            member_id,
            cluster_id: "default-cluster".to_string(),
            gossip_interval: Duration::from_millis(50),
            listen_addr,
            seed_members: Vec::new(),
            failure_detector: FailureDetectorConfig::default(),
            quarantine_period: Duration::from_secs(10_000),
        }
    }
}
