use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

use bytestring::ByteString;
use serde::{Deserialize, Serialize};

/// The current version of a key. Versions are dense (`1..N`), assigned in key
/// order at startup, and immutable thereafter.
pub type Version = u64;

/// Identifies a member across the lifetime of a cluster.
///
/// A member may go down and come back up multiple times. A [`MemberId`] has three
/// components:
/// - `id`: an identifier unique across the cluster.
/// - `incarnation`: a monotonic counter bumped every time the member restarts, so
///   peers can distinguish successive lives of a member (SWIM's "incarnation";
///   chitchat called this `generation_id`). A restart yields a brand-new
///   `MemberId`, which is also how a restart bypasses the dead-member quarantine.
/// - `gossip_addr`: the socket address peers should use to gossip with the member.
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct MemberId {
    /// An identifier unique across the cluster.
    pub id: Arc<str>,
    /// A monotonic counter incremented every time the member restarts.
    pub incarnation: u64,
    /// The socket address peers should use to gossip with the member.
    pub gossip_addr: SocketAddr,
}

impl Debug for MemberId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}:{}:{}", &*self.id, self.incarnation, self.gossip_addr)
    }
}

impl MemberId {
    pub fn new(id: impl Into<Arc<str>>, incarnation: u64, gossip_addr: SocketAddr) -> Self {
        Self {
            id: id.into(),
            incarnation,
            gossip_addr,
        }
    }
}

#[cfg(any(test, feature = "testsuite"))]
impl MemberId {
    /// Returns the gossip advertise port, for assertions during tests.
    pub fn advertise_port(&self) -> u16 {
        self.gossip_addr.port()
    }

    /// Creates a new [`MemberId`] for local testing.
    pub fn for_local_test(port: u16) -> Self {
        Self::new(format!("member-{port}"), 0, ([127, 0, 0, 1], port).into())
    }
}

/// A versioned, immutable key-value pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionedValue {
    pub value: ByteString,
    pub version: Version,
}

impl VersionedValue {
    pub fn new(value: impl Into<ByteString>, version: Version) -> Self {
        Self {
            value: value.into(),
            version,
        }
    }
}

/// The current heartbeat of a member. Monotonically increasing; the only mutable
/// per-member datum (key-values are immutable).
#[derive(
    Debug, Clone, Copy, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize,
)]
pub struct Heartbeat(pub(crate) u64);

impl Heartbeat {
    pub(crate) fn inc(&mut self) {
        // Heartbeat is bumped on every gossip round/message. In real-world
        // scenarios this happens at most a few hundred times per second, so we
        // never overflow in practice.
        self.0 = self.0.checked_add(1).expect("heartbeat overflow");
    }
}

impl From<Heartbeat> for u64 {
    fn from(heartbeat: Heartbeat) -> Self {
        heartbeat.0
    }
}
