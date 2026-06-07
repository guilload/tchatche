use std::collections::HashSet;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rand::prelude::*;
use rand::rng;
use rand::rngs::SmallRng;
use tokio::net::lookup_host;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tokio::time;
use tracing::{debug, warn};

use crate::message::Message;
use crate::transport::{Socket, Transport};
use crate::{Config, MemberId, Tchatche};

/// Number of members picked for random gossip each round.
const GOSSIP_COUNT: usize = 3;

const DNS_POLLING_DURATION: Duration = Duration::from_secs(60);

/// Handle to a running tchatche server. Holding it keeps the server alive.
pub struct TchatcheHandle {
    member_id: MemberId,
    command_tx: UnboundedSender<Command>,
    tchatche: Arc<Mutex<Tchatche>>,
    join_handle: JoinHandle<anyhow::Result<()>>,
    termination_watcher: watch::Receiver<Option<Result<(), String>>>,
}

/// Launches a new tchatche server as a background Tokio task.
pub async fn spawn(
    config: Config,
    key_values: Vec<(String, String)>,
    transport: &dyn Transport,
) -> anyhow::Result<TchatcheHandle> {
    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let seed_addrs = spawn_dns_refresh_loop(&config.seed_members).await;
    let socket = transport.open(config.listen_addr).await?;
    let member_id = config.member_id.clone();

    let tchatche = Tchatche::new(config, seed_addrs, key_values);
    let tchatche_arc = Arc::new(Mutex::new(tchatche));
    let tchatche_arc_clone = tchatche_arc.clone();

    let (termination_tx, termination_watcher) = watch::channel(None);
    let join_handle = tokio::spawn(async move {
        let result = Server::new(command_rx, tchatche_arc_clone, socket)
            .run()
            .await;
        let cloned_result = result.as_ref().map(|_| ()).map_err(|err| err.to_string());
        let _ = termination_tx.send(Some(cloned_result));
        result
    });

    Ok(TchatcheHandle {
        member_id,
        command_tx,
        tchatche: tchatche_arc,
        join_handle,
        termination_watcher,
    })
}

impl TchatcheHandle {
    pub fn member_id(&self) -> &MemberId {
        &self.member_id
    }

    pub fn tchatche(&self) -> Arc<Mutex<Tchatche>> {
        self.tchatche.clone()
    }

    /// Calls a function with mutable access to the [`Tchatche`] instance.
    pub async fn with_tchatche<F, T>(&self, mut fun: F) -> T
    where
        F: FnMut(&mut Tchatche) -> T,
    {
        let mut tchatche = self.tchatche.lock().await;
        fun(&mut tchatche)
    }

    pub fn initiate_shutdown(&self) -> anyhow::Result<()> {
        self.command_tx
            .send(Command::Shutdown)
            .map_err(|_| anyhow::anyhow!("failed to initiate shutdown: command channel closed"))
    }

    /// Shuts the server down and waits for it to finish.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        let _ = self.initiate_shutdown();
        self.join_handle.await?
    }

    pub fn abort(&self) {
        self.join_handle.abort();
    }

    /// Triggers a gossip handshake with the given address.
    pub fn gossip(&self, addr: SocketAddr) -> anyhow::Result<()> {
        self.command_tx.send(Command::Gossip(addr))?;
        Ok(())
    }

    pub fn termination_watcher(&self) -> impl Future<Output = anyhow::Result<()>> + use<> {
        let mut watcher = self.termination_watcher.clone();
        async move {
            let termination_res = watcher.wait_for(|res| res.is_some()).await;
            if let Ok(result_opt) = termination_res {
                result_opt.clone().unwrap().map_err(anyhow::Error::msg)
            } else {
                Err(anyhow::anyhow!("tchatche server panicked"))
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Gossip(SocketAddr),
    Shutdown,
}

struct Server {
    command_rx: UnboundedReceiver<Command>,
    tchatche: Arc<Mutex<Tchatche>>,
    transport: Box<dyn Socket>,
    rng: SmallRng,
}

impl Server {
    fn new(
        command_rx: UnboundedReceiver<Command>,
        tchatche: Arc<Mutex<Tchatche>>,
        transport: Box<dyn Socket>,
    ) -> Self {
        let rng = SmallRng::from_rng(&mut rng());
        Self {
            command_rx,
            tchatche,
            transport,
            rng,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let gossip_interval = self.tchatche.lock().await.config.gossip_interval;
        let mut gossip_interval = time::interval(gossip_interval);
        loop {
            tokio::select! {
                result = self.transport.recv() => match result {
                    Ok((from_addr, message)) => {
                        let _ = self.handle_message(from_addr, message).await;
                    }
                    Err(err) => {
                        warn!(err=%err, "fatal recv error, stopping gossip loop");
                        return Err(err);
                    }
                },
                _ = gossip_interval.tick() => {
                    self.gossip_multiple().await;
                },
                command = self.command_rx.recv() => match command {
                    Some(Command::Gossip(addr)) => {
                        let _ = self.gossip(addr).await;
                    },
                    Some(Command::Shutdown) | None => break,
                }
            }
        }
        Ok(())
    }

    async fn handle_message(
        &mut self,
        from_addr: SocketAddr,
        message: Message,
    ) -> anyhow::Result<()> {
        let response = self.tchatche.lock().await.process_message(message);
        if let Some(message) = response {
            self.transport.send(from_addr, message).await?;
        }
        Ok(())
    }

    async fn gossip_multiple(&mut self) {
        let mut tchatche_guard = self.tchatche.lock().await;
        let self_addr = tchatche_guard.self_id().gossip_addr;

        let peer_members: HashSet<SocketAddr> = tchatche_guard
            .cluster_state()
            .members()
            .filter(|member_id| **member_id != *tchatche_guard.self_id())
            .map(|member_id| member_id.gossip_addr)
            .collect();
        let live_members: HashSet<SocketAddr> = tchatche_guard
            .live_members()
            .filter(|member_id| **member_id != *tchatche_guard.self_id())
            .map(|member_id| member_id.gossip_addr)
            .collect();
        let dead_members: HashSet<SocketAddr> = tchatche_guard
            .dead_members()
            .map(|member_id| member_id.gossip_addr)
            .collect();
        let seed_members: HashSet<SocketAddr> = tchatche_guard
            .seed_members()
            .into_iter()
            .filter(|addr| *addr != self_addr)
            .collect();

        let (selected_members, random_dead_member_opt, random_seed_member_opt) =
            select_members_for_gossip(
                &mut self.rng,
                peer_members,
                live_members,
                dead_members,
                seed_members,
            );

        tchatche_guard.update_self_heartbeat();
        drop(tchatche_guard);

        for member in selected_members {
            if let Err(error) = self.gossip(member).await {
                warn!(error=?error, member_address=%member, "failed to gossip with live member");
            }
        }
        if let Some(random_dead_member) = random_dead_member_opt {
            if let Err(error) = self.gossip(random_dead_member).await {
                debug!(error=?error, member_address=%random_dead_member, "failed to gossip with dead member");
            }
        }
        if let Some(random_seed_member) = random_seed_member_opt {
            if let Err(error) = self.gossip(random_seed_member).await {
                warn!(error=?error, member_address=%random_seed_member, "failed to gossip with seed member");
            }
        }

        self.tchatche.lock().await.update_members_liveness();
    }

    async fn gossip(&mut self, addr: SocketAddr) -> anyhow::Result<()> {
        let syn = self.tchatche.lock().await.create_syn_message();
        self.transport.send(addr, syn).await?;
        Ok(())
    }
}

fn select_members_for_gossip<R>(
    rng: &mut R,
    peer_members: HashSet<SocketAddr>,
    live_members: HashSet<SocketAddr>,
    dead_members: HashSet<SocketAddr>,
    seed_members: HashSet<SocketAddr>,
) -> (Vec<SocketAddr>, Option<SocketAddr>, Option<SocketAddr>)
where
    R: Rng + ?Sized,
{
    let live_members_count = live_members.len();
    let dead_members_count = dead_members.len();

    // On startup we don't know any live member yet, so select from all peers.
    let members: Vec<SocketAddr> = if live_members_count == 0 {
        peer_members
    } else {
        live_members
    }
    .into_iter()
    .sample(rng, GOSSIP_COUNT);

    let has_gossiped_with_a_seed_member = members.iter().any(|addr| seed_members.contains(addr));

    let random_dead_member_opt = select_dead_member_to_gossip_with(
        rng,
        &dead_members,
        live_members_count,
        dead_members_count,
    );

    // Gossip with a seed member periodically to avoid partitions.
    let random_seed_member_opt =
        if !has_gossiped_with_a_seed_member || live_members_count < seed_members.len() {
            select_seed_member_to_gossip_with(
                rng,
                &seed_members,
                live_members_count,
                dead_members_count,
            )
        } else {
            None
        };

    (members, random_dead_member_opt, random_seed_member_opt)
}

fn select_dead_member_to_gossip_with<R>(
    rng: &mut R,
    dead_members: &HashSet<SocketAddr>,
    live_members_count: usize,
    dead_members_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability = dead_members_count as f64 / (live_members_count + 1) as f64;
    if selection_probability > rng.random::<f64>() {
        return dead_members.iter().choose(rng).copied();
    }
    None
}

fn select_seed_member_to_gossip_with<R>(
    rng: &mut R,
    seed_members: &HashSet<SocketAddr>,
    live_members_count: usize,
    dead_members_count: usize,
) -> Option<SocketAddr>
where
    R: Rng + ?Sized,
{
    let selection_probability =
        seed_members.len() as f64 / (live_members_count + dead_members_count + 1) as f64;
    if live_members_count == 0 || rng.random::<f64>() <= selection_probability {
        return seed_members.iter().choose(rng).copied();
    }
    None
}

async fn spawn_dns_refresh_loop(seeds: &[String]) -> watch::Receiver<HashSet<SocketAddr>> {
    let mut seed_addrs_static: HashSet<SocketAddr> = HashSet::new();
    let mut first_round_resolution: HashSet<SocketAddr> = HashSet::new();
    let mut seeds_requiring_dns: HashSet<String> = HashSet::new();
    for seed in seeds {
        if let Ok(seed_addr) = seed.parse() {
            seed_addrs_static.insert(seed_addr);
        } else {
            seeds_requiring_dns.insert(seed.clone());
            resolve_seed_host(seed, &mut first_round_resolution).await;
        }
    }
    let initial_seed_addrs: HashSet<SocketAddr> = seed_addrs_static
        .union(&first_round_resolution)
        .copied()
        .collect();
    let (seed_addrs_tx, seed_addrs_rx) = watch::channel(initial_seed_addrs);
    if !seeds_requiring_dns.is_empty() {
        tokio::spawn(dns_refresh_loop(
            seeds_requiring_dns,
            seed_addrs_static,
            seed_addrs_tx,
        ));
    }
    seed_addrs_rx
}

async fn dns_refresh_loop(
    seeds_requiring_dns: HashSet<String>,
    seed_addrs_static: HashSet<SocketAddr>,
    seed_addrs_tx: watch::Sender<HashSet<SocketAddr>>,
) {
    let mut interval = time::interval(DNS_POLLING_DURATION);
    interval.tick().await; // first tick is immediate; skip it
    loop {
        interval.tick().await;
        let mut seed_addrs = seed_addrs_static.clone();
        for seed_host in &seeds_requiring_dns {
            resolve_seed_host(seed_host, &mut seed_addrs).await;
        }
        if seed_addrs_tx.send(seed_addrs).is_err() {
            return;
        }
    }
}

async fn resolve_seed_host(seed_host: &str, seed_addrs: &mut HashSet<SocketAddr>) {
    match lookup_host(seed_host).await {
        Ok(resolved) => {
            for seed_addr in resolved {
                if seed_addrs.insert(seed_addr) {
                    debug!(seed_host=%seed_host, seed_addr=%seed_addr, "resolved peer seed host");
                }
            }
        }
        Err(error) => {
            warn!(seed_host=%seed_host, error=?error, "failed to look up host");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use tokio_stream::StreamExt;

    use super::*;
    use crate::FailureDetectorConfig;
    use crate::transport::ChannelTransport;

    /// A snappy failure-detector config so tests detect death in well under a second.
    fn fast_failure_detector() -> FailureDetectorConfig {
        FailureDetectorConfig {
            phi_threshold: 8.0,
            max_sample_size: 1_000,
            min_std_deviation: Duration::from_millis(50),
            acceptable_heartbeat_pause: Duration::from_millis(200),
            first_heartbeat_estimate: Duration::from_millis(200),
        }
    }

    fn make_config(port: u16, seeds: &[u16]) -> Config {
        let member_id = MemberId::for_local_test(port);
        let listen_addr = member_id.gossip_addr;
        Config {
            member_id,
            cluster_id: "test-cluster".to_string(),
            gossip_interval: Duration::from_millis(50),
            listen_addr,
            seed_members: seeds
                .iter()
                .map(|port| MemberId::for_local_test(*port).gossip_addr.to_string())
                .collect(),
            failure_detector: fast_failure_detector(),
            quarantine_period: Duration::from_secs(1),
        }
    }

    async fn wait_for_live_count(handle: &TchatcheHandle, expected: usize) {
        let mut stream = handle.tchatche().lock().await.live_members_watch_stream();
        timeout(Duration::from_secs(10), async {
            loop {
                let live_members = stream.next().await.unwrap();
                if live_members.len() == expected {
                    return;
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {expected} live members"));
    }

    #[tokio::test]
    async fn test_two_members_converge_and_share_immutable_kvs() {
        let transport = ChannelTransport::with_mtu(crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let member1 = spawn(
            make_config(20_001, &[]),
            vec![("role".to_string(), "indexer".to_string())],
            &transport,
        )
        .await
        .unwrap();
        let member2 = spawn(
            make_config(20_002, &[20_001]),
            vec![("role".to_string(), "searcher".to_string())],
            &transport,
        )
        .await
        .unwrap();

        wait_for_live_count(&member2, 2).await;
        wait_for_live_count(&member1, 2).await;

        let id1 = member1.member_id().clone();
        let id2 = member2.member_id().clone();
        // Each member learned the other's immutable key-values.
        let member2_tchatche = member2.tchatche();
        let member2_guard = member2_tchatche.lock().await;
        assert_eq!(
            member2_guard.member_state(&id1).unwrap().get("role"),
            Some("indexer")
        );
        drop(member2_guard);
        let member1_tchatche = member1.tchatche();
        let member1_guard = member1_tchatche.lock().await;
        assert_eq!(
            member1_guard.member_state(&id2).unwrap().get("role"),
            Some("searcher")
        );
        drop(member1_guard);

        member1.shutdown().await.unwrap();
        member2.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_member_death_is_detected() {
        let transport = ChannelTransport::with_mtu(crate::MAX_UDP_DATAGRAM_PAYLOAD_SIZE);
        let member1 = spawn(make_config(21_001, &[21_002, 21_003]), vec![], &transport)
            .await
            .unwrap();
        let member2 = spawn(make_config(21_002, &[21_001, 21_003]), vec![], &transport)
            .await
            .unwrap();
        let member3 = spawn(make_config(21_003, &[21_001, 21_002]), vec![], &transport)
            .await
            .unwrap();

        wait_for_live_count(&member1, 3).await;

        // Kill member3; member1 should drop back to seeing 2 live members.
        member3.shutdown().await.unwrap();
        wait_for_live_count(&member1, 2).await;

        member1.shutdown().await.unwrap();
        member2.shutdown().await.unwrap();
    }
}
