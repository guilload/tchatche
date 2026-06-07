use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tracing::debug;

use crate::MemberId;

/// A phi-accrual failure detector, ported from Akka's `PhiAccrualFailureDetector`.
///
/// Unlike a naive `elapsed / mean` estimator, this tracks the mean *and* standard
/// deviation of heartbeat inter-arrival intervals and feeds them through a
/// logistic approximation of the Gaussian CDF, so phi reflects how *surprising*
/// the current silence is given the observed jitter.
///
/// This struct multiplexes one [`PhiAccrual`] per monitored member and tracks the
/// live/dead sets and garbage collection on top.
pub struct FailureDetector {
    member_samples: HashMap<MemberId, PhiAccrual>,
    config: FailureDetectorConfig,
    live_members: HashSet<MemberId>,
    dead_members: HashMap<MemberId, Instant>,
}

impl FailureDetector {
    pub fn new(config: FailureDetectorConfig) -> Self {
        Self {
            member_samples: HashMap::new(),
            config,
            live_members: HashSet::new(),
            dead_members: HashMap::new(),
        }
    }

    /// Records a heartbeat arrival for a member.
    pub fn report_heartbeat(&mut self, member_id: &MemberId) {
        debug!(member_id=%member_id.id, "reporting member heartbeat");
        let config = &self.config;
        self.member_samples
            .entry(member_id.clone())
            .or_insert_with(|| PhiAccrual::new(config))
            .heartbeat(Instant::now());
    }

    /// Marks the member live or dead based on its current phi value. A member with no
    /// samples yet is treated as not (yet) available.
    pub fn update_member_liveness(&mut self, member_id: &MemberId) {
        let now = Instant::now();
        let is_alive = self
            .member_samples
            .get(member_id)
            .map(|sample| sample.is_available(now))
            .unwrap_or(false);
        debug!(member_id=%member_id.id, is_alive=is_alive, "computing member liveness");
        if is_alive {
            self.live_members.insert(member_id.clone());
            self.dead_members.remove(member_id);
        } else {
            self.live_members.remove(member_id);
            self.dead_members
                .entry(member_id.clone())
                .or_insert_with(Instant::now);
            // We deliberately do NOT reset the sample's history here: per Akka,
            // the (huge) post-failure interval is simply not recorded — see
            // `PhiAccrual::heartbeat` — so a revival self-corrects without a reset.
        }
    }

    /// Removes and returns the members dead longer than `quarantine_period`.
    pub fn garbage_collect(&mut self, quarantine_period: Duration) -> Vec<MemberId> {
        let now = Instant::now();
        let garbage_collected_members: Vec<MemberId> = self
            .dead_members
            .iter()
            .filter(|(_, time_of_death)| now >= **time_of_death + quarantine_period)
            .map(|(member_id, _)| member_id.clone())
            .collect();
        for member_id in &garbage_collected_members {
            self.dead_members.remove(member_id);
            self.member_samples.remove(member_id);
        }
        garbage_collected_members
    }

    pub fn live_members(&self) -> impl Iterator<Item = &MemberId> {
        self.live_members.iter()
    }

    pub fn dead_members(&self) -> impl Iterator<Item = &MemberId> {
        self.dead_members.keys()
    }
}

/// Configuration for the phi-accrual failure detector. Field meanings and
/// defaults follow Akka's cluster failure detector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureDetectorConfig {
    /// Phi threshold above which a member is flagged faulty.
    pub phi_threshold: f64,
    /// Number of most recent inter-arrival intervals retained.
    pub max_sample_size: usize,
    /// Lower bound on the standard deviation used in the phi calculation,
    /// avoiding over-sensitivity when intervals are very regular.
    pub min_std_deviation: Duration,
    /// Margin added to the mean, accounting for sporadic GC/network pauses.
    pub acceptable_heartbeat_pause: Duration,
    /// Bootstrap estimate of the heartbeat interval, used to seed history before
    /// real samples arrive.
    pub first_heartbeat_estimate: Duration,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 8.0,
            max_sample_size: 1_000,
            min_std_deviation: Duration::from_millis(100),
            acceptable_heartbeat_pause: Duration::from_secs(3),
            first_heartbeat_estimate: Duration::from_secs(1),
        }
    }
}

/// Per-member phi-accrual state.
struct PhiAccrual {
    threshold: f64,
    min_std_deviation_millis: f64,
    acceptable_heartbeat_pause_millis: f64,
    first_heartbeat_estimate_millis: f64,
    history: HeartbeatHistory,
    last_timestamp: Option<Instant>,
}

impl PhiAccrual {
    fn new(config: &FailureDetectorConfig) -> Self {
        PhiAccrual {
            threshold: config.phi_threshold,
            min_std_deviation_millis: config.min_std_deviation.as_secs_f64() * 1_000.0,
            acceptable_heartbeat_pause_millis: config.acceptable_heartbeat_pause.as_secs_f64()
                * 1_000.0,
            first_heartbeat_estimate_millis: config.first_heartbeat_estimate.as_secs_f64()
                * 1_000.0,
            history: HeartbeatHistory::new(config.max_sample_size),
            last_timestamp: None,
        }
    }

    /// Seeds the history with two entries (`mean ± mean/4`) so phi is meaningful
    /// before real intervals are observed.
    fn seed_first_heartbeat(&mut self) {
        let mean = self.first_heartbeat_estimate_millis;
        let std_deviation = mean / 4.0;
        let mut history = HeartbeatHistory::new(self.history.max_sample_size);
        history.push(mean - std_deviation);
        history.push(mean + std_deviation);
        self.history = history;
    }

    fn ensure_valid_std_deviation(&self, std_deviation: f64) -> f64 {
        std_deviation.max(self.min_std_deviation_millis)
    }

    fn phi(&self, now: Instant) -> f64 {
        let Some(last_timestamp) = self.last_timestamp else {
            // No heartbeat yet: treated as healthy (phi = 0).
            return 0.0;
        };
        let time_diff_millis = now.duration_since(last_timestamp).as_secs_f64() * 1_000.0;
        let mean = self.history.mean() + self.acceptable_heartbeat_pause_millis;
        let std_deviation = self.ensure_valid_std_deviation(self.history.std_deviation());
        phi(time_diff_millis, mean, std_deviation)
    }

    fn is_available(&self, now: Instant) -> bool {
        self.phi(now) < self.threshold
    }

    fn heartbeat(&mut self, now: Instant) {
        match self.last_timestamp {
            None => self.seed_first_heartbeat(),
            Some(last_timestamp) => {
                // Don't record the first interval after a failure: a long pause
                // would skew the statistics. Only record while still available.
                if self.is_available(now) {
                    let interval_millis =
                        now.duration_since(last_timestamp).as_secs_f64() * 1_000.0;
                    self.history.push(interval_millis);
                }
            }
        }
        self.last_timestamp = Some(now);
    }
}

/// The logistic approximation of the Gaussian CDF used to compute phi.
fn phi(time_diff_millis: f64, mean: f64, std_deviation: f64) -> f64 {
    let y = (time_diff_millis - mean) / std_deviation;
    let e = (-y * (1.5976 + 0.070566 * y * y)).exp();
    if time_diff_millis > mean {
        -(e / (1.0 + e)).log10()
    } else {
        -(1.0 - 1.0 / (1.0 + e)).log10()
    }
}

/// A bounded window of inter-arrival intervals (in milliseconds) maintaining a
/// running sum and sum-of-squares, so mean and variance are O(1).
struct HeartbeatHistory {
    max_sample_size: usize,
    intervals: VecDeque<f64>,
    interval_sum: f64,
    squared_interval_sum: f64,
}

impl HeartbeatHistory {
    fn new(max_sample_size: usize) -> Self {
        HeartbeatHistory {
            max_sample_size,
            intervals: VecDeque::with_capacity(max_sample_size),
            interval_sum: 0.0,
            squared_interval_sum: 0.0,
        }
    }

    fn mean(&self) -> f64 {
        self.interval_sum / self.intervals.len() as f64
    }

    fn variance(&self) -> f64 {
        let mean = self.mean();
        (self.squared_interval_sum / self.intervals.len() as f64) - (mean * mean)
    }

    fn std_deviation(&self) -> f64 {
        self.variance().max(0.0).sqrt()
    }

    fn push(&mut self, interval: f64) {
        if self.intervals.len() >= self.max_sample_size {
            if let Some(oldest) = self.intervals.pop_front() {
                self.interval_sum -= oldest;
                self.squared_interval_sum -= oldest * oldest;
            }
        }
        self.intervals.push_back(interval);
        self.interval_sum += interval;
        self.squared_interval_sum += interval * interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_phi_accrual_seeds_after_one_heartbeat() {
        // Akka seeds the history on the first heartbeat, so a single heartbeat is
        // already enough to consider the member available (phi ~ 0).
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let member = MemberId::for_local_test(10_001);
        failure_detector.report_heartbeat(&member);
        failure_detector.update_member_liveness(&member);
        assert_eq!(failure_detector.live_members().count(), 1);
        assert_eq!(failure_detector.dead_members().count(), 0);
    }

    #[tokio::test]
    async fn test_phi_accrual_lifecycle() {
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let member = MemberId::for_local_test(10_001);

        // Regular ~1s heartbeats keep the member alive.
        for _ in 0..200 {
            tokio::time::advance(Duration::from_secs(1)).await;
            failure_detector.report_heartbeat(&member);
        }
        failure_detector.update_member_liveness(&member);
        assert_eq!(failure_detector.live_members().count(), 1);
        assert!(
            failure_detector
                .garbage_collect(Duration::from_secs(10))
                .is_empty()
        );

        // Silence: the member should be flagged dead.
        tokio::time::advance(Duration::from_secs(30)).await;
        failure_detector.update_member_liveness(&member);
        assert_eq!(failure_detector.dead_members().count(), 1);
        assert_eq!(failure_detector.live_members().count(), 0);
        assert!(
            failure_detector
                .garbage_collect(Duration::from_secs(10))
                .is_empty()
        );

        // After the quarantine period elapses, the member is collectible.
        tokio::time::advance(Duration::from_secs(20)).await;
        let collected = failure_detector.garbage_collect(Duration::from_secs(10));
        assert_eq!(collected, vec![member]);
    }

    #[tokio::test]
    async fn test_phi_accrual_revival_does_not_record_pause() {
        tokio::time::pause();
        let mut failure_detector = FailureDetector::new(FailureDetectorConfig::default());
        let member = MemberId::for_local_test(10_001);
        for _ in 0..200 {
            tokio::time::advance(Duration::from_secs(1)).await;
            failure_detector.report_heartbeat(&member);
        }
        let mean_before = failure_detector.member_samples[&member].history.mean();

        // Long pause, then resume.
        tokio::time::advance(Duration::from_secs(60)).await;
        failure_detector.report_heartbeat(&member); // post-failure interval, not recorded
        tokio::time::advance(Duration::from_secs(1)).await;
        failure_detector.report_heartbeat(&member);
        failure_detector.update_member_liveness(&member);

        // The 60s pause must not have polluted the mean.
        let mean_after = failure_detector.member_samples[&member].history.mean();
        assert!(mean_after < mean_before + 1.0);
        assert_eq!(failure_detector.live_members().count(), 1);
    }
}
