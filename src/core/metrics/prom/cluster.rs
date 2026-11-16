use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering::Relaxed},
	},
	time::Duration,
};

use super::{
	family::{DynFamily, EnumFamily},
	types::{Counter, Gauge, Histogram, Metric},
	write::Writer,
};
use crate::label_set;

/// Ceiling on the cluster nodes that get a lag series of their own.
pub const NODE_CAP: usize = 16;

static BARRIER_SECONDS: [u64; 12] = [
	500_000,
	1_000_000,
	2_500_000,
	5_000_000,
	10_000_000,
	20_000_000,
	50_000_000,
	100_000_000,
	250_000_000,
	1_000_000_000,
	5_000_000_000,
	10_000_000_000,
];

static SNAPSHOT_SECONDS: [u64; 10] = [
	1_000_000_000,
	5_000_000_000,
	15_000_000_000,
	30_000_000_000,
	60_000_000_000,
	120_000_000_000,
	300_000_000_000,
	600_000_000_000,
	1_200_000_000_000,
	3_600_000_000_000,
];

label_set! {
	/// Why a request on the cluster listener was turned away before it
	/// reached the Raft core.
	pub enum Rejection("reason") {
		/// The bearer token was missing or wrong.
		Unauthorized => "unauthorized",
		/// The sender or the recipient identity did not match the membership.
		Identity => "identity",
		/// The headers or the body could not be read.
		Malformed => "malformed",
		/// The body exceeded the limit of its route.
		TooLarge => "too_large",
		/// A log entry or a member record in the body was invalid.
		Invalid => "invalid",
	}
}

/// What the Raft core reports a node to be, as the numeric value of
/// `tuwunel_cluster_role`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NodeState {
	/// Not a voter: a learner, or a node that is not in a cluster yet.
	Learner,
	/// A voter following a leader.
	Follower,
	/// A voter standing for election.
	Candidate,
	/// The leader.
	Leader,
	/// The Raft core has stopped.
	Shutdown,
}

/// Metrics of the cluster replication.
///
/// Nothing is exposed until [`Self::enable`] is called, so a server that is not
/// part of a cluster renders exactly what it did before the group existed.
pub struct Cluster {
	enabled: AtomicBool,
	role: Gauge,
	term: Gauge,
	commit_index: Gauge,
	apply_index: Gauge,
	barrier: Histogram<12>,
	barrier_timeouts: Counter,
	follower_lag: DynFamily<Gauge>,
	queue_bytes: Gauge,
	leader_changes: Counter,
	snapshot: Histogram<10>,
	quorum_ack_age: Gauge,
	reflected: Counter,
	rejected: EnumFamily<Rejection, Counter>,
}

impl NodeState {
	#[must_use]
	/// The value `tuwunel_cluster_role` takes: 0 learner, 1 follower, 2
	/// candidate, 3 leader, 4 shutdown.
	pub const fn value(self) -> i64 {
		match self {
			| Self::Learner => 0,
			| Self::Follower => 1,
			| Self::Candidate => 2,
			| Self::Leader => 3,
			| Self::Shutdown => 4,
		}
	}
}

impl Cluster {
	#[must_use]
	/// Creates the group, disabled and with every metric at zero.
	pub fn new() -> Self {
		Self {
			enabled: AtomicBool::new(false),
			role: Gauge::new(),
			term: Gauge::new(),
			commit_index: Gauge::new(),
			apply_index: Gauge::new(),
			barrier: Histogram::seconds(&BARRIER_SECONDS),
			barrier_timeouts: Counter::new(),
			follower_lag: DynFamily::new(&["node"], NODE_CAP, Gauge::new),
			queue_bytes: Gauge::new(),
			leader_changes: Counter::new(),
			snapshot: Histogram::seconds(&SNAPSHOT_SECONDS),
			quorum_ack_age: Gauge::new(),
			reflected: Counter::new(),
			rejected: EnumFamily::new(Counter::new),
		}
	}

	/// Starts exposing the group. The cluster runtime calls it once, when the
	/// node joins the cluster machinery.
	pub fn enable(&self) { self.enabled.store(true, Relaxed); }

	#[must_use]
	/// Returns whether the group is exposed.
	pub fn is_enabled(&self) -> bool { self.enabled.load(Relaxed) }

	/// Stores the role of the node.
	pub fn set_state(&self, state: NodeState) { self.role.set(state.value()); }

	/// Stores the current Raft term.
	pub fn set_term(&self, term: u64) { self.term.set_unsigned(term); }

	/// Stores the index of the last log entry the cluster has committed, as
	/// far as this node knows.
	pub fn set_commit_index(&self, index: u64) { self.commit_index.set_unsigned(index); }

	/// Stores the index of the last log entry this node has applied.
	pub fn set_apply_index(&self, index: u64) { self.apply_index.set_unsigned(index); }

	/// Records one wait of the commit barrier that ended in a confirmation.
	pub fn observe_barrier(&self, elapsed: Duration) { self.barrier.observe_duration(elapsed); }

	/// Records a commit barrier that gave up waiting.
	pub fn barrier_timeout(&self) { self.barrier_timeouts.inc(); }

	#[must_use]
	/// The lag gauge of a follower, in log entries behind the leader. The
	/// series exists once it is first asked for, and the leader that owns it
	/// sets it to -1 when it stops leading.
	pub fn follower_lag(&self, node: &str) -> Arc<Gauge> { self.follower_lag.get(&[node]) }

	/// Stores the bytes of captured writes waiting to be replicated.
	pub fn set_queue_bytes(&self, bytes: u64) { self.queue_bytes.set_unsigned(bytes); }

	/// Records that this node saw the leader change.
	pub fn leader_changed(&self) { self.leader_changes.inc(); }

	/// Records the time a snapshot took to build or to install.
	pub fn observe_snapshot(&self, elapsed: Duration) { self.snapshot.observe_duration(elapsed); }

	/// Stores how long ago a majority last confirmed this leader, in
	/// milliseconds. It is -1 when the node is not a leader, or has no
	/// confirmation yet.
	pub fn set_quorum_ack_age(&self, millis: i64) { self.quorum_ack_age.set(millis); }

	/// Records a request that was answered with a 503 because this node is not
	/// the leader.
	pub fn reflect(&self) { self.reflected.inc(); }

	/// Records a request the cluster listener turned away.
	pub fn reject(&self, reason: Rejection) { self.rejected.get(reason).inc(); }

	#[must_use]
	/// Returns the value of `tuwunel_cluster_role`.
	pub fn state_value(&self) -> i64 { self.role.get() }

	#[must_use]
	/// Returns the term last stored.
	pub fn term(&self) -> i64 { self.term.get() }

	#[must_use]
	/// Returns the commit index last stored.
	pub fn commit_index(&self) -> i64 { self.commit_index.get() }

	#[must_use]
	/// Returns the apply index last stored.
	pub fn apply_index(&self) -> i64 { self.apply_index.get() }

	#[must_use]
	/// Returns the number of leader changes seen.
	pub fn leader_changes(&self) -> u64 { self.leader_changes.get() }

	#[must_use]
	/// Returns the number of barrier waits that timed out.
	pub fn barrier_timeouts(&self) -> u64 { self.barrier_timeouts.get() }

	#[must_use]
	/// Returns the number of barrier waits that were confirmed.
	pub fn barrier_waits(&self) -> u64 { self.barrier.count() }

	#[must_use]
	/// Returns the number of requests reflected because the node is not the
	/// leader.
	pub fn reflected(&self) -> u64 { self.reflected.get() }

	#[must_use]
	/// Returns the number of requests turned away for `reason`.
	pub fn rejections(&self, reason: Rejection) -> u64 { self.rejected.get(reason).get() }

	pub(super) fn write(&self, w: &mut Writer) {
		if !self.is_enabled() {
			return;
		}

		w.metric(
			"tuwunel_cluster_role",
			"Role of this node: 0 learner, 1 follower, 2 candidate, 3 leader, 4 shutdown.",
			&self.role,
		);
		w.metric("tuwunel_cluster_term", "Current Raft term.", &self.term);
		w.metric(
			"tuwunel_cluster_commit_index",
			"Index of the last log entry the cluster has committed, as known to this node.",
			&self.commit_index,
		);
		w.metric(
			"tuwunel_cluster_apply_index",
			"Index of the last log entry this node has applied.",
			&self.apply_index,
		);
		w.metric(
			"tuwunel_cluster_barrier_seconds",
			"Time the commit barrier waited for a quorum before a reply was released.",
			&self.barrier,
		);
		w.metric(
			"tuwunel_cluster_barrier_timeouts_total",
			"Waits of the commit barrier that gave up; the outcome of the writes behind them is \
			 unknown.",
			&self.barrier_timeouts,
		);
		w.metric(
			"tuwunel_cluster_follower_lag_entries",
			"Log entries a follower is behind this leader. A node has these series once it has \
			 led, and they read -1 after it stops leading.",
			&self.follower_lag,
		);
		w.metric(
			"tuwunel_cluster_capture_queue_bytes",
			"Bytes of captured writes waiting to be replicated.",
			&self.queue_bytes,
		);
		w.metric(
			"tuwunel_cluster_leader_changes_total",
			"Changes of the leader this node has observed.",
			&self.leader_changes,
		);
		w.metric(
			"tuwunel_cluster_snapshot_seconds",
			"Time to build or install a snapshot of the database.",
			&self.snapshot,
		);
		w.metric(
			"tuwunel_cluster_quorum_ack_age_milliseconds",
			"Time since a majority last confirmed this leader, in milliseconds; -1 on a node \
			 that is not the leader or has no confirmation yet.",
			&self.quorum_ack_age,
		);
		w.metric(
			"tuwunel_cluster_reflected_requests_total",
			"Requests answered with 503 because this node is not the leader.",
			&self.reflected,
		);
		w.metric(
			"tuwunel_cluster_rpc_rejected_total",
			"Requests on the cluster listener turned away before the Raft core saw them, by \
			 reason.",
			&self.rejected,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.role.series(),
			self.term.series(),
			self.commit_index.series(),
			self.apply_index.series(),
			self.barrier.series(),
			self.barrier_timeouts.series(),
			self.follower_lag.series(),
			self.queue_bytes.series(),
			self.leader_changes.series(),
			self.snapshot.series(),
			self.quorum_ack_age.series(),
			self.reflected.series(),
			self.rejected.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Cluster {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for Cluster {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Cluster").finish_non_exhaustive()
	}
}
