//! The status report of a node, for operators.

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
	time::Duration,
};

use axum::{Json, extract::State};
use openraft::{ServerState, storage::RaftStateMachine};
use serde::Serialize;

use super::shared::{Shared, quorum_age};
use crate::types::TypeConfig;

/// What `/cluster/status` reports.
#[derive(Clone, Debug, Serialize)]
pub struct Status {
	/// The identifier of the node.
	pub node_id: u64,

	/// The incarnation of its log storage, as 32 hexadecimal digits.
	pub incarnation: String,

	/// The role of the node in Raft.
	pub role: &'static str,

	/// The current term.
	pub term: u64,

	/// The leader this node knows of.
	pub leader: Option<u64>,

	/// Whether the leader's services are marked as running on this node.
	pub serving: bool,

	/// Whether this node is a leader whose lease holds.
	pub lease_valid: bool,

	/// How long ago a majority last confirmed this leader, in milliseconds.
	pub quorum_ack_age_ms: Option<u64>,

	/// The index of the last entry in the log.
	pub last_log_index: Option<u64>,

	/// The index of the last entry known to be committed by the cluster.
	pub commit_index: Option<u64>,

	/// The index of the last entry applied.
	pub apply_index: Option<u64>,

	/// The index of the last entry a snapshot covers.
	pub snapshot_index: Option<u64>,

	/// The index of the last entry purged from the log.
	pub purged_index: Option<u64>,

	/// The membership in force.
	pub membership: Membership,

	/// For a leader, the index of the last entry each member has matched.
	pub replication: Option<BTreeMap<u64, Option<u64>>>,
}

/// The members of the cluster as this node sees them.
#[derive(Clone, Debug, Serialize)]
pub struct Membership {
	/// The index of the log entry that set the membership.
	pub log_index: Option<u64>,

	/// The voters, one set of identifiers per configuration; two sets while a
	/// change is in progress.
	pub voters: Vec<Vec<u64>>,

	/// The members that do not vote.
	pub learners: Vec<u64>,

	/// Every member, with its address and incarnation.
	pub nodes: BTreeMap<u64, Member>,
}

/// One member of the cluster.
#[derive(Clone, Debug, Serialize)]
pub struct Member {
	/// Where the other nodes reach it.
	pub addr: String,

	/// Its incarnation, as 32 hexadecimal digits.
	pub incarnation: String,
}

pub(super) async fn report<SM>(State(shared): State<Arc<Shared<SM>>>) -> Json<Status>
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let metrics = shared.snapshot();
	let membership = &metrics.membership_config;
	let configuration = membership.membership();
	let voters: BTreeSet<u64> = configuration.voter_ids().collect();

	Json(Status {
		node_id: shared.me.id,
		incarnation: format!("{:032x}", shared.me.incarnation),
		role: role(metrics.state),
		term: metrics.current_term,
		leader: metrics.current_leader,
		serving: shared.serving.is_set(),
		lease_valid: shared.lease_valid(&metrics),
		quorum_ack_age_ms: quorum_age(&metrics).map(millis),
		last_log_index: metrics.last_log_index,
		commit_index: metrics.cluster_committed.map(|id| id.index),
		apply_index: metrics.last_applied.map(|id| id.index),
		snapshot_index: metrics.snapshot.map(|id| id.index),
		purged_index: metrics.purged.map(|id| id.index),
		membership: Membership {
			log_index: membership.log_id().map(|id| id.index),
			voters: configuration
				.get_joint_config()
				.iter()
				.map(|voters| voters.iter().copied().collect())
				.collect(),
			learners: configuration
				.nodes()
				.map(|(id, _)| *id)
				.filter(|id| !voters.contains(id))
				.collect(),
			nodes: configuration
				.nodes()
				.map(|(id, node)| {
					(*id, Member {
						addr: node.addr.clone(),
						incarnation: format!("{:032x}", node.incarnation),
					})
				})
				.collect(),
		},
		replication: metrics.replication.as_ref().map(|matched| {
			matched
				.iter()
				.map(|(id, log_id)| (*id, log_id.map(|log_id| log_id.index)))
				.collect()
		}),
	})
}

const fn role(state: ServerState) -> &'static str {
	match state {
		| ServerState::Learner => "learner",
		| ServerState::Follower => "follower",
		| ServerState::Candidate => "candidate",
		| ServerState::Leader => "leader",
		| ServerState::Shutdown => "shutdown",
	}
}

fn millis(age: Duration) -> u64 { u64::try_from(age.as_millis()).unwrap_or(u64::MAX) }
