//! The task that turns what Raft reports into the `tuwunel_cluster_*`
//! metrics.

use std::{sync::Arc, time::Duration};

use openraft::{
	ServerState, metrics::RaftMetrics, storage::RaftStateMachine,
	type_config::async_runtime::WatchReceiver,
};
use tokio::{
	task::JoinHandle,
	time::{MissedTickBehavior, interval},
};
use tuwunel_core::metrics::prom::{Gauge, NodeState};

use super::shared::{Shared, quorum_age};
use crate::types::{NodeId, TypeConfig};

const TICK: Duration = Duration::from_millis(250);

#[derive(Default)]
struct Seen {
	leader: Option<NodeId>,
	lagging: Vec<Arc<Gauge>>,
}

pub(super) fn spawn<SM>(shared: Arc<Shared<SM>>) -> JoinHandle<()>
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	tokio::spawn(async move {
		let metrics = shared.raft.metrics();
		let mut tick = interval(TICK);
		tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

		let mut seen = Seen::default();
		loop {
			tick.tick().await;

			let current = metrics.borrow_watched().clone();
			observe(&shared, &current, &mut seen);

			if current.running_state.is_err() || current.state == ServerState::Shutdown {
				break;
			}
		}
	})
}

fn observe<SM>(shared: &Shared<SM>, metrics: &RaftMetrics<TypeConfig>, seen: &mut Seen)
where
	SM: RaftStateMachine<TypeConfig>,
{
	shared.follower_lag(metrics);

	let Some(group) = shared.group() else {
		return;
	};

	group.enable();
	group.set_state(state(metrics));
	group.set_term(metrics.current_term);
	group.set_commit_index(metrics.cluster_committed.map_or(0, |id| id.index));
	group.set_apply_index(metrics.last_applied.map_or(0, |id| id.index));

	if let Some(leader) = metrics.current_leader
		&& leader_changed(&mut seen.leader, leader)
	{
		group.leader_changed();
	}

	let mut lagging = Vec::new();
	let mut age = -1;
	if metrics.state == ServerState::Leader {
		age = if shared.self_quorum(metrics) {
			0
		} else {
			quorum_age(metrics)
				.map_or(-1, |age| i64::try_from(age.as_millis()).unwrap_or(i64::MAX))
		};

		for (id, matched) in metrics.replication.iter().flatten() {
			if *id == shared.me.id {
				continue;
			}

			let behind = match (metrics.last_log_index, matched) {
				| (None, _) => 0,
				| (Some(tail), None) => tail.saturating_add(1),
				| (Some(tail), Some(matched)) => tail.saturating_sub(matched.index),
			};

			let gauge = group.follower_lag(&id.to_string());
			gauge.set_unsigned(behind);
			lagging.push(gauge);
		}
	}

	for gone in seen
		.lagging
		.drain(..)
		.filter(|old| !lagging.iter().any(|now| Arc::ptr_eq(old, now)))
	{
		gone.set(-1);
	}

	seen.lagging = lagging;
	group.set_quorum_ack_age(age);
}

fn state(metrics: &RaftMetrics<TypeConfig>) -> NodeState {
	match metrics.state {
		| ServerState::Learner => NodeState::Learner,
		| ServerState::Follower => NodeState::Follower,
		| ServerState::Candidate => NodeState::Candidate,
		| ServerState::Leader => NodeState::Leader,
		| ServerState::Shutdown => NodeState::Shutdown,
	}
}

pub(super) fn leader_changed(seen: &mut Option<NodeId>, leader: NodeId) -> bool {
	let changed = seen.is_some_and(|known| known != leader);
	*seen = Some(leader);

	changed
}
