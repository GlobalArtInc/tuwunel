//! The probes served without a token: liveness, readiness and the leader
//! check the edge uses to route clients.

use std::sync::Arc;

use axum::{
	extract::State,
	http::StatusCode,
	response::{IntoResponse, Response},
};
use openraft::{ServerState, storage::RaftStateMachine};

use super::shared::Shared;
use crate::types::TypeConfig;

pub(super) async fn live<SM>(State(shared): State<Arc<Shared<SM>>>) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let metrics = shared.snapshot();
	if metrics.running_state.is_err() || metrics.state == ServerState::Shutdown {
		return verdict(Err("the Raft core has stopped".to_owned()));
	}

	verdict(Ok(()))
}

pub(super) async fn ready<SM>(State(shared): State<Arc<Shared<SM>>>) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	verdict(readiness(&shared))
}

pub(super) async fn leader<SM>(State(shared): State<Arc<Shared<SM>>>) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	verdict(leading(&shared))
}

pub(super) fn readiness<SM>(shared: &Shared<SM>) -> Result<(), String>
where
	SM: RaftStateMachine<TypeConfig>,
{
	let metrics = shared.snapshot();
	if metrics.running_state.is_err() || metrics.state == ServerState::Shutdown {
		return Err("the Raft core has stopped".to_owned());
	}

	if metrics
		.membership_config
		.get_node(&shared.me.id)
		.is_none()
	{
		return Err("this node is not in the membership".to_owned());
	}

	if metrics.state == ServerState::Leader {
		return Ok(());
	}

	if metrics.current_leader.is_none() {
		return Err("no leader is known".to_owned());
	}

	shared.follower_lag(&metrics).verdict()
}

pub(super) fn leading<SM>(shared: &Shared<SM>) -> Result<(), String>
where
	SM: RaftStateMachine<TypeConfig>,
{
	let metrics = shared.snapshot();
	if metrics.running_state.is_err() {
		return Err("the Raft core has stopped".to_owned());
	}

	if metrics.state != ServerState::Leader {
		return Err("this node is not the leader".to_owned());
	}

	if !shared.serving.is_set() {
		return Err("the services of the leader are not running".to_owned());
	}

	if !shared.lease_valid(&metrics) {
		return Err("no majority has confirmed this leader within the lease".to_owned());
	}

	Ok(())
}

fn verdict(result: Result<(), String>) -> Response {
	match result {
		| Ok(()) => (StatusCode::OK, "ok").into_response(),
		| Err(reason) => (StatusCode::SERVICE_UNAVAILABLE, reason).into_response(),
	}
}
