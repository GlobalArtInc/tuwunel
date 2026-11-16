//! The side of the Raft network that receives: the guards every request
//! passes and the handlers of the RPCs.

use std::sync::Arc;

use axum::{
	Router,
	body::Bytes,
	extract::{DefaultBodyLimit, Request, State, rejection::BytesRejection},
	http::{
		StatusCode,
		header::{AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
	},
	middleware::{Next, from_fn_with_state},
	response::{IntoResponse, Response},
	routing::{get, post},
};
use openraft::{
	errors::{Fatal, RaftError},
	raft::{AppendEntriesRequest, AppendError},
	storage::RaftStateMachine,
};
use serde::{Serialize, de::DeserializeOwned};
use tuwunel_core::{error, metrics::prom::Rejection};

use super::{
	health,
	shared::{Refusal, Shared},
	status, validate,
	wire::{self, APPEND, APPEND_LIMIT, AppendReply, PRE_VOTE, SMALL_LIMIT, TRANSFER, VOTE},
};
use crate::types::TypeConfig;

type Ctx<SM> = Arc<Shared<SM>>;

pub(super) fn router<SM>(shared: &Ctx<SM>) -> Router
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let append_route = Router::new()
		.route(APPEND, post(append::<SM>).layer(DefaultBodyLimit::max(APPEND_LIMIT)))
		.layer(from_fn_with_state(Arc::clone(shared), identity::<SM, true>));

	let vote_routes = Router::new()
		.route(VOTE, post(vote::<SM>).layer(DefaultBodyLimit::max(SMALL_LIMIT)))
		.route(PRE_VOTE, post(pre_vote::<SM>).layer(DefaultBodyLimit::max(SMALL_LIMIT)))
		.route(TRANSFER, post(transfer::<SM>).layer(DefaultBodyLimit::max(SMALL_LIMIT)))
		.layer(from_fn_with_state(Arc::clone(shared), identity::<SM, false>));

	let rpc = append_route
		.merge(vote_routes)
		.layer(from_fn_with_state(Arc::clone(shared), bearer::<SM>))
		.with_state(Arc::clone(shared));

	let mut operator = Router::new()
		.route(wire::STATUS, get(status::report::<SM>))
		.with_state(Arc::clone(shared));

	if shared.faults.is_enabled() {
		operator = operator.merge(shared.faults.router());
	}

	let operator = operator.layer(from_fn_with_state(Arc::clone(shared), bearer::<SM>));

	let public = Router::new()
		.route(wire::LIVE, get(health::live::<SM>))
		.route(wire::READY, get(health::ready::<SM>))
		.route(wire::LEADER, get(health::leader::<SM>))
		.with_state(Arc::clone(shared));

	Router::new()
		.merge(rpc)
		.merge(operator)
		.merge(public)
}

async fn bearer<SM>(State(shared): State<Ctx<SM>>, request: Request, next: Next) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	if shared
		.secret
		.accepts(request.headers().get(AUTHORIZATION))
	{
		return next.run(request).await;
	}

	shared.reject(Rejection::Unauthorized);

	(StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, "Bearer")]).into_response()
}

async fn identity<SM, const BOOTSTRAP: bool>(
	State(shared): State<Ctx<SM>>,
	request: Request,
	next: Next,
) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	match shared.admit(request.headers(), BOOTSTRAP) {
		| Ok(()) => next.run(request).await,
		| Err(Refusal::Malformed(message)) =>
			refuse(&shared, Rejection::Malformed, StatusCode::BAD_REQUEST, &message),
		| Err(Refusal::Conflict { message, from, to }) => {
			shared.warn_conflict(&message, from, to);
			refuse(&shared, Rejection::Identity, StatusCode::CONFLICT, &message)
		},
	}
}

async fn append<SM>(
	State(shared): State<Ctx<SM>>,
	body: Result<Bytes, BytesRejection>,
) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let request: AppendEntriesRequest<TypeConfig> = match read(&shared, body) {
		| Ok(request) => request,
		| Err(response) => return response,
	};

	if let Err(message) = validate::append(&request) {
		return refuse(&shared, Rejection::Invalid, StatusCode::BAD_REQUEST, &message);
	}

	let commit = request.leader_commit.map(|id| id.index);

	match shared.raft.append_entries(request).await {
		| Ok(result) => {
			if !matches!(result, Err(AppendError::HigherVote(_))) {
				shared.heard(commit);
			}

			answer(&AppendReply::from(result))
		},
		| Err(RaftError::Fatal(fatal)) => stopped(&fatal),
		| Err(RaftError::APIError(never)) => match never {},
	}
}

async fn vote<SM>(State(shared): State<Ctx<SM>>, body: Result<Bytes, BytesRejection>) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let request = match read(&shared, body) {
		| Ok(request) => request,
		| Err(response) => return response,
	};

	match shared.raft.vote(request).await {
		| Ok(response) => answer(&response),
		| Err(RaftError::Fatal(fatal)) => stopped(&fatal),
		| Err(RaftError::APIError(never)) => match never {},
	}
}

async fn pre_vote<SM>(
	State(shared): State<Ctx<SM>>,
	body: Result<Bytes, BytesRejection>,
) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let request = match read(&shared, body) {
		| Ok(request) => request,
		| Err(response) => return response,
	};

	match shared.raft.pre_vote(request).await {
		| Ok(response) => answer(&response),
		| Err(RaftError::Fatal(fatal)) => stopped(&fatal),
		| Err(RaftError::APIError(never)) => match never {},
	}
}

async fn transfer<SM>(
	State(shared): State<Ctx<SM>>,
	body: Result<Bytes, BytesRejection>,
) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	let request = match read(&shared, body) {
		| Ok(request) => request,
		| Err(response) => return response,
	};

	match shared.raft.handle_transfer_leader(request).await {
		| Ok(response) => answer(&response),
		| Err(fatal) => stopped(&fatal),
	}
}

fn read<SM, T>(shared: &Ctx<SM>, body: Result<Bytes, BytesRejection>) -> Result<T, Response>
where
	SM: RaftStateMachine<TypeConfig> + Sync,
	T: DeserializeOwned,
{
	let body = body.map_err(|rejection| {
		let status = rejection.status();
		let reason = if status == StatusCode::PAYLOAD_TOO_LARGE {
			Rejection::TooLarge
		} else {
			Rejection::Malformed
		};

		refuse(shared, reason, status, &rejection.body_text())
	})?;

	wire::decode(&body).map_err(|message| {
		refuse(shared, Rejection::Malformed, StatusCode::BAD_REQUEST, &message)
	})
}

fn refuse<SM>(shared: &Ctx<SM>, reason: Rejection, status: StatusCode, message: &str) -> Response
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	shared.reject(reason);

	(status, message.to_owned()).into_response()
}

fn answer<T: Serialize>(value: &T) -> Response {
	match wire::encode(value) {
		| Ok(body) =>
			(StatusCode::OK, [(CONTENT_TYPE, "application/octet-stream")], body).into_response(),
		| Err(message) => {
			error!("Cannot encode a cluster answer: {message}");

			StatusCode::INTERNAL_SERVER_ERROR.into_response()
		},
	}
}

fn stopped(fatal: &Fatal<TypeConfig>) -> Response {
	error!("The Raft core cannot serve a cluster request: {fatal}");

	(StatusCode::SERVICE_UNAVAILABLE, "the Raft core has stopped").into_response()
}
