//! The side of the Raft network that sends: one client per peer, made by a
//! factory Raft calls whenever it needs one.

use std::{
	io,
	marker::PhantomData,
	ops::Range,
	sync::Arc,
	time::{Duration, Instant},
};

use openraft::{
	OptionalSend,
	entry::RaftEntry,
	errors::{NetworkError, RPCError, ReplicationClosed, StreamingError, Timeout, Unreachable},
	network::{RPCOption, RPCTypes, RaftNetworkFactory, RaftNetworkV2},
	raft::{
		AppendEntriesRequest, AppendResult, SnapshotResponse, TransferLeaderRequest,
		TransferLeaderResponse, VoteRequest, VoteResponse,
	},
	type_config::alias::{EntryOf, LogIdOf, SnapshotOf, VoteOf},
};
use postcard::experimental::serialized_size;
use reqwest::{
	StatusCode, Url,
	header::{AUTHORIZATION, CONTENT_TYPE},
	redirect::Policy,
};
use serde::{Serialize, de::DeserializeOwned};
use tokio::time::sleep;

use super::{
	faults::Faults,
	secret::Secret,
	validate,
	wire::{
		self, APPEND, APPEND_LIMIT, AppendReply, CHUNK_BYTES, FROM, PRE_VOTE, Peer, SMALL_LIMIT,
		TO, TRANSFER, VOTE,
	},
};
use crate::types::{Node, NodeId, TypeConfig};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

const KEEPALIVE: Duration = Duration::from_secs(30);

const IDLE: Duration = Duration::from_secs(90);

const SHOWN_BYTES: usize = 256;

type Failure = RPCError<TypeConfig>;

/// Makes the clients Raft sends its RPCs with.
///
/// All clients share one connection pool, with keep-alive, so the heartbeats
/// of an idle cluster reuse their connections. The pool ignores the proxy
/// settings of the environment and follows no redirect, since every request
/// carries the cluster secret. `S` is the type of the snapshot data of the
/// state machine; this factory carries none yet.
pub struct Factory<S> {
	me: Peer,
	secret: Secret,
	faults: Arc<Faults>,
	http: reqwest::Client,
	snapshot: PhantomData<fn() -> S>,
}

/// The client of one peer.
///
/// The record of the peer is checked when the client is made, and a peer whose
/// address is not a plain base URL gets no request, so the secret is never sent
/// where a corrupt record points.
///
/// An RPC that does not finish within the deadline Raft gives it fails with a
/// timeout. A peer that refuses the secret or the identity of this node, or
/// that cannot be reached, is reported as unreachable, which makes Raft wait
/// longer before it tries again.
pub struct Client<S> {
	from: Peer,
	to: Peer,
	base: Result<Url, String>,
	http: reqwest::Client,
	secret: Secret,
	faults: Arc<Faults>,
	snapshot: PhantomData<fn() -> S>,
}

#[derive(Serialize)]
struct Part<'a> {
	vote: &'a VoteOf<TypeConfig>,
	prev_log_id: Option<LogIdOf<TypeConfig>>,
	entries: &'a [EntryOf<TypeConfig>],
	leader_commit: Option<LogIdOf<TypeConfig>>,
}

struct Deadline {
	started: Instant,
	total: Duration,
}

pub(super) struct Chunk {
	pub(super) body: Vec<u8>,
	pub(super) last: Option<LogIdOf<TypeConfig>>,
}

impl<S> Factory<S> {
	/// Creates the factory for the node `me`, which presents `secret`.
	pub fn new(me: Peer, secret: Secret, faults: Arc<Faults>) -> io::Result<Self> {
		let http = reqwest::Client::builder()
			.no_proxy()
			.redirect(Policy::none())
			.connect_timeout(CONNECT_TIMEOUT)
			.tcp_keepalive(KEEPALIVE)
			.tcp_nodelay(true)
			.pool_idle_timeout(IDLE)
			.user_agent("tuwunel-cluster")
			.build()
			.map_err(io::Error::other)?;

		Ok(Self {
			me,
			secret,
			faults,
			http,
			snapshot: PhantomData,
		})
	}
}

impl<S> RaftNetworkFactory<TypeConfig> for Factory<S>
where
	S: OptionalSend + 'static,
{
	type Network = Client<S>;

	async fn new_client(&mut self, target: NodeId, node: &Node) -> Self::Network {
		Client {
			from: self.me,
			to: Peer::new(target, node.incarnation),
			base: validate::node(node)
				.and_then(|()| Url::parse(&node.addr).map_err(|error| error.to_string())),
			http: self.http.clone(),
			secret: self.secret.clone(),
			faults: Arc::clone(&self.faults),
			snapshot: PhantomData,
		}
	}
}

impl<S> RaftNetworkV2<TypeConfig> for Client<S>
where
	S: OptionalSend + 'static,
{
	type SnapshotData = S;

	async fn append_entries(
		&mut self,
		rpc: AppendEntriesRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<AppendResult<TypeConfig>, Failure> {
		let first = Deadline::new(option.hard_ttl());
		self.disturb(RPCTypes::AppendEntries, &first)
			.await?;

		let plan = plan(&rpc.entries).map_err(|error| unreachable_peer(&error))?;
		let count = plan.len();
		let mut deadline = Some(first);

		for (position, range) in plan.into_iter().enumerate() {
			let finished = position.saturating_add(1) == count;
			let chunk = part(&rpc, range, finished).map_err(|error| unreachable_peer(&error))?;
			let deadline = deadline
				.take()
				.unwrap_or_else(|| Deadline::new(option.hard_ttl()));

			let reply: AppendReply = self
				.send(APPEND, RPCTypes::AppendEntries, chunk.body, &deadline)
				.await?;

			let reached_end =
				matches!(&reply, AppendReply::Matched(matched) if *matched == chunk.last);

			if finished || !reached_end {
				return Ok(reply.into());
			}
		}

		Err(network_failure("an append request had no chunk to send"))
	}

	async fn vote(
		&mut self,
		rpc: VoteRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<VoteResponse<TypeConfig>, Failure> {
		self.unary(VOTE, RPCTypes::Vote, &rpc, &option)
			.await
	}

	async fn pre_vote(
		&mut self,
		rpc: VoteRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<VoteResponse<TypeConfig>, Failure> {
		self.unary(PRE_VOTE, RPCTypes::Vote, &rpc, &option)
			.await
	}

	async fn full_snapshot(
		&mut self,
		_vote: VoteOf<TypeConfig>,
		_snapshot: SnapshotOf<TypeConfig, S>,
		_cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
		_option: RPCOption,
	) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
		Err(StreamingError::Unreachable(Unreachable::from_string(
			"snapshot transfer is not implemented",
		)))
	}

	async fn transfer_leader(
		&mut self,
		req: TransferLeaderRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<TransferLeaderResponse<TypeConfig>, Failure> {
		self.unary(TRANSFER, RPCTypes::TransferLeader, &req, &option)
			.await
	}
}

impl<S> Client<S> {
	async fn unary<Req, Rep>(
		&self,
		route: &str,
		action: RPCTypes,
		request: &Req,
		option: &RPCOption,
	) -> Result<Rep, Failure>
	where
		Req: Serialize + Sync,
		Rep: DeserializeOwned,
	{
		let deadline = Deadline::new(option.hard_ttl());
		let body = wire::encode(request).map_err(|error| network_failure(&error))?;

		self.disturb(action, &deadline).await?;
		self.send(route, action, body, &deadline).await
	}

	async fn send<Rep>(
		&self,
		route: &str,
		action: RPCTypes,
		body: Vec<u8>,
		deadline: &Deadline,
	) -> Result<Rep, Failure>
	where
		Rep: DeserializeOwned,
	{
		let base = self.base.as_ref().map_err(|error| {
			unreachable_peer(&format!("the address of the node is refused: {error}"))
		})?;

		let url = base.join(route).map_err(|error| {
			unreachable_peer(&format!("route {route} does not fit the address: {error}"))
		})?;

		let remaining = deadline
			.remaining()
			.ok_or_else(|| self.timeout(action, deadline))?;

		let mut response = self
			.http
			.post(url)
			.timeout(remaining)
			.header(AUTHORIZATION, self.secret.authorization())
			.header(FROM, self.from.to_string())
			.header(TO, self.to.to_string())
			.header(CONTENT_TYPE, "application/octet-stream")
			.body(body)
			.send()
			.await
			.map_err(|error| self.transport(action, deadline, &error))?;

		let status = response.status();
		let mut answer = Vec::new();
		while let Some(part) = response
			.chunk()
			.await
			.map_err(|error| self.transport(action, deadline, &error))?
		{
			if answer.len().saturating_add(part.len()) > SMALL_LIMIT {
				return Err(network_failure("the answer is larger than a cluster answer can be"));
			}

			answer.extend_from_slice(&part);
		}

		if status != StatusCode::OK {
			return Err(self.refused(status, &answer));
		}

		wire::decode(&answer).map_err(|error| network_failure(&error))
	}

	async fn disturb(&self, action: RPCTypes, deadline: &Deadline) -> Result<(), Failure> {
		let Some(rule) = self.faults.rule(self.to.id) else {
			return Ok(());
		};

		let lost = rule.loss_percent > 0 && rand::random_range(0..100_u8) < rule.loss_percent;
		let delay = Duration::from_millis(rule.delay_ms);
		let remaining = deadline.remaining().unwrap_or_default();

		if lost || delay >= remaining {
			sleep(remaining).await;

			return Err(self.timeout(action, deadline));
		}

		sleep(delay).await;

		Ok(())
	}

	fn transport(
		&self,
		action: RPCTypes,
		deadline: &Deadline,
		error: &reqwest::Error,
	) -> Failure {
		if error.is_timeout() {
			self.timeout(action, deadline)
		} else if error.is_connect() {
			RPCError::Unreachable(Unreachable::new(error))
		} else {
			RPCError::Network(NetworkError::new(error))
		}
	}

	fn refused(&self, status: StatusCode, answer: &[u8]) -> Failure {
		let node = self.to.id;
		let text = shown(answer);

		match status {
			| StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => unreachable_peer(&format!(
				"node {node} refused the cluster credentials ({status})"
			)),
			| StatusCode::CONFLICT => unreachable_peer(&format!(
				"node {node} does not accept the identities of this exchange: {text}"
			)),
			| _ => network_failure(&format!("node {node} answered {status}: {text}")),
		}
	}

	fn timeout(&self, action: RPCTypes, deadline: &Deadline) -> Failure {
		RPCError::Timeout(Timeout {
			action,
			id: self.from.id,
			target: self.to.id,
			timeout: deadline.total,
		})
	}
}

impl Deadline {
	fn new(total: Duration) -> Self { Self { started: Instant::now(), total } }

	fn remaining(&self) -> Option<Duration> {
		self.total
			.checked_sub(self.started.elapsed())
			.filter(|left| !left.is_zero())
	}
}

pub(super) fn plan(entries: &[EntryOf<TypeConfig>]) -> Result<Vec<Range<usize>>, String> {
	let mut ranges = Vec::new();
	let mut start = 0_usize;
	let mut size = 0_usize;

	for (index, entry) in entries.iter().enumerate() {
		let weight = serialized_size(entry)
			.map_err(|error| format!("an entry is not encodable: {error}"))?;

		if index > start && size.saturating_add(weight) > CHUNK_BYTES {
			ranges.push(start..index);
			start = index;
			size = 0;
		}

		size = size.saturating_add(weight);
	}

	ranges.push(start..entries.len());

	Ok(ranges)
}

pub(super) fn part(
	rpc: &AppendEntriesRequest<TypeConfig>,
	range: Range<usize>,
	finished: bool,
) -> Result<Chunk, String> {
	let prev_log_id = match range.start.checked_sub(1) {
		| Some(before) => rpc.entries.get(before).map(RaftEntry::log_id),
		| None => rpc.prev_log_id,
	};

	let entries = rpc
		.entries
		.get(range)
		.ok_or_else(|| "an append request lost track of its entries".to_owned())?;

	let last = entries
		.last()
		.map(RaftEntry::log_id)
		.or(prev_log_id);

	let leader_commit = if finished {
		rpc.leader_commit
	} else {
		capped(rpc.leader_commit, last)
	};

	let body = wire::encode(&Part {
		vote: &rpc.vote,
		prev_log_id,
		entries,
		leader_commit,
	})?;

	sealed(body, last)
}

#[cfg(test)]
pub(super) fn chunks(rpc: &AppendEntriesRequest<TypeConfig>) -> Result<Vec<Chunk>, String> {
	let plan = plan(&rpc.entries)?;
	let count = plan.len();

	plan.into_iter()
		.enumerate()
		.map(|(position, range)| part(rpc, range, position.saturating_add(1) == count))
		.collect()
}

fn sealed(body: Vec<u8>, last: Option<LogIdOf<TypeConfig>>) -> Result<Chunk, String> {
	if body.len() > APPEND_LIMIT {
		return Err(format!(
			"an append request of {} bytes exceeds the {APPEND_LIMIT} bytes a node accepts; a \
			 single entry cannot be split",
			body.len()
		));
	}

	Ok(Chunk { body, last })
}

fn capped(
	commit: Option<LogIdOf<TypeConfig>>,
	last: Option<LogIdOf<TypeConfig>>,
) -> Option<LogIdOf<TypeConfig>> {
	match (commit, last) {
		| (Some(commit), Some(last)) if commit.index > last.index => Some(last),
		| (commit, _) => commit,
	}
}

fn shown(answer: &[u8]) -> String {
	answer
		.iter()
		.take(SHOWN_BYTES)
		.map(|byte| {
			if byte.is_ascii_graphic() || *byte == b' ' {
				char::from(*byte)
			} else {
				'?'
			}
		})
		.collect()
}

fn unreachable_peer(message: &str) -> Failure {
	RPCError::Unreachable(Unreachable::from_string(message))
}

fn network_failure(message: &str) -> Failure {
	RPCError::Network(NetworkError::from_string(message))
}
