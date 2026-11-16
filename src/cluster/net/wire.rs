//! The wire format between cluster nodes: routes, identity headers, limits and
//! the bodies of the RPCs.

use std::{
	fmt::{self, Display},
	str::FromStr,
};

use axum::http::HeaderName;
use openraft::{
	raft::{AppendError, AppendResult, ConflictHint},
	type_config::alias::{LogIdOf, VoteOf},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::types::{NodeId, TypeConfig};

/// Route of the AppendEntries RPC.
pub const APPEND: &str = "/cluster/raft/append";

/// Route of the vote RPC.
pub const VOTE: &str = "/cluster/raft/vote";

/// Route of the pre-vote RPC.
pub const PRE_VOTE: &str = "/cluster/raft/pre_vote";

/// Route of the leadership transfer RPC.
pub const TRANSFER: &str = "/cluster/raft/transfer";

/// Route of the status report.
pub const STATUS: &str = "/cluster/status";

/// Route of the fault injection controls.
pub const FAULTS: &str = "/cluster/faults";

/// Route of the liveness probe.
pub const LIVE: &str = "/health/live";

/// Route of the readiness probe.
pub const READY: &str = "/health/ready";

/// Route of the check that tells the edge which node is the leader.
pub const LEADER: &str = "/_tuwunel/cluster/leader";

/// The size a sender aims to keep an AppendEntries body under, in bytes.
///
/// A request with more entries is sent as several requests, each continuing
/// where the previous one ended. A single entry is never split, so one entry
/// may be larger.
pub const CHUNK_BYTES: usize = 1 << 20;

/// The largest AppendEntries body a node accepts, in bytes. A body above it is
/// refused with 413 before it is decoded.
pub const APPEND_LIMIT: usize = 2 << 20;

/// The largest body of any other RPC, in bytes.
pub const SMALL_LIMIT: usize = 64 << 10;

/// Header naming the node that sends a request.
pub(crate) const FROM: HeaderName = HeaderName::from_static("x-tuwunel-from");

/// Header naming the node a request is meant for.
pub(crate) const TO: HeaderName = HeaderName::from_static("x-tuwunel-to");

/// A node as the cluster tells it apart: its identifier and the incarnation of
/// its log storage.
///
/// Every request names both its sender and its recipient this way, so a
/// request meant for the node that used to hold a volume is not mistaken for
/// one meant for the node that holds it now.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Peer {
	/// The identifier of the node.
	pub id: NodeId,

	/// The incarnation of its log storage.
	pub incarnation: u128,
}

impl Peer {
	/// Names a node.
	#[must_use]
	pub const fn new(id: NodeId, incarnation: u128) -> Self { Self { id, incarnation } }
}

impl Display for Peer {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}:{:032x}", self.id, self.incarnation)
	}
}

impl FromStr for Peer {
	type Err = String;

	fn from_str(text: &str) -> Result<Self, Self::Err> {
		let (id, incarnation) = text
			.split_once(':')
			.ok_or_else(|| "node name has no incarnation".to_owned())?;

		if incarnation.len() != 32 {
			return Err("incarnation is not 32 hexadecimal digits".to_owned());
		}

		Ok(Self {
			id: id
				.parse()
				.map_err(|_| "node identifier is not a number".to_owned())?,
			incarnation: u128::from_str_radix(incarnation, 16)
				.map_err(|_| "incarnation is not hexadecimal".to_owned())?,
		})
	}
}

/// The answer to an AppendEntries RPC.
///
/// Raft's own result type carries no serialization, so the reply keeps its
/// parts: the last log id that matched, or the conflict with the hint the
/// leader uses to back off in fewer steps, or the vote that is higher.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(crate) enum AppendReply {
	Matched(Option<LogIdOf<TypeConfig>>),
	Conflict {
		log_id: LogIdOf<TypeConfig>,
		hint: Option<Vec<LogIdOf<TypeConfig>>>,
	},
	HigherVote(VoteOf<TypeConfig>),
}

impl From<AppendResult<TypeConfig>> for AppendReply {
	fn from(result: AppendResult<TypeConfig>) -> Self {
		match result {
			| Ok(matched) => Self::Matched(matched),
			| Err(AppendError::Conflict { log_id, hint }) => Self::Conflict {
				log_id,
				hint: hint.map(|hint| hint.log_ids),
			},
			| Err(AppendError::HigherVote(vote)) => Self::HigherVote(vote),
		}
	}
}

impl From<AppendReply> for AppendResult<TypeConfig> {
	fn from(reply: AppendReply) -> Self {
		match reply {
			| AppendReply::Matched(matched) => Ok(matched),
			| AppendReply::Conflict { log_id, hint } => Err(AppendError::Conflict {
				log_id,
				hint: hint.map(ConflictHint::new),
			}),
			| AppendReply::HigherVote(vote) => Err(AppendError::HigherVote(vote)),
		}
	}
}

/// Encodes a body.
pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
	postcard::to_allocvec(value).map_err(|error| format!("body is not encodable: {error}"))
}

/// Decodes a body that must be exactly one value; bytes after it are an error.
pub(crate) fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
	let (value, rest) = postcard::take_from_bytes::<T>(bytes)
		.map_err(|error| format!("body is malformed: {error}"))?;

	if !rest.is_empty() {
		return Err(format!("body has {} bytes after its end", rest.len()));
	}

	Ok(value)
}
