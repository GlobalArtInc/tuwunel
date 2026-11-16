//! Types the cluster instantiates Raft with.

use std::fmt::{self, Debug, Display};

use serde::{Deserialize, Serialize};
use tuwunel_database::Entry;

/// Identifier of a cluster node. It is stable across restarts and replacements
/// of the node's volume.
pub type NodeId = u64;

/// What the cluster knows about a member besides its identifier.
///
/// The membership of the Raft group is a map from [`NodeId`] to this record, so
/// it is replicated and agreed upon like any other part of the group state.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct Node {
	/// Base URL peers reach the node at.
	pub addr: String,

	/// Random number created when the node's log storage was first created.
	/// A node started on a fresh volume under an old identifier carries a
	/// different value, which tells the group it is not the node it replaces.
	pub incarnation: u128,
}

/// The payload of a replicated log entry: one captured write of the database
/// leader.
///
/// The type exists only to carry the `Display` implementation Raft requires of
/// its application data. It serializes exactly as the [`Entry`] it wraps.
/// Neither `Display` nor `Debug` prints the records, which hold user data.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Request(pub Entry);

impl Display for Request {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(
			f,
			"Request(origin: {:016x}, seq: {}..{}, cfs: {}, bytes: {})",
			self.0.origin,
			self.0.seq.start,
			self.0.seq.end,
			self.0.cfs.len(),
			self.0.rep.len()
		)
	}
}

impl Debug for Request {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { Display::fmt(self, f) }
}

openraft::declare_raft_types!(
	/// Raft type configuration of the cluster.
	///
	/// One leader per term, as in the Raft paper.
	pub TypeConfig:
		D = Request,
		R = (),
		NodeId = NodeId,
		Node = Node,
		LeaderId = openraft::impls::leader_id_std::LeaderId<u64, u64>,
);
