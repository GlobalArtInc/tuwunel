//! Configures the replicated cluster mode.
//!
//! The section names the identity of a node, where it listens and how its
//! peers reach it, where the Raft log and the snapshots live, and the timings
//! of the protocol. While `enable` is false nothing in it is used or checked.

use std::{
	net::{IpAddr, Ipv4Addr, SocketAddr},
	path::PathBuf,
};

use serde::Deserialize;
use tuwunel_macros::config_example_generator;

use super::deserialize_bytesize_u64;
use crate::{Err, Result, err};

/// Cluster replication
///
/// Runs this server as one node of a cluster that replicates its database
/// through a Raft quorum, so that a write confirmed to a client survives the
/// loss of any one node. It is off by default, and while it is off the other
/// keys in this section are neither used nor checked. Every setting is read
/// once at startup; restart the server after changing one.
///
/// Tips:
/// - Startup refuses `enable` without a node identity (`node_id` or
///   `node_id_from_hostname`, not both), `advertise`, `secret_file`,
///   `raft_path` and `snapshot_path`.
/// - The cluster listener is separate from `http.listen`. Do not publish it
///   through a reverse proxy; only the other nodes and the health probes need
///   to reach it.
/// - A node never forms a cluster on its own. An empty `raft_path` yields an
///   empty log, and only `initialize` turns it into a one-node cluster.
///
/// Security:
/// - The cluster listener speaks plain HTTP, and the shared secret is its only
///   credential. Whoever holds the secret can append any entry to the log of
///   every node, which is the right to write anything into the database of
///   every node.
/// - The sender and recipient a request names are not authentication. They
///   guard against an honest process that went stale, such as a node started on
///   a replaced volume, and whoever holds the secret can forge them.
/// - Close the port with a network policy to the nodes and the health probes,
///   never publish it through an ingress, make the secret at least 32 random
///   bytes, and keep its file readable by the server only (mode 0400).
#[expect(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#[derive(Clone, Debug, Deserialize)]
#[config_example_generator(
	filename = "tuwunel-example.toml",
	section = "global.cluster",
	group = "true"
)]
pub struct Cluster {
	/// Turns on cluster mode.
	///
	/// While off, the server runs on its own and the rest of this section is
	/// ignored.
	#[serde(default)]
	pub enable: bool,

	/// Sets the numeric identity of this node in the cluster.
	///
	/// It must differ between the nodes and stay the same for the life of the
	/// node. Set this or `node_id_from_hostname`, not both.
	///
	/// example: 0
	pub node_id: Option<u64>,

	/// Takes the identity of this node from the number at the end of the host
	/// name.
	///
	/// This is the ordinal a StatefulSet gives its pods: `tuwunel-2` becomes
	/// node 2. A host name that does not end in a dash and digits fails the
	/// startup. It cannot be combined with `node_id`.
	#[serde(default)]
	pub node_id_from_hostname: bool,

	/// Sets the address of the cluster listener.
	///
	/// It carries the traffic between the nodes and the health endpoints, on a
	/// port of its own.
	///
	/// default: "0.0.0.0:8009"
	#[serde(default = "default_listen")]
	pub listen: SocketAddr,

	/// Sets the base URL the other nodes reach this node at.
	///
	/// The URL is shared with the cluster as part of its membership. It must
	/// use `http` or `https` and name a host.
	///
	/// example: "http://tuwunel-0.tuwunel-peers.default.svc:8009"
	pub advertise: Option<String>,

	/// Reads the shared secret that authenticates cluster traffic from this
	/// file.
	///
	/// Every node must hold the same secret. Surrounding whitespace is ignored.
	/// With the cluster enabled, startup refuses a file that cannot be read, is
	/// empty, is shorter than 16 bytes, or holds anything but printable ASCII
	/// characters without spaces.
	///
	/// example: "/etc/tuwunel/cluster-secret"
	pub secret_file: Option<PathBuf>,

	/// Sets the directory of the Raft log.
	///
	/// Votes and log entries are synced to disk before they count, so keep it
	/// on storage that survives restarts of the node. It must not be the
	/// database directory.
	///
	/// example: "/var/lib/tuwunel/raft"
	pub raft_path: Option<PathBuf>,

	/// Sets the directory the snapshots of the database are kept in and
	/// received snapshots are staged in.
	///
	/// Leave room for about two copies of the database.
	///
	/// example: "/var/lib/tuwunel/snapshots"
	pub snapshot_path: Option<PathBuf>,

	/// Forms a new cluster of this node alone when it starts with an empty
	/// Raft log.
	///
	/// Set it on one node, for the first start only; the other nodes are added
	/// to the cluster afterwards. It does nothing once a log exists.
	#[serde(default)]
	pub initialize: bool,

	/// Sets how often a leader contacts each follower to keep its leadership,
	/// in milliseconds.
	///
	/// default: 250
	#[serde(default = "default_heartbeat_ms")]
	pub heartbeat_ms: u64,

	/// Sets the range, in milliseconds, a follower draws the time it waits for
	/// its leader from before it stands for election.
	///
	/// Two numbers, the lower bound first. The lower bound must be below the
	/// upper one and at least three times `heartbeat_ms`.
	///
	/// default: [1500, 3000]
	#[serde(default = "default_election_timeout_ms")]
	pub election_timeout_ms: ElectionTimeout,

	/// Sets how long, in milliseconds, a leader counts as confirmed by a
	/// majority after it last heard from one.
	///
	/// It must be above `heartbeat_ms` and below the lower bound of
	/// `election_timeout_ms`.
	///
	/// default: 1000
	#[serde(default = "default_lease_ms")]
	pub lease_ms: u64,

	/// Sets how long, in milliseconds, a reply waits for the writes behind it
	/// to be confirmed by a majority.
	///
	/// A request that times out fails with an error that leaves the outcome of
	/// its writes unknown.
	///
	/// default: 10000
	#[serde(default = "default_commit_timeout_ms")]
	pub commit_timeout_ms: u64,

	/// Sets how much new Raft log makes the node take a snapshot of the
	/// database.
	///
	/// Accepts an integer byte count or a string with an SI/IEC suffix such as
	/// "256 MiB".
	///
	/// default: "256 MiB"
	#[serde(
		default = "default_snapshot_log_bytes",
		deserialize_with = "deserialize_bytesize_u64"
	)]
	pub snapshot_log_bytes: u64,

	/// Sets how much of the Raft log is kept, so that a follower that fell
	/// behind catches up from the log instead of from a snapshot.
	///
	/// Accepts an integer byte count or a string with an SI/IEC suffix such as
	/// "1 GiB".
	///
	/// default: "1 GiB"
	#[serde(
		default = "default_keep_log_bytes",
		deserialize_with = "deserialize_bytesize_u64"
	)]
	pub keep_log_bytes: u64,

	/// Limits the speed a snapshot is sent to another node at, in megabytes
	/// per second.
	///
	/// default: 50
	#[serde(default = "default_snapshot_rate_mb")]
	pub snapshot_rate_mb: u64,

	/// Limits the writes a leader holds back while they wait to be replicated.
	///
	/// A leader whose queue outgrows the limit gives up leadership.
	/// Accepts an integer byte count or a string with an SI/IEC suffix such as
	/// "256 MiB".
	///
	/// default: "256 MiB"
	#[serde(
		default = "default_capture_queue_bytes",
		deserialize_with = "deserialize_bytesize_u64"
	)]
	pub capture_queue_bytes: u64,

	/// Allows requests that inject faults into the cluster traffic: lost or
	/// delayed messages to a node and delayed disk syncs.
	///
	/// For rehearsals of failures only. Never enable it on a server that
	/// serves users.
	#[serde(default)]
	pub allow_faults: bool,
}

/// The range an election timeout is drawn from, in milliseconds. In the
/// configuration it is an array of exactly two numbers, the lower bound first.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(try_from = "Vec<u64>")]
pub struct ElectionTimeout {
	/// The shortest time a follower waits for its leader.
	pub min: u64,

	/// The longest time a follower waits for its leader.
	pub max: u64,
}

impl TryFrom<Vec<u64>> for ElectionTimeout {
	type Error = String;

	fn try_from(bounds: Vec<u64>) -> Result<Self, Self::Error> {
		match bounds[..] {
			| [min, max] => Ok(Self { min, max }),
			| _ => Err(format!(
				"expected two numbers, the lower and the upper bound, found {}",
				bounds.len()
			)),
		}
	}
}

impl Default for Cluster {
	fn default() -> Self {
		Self {
			enable: Default::default(),
			node_id: Default::default(),
			node_id_from_hostname: Default::default(),
			listen: default_listen(),
			advertise: Default::default(),
			secret_file: Default::default(),
			raft_path: Default::default(),
			snapshot_path: Default::default(),
			initialize: Default::default(),
			heartbeat_ms: default_heartbeat_ms(),
			election_timeout_ms: default_election_timeout_ms(),
			lease_ms: default_lease_ms(),
			commit_timeout_ms: default_commit_timeout_ms(),
			snapshot_log_bytes: default_snapshot_log_bytes(),
			keep_log_bytes: default_keep_log_bytes(),
			snapshot_rate_mb: default_snapshot_rate_mb(),
			capture_queue_bytes: default_capture_queue_bytes(),
			allow_faults: Default::default(),
		}
	}
}

impl Cluster {
	/// Resolves the identity of this node for the given host name.
	///
	/// With `node_id` set that is the answer. With `node_id_from_hostname` it
	/// is the number after the last dash of the first label of the host name,
	/// and a host name without one is an error. A configuration that sets both
	/// keys or neither is an error as well.
	pub fn resolve_node_id(&self, hostname: &str) -> Result<u64> {
		self.check_node_identity()?;

		match self.node_id {
			| Some(id) => Ok(id),
			| None => ordinal(hostname),
		}
	}

	pub(super) fn check_node_identity(&self) -> Result {
		match (self.node_id, self.node_id_from_hostname) {
			| (Some(_), false) | (None, true) => Ok(()),
			| (Some(_), true) => Err!(Config(
				"cluster.node_id_from_hostname",
				"cluster.node_id and cluster.node_id_from_hostname are mutually exclusive; set \
				 only one of them"
			)),
			| (None, false) => Err!(Config(
				"cluster.node_id",
				"set cluster.node_id or cluster.node_id_from_hostname when the cluster is \
				 enabled"
			)),
		}
	}
}

fn ordinal(hostname: &str) -> Result<u64> {
	let label = hostname
		.split_once('.')
		.map_or(hostname, |(label, _)| label);

	label
		.rsplit_once('-')
		.filter(|(_, digits)| digits.bytes().all(|byte| byte.is_ascii_digit()))
		.and_then(|(_, digits)| digits.parse().ok())
		.ok_or_else(|| {
			err!(Config(
				"cluster.node_id_from_hostname",
				"host name {hostname:?} does not end in a dash and a node number"
			))
		})
}

fn default_listen() -> SocketAddr { SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 8009) }

fn default_heartbeat_ms() -> u64 { 250 }

fn default_election_timeout_ms() -> ElectionTimeout { ElectionTimeout { min: 1500, max: 3000 } }

fn default_lease_ms() -> u64 { 1000 }

fn default_commit_timeout_ms() -> u64 { 10_000 }

fn default_snapshot_log_bytes() -> u64 { 256 * 1024 * 1024 }

fn default_keep_log_bytes() -> u64 { 1024 * 1024 * 1024 }

fn default_snapshot_rate_mb() -> u64 { 50 }

fn default_capture_queue_bytes() -> u64 { 256 * 1024 * 1024 }
