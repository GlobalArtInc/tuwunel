//! The network of the cluster: the RPCs of Raft between the nodes, and the
//! probes and reports of a node, on the cluster listener.
//!
//! Nodes talk over HTTP with bodies in `postcard`. Every request carries the
//! shared secret as a bearer token, and every RPC names its sender and its
//! recipient with their incarnations; a node refuses with 409, and changes
//! nothing, a request that names an incarnation other than the one it knows, or
//! a sender it has never heard of. Only AppendEntries from an unknown sender is
//! heard by a node that is in no membership yet, since that is how it learns
//! its first one.
//! What arrives in an AppendEntries request is checked before it can reach the
//! log. The health probes and the leader check are the only routes open
//! without the secret.
//!
//! # Security
//!
//! The listener speaks plain HTTP, and the shared secret is its only
//! credential. Whoever holds the secret can append any entry to the log of every
//! node, which is the right to write anything into the database of every node.
//! The `from` and `to` headers that name the sender and the recipient are not
//! authentication: they guard against an honest process that went stale, such
//! as a node started on a replaced volume, and whoever holds the secret can
//! forge them. Close the port with a network policy to the nodes and the health
//! probes, never publish it through an ingress, make the secret at least 32
//! random bytes, and keep its file readable by the server only (mode 0400).
//!
//! # Wiring
//!
//! A node builds the pieces in this order. Open the [`crate::LogStore`] and
//! bind it to the node with [`crate::Meta::bind_node`], which refuses a volume
//! that belonged to another node. Name the node with [`Peer::new`] from its
//! identifier and [`crate::Meta::incarnation`], load the [`Secret`], and make
//! the [`Faults`]: [`Faults::on`] with [`crate::LogStore::sync_delay`] when the
//! configuration allows faults, [`Faults::off`] otherwise. The [`Factory`] takes
//! these and goes to `Raft::new` as its network. Once the Raft core exists,
//! [`Server::new`] builds the routes over it, and [`serve`] runs them on the
//! socket bound to `cluster.listen`.
//!
//! # Size of a request
//!
//! A sender keeps an AppendEntries body near [`CHUNK_BYTES`] by sending a
//! request with many entries as several requests, which Raft's rules allow, and
//! a node refuses a body over [`APPEND_LIMIT`] with 413. A single entry is
//! atomic and is never split, so an entry too large for one request cannot be
//! replicated: the sender reports the peer as unreachable and keeps trying.
//! Whoever captures the entries has to keep each below the limit, or depose the
//! leader, because a leader whose entry no follower can take can never confirm
//! the writes behind it.

mod client;
mod faults;
mod health;
mod limiter;
mod observe;
mod readiness;
mod secret;
mod server;
mod shared;
mod status;
#[cfg(test)]
mod tests;
mod validate;
mod wire;

use std::{io, sync::Arc};

use axum::Router;
use openraft::{Raft, storage::RaftStateMachine};
use tokio::{net::TcpListener, task::JoinHandle};

use self::shared::Shared;
pub use self::{
	client::{Client, Factory},
	faults::{Faults, MAX_RPC_DELAY_MS, Rule, Setup, Target},
	secret::{MIN_SECRET_LEN, Secret},
	shared::{Serving, Settings},
	status::{Member, Membership, Status},
	validate::{
		append as validate_append, membership as validate_membership, node as validate_node,
	},
	wire::{
		APPEND, APPEND_LIMIT, CHUNK_BYTES, FAULTS, LEADER, LIVE, PRE_VOTE, Peer, READY,
		SMALL_LIMIT, STATUS, TRANSFER, VOTE,
	},
};
use crate::types::TypeConfig;

/// The listener side of the cluster network of one node.
///
/// It owns the routes and the task that publishes metrics; the caller owns the
/// socket, which it binds to `cluster.listen`, and runs [`serve`] on it.
pub struct Server<SM>
where
	SM: RaftStateMachine<TypeConfig>,
{
	shared: Arc<Shared<SM>>,
}

impl<SM> Server<SM>
where
	SM: RaftStateMachine<TypeConfig> + Sync,
{
	/// Builds the server of the node whose Raft core is `raft`.
	#[must_use]
	pub fn new(raft: Raft<TypeConfig, SM>, settings: Settings) -> Self {
		Self {
			shared: Arc::new(Shared::new(raft, settings)),
		}
	}

	/// The routes of the listener.
	///
	/// The RPCs, `/cluster/status` and, when faults are allowed,
	/// `/cluster/faults` require the bearer token. `/health/live`,
	/// `/health/ready` and `/_tuwunel/cluster/leader` do not.
	#[must_use]
	pub fn router(&self) -> Router { server::router(&self.shared) }

	/// The flag that tells the leader check whether the services are running.
	#[must_use]
	pub fn serving(&self) -> Serving { self.shared.serving.clone() }

	/// Starts the task that publishes the `tuwunel_cluster_*` metrics and keeps
	/// the follower readiness current. It ends when the Raft core does.
	#[must_use]
	pub fn observe(&self) -> JoinHandle<()> { observe::spawn(Arc::clone(&self.shared)) }
}

/// Serves `router` on `listener` until `shutdown` completes.
pub async fn serve<F>(listener: TcpListener, router: Router, shutdown: F) -> io::Result<()>
where
	F: Future<Output = ()> + Send + 'static,
{
	axum::serve(listener, router)
		.with_graceful_shutdown(shutdown)
		.await
}
