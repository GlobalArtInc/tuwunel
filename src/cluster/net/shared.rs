//! The state the cluster listener shares between its routes.

use std::{
	sync::{
		Arc, Mutex, PoisonError,
		atomic::{AtomicBool, Ordering},
	},
	time::{Duration, Instant},
};

use axum::http::{HeaderMap, HeaderName};
use openraft::{
	Instant as _, Raft, ServerState,
	metrics::RaftMetrics,
	storage::RaftStateMachine,
	type_config::{alias::StoredMembershipOf, async_runtime::WatchReceiver},
};
use tuwunel_core::{
	metrics::{
		Metrics,
		prom::{Cluster as Group, Rejection},
	},
	warn,
};

use super::{
	faults::Faults,
	limiter::Limiter,
	readiness::{Contact, Lag},
	secret::Secret,
	wire::{FROM, Peer, TO},
};
use crate::types::TypeConfig;

const WARN_EVERY: Duration = Duration::from_secs(10);

/// Everything a node is told to build its listener with.
pub struct Settings {
	/// This node.
	pub me: Peer,

	/// The secret every request must present.
	pub secret: Secret,

	/// The fault controls, shared with the client factory.
	pub faults: Arc<Faults>,

	/// How long a leader counts as confirmed by a majority after it last heard
	/// from one. From `cluster.lease_ms`.
	pub lease: Duration,

	/// Where the `tuwunel_cluster_*` metrics go. They are written once the
	/// registry has been enabled.
	pub metrics: Option<Arc<Metrics>>,
}

/// Tells whether the services of this node are running.
///
/// The cluster leader is only offered to the edge while they are. The server
/// sets the flag once it has started its services after being promoted, and
/// clears it before it stops them.
#[derive(Clone, Debug, Default)]
pub struct Serving(Arc<AtomicBool>);

pub(super) struct Shared<SM>
where
	SM: RaftStateMachine<TypeConfig>,
{
	pub(super) raft: Raft<TypeConfig, SM>,
	pub(super) me: Peer,
	pub(super) secret: Secret,
	pub(super) faults: Arc<Faults>,
	pub(super) lease: Duration,
	pub(super) serving: Serving,
	metrics: Option<Arc<Metrics>>,
	contact: Mutex<Contact>,
	warned: Mutex<Limiter>,
}

pub(super) enum Refusal {
	Malformed(String),
	Conflict {
		message: String,
		from: Peer,
		to: Peer,
	},
}

impl Serving {
	/// Records whether the services are running.
	pub fn set(&self, serving: bool) { self.0.store(serving, Ordering::SeqCst); }

	/// Tells whether the services are running.
	#[must_use]
	pub fn is_set(&self) -> bool { self.0.load(Ordering::SeqCst) }
}

impl<SM> Shared<SM>
where
	SM: RaftStateMachine<TypeConfig>,
{
	pub(super) fn new(raft: Raft<TypeConfig, SM>, settings: Settings) -> Self {
		Self {
			raft,
			me: settings.me,
			secret: settings.secret,
			faults: settings.faults,
			lease: settings.lease,
			serving: Serving::default(),
			metrics: settings.metrics,
			contact: Mutex::new(Contact::new(Instant::now())),
			warned: Mutex::new(Limiter::new(WARN_EVERY)),
		}
	}

	pub(super) fn snapshot(&self) -> RaftMetrics<TypeConfig> {
		self.raft.metrics().borrow_watched().clone()
	}

	pub(super) fn group(&self) -> Option<&Group> {
		self.metrics
			.as_ref()?
			.prometheus()
			.map(|registry| &registry.cluster)
	}

	pub(super) fn reject(&self, reason: Rejection) {
		if let Some(group) = self.group() {
			group.reject(reason);
		}
	}

	/// Checks the identities a request names. `bootstrap` lets through a
	/// sender this node has never heard of while it is in no membership at all;
	/// only AppendEntries, by which a node learns its first membership, gets it.
	pub(super) fn admit(&self, headers: &HeaderMap, bootstrap: bool) -> Result<(), Refusal> {
		let from = peer(headers, &FROM)?;
		let to = peer(headers, &TO)?;
		let conflict =
			|message: &str| Refusal::Conflict { message: message.to_owned(), from, to };

		if to != self.me {
			return Err(conflict(
				"the request is meant for another node, or for another incarnation of this one",
			));
		}

		if from.id == self.me.id {
			return Err(conflict("the request claims to come from this node"));
		}

		let metrics = self.snapshot();
		let membership: &StoredMembershipOf<TypeConfig> = &metrics.membership_config;

		if let Some(own) = membership.get_node(&self.me.id)
			&& own.incarnation != self.me.incarnation
		{
			return Err(conflict(
				"this node is recorded in the membership under another incarnation",
			));
		}

		match membership.get_node(&from.id) {
			| Some(node) if node.incarnation == from.incarnation => Ok(()),
			| Some(_) =>
				Err(conflict("the sender's incarnation differs from the one in the membership")),
			| None if bootstrap && membership.nodes().next().is_none() => Ok(()),
			| None => Err(conflict("the sender is not a member of the cluster")),
		}
	}

	/// Logs a refusal for a conflict of identities, at most once per
	/// [`WARN_EVERY`], with the number of refusals held back since.
	pub(super) fn warn_conflict(&self, message: &str, from: Peer, to: Peer) {
		let held_back = self
			.warned
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.take(Instant::now());

		if let Some(held_back) = held_back {
			warn!(
				"Refused a cluster request from {from} to {to}: {message} ({held_back} similar \
				 refusals were not logged)"
			);
		}
	}

	pub(super) fn heard(&self, commit: Option<u64>) {
		self.contact().heard(commit, Instant::now());
	}

	pub(super) fn follower_lag(&self, metrics: &RaftMetrics<TypeConfig>) -> Lag {
		let known = metrics.cluster_committed.map(|id| id.index);
		let applied = metrics.last_applied.map(|id| id.index);

		self.contact()
			.measure(known, applied, Instant::now())
	}

	pub(super) fn self_quorum(&self, metrics: &RaftMetrics<TypeConfig>) -> bool {
		let joint = metrics
			.membership_config
			.membership()
			.get_joint_config();

		matches!(joint.as_slice(), [voters] if voters.len() == 1 && voters.contains(&self.me.id))
	}

	pub(super) fn lease_valid(&self, metrics: &RaftMetrics<TypeConfig>) -> bool {
		metrics.state == ServerState::Leader
			&& (self.self_quorum(metrics)
				|| quorum_age(metrics).is_some_and(|age| age < self.lease))
	}

	fn contact(&self) -> std::sync::MutexGuard<'_, Contact> {
		self.contact
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

pub(super) fn quorum_age(metrics: &RaftMetrics<TypeConfig>) -> Option<Duration> {
	metrics
		.last_quorum_acked
		.as_ref()
		.map(|acked| acked.elapsed())
}

fn peer(headers: &HeaderMap, name: &HeaderName) -> Result<Peer, Refusal> {
	headers
		.get(name)
		.ok_or_else(|| Refusal::Malformed(format!("header {name} is missing")))?
		.to_str()
		.map_err(|_| Refusal::Malformed(format!("header {name} is not text")))?
		.parse()
		.map_err(|error| Refusal::Malformed(format!("header {name}: {error}")))
}
