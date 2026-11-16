//! Faults injected into the cluster traffic, for rehearsals of failures.
//!
//! They exist only when the configuration allows them. The controls are
//! reached through the `/cluster/faults` route, which is not registered
//! otherwise, and what they set is read by the client of every RPC and by the
//! disk syncs of the log store. A node without faults behaves as if this module
//! were not there.

use std::{
	collections::BTreeMap,
	sync::{Arc, PoisonError, RwLock},
	time::Duration,
};

use axum::{
	Json, Router,
	extract::{DefaultBodyLimit, State},
	http::StatusCode,
	response::{IntoResponse, Response},
	routing::get,
};
use serde::{Deserialize, Serialize};

use crate::{
	store::{MAX_SYNC_DELAY, SyncDelay},
	types::NodeId,
};

/// The longest delay an RPC can be given, in milliseconds.
pub const MAX_RPC_DELAY_MS: u64 = 10_000;

/// What happens to the RPCs sent to one node, or to every node.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Rule {
	/// The share of RPCs that are lost, in percent. A lost RPC is never sent
	/// and fails with a timeout once its deadline passes.
	pub loss_percent: u8,

	/// How long every RPC that is not lost is held back before it is sent, in
	/// milliseconds. The delay counts against the deadline of the RPC.
	pub delay_ms: u64,
}

/// A rule and the node it applies to; none means every node.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
	/// The node the RPCs go to, or none for all of them.
	#[serde(default)]
	pub node: Option<NodeId>,

	/// The share of RPCs that are lost, in percent.
	#[serde(default)]
	pub loss_percent: u8,

	/// How long every RPC that is not lost is held back, in milliseconds.
	#[serde(default)]
	pub delay_ms: u64,
}

/// The whole configuration of the faults: a rule per target and the delay of
/// the disk syncs. A request replaces the previous configuration.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Setup {
	/// Rules for the RPCs sent by this node. A rule for a node wins over the
	/// rule for every node.
	#[serde(default)]
	pub rpc: Vec<Target>,

	/// The delay of every sync of the Raft log of this node, in milliseconds.
	#[serde(default)]
	pub sync_delay_ms: u64,
}

/// The fault controls of one node.
#[derive(Debug, Default)]
pub struct Faults {
	enabled: bool,
	sync: SyncDelay,
	rules: RwLock<BTreeMap<Option<NodeId>, Rule>>,
}

impl Faults {
	/// Faults that are not allowed: nothing can be set, and nothing is
	/// injected.
	#[must_use]
	pub fn off() -> Arc<Self> { Arc::new(Self::default()) }

	/// Faults that are allowed. `sync` is the delay handle of the log store,
	/// from [`crate::LogStore::sync_delay`].
	#[must_use]
	pub fn on(sync: SyncDelay) -> Arc<Self> {
		Arc::new(Self {
			enabled: true,
			sync,
			rules: RwLock::default(),
		})
	}

	/// Tells whether faults are allowed.
	#[must_use]
	pub fn is_enabled(&self) -> bool { self.enabled }

	/// The rule that applies to RPCs sent to `node`, if any.
	#[must_use]
	pub fn rule(&self, node: NodeId) -> Option<Rule> {
		if !self.enabled {
			return None;
		}

		let rules = self
			.rules
			.read()
			.unwrap_or_else(PoisonError::into_inner);

		rules
			.get(&Some(node))
			.or_else(|| rules.get(&None))
			.copied()
			.filter(|rule| rule.loss_percent > 0 || rule.delay_ms > 0)
	}

	/// The configuration in force.
	#[must_use]
	pub fn setup(&self) -> Setup {
		let rules = self
			.rules
			.read()
			.unwrap_or_else(PoisonError::into_inner);

		Setup {
			rpc: rules
				.iter()
				.map(|(node, rule)| Target {
					node: *node,
					loss_percent: rule.loss_percent,
					delay_ms: rule.delay_ms,
				})
				.collect(),
			sync_delay_ms: u64::try_from(self.sync.get().as_millis()).unwrap_or(u64::MAX),
		}
	}

	/// Replaces the configuration, after checking all of it: nothing is
	/// applied when any part is refused.
	pub fn apply(&self, setup: &Setup) -> Result<(), String> {
		if !self.enabled {
			return Err("faults are not allowed on this node".to_owned());
		}

		let mut rules = BTreeMap::new();
		for target in &setup.rpc {
			if target.loss_percent > 100 {
				return Err("loss_percent is above 100".to_owned());
			}

			if target.delay_ms > MAX_RPC_DELAY_MS {
				return Err(format!("delay_ms is above {MAX_RPC_DELAY_MS}"));
			}

			let rule = Rule {
				loss_percent: target.loss_percent,
				delay_ms: target.delay_ms,
			};

			if rules.insert(target.node, rule).is_some() {
				return Err("two rules name the same node".to_owned());
			}
		}

		if Duration::from_millis(setup.sync_delay_ms) > MAX_SYNC_DELAY {
			return Err(format!("sync_delay_ms is above {}", MAX_SYNC_DELAY.as_millis()));
		}

		*self
			.rules
			.write()
			.unwrap_or_else(PoisonError::into_inner) = rules;

		self.sync
			.set(Duration::from_millis(setup.sync_delay_ms));

		Ok(())
	}

	pub(super) fn router(self: &Arc<Self>) -> Router {
		Router::new()
			.route(super::wire::FAULTS, get(show).put(set).delete(clear))
			.layer(DefaultBodyLimit::max(super::wire::SMALL_LIMIT))
			.with_state(Arc::clone(self))
	}
}

async fn show(State(faults): State<Arc<Faults>>) -> Json<Setup> { Json(faults.setup()) }

async fn set(State(faults): State<Arc<Faults>>, Json(setup): Json<Setup>) -> Response {
	match faults.apply(&setup) {
		| Ok(()) => Json(faults.setup()).into_response(),
		| Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
	}
}

async fn clear(State(faults): State<Arc<Faults>>) -> Response {
	match faults.apply(&Setup::default()) {
		| Ok(()) => Json(faults.setup()).into_response(),
		| Err(message) => (StatusCode::BAD_REQUEST, message).into_response(),
	}
}
