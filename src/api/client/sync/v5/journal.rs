use std::collections::BTreeMap;

use futures::{StreamExt, future::join};
use ruma::{
	DeviceId, UInt, UserId,
	api::client::sync::sync_events::v5::{
		ListId, Request, Response,
		response::{E2EE, Extensions, List},
	},
};
use tokio::{
	sync::Notify,
	time::{Instant, timeout_at},
};
use tuwunel_core::{
	Result, debug,
	metrics::prom::{Api, V5Path, Verdict, WaitStage, WakeReason},
	warn,
};
use tuwunel_service::{
	Services,
	sync::{Connection, Woken},
};

use super::{
	ResponseLists,
	extensions::{acknowledge_to_device, to_device_pending},
	is_empty_response, long_poll,
};

/// How the long poll of one loop iteration waits for the next change.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Wait {
	/// Database watchers, registered before the pass.
	Watchers,

	/// The sync journal, with no watcher registered.
	Journal,

	/// Database watchers, with the journal asked afterwards whether it would
	/// have woken the poll.
	Verify,
}

impl Wait {
	/// The mode the keys select now, which a configuration reload can change
	/// between two iterations of one request.
	///
	/// Whether the journal is on comes from the journal itself, whose capacity
	/// is the one it started with.
	pub(super) fn current(services: &Services) -> Self {
		let sync = &services.config.client.sync;

		Self::select(
			services.sync.journal.is_on(),
			sync.v5_journal_wait,
			sync.v5_journal_wait_verify,
		)
	}

	fn select(journal_on: bool, wait: bool, verify: bool) -> Self {
		if !journal_on {
			Self::Watchers
		} else if verify {
			Self::Verify
		} else if wait {
			Self::Journal
		} else {
			Self::Watchers
		}
	}

	pub(super) fn registers(self) -> bool { self != Self::Journal }
}

/// How the wait of a long poll ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Ended {
	/// A watcher fired or the journal holds a change, so the next pass looks
	/// again.
	Woken,

	/// The poll ran out of time.
	TimedOut,

	/// The poll cannot go on: the server is stopping, the client went away, or
	/// the request asked for no wait.
	Stopped,
}

/// Waits for a change that the next pass must look at, through the watchers
/// when there are any and through the sync journal when there are none.
///
/// `after` is the position of the pass that just ended, which is the
/// `next_batch` that pass read up to, so a change that landed between that
/// pass and this call lies in the window the journal first looks at. The
/// future keeps nothing outside itself, so dropping it at any await is safe.
pub(super) async fn wait_for_change<Watchers>(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	after: u64,
	stop_at: Instant,
	watchers: Option<Watchers>,
) -> Ended
where
	Watchers: Future<Output = ()>,
{
	match watchers {
		| Some(watchers) => match timeout_at(stop_at, watchers).await {
			| Ok(()) => Ended::Woken,
			| Err(_) => Ended::TimedOut,
		},
		| None => wait_on_journal(services, sender_user, sender_device, after, stop_at).await,
	}
}

/// Waits through the sync journal for a change after `after` that concerns the
/// user, and counts why the wait ended.
///
/// A poll whose client went away is dropped by its caller before the wait ends
/// and is not counted. A journal that dropped changes ends the wait too, but
/// that is a reason to pass again from a position at or above the journal's
/// horizon.
async fn wait_on_journal(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	after: u64,
	stop_at: Instant,
) -> Ended {
	let woken = timeout_at(
		stop_at,
		services
			.sync
			.wait_journal(sender_user, sender_device, after),
	)
	.await;

	let reason = match woken {
		| Ok(Woken::Journal(_)) => WakeReason::Journal,
		| Ok(Woken::Horizon) => WakeReason::Horizon,
		| Ok(Woken::Shutdown) => WakeReason::Shutdown,
		| Err(_) => WakeReason::Timeout,
	};

	if let Some(registry) = services.server.metrics.prometheus() {
		registry.sync.v5_wait_woken(reason);
	}

	match reason {
		| WakeReason::Journal | WakeReason::Horizon => Ended::Woken,
		| WakeReason::Timeout => Ended::TimedOut,
		| WakeReason::Shutdown => Ended::Stopped,
	}
}

/// What a request brings to the gate, as the route has worked it out before any
/// pass.
#[derive(Clone, Copy, Debug)]
pub(super) struct Arrival {
	/// The position the client resumed from, zero for a connection that begins.
	pub(super) since: u64,

	/// Whether `since` is the position of the connection's last response, as
	/// opposed to a replay of an earlier one.
	pub(super) advancing: bool,

	/// Whether the request changed the lists, the subscriptions or the e2ee
	/// extension of the connection.
	pub(super) config_changed: bool,

	/// Whether the request changed an extension setting the connection keeps for
	/// later requests, which an answer that saves nothing would lose.
	pub(super) settings_changed: bool,

	/// How long the request may long-poll, in milliseconds.
	pub(super) timeout: u64,
}

/// What the gate decided about a request before its first pass.
#[derive(Debug)]
pub(super) enum Gated {
	/// The request waited for the journal to name a change, none came, and this
	/// is its answer. No pass ran and the connection was not saved.
	Quiet(Box<Response>),

	/// The request runs its pass.
	Pass(Passage),
}

/// What the gate leaves for the pass it let through.
#[derive(Debug, Default)]
pub(super) struct Passage {
	/// Whether the gate ended its wait with a change, which makes the first pass
	/// a woken one.
	pub(super) woken: bool,

	/// Whether the gate already acknowledged the to-device events, which the
	/// route then does not do a second time.
	pub(super) acknowledged: bool,

	/// Whether the first pass is to be compared with the journal, which only the
	/// shadow mode does for a request the gate would have held.
	pub(super) checked: bool,
}

/// Holds a request that the journal names no change for until it does or until
/// the request times out.
///
/// A request resuming from a position with a timeout waits on the journal from
/// that position before its pass, and is answered without a pass when nothing
/// came. The answer repeats the position and the list counts of the last pass, and
/// the connection is neither moved nor saved, so the next request resumes from the
/// same position. The wait starts from `since`, a position the client has seen
/// answered, so a change that landed since then is found by its first scan.
///
/// A request is held only when nothing is owed to it that the journal would not
/// announce: no change of lists, subscriptions or extension settings, no profile,
/// no rooms the last pass failed to collect, no to-device events left over, and list counts
/// from a pass to repeat. Any other request passes at once, and so does every
/// request in the shadow mode, which only marks the first pass of a request it
/// would have held for comparison with the journal. With the journal off the gate
/// does nothing.
pub(super) async fn gate(
	services: &Services,
	(sender_user, sender_device): (&UserId, Option<&DeviceId>),
	request: &Request,
	conn: &Connection,
	arrival: Arrival,
	(interrupted, stop_at): (&Notify, Instant),
) -> Result<Gated> {
	let wait = Wait::current(services);

	if wait == Wait::Watchers || !admits(conn, arrival, services.server.is_stopping()) {
		return Ok(Gated::Pass(Passage::default()));
	}

	if wait == Wait::Verify {
		let tail = to_device_pending(services, sender_user, sender_device, conn).await?;

		return Ok(Gated::Pass(Passage { checked: !tail, ..Passage::default() }));
	}

	acknowledge_to_device(services, sender_user, sender_device, conn).await?;

	let acknowledged = Passage { acknowledged: true, ..Passage::default() };

	if to_device_pending(services, sender_user, sender_device, conn).await? {
		return Ok(Gated::Pass(acknowledged));
	}

	let ended = {
		let _long_poll = long_poll(services, Api::V5);

		tokio::select! {
			() = interrupted.notified() => Ended::Stopped,
			ended = wait_on_journal(
				services,
				sender_user,
				sender_device,
				arrival.since,
				stop_at,
			) => ended,
		}
	};

	match ended {
		| Ended::Woken => Ok(Gated::Pass(Passage { woken: true, ..acknowledged })),
		| Ended::TimedOut | Ended::Stopped => {
			let response =
				quiet(services, (sender_user, sender_device), request, conn, arrival).await;

			Ok(Gated::Quiet(Box::new(response)))
		},
	}
}

/// Whether a request is one the gate may hold.
///
/// To-device events left over are not part of this, since finding them reads the
/// database.
fn admits(conn: &Connection, arrival: Arrival, stopping: bool) -> bool {
	arrival.since != 0
		&& arrival.advancing
		&& arrival.timeout > 0
		&& !stopping
		&& !arrival.config_changed
		&& !arrival.settings_changed
		&& !conn.own_profile_owed()
		&& !conn.ranges_owed
		&& conn
			.last_counts
			.as_ref()
			.is_some_and(|counts| counts.keys().eq(conn.lists.keys()))
}

/// The answer to a request the journal named no change for.
///
/// It carries the position of the request and the list counts of the last pass,
/// and from the extensions only the key counts and fallback key types, which a
/// pass that times out carries as well. The key counts are read now, since they
/// are current state.
async fn quiet(
	services: &Services,
	(sender_user, sender_device): (&UserId, Option<&DeviceId>),
	request: &Request,
	conn: &Connection,
	arrival: Arrival,
) -> Response {
	let e2ee = quiet_keys(services, sender_user, sender_device, conn).await;

	let lists = conn
		.last_counts
		.iter()
		.flatten()
		.map(|(id, count)| (id.clone(), List { count: *count }))
		.collect();

	if let Some(registry) = services.server.metrics.prometheus() {
		registry.sync.v5_request(V5Path::Quiet);
		registry.sync.v5_response_rooms(0);
	}

	debug!(since = arrival.since, "Answering a request without a pass");

	Response {
		txn_id: request.txn_id.clone(),
		pos: arrival.since.to_string().into(),
		lists,
		rooms: Default::default(),
		extensions: Extensions { e2ee, ..Default::default() },
	}
}

async fn quiet_keys(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	conn: &Connection,
) -> E2EE {
	let Some(sender_device) =
		sender_device.filter(|_| conn.extensions.e2ee.enabled.unwrap_or(false))
	else {
		return E2EE::default();
	};

	let counts = services
		.users
		.count_one_time_keys(sender_user, sender_device);

	let fallback = services
		.users
		.unused_fallback_key_algorithms(sender_user, sender_device)
		.collect::<Vec<_>>();

	let (device_one_time_keys_count, fallback) = join(counts, fallback).await;

	E2EE {
		device_one_time_keys_count,
		device_unused_fallback_key_types: Some(fallback),
		..Default::default()
	}
}

/// The counts of the lists a pass answered with, which the gate repeats.
pub(super) fn list_counts(lists: &ResponseLists) -> BTreeMap<ListId, UInt> {
	lists
		.iter()
		.map(|(id, list)| (id.clone(), list.count))
		.collect()
}

/// A pass of the shadow mode that is compared with the journal afterwards.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Checked {
	/// The first pass of a request the gate would have held.
	Gate,

	/// A pass that followed a wake of the database watchers.
	Woken,
}

impl Checked {
	fn stage(self) -> WaitStage {
		match self {
			| Self::Gate => WaitStage::Gate,
			| Self::Woken => WaitStage::Woken,
		}
	}

	/// The verdict on a pass, given whether the journal names a change in its
	/// window and whether the pass found data.
	///
	/// A pass the watchers woke is not blamed for finding nothing, while a
	/// request the gate would have held only counts as saved when its pass found
	/// nothing and the journal named nothing.
	fn verdict(self, relevant: bool, data: bool) -> Verdict {
		match (self, relevant, data) {
			| (Self::Woken, true, _) | (Self::Gate, true, true) => Verdict::Both,
			| (_, false, true) => Verdict::Miss,
			| (Self::Woken, false, false) => Verdict::WatcherSpurious,
			| (Self::Gate, true, false) => Verdict::JournalExtra,
			| (Self::Gate, false, false) => Verdict::Quiet,
		}
	}
}

/// Compares a pass of the shadow mode with the journal, and counts the verdict.
///
/// The journal is asked after the pass, so every change the pass could have seen
/// is in it. The window is `(after, next_batch]`: `after` is the position the
/// wait or the request began from, and `next_batch` the one the pass read up to, so
/// a change the pass could not have seen does not turn a miss into a hit.
/// `response` is `None` when no pass ran because `after` already was the position
/// it would have ended at. Key counts and fallback key types do not count as
/// data, since a woken pass answers with them whatever woke it.
pub(super) async fn verify_pass(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	checked: Checked,
	(after, next_batch): (u64, u64),
	response: Option<&mut Response>,
) {
	let Some(relevant) = services
		.sync
		.journal_relevant_upto(sender_user, sender_device, after, next_batch)
		.await
	else {
		skipped(sender_user, after);

		return;
	};

	let mut response = response;
	let data = response
		.as_mut()
		.is_some_and(|response| !is_empty_response(response, false));

	let verdict = checked.verdict(relevant, data);

	if verdict == Verdict::Miss {
		warn!(
			user_id = %sender_user,
			device_id = ?sender_device,
			stage = ?checked,
			after,
			next_batch,
			parts = ?response.map(|response| filled_parts(response)).unwrap_or_default(),
			"sync journal miss, a sliding sync pass found data that the journal would have left \
			 asleep"
		);
	}

	if let Some(registry) = services.server.metrics.prometheus() {
		registry
			.sync
			.v5_wait_verified(checked.stage(), verdict);
	}
}

/// Compares a long poll that ran out of time with the journal, and counts the
/// verdict.
///
/// The window is the one the wait covered, from the `next_batch` it began at up
/// to the frontier the counter has retired now.
pub(super) async fn verify_timeout(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
	after: u64,
) {
	let Some(relevant) = services
		.sync
		.journal_relevant(sender_user, sender_device, after)
		.await
	else {
		skipped(sender_user, after);

		return;
	};

	if let Some(registry) = services.server.metrics.prometheus() {
		registry
			.sync
			.v5_wait_verified(WaitStage::Timeout, timeout_verdict(relevant));
	}
}

fn skipped(sender_user: &UserId, after: u64) {
	debug!(
		user_id = %sender_user,
		after,
		"sync journal wait verification skipped, the journal no longer covers after"
	);
}

fn timeout_verdict(relevant: bool) -> Verdict {
	if relevant {
		Verdict::JournalExtra
	} else {
		Verdict::Quiet
	}
}

fn filled_parts(response: &Response) -> Vec<&'static str> {
	let extensions = &response.extensions;

	[
		("rooms", !response.rooms.is_empty()),
		("to_device", extensions.to_device.is_some()),
		("device_lists", !extensions.e2ee.device_lists.is_empty()),
		("account_data", !extensions.account_data.is_empty()),
		("receipts", !extensions.receipts.is_empty()),
		("typing", !extensions.typing.is_empty()),
		("profiles", !extensions.profiles.is_empty()),
	]
	.into_iter()
	.filter_map(|(part, filled)| filled.then_some(part))
	.collect()
}

#[cfg(test)]
mod tests {
	use ruma::{api::client::sync::sync_events::v5::ListId, uint};
	use tuwunel_core::metrics::prom::Verdict;
	use tuwunel_service::sync::Connection;

	use super::{Arrival, Checked, Wait, admits, timeout_verdict};

	fn connection() -> Connection {
		Connection {
			lists: [(ListId::from("main"), Default::default())].into(),
			last_counts: Some([(ListId::from("main"), uint!(3))].into()),
			..Default::default()
		}
	}

	fn arrival() -> Arrival {
		Arrival {
			since: 7,
			advancing: true,
			config_changed: false,
			settings_changed: false,
			timeout: 30_000,
		}
	}

	#[test]
	fn the_journal_must_be_on_for_any_mode_but_the_watchers() {
		assert_eq!(Wait::select(true, false, false), Wait::Watchers);
		assert_eq!(Wait::select(true, true, false), Wait::Journal);
		assert_eq!(Wait::select(true, false, true), Wait::Verify);
		assert_eq!(Wait::select(true, true, true), Wait::Verify);
		assert_eq!(Wait::select(false, true, false), Wait::Watchers);
		assert_eq!(Wait::select(false, true, true), Wait::Watchers);
	}

	#[test]
	fn only_the_journal_mode_registers_no_watchers() {
		assert!(Wait::Watchers.registers());
		assert!(Wait::Verify.registers());
		assert!(!Wait::Journal.registers());
	}

	#[test]
	fn a_woken_pass_is_a_miss_only_with_data_and_no_journal_entry() {
		assert_eq!(Checked::Woken.verdict(true, true), Verdict::Both);
		assert_eq!(Checked::Woken.verdict(true, false), Verdict::Both);
		assert_eq!(Checked::Woken.verdict(false, true), Verdict::Miss);
		assert_eq!(Checked::Woken.verdict(false, false), Verdict::WatcherSpurious);
	}

	#[test]
	fn a_gated_pass_is_saved_only_when_it_found_nothing_and_the_journal_named_nothing() {
		assert_eq!(Checked::Gate.verdict(true, true), Verdict::Both);
		assert_eq!(Checked::Gate.verdict(true, false), Verdict::JournalExtra);
		assert_eq!(Checked::Gate.verdict(false, true), Verdict::Miss);
		assert_eq!(Checked::Gate.verdict(false, false), Verdict::Quiet);
	}

	#[test]
	fn a_timeout_is_extra_when_the_journal_holds_a_change() {
		assert_eq!(timeout_verdict(true), Verdict::JournalExtra);
		assert_eq!(timeout_verdict(false), Verdict::Quiet);
	}

	#[test]
	fn a_request_with_nothing_owed_is_held() {
		assert!(admits(&connection(), arrival(), false));
	}

	#[test]
	fn a_request_is_not_held_without_a_position_to_resume_or_a_poll_to_wait_out() {
		for arrival in [
			Arrival { since: 0, ..arrival() },
			Arrival { advancing: false, ..arrival() },
			Arrival { timeout: 0, ..arrival() },
		] {
			assert!(!admits(&connection(), arrival, false));
		}

		assert!(!admits(&connection(), arrival(), true));
	}

	#[test]
	fn a_request_is_not_held_while_something_is_owed() {
		let mut profiles = connection();
		profiles.extensions.profiles.enabled = Some(true);

		let owing = [
			(connection(), Arrival { config_changed: true, ..arrival() }),
			(connection(), Arrival { settings_changed: true, ..arrival() }),
			(Connection { ranges_owed: true, ..connection() }, arrival()),
			(profiles, arrival()),
			(Connection { last_counts: None, ..connection() }, arrival()),
			(
				Connection {
					last_counts: Some(Default::default()),
					..connection()
				},
				arrival(),
			),
		];

		for (connection, arrival) in owing {
			assert!(!admits(&connection, arrival, false));
		}
	}
}
