use ruma::{DeviceId, UserId, api::client::sync::sync_events::v5::Response};
use tokio::time::{Instant, timeout_at};
use tuwunel_core::{
	debug,
	metrics::prom::{Verdict, WaitStage, WakeReason},
	warn,
};
use tuwunel_service::{Services, sync::Woken};

use super::is_empty_response;

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

/// Compares a pass that followed a wake of the database watchers with the
/// journal, and counts the verdict.
///
/// The journal is asked after the pass, so every change the pass could have seen
/// is in it. The window is `(after, next_batch]`: `after` is the position the
/// wait began from, and `next_batch` the one the pass read up to, so a change
/// the pass could not have seen does not turn a miss into a hit. `response` is
/// `None` when no pass ran because `after` already was the position it would
/// have ended at. Key counts and fallback key types do not count as data, since
/// a woken pass answers with them whatever woke it.
pub(super) async fn verify_woken(
	services: &Services,
	sender_user: &UserId,
	sender_device: Option<&DeviceId>,
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

	let verdict = woken_verdict(relevant, data);

	if verdict == Verdict::Miss {
		warn!(
			user_id = %sender_user,
			device_id = ?sender_device,
			after,
			next_batch,
			parts = ?response.map(|response| filled_parts(response)).unwrap_or_default(),
			"sync journal miss, the database watchers woke a sliding sync long poll that the \
			 journal would have left asleep"
		);
	}

	if let Some(registry) = services.server.metrics.prometheus() {
		registry
			.sync
			.v5_wait_verified(WaitStage::Woken, verdict);
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

fn woken_verdict(relevant: bool, data: bool) -> Verdict {
	match (relevant, data) {
		| (true, _) => Verdict::Both,
		| (false, true) => Verdict::Miss,
		| (false, false) => Verdict::WatcherSpurious,
	}
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
	use tuwunel_core::metrics::prom::Verdict;

	use super::{Wait, timeout_verdict, woken_verdict};

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
		assert_eq!(woken_verdict(true, true), Verdict::Both);
		assert_eq!(woken_verdict(true, false), Verdict::Both);
		assert_eq!(woken_verdict(false, true), Verdict::Miss);
		assert_eq!(woken_verdict(false, false), Verdict::WatcherSpurious);
	}

	#[test]
	fn a_timeout_is_extra_when_the_journal_holds_a_change() {
		assert_eq!(timeout_verdict(true), Verdict::JournalExtra);
		assert_eq!(timeout_verdict(false), Verdict::Quiet);
	}
}
