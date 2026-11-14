use std::collections::BTreeMap;

use ruma::{
	EventId, OwnedEventId, UInt,
	api::client::sync::sync_events::v3::{State as RoomState, StateEvents},
	uint,
};
use tuwunel_core::metrics::prom::Registry;

use super::{
	Since, StateAfter, capped_timeline_limit,
	joined::{
		fastpath::{Busy, Verdict},
		notifications::{
			GateInputs, Gates as NotificationGates,
			assemble_threads as assemble_thread_notifications,
			assemble_unread as assemble_unread_notifications,
			compute_gates as compute_notification_gates, in_window, quiet_counts_reported,
			send_thread_counts,
		},
	},
	stale_since,
};

const SINCE: u64 = 100;
const NEXT_BATCH: u64 = 200;
const INITIAL_SINCE: u64 = 0;

#[test]
fn state_after_wraps_into_named_variant() {
	let events = StateEvents::default;

	assert!(matches!(StateAfter::Off.wrap(events()), RoomState::Before(_)));
	assert!(matches!(StateAfter::Stable.wrap(events()), RoomState::After(_)));
	assert!(matches!(StateAfter::Unstable.wrap(events()), RoomState::AfterUnstable(_)));

	assert!(!StateAfter::Off.requested());
	assert!(StateAfter::Stable.requested());
	assert!(StateAfter::Unstable.requested());
}

#[test]
fn state_after_selects_unstable_when_both_opted_in() {
	// (use_state_after, use_state_after_unstable)
	assert!(matches!(StateAfter::from((false, false)), StateAfter::Off));
	assert!(matches!(StateAfter::from((true, false)), StateAfter::Stable));
	assert!(matches!(StateAfter::from((false, true)), StateAfter::Unstable));
	assert!(matches!(StateAfter::from((true, true)), StateAfter::Unstable));
}

#[test]
fn since_without_token_is_initial() {
	assert_eq!(Since::from(None), Since::Initial);
	assert_eq!(Since::from(Some("")), Since::Initial);
}

#[test]
fn since_numeric_token_is_count() {
	assert_eq!(Since::from(Some("123")), Since::Count(123));
	assert_eq!(Since::from(Some("0")), Since::Count(0));
	assert_eq!(Since::from(Some("18446744073709551615")), Since::Count(u64::MAX));
}

#[test]
fn since_non_numeric_token_is_foreign() {
	assert_eq!(Since::from(Some("s462025_2419382_0_94074")), Since::Foreign);
	assert_eq!(Since::from(Some("garbage")), Since::Foreign);
	assert_eq!(Since::from(Some("-5")), Since::Foreign);
	assert_eq!(Since::from(Some("18446744073709551616")), Since::Foreign);
}

#[test]
fn busy_round_delivers_reset_zeros() {
	let gates = notification_gates(Some(150), &no_threads(), false, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn busy_round_without_reset_suppresses_zeros() {
	// A cursor at 50 advanced before this window, so it is not a reset.
	let gates = notification_gates(Some(50), &no_threads(), false, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!(!(gates.send_notification_count_filter)(&uint!(0)));
	assert!((gates.send_notification_count_filter)(&uint!(5)));
}

#[test]
fn busy_round_without_cursor_sends_counts() {
	let gates = notification_gates(None, &no_threads(), false, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!(!(gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn quiet_round_with_advanced_cursor_sends_zeros() {
	let gates = notification_gates(Some(150), &no_threads(), true, false, SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn quiet_round_with_stale_cursor_sends_nothing() {
	// A cursor at 50 advanced before this window, so nothing changed.
	let gates = notification_gates(Some(50), &no_threads(), true, false, SINCE);

	assert!(!gates.send_notification_counts);
	assert!(!(gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn quiet_round_without_cursor_or_timeline_sends_nothing() {
	let gates = notification_gates(None, &no_threads(), true, false, SINCE);

	assert!(!gates.send_notification_counts);
	assert!(!(gates.send_notification_count_filter)(&uint!(0)));
	assert!((gates.send_notification_count_filter)(&uint!(5)));
}

#[test]
fn quiet_round_without_cursor_after_filtered_timeline_sends_counts() {
	let gates = notification_gates(None, &no_threads(), true, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!(!(gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn quiet_round_with_cursor_in_window_ignores_timeline() {
	let unchanged = notification_gates(Some(150), &no_threads(), true, false, SINCE);
	let changed = notification_gates(Some(150), &no_threads(), true, true, SINCE);

	assert!(unchanged.send_notification_counts);
	assert!(changed.send_notification_counts);
}

#[test]
fn initial_sync_delivers_reset_zeros() {
	// A client can start a fresh token while still caching a count, so an
	// initial round must be able to reconcile it.
	let gates = notification_gates(Some(150), &no_threads(), false, true, INITIAL_SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn initial_sync_without_cursor_sends_counts() {
	let gates = notification_gates(None, &no_threads(), true, true, INITIAL_SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(5)));
}

#[test]
fn cursor_past_the_cutoff_delivers_reset_zeros() {
	// The response cutoff is captured before the cursor is read, so a
	// cursor beyond it repeats the zero until the token catches up.
	let gates = notification_gates(Some(NEXT_BATCH + 1), &no_threads(), false, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn thread_only_reset_delivers_zeros() {
	let root = EventId::parse("$thread:example.org").unwrap();
	let thread_reads = BTreeMap::from([(root, 150)]);

	let gates = notification_gates(None, &thread_reads, false, true, SINCE);

	assert!(gates.send_notification_counts);
	assert!((gates.send_notification_count_filter)(&uint!(0)));
}

#[test]
fn quiet_round_keeps_unchanged_thread_counts() {
	let unread = EventId::parse("$unread:example.org").unwrap();
	let reset = EventId::parse("$reset:example.org").unwrap();
	let idle = EventId::parse("$idle:example.org").unwrap();
	let thread_counts =
		BTreeMap::from([(unread.clone(), (1, 1)), (reset.clone(), (0, 0)), (idle, (0, 0))]);

	// Only the reset thread's cursor advanced this round.
	let threads = assemble_thread_notifications(Some(thread_counts), |root| *root == reset);

	assert_eq!(threads.keys().collect::<Vec<_>>(), [&reset, &unread]);
	assert_eq!(threads[&unread].notification_count, Some(uint!(1)));
	assert_eq!(threads[&unread].highlight_count, Some(uint!(1)));
	assert_eq!(threads[&reset].notification_count, Some(uint!(0)));
}

#[test]
fn thread_counts_never_report_an_unchanged_room() {
	let unread = EventId::parse("$unread:example.org").unwrap();
	let roots = [&unread];
	let none_admitted = |_: &OwnedEventId| false;
	let admitted = |root: &OwnedEventId| *root == unread;

	let (reported, unreported) = (true, false);

	assert!(!send_thread_counts(unreported, roots, none_admitted));
	assert!(!send_thread_counts(unreported, [], admitted));
	assert!(send_thread_counts(unreported, roots, admitted));
	assert!(send_thread_counts(reported, roots, none_admitted));
	assert!(send_thread_counts(reported, [], none_admitted));
}

#[test]
fn thread_totals_merge_only_without_opt_in() {
	let root = EventId::parse("$thread:example.org").unwrap();
	let thread_counts = BTreeMap::from([(root, (2, 1))]);
	let unread = |want_thread_unread| {
		assemble_unread_notifications(
			Some(uint!(3)),
			Some(uint!(0)),
			Some(&thread_counts),
			|_: &UInt| true,
			want_thread_unread,
		)
	};

	let merged = unread(false);
	let partitioned = unread(true);

	assert_eq!(merged.notification_count, Some(uint!(5)));
	assert_eq!(merged.highlight_count, Some(uint!(1)));
	assert_eq!(partitioned.notification_count, Some(uint!(3)));
	assert_eq!(partitioned.highlight_count, Some(uint!(0)));
}

#[test]
fn quiet_counts_stay_quiet_when_all_zero() {
	let nonzero = |count: &UInt| *count != uint!(0);
	let thread_counts = BTreeMap::new();

	let reported = quiet_counts_reported(
		Some(uint!(0)),
		Some(uint!(0)),
		Some(&thread_counts),
		nonzero,
		false,
		|_: &OwnedEventId| false,
	);

	assert!(!reported);
}

#[test]
fn quiet_counts_report_a_nonzero_room_count() {
	let nonzero = |count: &UInt| *count != uint!(0);

	let reported = quiet_counts_reported(
		Some(uint!(1)),
		Some(uint!(0)),
		None,
		nonzero,
		false,
		|_: &OwnedEventId| false,
	);

	assert!(reported);
}

#[test]
fn quiet_counts_ignore_unadmitted_thread_counts() {
	let nonzero = |count: &UInt| *count != uint!(0);
	let root = EventId::parse("$thread:example.org").unwrap();
	let thread_counts = BTreeMap::from([(root, (2, 1))]);

	let reported = quiet_counts_reported(
		Some(uint!(0)),
		Some(uint!(0)),
		Some(&thread_counts),
		nonzero,
		true,
		|_: &OwnedEventId| false,
	);

	assert!(!reported);
}

#[test]
fn quiet_counts_report_an_admitted_thread() {
	let nonzero = |count: &UInt| *count != uint!(0);
	let root = EventId::parse("$thread:example.org").unwrap();
	let thread_counts = BTreeMap::from([(root.clone(), (0, 0))]);

	let reported = quiet_counts_reported(
		Some(uint!(0)),
		Some(uint!(0)),
		Some(&thread_counts),
		nonzero,
		true,
		|candidate: &OwnedEventId| *candidate == root,
	);

	assert!(reported);
}

#[test]
fn quiet_counts_merge_thread_totals_without_opt_in() {
	let nonzero = |count: &UInt| *count != uint!(0);
	let root = EventId::parse("$thread:example.org").unwrap();
	let thread_counts = BTreeMap::from([(root, (2, 1))]);

	let reported = quiet_counts_reported(
		Some(uint!(0)),
		Some(uint!(0)),
		Some(&thread_counts),
		nonzero,
		false,
		|_: &OwnedEventId| false,
	);

	assert!(reported);
}

fn notification_gates(
	last_read: Option<u64>,
	thread_reads: &BTreeMap<OwnedEventId, u64>,
	quiet_round: bool,
	timeline_changed: bool,
	since: u64,
) -> NotificationGates<impl Fn(&UInt) -> bool + use<>> {
	compute_notification_gates(
		GateInputs {
			last_notification_read: last_read,
			thread_last_reads: thread_reads,
			quiet_round,
			timeline_changed,
			since,
		},
		in_window(since, NEXT_BATCH),
	)
}

fn no_threads() -> BTreeMap<OwnedEventId, u64> { BTreeMap::new() }

#[test]
fn initial_cap_is_off_when_the_threshold_is_zero() {
	assert_eq!(capped_timeline_limit(5000, 0, 1, Some(uint!(20))), None);
}

#[test]
fn initial_cap_applies_only_above_the_threshold() {
	assert_eq!(capped_timeline_limit(1000, 1000, 1, Some(uint!(20))), None);
	assert_eq!(capped_timeline_limit(1001, 1000, 1, Some(uint!(20))), Some(uint!(1)));
}

#[test]
fn initial_cap_keeps_a_smaller_client_limit() {
	assert_eq!(capped_timeline_limit(1001, 1000, 5, Some(uint!(3))), None);
	assert_eq!(capped_timeline_limit(1001, 1000, 5, Some(uint!(5))), None);
	assert_eq!(capped_timeline_limit(1001, 1000, 5, Some(uint!(0))), None);
	assert_eq!(capped_timeline_limit(1001, 1000, 5, Some(uint!(6))), Some(uint!(5)));
}

#[test]
fn initial_cap_without_a_client_limit_never_raises_the_default() {
	assert_eq!(capped_timeline_limit(1001, 1000, 1, None), Some(uint!(1)));
	assert_eq!(capped_timeline_limit(1001, 1000, 50, None), None);
}

#[test]
fn initial_cap_of_zero_acts_as_one() {
	assert_eq!(capped_timeline_limit(1001, 1000, 0, Some(uint!(20))), Some(uint!(1)));
}

#[test]
fn since_is_stale_only_past_the_gap() {
	assert!(!stale_since(1000, 900, 100));
	assert!(stale_since(1000, 899, 100));
	assert!(!stale_since(1000, 1000, 100));
}

#[test]
fn since_is_never_stale_with_a_zero_gap() {
	assert!(!stale_since(1_000_000, 0, 0));
}

#[test]
fn since_from_the_future_is_not_stale() {
	assert!(!stale_since(1000, 5000, 100));
	assert!(!stale_since(0, u64::MAX, 1));
}

#[test]
fn every_fastpath_verdict_has_its_own_label() {
	let verdicts = [
		Verdict::Off,
		Verdict::Quiet,
		Verdict::Busy(Busy::Timeline),
		Verdict::Busy(Busy::NoTimelineCount),
		Verdict::Busy(Busy::Typing),
		Verdict::Busy(Busy::PrivateRead),
		Verdict::Busy(Busy::AccountData),
		Verdict::Busy(Busy::Receipts),
		Verdict::Busy(Busy::Notifications),
		Verdict::Busy(Busy::Error),
	];

	let registry = Registry::new();
	for verdict in verdicts {
		registry.sync.v3_rooms(verdict.label(), 1);
	}

	let text = registry.render();
	let counted = text
		.lines()
		.filter(|line| line.starts_with("tuwunel_sync_v3_rooms_total{"))
		.collect::<Vec<_>>();

	assert_eq!(counted.len(), verdicts.len(), "a label is shared or missing: {counted:?}");
	assert!(
		counted.iter().all(|line| line.ends_with(" 1")),
		"each verdict counts in its own series: {counted:?}"
	);
}
