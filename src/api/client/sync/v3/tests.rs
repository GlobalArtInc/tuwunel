use std::{
	cell::Cell,
	collections::{BTreeMap, HashSet},
};

use ruma::{
	EventId, OwnedEventId, OwnedRoomId, UInt,
	api::client::{
		filter::FilterDefinition,
		sync::sync_events::v3::{JoinedRoom, Response, State as RoomState, StateEvents},
	},
	room_id,
	serde::Raw,
	uint,
};
use tuwunel_core::metrics::prom::{JournalKind, Registry, Verdict as WaitVerdict};

use super::{
	Since, StateAfter, capped_timeline_limit,
	joined::{
		fastpath::{Busy, Changed, Verdict},
		notifications::{
			GateInputs, Gates as NotificationGates,
			assemble_threads as assemble_thread_notifications,
			assemble_unread as assemble_unread_notifications,
			compute_gates as compute_notification_gates, in_window, quiet_counts_reported,
			send_thread_counts,
		},
	},
	stale_since,
	sync_events::{
		Wait, keep_joined, missed_rooms, presence_only, timeout_verdict, unaccounted_rooms,
		union_rooms, woken_verdict,
	},
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

	let mut counted = Vec::new();
	for verdict in verdicts {
		let registry = Registry::new();
		registry.sync.v3_rooms(verdict.label(), 1);

		let text = registry.render();
		let rows = text
			.lines()
			.filter(|line| line.starts_with("tuwunel_sync_v3_rooms_total{"))
			.collect::<Vec<_>>();

		let ones = rows
			.iter()
			.filter(|line| line.ends_with(" 1"))
			.collect::<Vec<_>>();

		assert_eq!(ones.len(), 1, "{verdict:?} must count in exactly one series: {rows:?}");
		assert!(
			rows.iter()
				.all(|line| line.ends_with(" 1") || line.ends_with(" 0")),
			"{verdict:?} must leave the other series at zero: {rows:?}"
		);

		counted.push((*ones[0]).to_owned());
	}

	counted.sort_unstable();
	counted.dedup();

	assert_eq!(counted.len(), verdicts.len(), "two verdicts share a label: {counted:?}");
}

fn raw_event<T>() -> Raw<T> {
	Raw::from_json(serde_json::value::to_raw_value(&serde_json::json!({})).unwrap())
}

fn response_with_presence() -> Response {
	let mut response = Response::new(NEXT_BATCH.to_string());
	response.presence.events.push(raw_event());

	response
}

#[test]
fn presence_only_response_is_held() {
	assert!(presence_only(&response_with_presence()));
}

#[test]
fn presence_with_a_room_is_not_held() {
	let mut response = response_with_presence();
	response
		.rooms
		.join
		.insert(room_id!("!room:example.org").to_owned(), JoinedRoom::new());

	assert!(!presence_only(&response));
}

#[test]
fn empty_response_is_not_presence_only() {
	assert!(!presence_only(&Response::new(NEXT_BATCH.to_string())));
}

#[test]
fn presence_with_to_device_is_not_held() {
	let mut response = response_with_presence();
	response.to_device.events.push(raw_event());

	assert!(!presence_only(&response));
}

#[test]
fn only_busy_verdicts_with_a_journaled_cause_are_audited() {
	let audited = [
		(Busy::Timeline, JournalKind::Timeline),
		(Busy::Typing, JournalKind::Typing),
		(Busy::PrivateRead, JournalKind::PrivateRead),
		(Busy::AccountData, JournalKind::AccountData),
		(Busy::Receipts, JournalKind::Receipts),
		(Busy::Notifications, JournalKind::Notifications),
	];

	for (busy, kind) in audited {
		assert_eq!(busy.journal_kind(), Some(kind));
	}

	assert_eq!(Busy::NoTimelineCount.journal_kind(), None);
	assert_eq!(Busy::Error.journal_kind(), None);
}

#[test]
fn the_journal_is_asked_once_per_build_however_many_rooms_ask() {
	let changed = Changed::default();
	let asked = Cell::new(0_u32);
	let ask = || {
		asked.set(asked.get().saturating_add(1));

		Some(HashSet::from([room_id!("!busy:localhost").to_owned()]))
	};

	let first = changed.rooms_with(ask).cloned();
	let second = changed.rooms_with(ask).cloned();

	assert_eq!(asked.get(), 1);
	assert_eq!(first, second);
	assert_eq!(first.map(|rooms| rooms.len()), Some(1));

	let unanswered = Changed::default();
	let none = || -> Option<HashSet<OwnedRoomId>> {
		asked.set(asked.get().saturating_add(1));

		None
	};

	assert_eq!(unanswered.rooms_with(none), None);
	assert_eq!(unanswered.rooms_with(none), None);
	assert_eq!(asked.get(), 2);
}

#[tokio::test]
async fn candidates_are_the_journal_and_typing_rooms_that_match_the_filter_and_are_joined() {
	let room = |id: &str| OwnedRoomId::try_from(id).unwrap();
	let changed =
		HashSet::from([room("!a:localhost"), room("!b:localhost"), room("!hidden:localhost")]);
	let typing = vec![room("!b:localhost"), room("!c:localhost"), room("!left:localhost")];
	let mut filter = FilterDefinition::default();

	filter.room.not_rooms = vec![room("!hidden:localhost")];

	let union = union_rooms(&changed, typing, &filter);

	assert_eq!(
		union,
		HashSet::from([
			room("!a:localhost"),
			room("!b:localhost"),
			room("!c:localhost"),
			room("!left:localhost"),
		])
	);

	let member = HashSet::from([room("!a:localhost"), room("!c:localhost")]);
	let member = &member;
	let mut joined = keep_joined(union, async |room_id| member.contains(&room_id)).await;

	joined.sort();

	assert_eq!(joined, [room("!a:localhost"), room("!c:localhost")]);
}

#[test]
fn only_answered_rooms_outside_the_candidates_are_unaccounted() {
	let room = |id: &str| OwnedRoomId::try_from(id).unwrap();
	let answered = BTreeMap::from([
		(room("!held:localhost"), JoinedRoom::default()),
		(room("!lost:localhost"), JoinedRoom::default()),
	]);
	let candidates = HashSet::from([room("!held:localhost"), room("!other:localhost")]);

	assert_eq!(unaccounted_rooms(&answered, &candidates), [&room("!lost:localhost")]);
	assert!(unaccounted_rooms(&BTreeMap::new(), &candidates).is_empty());
}

#[test]
fn a_room_changed_after_the_snapshot_is_no_miss_unless_the_live_journal_lacks_it() {
	let room = |id: &str| OwnedRoomId::try_from(id).unwrap();
	let answered = BTreeMap::from([
		(room("!held:localhost"), JoinedRoom::default()),
		(room("!late:localhost"), JoinedRoom::default()),
		(room("!lost:localhost"), JoinedRoom::default()),
	]);
	let snapshot = HashSet::from([room("!held:localhost")]);
	let live = HashSet::from([room("!held:localhost"), room("!late:localhost")]);

	assert_eq!(unaccounted_rooms(&answered, &snapshot).len(), 2);
	assert_eq!(missed_rooms(&answered, &snapshot, &live), [&room("!lost:localhost")]);
	assert!(missed_rooms(&answered, &snapshot, &answered.keys().cloned().collect()).is_empty());
}

#[test]
fn the_wait_follows_the_switches_and_only_a_journal_that_is_on() {
	assert_eq!(Wait::select(true, false, false), Wait::Watchers);
	assert_eq!(Wait::select(true, true, false), Wait::Journal);
	assert_eq!(Wait::select(true, false, true), Wait::Verify);
	assert_eq!(Wait::select(true, true, true), Wait::Verify);
	assert_eq!(Wait::select(false, true, false), Wait::Watchers);
	assert_eq!(Wait::select(false, true, true), Wait::Watchers);
}

#[test]
fn a_woken_poll_is_a_miss_only_when_the_journal_is_silent_and_the_build_held_data() {
	assert_eq!(woken_verdict(true, true), WaitVerdict::Both);
	assert_eq!(woken_verdict(true, false), WaitVerdict::Both);
	assert_eq!(woken_verdict(false, true), WaitVerdict::Miss);
	assert_eq!(woken_verdict(false, false), WaitVerdict::WatcherSpurious);
}

#[test]
fn a_poll_that_timed_out_is_judged_by_the_journal_alone() {
	assert_eq!(timeout_verdict(true), WaitVerdict::JournalExtra);
	assert_eq!(timeout_verdict(false), WaitVerdict::Quiet);
}
