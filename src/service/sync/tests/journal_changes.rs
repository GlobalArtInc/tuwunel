use std::collections::HashSet;

use ruma::{DeviceId, OwnedRoomId, RoomId, UserId, device_id, room_id, user_id};
use tuwunel_core::metrics::{Metrics, prom::JournalKind};

use super::super::{Journal, journal::Changes};

const ROOM_KINDS: [JournalKind; 7] = [
	JournalKind::Timeline,
	JournalKind::Typing,
	JournalKind::PrivateRead,
	JournalKind::AccountData,
	JournalKind::Receipts,
	JournalKind::Notifications,
	JournalKind::DeviceLists,
];

fn alice() -> &'static UserId { user_id!("@alice:localhost") }
fn bob() -> &'static UserId { user_id!("@bob:localhost") }
fn laptop() -> &'static DeviceId { device_id!("LAPTOP") }
fn room() -> &'static RoomId { room_id!("!first:localhost") }
fn other_room() -> &'static RoomId { room_id!("!second:localhost") }
fn third_room() -> &'static RoomId { room_id!("!third:localhost") }

fn journal(capacity: usize, horizon: u64) -> Journal {
	Journal::new(capacity, horizon, Metrics::new(None))
}

fn set(rooms: &[&RoomId]) -> HashSet<OwnedRoomId> {
	rooms
		.iter()
		.map(|room| (*room).to_owned())
		.collect()
}

fn changes(rooms: &[&RoomId], own_membership: &[&RoomId], global_account_data: bool) -> Changes {
	Changes {
		rooms: set(rooms),
		own_membership: set(own_membership),
		global_account_data,
	}
}

#[test]
fn a_room_kind_for_everyone_or_for_the_user_puts_its_room_in_rooms() {
	for kind in ROOM_KINDS {
		let journal = journal(10, 0);

		journal.record(1, kind, room(), None);
		journal.record(2, kind, other_room(), Some(alice()));

		assert_eq!(
			journal.changes(alice(), 0, 2, 10),
			Some(changes(&[room(), other_room()], &[], false)),
			"{kind:?}"
		);
	}
}

#[test]
fn a_room_kind_for_another_user_is_left_out() {
	for kind in ROOM_KINDS {
		let journal = journal(10, 0);

		journal.record(1, kind, room(), Some(bob()));

		assert_eq!(
			journal.changes(alice(), 0, 1, 10),
			Some(changes(&[], &[], false)),
			"{kind:?}"
		);
		assert_eq!(journal.changes(bob(), 0, 1, 10), Some(changes(&[room()], &[], false)));
	}
}

#[test]
fn a_profile_with_a_room_is_a_room_and_one_without_is_not() {
	let journal = journal(10, 0);

	journal.record(1, JournalKind::Profile, room(), None);
	journal.record_user(2, JournalKind::Profile, alice(), None);

	assert_eq!(journal.changes(alice(), 0, 2, 10), Some(changes(&[room()], &[], false)));
	assert_eq!(journal.changed_rooms(alice(), 0), Some(set(&[])));
}

#[test]
fn the_own_membership_is_reported_apart_and_another_is_left_out() {
	let journal = journal(10, 0);

	journal.record(1, JournalKind::Membership, room(), Some(alice()));
	journal.record(2, JournalKind::Membership, other_room(), Some(bob()));

	assert_eq!(journal.changes(alice(), 0, 2, 10), Some(changes(&[], &[room()], false)));
	assert_eq!(journal.changes(bob(), 0, 2, 10), Some(changes(&[], &[other_room()], false)));
}

#[test]
fn the_global_account_data_of_the_user_sets_the_flag() {
	let journal = journal(10, 0);

	journal.record_user(1, JournalKind::GlobalAccountData, bob(), None);

	assert_eq!(journal.changes(alice(), 0, 1, 10), Some(changes(&[], &[], false)));

	journal.record_user(2, JournalKind::GlobalAccountData, alice(), None);

	assert_eq!(journal.changes(alice(), 0, 2, 10), Some(changes(&[], &[], true)));
	assert_eq!(journal.changes(alice(), 1, 2, 10), Some(changes(&[], &[], true)));
}

#[test]
fn kinds_of_no_room_are_left_out() {
	let journal = journal(10, 0);

	for (count, kind) in [
		(1, JournalKind::OwnKeys),
		(2, JournalKind::OneTimeKeys),
		(3, JournalKind::ToDevice),
	] {
		journal.record_user(count, kind, alice(), Some(laptop()));
	}

	assert_eq!(journal.changes(alice(), 0, 3, 10), Some(changes(&[], &[], false)));
}

#[test]
fn the_window_excludes_after_and_includes_upto() {
	let journal = journal(10, 0);

	journal.record(3, JournalKind::Timeline, room(), None);
	journal.record(4, JournalKind::Timeline, other_room(), None);
	journal.record(5, JournalKind::Timeline, third_room(), None);

	assert_eq!(
		journal.changes(alice(), 3, 5, 10),
		Some(changes(&[other_room(), third_room()], &[], false))
	);
	assert_eq!(journal.changes(alice(), 3, 4, 10), Some(changes(&[other_room()], &[], false)));
	assert_eq!(journal.changes(alice(), 2, 3, 10), Some(changes(&[room()], &[], false)));
}

#[test]
fn an_empty_window_is_quiet_and_does_not_panic() {
	let journal = journal(10, 0);

	journal.record(3, JournalKind::Timeline, room(), None);

	assert_eq!(journal.changes(alice(), 3, 3, 10), Some(changes(&[], &[], false)));
	assert_eq!(journal.changes(alice(), 7, 3, 10), Some(changes(&[], &[], false)));
}

#[test]
fn an_after_below_the_horizon_has_no_answer() {
	let journal = journal(10, 5);

	journal.record(6, JournalKind::Timeline, room(), None);

	assert_eq!(journal.changes(alice(), 4, 6, 10), None);
	assert_eq!(journal.changes(alice(), 5, 6, 10), Some(changes(&[room()], &[], false)));
	assert_eq!(journal.changes(alice(), 4, 4, 10), None);
}

#[test]
fn an_eviction_raises_the_horizon_past_after() {
	let journal = journal(2, 0);

	for count in 1..=4 {
		journal.record(count, JournalKind::Timeline, room(), None);
	}

	assert_eq!(journal.horizon(), 2);
	assert_eq!(journal.changes(alice(), 1, 4, 10), None);
	assert_eq!(journal.changes(alice(), 2, 4, 10), Some(changes(&[room()], &[], false)));
}

#[test]
fn more_rooms_than_the_limit_have_no_answer() {
	let journal = journal(10, 0);

	journal.record(1, JournalKind::Timeline, room(), None);
	journal.record(2, JournalKind::Timeline, other_room(), None);
	journal.record(3, JournalKind::Timeline, third_room(), None);

	assert_eq!(
		journal.changes(alice(), 0, 3, 3),
		Some(changes(&[room(), other_room(), third_room()], &[], false))
	);
	assert_eq!(journal.changes(alice(), 0, 3, 2), None);
	assert_eq!(
		journal.changes(alice(), 0, 2, 2),
		Some(changes(&[room(), other_room()], &[], false))
	);
}

#[test]
fn the_limit_counts_distinct_rooms_over_both_sets() {
	let journal = journal(10, 0);

	for count in 1..=3 {
		journal.record(count, JournalKind::Timeline, room(), None);
	}

	journal.record(4, JournalKind::Membership, room(), Some(alice()));
	journal.record(5, JournalKind::Membership, other_room(), Some(alice()));

	assert_eq!(
		journal.changes(alice(), 0, 5, 2),
		Some(changes(&[room()], &[room(), other_room()], false))
	);
	assert_eq!(journal.changes(alice(), 0, 5, 1), None);
}

#[test]
fn a_limit_of_zero_allows_only_a_window_with_no_room() {
	let journal = journal(10, 0);

	journal.record_user(1, JournalKind::GlobalAccountData, alice(), None);

	assert_eq!(journal.changes(alice(), 0, 1, 0), Some(changes(&[], &[], true)));

	journal.record(2, JournalKind::Timeline, room(), None);

	assert_eq!(journal.changes(alice(), 0, 2, 0), None);
}

#[test]
fn a_journal_with_no_capacity_has_no_answer() {
	let journal = journal(0, 0);

	journal.record(1, JournalKind::Timeline, room(), None);

	assert_eq!(journal.changes(alice(), 0, 1, 10), None);
	assert_eq!(journal.changes(alice(), 0, 0, 10), None);
}

#[test]
fn changed_rooms_answers_as_before() {
	let journal = journal(10, 0);

	journal.record(1, JournalKind::Timeline, room(), None);
	journal.record(2, JournalKind::Profile, other_room(), None);
	journal.record(3, JournalKind::Membership, third_room(), Some(alice()));
	journal.record_user(4, JournalKind::GlobalAccountData, alice(), None);

	assert_eq!(journal.changed_rooms(alice(), 0), Some(set(&[room()])));
	assert_eq!(journal.changed_since(other_room(), alice(), 0), Some(false));
	assert_eq!(
		journal.changes(alice(), 0, 4, 10),
		Some(changes(&[room(), other_room()], &[third_room()], true))
	);
}
