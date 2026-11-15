use std::{collections::HashSet, sync::Arc};

use futures::StreamExt;
use ruma::{
	DeviceId, MilliSecondsSinceUnixEpoch, OwnedRoomId, RoomId, RoomVersionId, UserId, device_id,
	event_id,
	events::{
		RoomAccountDataEventType,
		receipt::{ReceiptEvent, ReceiptThread},
		room::{create::RoomCreateEventContent, member::MembershipState},
	},
	room_id, user_id,
};
use serde_json::{from_value, json};
use tuwunel_core::{
	Result,
	config::Figment,
	metrics::{Metrics, prom::JournalKind},
	utils::time::now_millis,
};
use tuwunel_matrix::pdu::PduBuilder;

use super::super::{Journal, RoomMemo, Scan, journal::counter_at_start};
use crate::{
	Services,
	globals::Data,
	rooms::read_receipt::PrivateRead,
	test_utils::{fixture, membership_update},
	users::DeviceListChange,
};

fn alice() -> &'static UserId { user_id!("@alice:localhost") }
fn bob() -> &'static UserId { user_id!("@bob:localhost") }
fn remote() -> &'static UserId { user_id!("@typist:remote.example") }
fn laptop() -> &'static DeviceId { device_id!("LAPTOP") }
fn phone() -> &'static DeviceId { device_id!("PHONE") }
fn room() -> &'static RoomId { room_id!("!journaled:localhost") }
fn other_room() -> &'static RoomId { room_id!("!untouched:localhost") }
fn third_room() -> &'static RoomId { room_id!("!third:localhost") }

fn journal(capacity: usize, horizon: u64) -> (Journal, Arc<Metrics>) {
	let metrics = Metrics::new(None);

	(Journal::new(capacity, horizon, metrics.clone()), metrics)
}

fn rooms(journal: &Journal, user: &UserId, since: u64) -> Option<HashSet<OwnedRoomId>> {
	journal.changed_rooms(user, since)
}

fn set(rooms: &[&RoomId]) -> HashSet<OwnedRoomId> {
	rooms
		.iter()
		.map(|room| (*room).to_owned())
		.collect()
}

#[test]
fn a_since_below_the_horizon_has_no_answer() {
	let (journal, _) = journal(10, 5);

	journal.record(6, JournalKind::Timeline, room(), None);

	assert_eq!(rooms(&journal, alice(), 4), None);
	assert_eq!(rooms(&journal, alice(), 5), Some(set(&[room()])));
	assert_eq!(rooms(&journal, alice(), 6), Some(set(&[])));
}

#[test]
fn a_change_for_one_user_answers_only_for_that_user() {
	let (journal, _) = journal(10, 0);

	journal.record(3, JournalKind::Timeline, room(), Some(alice()));

	assert_eq!(rooms(&journal, alice(), 2), Some(set(&[room()])));
	assert_eq!(rooms(&journal, bob(), 2), Some(set(&[])));

	journal.record(4, JournalKind::Timeline, other_room(), None);

	assert_eq!(rooms(&journal, bob(), 2), Some(set(&[other_room()])));
	assert_eq!(rooms(&journal, alice(), 3), Some(set(&[other_room()])));
}

#[test]
fn eviction_bounds_the_journal_and_raises_the_horizon() {
	let (journal, metrics) = journal(3, 0);
	let registry = metrics.enable_prometheus();

	for count in 1..=5 {
		journal.record(count, JournalKind::Timeline, room(), None);
	}

	assert_eq!(journal.len(), 3);
	assert_eq!(journal.horizon(), 2);
	assert_eq!(registry.sync.journal_held(), 3);
	assert_eq!(registry.sync.journal_evicted_total(), 2);
	assert_eq!(rooms(&journal, alice(), 1), None);
	assert_eq!(rooms(&journal, alice(), 2), Some(set(&[room()])));
}

#[test]
fn eviction_drops_a_whole_number_shared_by_several_rooms() {
	let (journal, metrics) = journal(3, 0);
	let registry = metrics.enable_prometheus();
	let third = room_id!("!third:localhost");

	journal.record(1, JournalKind::Timeline, room(), None);

	for shared in [room(), other_room(), third] {
		journal.record(2, JournalKind::Timeline, shared, None);
	}

	assert_eq!(journal.len(), 3);
	assert_eq!(journal.horizon(), 1);

	journal.record(3, JournalKind::Timeline, room(), None);

	assert_eq!(journal.len(), 1);
	assert_eq!(journal.horizon(), 2);
	assert_eq!(registry.sync.journal_held(), 1);
	assert_eq!(registry.sync.journal_evicted_total(), 4);
	assert_eq!(rooms(&journal, alice(), 1), None);
	assert_eq!(rooms(&journal, alice(), 2), Some(set(&[room()])));
}

#[test]
fn a_capacity_of_zero_turns_the_journal_off() {
	let (journal, _) = journal(0, 0);

	journal.record(1, JournalKind::Timeline, room(), None);

	assert!(journal.is_empty());
	assert_eq!(rooms(&journal, alice(), 0), None);
}

#[test]
fn a_miss_is_counted_only_when_the_journal_answers_no() {
	let (journal, metrics) = journal(10, 5);
	let registry = metrics.enable_prometheus();

	journal.record(7, JournalKind::Timeline, room(), None);

	let held = journal.changed_rooms(alice(), 6);
	let stale = journal.changed_rooms(alice(), 4);

	assert!(!journal.audit(JournalKind::Timeline, room(), alice(), 6, held.as_ref()));
	assert!(!journal.audit(JournalKind::Timeline, room(), alice(), 4, stale.as_ref()));
	assert!(!journal.audit(JournalKind::Timeline, other_room(), alice(), 4, stale.as_ref()));
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
		0
	);

	assert!(journal.audit(JournalKind::Typing, other_room(), alice(), 6, held.as_ref()));
	assert_eq!(registry.sync.journal_missed(JournalKind::Typing), 1);
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
		0
	);
}

#[test]
fn a_room_changed_after_the_snapshot_is_no_miss() {
	let (journal, metrics) = journal(10, 0);
	let registry = metrics.enable_prometheus();

	let snapshot = journal.changed_rooms(alice(), 1);

	journal.record(2, JournalKind::Timeline, room(), None);
	journal.record(3, JournalKind::Timeline, other_room(), Some(bob()));

	assert!(!journal.audit(JournalKind::Timeline, room(), alice(), 1, snapshot.as_ref()));
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
		0
	);

	assert!(journal.audit(JournalKind::Timeline, other_room(), alice(), 1, snapshot.as_ref()));
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
		1
	);
}

fn memo(known: &[(&RoomId, bool)]) -> RoomMemo {
	let mut memo = RoomMemo::default();

	for (room, joined) in known {
		memo.insert((*room).to_owned(), *joined);
	}

	memo
}

fn scan(
	journal: &Journal,
	device: Option<&DeviceId>,
	window: (u64, u64),
	memo: &RoomMemo,
) -> Scan {
	journal.scan(alice(), device, window.0, window.1, memo, 32)
}

fn resolve(rooms: &[&RoomId], overflow: bool) -> Scan {
	Scan::Resolve {
		rooms: rooms
			.iter()
			.map(|room| (*room).to_owned())
			.collect(),
		overflow,
	}
}

#[test]
fn changes_without_a_room_and_of_new_kinds_are_no_candidates() {
	let (journal, _) = journal(10, 0);

	journal.record(1, JournalKind::Timeline, room(), None);
	journal.record_user(2, JournalKind::OwnKeys, alice(), None);
	journal.record_user(3, JournalKind::Timeline, alice(), None);
	journal.record(4, JournalKind::Membership, other_room(), Some(alice()));
	journal.record(5, JournalKind::Profile, third_room(), None);
	journal.record(6, JournalKind::Profile, room(), None);

	assert_eq!(rooms(&journal, alice(), 0), Some(set(&[room()])));
	assert_eq!(rooms(&journal, alice(), 1), Some(set(&[])));
	assert_eq!(journal.changed_since(room(), alice(), 0), Some(true));
	assert_eq!(journal.changed_since(room(), alice(), 1), Some(false));
	assert_eq!(journal.changed_since(other_room(), alice(), 0), Some(false));
	assert_eq!(journal.changed_since(third_room(), alice(), 0), Some(false));
}

#[test]
fn a_scan_looks_only_at_the_window_after_and_up_to() {
	let (journal, _) = journal(10, 0);
	let none = RoomMemo::default();

	journal.record_user(5, JournalKind::OwnKeys, alice(), None);

	assert_eq!(scan(&journal, None, (4, 5), &none), Scan::Wake(JournalKind::OwnKeys));
	assert_eq!(scan(&journal, None, (0, 9), &none), Scan::Wake(JournalKind::OwnKeys));
	assert_eq!(scan(&journal, None, (5, 9), &none), Scan::Quiet);
	assert_eq!(scan(&journal, None, (2, 4), &none), Scan::Quiet);
	assert_eq!(scan(&journal, None, (5, 5), &none), Scan::Quiet);
	assert_eq!(scan(&journal, None, (6, 5), &none), Scan::Quiet);
}

#[test]
fn a_scan_wakes_on_a_change_of_its_user_and_passes_over_another() {
	let (journal, _) = journal(10, 0);
	let none = RoomMemo::default();

	journal.record(1, JournalKind::Notifications, room(), Some(bob()));

	assert_eq!(scan(&journal, None, (0, 1), &none), Scan::Quiet);

	journal.record(2, JournalKind::AccountData, room(), Some(alice()));

	assert_eq!(scan(&journal, None, (0, 2), &none), Scan::Wake(JournalKind::AccountData));
	assert_eq!(
		journal.scan(bob(), None, 0, 2, &none, 32),
		Scan::Wake(JournalKind::Notifications)
	);
}

#[test]
fn a_scan_passes_over_a_change_of_another_device_and_wakes_on_its_own() {
	let (journal, _) = journal(10, 0);
	let none = RoomMemo::default();

	journal.record_user(1, JournalKind::ToDevice, alice(), Some(phone()));

	assert_eq!(scan(&journal, Some(laptop()), (0, 1), &none), Scan::Quiet);
	assert_eq!(scan(&journal, None, (0, 1), &none), Scan::Quiet);
	assert_eq!(scan(&journal, Some(phone()), (0, 1), &none), Scan::Wake(JournalKind::ToDevice));

	journal.record_user(2, JournalKind::OneTimeKeys, alice(), None);

	assert_eq!(
		scan(&journal, Some(laptop()), (0, 2), &none),
		Scan::Wake(JournalKind::OneTimeKeys)
	);
	assert_eq!(scan(&journal, None, (0, 2), &none), Scan::Wake(JournalKind::OneTimeKeys));
}

#[test]
fn a_scan_wakes_on_a_joined_room_passes_over_a_foreign_one_and_resolves_the_unknown() {
	let (journal, _) = journal(10, 0);

	journal.record(1, JournalKind::Typing, room(), None);
	journal.record(2, JournalKind::Timeline, room(), None);
	journal.record(3, JournalKind::Receipts, other_room(), None);

	assert_eq!(
		scan(&journal, None, (0, 3), &memo(&[])),
		resolve(&[room(), other_room()], false)
	);
	assert_eq!(
		scan(&journal, None, (0, 3), &memo(&[(room(), true)])),
		Scan::Wake(JournalKind::Typing)
	);
	assert_eq!(
		scan(&journal, None, (0, 3), &memo(&[(room(), false)])),
		resolve(&[other_room()], false)
	);
	assert_eq!(
		scan(&journal, None, (0, 3), &memo(&[(room(), false), (other_room(), false)])),
		Scan::Quiet
	);
	assert_eq!(
		scan(&journal, None, (0, 3), &memo(&[(room(), false), (other_room(), true)])),
		Scan::Wake(JournalKind::Receipts)
	);
}

#[test]
fn a_scan_wakes_on_a_later_change_of_its_user_before_it_resolves() {
	let (journal, _) = journal(10, 0);

	journal.record(1, JournalKind::Timeline, room(), None);
	journal.record_user(2, JournalKind::Membership, alice(), None);

	assert_eq!(scan(&journal, None, (0, 2), &memo(&[])), Scan::Wake(JournalKind::Membership));
}

#[test]
fn a_scan_overflows_with_one_more_unknown_room_than_the_limit() {
	let (journal, _) = journal(10, 0);
	let none = RoomMemo::default();

	for (count, changed) in [(1, room()), (2, other_room()), (3, third_room())] {
		journal.record(count, JournalKind::Timeline, changed, None);
	}

	assert_eq!(
		journal.scan(alice(), None, 0, 3, &none, 3),
		resolve(&[room(), other_room(), third_room()], false)
	);

	let Scan::Resolve { rooms, overflow } = journal.scan(alice(), None, 0, 3, &none, 2) else {
		panic!("a scan with more unknown rooms than the limit resolves");
	};

	assert!(overflow);
	assert_eq!(rooms.len(), 3);
}

#[test]
fn a_scan_with_a_complete_memo_passes_over_the_unknown_rooms() {
	let (journal, _) = journal(10, 0);
	let mut known = memo(&[(other_room(), true)]);

	journal.record(1, JournalKind::Timeline, room(), None);

	assert_eq!(scan(&journal, None, (0, 1), &known), resolve(&[room()], false));

	known.mark_complete();

	assert_eq!(scan(&journal, None, (0, 1), &known), Scan::Quiet);

	journal.record(2, JournalKind::Typing, other_room(), None);

	assert_eq!(scan(&journal, None, (0, 2), &known), Scan::Wake(JournalKind::Typing));
}

#[test]
fn a_scan_from_below_the_horizon_answers_with_the_horizon() {
	let (journal, _) = journal(2, 5);
	let none = RoomMemo::default();

	assert_eq!(scan(&journal, None, (4, 9), &none), Scan::Horizon(5));
	assert_eq!(scan(&journal, None, (4, 4), &none), Scan::Horizon(5));
	assert_eq!(scan(&journal, None, (5, 9), &none), Scan::Quiet);

	for count in 6..=8 {
		journal.record_user(count, JournalKind::OwnKeys, bob(), None);
	}

	assert_eq!(scan(&journal, None, (5, 9), &none), Scan::Horizon(6));
	assert_eq!(scan(&journal, None, (6, 9), &none), Scan::Quiet);
}

#[test]
fn every_stored_change_is_counted_by_kind_and_a_journal_that_is_off_counts_none() {
	let (on, metrics) = journal(10, 0);
	let registry = metrics.enable_prometheus();

	on.record(1, JournalKind::Timeline, room(), None);
	on.record(1, JournalKind::Timeline, other_room(), None);
	on.record_user(2, JournalKind::ToDevice, alice(), Some(phone()));

	assert_eq!(
		registry
			.sync
			.journal_recorded_total(JournalKind::Timeline),
		2
	);
	assert_eq!(
		registry
			.sync
			.journal_recorded_total(JournalKind::ToDevice),
		1
	);
	assert_eq!(
		registry
			.sync
			.journal_recorded_total(JournalKind::Typing),
		0
	);

	let (off, metrics) = journal(0, 0);
	let registry = metrics.enable_prometheus();

	off.record(1, JournalKind::Timeline, room(), None);
	off.record_user(2, JournalKind::ToDevice, alice(), None);

	assert_eq!(
		registry
			.sync
			.journal_recorded_total(JournalKind::Timeline),
		0
	);
	assert_eq!(
		registry
			.sync
			.journal_recorded_total(JournalKind::ToDevice),
		0
	);
}

#[tokio::test]
async fn the_counter_at_start_is_read_strictly() -> Result {
	with_services(async |services| {
		let map = &services.db["server_signingkeys"];

		assert_eq!(counter_at_start(map)?, 0);

		map.insert(Data::COUNTER, 41_u64.to_be_bytes());
		assert_eq!(counter_at_start(map)?, 41);

		map.insert(Data::COUNTER, [1_u8, 2, 3]);
		counter_at_start(map).unwrap_err();

		Ok(())
	})
	.await
}

#[tokio::test]
async fn appending_a_pdu_journals_its_room_for_everyone_but_the_sender_resets() -> Result {
	with_services(async |services| {
		let state_lock = services.state.mutex.lock(room()).await;
		let create = RoomCreateEventContent {
			room_version: RoomVersionId::V11,
			..RoomCreateEventContent::new_v11()
		};

		services
			.short
			.get_or_create_shortroomid(room())
			.await;
		let event_id = services
			.timeline
			.build_and_append_pdu(PduBuilder::state("", &create), alice(), room(), &state_lock)
			.await?;
		let number = services
			.timeline
			.get_pdu_count(&event_id)
			.await?
			.into_unsigned();

		assert_boundary(services, bob(), number, room(), true);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn typing_start_stop_and_expiry_each_journal_the_room() -> Result {
	with_services(async |services| {
		let typing = &services.typing;

		typing
			.typing_add(remote(), room(), now_millis().saturating_add(60_000))
			.await?;
		assert_for_everyone(services, typing.last_typing_update(room()).await?);

		typing.typing_remove(remote(), room()).await?;
		assert_for_everyone(services, typing.last_typing_update(room()).await?);

		typing
			.typing_add(remote(), room(), now_millis().saturating_sub(1))
			.await?;
		assert_for_everyone(services, typing.last_typing_update(room()).await?);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_public_receipt_journals_its_room_for_everyone() -> Result {
	with_services(async |services| {
		let receipt: ReceiptEvent = from_value(json!({
			"type": "m.receipt",
			"room_id": room(),
			"content": { "$read:localhost": { "m.read": { alice().as_str(): { "ts": 1 } } } },
		}))?;

		assert!(
			services
				.read_receipt
				.readreceipt_update(alice(), room(), &receipt)
				.await
		);

		let number = services
			.read_receipt
			.readreceipts_since(room(), 0, None)
			.map(|(_, count, _)| count)
			.fold(0, async |latest, count| latest.max(count))
			.await;

		assert_for_everyone(services, number);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn an_announced_private_read_journals_its_room_for_its_user() -> Result {
	with_services(async |services| {
		let advanced = services
			.read_receipt
			.private_read_set(PrivateRead {
				room_id: room(),
				user_id: alice(),
				count: 5,
				ts: MilliSecondsSinceUnixEpoch::now(),
				thread: &ReceiptThread::Unthreaded,
				announce: true,
			})
			.await;

		assert!(advanced);

		let number = services
			.read_receipt
			.last_privateread_update(alice(), room())
			.await;

		assert_for_alice_only(services, number);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn room_account_data_journals_for_its_owner_and_global_data_does_not() -> Result {
	with_services(async |services| {
		let account_data = &services.account_data;
		let kind = RoomAccountDataEventType::from("m.test");
		let data = json!({ "type": "m.test", "content": {} });

		let held = services.sync.journal.len();
		account_data
			.update(None, alice(), kind.clone(), &data)
			.await?;
		assert_eq!(services.sync.journal.len(), held);

		account_data
			.update(Some(room()), alice(), kind, &data)
			.await?;

		let number = account_data
			.last_count(Some(room()), alice(), None)
			.await?;

		assert_for_alice_only(services, number);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn notification_resets_journal_the_room_for_their_user() -> Result {
	with_services(async |services| {
		let pusher = &services.pusher;
		let root = event_id!("$root:localhost");

		pusher
			.reset_notification_counts(alice(), room())
			.await;
		assert_for_alice_only(
			services,
			pusher
				.last_notification_read(alice(), room())
				.await?,
		);

		pusher
			.reset_thread_notification_counts(alice(), other_room(), root)
			.await;

		let number = pusher
			.last_thread_notification_read(alice(), other_room(), root)
			.await?;

		assert_boundary(services, alice(), number, other_room(), true);
		assert_boundary(services, bob(), number, other_room(), false);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_device_list_change_journals_each_joined_room() -> Result {
	with_services(async |services| {
		let update = membership_update(room(), alice(), MembershipState::Join, 1);
		services
			.state_cache
			.update_membership(update)
			.await?;

		services
			.users
			.mark_device_key_update(alice(), DeviceListChange::Resync)
			.await;

		let number = services
			.users
			.room_keys_changed(room(), 0, None)
			.map(|(_, count)| count)
			.fold(0, async |latest, count| latest.max(count))
			.await;

		assert_for_everyone(services, number);

		Ok(())
	})
	.await
}

async fn with_services<Test>(test: Test) -> Result
where
	Test: AsyncFnOnce(&Services) -> Result,
{
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	test(&fixture.services).await
}

fn assert_boundary(
	services: &Services,
	user: &UserId,
	number: u64,
	changed: &RoomId,
	held: bool,
) {
	let journal = &services.sync.journal;
	let before = journal
		.changed_rooms(user, number.saturating_sub(1))
		.expect("the journal covers the write");
	let at = journal
		.changed_rooms(user, number)
		.expect("the journal covers the write");

	assert_eq!(before.contains(changed), held, "{user} before {number}");
	assert!(!at.contains(changed), "{user} at {number}");
}

fn assert_for_everyone(services: &Services, number: u64) {
	assert_boundary(services, alice(), number, room(), true);
	assert_boundary(services, bob(), number, room(), true);
	assert_boundary(services, alice(), number, other_room(), false);
}

fn assert_for_alice_only(services: &Services, number: u64) {
	assert_boundary(services, alice(), number, room(), true);
	assert_boundary(services, bob(), number, room(), false);
	assert_boundary(services, alice(), number, other_room(), false);
}
