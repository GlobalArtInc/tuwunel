use std::{collections::HashSet, sync::Arc};

use futures::StreamExt;
use ruma::{
	MilliSecondsSinceUnixEpoch, OwnedRoomId, RoomId, RoomVersionId, UserId, event_id,
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

use super::super::{Journal, journal::counter_at_start};
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
fn room() -> &'static RoomId { room_id!("!journaled:localhost") }
fn other_room() -> &'static RoomId { room_id!("!untouched:localhost") }

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

	journal.record(6, room(), None);

	assert_eq!(rooms(&journal, alice(), 4), None);
	assert_eq!(rooms(&journal, alice(), 5), Some(set(&[room()])));
	assert_eq!(rooms(&journal, alice(), 6), Some(set(&[])));
}

#[test]
fn a_change_for_one_user_answers_only_for_that_user() {
	let (journal, _) = journal(10, 0);

	journal.record(3, room(), Some(alice()));

	assert_eq!(rooms(&journal, alice(), 2), Some(set(&[room()])));
	assert_eq!(rooms(&journal, bob(), 2), Some(set(&[])));

	journal.record(4, other_room(), None);

	assert_eq!(rooms(&journal, bob(), 2), Some(set(&[other_room()])));
	assert_eq!(rooms(&journal, alice(), 3), Some(set(&[other_room()])));
}

#[test]
fn eviction_bounds_the_journal_and_raises_the_horizon() {
	let (journal, metrics) = journal(3, 0);
	let registry = metrics.enable_prometheus();

	for count in 1..=5 {
		journal.record(count, room(), None);
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

	journal.record(1, room(), None);

	for shared in [room(), other_room(), third] {
		journal.record(2, shared, None);
	}

	assert_eq!(journal.len(), 3);
	assert_eq!(journal.horizon(), 1);

	journal.record(3, room(), None);

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

	journal.record(1, room(), None);

	assert!(journal.is_empty());
	assert_eq!(rooms(&journal, alice(), 0), None);
}

#[test]
fn a_miss_is_counted_only_when_the_journal_answers_no() {
	let (journal, metrics) = journal(10, 5);
	let registry = metrics.enable_prometheus();

	journal.record(7, room(), None);

	let held = journal.changed_rooms(alice(), 6);
	let stale = journal.changed_rooms(alice(), 4);

	assert!(!journal.audit(JournalKind::Timeline, room(), held.as_ref()));
	assert!(!journal.audit(JournalKind::Timeline, room(), stale.as_ref()));
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
		0
	);

	assert!(journal.audit(JournalKind::Typing, other_room(), held.as_ref()));
	assert_eq!(registry.sync.journal_missed(JournalKind::Typing), 1);
	assert_eq!(
		registry
			.sync
			.journal_missed(JournalKind::Timeline),
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
