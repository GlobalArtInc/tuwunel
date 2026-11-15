use std::{
	array,
	collections::VecDeque,
	iter::repeat_with,
	pin::{Pin, pin},
	task::{Poll, Waker},
	time::Duration,
};

use ruma::{
	DeviceId, OwnedRoomId, RoomId, UserId, device_id,
	events::room::member::{MembershipState, RoomMemberEventContent},
	room_id, user_id,
};
use tokio::time::timeout;
use tuwunel_core::{
	Result,
	config::Figment,
	metrics::prom::{JournalKind, Lookup},
};
use tuwunel_matrix::PduCount;

use super::super::Woken;
use crate::{
	Services,
	rooms::state_cache::MembershipUpdate,
	test_utils::{fixture, poll_once},
};

/// How long a wait has to wake.
///
/// Only a failing test waits it out.
const WAKE_WINDOW: Duration = Duration::from_secs(5);

/// How long a wait that must keep waiting is watched.
const QUIET_WINDOW: Duration = Duration::from_millis(200);

/// Rooms of the user nobody else is in, to push a window past the lookup limit.
const STRANGER_ROOMS: usize = 33;

fn alice() -> &'static UserId { user_id!("@alice:localhost") }
fn bob() -> &'static UserId { user_id!("@bob:localhost") }
fn laptop() -> &'static DeviceId { device_id!("LAPTOP") }
fn phone() -> &'static DeviceId { device_id!("PHONE") }
fn home() -> &'static RoomId { room_id!("!home:localhost") }
fn invited() -> &'static RoomId { room_id!("!invited:localhost") }
fn stranger() -> &'static RoomId { room_id!("!stranger:localhost") }

#[tokio::test]
async fn a_change_retired_before_the_call_wakes_the_first_poll() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;

		journal_user(services, alice(), Some(phone()));

		let wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert_eq!(
			poll_once(wait, Waker::noop()),
			Poll::Ready(Woken::Journal(JournalKind::ToDevice))
		);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_at_or_before_after_keeps_waiting() -> Result {
	with_services(async |services| {
		let count = services.globals.next_count();
		let own = *count;

		services
			.sync
			.journal
			.record_user(own, JournalKind::ToDevice, alice(), Some(phone()));
		drop(count);

		journal_user(services, bob(), None);

		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), own)
		);

		assert!(stays_pending(wait.as_mut()).await);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_of_another_user_keeps_waiting() -> Result {
	with_services(async |services| {
		with_membership(services, home(), alice(), MembershipState::Join).await?;

		let after = services.globals.wait_pending().await?;

		journal_user(services, bob(), Some(phone()));

		let count = services.globals.next_count();
		services
			.sync
			.journal
			.record(*count, JournalKind::Receipts, home(), Some(bob()));
		drop(count);

		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert!(stays_pending(wait.as_mut()).await);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_to_device_event_for_another_device_keeps_waiting() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;

		journal_user(services, alice(), Some(phone()));

		for device in [Some(laptop()), None] {
			let mut wait = pin!(services.sync.wait_journal(alice(), device, after));

			assert!(stays_pending(wait.as_mut()).await, "{device:?}");
		}

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_room_change_wakes_for_a_joined_or_invited_room_only() -> Result {
	with_services(async |services| {
		with_membership(services, home(), alice(), MembershipState::Join).await?;
		with_membership(services, invited(), alice(), MembershipState::Invite).await?;

		let after = services.globals.wait_pending().await?;

		journal_room(services, stranger());

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert!(stays_pending(wait.as_mut()).await);

		journal_room(services, home());

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::Timeline)));

		let after = services.globals.wait_pending().await?;

		journal_room(services, invited());

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::Timeline)));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_written_out_of_order_wakes_once_the_frontier_reaches_it() -> Result {
	with_services(async |services| {
		with_membership(services, home(), alice(), MembershipState::Join).await?;

		let after = services.globals.wait_pending().await?;
		let early = services.globals.next_count();
		let late = services.globals.next_count();
		let journal = &services.sync.journal;

		journal.record(*late, JournalKind::Timeline, stranger(), None);
		drop(late);

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert!(stays_pending(wait.as_mut()).await);

		journal.record(*early, JournalKind::Timeline, home(), None);
		drop(early);

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::Timeline)));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_after_the_first_poll_wakes_the_sleeper() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;
		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());

		journal_user(services, alice(), Some(phone()));

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::ToDevice)));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn more_unknown_rooms_than_the_limit_load_the_list_of_rooms_once() -> Result {
	with_services(async |services| {
		let registry = services.server.metrics.enable_prometheus();

		with_membership(services, home(), alice(), MembershipState::Join).await?;

		let after = services.globals.wait_pending().await?;

		for number in 0..STRANGER_ROOMS {
			journal_room(services, &OwnedRoomId::try_from(format!("!room{number}:localhost"))?);
		}

		journal_room(services, home());

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::Timeline)));
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Set), 1);
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Room), 0);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_above_a_lagging_frontier_wakes_the_sleeper_without_the_oldest_permit() -> Result
{
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;
		let journal = &services.sync.journal;
		let [oldest, second, third, youngest] = array::from_fn(|_| services.globals.next_count());
		let upto = *third;

		journal.record(*second, JournalKind::Timeline, stranger(), None);
		journal.record_user(*third, JournalKind::ToDevice, alice(), Some(phone()));

		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert!(stays_pending(wait.as_mut()).await);

		drop(second);
		drop(third);
		drop(oldest);

		let frontier = *services.globals.watch_retired().borrow();

		assert!(frontier < upto);
		assert_eq!(services.globals.current_count(), upto);
		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::ToDevice)));

		drop(youngest);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_wait_counts_the_changes_it_passes_over() -> Result {
	with_services(async |services| {
		let registry = services.server.metrics.enable_prometheus();
		let after = services.globals.wait_pending().await?;

		for _ in 0..5 {
			journal_user(services, bob(), None);
		}

		journal_user(services, alice(), Some(phone()));

		let counted = registry.sync.wait_scanned_total();
		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::ToDevice)));
		assert_eq!(registry.sync.wait_scanned_total(), counted.saturating_add(6));

		let upto = services.globals.current_count();

		assert_eq!(
			services
				.sync
				.journal_relevant(alice(), Some(phone()), upto)
				.await,
			Some(false)
		);
		assert_eq!(registry.sync.wait_scanned_total(), counted.saturating_add(6));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn journal_relevant_counts_neither_the_entries_nor_the_lookups_of_a_wait() -> Result {
	with_services(async |services| {
		let registry = services.server.metrics.enable_prometheus();

		with_membership(services, home(), alice(), MembershipState::Join).await?;

		let after = services.globals.wait_pending().await?;

		journal_room(services, stranger());
		journal_room(services, home());

		let counted = registry.sync.wait_scanned_total();

		assert_eq!(
			services
				.sync
				.journal_relevant(alice(), None, after)
				.await,
			Some(true)
		);
		assert_eq!(registry.sync.wait_scanned_total(), counted);
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Room), 0);

		let mut wait = pin!(services.sync.wait_journal(alice(), None, after));

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Journal(JournalKind::Timeline)));
		assert!(registry.sync.wait_scanned_total() > counted);
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Room), 2);

		let after = services.globals.wait_pending().await?;

		for number in 0..STRANGER_ROOMS {
			journal_room(services, &OwnedRoomId::try_from(format!("!room{number}:localhost"))?);
		}

		journal_room(services, home());

		assert_eq!(
			services
				.sync
				.journal_relevant(alice(), None, after)
				.await,
			Some(true)
		);
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Set), 0);
		assert_eq!(registry.sync.wait_lookups_total(Lookup::Room), 2);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn changes_evicted_above_after_wake_with_the_horizon_once_the_counter_reaches_it() -> Result
{
	let config = Figment::new().merge(("client.sync.journal_capacity", 2));

	with_config(config, async |services| {
		let after = services.globals.wait_pending().await?;
		let journal = &services.sync.journal;

		let mut permits: VecDeque<_> = repeat_with(|| services.globals.next_count())
			.take(4)
			.collect();

		for permit in &permits {
			journal.record_user(**permit, JournalKind::ToDevice, bob(), None);
		}

		let horizon = journal.horizon();

		assert!(horizon > after);

		drop(permits.pop_front());

		let frontier = services.globals.current_count();

		assert!(frontier > after && frontier < horizon);

		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert!(stays_pending(wait.as_mut()).await);
		assert_eq!(
			services
				.sync
				.journal_relevant(alice(), Some(phone()), after)
				.await,
			None
		);

		drop(permits.pop_front());

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Horizon));
		assert!(services.globals.current_count() >= horizon);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn shutdown_ends_the_wait() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;
		let mut wait = pin!(
			services
				.sync
				.wait_journal(alice(), Some(phone()), after)
		);

		assert!(poll_once(wait.as_mut(), Waker::noop()).is_pending());

		services.server.shutdown()?;

		assert_eq!(wakes(wait.as_mut()).await, Some(Woken::Shutdown));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn journal_relevant_answers_as_the_wait_does() -> Result {
	with_services(async |services| {
		with_membership(services, home(), alice(), MembershipState::Join).await?;
		with_membership(services, invited(), alice(), MembershipState::Invite).await?;

		let after = services.globals.wait_pending().await?;

		journal_user(services, alice(), Some(phone()));
		agree(services, Some(phone()), after, true).await;
		agree(services, Some(laptop()), after, false).await;

		let after = services.globals.wait_pending().await?;

		agree(services, Some(phone()), after, false).await;

		journal_user(services, bob(), Some(phone()));
		agree(services, Some(phone()), after, false).await;

		let after = services.globals.wait_pending().await?;

		journal_room(services, stranger());
		agree(services, None, after, false).await;

		let after = services.globals.wait_pending().await?;

		journal_room(services, home());
		agree(services, None, after, true).await;

		let after = services.globals.wait_pending().await?;

		journal_room(services, invited());
		agree(services, None, after, true).await;

		Ok(())
	})
	.await
}

async fn agree(services: &Services, device: Option<&DeviceId>, after: u64, relevant: bool) {
	assert_eq!(
		services
			.sync
			.journal_relevant(alice(), device, after)
			.await,
		Some(relevant)
	);

	let mut wait = pin!(services.sync.wait_journal(alice(), device, after));

	if relevant {
		assert!(wakes(wait.as_mut()).await.is_some(), "{device:?} after {after}");
	} else {
		assert!(stays_pending(wait.as_mut()).await, "{device:?} after {after}");
	}
}

async fn stays_pending<F: Future>(wait: Pin<&mut F>) -> bool {
	timeout(QUIET_WINDOW, wait).await.is_err()
}

async fn wakes<F: Future>(wait: Pin<&mut F>) -> Option<F::Output> {
	timeout(WAKE_WINDOW, wait).await.ok()
}

fn journal_user(services: &Services, user: &UserId, device: Option<&DeviceId>) {
	let count = services.globals.next_count();

	services
		.sync
		.journal
		.record_user(*count, JournalKind::ToDevice, user, device);
}

fn journal_room(services: &Services, room: &RoomId) {
	let count = services.globals.next_count();

	services
		.sync
		.journal
		.record(*count, JournalKind::Timeline, room, None);
}

async fn with_membership(
	services: &Services,
	room: &RoomId,
	user: &UserId,
	membership: MembershipState,
) -> Result {
	let count = services.globals.next_count();
	let content = RoomMemberEventContent::new(membership);

	services
		.state_cache
		.update_membership(MembershipUpdate {
			sender: bob(),
			..MembershipUpdate::self_sent(room, user, content, PduCount::Normal(*count))
		})
		.await
}

async fn with_services<Test>(test: Test) -> Result
where
	Test: AsyncFnOnce(&Services) -> Result,
{
	with_config(Figment::new(), test).await
}

async fn with_config<Test>(config: Figment, test: Test) -> Result
where
	Test: AsyncFnOnce(&Services) -> Result,
{
	let Some(fixture) = fixture(config).await? else {
		return Ok(());
	};

	test(&fixture.services).await
}
