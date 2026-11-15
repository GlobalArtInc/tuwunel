use std::collections::HashSet;

use ruma::{
	OwnedRoomId, RoomId, UserId,
	events::room::member::{MembershipState, RoomMemberEventContent},
	profile::ProfileFieldName,
	room_id, user_id,
};
use serde_json::json;
use tuwunel_core::{Result, config::Figment, metrics::prom::JournalKind};
use tuwunel_matrix::PduCount;

use super::super::{RoomMemo, Scan};
use crate::{
	Services, profile::Propagation, rooms::state_cache::MembershipUpdate, test_utils::fixture,
};

const LIMIT: usize = 32;

fn alice() -> &'static UserId { user_id!("@alice:localhost") }
fn bob() -> &'static UserId { user_id!("@bob:localhost") }
fn remote() -> &'static UserId { user_id!("@invitee:remote.example") }
fn room() -> &'static RoomId { room_id!("!membership:localhost") }
fn other_room() -> &'static RoomId { room_id!("!second:localhost") }

#[tokio::test]
async fn a_local_invite_journals_the_membership_of_its_user() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;

		update(services, room(), alice(), MembershipState::Invite).await?;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(
			scan(services, alice(), (after, upto), &complete()),
			Scan::Wake(JournalKind::Membership)
		);
		assert_eq!(scan(services, bob(), (after, upto), &complete()), Scan::Quiet);
		assert_eq!(scan(services, bob(), (after, upto), &RoomMemo::default()), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn the_invite_of_a_remote_user_is_not_journaled() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		update(services, room(), remote(), MembershipState::Invite).await?;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(services.sync.journal.len(), held);
		assert_eq!(scan(services, remote(), (after, upto), &complete()), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_departure_journals_the_membership_of_its_user() -> Result {
	with_services(async |services| {
		update(services, room(), alice(), MembershipState::Join).await?;

		let after = services.globals.wait_pending().await?;

		update(services, room(), alice(), MembershipState::Leave).await?;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(
			scan(services, alice(), (after, upto), &complete()),
			Scan::Wake(JournalKind::Membership)
		);
		assert_eq!(scan(services, bob(), (after, upto), &complete()), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_backfilled_membership_is_not_journaled() -> Result {
	with_services(async |services| {
		let held = services.sync.journal.len();
		let content = RoomMemberEventContent::new(MembershipState::Join);

		services
			.state_cache
			.update_membership(MembershipUpdate::self_sent(
				room(),
				alice(),
				content,
				PduCount::Backfilled(-3),
			))
			.await?;

		assert_eq!(services.sync.journal.len(), held);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_profile_change_journals_the_user_once_and_each_joined_room() -> Result {
	with_services(async |services| {
		for joined in [room(), other_room()] {
			update(services, joined, alice(), MembershipState::Join).await?;
		}

		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		services
			.profile
			.set_profile_keys(
				alice(),
				&[(ProfileFieldName::DisplayName, Some(json!("Alice")))],
				Some(Propagation::None),
			)
			.await?;

		let upto = services.globals.wait_pending().await?;
		let window = (after, upto);

		assert_eq!(services.sync.journal.len(), held.saturating_add(3));
		assert_eq!(
			scan(services, alice(), window, &complete()),
			Scan::Wake(JournalKind::Profile)
		);
		assert_eq!(scan(services, bob(), window, &complete()), Scan::Quiet);

		let Scan::Resolve { rooms, overflow: false } =
			scan(services, bob(), window, &RoomMemo::default())
		else {
			panic!("the rooms of a profile change are left for the scan caller to resolve");
		};

		assert_eq!(
			rooms
				.into_iter()
				.collect::<HashSet<OwnedRoomId>>(),
			HashSet::from([room().to_owned(), other_room().to_owned()])
		);

		let mut member = RoomMemo::default();
		member.insert(other_room().to_owned(), true);

		assert_eq!(scan(services, bob(), window, &member), Scan::Wake(JournalKind::Profile));

		Ok(())
	})
	.await
}

#[tokio::test]
async fn membership_and_profile_changes_are_not_changed_rooms() -> Result {
	with_services(async |services| {
		let after = services.globals.wait_pending().await?;

		update(services, room(), alice(), MembershipState::Join).await?;
		update(services, other_room(), alice(), MembershipState::Invite).await?;
		services
			.profile
			.set_profile_keys(
				alice(),
				&[(ProfileFieldName::DisplayName, Some(json!("Alice")))],
				Some(Propagation::None),
			)
			.await?;

		let journal = &services.sync.journal;

		assert!(journal.len() >= 4);

		for user in [alice(), bob()] {
			assert_eq!(journal.changed_rooms(user, after), Some(HashSet::new()));
			assert_eq!(journal.changed_since(room(), user, after), Some(false));
			assert_eq!(journal.changed_since(other_room(), user, after), Some(false));
		}

		Ok(())
	})
	.await
}

async fn update(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
	membership: MembershipState,
) -> Result {
	let count = services.globals.next_count();
	let content = RoomMemberEventContent::new(membership);

	services
		.state_cache
		.update_membership(MembershipUpdate {
			sender: bob(),
			..MembershipUpdate::self_sent(room_id, user_id, content, PduCount::Normal(*count))
		})
		.await
}

fn complete() -> RoomMemo {
	let mut memo = RoomMemo::default();
	memo.mark_complete();

	memo
}

fn scan(services: &Services, user: &UserId, (after, upto): (u64, u64), memo: &RoomMemo) -> Scan {
	services
		.sync
		.journal
		.scan(user, None, after, upto, memo, LIMIT)
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
