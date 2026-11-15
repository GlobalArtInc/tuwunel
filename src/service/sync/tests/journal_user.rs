use std::{collections::HashSet, iter::once};

use ruma::{
	DeviceId, OneTimeKeyAlgorithm, OneTimeKeyId, RoomId, UserId, device_id,
	encryption::OneTimeKey,
	events::{RoomAccountDataEventType, room::member::MembershipState},
	room_id,
	serde::Raw,
	user_id,
};
use serde_json::{from_value, json};
use tuwunel_core::{Result, config::Figment, metrics::prom::JournalKind};

use super::super::{RoomMemo, Scan};
use crate::{
	Services,
	test_utils::{create_devices, fixture, membership_update},
	users::DeviceListChange,
};

const LIMIT: usize = 32;

fn alice() -> &'static UserId { user_id!("@alice:localhost") }
fn bob() -> &'static UserId { user_id!("@bob:localhost") }
fn remote() -> &'static UserId { user_id!("@keys:remote.example") }
fn laptop() -> &'static DeviceId { device_id!("LAPTOP") }
fn phone() -> &'static DeviceId { device_id!("PHONE") }
fn room() -> &'static RoomId { room_id!("!keys:localhost") }

#[tokio::test]
async fn a_to_device_event_journals_the_device_it_is_for() -> Result {
	with_services(async |services| {
		create_devices(&services.users, alice(), &[laptop(), phone()]).await?;

		let after = services.globals.wait_pending().await?;

		services
			.users
			.add_to_device_event(bob(), alice(), phone(), "m.test", &json!({}));

		let upto = services.globals.wait_pending().await?;
		let window = (after, upto);

		assert_eq!(
			scan(services, alice(), Some(phone()), window),
			Scan::Wake(JournalKind::ToDevice)
		);
		assert_eq!(scan(services, alice(), Some(laptop()), window), Scan::Quiet);
		assert_eq!(scan(services, bob(), Some(phone()), window), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn uploading_one_time_and_fallback_keys_journals_the_user() -> Result {
	with_services(async |services| {
		let users = &services.users;
		let key: Raw<OneTimeKey> = from_value(json!("key"))?;
		let key_id = <&OneTimeKeyId>::try_from("signed_curve25519:AAAAAQ")?;
		let fallback_id = <&OneTimeKeyId>::try_from("signed_curve25519:AAAAAg")?;

		create_devices(users, alice(), &[laptop()]).await?;

		let after = services.globals.wait_pending().await?;

		users
			.add_one_time_keys(
				alice(),
				laptop(),
				&once((key_id.to_owned(), key.clone())).collect(),
				10,
			)
			.await?;

		let uploaded = services.globals.wait_pending().await?;

		assert_eq!(
			scan(services, alice(), Some(laptop()), (after, uploaded)),
			Scan::Wake(JournalKind::OneTimeKeys)
		);
		assert_eq!(scan(services, bob(), Some(laptop()), (after, uploaded)), Scan::Quiet);

		users
			.add_fallback_keys(alice(), laptop(), once((fallback_id, &key)))
			.await?;

		let fallback = services.globals.wait_pending().await?;

		assert_eq!(
			scan(services, alice(), Some(laptop()), (uploaded, fallback)),
			Scan::Wake(JournalKind::OneTimeKeys)
		);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_of_own_keys_journals_the_user_beside_the_device_lists_of_its_rooms() -> Result {
	with_services(async |services| {
		let count = services.globals.next_count();
		let update = membership_update(room(), alice(), MembershipState::Join, *count);
		services
			.state_cache
			.update_membership(update)
			.await?;

		drop(count);

		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		services
			.users
			.mark_device_key_update(alice(), DeviceListChange::Resync)
			.await;

		let upto = services.globals.wait_pending().await?;
		let window = (after, upto);

		assert_eq!(services.sync.journal.len(), held.saturating_add(2));
		assert_eq!(scan(services, alice(), None, window), Scan::Wake(JournalKind::OwnKeys));

		let mut member = RoomMemo::default();
		member.insert(room().to_owned(), true);

		assert_eq!(
			services
				.sync
				.journal
				.scan(bob(), None, after, upto, &member, LIMIT),
			Scan::Wake(JournalKind::DeviceLists)
		);
		assert_eq!(
			services.sync.journal.changed_rooms(bob(), after),
			Some(HashSet::from([room().to_owned()]))
		);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn claiming_a_one_time_key_journals_its_owner() -> Result {
	with_services(async |services| {
		let users = &services.users;
		let key: Raw<OneTimeKey> = from_value(json!("key"))?;
		let key_id = <&OneTimeKeyId>::try_from("signed_curve25519:AAAAAQ")?;

		create_devices(users, alice(), &[laptop()]).await?;
		users
			.add_one_time_keys(alice(), laptop(), &once((key_id.to_owned(), key)).collect(), 10)
			.await?;

		let after = services.globals.wait_pending().await?;

		users
			.take_one_time_key(alice(), laptop(), &OneTimeKeyAlgorithm::SignedCurve25519)
			.await?;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(
			scan(services, alice(), Some(laptop()), (after, upto)),
			Scan::Wake(JournalKind::OneTimeKeys)
		);
		assert_eq!(scan(services, bob(), Some(laptop()), (after, upto)), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn a_change_of_remote_keys_journals_only_the_device_lists_of_its_rooms() -> Result {
	with_services(async |services| {
		let count = services.globals.next_count();
		let update = membership_update(room(), remote(), MembershipState::Join, *count);
		services
			.state_cache
			.update_membership(update)
			.await?;

		drop(count);

		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		services
			.users
			.mark_device_key_update(remote(), DeviceListChange::Resync)
			.await;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(services.sync.journal.len(), held.saturating_add(1));
		assert_eq!(scan(services, remote(), None, (after, upto)), Scan::Quiet);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn global_account_data_is_journaled_for_its_user_when_set_and_when_deleted() -> Result {
	with_services(async |services| {
		let account_data = &services.account_data;
		let kind = RoomAccountDataEventType::from("m.test");
		let data = json!({ "type": "m.test", "content": {} });
		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		account_data
			.update(None, alice(), kind.clone(), &data)
			.await?;

		let set = services.globals.wait_pending().await?;

		assert_eq!(services.sync.journal.len(), held.saturating_add(1));
		assert_eq!(
			scan(services, alice(), None, (after, set)),
			Scan::Wake(JournalKind::GlobalAccountData)
		);
		assert_eq!(scan(services, bob(), None, (after, set)), Scan::Quiet);

		account_data.delete(None, alice(), kind).await?;

		let deleted = services.globals.wait_pending().await?;

		assert_eq!(services.sync.journal.len(), held.saturating_add(2));
		assert_eq!(
			scan(services, alice(), None, (set, deleted)),
			Scan::Wake(JournalKind::GlobalAccountData)
		);
		assert_eq!(
			services
				.sync
				.journal
				.changed_rooms(alice(), after),
			Some(HashSet::new())
		);

		Ok(())
	})
	.await
}

fn scan(
	services: &Services,
	user: &UserId,
	device: Option<&DeviceId>,
	(after, upto): (u64, u64),
) -> Scan {
	let mut memo = RoomMemo::default();
	memo.mark_complete();

	services
		.sync
		.journal
		.scan(user, device, after, upto, &memo, LIMIT)
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
