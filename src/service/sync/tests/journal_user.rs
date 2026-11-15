use std::{collections::HashSet, iter::once};

use ruma::{
	DeviceId, OneTimeKeyAlgorithm, OneTimeKeyId, RoomId, UserId, device_id,
	encryption::OneTimeKey,
	events::{RoomAccountDataEventType, room::member::MembershipState},
	room_id,
	serde::{Base64, Raw, base64::Standard},
	signatures::{Ed25519KeyPair, KeyPair, to_canonical_json_string_for_signing},
	user_id,
};
use serde_json::{Value, from_value, json};
use tuwunel_core::{
	Result, config::Figment, metrics::prom::JournalKind, utils::to_canonical_object,
};
use tuwunel_database::{Json, serialize_key};

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
async fn one_upload_of_several_keys_journals_once_under_the_last_key() -> Result {
	with_services(async |services| {
		let users = &services.users;
		let key: Raw<OneTimeKey> = from_value(json!("key"))?;
		let keys = ["AAAAAQ", "AAAAAg", "AAAAAw"]
			.into_iter()
			.map(|name| {
				<&OneTimeKeyId>::try_from(format!("signed_curve25519:{name}").as_str())
					.map(|id| (id.to_owned(), key.clone()))
			})
			.collect::<Result<_, _>>()?;

		create_devices(users, alice(), &[laptop()]).await?;

		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		users
			.add_one_time_keys(alice(), laptop(), &keys, 10)
			.await?;

		let last = services.globals.wait_pending().await?;

		assert_eq!(last, after.saturating_add(3));
		assert_eq!(services.sync.journal.len(), held.saturating_add(1));
		assert_eq!(
			scan(services, alice(), Some(laptop()), (after, last.saturating_sub(1))),
			Scan::Quiet
		);
		assert_eq!(
			scan(services, alice(), Some(laptop()), (last.saturating_sub(1), last)),
			Scan::Wake(JournalKind::OneTimeKeys)
		);
		assert_eq!(
			scan(services, alice(), Some(laptop()), (after, last)),
			Scan::Wake(JournalKind::OneTimeKeys)
		);

		Ok(())
	})
	.await
}

#[tokio::test]
async fn signing_the_key_of_another_user_journals_the_signer() -> Result {
	with_services(async |services| {
		let keyid_key = &services.db["keyid_key"];
		let signer = key_pair("SIGNER");
		let master = key_pair("MASTER");
		let (signer_key, master_key) = (public_key(&signer), public_key(&master));
		let signer_row = (alice(), signer_key.as_str());
		let master_row = (bob(), master_key.as_str());
		let master_json = cross_signing_key(bob(), "master", &master_key);

		keyid_key.put(signer_row, Json(cross_signing_key(alice(), "user_signing", &signer_key)));
		keyid_key.put(master_row, Json(&master_json));
		services.db["userid_usersigningkeyid"].insert(alice(), serialize_key(signer_row)?);
		services.db["userid_masterkeyid"].insert(bob(), serialize_key(master_row)?);

		let canonical =
			to_canonical_json_string_for_signing(&to_canonical_object(&master_json)?)?;
		let signature = signer.sign(canonical.as_bytes()).base64();
		let signatures = [(format!("ed25519:{signer_key}"), signature)].into();
		let after = services.globals.wait_pending().await?;
		let held = services.sync.journal.len();

		services
			.users
			.sign_key(bob(), &master_key, signatures, alice())
			.await?;

		let upto = services.globals.wait_pending().await?;

		assert_eq!(services.sync.journal.len(), held.saturating_add(1));
		assert_eq!(
			scan(services, alice(), None, (after, upto)),
			Scan::Wake(JournalKind::OwnKeys)
		);
		assert_eq!(scan(services, bob(), None, (after, upto)), Scan::Quiet);

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

fn key_pair(version: &str) -> Ed25519KeyPair {
	Ed25519KeyPair::from_der(&Ed25519KeyPair::generate(), version.into())
		.expect("generated key pair should parse")
}

fn public_key(key_pair: &Ed25519KeyPair) -> String {
	Base64::<Standard, _>::new(key_pair.public_key()).encode()
}

fn cross_signing_key(user_id: &UserId, usage: &str, public_key: &str) -> Value {
	json!({
		"user_id": user_id,
		"usage": [usage],
		"keys": { format!("ed25519:{public_key}"): public_key },
		"signatures": {},
	})
}
