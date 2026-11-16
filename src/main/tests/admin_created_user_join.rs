#![cfg(test)]

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering::SeqCst},
};

use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tuwunel_core::{
	Result,
	log::{Capture, capture::Data},
	ruma::{OwnedRoomId, OwnedUserId, UserId},
};
use tuwunel_service::Services;

use self::{
	client::{Client, field, register},
	fixture::boot,
};

mod client;
mod fixture;

#[path = "client/admin_api.rs"]
mod admin_api;

#[expect(dead_code)] // Only the membership action on another user is used from the add-on.
#[path = "client/membership.rs"]
mod membership;

const ADMIN_TOKEN: &str = "admin-created-user-join-admin-token";
const OWNER_TOKEN: &str = "admin-created-user-join-owner-token";
const CREATED_TOKEN: &str = "admin-created-user-join-created-token";
const BANNED_TOKEN: &str = "admin-created-user-join-banned-token";
const EXHAUSTED: &str = "Several servers failed";

#[test]
fn admin_created_user_is_not_a_guest() -> Result {
	let options = [
		"admin.room.create=true",
		"registration.grant_admin_to_first_user=false",
		"registration.enable=true",
		"registration.\
		 yes_i_am_very_very_sure_i_want_an_open_registration_server_prone_to_abuse=true",
		"registration.guests.enable=true",
		"rooms.joins.max_attempts_per_request=3",
	];

	boot("admin-created-user-join", options, exercise)
}

async fn exercise(services: &Services, base: &str) -> Result {
	let admin_id = register(services, "createadmin", ADMIN_TOKEN).await?;
	register(services, "createowner", OWNER_TOKEN).await?;

	services.admin.make_user_admin(&admin_id).await?;

	let admin = Client { services, base, token: ADMIN_TOKEN };
	let owner = Client { services, base, token: OWNER_TOKEN };
	let server = services.globals.server_name();

	let created_id = UserId::parse(format!("@newuser:{server}"))?;
	admin_put(&admin, &created_id, &json!({})).await?;

	services
		.users
		.create_device(&created_id, None, (Some(CREATED_TOKEN), None), None, None, None)
		.await?;

	let room_id = owner
		.create_room(&json!({
			"preset": "private_chat",
			"invite": [&created_id],
			"initial_state": [{
				"type": "m.room.guest_access",
				"state_key": "",
				"content": { "guest_access": "forbidden" },
			}],
		}))
		.await?;

	let created = Client { services, base, token: CREATED_TOKEN };
	let joined = join_by_id(&created, &room_id).await?;

	assert_eq!(joined.0, StatusCode::OK, "{}", joined.1);
	assert!(!services.users.is_guest(&created_id).await, "the created user is a guest");
	assert!(
		!details(&admin, &created_id).await?["deactivated"]
			.as_bool()
			.unwrap_or(true)
	);

	let (guest_id, guest_token) = register_guest(services, base).await?;
	let guest = Client { services, base, token: &guest_token };

	owner
		.act_on(&room_id, "invite", &guest_id)
		.await?;
	assert!(services.users.is_guest(&guest_id).await);

	let exhausted = Arc::new(AtomicUsize::new(0));
	let counter = exhausted.clone();
	let capture = Capture::new(
		&services.server.log.capture,
		Some(|data: Data<'_>| data.our_modules()),
		move |data: Data<'_>| {
			if data.message().starts_with(EXHAUSTED) {
				counter.fetch_add(1, SeqCst);
			}
		},
	);

	let _guard = capture.start();

	let (status, body) = join_by_id(&guest, &room_id).await?;
	assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
	assert_eq!(field(&body, "errcode")?, "M_FORBIDDEN", "{body}");
	assert_eq!(exhausted.load(SeqCst), 0, "join by id retried a refused guest");

	let (status, body) = join_by_alias(&guest, &room_id).await?;
	assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
	assert_eq!(field(&body, "errcode")?, "M_FORBIDDEN", "{body}");
	assert_eq!(exhausted.load(SeqCst), 0, "join by alias retried a refused guest");

	let banned_id = register(services, "createbanned", BANNED_TOKEN).await?;
	let banned = Client { services, base, token: BANNED_TOKEN };

	owner.act_on(&room_id, "ban", &banned_id).await?;

	let (status, body) = join_by_id(&banned, &room_id).await?;
	assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
	assert_eq!(field(&body, "errcode")?, "M_FORBIDDEN", "{body}");
	assert_eq!(exhausted.load(SeqCst), 0, "join by id retried a banned user");

	let (status, body) = join_by_alias(&banned, &room_id).await?;
	assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
	assert_eq!(field(&body, "errcode")?, "M_FORBIDDEN", "{body}");
	assert_eq!(exhausted.load(SeqCst), 0, "join by alias retried a banned user");

	let (status, body) = join_by_id(&created, &room_id).await?;
	assert_eq!(status, StatusCode::OK, "{body}");

	let (status, body) = join_by_alias(&created, &room_id).await?;
	assert_eq!(status, StatusCode::OK, "{body}");
	assert_eq!(exhausted.load(SeqCst), 0, "a member's repeated join was retried");

	let deactivated_id = UserId::parse(format!("@gone:{server}"))?;
	admin_put(&admin, &deactivated_id, &json!({ "deactivated": true })).await?;

	assert_eq!(details(&admin, &deactivated_id).await?["deactivated"], true);

	Ok(())
}

async fn admin_put(admin: &Client<'_>, user_id: &UserId, body: &Value) -> Result {
	admin
		.admin_api(Method::PUT, &format!("v2/users/{user_id}"), body, 200)
		.await
		.map(drop)
}

async fn details(admin: &Client<'_>, user_id: &UserId) -> Result<Value> {
	let response = admin
		.admin_api(Method::GET, &format!("v2/users/{user_id}"), &json!({}), 200)
		.await?;

	Ok(response.parse()?)
}

async fn join_by_id(client: &Client<'_>, room_id: &OwnedRoomId) -> Result<(StatusCode, Value)> {
	join(client, &format!("rooms/{room_id}/join")).await
}

async fn join_by_alias(
	client: &Client<'_>,
	room_id: &OwnedRoomId,
) -> Result<(StatusCode, Value)> {
	join(client, &format!("join/{room_id}")).await
}

async fn join(client: &Client<'_>, path: &str) -> Result<(StatusCode, Value)> {
	let response = client
		.post_url(&client.url(path), &json!({}))
		.await?;

	let status = response.status();

	Ok((status, response.json().await?))
}

async fn register_guest(services: &Services, base: &str) -> Result<(OwnedUserId, String)> {
	let url = format!("{base}/_matrix/client/v3/register?kind=guest");
	let response = services
		.client
		.clients
		.default
		.post(&url)
		.json(&json!({}))
		.send()
		.await?;

	let body: Value = response.error_for_status()?.json().await?;
	let user_id = field(&body, "user_id")?.try_into()?;
	let token = field(&body, "access_token")?.to_owned();

	Ok((user_id, token))
}
