#![cfg(test)]

use serde_json::{Value, json};
use tuwunel_core::{
	Result, err, implement,
	ruma::{OwnedRoomId, RoomId},
};
use tuwunel_service::Services;

use self::{
	client::{Client, field, register, wait_until_ready},
	fixture::boot,
	require::require,
	sync::joined_room,
};

mod client;
mod fixture;

#[path = "client/message.rs"]
mod message;

#[path = "client/put.rs"]
mod put;

#[path = "client/require.rs"]
mod require;

#[path = "client/sync.rs"]
mod sync;

const LARGE_TOKEN: &str = "sync-initial-large-account-large-token";
const SMALL_TOKEN: &str = "sync-initial-large-account-small-token";

const FILTER: &str = r#"{"room":{"timeline":{"limit":20}}}"#;
const MESSAGES: usize = 5;

const OPTIONS: [&str; 3] = [
	"logging.global_default=false",
	"client.sync.initial_large_account_rooms=2",
	"client.sync.initial_large_account_timeline_limit=1",
];

#[test]
fn initial_sync_of_a_large_account_caps_the_timeline() -> Result {
	boot("sync-initial-large-account", OPTIONS, exercise)
}

#[test]
fn stale_token_of_a_large_account_caps_the_first_timeline() -> Result {
	let options = [
		OPTIONS[0],
		OPTIONS[1],
		OPTIONS[2],
		"client.sync.initial_large_account_stale_gap=1",
	];

	boot("sync-stale-large-account", options, capped_stale)
}

#[test]
fn stale_check_off_leaves_the_timeline_whole() -> Result {
	let options = [
		OPTIONS[0],
		OPTIONS[1],
		OPTIONS[2],
		"client.sync.initial_large_account_stale_gap=0",
	];

	boot("sync-stale-check-off", options, whole_stale)
}

#[test]
fn recent_token_within_the_gap_leaves_the_timeline_whole() -> Result {
	let options = [
		OPTIONS[0],
		OPTIONS[1],
		OPTIONS[2],
		"client.sync.initial_large_account_stale_gap=1000000",
	];

	boot("sync-stale-within-gap", options, whole_stale)
}

async fn capped_stale(services: &Services, base: &str) -> Result {
	stale(services, base, true).await
}

async fn whole_stale(services: &Services, base: &str) -> Result {
	stale(services, base, false).await
}

async fn stale(services: &Services, base: &str, capped: bool) -> Result {
	wait_until_ready(services, base).await?;

	register(services, "staleaccount", LARGE_TOKEN).await?;

	let client = Client { services, base, token: LARGE_TOKEN };
	let rooms = rooms_with_messages(&client, 3).await?;
	let since = field(&client.sync(None).await?, "next_batch")?.to_owned();

	for room in &rooms {
		for index in 0..4 {
			client
				.send_text(room, &format!("new-{index}"), &format!("new{index}"))
				.await?;
		}
	}

	let response = client.sync(Some(&since)).await?;

	for room in &rooms {
		let section = joined_room(&response, room)?;
		let events = timeline_events(section, room)?.len();

		if capped {
			require(events == 1, "the stale token's first response was not trimmed")?;
			require(limited(section), "the trimmed stale response was not marked limited")?;
			require(
				section["timeline"]["prev_batch"].is_string(),
				"the trimmed stale response carried no prev_batch",
			)?;
		} else {
			require(events == 4, "a response that must stay whole was trimmed")?;
			require(!limited(section), "a response that must stay whole was marked limited")?;
		}
	}

	Ok(())
}

async fn exercise(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	register(services, "largeaccount", LARGE_TOKEN).await?;
	register(services, "smallaccount", SMALL_TOKEN).await?;

	let large = Client { services, base, token: LARGE_TOKEN };
	let small = Client { services, base, token: SMALL_TOKEN };

	let large_rooms = rooms_with_messages(&large, 3).await?;
	let small_rooms = rooms_with_messages(&small, 2).await?;

	let opening = large.sync(None).await?;

	for room in &large_rooms {
		let section = joined_room(&opening, room)?;

		require(
			timeline_events(section, room)?.len() == 1,
			"the capped initial sync did not trim the timeline to one event",
		)?;
		require(limited(section), "the capped timeline was not marked limited")?;
		require(
			section["timeline"]["prev_batch"].is_string(),
			"the capped timeline carried no prev_batch",
		)?;
		require(
			section["timeline"]["events"][0]["content"]["body"]
				== json!(format!("m{}", MESSAGES - 1)),
			"the capped timeline did not keep the newest event",
		)?;
	}

	let since = field(&opening, "next_batch")?.to_owned();
	let first = &large_rooms[0];

	for index in 0..3 {
		large
			.send_text(first, &format!("later-{index}"), &format!("later{index}"))
			.await?;
	}

	let incremental = large.sync(Some(&since)).await?;
	let section = joined_room(&incremental, first)?;

	require(
		timeline_events(section, first)?.len() == 3,
		"the incremental sync was capped like the initial one",
	)?;
	require(!limited(section), "the incremental sync marked its timeline limited")?;

	let uncapped = small.sync(None).await?;

	for room in &small_rooms {
		let section = joined_room(&uncapped, room)?;

		require(
			timeline_events(section, room)?.len() > MESSAGES,
			"an account within the threshold had its initial timeline capped",
		)?;
	}

	Ok(())
}

async fn rooms_with_messages(client: &Client<'_>, count: usize) -> Result<Vec<OwnedRoomId>> {
	let mut rooms = Vec::with_capacity(count);

	for _ in 0..count {
		let room = client
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		for message in 0..MESSAGES {
			client
				.send_text(&room, &format!("m{message}"), &format!("m{message}"))
				.await?;
		}

		rooms.push(room);
	}

	Ok(rooms)
}

fn limited(section: &Value) -> bool {
	section["timeline"]["limited"]
		.as_bool()
		.unwrap_or(false)
}

fn timeline_events<'a>(section: &'a Value, room_id: &RoomId) -> Result<&'a Vec<Value>> {
	section["timeline"]["events"]
		.as_array()
		.ok_or_else(|| err!("joined room {room_id} omitted its timeline events"))
}

#[implement(Client, params = "<'_>")]
async fn sync(&self, since: Option<&str>) -> Result<Value> {
	let since = since.map(|since| ("since", since));

	self.services
		.client
		.clients
		.default
		.get(self.url("sync"))
		.bearer_auth(self.token)
		.query(&[("timeout", "0"), ("filter", FILTER)])
		.query(since.as_slice())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}
