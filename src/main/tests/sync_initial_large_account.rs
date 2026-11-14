#![cfg(test)]

use std::{fs::remove_dir_all, net::TcpListener};

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, implement,
	ruma::{OwnedRoomId, RoomId},
};
use tuwunel_service::Services;

use self::{
	client::{Client, field, register, wait_until_ready},
	require::require,
	sync::joined_room,
};

mod client;

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

#[test]
fn initial_sync_of_a_large_account_caps_the_timeline() -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let db_path = Args::test_database_path("sync-initial-large-account");

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_database_path(&db_path)
		.with_loopback_listener(port)
		.with_option("listening=true")
		.with_option("client.sync.initial_large_account_rooms=2")
		.with_option("client.sync.initial_large_account_timeline_limit=1");

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;
	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let exercise = async {
			let outcome = exercise(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (run_result, outcome) = join(async_run(&server), exercise).await;

		drop(services);
		async_stop(&server).await?;
		run_result?;

		outcome
	});

	drop(runtime);
	remove_dir_all(&db_path).ok();

	result
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
