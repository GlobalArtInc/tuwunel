#![cfg(test)]

use std::net::TcpListener;

use futures::future::join;
use serde_json::{Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{Result, err, implement, metrics::prom::Build};
use tuwunel_service::Services;

use self::{
	account::account,
	client::{Client, field, wait_until_ready},
	require::require,
};

mod client;

#[path = "client/account.rs"]
mod account;

#[path = "client/membership.rs"]
mod membership;

#[path = "client/put.rs"]
mod put;

#[path = "client/require.rs"]
mod require;

const ALICE_TOKEN: &str = "sync-presence-only-alice-device-token-one";
const BOB_TOKEN: &str = "sync-presence-only-bob-device-token-one";

const PRESENCE_TYPES_NONE: &str = r#"{"presence":{"types":[]}}"#;

#[test]
fn presence_only_sync_keeps_waiting_above_the_threshold() -> Result { run(1, true) }

#[test]
fn presence_only_sync_answers_when_the_mode_is_off() -> Result { run(0, false) }

#[test]
fn presence_only_sync_answers_below_the_threshold() -> Result { run(2, false) }

#[test]
fn presence_types_filter_drops_presence() -> Result { serve(0, exercise_types_filter) }

fn run(min_rooms: usize, held: bool) -> Result {
	serve(min_rooms, async |services, base| exercise(services, base, held).await)
}

fn serve(min_rooms: usize, scenario: impl AsyncFnOnce(&Services, &str) -> Result) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_loopback_listener(port)
		.with_option("listening=true")
		.with_option("logging.global_default=false")
		.with_option("client.sync.timeout_min=400")
		.with_option(format!("client.sync.presence_only_min_rooms={min_rooms}"));

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	server.server.metrics.enable_prometheus();

	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let driven = async {
			let outcome = scenario(&services, &base).await;
			let shutdown = server.server.shutdown();

			outcome.and(shutdown)
		};

		let (served, outcome) = join(async_run(&server), driven).await;

		drop(services);
		async_stop(&server).await?;
		served?;

		outcome
	});

	drop(runtime);

	result
}

async fn exercise(services: &Services, base: &str, held: bool) -> Result {
	wait_until_ready(services, base).await?;

	let (alice_id, alice) = account(services, base, "poalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "pobob", BOB_TOKEN).await?;

	let room = bob
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	bob.admit(&room, &alice, &alice_id).await?;

	let since = field(&alice.sync(None, None).await?, "next_batch")?.to_owned();

	bob.put(
		&format!("presence/{bob_id}/status"),
		&json!({
			"presence": "unavailable",
			"status_msg": "away from keyboard",
		}),
	)
	.await?;

	let response = alice.sync(Some(&since), None).await?;
	let from_bob = presence_from(&response, bob_id.as_str());

	require(
		from_bob != held,
		if held {
			"a presence-only response ended the wait above the threshold"
		} else {
			"the response omitted the presence of the other user"
		},
	)?;

	let sync = &services
		.server
		.metrics
		.prometheus()
		.ok_or_else(|| err!("the metrics registry is off"))?
		.sync;

	require(
		(sync.v3_outcomes(Build::PresenceOnly) > 0) == held,
		"the presence-only build count disagrees with the mode",
	)
}

async fn exercise_types_filter(services: &Services, base: &str) -> Result {
	wait_until_ready(services, base).await?;

	let (alice_id, alice) = account(services, base, "poalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "pobob", BOB_TOKEN).await?;

	let room = bob
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	bob.admit(&room, &alice, &alice_id).await?;

	let since = field(&alice.sync(None, None).await?, "next_batch")?.to_owned();

	bob.put(
		&format!("presence/{bob_id}/status"),
		&json!({
			"presence": "unavailable",
			"status_msg": "away from keyboard",
		}),
	)
	.await?;

	let filtered = alice
		.sync(Some(&since), Some(PRESENCE_TYPES_NONE))
		.await?;

	require(
		!presence_from(&filtered, bob_id.as_str()),
		"the presence types filter let the presence of the other user through",
	)?;

	let plain = alice.sync(Some(&since), None).await?;

	require(
		presence_from(&plain, bob_id.as_str()),
		"the response without the filter omitted the presence of the other user",
	)
}

fn presence_from(response: &Value, user_id: &str) -> bool {
	response["presence"]["events"]
		.as_array()
		.is_some_and(|events| {
			events
				.iter()
				.any(|event| event["sender"] == user_id)
		})
}

#[implement(Client, params = "<'_>")]
async fn sync(&self, since: Option<&str>, filter: Option<&str>) -> Result<Value> {
	let since = since.map(|since| ("since", since));
	let filter = filter.map(|filter| ("filter", filter));

	self.services
		.client
		.clients
		.default
		.get(self.url("sync"))
		.bearer_auth(self.token)
		.query(&[("timeout", "0")])
		.query(since.as_slice())
		.query(filter.as_slice())
		.send()
		.await?
		.error_for_status()?
		.json()
		.await
		.map_err(Into::into)
}
