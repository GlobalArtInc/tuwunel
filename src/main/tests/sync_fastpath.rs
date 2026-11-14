#![cfg(test)]

use std::net::TcpListener;

use futures::future::join;
use serde_json::{Map, Value, json};
use tuwunel::{Args, Runtime, Server, async_run, async_start, async_stop};
use tuwunel_core::{
	Result, err, implement,
	metrics::prom::{Api, Fastpath, Reply, SinceKind},
	ruma::{MilliSecondsSinceUnixEpoch, RoomId, events::receipt::ReceiptThread},
};
use tuwunel_service::{Services, rooms::read_receipt::PrivateRead, users::DeviceListChange};

use self::{
	account::account,
	client::{Client, field, wait_until_ready},
	device_lists::device_list_changed,
	require::require,
};

mod client;

#[path = "client/account.rs"]
mod account;

#[expect(dead_code)]
#[path = "client/device_lists.rs"]
mod device_lists;

#[path = "client/membership.rs"]
mod membership;

#[path = "client/message.rs"]
mod message;

#[path = "client/profile.rs"]
mod profile;

#[path = "client/put.rs"]
mod put;

#[path = "client/require.rs"]
mod require;

const ALICE_TOKEN: &str = "sync-fastpath-alice-device-token-one";
const ALICE_SECOND_TOKEN: &str = "sync-fastpath-alice-device-token-two";
const BOB_TOKEN: &str = "sync-fastpath-bob-device-token-one";
const CAROL_TOKEN: &str = "sync-fastpath-carol-device-token-one";

#[test]
fn fastpath_answers_each_change_like_the_assembled_path() -> Result {
	run("client.sync.fastpath=true", false)
}

#[test]
fn fastpath_verification_finds_no_mismatch() -> Result {
	run("client.sync.fastpath_verify=true", true)
}

fn run(option: &str, verify: bool) -> Result {
	let listener = TcpListener::bind(("127.0.0.1", 0))?;
	let port = listener.local_addr()?.port();

	let args = Args::default_test(&["fresh", "cleanup"])
		.with_loopback_listener(port)
		.with_option("listening=true")
		.with_option("logging.global_default=false")
		.with_option(option);

	let runtime = Runtime::new(Some(&args))?;
	let server = Server::new(Some(&args), Some(&runtime))?;

	server.server.metrics.enable_prometheus();

	let result = runtime.block_on(async {
		let services = async_start(&server).await?;
		let base = format!("http://127.0.0.1:{port}");

		drop(listener);

		let driven = async {
			let outcome = exercise(&services, &base, verify).await;
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

async fn exercise(services: &Services, base: &str, verify: bool) -> Result {
	wait_until_ready(services, base).await?;

	let (alice_id, alice) = account(services, base, "fpalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "fpbob", BOB_TOKEN).await?;
	let (carol_id, carol) = account(services, base, "fpcarol", CAROL_TOKEN).await?;

	services
		.users
		.create_device(&alice_id, None, (Some(ALICE_SECOND_TOKEN), None), None, None, None)
		.await?;

	let alice_second = Client {
		services,
		base,
		token: ALICE_SECOND_TOKEN,
	};
	let encryption = json!({
		"type": "m.room.encryption",
		"state_key": "",
		"content": { "algorithm": "m.megolm.v1.aes-sha2" },
	});

	let target = bob
		.create_room(&json!({ "preset": "private_chat", "initial_state": [encryption] }))
		.await?;

	bob.admit(&target, &alice, &alice_id).await?;

	let quiet = alice
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	let since = field(&alice.sync(None, None).await?, "next_batch")?.to_owned();
	let mut watch = Watch {
		alice: &alice,
		since,
		quiet: &quiet,
		target: &target,
		filter: None,
	};

	let message = bob
		.send_text(&target, "fastpath-one", "hello")
		.await?;

	watch.sees("timeline", message.as_str()).await?;

	bob.post(&format!("rooms/{target}/receipt/m.read/{message}"), &json!({}))
		.await?;

	watch.sees("ephemeral", "m.receipt").await?;

	alice_second
		.post(&format!("rooms/{target}/receipt/m.read.private/{message}"), &json!({}))
		.await?;

	watch.sees("ephemeral", "m.read.private").await?;

	let typing = json!({ "typing": true, "timeout": 60000 });

	bob.put(&format!("rooms/{target}/typing/{bob_id}"), &typing)
		.await?;

	watch.sees("ephemeral", "m.typing").await?;

	let tag = format!("user/{alice_id}/rooms/{target}/tags/u.fastpath");

	alice.put(&tag, &json!({ "order": 0.5 })).await?;

	watch.sees("account_data", "m.tag").await?;

	bob.admit(&target, &carol, &carol_id).await?;

	let joined = bob
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	alice.act(&joined, "join").await?;

	let seen = watch.sees("timeline", carol_id.as_str()).await?;

	require(
		seen["rooms"]["join"]
			.get(joined.as_str())
			.is_some(),
		"the join round omitted the newly joined room",
	)?;

	let rekey = DeviceListChange::Resync;

	services
		.users
		.mark_device_key_update(&bob_id, rekey)
		.await;

	let seen = watch.round().await?;

	require(
		device_list_changed(&seen["device_lists"], &bob_id),
		"the rekey round omitted the device-list change",
	)?;
	require(
		seen["rooms"]["join"]
			.get(target.as_str())
			.is_none(),
		"the rekey round reported the room",
	)?;

	bob.rename(&bob_id, "Bobby").await?;

	watch.sees("timeline", "Bobby").await?;

	let global = format!("user/{alice_id}/account_data/u.fastpath");

	alice
		.put(&global, &json!({ "kept": true }))
		.await?;

	let seen = watch.round().await?;

	require(
		seen["account_data"]
			.to_string()
			.contains("u.fastpath"),
		"the global account data round omitted the event",
	)?;
	require(
		seen["rooms"]["join"]
			.as_object()
			.is_none_or(Map::is_empty),
		"the global account data round reported rooms",
	)?;

	let later = bob
		.send_text(&target, "fastpath-two", "later")
		.await?;

	watch.sees("timeline", later.as_str()).await?;

	services
		.pusher
		.reset_notification_counts(&alice_id, &target)
		.await;

	watch
		.sees("unread_notifications", "notification_count")
		.await?;

	services
		.read_receipt
		.private_read_set(PrivateRead {
			room_id: &target,
			user_id: &alice_id,
			count: services
				.timeline
				.get_pdu_count(&later)
				.await?
				.into_unsigned(),
			ts: MilliSecondsSinceUnixEpoch::now(),
			thread: &ReceiptThread::Unthreaded,
			announce: true,
		})
		.await;

	watch.sees("ephemeral", "m.read.private").await?;

	let reply = json!({
		"msgtype": "m.text",
		"body": "reply",
		"m.relates_to": { "rel_type": "m.thread", "event_id": later },
	});

	watch.filter = Some(
		json!({ "room": { "timeline": { "unread_thread_notifications": true } } }).to_string(),
	);

	bob.put(&format!("rooms/{target}/send/m.room.message/fastpath-reply"), &reply)
		.await?;

	watch.sees("timeline", "m.thread").await?;

	services
		.pusher
		.reset_thread_notification_counts(&alice_id, &target, &later)
		.await;

	watch
		.sees("unread_thread_notifications", later.as_str())
		.await?;

	finish(services, verify)
}

fn finish(services: &Services, verify: bool) -> Result {
	require(
		!verify || services.sync.fastpath_mismatches() == 0,
		"the verification mode recorded a mismatch",
	)?;

	let registry = services
		.server
		.metrics
		.prometheus()
		.ok_or_else(|| err!("the metrics registry is off"))?;
	let sync = &registry.sync;

	require(
		sync.fastpath_mismatches() == services.sync.fastpath_mismatches(),
		"the mismatch metric differs from the service counter",
	)?;

	require(
		sync.v3_rooms_counted(Fastpath::Quiet) > 0
			&& sync.v3_rooms_counted(Fastpath::BusyTimeline) > 0,
		"the room verdicts were not counted",
	)?;

	require(
		sync.v3_builds(SinceKind::Initial) > 0 && sync.v3_builds(SinceKind::Incremental) > 0,
		"the builds were not timed",
	)?;

	require(
		sync.v3_answers(SinceKind::Incremental, Reply::Data) > 0,
		"the data responses were not counted",
	)?;

	require(sync.long_polling(Api::V3) == 0, "a long poll was left in the gauge")
}

struct Watch<'a> {
	alice: &'a Client<'a>,
	since: String,
	quiet: &'a RoomId,
	target: &'a RoomId,
	filter: Option<String>,
}

impl Watch<'_> {
	async fn round(&mut self) -> Result<Value> {
		let since = Some(self.since.as_str());
		let response = self
			.alice
			.sync(since, self.filter.as_deref())
			.await?;

		self.since = field(&response, "next_batch")?.to_owned();

		require(
			response["rooms"]["join"]
				.get(self.quiet.as_str())
				.is_none(),
			"a round reported the quiet room",
		)?;

		Ok(response)
	}

	async fn sees(&mut self, section: &str, needle: &str) -> Result<Value> {
		let response = self.round().await?;
		let holds = response["rooms"]["join"][self.target.as_str()][section]
			.to_string()
			.contains(needle);

		require(holds, &format!("the {section} round omitted {needle}"))?;

		Ok(response)
	}
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
