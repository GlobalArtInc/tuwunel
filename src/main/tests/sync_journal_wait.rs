#![cfg(test)]

use std::{
	sync::atomic::{AtomicU64, Ordering},
	time::Duration,
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel_core::{
	Result, err,
	metrics::prom::{Api, Registry, SinceKind, SyncMetrics, Verdict, WakeReason},
	ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId},
	utils::future::ExtExt,
};
use tuwunel_service::Services;

use self::{
	account::account,
	client::{Client, field, poll_until},
	fixture::boot,
	require::require,
};

mod client;
mod fixture;

#[path = "client/account.rs"]
mod account;

#[path = "client/get.rs"]
mod get;

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

const ALICE_TOKEN: &str = "sync-journal-wait-alice-device-token-one";
const ALICE_SECOND_TOKEN: &str = "sync-journal-wait-alice-device-token-two";
const BOB_TOKEN: &str = "sync-journal-wait-bob-device-token-one";
const DANA_TOKEN: &str = "sync-journal-wait-dana-device-token-one";

const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Server options every mode shares.
///
/// The modes boot one server each in this process, so none may claim the global
/// tracing subscriber. Presence is off so that pings move neither the counter
/// nor the responses, and a caught-up poll really waits. The shortest long poll
/// is 400 ms so that the idle rounds need not wait out the default minimum.
const OPTIONS: [&str; 4] = [
	"logging.global_default=false",
	"presence.allow_local=false",
	"presence.allow_outgoing=false",
	"client.sync.timeout_min=400",
];

/// How long a round that must be woken polls for, in milliseconds.
const WAKE_POLL_MS: u64 = 10_000;

/// How long a round that must stay asleep polls for, in milliseconds.
const IDLE_POLL_MS: u64 = 1_500;

/// How long a woken round may take from request to parsed response.
///
/// It sits far below the poll budget, so a round that waited out its poll fails.
const WAKE_DEADLINE: Duration = Duration::from_secs(3);

/// How long an idle round may take, which is its poll and a margin.
const IDLE_DEADLINE: Duration = Duration::from_secs(5);

/// How far short of its poll an idle round may answer, so that clock granularity
/// does not fail a round that did wait.
const IDLE_SLACK: Duration = Duration::from_millis(100);

/// How long the harness waits for the gauge of long polls to reach a value.
const GAUGE_DEADLINE: Duration = Duration::from_secs(5);

/// How long the room of a typing round keeps its typist.
const TYPING_MS: u64 = 60_000;

/// Rounds that must stay asleep.
const IDLE_ROUNDS: u64 = 2;

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
	Journal,
	Verify,
	Control,
}

impl Mode {
	fn option(self) -> &'static str {
		match self {
			| Self::Journal => "client.sync.journal_wait=true",
			| Self::Verify => "client.sync.journal_wait_verify=true",
			| Self::Control => "client.sync.journal_wait=false",
		}
	}
}

#[test]
fn a_journal_wait_wakes_on_every_kind_of_change() -> Result {
	run("sync-journal-wait", Mode::Journal)
}

#[test]
fn the_shadow_comparison_finds_no_miss() -> Result {
	run("sync-journal-wait-verify", Mode::Verify)
}

#[test]
fn the_database_watchers_wake_on_the_same_changes_when_both_switches_are_off() -> Result {
	run("sync-journal-wait-control", Mode::Control)
}

fn run(name: &str, mode: Mode) -> Result {
	let options = OPTIONS.into_iter().chain([mode.option()]);

	boot(name, options, async |services, base| exercise(services, base, mode).await)
}

async fn exercise(services: &Services, base: &str, mode: Mode) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "jwalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "jwbob", BOB_TOKEN).await?;
	let (dana_id, dana) = account(services, base, "jwdana", DANA_TOKEN).await?;

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

	let shared = bob
		.create_room(&json!({ "preset": "private_chat", "initial_state": [encryption] }))
		.await?;

	bob.admit(&shared, &alice, &alice_id).await?;

	let apart = bob
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	let world = World {
		mode,
		registry,
		alice,
		alice_second,
		alice_id,
		bob,
		bob_id,
		dana,
		dana_id,
		shared,
		apart,
		woken: AtomicU64::new(0),
	};

	let outcomes = [
		("message", world.message().await),
		("typing", world.typing().await),
		("receipt", world.receipt().await),
		("private read", world.private_read().await),
		("room account data", world.room_account_data().await),
		("global account data", world.global_account_data().await),
		("invite", world.invite().await),
		("kick", world.kick().await),
		("own device keys", world.own_keys().await),
		("device keys of a peer", world.peer_keys().await),
		("one-time keys", world.one_time_keys().await),
		("display name", world.rename().await),
		("to-device event", world.to_device().await),
		("message in a room apart", world.message_apart().await),
		("to-device event for another device", world.to_device_elsewhere().await),
	];

	let failed: Vec<_> = outcomes
		.into_iter()
		.filter_map(|(name, outcome)| {
			outcome
				.err()
				.map(|error| format!("{name}: {error}"))
		})
		.collect();

	require(failed.is_empty(), &format!("rounds failed: {}", failed.join("; ")))?;

	world.finish()
}

struct World<'a> {
	mode: Mode,
	registry: &'a Registry,
	alice: Client<'a>,
	alice_second: Client<'a>,
	alice_id: OwnedUserId,
	bob: Client<'a>,
	bob_id: OwnedUserId,
	dana: Client<'a>,
	dana_id: OwnedUserId,
	shared: OwnedRoomId,
	apart: OwnedRoomId,
	woken: AtomicU64,
}

impl World<'_> {
	async fn message(&self) -> Result {
		let (seen, event) = self
			.wakes(&self.alice, async || {
				self.bob
					.send_text(&self.shared, "journal-message", "hello")
					.await
			})
			.await?;

		require(
			in_room(&seen, &self.shared, "timeline", event.as_str()),
			"the response omitted the message",
		)
	}

	async fn typing(&self) -> Result {
		let path = format!("rooms/{}/typing/{}", self.shared, self.bob_id);
		let typing = json!({ "typing": true, "timeout": TYPING_MS });

		let (seen, ()) = self
			.wakes(&self.alice, async || self.bob.put(&path, &typing).await.map(drop))
			.await?;

		require(
			in_room(&seen, &self.shared, "ephemeral", "m.typing"),
			"the response omitted the typing notice",
		)
	}

	async fn receipt(&self) -> Result {
		let event = self
			.bob
			.send_text(&self.shared, "journal-receipt-target", "target")
			.await?;

		let path = format!("rooms/{}/receipt/m.read/{event}", self.shared);

		let (seen, ()) = self
			.wakes(&self.alice, async || self.bob.post(&path, &json!({})).await.map(drop))
			.await?;

		require(
			in_room(&seen, &self.shared, "ephemeral", "m.receipt"),
			"the response omitted the read receipt",
		)
	}

	async fn private_read(&self) -> Result {
		let event = self
			.bob
			.send_text(&self.shared, "journal-private-target", "target")
			.await?;

		let path = format!("rooms/{}/receipt/m.read.private/{event}", self.shared);

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.alice_second
					.post(&path, &json!({}))
					.await
					.map(drop)
			})
			.await?;

		require(
			in_room(&seen, &self.shared, "ephemeral", "m.read.private"),
			"the response omitted the private read receipt",
		)
	}

	async fn room_account_data(&self) -> Result {
		let path = format!("user/{}/rooms/{}/tags/u.journal", self.alice_id, self.shared);

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.alice_second
					.put(&path, &json!({ "order": 0.5 }))
					.await
					.map(drop)
			})
			.await?;

		require(
			in_room(&seen, &self.shared, "account_data", "m.tag"),
			"the response omitted the room account data",
		)
	}

	async fn global_account_data(&self) -> Result {
		let path = format!("user/{}/account_data/u.journal", self.alice_id);

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.alice_second
					.put(&path, &json!({ "kept": true }))
					.await
					.map(drop)
			})
			.await?;

		require(
			seen["account_data"]
				.to_string()
				.contains("u.journal"),
			"the response omitted the global account data",
		)
	}

	async fn invite(&self) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.bob
					.act_on(&room, "invite", &self.alice_id)
					.await
			})
			.await?;

		require(
			seen["rooms"]["invite"]
				.get(room.as_str())
				.is_some(),
			"the response omitted the invitation",
		)
	}

	async fn kick(&self) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		self.bob
			.admit(&room, &self.alice, &self.alice_id)
			.await?;

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.bob
					.act_on(&room, "kick", &self.alice_id)
					.await
			})
			.await?;

		require(
			seen["rooms"]["leave"]
				.get(room.as_str())
				.is_some(),
			"the response omitted the room the user was removed from",
		)
	}

	async fn own_keys(&self) -> Result {
		let upload = device_keys(&self.dana_id, &device_of(&self.dana).await?);

		let (seen, ()) = self
			.wakes(&self.dana, async || {
				self.dana
					.post("keys/upload", &upload)
					.await
					.map(drop)
			})
			.await?;

		require(
			lists_as_changed(&seen, &self.dana_id),
			"the response omitted the change of the user's own device list",
		)
	}

	async fn peer_keys(&self) -> Result {
		let upload = device_keys(&self.bob_id, &device_of(&self.bob).await?);

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.bob
					.post("keys/upload", &upload)
					.await
					.map(drop)
			})
			.await?;

		require(
			lists_as_changed(&seen, &self.bob_id),
			"the response omitted the change of the device list of a member of a shared \
			 encrypted room",
		)
	}

	async fn one_time_keys(&self) -> Result {
		let upload = json!({ "one_time_keys": { "curve25519:AAAAAQ": KEY } });
		let finish = format!("user/{}/account_data/u.finish", self.dana_id);
		let builds = || {
			self.registry
				.sync
				.v3_builds(SinceKind::Incremental)
		};

		let (seen, rebuilt) = self
			.wakes(&self.dana, async || {
				let before = builds();

				self.dana.post("keys/upload", &upload).await?;

				let rebuilt = poll_until(WAKE_DEADLINE, async || builds() > before).await;

				self.dana
					.put(&finish, &json!({ "done": true }))
					.await?;

				Ok(rebuilt)
			})
			.await?;

		require(rebuilt, "the upload of one-time keys did not wake the long poll")?;
		require(
			seen["account_data"]
				.to_string()
				.contains("u.finish"),
			"the response omitted the account data that ended the round",
		)
	}

	async fn rename(&self) -> Result {
		let (seen, ()) = self
			.wakes(&self.alice, async || self.bob.rename(&self.bob_id, "Bobby").await)
			.await?;

		require(
			in_room(&seen, &self.shared, "timeline", "Bobby"),
			"the response omitted the new display name",
		)
	}

	async fn to_device(&self) -> Result {
		let device = device_of(&self.alice).await?;
		let message = json!({
			"messages": { self.alice_id.as_str(): { device.as_str(): { "seq": 1 } } },
		});

		let (seen, ()) = self
			.wakes(&self.alice, async || {
				self.bob
					.put("sendToDevice/m.test.journal/wake", &message)
					.await
					.map(drop)
			})
			.await?;

		require(
			seen["to_device"]["events"]
				.to_string()
				.contains("m.test.journal"),
			"the response omitted the to-device event",
		)
	}

	async fn message_apart(&self) -> Result {
		self.idles(async || {
			self.bob
				.send_text(&self.apart, "journal-apart", "elsewhere")
				.await
				.map(drop)
		})
		.await
	}

	async fn to_device_elsewhere(&self) -> Result {
		let device = device_of(&self.alice_second).await?;
		let message = json!({
			"messages": { self.alice_id.as_str(): { device.as_str(): { "seq": 2 } } },
		});

		self.idles(async || {
			self.bob
				.put("sendToDevice/m.test.journal/sleep", &message)
				.await
				.map(drop)
		})
		.await
	}

	async fn wakes<Act, Out>(&self, observer: &Client<'_>, act: Act) -> Result<(Value, Out)>
	where
		Act: AsyncFnOnce() -> Result<Out>,
	{
		let (seen, _, out) = self
			.polling(observer, WAKE_POLL_MS, WAKE_DEADLINE, act)
			.await?;

		self.woken.fetch_add(1, Ordering::Relaxed);

		Ok((seen, out))
	}

	async fn idles<Act>(&self, act: Act) -> Result
	where
		Act: AsyncFnOnce() -> Result,
	{
		let before = Counts::read(&self.registry.sync);
		let (seen, waited, ()) = self
			.polling(&self.alice, IDLE_POLL_MS, IDLE_DEADLINE, act)
			.await?;

		let after = Counts::read(&self.registry.sync);
		let poll = Duration::from_millis(IDLE_POLL_MS);

		require(!carries_data(&seen), "the idle long poll answered with data")?;
		require(
			waited >= poll.saturating_sub(IDLE_SLACK),
			&format!("the idle long poll answered after {waited:?}"),
		)?;

		match self.mode {
			| Mode::Journal => require(
				after.journal == before.journal && after.timeout > before.timeout,
				"the journal woke an idle long poll, or the poll did not end by its timeout",
			),
			| Mode::Verify => require(
				after.quiet > before.quiet
					&& after.extra == before.extra
					&& after.miss == before.miss,
				"the shadow comparison did not call the idle long poll quiet",
			),
			| Mode::Control => Ok(()),
		}
	}

	async fn polling<Act, Out>(
		&self,
		observer: &Client<'_>,
		poll_ms: u64,
		deadline: Duration,
		act: Act,
	) -> Result<(Value, Duration, Out)>
	where
		Act: AsyncFnOnce() -> Result<Out>,
	{
		self.until_polling(0, "a long poll of an earlier round was still waiting")
			.await?;

		let opening = observer.get("sync?timeout=0").await?;
		let since = field(&opening, "next_batch")?;
		let path = format!("sync?since={since}&timeout={poll_ms}");

		let polled = timeout(deadline, observer.get(&path).timed());
		let acting = async {
			self.until_polling(1, "the long poll did not begin to wait")
				.await?;

			act().await
		};

		let (polled, acted) = join(polled, acting).await;
		let out = acted?;
		let (seen, waited) =
			polled.map_err(|_| err!("the long poll did not answer in {deadline:?}"))?;

		Ok((seen?, waited, out))
	}

	async fn until_polling(&self, count: i64, failure: &str) -> Result {
		let reached = poll_until(GAUGE_DEADLINE, async || {
			self.registry.sync.long_polling(Api::V3) == count
		})
		.await;

		require(reached, failure)
	}

	fn finish(&self) -> Result {
		let sync = &self.registry.sync;
		let counts = Counts::read(sync);

		require(sync.long_polling(Api::V3) == 0, "a long poll was left in the gauge")?;

		let rounds = self.woken.load(Ordering::Relaxed);

		match self.mode {
			| Mode::Journal => {
				require(sync.v3_watcher_syncs() == 0, "a journal wait registered watchers")?;
				require(
					counts.journal >= rounds && counts.timeout >= IDLE_ROUNDS,
					"the journal waits were not counted by the reason they ended",
				)?;
				require(
					sync.wait_woken_total(WakeReason::Horizon) == 0
						&& sync.wait_woken_total(WakeReason::Shutdown) == 0,
					"a journal wait ended for the horizon or the shutdown",
				)?;

				counts.require_unverified()
			},
			| Mode::Verify => {
				require(sync.v3_watcher_syncs() > 0, "the shadow mode registered no watchers")?;
				require(counts.miss == 0, "the journal would have slept through a change")?;
				require(
					counts.both >= rounds && counts.quiet >= IDLE_ROUNDS,
					"the shadow verdicts were not counted",
				)?;

				counts.require_unwoken()
			},
			| Mode::Control => {
				require(sync.v3_watcher_syncs() > 0, "the long polls registered no watchers")?;

				counts
					.require_unwoken()
					.and_then(|()| counts.require_unverified())
			},
		}
	}
}

struct Counts {
	journal: u64,
	timeout: u64,
	horizon: u64,
	shutdown: u64,
	both: u64,
	miss: u64,
	spurious: u64,
	extra: u64,
	quiet: u64,
}

impl Counts {
	fn read(sync: &SyncMetrics) -> Self {
		Self {
			journal: sync.wait_woken_total(WakeReason::Journal),
			timeout: sync.wait_woken_total(WakeReason::Timeout),
			horizon: sync.wait_woken_total(WakeReason::Horizon),
			shutdown: sync.wait_woken_total(WakeReason::Shutdown),
			both: sync.wait_verified_total(Verdict::Both),
			miss: sync.wait_verified_total(Verdict::Miss),
			spurious: sync.wait_verified_total(Verdict::WatcherSpurious),
			extra: sync.wait_verified_total(Verdict::JournalExtra),
			quiet: sync.wait_verified_total(Verdict::Quiet),
		}
	}

	fn require_unwoken(&self) -> Result {
		let wakes = [self.journal, self.timeout, self.horizon, self.shutdown];

		require(
			wakes.iter().all(|&count| count == 0),
			"a long poll counted the reason it ended without waiting through the journal",
		)
	}

	fn require_unverified(&self) -> Result {
		let verdicts = [self.both, self.miss, self.spurious, self.extra, self.quiet];

		require(
			verdicts.iter().all(|&count| count == 0),
			"a long poll counted a shadow verdict without the shadow mode",
		)
	}
}

async fn device_of(client: &Client<'_>) -> Result<String> {
	let whoami = client.get("account/whoami").await?;

	Ok(field(&whoami, "device_id")?.to_owned())
}

fn device_keys(user_id: &UserId, device: &str) -> Value {
	json!({
		"device_keys": {
			"user_id": user_id,
			"device_id": device,
			"algorithms": [],
			"keys": { format!("curve25519:{device}"): KEY },
			"signatures": {},
		},
	})
}

fn in_room(response: &Value, room: &RoomId, section: &str, needle: &str) -> bool {
	response["rooms"]["join"][room.as_str()][section]
		.to_string()
		.contains(needle)
}

fn lists_as_changed(response: &Value, user_id: &UserId) -> bool {
	response["device_lists"]["changed"]
		.as_array()
		.into_iter()
		.flatten()
		.any(|user| user == user_id.as_str())
}

fn carries_data(response: &Value) -> bool {
	let rooms = ["join", "invite", "leave", "knock"]
		.into_iter()
		.any(|membership| filled(&response["rooms"][membership]));

	rooms
		|| filled(&response["account_data"]["events"])
		|| filled(&response["to_device"]["events"])
		|| filled(&response["device_lists"]["changed"])
		|| filled(&response["device_lists"]["left"])
}

fn filled(value: &Value) -> bool {
	value
		.as_array()
		.is_some_and(|items| !items.is_empty())
		|| value
			.as_object()
			.is_some_and(|items| !items.is_empty())
}
