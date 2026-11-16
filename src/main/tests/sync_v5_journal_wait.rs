#![cfg(test)]

use std::{
	sync::{
		Mutex, MutexGuard, PoisonError,
		atomic::{AtomicU64, Ordering},
	},
	time::Duration,
};

use futures::future::join;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};
use tuwunel_core::{
	Result, err,
	metrics::prom::{
		Api, JournalKind, Pass, Registry, SyncMetrics, Verdict, WaitStage, WakeReason,
	},
	ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId, events::room::member::MembershipState},
	utils::future::ExtExt,
};
use tuwunel_service::{Services, rooms::state_cache::MembershipUpdate};

use self::{
	account::account,
	client::{Client, field, poll_until},
	fixture::boot,
	require::require,
	state_cache::membership_update,
};

mod client;
mod fixture;
mod sliding_sync;

#[path = "client/account.rs"]
mod account;

#[path = "client/get.rs"]
mod get;

#[path = "client/membership.rs"]
mod membership;

#[path = "client/message.rs"]
mod message;

#[path = "client/put.rs"]
mod put;

#[path = "client/require.rs"]
mod require;

#[path = "fixture/state_cache.rs"]
mod state_cache;

const ALICE_TOKEN: &str = "sync-v5-journal-wait-alice-device-token-one";
const ALICE_SECOND_TOKEN: &str = "sync-v5-journal-wait-alice-device-token-two";
const BOB_TOKEN: &str = "sync-v5-journal-wait-bob-device-token-one";

const KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

/// Server options every run shares.
///
/// The runs boot one server each in this process, so none may claim the global
/// tracing subscriber. Presence is off so that pings move neither the counter
/// nor the responses, and a caught-up poll really waits.
const OPTIONS: [&str; 3] = [
	"logging.global_default=false",
	"presence.allow_local=false",
	"presence.allow_outgoing=false",
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

/// Rooms the user is invited to, which make a pass long enough for a write to
/// land inside it.
const CROWD: u32 = 1_500;

/// Messages written while a poll is being answered, each a step later than the
/// one before.
const RACES: u32 = 40;

/// The step between the delays of two races.
const RACE_STEP: Duration = Duration::from_millis(2);

/// The capacity of the journal that the horizon run starts with.
const HORIZON_CAPACITY: &str = "client.sync.journal_capacity=16";

/// Changes of one write, which the journal of that capacity cannot hold, so
/// its horizon moves past any position before them.
const BURST: u64 = 20;

/// How long the horizon run lets a poll take.
const HORIZON_DEADLINE: Duration = Duration::from_secs(6);

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
	Journal,
	Verify,
	Control,
}

impl Mode {
	fn option(self) -> &'static str {
		match self {
			| Self::Journal => "client.sync.v5_journal_wait=true",
			| Self::Verify => "client.sync.v5_journal_wait_verify=true",
			| Self::Control => "client.sync.v5_journal_wait=false",
		}
	}
}

#[test]
fn a_journal_wait_wakes_on_every_kind_of_change() -> Result {
	run("sync-v5-journal-wait", Mode::Journal)
}

#[test]
fn the_shadow_comparison_finds_no_miss() -> Result {
	run("sync-v5-journal-wait-verify", Mode::Verify)
}

#[test]
fn the_database_watchers_wake_on_the_same_changes_when_both_switches_are_off() -> Result {
	run("sync-v5-journal-wait-control", Mode::Control)
}

#[test]
fn a_change_written_during_a_pass_wakes_the_wait_that_follows_it() -> Result {
	let options = OPTIONS
		.into_iter()
		.chain([Mode::Journal.option()]);

	boot("sync-v5-journal-wait-crowd", options, exercise_crowd)
}

#[test]
fn a_wait_whose_journal_forgot_its_position_passes_again_and_loses_nothing() -> Result {
	let options = OPTIONS
		.into_iter()
		.chain([Mode::Journal.option(), HORIZON_CAPACITY]);

	boot("sync-v5-journal-wait-horizon", options, exercise_horizon)
}

fn run(name: &str, mode: Mode) -> Result {
	let options = OPTIONS.into_iter().chain([mode.option()]);

	boot(name, options, async |services, base| exercise(services, base, mode).await)
}

/// One sliding sync connection of a user, resumed from the position of its last
/// answer.
///
/// Like a client it hands the to-device token of an answer back in its next
/// request, which is how the server learns the events were received.
struct Conn<'a> {
	client: Client<'a>,
	body: Mutex<Value>,
	pos: Mutex<String>,
}

impl<'a> Conn<'a> {
	async fn open(
		services: &'a Services,
		base: &'a str,
		token: &'a str,
		body: Value,
	) -> Result<(Self, Value)> {
		let client = Client { services, base, token };
		let opening = client.sliding_sync(None, 0, &body).await?;
		let pos = Mutex::new(field(&opening, "pos")?.to_owned());

		Ok((Self { client, body: Mutex::new(body), pos }, opening))
	}

	fn pos(&self) -> String { locked(&self.pos).clone() }

	fn body(&self) -> Value { locked(&self.body).clone() }

	async fn poll(&self, poll_ms: u64) -> Result<(Value, Duration)> {
		let (pos, body) = (self.pos(), self.body());
		let (seen, waited) = self
			.client
			.sliding_sync(Some(&pos), poll_ms, &body)
			.timed()
			.await;

		let seen = seen?;
		*locked(&self.pos) = field(&seen, "pos")?.to_owned();

		if let Some(token) = seen["extensions"]["to_device"]["next_batch"].as_str() {
			locked(&self.body)["extensions"]["to_device"]["since"] = json!(token);
		}

		Ok((seen, waited))
	}

	async fn catch_up(&self) -> Result { self.poll(0).await.map(drop) }
}

async fn exercise(services: &Services, base: &str, mode: Mode) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "vjalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "vjbob", BOB_TOKEN).await?;

	services
		.users
		.create_device(&alice_id, None, (Some(ALICE_SECOND_TOKEN), None), None, None, None)
		.await?;

	let alice_second = Client {
		services,
		base,
		token: ALICE_SECOND_TOKEN,
	};

	let private = json!({ "preset": "private_chat" });
	let older = bob.create_room(&private).await?;
	bob.admit(&older, &alice, &alice_id).await?;

	let shared = bob.create_room(&private).await?;
	bob.admit(&shared, &alice, &alice_id).await?;

	let apart = bob
		.create_room(&json!({ "preset": "public_chat" }))
		.await?;

	let main = json!({
		"conn_id": "main",
		"lists": { "all": { "ranges": [[0, 9]], "required_state": [], "timeline_limit": 1 } },
		"extensions": {
			"account_data": { "enabled": true },
			"receipts": { "enabled": true },
			"typing": { "enabled": true },
		},
	});

	let narrow = json!({
		"conn_id": "narrow",
		"lists": { "all": { "ranges": [[0, 0]], "required_state": [], "timeline_limit": 1 } },
	});

	let keys = json!({ "conn_id": "keys", "extensions": { "e2ee": { "enabled": true } } });
	let device =
		json!({ "conn_id": "device", "extensions": { "to_device": { "enabled": true } } });

	let (main, _) = Conn::open(services, base, ALICE_TOKEN, main).await?;
	let (narrow, opening) = Conn::open(services, base, ALICE_TOKEN, narrow).await?;
	let (keys, _) = Conn::open(services, base, ALICE_TOKEN, keys).await?;
	let (device, _) = Conn::open(services, base, ALICE_TOKEN, device).await?;

	require(
		opening["rooms"][shared.as_str()].is_object()
			&& opening["rooms"][older.as_str()].is_null(),
		"the narrow window did not hold the newest room alone",
	)?;

	let world = World {
		mode,
		registry,
		alice,
		alice_second,
		alice_id,
		bob,
		bob_id,
		shared,
		older,
		apart,
		main,
		narrow,
		keys,
		device,
		woken: AtomicU64::new(0),
	};

	let outcomes = [
		("message", world.message().await),
		("message in a room outside the window", world.rises_into_the_window().await),
		("typing", world.typing().await),
		("receipt", world.receipt().await),
		("room account data", world.room_account_data().await),
		("global account data", world.global_account_data().await),
		("to-device event", world.to_device().await),
		("one-time keys", world.one_time_keys().await),
		("invite", world.invite().await),
		("kick", world.kick().await),
		("message written between two requests", world.writes_between_requests().await),
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
	shared: OwnedRoomId,
	older: OwnedRoomId,
	apart: OwnedRoomId,
	main: Conn<'a>,
	narrow: Conn<'a>,
	keys: Conn<'a>,
	device: Conn<'a>,
	woken: AtomicU64,
}

impl World<'_> {
	async fn message(&self) -> Result {
		let (seen, event) = self
			.wakes(&self.main, async || {
				self.bob
					.send_text(&self.shared, "v5-journal-message", "hello")
					.await
			})
			.await?;

		require(in_room(&seen, &self.shared, event.as_str()), "the response omitted the message")
	}

	async fn rises_into_the_window(&self) -> Result {
		let (seen, event) = self
			.wakes(&self.narrow, async || {
				self.bob
					.send_text(&self.older, "v5-journal-older", "rises")
					.await
			})
			.await?;

		require(
			in_room(&seen, &self.older, event.as_str()),
			"the response omitted the room that rose into the window",
		)
	}

	async fn typing(&self) -> Result {
		let path = format!("rooms/{}/typing/{}", self.shared, self.bob_id);
		let typing = json!({ "typing": true, "timeout": TYPING_MS });

		let (seen, ()) = self
			.wakes(&self.main, async || self.bob.put(&path, &typing).await.map(drop))
			.await?;

		require(
			in_extension(&seen, "typing", self.bob_id.as_str()),
			"the response omitted the typing notice",
		)
	}

	async fn receipt(&self) -> Result {
		let event = self
			.bob
			.send_text(&self.shared, "v5-journal-receipt-target", "target")
			.await?;

		let path = format!("rooms/{}/receipt/m.read/{event}", self.shared);

		let (seen, ()) = self
			.wakes(&self.main, async || self.bob.post(&path, &json!({})).await.map(drop))
			.await?;

		require(
			in_extension(&seen, "receipts", "m.receipt"),
			"the response omitted the read receipt",
		)
	}

	async fn room_account_data(&self) -> Result {
		let path = format!("user/{}/rooms/{}/tags/u.journal", self.alice_id, self.shared);

		let (seen, ()) = self
			.wakes(&self.main, async || {
				self.alice_second
					.put(&path, &json!({ "order": 0.5 }))
					.await
					.map(drop)
			})
			.await?;

		require(
			in_extension(&seen, "account_data", "m.tag"),
			"the response omitted the room account data",
		)
	}

	async fn global_account_data(&self) -> Result {
		let path = format!("user/{}/account_data/u.journal", self.alice_id);

		let (seen, ()) = self
			.wakes(&self.main, async || {
				self.alice_second
					.put(&path, &json!({ "kept": true }))
					.await
					.map(drop)
			})
			.await?;

		require(
			in_extension(&seen, "account_data", "u.journal"),
			"the response omitted the global account data",
		)
	}

	async fn to_device(&self) -> Result {
		let device = device_of(&self.alice).await?;
		let message = json!({
			"messages": { self.alice_id.as_str(): { device.as_str(): { "seq": 1 } } },
		});

		let (seen, ()) = self
			.wakes(&self.device, async || {
				self.bob
					.put("sendToDevice/m.test.journal/wake", &message)
					.await
					.map(drop)
			})
			.await?;

		require(
			in_extension(&seen, "to_device", "m.test.journal"),
			"the response omitted the to-device event",
		)
	}

	async fn one_time_keys(&self) -> Result {
		let upload = json!({ "one_time_keys": { "curve25519:AAAAAQ": KEY } });

		let (seen, ()) = self
			.wakes(&self.keys, async || {
				self.alice
					.post("keys/upload", &upload)
					.await
					.map(drop)
			})
			.await?;

		require(
			seen["extensions"]["e2ee"]["device_one_time_keys_count"]["curve25519"] == 1,
			"the response omitted the uploaded one-time key",
		)
	}

	async fn invite(&self) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		let (seen, ()) = self
			.wakes(&self.main, async || {
				self.bob
					.act_on(&room, "invite", &self.alice_id)
					.await
			})
			.await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "invite",
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
			.wakes(&self.main, async || {
				self.bob
					.act_on(&room, "kick", &self.alice_id)
					.await
			})
			.await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "leave",
			"the response omitted the room the user was removed from",
		)
	}

	async fn writes_between_requests(&self) -> Result {
		self.main.catch_up().await?;

		let event = self
			.bob
			.send_text(&self.shared, "v5-journal-between", "between")
			.await?;

		let (seen, _) = timeout(WAKE_DEADLINE, self.main.poll(WAKE_POLL_MS))
			.await
			.map_err(|_| err!("the poll slept through a message written before it began"))??;

		require(
			in_room(&seen, &self.shared, event.as_str()),
			"the response omitted the message written between two requests",
		)
	}

	async fn message_apart(&self) -> Result {
		self.idles(&self.main, async || {
			self.bob
				.send_text(&self.apart, "v5-journal-apart", "elsewhere")
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

		self.idles(&self.device, async || {
			self.bob
				.put("sendToDevice/m.test.journal/sleep", &message)
				.await
				.map(drop)
		})
		.await
	}

	async fn wakes<Act, Out>(&self, conn: &Conn<'_>, act: Act) -> Result<(Value, Out)>
	where
		Act: AsyncFnOnce() -> Result<Out>,
	{
		let (seen, _, out) = self
			.polling(conn, WAKE_POLL_MS, WAKE_DEADLINE, act)
			.await?;

		self.woken.fetch_add(1, Ordering::Relaxed);

		Ok((seen, out))
	}

	async fn idles<Act>(&self, conn: &Conn<'_>, act: Act) -> Result
	where
		Act: AsyncFnOnce() -> Result,
	{
		let before = Tally::read(&self.registry.sync);
		let (seen, waited, ()) = self
			.polling(conn, IDLE_POLL_MS, IDLE_DEADLINE, act)
			.await?;

		let after = Tally::read(&self.registry.sync);
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
		conn: &Conn<'_>,
		poll_ms: u64,
		deadline: Duration,
		act: Act,
	) -> Result<(Value, Duration, Out)>
	where
		Act: AsyncFnOnce() -> Result<Out>,
	{
		self.until_polling(0, "a long poll of an earlier round was still waiting")
			.await?;

		conn.catch_up().await?;

		let polled = timeout(deadline, conn.poll(poll_ms));
		let acting = async {
			self.until_polling(1, "the long poll did not begin to wait")
				.await?;

			act().await
		};

		let (polled, acted) = join(polled, acting).await;
		let out = acted?;
		let (seen, waited) =
			polled.map_err(|_| err!("the long poll did not answer in {deadline:?}"))??;

		Ok((seen, waited, out))
	}

	async fn until_polling(&self, count: i64, failure: &str) -> Result {
		let reached = poll_until(GAUGE_DEADLINE, async || {
			self.registry.sync.long_polling(Api::V5) == count
		})
		.await;

		require(reached, failure)
	}

	fn finish(&self) -> Result {
		let sync = &self.registry.sync;
		let rounds = self.woken.load(Ordering::Relaxed);
		let wakes = |reason| sync.v5_wait_woken_total(reason);

		require(sync.long_polling(Api::V5) == 0, "a long poll was left in the gauge")?;
		require(sync.v5_passes(Pass::Woken) > 0, "no woken pass was counted")?;

		match self.mode {
			| Mode::Journal => {
				require(sync.v3_watcher_syncs() == 0, "a journal wait registered watchers")?;
				require(
					wakes(WakeReason::Journal) >= rounds
						&& wakes(WakeReason::Timeout) >= IDLE_ROUNDS,
					"the journal waits were not counted by the reason they ended",
				)?;
				require(
					wakes(WakeReason::Horizon) == 0 && wakes(WakeReason::Shutdown) == 0,
					"a journal wait ended for the horizon or the shutdown",
				)?;

				require(
					verdicts(sync, None) == 0,
					"a shadow verdict was counted without the shadow mode",
				)
			},
			| Mode::Verify => {
				require(sync.v3_watcher_syncs() > 0, "the shadow mode registered no watchers")?;
				require(
					verdicts(sync, Some(Verdict::Miss)) == 0,
					"the journal would have slept through a change",
				)?;
				require(
					sync.v5_wait_verified_total(WaitStage::Woken, Verdict::Both) >= rounds
						&& sync.v5_wait_verified_total(WaitStage::Timeout, Verdict::Quiet)
							>= IDLE_ROUNDS,
					"the shadow verdicts were not counted",
				)?;

				require(
					unwoken(sync),
					"a long poll counted the reason it ended without waiting through the journal",
				)
			},
			| Mode::Control => {
				require(sync.v3_watcher_syncs() > 0, "the long polls registered no watchers")?;
				require(
					unwoken(sync) && verdicts(sync, None) == 0,
					"a long poll counted a journal wait or a shadow verdict with both switches \
					 off",
				)
			},
		}
	}
}

async fn exercise_crowd(services: &Services, base: &str) -> Result {
	let (alice_id, alice) = account(services, base, "vjcalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "vjcbob", BOB_TOKEN).await?;

	let shared = bob
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	bob.admit(&shared, &alice, &alice_id).await?;

	for index in 0..CROWD {
		let room = RoomId::parse(format!("!crowd{index}:remote.test"))?;
		let count = services.globals.next_count();
		let invite = membership_update(&room, &alice_id, MembershipState::Invite, *count);

		services
			.state_cache
			.update_membership(MembershipUpdate {
				sender: &bob_id,
				update_joined_count: false,
				..invite
			})
			.await?;
	}

	let body = json!({
		"conn_id": "crowd",
		"lists": { "all": { "ranges": [[0, 0]], "required_state": [], "timeline_limit": 1 } },
	});

	let (conn, _) = Conn::open(services, base, ALICE_TOKEN, body).await?;

	for race in 0..RACES {
		conn.catch_up().await?;

		let txn = format!("v5-journal-race-{race}");
		let poll = timeout(WAKE_DEADLINE, conn.poll(WAKE_POLL_MS));
		let write = async {
			sleep(RACE_STEP.saturating_mul(race)).await;

			bob.send_text(&shared, &txn, "race").await
		};

		let (polled, event) = join(poll, write).await;
		let event = event?;
		let (seen, _) = polled
			.map_err(|_| err!("the poll {race} slept through a message written during it"))??;

		require(
			in_room(&seen, &shared, event.as_str()),
			"the response omitted a message written during the request",
		)?;
	}

	Ok(())
}

async fn exercise_horizon(services: &Services, base: &str) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "vjhalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "vjhbob", BOB_TOKEN).await?;

	let shared = bob
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	bob.admit(&shared, &alice, &alice_id).await?;

	let body = json!({
		"conn_id": "horizon",
		"lists": { "all": { "ranges": [[0, 9]], "required_state": [], "timeline_limit": 1 } },
	});

	let (conn, _) = Conn::open(services, base, ALICE_TOKEN, body).await?;

	conn.catch_up().await?;

	let poll = timeout(HORIZON_DEADLINE, conn.poll(WAKE_POLL_MS));
	let acting = async {
		let waiting =
			poll_until(GAUGE_DEADLINE, async || registry.sync.long_polling(Api::V5) == 1).await;

		require(waiting, "the long poll did not begin to wait")?;
		burst(services, &bob_id);

		let forgotten = poll_until(GAUGE_DEADLINE, async || {
			registry
				.sync
				.v5_wait_woken_total(WakeReason::Horizon)
				> 0
		})
		.await;

		require(forgotten, "the journal outran the wait without ending it")?;

		bob.send_text(&shared, "v5-journal-after-horizon", "kept")
			.await
	};

	let (polled, event) = join(poll, acting).await;
	let event = event?;
	let (seen, _) =
		polled.map_err(|_| err!("the poll slept through a message after the horizon"))??;

	require(
		in_room(&seen, &shared, event.as_str()),
		"the response omitted the message written after the wait outlived its horizon",
	)?;

	let stale = conn.pos();

	burst(services, &bob_id);

	let event = bob
		.send_text(&shared, "v5-journal-below-horizon", "kept")
		.await?;

	let body = conn.body();
	let (seen, waited) = timeout(
		HORIZON_DEADLINE,
		alice
			.sliding_sync(Some(&stale), WAKE_POLL_MS, &body)
			.timed(),
	)
	.await
	.map_err(|_| err!("a position below the horizon was not answered"))?;

	require(
		in_room(&seen?, &shared, event.as_str()),
		&format!("the response to a position below the horizon omitted the message ({waited:?})"),
	)?;

	require(registry.sync.long_polling(Api::V5) == 0, "a long poll was left in the gauge")
}

fn burst(services: &Services, user: &UserId) {
	let count = services.globals.next_count();

	for _ in 0..BURST {
		services
			.sync
			.journal
			.record_user(*count, JournalKind::ToDevice, user, None);
	}
}

/// What a run counted so far of the idle round's evidence.
struct Tally {
	journal: u64,
	timeout: u64,
	miss: u64,
	extra: u64,
	quiet: u64,
}

impl Tally {
	fn read(sync: &SyncMetrics) -> Self {
		Self {
			journal: sync.v5_wait_woken_total(WakeReason::Journal),
			timeout: sync.v5_wait_woken_total(WakeReason::Timeout),
			miss: verdicts(sync, Some(Verdict::Miss)),
			extra: sync.v5_wait_verified_total(WaitStage::Timeout, Verdict::JournalExtra),
			quiet: sync.v5_wait_verified_total(WaitStage::Timeout, Verdict::Quiet),
		}
	}
}

/// How many verdicts of one kind, or of every kind, the shadow comparison
/// counted at both stages.
fn verdicts(sync: &SyncMetrics, only: Option<Verdict>) -> u64 {
	[
		Verdict::Both,
		Verdict::Miss,
		Verdict::WatcherSpurious,
		Verdict::JournalExtra,
		Verdict::Quiet,
	]
	.into_iter()
	.filter(|verdict| only.is_none_or(|only| only == *verdict))
	.flat_map(|verdict| {
		[WaitStage::Woken, WaitStage::Timeout]
			.into_iter()
			.map(move |stage| sync.v5_wait_verified_total(stage, verdict))
	})
	.sum()
}

fn unwoken(sync: &SyncMetrics) -> bool {
	[
		WakeReason::Journal,
		WakeReason::Horizon,
		WakeReason::Timeout,
		WakeReason::Shutdown,
	]
	.into_iter()
	.all(|reason| sync.v5_wait_woken_total(reason) == 0)
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
	mutex
		.lock()
		.unwrap_or_else(PoisonError::into_inner)
}

async fn device_of(client: &Client<'_>) -> Result<String> {
	let whoami = client.get("account/whoami").await?;

	Ok(field(&whoami, "device_id")?.to_owned())
}

fn in_room(response: &Value, room: &RoomId, needle: &str) -> bool {
	response["rooms"][room.as_str()]["timeline"]
		.to_string()
		.contains(needle)
}

fn in_extension(response: &Value, extension: &str, needle: &str) -> bool {
	response["extensions"][extension]
		.to_string()
		.contains(needle)
}

fn carries_data(response: &Value) -> bool {
	filled(&response["rooms"]) || filled(&response["extensions"])
}

fn filled(value: &Value) -> bool {
	value
		.as_array()
		.is_some_and(|items| !items.is_empty())
		|| value
			.as_object()
			.is_some_and(|items| !items.is_empty())
}
