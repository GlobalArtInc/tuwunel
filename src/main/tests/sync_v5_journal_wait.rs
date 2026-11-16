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
		Api, JournalKind, Pass, Registry, SyncMetrics, V5Path, V5Phase, Verdict, WaitStage,
		WakeReason,
	},
	ruma::{
		OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
		events::room::member::MembershipState,
	},
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
/// nor the responses, and a caught-up poll really waits. The shortest typing
/// timeout is one second, so that the typing of a round can run out within it.
const OPTIONS: [&str; 4] = [
	"logging.global_default=false",
	"presence.allow_local=false",
	"presence.allow_outgoing=false",
	"typing.client_timeout_min_s=1",
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

/// How long the typist of the round that lets typing run out keeps typing.
const FADE_MS: u64 = 1_500;

/// How long after its typist stopped a poll may take to hear of it.
const FADE_SLACK: Duration = Duration::from_secs(3);

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

/// To-device events sent before the tail run opens its connection, which it
/// receives two at a time.
const TAIL: u64 = 5;

/// To-device events the tail run's connection takes in one answer.
const TAIL_LIMIT: u64 = 2;

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

#[test]
fn a_change_outside_the_window_wakes_the_wait_once_and_not_again() -> Result {
	let options = OPTIONS
		.into_iter()
		.chain([Mode::Journal.option()]);

	boot("sync-v5-journal-wait-outside", options, exercise_outside)
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
	lists: Mutex<Value>,
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
		let lists = Mutex::new(opening["lists"].clone());

		Ok((
			Self {
				client,
				body: Mutex::new(body),
				pos,
				lists,
			},
			opening,
		))
	}

	fn pos(&self) -> String { locked(&self.pos).clone() }

	fn lists(&self) -> Value { locked(&self.lists).clone() }

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
		*locked(&self.lists) = seen["lists"].clone();

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
		("quiet rounds", world.quiet_rounds().await),
		("quiet rounds with the e2ee extension", world.quiet_rounds_with_keys().await),
		("change of the ranges", world.reshaped().await),
		("to-device events past the limit", world.to_device_tail().await),
		("change of an extension setting", world.settings_change().await),
		("typing that runs out", world.typing_runs_out().await),
		("resuming after quiet rounds", world.resumes_after_quiet_rounds().await),
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

		let before = Tally::read(&self.registry.sync);
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
		)?;

		let after = Tally::read(&self.registry.sync);

		require(
			self.mode != Mode::Journal
				|| after.journal > before.journal && after.timeout == before.timeout,
			"the gate did not find the message written before its wait began",
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

	async fn quiet_rounds(&self) -> Result { self.quiet(&self.main, false).await }

	async fn quiet_rounds_with_keys(&self) -> Result { self.quiet(&self.keys, true).await }

	async fn quiet(&self, conn: &Conn<'_>, keys: bool) -> Result {
		self.until_polling(0, "a long poll of an earlier round was still waiting")
			.await?;

		conn.catch_up().await?;

		let (pos, lists) = (conn.pos(), conn.lists());
		let before = Tally::read(&self.registry.sync);
		let poll = Duration::from_millis(IDLE_POLL_MS);

		for round in 0..IDLE_ROUNDS {
			let (seen, waited) = timeout(IDLE_DEADLINE, conn.poll(IDLE_POLL_MS))
				.await
				.map_err(|_| err!("quiet round {round} did not answer in {IDLE_DEADLINE:?}"))??;

			require(
				waited >= poll.saturating_sub(IDLE_SLACK),
				&format!("quiet round {round} answered after {waited:?}"),
			)?;
			require(
				quiet_answer(&seen, keys),
				&format!("quiet round {round} answered with data: {seen}"),
			)?;
			require(
				seen["lists"] == lists,
				&format!("quiet round {round} changed the list counts: {seen}"),
			)?;

			if keys && self.mode == Mode::Journal {
				require(
					seen["extensions"]["e2ee"]["device_one_time_keys_count"]["curve25519"] == 1
						&& seen["extensions"]["e2ee"]["device_unused_fallback_key_types"]
							.is_array(),
					&format!("quiet round {round} omitted the key counts: {seen}"),
				)?;
			}

			if self.mode == Mode::Journal {
				require(
					seen["pos"] == pos && conn.pos() == pos,
					&format!("quiet round {round} moved the position: {seen}"),
				)?;
			}
		}

		if self.mode != Mode::Journal {
			return Ok(());
		}

		let after = Tally::read(&self.registry.sync);

		require(after.passes == before.passes, "a quiet round ran a pass")?;
		require(after.stores == before.stores, "a quiet round saved the connection")?;
		require(
			after.answered == before.answered.saturating_add(IDLE_ROUNDS),
			"the quiet rounds were not counted as answered without a pass",
		)?;
		require(
			after.journal == before.journal
				&& after.timeout == before.timeout.saturating_add(IDLE_ROUNDS),
			"a quiet round did not end by its timeout alone",
		)
	}

	async fn reshaped(&self) -> Result {
		let body = json!({
			"conn_id": "reshape",
			"lists": { "all": { "ranges": [[0, 0]], "required_state": [], "timeline_limit": 1 } },
		});

		let (conn, opening) =
			Conn::open(self.alice.services, self.alice.base, ALICE_TOKEN, body).await?;

		conn.catch_up().await?;

		locked(&conn.body)["lists"]["all"]["ranges"] = json!([[0, 9]]);

		let (seen, _) = timeout(WAKE_DEADLINE, conn.poll(WAKE_POLL_MS))
			.await
			.map_err(|_| err!("the poll with new ranges waited for a change"))??;

		require(
			room_count(&seen) > room_count(&opening),
			"the response to new ranges omitted the rooms they added",
		)
	}

	async fn typing_runs_out(&self) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		self.bob
			.admit(&room, &self.alice, &self.alice_id)
			.await?;

		let body = json!({
			"conn_id": "fading",
			"lists": { "all": { "ranges": [[0, 9]], "required_state": [], "timeline_limit": 1 } },
			"extensions": { "typing": { "enabled": true } },
		});

		let (conn, _) =
			Conn::open(self.alice.services, self.alice.base, ALICE_TOKEN, body).await?;

		let path = format!("rooms/{room}/typing/{}", self.bob_id);
		let typing = json!({ "typing": true, "timeout": FADE_MS });

		let (seen, ()) = self
			.wakes(&conn, async || self.bob.put(&path, &typing).await.map(drop))
			.await?;

		require(
			in_extension(&seen, "typing", self.bob_id.as_str()),
			"the response omitted the typing notice",
		)?;

		let deadline = Duration::from_millis(FADE_MS).saturating_add(FADE_SLACK);
		let (seen, _) = timeout(deadline, conn.poll(WAKE_POLL_MS))
			.await
			.map_err(|_| {
				err!("the long poll was not told within {deadline:?} that typing ran out")
			})??;

		require(
			seen["extensions"]["typing"]["rooms"][room.as_str()]["content"]["user_ids"]
				== json!([]),
			&format!("the response omitted the end of the typing: {seen}"),
		)
	}

	async fn resumes_after_quiet_rounds(&self) -> Result {
		let body = json!({
			"conn_id": "resume",
			"lists": { "all": { "ranges": [[0, 9]], "required_state": [], "timeline_limit": 1 } },
		});

		let (conn, _) =
			Conn::open(self.alice.services, self.alice.base, ALICE_TOKEN, body).await?;

		conn.catch_up().await?;

		let resumed = conn.pos();

		for round in 0..IDLE_ROUNDS {
			let (seen, _) = timeout(IDLE_DEADLINE, conn.poll(IDLE_POLL_MS))
				.await
				.map_err(|_| err!("quiet round {round} did not answer in {IDLE_DEADLINE:?}"))??;

			require(!carries_data(&seen), &format!("quiet round {round} answered with data"))?;
		}

		let (seen, first) = self
			.message_during_poll(&conn, "v5-journal-resume-first")
			.await?;

		require(
			in_room(&seen, &self.shared, first.as_str()) && field(&seen, "pos")? != resumed,
			"the message after the quiet rounds was not delivered at a new position",
		)?;

		let body = conn.body();
		let replayed = self
			.alice
			.sliding_sync(Some(&resumed), 0, &body)
			.await?;

		require(
			in_room(&replayed, &self.shared, first.as_str()),
			"the replay of the position the quiet rounds resumed from omitted the message",
		)?;

		*locked(&conn.pos) = field(&replayed, "pos")?.to_owned();

		let (seen, _) = timeout(IDLE_DEADLINE, conn.poll(IDLE_POLL_MS))
			.await
			.map_err(|_| {
				err!("the round after the replay did not answer in {IDLE_DEADLINE:?}")
			})??;

		require(!carries_data(&seen), "the round after the replay answered with data")?;

		let before = conn.pos();
		let (seen, second) = self
			.message_during_poll(&conn, "v5-journal-resume-second")
			.await?;

		require(
			in_room(&seen, &self.shared, second.as_str()) && field(&seen, "pos")? != before,
			"the message after the replay was not delivered at a new position",
		)
	}

	async fn message_during_poll(
		&self,
		conn: &Conn<'_>,
		txn: &str,
	) -> Result<(Value, OwnedEventId)> {
		self.until_polling(0, "a long poll of an earlier round was still waiting")
			.await?;

		let polled = timeout(WAKE_DEADLINE, conn.poll(WAKE_POLL_MS));
		let acting = async {
			self.until_polling(1, "the long poll did not begin to wait")
				.await?;

			self.bob
				.send_text(&self.shared, txn, "resumed")
				.await
		};

		let (polled, event) = join(polled, acting).await;
		let event = event?;
		let (seen, _) = polled
			.map_err(|_| err!("the long poll slept through a message written during it"))??;

		Ok((seen, event))
	}

	async fn settings_change(&self) -> Result {
		self.until_polling(0, "a long poll of an earlier round was still waiting")
			.await?;

		let body = json!({
			"conn_id": "settings",
			"lists": { "all": { "ranges": [[0, 0]], "required_state": [], "timeline_limit": 1 } },
		});

		let (conn, _) =
			Conn::open(self.alice.services, self.alice.base, ALICE_TOKEN, body).await?;

		conn.catch_up().await?;

		locked(&conn.body)["extensions"] = json!({ "typing": { "enabled": true } });

		let before = Tally::read(&self.registry.sync);

		timeout(IDLE_DEADLINE, conn.poll(IDLE_POLL_MS))
			.await
			.map_err(|_| {
				err!("the poll that switched an extension on did not answer in time")
			})??;

		let after = Tally::read(&self.registry.sync);

		require(
			after.passes > before.passes
				&& after.stores > before.stores
				&& after.answered == before.answered,
			"a request that only switched an extension on was answered without a pass that \
			 saved the connection",
		)
	}

	async fn to_device_tail(&self) -> Result {
		let device = device_of(&self.alice).await?;

		for seq in 0..TAIL {
			let message = json!({
				"messages": { self.alice_id.as_str(): { device.as_str(): { "seq": seq } } },
			});

			self.bob
				.put(&format!("sendToDevice/m.test.tail/tail{seq}"), &message)
				.await?;
		}

		let body = json!({
			"conn_id": "tail",
			"extensions": { "to_device": { "enabled": true, "limit": TAIL_LIMIT } },
		});

		let (conn, opening) =
			Conn::open(self.alice.services, self.alice.base, ALICE_TOKEN, body).await?;

		if let Some(token) = opening["extensions"]["to_device"]["next_batch"].as_str() {
			locked(&conn.body)["extensions"]["to_device"]["since"] = json!(token);
		}

		let mut received = tail_events(&opening);

		for answer in 0..TAIL {
			if received.len() >= usize::try_from(TAIL)? {
				break;
			}

			let (seen, _) = timeout(WAKE_DEADLINE, conn.poll(WAKE_POLL_MS))
				.await
				.map_err(|_| {
					err!("answer {answer} waited for a change while to-device events were left")
				})??;

			received.extend(tail_events(&seen));
		}

		received.sort_unstable();
		received.dedup();

		require(
			received == (0..TAIL).collect::<Vec<_>>(),
			&format!("the to-device events received were {received:?}"),
		)
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
		require(
			sync.v5_requests_total(V5Path::Pass) > 0,
			"no request was counted as answered after a pass",
		)?;
		require(
			sync.v5_fresh_payload_passes() > 0,
			"no pass counted the rooms it built a payload for",
		)?;

		let collected = sync.v5_phases(V5Phase::Collect);

		require(
			collected > 0
				&& sync.v5_phases(V5Phase::Extensions) == collected
				&& sync.v5_phases(V5Phase::Profiles) == collected
				&& sync.v5_phases(V5Phase::Select) >= collected,
			"the phases of a pass were not timed once each",
		)?;

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
					sync.v5_requests_total(V5Path::Quiet) >= IDLE_ROUNDS,
					"no request was counted as answered without a pass",
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
					sync.v5_wait_verified_total(WaitStage::Gate, Verdict::Both) > 0
						&& sync.v5_wait_verified_total(WaitStage::Gate, Verdict::Quiet)
							>= IDLE_ROUNDS,
					"the first passes of the requests the gate would hold were not compared",
				)?;

				require(
					sync.v5_requests_total(V5Path::Quiet) == 0,
					"the shadow mode answered a request without a pass",
				)?;

				require(
					unwoken(sync),
					"a long poll counted the reason it ended without waiting through the journal",
				)
			},
			| Mode::Control => {
				require(sync.v3_watcher_syncs() > 0, "the long polls registered no watchers")?;
				require(
					sync.v5_requests_total(V5Path::Quiet) == 0,
					"a request was answered without a pass with both switches off",
				)?;

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

	for lost_first in [false, true] {
		horizon_round(services, registry, (&conn, &bob), (&shared, &bob_id), lost_first).await?;
	}

	require(registry.sync.long_polling(Api::V5) == 0, "a long poll was left in the gauge")
}

/// One request resumed from the position of the last answer, whose journal
/// loses that position and which is then given a message to find.
///
/// The request waits at the gate. When `lost_first` is set the journal loses the
/// position before the request is sent, so the gate meets a position below the
/// horizon on arrival; otherwise the request is waiting when the journal loses it.
/// Either way the loss is counted as the reason the gate's wait ended before the
/// message is written, so an answer without a pass would carry no message. The
/// answer must hold the message and a position beyond the one the request resumed
/// from.
async fn horizon_round(
	services: &Services,
	registry: &Registry,
	(conn, bob): (&Conn<'_>, &Client<'_>),
	(room, bob_id): (&RoomId, &UserId),
	lost_first: bool,
) -> Result {
	let stale = conn.pos();
	let lost = || {
		registry
			.sync
			.v5_wait_woken_total(WakeReason::Horizon)
	};
	let before = lost();

	if lost_first {
		burst(services, bob_id);
	}

	let poll = timeout(HORIZON_DEADLINE, conn.poll(WAKE_POLL_MS));
	let acting = async {
		if !lost_first {
			let waiting =
				poll_until(GAUGE_DEADLINE, async || registry.sync.long_polling(Api::V5) == 1)
					.await;

			require(waiting, "the request did not begin to wait at the gate")?;
			burst(services, bob_id);
		}

		let forgotten = poll_until(GAUGE_DEADLINE, async || lost() > before).await;

		require(forgotten, "the gate was not ended by the journal losing its position")?;

		bob.send_text(room, &format!("v5-journal-horizon-{lost_first}"), "kept")
			.await
	};

	let (polled, event) = join(poll, acting).await;
	let event = event?;
	let (seen, _) = polled.map_err(|_| {
		err!("the request slept through a message after the journal lost its position")
	})??;

	require(
		in_room(&seen, room, event.as_str()),
		"the answer omitted the message written after the journal lost the position",
	)?;

	require(
		field(&seen, "pos")? != stale,
		"the answer to a position the journal lost repeated the position",
	)
}

async fn exercise_outside(services: &Services, base: &str) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "vjoalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "vjobob", BOB_TOKEN).await?;

	let private = json!({ "preset": "private_chat" });
	let older = bob.create_room(&private).await?;
	bob.admit(&older, &alice, &alice_id).await?;

	let newer = bob.create_room(&private).await?;
	bob.admit(&newer, &alice, &alice_id).await?;

	let body = json!({
		"conn_id": "outside",
		"lists": { "all": { "ranges": [[0, 0]], "required_state": [], "timeline_limit": 1 } },
	});

	let (conn, opening) = Conn::open(services, base, ALICE_TOKEN, body).await?;

	require(
		opening["rooms"][newer.as_str()].is_object()
			&& opening["rooms"][older.as_str()].is_null(),
		"the window did not hold the newest room alone",
	)?;

	conn.catch_up().await?;

	let passes = || {
		registry
			.sync
			.v5_passes(Pass::First)
			.saturating_add(registry.sync.v5_passes(Pass::Woken))
	};

	let before = passes();
	let path = format!("rooms/{older}/typing/{bob_id}");
	let typing = json!({ "typing": true, "timeout": TYPING_MS });

	let poll = timeout(IDLE_DEADLINE, conn.poll(IDLE_POLL_MS));
	let acting = async {
		let waiting =
			poll_until(GAUGE_DEADLINE, async || registry.sync.long_polling(Api::V5) == 1).await;

		require(waiting, "the long poll did not begin to wait")?;

		bob.put(&path, &typing).await.map(drop)
	};

	let (polled, acted) = join(poll, acting).await;
	acted?;
	let (seen, waited) = polled.map_err(|_| err!("the long poll did not answer in time"))??;

	require(
		!carries_data(&seen),
		"a typing notice outside the window answered the long poll",
	)?;
	require(
		waited >= Duration::from_millis(IDLE_POLL_MS).saturating_sub(IDLE_SLACK),
		&format!("the long poll was answered after {waited:?} instead of by its timeout"),
	)?;
	require(
		passes().saturating_sub(before) == 1,
		"a change outside the window woke the long poll more than once",
	)
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
	passes: u64,
	stores: u64,
	answered: u64,
}

impl Tally {
	fn read(sync: &SyncMetrics) -> Self {
		Self {
			journal: sync.v5_wait_woken_total(WakeReason::Journal),
			timeout: sync.v5_wait_woken_total(WakeReason::Timeout),
			miss: verdicts(sync, Some(Verdict::Miss)),
			extra: sync.v5_wait_verified_total(WaitStage::Timeout, Verdict::JournalExtra),
			quiet: sync.v5_wait_verified_total(WaitStage::Timeout, Verdict::Quiet),
			passes: sync
				.v5_passes(Pass::First)
				.saturating_add(sync.v5_passes(Pass::Woken)),
			stores: sync.v5_states(),
			answered: sync.v5_requests_total(V5Path::Quiet),
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
		[WaitStage::Gate, WaitStage::Woken, WaitStage::Timeout]
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

fn quiet_answer(response: &Value, keys: bool) -> bool {
	if !keys {
		return !carries_data(response);
	}

	let only = |value: &Value, allowed: fn(&str) -> bool| {
		value
			.as_object()
			.is_none_or(|items| items.keys().all(|name| allowed(name)))
	};

	!filled(&response["rooms"])
		&& only(&response["extensions"], |name| name == "e2ee")
		&& only(&response["extensions"]["e2ee"], |name| {
			matches!(name, "device_one_time_keys_count" | "device_unused_fallback_key_types")
		})
}

fn tail_events(response: &Value) -> Vec<u64> {
	response["extensions"]["to_device"]["events"]
		.as_array()
		.into_iter()
		.flatten()
		.filter(|event| event["type"] == "m.test.tail")
		.filter_map(|event| event["content"]["seq"].as_u64())
		.collect()
}

fn room_count(response: &Value) -> usize {
	response["rooms"]
		.as_object()
		.map_or(0, serde_json::Map::len)
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
