#![cfg(test)]

use std::{
	collections::BTreeSet,
	sync::atomic::{AtomicBool, Ordering},
	time::Duration,
};

use futures::future::join;
use reqwest::Method;
use serde_json::{Value, json};
use tokio::time::timeout;
use tuwunel_core::{
	Result, err,
	metrics::prom::{
		Api, JournalKind, Mismatch, Registry, SelectPath, SelectReason, Skipped, SyncMetrics,
	},
	ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::Services;

use self::{
	account::account,
	client::{Client, accepted, field, poll_until},
	fixture::boot,
	require::require,
};

mod client;
mod fixture;
mod sliding_sync;

#[path = "client/account.rs"]
mod account;

#[path = "client/membership.rs"]
mod membership;

#[path = "client/message.rs"]
mod message;

#[path = "client/put.rs"]
mod put;

#[path = "client/require.rs"]
mod require;

const ALICE_TOKEN: &str = "sync-v5-candidates-alice-device-token-one";
const BOB_TOKEN: &str = "sync-v5-candidates-bob-device-token-one";

/// Server options every run shares.
///
/// The runs boot one server each in this process, so none may claim the global
/// tracing subscriber. Presence is off so that pings move neither the counter
/// nor the responses.
const OPTIONS: [&str; 3] = [
	"logging.global_default=false",
	"presence.allow_local=false",
	"presence.allow_outgoing=false",
];

/// Rooms the user shares with the other, the oldest first.
const ROOMS: usize = 12;

/// The capacity of the journal of the run that makes it lose a position.
const HORIZON_CAPACITY: &str = "client.sync.journal_capacity=16";

/// Changes of one write, which the journal of that capacity cannot hold.
const BURST: u32 = 20;

/// Passes the run that writes during them makes at least and at most, and rooms
/// it shares.
const RACE_PASSES: usize = 60;
const RACE_LIMIT: usize = 3000;
const RACE_ROOMS: usize = 6;

/// How long a long poll of a run may wait for a change, how long one that a
/// write wakes may take to answer, and how long the harness waits for a poll to
/// begin to wait.
const WAKE_POLL_MS: u64 = 5_000;
const WAKE_DEADLINE: Duration = Duration::from_secs(3);
const GAUGE_DEADLINE: Duration = Duration::from_secs(5);

/// The tag the tagged list filters by.
const TAG: &str = "u.pick";

/// Every path a pass can be counted under.
const PATHS: [(SelectPath, SelectReason); 11] = [
	(SelectPath::Candidates, SelectReason::None),
	(SelectPath::Full, SelectReason::Off),
	(SelectPath::Full, SelectReason::NoIndex),
	(SelectPath::Full, SelectReason::Replay),
	(SelectPath::Full, SelectReason::Filters),
	(SelectPath::Full, SelectReason::Spaces),
	(SelectPath::Full, SelectReason::Refresh),
	(SelectPath::Full, SelectReason::Horizon),
	(SelectPath::Full, SelectReason::Overflow),
	(SelectPath::Full, SelectReason::AccountData),
	(SelectPath::Full, SelectReason::Error),
];

type Path = (SelectPath, SelectReason);

const CANDIDATES: Path = (SelectPath::Candidates, SelectReason::None);

#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
	Candidates,
	Verify,
	Control,
}

impl Mode {
	fn option(self) -> &'static str {
		match self {
			| Self::Candidates => "client.sync.v5_candidates=true",
			| Self::Verify => "client.sync.v5_candidates_verify=true",
			| Self::Control => "client.sync.v5_candidates=false",
		}
	}

	/// Where the mode takes a pass that the index would take, or that has the
	/// given reason to look at every room.
	fn expect(self, path: Path) -> Path {
		match self {
			| Self::Control => (SelectPath::Full, SelectReason::Off),
			| Self::Candidates | Self::Verify => path,
		}
	}
}

#[test]
fn the_index_selects_the_rooms_a_full_pass_does() -> Result {
	run("sync-v5-candidates", Mode::Candidates)
}

#[test]
fn the_shadow_comparison_finds_no_difference() -> Result {
	run("sync-v5-candidates-verify", Mode::Verify)
}

#[test]
fn every_pass_looks_at_every_room_when_both_switches_are_off() -> Result {
	run("sync-v5-candidates-control", Mode::Control)
}

#[test]
fn a_journal_that_forgot_the_index_sends_the_pass_to_every_room_and_loses_no_room() -> Result {
	let options = OPTIONS
		.into_iter()
		.chain([Mode::Candidates.option(), HORIZON_CAPACITY]);

	boot("sync-v5-candidates-horizon", options, exercise_horizon)
}

#[test]
fn a_write_during_a_pass_is_no_difference_of_the_index() -> Result {
	let options = OPTIONS.into_iter().chain([Mode::Verify.option()]);

	boot("sync-v5-candidates-race", options, exercise_race)
}

fn run(name: &str, mode: Mode) -> Result {
	let options = OPTIONS.into_iter().chain([mode.option()]);

	boot(name, options, async |services, base| exercise(services, base, mode).await)
}

/// One sliding sync connection of a user, resumed from the position of its last
/// answer with every request asking for no wait.
struct Conn<'a> {
	client: &'a Client<'a>,
	body: Value,
	pos: String,
}

impl<'a> Conn<'a> {
	async fn open(client: &'a Client<'a>, body: Value) -> Result<(Self, Value)> {
		let opening = client.sliding_sync(None, 0, &body).await?;
		let pos = field(&opening, "pos")?.to_owned();

		Ok((Self { client, body, pos }, opening))
	}

	async fn poll(&mut self) -> Result<Value> { self.poll_for(0).await }

	async fn poll_for(&mut self, wait: u64) -> Result<Value> {
		let seen = self
			.client
			.sliding_sync(Some(&self.pos), wait, &self.body)
			.await?;

		self.pos = field(&seen, "pos")?.to_owned();

		Ok(seen)
	}
}

struct World<'a> {
	mode: Mode,
	services: &'a Services,
	registry: &'a Registry,
	alice: Client<'a>,
	alice_id: OwnedUserId,
	bob: Client<'a>,
	bob_id: OwnedUserId,
	rooms: Vec<OwnedRoomId>,
}

impl World<'_> {
	/// Runs one request and checks the one pass it made was counted under the
	/// path expected for the mode.
	async fn pass(&self, conn: &mut Conn<'_>, expected: Path, what: &str) -> Result<Value> {
		let before = snapshot(&self.registry.sync);
		let seen = conn.poll().await?;

		self.counted(&before, expected, what)?;

		Ok(seen)
	}

	/// Runs one request like `pass`, and checks how many rooms of its window it
	/// collected the ranges of: the `owed` ones when the index chose the rooms, and
	/// all of the `window` otherwise.
	async fn collecting(
		&self,
		conn: &mut Conn<'_>,
		expected: Path,
		what: &str,
		(owed, window): (usize, usize),
	) -> Result<Value> {
		let before = self.collected()?;
		let seen = self.pass(conn, expected, what).await?;

		self.collected_since(before, what, (owed, window))?;

		Ok(seen)
	}

	/// Runs one request that waits for a change, as `collecting` does, with
	/// `write` made once it waits. The comparison with the full pass must find no
	/// room of the window left out that has something to send.
	async fn waking<Write>(
		&self,
		conn: &mut Conn<'_>,
		what: &str,
		counts: (usize, usize),
		write: Write,
	) -> Result<Value>
	where
		Write: Future<Output = Result>,
	{
		let sync = &self.registry.sync;
		let before = snapshot(sync);
		let collected = self.collected()?;
		let range = sync.v5_candidates_mismatched_total(Mismatch::Range);

		let polled = timeout(WAKE_DEADLINE, conn.poll_for(WAKE_POLL_MS));
		let writing = async {
			let waiting =
				poll_until(GAUGE_DEADLINE, async || sync.long_polling(Api::V5) == 1).await;

			require(waiting, "the long poll did not begin to wait")?;

			write.await
		};

		let (polled, written) = join(polled, writing).await;

		written?;

		let seen =
			polled.map_err(|_| err!("{what}: the long poll slept through the write"))??;

		self.counted(&before, CANDIDATES, what)?;
		self.collected_since(collected, what, counts)?;

		require(
			sync.v5_candidates_mismatched_total(Mismatch::Range) == range,
			&format!(
				"{what}: the full pass found something to send in a room the index left out"
			),
		)?;

		Ok(seen)
	}

	fn collected_since(
		&self,
		before: usize,
		what: &str,
		(owed, window): (usize, usize),
	) -> Result {
		let collected = self.collected()?.saturating_sub(before);
		let wanted = if self.mode == Mode::Candidates { owed } else { window };

		require(
			collected == wanted,
			&format!(
				"{what}: the pass collected the ranges of {collected} rooms instead of {wanted}"
			),
		)
	}

	fn collected(&self) -> Result<usize> {
		self.registry
			.render()
			.lines()
			.find_map(|line| line.strip_prefix("tuwunel_sync_v5_ranges_per_pass_sum "))
			.and_then(|sum| sum.trim().parse().ok())
			.ok_or_else(|| err!("the rooms collected per pass were not exposed"))
	}

	fn counted(&self, before: &[u64], expected: Path, what: &str) -> Result {
		let moved = moved(before, &snapshot(&self.registry.sync));
		let expected = self.mode.expect(expected);

		require(
			moved == [expected],
			&format!("{what}: the pass was counted {moved:?} instead of {expected:?}"),
		)
	}

	async fn message(&self, room: &RoomId, txn: &str) -> Result<String> {
		let event = self
			.bob
			.send_text(room, txn, "candidates")
			.await?;

		Ok(event.to_string())
	}

	async fn tag(&self, room: &RoomId, method: Method) -> Result {
		let path = format!("user/{}/rooms/{room}/tags/{TAG}", self.alice_id);
		let response = self
			.alice
			.request(method, &self.alice.url(&path), &json!({}))
			.await?;

		accepted(response).await.map(drop)
	}
}

async fn exercise(services: &Services, base: &str, mode: Mode) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "cvalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "cvbob", BOB_TOKEN).await?;

	let private = json!({ "preset": "private_chat" });
	let mut rooms = Vec::new();

	for _ in 0..ROOMS {
		let room = bob.create_room(&private).await?;
		bob.admit(&room, &alice, &alice_id).await?;
		rooms.push(room);
	}

	let world = World {
		mode,
		services,
		registry,
		alice,
		alice_id,
		bob,
		bob_id,
		rooms,
	};

	world.rounds().await?;
	world.finish()
}

impl World<'_> {
	async fn rounds(&self) -> Result {
		let body = json!({
			"conn_id": "main",
			"lists": {
				"all": list(json!({})),
				"tagged": list(json!({ "tags": [TAG] })),
				"direct": list(json!({ "is_dm": true })),
			},
		});

		let before = snapshot(&self.registry.sync);
		let (mut conn, opening) = Conn::open(&self.alice, body).await?;

		self.counted(&before, (SelectPath::Full, SelectReason::NoIndex), "the opening request")?;

		let newest = &self.rooms[ROOMS - 1];

		require(
			room_keys(&opening).len() == 5 && opening["rooms"][newest.as_str()].is_object(),
			"the opening request did not send the five newest rooms",
		)?;
		require(
			count(&opening, "all") == ROOMS
				&& count(&opening, "tagged") == 0
				&& count(&opening, "direct") == 0,
			"the opening request counted the lists wrongly",
		)?;

		let seen = self
			.pass(&mut conn, CANDIDATES, "a pass with nothing to report")
			.await?;

		require(room_keys(&seen).is_empty(), "a pass with nothing to report sent rooms")?;

		self.messaged_rooms(&mut conn).await?;
		self.joined_room(&mut conn).await?;
		self.invited_room(&mut conn).await?;
		self.left_room(&mut conn).await?;
		self.subscribed_room(&mut conn).await?;
		self.tagged_room(&mut conn).await?;
		self.direct_room(&mut conn).await?;
		self.changed_lists(&mut conn).await?;
		self.replayed_request(&mut conn).await?;
		self.ignored_invite(&mut conn).await?;
		self.spaced_list().await?;
		self.rooms_no_list_takes().await?;
		self.grown_window().await?;
		self.woken_polls().await?;
		self.failed_receipt().await
	}

	async fn messaged_rooms(&self, conn: &mut Conn<'_>) -> Result {
		let (r3, r7, r10) = (&self.rooms[3], &self.rooms[7], &self.rooms[10]);

		self.message(r10, "candidates-ten").await?;
		self.message(r3, "candidates-three").await?;
		self.message(r7, "candidates-seven").await?;

		let seen = self
			.collecting(conn, CANDIDATES, "messages in three rooms", (3, 5))
			.await?;

		require(
			room_keys(&seen) == keys([r3, r7, r10]),
			&format!("the answer to three messages named {:?}", room_keys(&seen)),
		)?;
		require(
			seen["rooms"][r3.as_str()]["initial"] == true
				&& seen["rooms"][r7.as_str()]["initial"].is_null()
				&& seen["rooms"][r10.as_str()]["initial"].is_null(),
			"the room that rose into the window was not sent whole, or one that was in it was",
		)?;
		require(count(&seen, "all") == ROOMS, "the messages changed the count of the list")
	}

	async fn joined_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		self.bob
			.admit(&room, &self.alice, &self.alice_id)
			.await?;

		let seen = self
			.pass(conn, CANDIDATES, "a room joined")
			.await?;

		require(
			room_keys(&seen) == keys([&room]) && seen["rooms"][room.as_str()]["initial"] == true,
			&format!("the answer to a joined room named {:?}", room_keys(&seen)),
		)?;
		require(count(&seen, "all") == ROOMS + 1, "the joined room was not counted")
	}

	async fn invited_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		self.bob
			.act_on(&room, "invite", &self.alice_id)
			.await?;

		let seen = self
			.pass(conn, CANDIDATES, "a room invited to")
			.await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "invite",
			"the answer omitted the invitation",
		)?;
		require(count(&seen, "all") == ROOMS + 2, "the invitation was not counted")?;

		self.alice.act(&room, "leave").await?;

		let seen = self
			.pass(conn, CANDIDATES, "an invitation declined")
			.await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "leave",
			"the answer omitted the declined invitation",
		)?;
		require(count(&seen, "all") == ROOMS + 1, "the declined invitation was still counted")
	}

	async fn left_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = &self.rooms[3];

		self.alice.act(room, "leave").await?;

		let seen = self.pass(conn, CANDIDATES, "a room left").await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "leave",
			"the answer omitted the room the user left",
		)?;
		require(count(&seen, "all") == ROOMS, "the room left was still counted")
	}

	async fn subscribed_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = &self.rooms[1];

		conn.body["room_subscriptions"] = json!({
			room.as_str(): { "required_state": [], "timeline_limit": 1 },
		});

		let seen = self
			.collecting(conn, CANDIDATES, "a room subscribed to", (1, 6))
			.await?;

		require(
			seen["rooms"][room.as_str()]["initial"] == true,
			"the subscription did not send the room",
		)?;

		let event = self
			.message(room, "candidates-subscribed")
			.await?;
		let seen = self
			.collecting(conn, CANDIDATES, "a message in a subscribed room", (1, 5))
			.await?;

		require(
			seen["rooms"][room.as_str()]["timeline"]
				.to_string()
				.contains(&event),
			"the message in the subscribed room was not sent",
		)?;

		conn.body["room_subscriptions"] = json!({});

		self.collecting(conn, CANDIDATES, "the subscription dropped", (0, 5))
			.await
			.map(drop)
	}

	async fn tagged_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = &self.rooms[2];

		self.tag(room, Method::PUT).await?;

		let seen = self
			.pass(conn, CANDIDATES, "a room tagged")
			.await?;

		require(
			count(&seen, "tagged") == 1 && seen["rooms"][room.as_str()]["initial"] == true,
			"the tagged room did not enter the list",
		)?;

		self.tag(room, Method::DELETE).await?;

		let seen = self
			.pass(conn, CANDIDATES, "a tag removed")
			.await?;

		require(count(&seen, "tagged") == 0, "the room left untagged stayed in the list")
	}

	async fn direct_room(&self, conn: &mut Conn<'_>) -> Result {
		let room = &self.rooms[5];
		let path = format!("user/{}/account_data/m.direct", self.alice_id);

		self.alice
			.put(&path, &json!({ self.bob_id.as_str(): [room.as_str()] }))
			.await?;

		let seen = self
			.pass(conn, (SelectPath::Full, SelectReason::AccountData), "the direct rooms changed")
			.await?;

		require(
			count(&seen, "direct") == 1 && seen["rooms"][room.as_str()]["initial"] == true,
			"the direct room did not enter the list",
		)?;

		self.pass(conn, CANDIDATES, "the pass after the direct rooms changed")
			.await
			.map(drop)
	}

	async fn changed_lists(&self, conn: &mut Conn<'_>) -> Result {
		conn.body["lists"]["plain"] = list(json!({ "is_encrypted": false }));

		let seen = self
			.pass(conn, (SelectPath::Full, SelectReason::Filters), "a list added")
			.await?;

		require(
			count(&seen, "plain") == count(&seen, "all"),
			"the list added did not count the rooms it takes",
		)?;

		self.pass(conn, CANDIDATES, "the pass after a list was added")
			.await
			.map(drop)
	}

	async fn replayed_request(&self, conn: &mut Conn<'_>) -> Result {
		let room = &self.rooms[0];
		let (pos, body) = (conn.pos.clone(), conn.body.clone());

		self.message(room, "candidates-replayed").await?;

		let answered = self
			.pass(conn, CANDIDATES, "a message before a replay")
			.await?;

		require(
			room_keys(&answered) == keys([room]),
			"the request before the replay did not send the room",
		)?;

		let before = snapshot(&self.registry.sync);
		let replayed = self
			.alice
			.sliding_sync(Some(&pos), 0, &body)
			.await?;

		self.counted(&before, (SelectPath::Full, SelectReason::Replay), "a repeated position")?;

		require(
			room_keys(&replayed) == room_keys(&answered),
			"the repeated position did not give the rooms of the answer it repeats",
		)?;

		conn.pos = field(&replayed, "pos")?.to_owned();

		self.pass(conn, CANDIDATES, "the pass after a replay")
			.await
			.map(drop)
	}

	async fn ignored_invite(&self, conn: &mut Conn<'_>) -> Result {
		let path = format!("user/{}/account_data/m.ignored_user_list", self.alice_id);
		let account = (SelectPath::Full, SelectReason::AccountData);

		self.alice
			.put(&path, &json!({ "ignored_users": { self.bob_id.as_str(): {} } }))
			.await?;

		self.pass(conn, account, "a sender ignored")
			.await?;

		let room = self
			.bob
			.create_room(&json!({ "preset": "private_chat" }))
			.await?;

		self.bob
			.act_on(&room, "invite", &self.alice_id)
			.await?;

		let seen = self
			.pass(conn, CANDIDATES, "an invitation from an ignored sender")
			.await?;

		require(
			room_keys(&seen).is_empty() && count(&seen, "all") == ROOMS,
			"the answer named an invitation from a sender the user ignores",
		)?;

		self.alice
			.put(&path, &json!({ "ignored_users": {} }))
			.await?;

		let seen = self
			.pass(conn, account, "a sender no longer ignored")
			.await?;

		require(
			seen["rooms"][room.as_str()]["membership"] == "invite"
				&& count(&seen, "all") == ROOMS + 1,
			"the invitation did not surface once its sender was no longer ignored",
		)
	}

	async fn spaced_list(&self) -> Result {
		let space = &self.rooms[0];
		let body = json!({
			"conn_id": "spaced",
			"lists": { "in_space": list(json!({ "spaces": [space.as_str()] })) },
		});

		let (mut conn, _) = Conn::open(&self.alice, body).await?;
		let seen = self
			.pass(&mut conn, (SelectPath::Full, SelectReason::Spaces), "a list of a space")
			.await?;

		require(count(&seen, "in_space") == 0, "a room outside the space was counted in it")
	}

	async fn rooms_no_list_takes(&self) -> Result {
		let room = &self.rooms[9];
		let body = json!({
			"conn_id": "tagged-only",
			"lists": { "tagged": list(json!({ "tags": [TAG] })) },
		});

		let (mut conn, opening) = Conn::open(&self.alice, body).await?;

		require(
			count(&opening, "tagged") == 0 && room_keys(&opening).is_empty(),
			"the list took a room before any was tagged",
		)?;

		self.pass(&mut conn, CANDIDATES, "a pass of lists that take no room")
			.await?;

		self.tag(room, Method::PUT).await?;

		let seen = self
			.pass(&mut conn, CANDIDATES, "a room tagged that no list took")
			.await?;

		require(
			count(&seen, "tagged") == 1 && room_keys(&seen) == keys([room]),
			&format!("a room no list took was not taken once tagged: {:?}", room_keys(&seen)),
		)
	}

	/// A window that grows and shrinks over rooms that change outside it.
	///
	/// A room that enters the window is sent whole, with what the extensions
	/// hold of it, whether it changed since it was last in the window or never was
	/// in it.
	async fn grown_window(&self) -> Result {
		let body = json!({
			"conn_id": "grown",
			"lists": { "all": list(json!({})) },
			"extensions": { "account_data": { "enabled": true }, "receipts": { "enabled": true } },
		});

		let before = snapshot(&self.registry.sync);
		let (mut conn, opening) = Conn::open(&self.alice, body).await?;

		self.counted(&before, (SelectPath::Full, SelectReason::NoIndex), "the opening request")?;

		let first = room_keys(&opening);
		let total = count(&opening, "all");
		let inside = self
			.rooms
			.iter()
			.filter(|room| first.contains(room.as_str()))
			.collect::<Vec<_>>();

		require(first.len() == 5 && inside.len() >= 2, "the opening window was not five rooms")?;

		self.pass(&mut conn, CANDIDATES, "a pass before the window grew")
			.await?;

		let (read, tagged) = (inside[0], inside[1]);
		let event = self
			.bob
			.send_text(read, "candidates-grown", "grown")
			.await?;

		self.collecting(&mut conn, CANDIDATES, "a message before a receipt", (1, 5))
			.await?;

		self.bob
			.post(&format!("rooms/{read}/receipt/m.read/{event}"), &json!({}))
			.await?;

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a receipt in a room of the window", (1, 5))
			.await?;

		require(
			room_keys(&seen).is_empty() && held(&seen, "receipts", read, "m.receipt"),
			"the receipt in a room of the window was not sent",
		)?;

		self.tag(tagged, Method::PUT).await?;

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a tag in a room of the window", (1, 5))
			.await?;

		require(
			room_keys(&seen).is_empty() && held(&seen, "account_data", tagged, "m.tag"),
			"the tag of a room of the window was not sent",
		)?;

		conn.body["lists"]["all"]["ranges"] = json!([[0, 9]]);

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a window that grew", (5, 10))
			.await?;

		let entered = room_keys(&seen);

		require(
			entered.len() == 5 && entered.is_disjoint(&first) && all_initial(&seen),
			&format!("the rooms that entered the window were not sent whole: {entered:?}"),
		)?;
		require(
			count(&seen, "all") == total,
			"the window growing changed the count of the list",
		)?;

		let outside = self
			.rooms
			.iter()
			.filter(|room| !first.contains(room.as_str()) && !entered.contains(room.as_str()))
			.filter(|room| **room != self.rooms[3])
			.collect::<Vec<_>>();

		let waiting = total
			.saturating_sub(first.len())
			.saturating_sub(entered.len());

		require(
			outside.len() == waiting && waiting > 0,
			"the rooms outside the window were not rooms of the run",
		)?;

		let outer = outside[0];

		self.tag(outer, Method::PUT).await?;

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a tag in a room outside the window", (0, 10))
			.await?;

		require(room_keys(&seen).is_empty(), "a tag outside the window sent a room")?;

		conn.body["lists"]["all"]["ranges"] = json!([[0, total.saturating_sub(1)]]);

		let seen = self
			.collecting(
				&mut conn,
				CANDIDATES,
				"a window that grew over a tagged room",
				(waiting, total),
			)
			.await?;

		require(
			room_keys(&seen) == keys(outside.iter().copied())
				&& all_initial(&seen)
				&& held(&seen, "account_data", outer, "m.tag"),
			"a room tagged outside the window did not arrive whole with its tag",
		)?;

		conn.body["lists"]["all"]["ranges"] = json!([[0, 4]]);

		self.collecting(&mut conn, CANDIDATES, "a window that shrank", (0, 5))
			.await?;

		let regrown = OwnedRoomId::try_from(
			entered
				.iter()
				.next()
				.expect("rooms entered the window")
				.as_str(),
		)?;

		self.tag(&regrown, Method::PUT).await?;
		self.collecting(&mut conn, CANDIDATES, "a tag in a room that left the window", (0, 5))
			.await?;

		conn.body["lists"]["all"]["ranges"] = json!([[0, 9]]);

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a window that grew over a room it held", (5, 10))
			.await?;

		require(
			room_keys(&seen).is_empty() && held(&seen, "account_data", &regrown, "m.tag"),
			"a room that came back into the window lost what changed while it was out",
		)?;

		conn.body["lists"]["all"]["timeline_limit"] = json!(2);

		let seen = self
			.collecting(&mut conn, CANDIDATES, "a timeline limit changed", (10, 10))
			.await?;

		require(
			room_keys(&seen).len() == 10,
			"a room of the window did not answer a changed timeline limit",
		)?;

		for room in [tagged, outer, &regrown] {
			self.tag(room, Method::DELETE).await?;
		}

		Ok(())
	}

	/// Long polls that a change in a quiet room of the window wakes.
	///
	/// The first pass of such a poll finds nothing to answer, and the pass the
	/// change wakes must still be judged against what the index owes.
	async fn woken_polls(&self) -> Result {
		let body = json!({
			"conn_id": "woken",
			"lists": { "all": list(json!({})) },
			"extensions": { "account_data": { "enabled": true } },
		});

		let (mut conn, opening) = Conn::open(&self.alice, body).await?;
		let first = room_keys(&opening);
		let room = self
			.rooms
			.iter()
			.find(|room| first.contains(room.as_str()))
			.ok_or_else(|| err!("the opening window held no room of the run"))?;

		self.pass(&mut conn, CANDIDATES, "a pass before the long polls")
			.await?;

		let path = format!("user/{}/rooms/{room}/account_data/u.woken", self.alice_id);
		let put = async {
			self.alice
				.put(&path, &json!({ "v": 1 }))
				.await
				.map(drop)
		};
		let seen = self
			.waking(&mut conn, "a long poll woken by room account data", (1, 5), put)
			.await?;

		require(
			room_keys(&seen).is_empty() && held(&seen, "account_data", room, "u.woken"),
			"the room account data that woke a long poll was not sent",
		)?;

		let seen = self
			.waking(&mut conn, "a long poll woken by a tag", (1, 5), self.tag(room, Method::PUT))
			.await?;

		require(
			room_keys(&seen).is_empty() && held(&seen, "account_data", room, "m.tag"),
			"the tag that woke a long poll was not sent",
		)?;

		self.tag(room, Method::DELETE).await
	}

	/// A room whose receipts cannot be read stays owed until a pass collects it.
	///
	/// Bob's receipt in a window room, and with it a row no receipt can be read
	/// from, make the room fail when it is collected. The row goes and nothing
	/// is written in that room, so only what the index owes brings the receipt
	/// back. A connection that a list of a space forces onto the full pass is the
	/// answer to match.
	async fn failed_receipt(&self) -> Result {
		let plain = json!({
			"conn_id": "owing",
			"lists": { "all": list(json!({})) },
			"extensions": { "receipts": { "enabled": true } },
		});

		let mut spaced = plain.clone();
		spaced["conn_id"] = json!("owing-full");
		spaced["lists"]["spaced"] = list(json!({ "spaces": ["!nospace:example.com"] }));

		let (mut cand, opening) = Conn::open(&self.alice, plain).await?;
		let (mut full, matching) = Conn::open(&self.alice, spaced).await?;

		require(strip(&opening) == strip(&matching), "the two connections opened differently")?;

		let first = room_keys(&opening);
		let inside = self
			.rooms
			.iter()
			.filter(|room| first.contains(room.as_str()))
			.collect::<Vec<_>>();

		require(inside.len() >= 2, "the opening window held fewer than two rooms of the run")?;

		let (failing, other) = (inside[0], inside[1]);
		let message = self.message(failing, "candidates-owing").await?;

		self.twin(&mut cand, &mut full, "a message in the room that is to fail", (1, 5))
			.await?;

		self.bob
			.post(&format!("rooms/{failing}/receipt/m.read/{message}"), &json!({}))
			.await?;

		let permit = self.services.globals.next_count();
		let count = *permit;
		let key = (failing.as_str(), count, self.alice_id.as_str(), "");
		let receipts = &self.services.db["readreceiptid_readreceipt"];

		receipts.put_raw(key, br#"{"content":5}"#);
		drop(permit);

		let seen = self
			.twin(&mut cand, &mut full, "a receipt that cannot be read", (1, 5))
			.await?;

		require(
			!held(&seen, "receipts", failing, "m.receipt"),
			"a room that failed to be collected sent its receipts",
		)?;

		receipts.del(key);
		self.message(other, "candidates-owing-elsewhere")
			.await?;

		let seen = self
			.twin(&mut cand, &mut full, "a write elsewhere once the row is gone", (2, 5))
			.await?;

		require(
			held(&seen, "receipts", failing, "m.receipt"),
			"the receipt of a room that failed to be collected was lost",
		)?;

		self.message(other, "candidates-owing-again")
			.await?;
		self.twin(&mut cand, &mut full, "a write elsewhere after the room was collected", (1, 5))
			.await
			.map(drop)
	}

	/// Polls the connection under test and the one the full pass answers, and
	/// requires the same rooms and extensions from both.
	async fn twin(
		&self,
		cand: &mut Conn<'_>,
		full: &mut Conn<'_>,
		what: &str,
		counts: (usize, usize),
	) -> Result<Value> {
		let before = self.collected()?;
		let seen = self.pass(cand, CANDIDATES, what).await?;
		let collected = self.collected_since(before, what, counts);
		let matching = full.poll().await?;

		require(
			strip(&seen) == strip(&matching),
			&format!(
				"{what}: the answer of the index differs from the full pass: {} | {}",
				strip(&seen),
				strip(&matching)
			),
		)?;

		collected.map(|()| seen)
	}

	fn finish(&self) -> Result {
		let sync = &self.registry.sync;

		require(
			sync.v5_selected_total(SelectPath::Full, SelectReason::Error) == 0,
			"a pass could not revise the index",
		)?;

		match self.mode {
			| Mode::Candidates => require(
				sync.v5_selected_total(SelectPath::Candidates, SelectReason::None) > 0
					&& sync.v5_candidate_passes() > 0
					&& sync.v5_range_passes() > 0,
				"no pass took its rooms from the index",
			),
			| Mode::Verify => require(
				sync.v5_candidate_passes() > 0
					&& sync.v5_candidates_mismatched_total(Mismatch::Window) == 0
					&& sync.v5_candidates_mismatched_total(Mismatch::Count) == 0
					&& sync.v5_candidates_mismatched_total(Mismatch::Range) == 0,
				"the index and the full pass differed, or the index was never revised",
			),
			| Mode::Control => require(
				sync.v5_selected_total(SelectPath::Candidates, SelectReason::None) == 0
					&& sync.v5_candidate_passes() == 0,
				"a pass took its rooms from the index with both switches off",
			),
		}
	}
}

async fn exercise_horizon(services: &Services, base: &str) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "cvhalice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "cvhbob", BOB_TOKEN).await?;

	let room = bob
		.create_room(&json!({ "preset": "private_chat" }))
		.await?;

	bob.admit(&room, &alice, &alice_id).await?;

	let world = World {
		mode: Mode::Candidates,
		services,
		registry,
		alice,
		alice_id,
		bob,
		bob_id,
		rooms: vec![room.clone()],
	};

	let body = json!({ "conn_id": "horizon", "lists": { "all": list(json!({})) } });
	let (mut conn, _) = Conn::open(&world.alice, body).await?;

	world
		.pass(&mut conn, CANDIDATES, "a pass before the journal lost the index")
		.await?;

	burst(services, &world.bob_id);

	let event = world.message(&room, "candidates-horizon").await?;
	let seen = world
		.pass(
			&mut conn,
			(SelectPath::Full, SelectReason::Horizon),
			"a journal that lost the index",
		)
		.await?;

	require(
		seen["rooms"][room.as_str()]["timeline"]
			.to_string()
			.contains(&event),
		"the pass after the journal lost the index omitted the message",
	)?;

	world
		.pass(&mut conn, CANDIDATES, "the pass after the index was built again")
		.await
		.map(drop)
}

/// Makes passes while the user's own tag of a room outside the window is put and
/// deleted as fast as the server takes it.
///
/// The index is revised from the state at the position a pass reads up to, and the
/// full pass reads live after it, so a tag written in between shows in the full
/// pass alone. The comparison must not count that, and must have met it.
async fn exercise_race(services: &Services, base: &str) -> Result {
	let registry = services.server.metrics.enable_prometheus();

	let (alice_id, alice) = account(services, base, "cvralice", ALICE_TOKEN).await?;
	let (bob_id, bob) = account(services, base, "cvrbob", BOB_TOKEN).await?;

	let private = json!({ "preset": "private_chat" });
	let mut rooms = Vec::new();

	for _ in 0..RACE_ROOMS {
		let room = bob.create_room(&private).await?;
		bob.admit(&room, &alice, &alice_id).await?;
		rooms.push(room);
	}

	let world = World {
		mode: Mode::Verify,
		services,
		registry,
		alice,
		alice_id,
		bob,
		bob_id,
		rooms,
	};

	let body = json!({
		"conn_id": "race",
		"lists": { "all": list(json!({})), "tagged": list(json!({ "tags": [TAG] })) },
	});

	let (mut conn, _) = Conn::open(&world.alice, body).await?;

	let done = AtomicBool::new(false);
	let polling = async {
		let outcome = poll_while_racing(&mut conn, &world.registry.sync).await;
		done.store(true, Ordering::Relaxed);

		outcome
	};

	let writing = async {
		let room = &world.rooms[0];

		while !done.load(Ordering::Relaxed) {
			world.tag(room, Method::PUT).await?;
			world.tag(room, Method::DELETE).await?;
		}

		Result::<()>::Ok(())
	};

	let (polled, written) = join(polling, writing).await;

	polled?;
	written?;

	let sync = &world.registry.sync;

	require(
		sync.v5_candidates_mismatched_total(Mismatch::Window) == 0
			&& sync.v5_candidates_mismatched_total(Mismatch::Count) == 0
			&& sync.v5_candidates_mismatched_total(Mismatch::Range) == 0,
		"a write during a pass was counted as a difference of the index",
	)?;
	require(
		sync.v5_candidates_skipped_total(Skipped::Explained) > 0,
		"no write landed inside a pass, so nothing was set aside",
	)?;
	require(
		sync.v5_candidates_skipped_total(Skipped::Journal) == 0,
		"the journal could not say what was written after a pass",
	)
}

/// Makes passes until the comparison has set a difference aside or found one, but
/// at least `RACE_PASSES` of them and at most `RACE_LIMIT`.
async fn poll_while_racing(conn: &mut Conn<'_>, sync: &SyncMetrics) -> Result {
	for pass in 0..RACE_LIMIT {
		conn.poll().await?;

		let settled = sync.v5_candidates_skipped_total(Skipped::Explained) > 0
			|| sync.v5_candidates_mismatched_total(Mismatch::Window) > 0
			|| sync.v5_candidates_mismatched_total(Mismatch::Count) > 0;

		if pass >= RACE_PASSES && settled {
			break;
		}
	}

	Ok(())
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

fn list(filters: Value) -> Value {
	let mut list = json!({ "ranges": [[0, 4]], "required_state": [], "timeline_limit": 1 });

	if filters
		.as_object()
		.is_some_and(|filters| !filters.is_empty())
	{
		list["filters"] = filters;
	}

	list
}

fn snapshot(sync: &SyncMetrics) -> Vec<u64> {
	PATHS
		.into_iter()
		.map(|(path, reason)| sync.v5_selected_total(path, reason))
		.collect()
}

fn moved(before: &[u64], after: &[u64]) -> Vec<Path> {
	PATHS
		.into_iter()
		.zip(before.iter().zip(after))
		.filter(|(_, (before, after))| after > before)
		.map(|(path, _)| path)
		.collect()
}

/// Whether what an extension holds of a room mentions `needle`.
fn held(response: &Value, extension: &str, room: &RoomId, needle: &str) -> bool {
	response["extensions"][extension]["rooms"][room.as_str()]
		.to_string()
		.contains(needle)
}

/// Whether every room of the response is sent whole.
fn all_initial(response: &Value) -> bool {
	response["rooms"]
		.as_object()
		.is_some_and(|rooms| rooms.values().all(|room| room["initial"] == true))
}

/// The rooms and extensions of an answer, without the ages that tell when it was
/// asked.
fn strip(response: &Value) -> Value {
	let mut stripped =
		json!({ "rooms": response["rooms"], "extensions": response["extensions"] });

	unaged(&mut stripped);

	stripped
}

fn unaged(value: &mut Value) {
	match value {
		| Value::Object(map) => {
			if let Some(Value::Object(unsigned)) = map.get_mut("unsigned") {
				unsigned.remove("age");
			}

			map.values_mut().for_each(unaged);
		},
		| Value::Array(items) => items.iter_mut().for_each(unaged),
		| _ => {},
	}
}

fn room_keys(response: &Value) -> BTreeSet<String> {
	response["rooms"]
		.as_object()
		.into_iter()
		.flatten()
		.map(|(room_id, _)| room_id.clone())
		.collect()
}

fn keys<'a, Rooms>(rooms: Rooms) -> BTreeSet<String>
where
	Rooms: IntoIterator<Item = &'a OwnedRoomId>,
{
	rooms
		.into_iter()
		.map(ToString::to_string)
		.collect()
}

fn count(response: &Value, list: &str) -> usize {
	response["lists"][list]["count"]
		.as_u64()
		.and_then(|count| usize::try_from(count).ok())
		.unwrap_or(usize::MAX)
}
