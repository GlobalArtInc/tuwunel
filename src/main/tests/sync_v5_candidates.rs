#![cfg(test)]

use std::{
	collections::BTreeSet,
	sync::atomic::{AtomicBool, Ordering},
};

use futures::future::join;
use reqwest::Method;
use serde_json::{Value, json};
use tuwunel_core::{
	Result,
	metrics::prom::{
		JournalKind, Mismatch, Registry, SelectPath, SelectReason, Skipped, SyncMetrics,
	},
	ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId},
};
use tuwunel_service::Services;

use self::{
	account::account,
	client::{Client, accepted, field},
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

	async fn poll(&mut self) -> Result<Value> {
		let seen = self
			.client
			.sliding_sync(Some(&self.pos), 0, &self.body)
			.await?;

		self.pos = field(&seen, "pos")?.to_owned();

		Ok(seen)
	}
}

struct World<'a> {
	mode: Mode,
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
		self.rooms_no_list_takes().await
	}

	async fn messaged_rooms(&self, conn: &mut Conn<'_>) -> Result {
		let (r3, r7, r10) = (&self.rooms[3], &self.rooms[7], &self.rooms[10]);

		self.message(r10, "candidates-ten").await?;
		self.message(r3, "candidates-three").await?;
		self.message(r7, "candidates-seven").await?;

		let seen = self
			.pass(conn, CANDIDATES, "messages in three rooms")
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
			.pass(conn, CANDIDATES, "a room subscribed to")
			.await?;

		require(
			seen["rooms"][room.as_str()]["initial"] == true,
			"the subscription did not send the room",
		)?;

		let event = self
			.message(room, "candidates-subscribed")
			.await?;
		let seen = self
			.pass(conn, CANDIDATES, "a message in a subscribed room")
			.await?;

		require(
			seen["rooms"][room.as_str()]["timeline"]
				.to_string()
				.contains(&event),
			"the message in the subscribed room was not sent",
		)?;

		conn.body["room_subscriptions"] = json!({});

		self.pass(conn, CANDIDATES, "the subscription dropped")
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

	fn finish(&self) -> Result {
		let sync = &self.registry.sync;

		require(
			sync.v5_selected_total(SelectPath::Full, SelectReason::Error) == 0,
			"a pass could not revise the index",
		)?;

		match self.mode {
			| Mode::Candidates => require(
				sync.v5_selected_total(SelectPath::Candidates, SelectReason::None) > 0
					&& sync.v5_candidate_passes() > 0,
				"no pass took its rooms from the index",
			),
			| Mode::Verify => require(
				sync.v5_candidate_passes() > 0
					&& sync.v5_candidates_mismatched_total(Mismatch::Window) == 0
					&& sync.v5_candidates_mismatched_total(Mismatch::Count) == 0,
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
			&& sync.v5_candidates_mismatched_total(Mismatch::Count) == 0,
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
