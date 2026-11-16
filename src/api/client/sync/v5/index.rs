use std::{
	collections::{HashMap, HashSet, hash_map::Entry},
	hash::{DefaultHasher, Hash, Hasher},
	time::{Duration, Instant},
};

use futures::StreamExt;
use ruma::{OwnedRoomId, RoomId, UserId, events::room::member::MembershipState};
use tuwunel_core::{
	Result, error,
	metrics::prom::{Mismatch, SelectPath, SelectReason, Skipped},
	utils::stream::{BroadbandExt, IterStream},
	warn,
};
use tuwunel_service::{
	Services,
	sync::{Changes, Connection, IndexedRoom, Lists, RoomIndex},
	users::InviteFilter,
};

use super::{
	super::invite_permitted_room,
	ResponseLists, SyncInfo, Window, WindowRoom, departed,
	range::Results,
	rooms::{merged_room_details, room_config},
	selector::{finish, full_path, matcher, room_sort, view},
};

/// How long an index is trusted before the next pass builds it again from every
/// room, which bounds how long a mistake in it can live.
const REFRESH: Duration = Duration::from_mins(10);

/// The fewest rooms a window of the journal may name before the index gives up
/// revising and a pass looks at every room.
const LIMIT_FLOOR: usize = 1024;

/// How many differences of one pass are logged one by one.
const MISMATCH_LOGS: usize = 32;

/// How a sliding sync pass chooses the rooms of its lists, as the switches
/// select it now.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Mode {
	/// Every room of the account, and no index kept.
	Off,

	/// The index of the connection, revised with the journal.
	Candidates,

	/// Every room of the account, answered from, and the index revised beside it
	/// and compared with the answer.
	Verify,
}

impl Mode {
	/// The mode the keys select now, which a configuration reload can change
	/// between two passes of one request.
	///
	/// Whether the journal is on comes from the journal itself, whose capacity
	/// is the one it started with.
	fn current(services: &Services) -> Self {
		let sync = &services.config.client.sync;

		Self::select(services.sync.journal.is_on(), sync.v5_candidates, sync.v5_candidates_verify)
	}

	fn select(journal_on: bool, candidates: bool, verify: bool) -> Self {
		if !journal_on {
			Self::Off
		} else if verify {
			Self::Verify
		} else if candidates {
			Self::Candidates
		} else {
			Self::Off
		}
	}
}

/// What a pass knows of itself before it asks the journal.
#[derive(Clone, Copy, Debug)]
struct Facts {
	/// Whether the request repeats a position the connection has since moved
	/// past.
	replay: bool,

	/// The digest of the lists of the connection now.
	digest: u64,

	/// Whether a list filters by space.
	spaces: bool,

	/// The position the pass reads up to.
	next_batch: u64,
}

/// What revising the index with the journal came to.
struct Revised {
	index: RoomIndex,
	rooms: Vec<WindowRoom>,
	revised: usize,
}

/// What one room of the journal's window is, now.
struct Revision {
	room_id: OwnedRoomId,
	member: bool,
	entry: Option<IndexedRoom>,
}

/// A part of the answer that differs between the index and the full pass.
#[derive(Debug, Eq, PartialEq)]
struct Difference {
	part: Mismatch,
	subject: String,
}

/// The rooms of the window a pass collects the ranges of.
pub(super) enum Dirty {
	/// Every room of the window, as the pass that chose its rooms from every room
	/// of the account collects them.
	Window,

	/// The rooms of the window the pass owes ranges, and no other: for the rest
	/// the journal names nothing since the pass before.
	Rooms(HashSet<OwnedRoomId>),

	/// Every room of the window, with what the index would have left out, to
	/// compare with what the full pass finds in them.
	Shadow(Shadow),
}

/// What the verifying mode keeps of the ranges the index would not have
/// collected, until the full pass has collected them.
pub(super) struct Shadow {
	/// The rooms of the window the index owes no ranges.
	clean: HashSet<OwnedRoomId>,

	/// The position the index was exact at.
	at: u64,

	/// The most rooms the journal may name when it is asked about writes after the
	/// pass.
	limit: usize,
}

impl Dirty {
	/// The only rooms to collect the ranges of, or none when it is every room of
	/// the window.
	pub(super) fn only(&self) -> Option<&HashSet<OwnedRoomId>> {
		match self {
			| Self::Rooms(rooms) => Some(rooms),
			| Self::Window | Self::Shadow(_) => None,
		}
	}

	/// In the verifying mode, counts a room the full pass found something to send
	/// in that the index would have collected nothing for.
	///
	/// A write after the position the pass reads up to can show in the full pass
	/// alone, and is not counted, as with the rooms of the window.
	pub(super) fn verify(
		&self,
		services: &Services,
		sender_user: &UserId,
		ranges: &Results,
		next_batch: u64,
	) {
		let Self::Shadow(Shadow { clean, at, limit }) = self else {
			return;
		};

		let differences = unowed(clean, ranges.with_content());

		judge(services, sender_user, differences, (*at, next_batch, *limit), |_| false);
	}
}

/// Chooses the rooms of the lists for one pass, and builds the lists and the
/// window from them.
///
/// The index of the connection is revised with the rooms the change journal
/// names after the position it is exact at, when the switches allow it and
/// nothing forbids it. Otherwise every room of the account is looked at, which
/// is also how the index is built. With the verifying switch the full pass
/// answers, and the revised index is compared with it. The index is stored as
/// soon as the rooms are chosen, before the pass collects its ranges and
/// extensions, and is exact at the position that pass reads up to, whether or not
/// the pass goes on to answer. It is stored only when the choice succeeded: a pass
/// that could not vouch for it leaves none behind. A request that repeats a
/// position the connection has moved past, `replay`, is always answered from
/// every room.
///
/// It also says which rooms of the window the pass is to collect the ranges of:
/// every room of the window, unless the rooms came from the index, and then the
/// rooms the pass owes ranges. The index records those as owed at once, and a pass
/// takes a room off only when it has collected it, so a room the pass does not
/// collect stays owed.
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn select(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	replay: bool,
) -> (Window, ResponseLists, Dirty) {
	let SyncInfo { services, sender_user, .. } = sync_info;

	// MSC4155: a stored invite whose sender the recipient ignores or blocks is
	// kept out of the window, and relaxing the configuration re-exposes it.
	let invite_filter = services.users.invite_filter(sender_user).await;

	let mode = Mode::current(services);

	if mode == Mode::Off {
		return full(conn, sync_info, &invite_filter, None, SelectReason::Off).await;
	}

	let facts = Facts {
		replay,
		digest: digest(&conn.lists),
		spaces: spaces(&conn.lists),
		next_batch: conn.next_batch,
	};

	let held = conn.index.take().map(|index| *index);

	let reason = match plan(services, sender_user, mode, held, facts) {
		| Ok((index, changes)) if mode == Mode::Verify => {
			return verified(conn, sync_info, &invite_filter, (index, &changes), facts.digest)
				.await;
		},
		| Ok((index, changes)) =>
			match candidates(conn, sync_info, &invite_filter, index, &changes).await {
				| Ok(selection) => return selection,
				| Err(error) => {
					warn!(
						user_id = %sender_user,
						%error,
						"sliding sync index could not be revised; selecting from every room"
					);

					SelectReason::Error
				},
			},
		| Err(reason) => reason,
	};

	full(conn, sync_info, &invite_filter, Some(facts.digest), reason).await
}

/// Decides whether the index can be revised, and with what the journal names.
fn plan(
	services: &Services,
	sender_user: &UserId,
	mode: Mode,
	held: Option<RoomIndex>,
	facts: Facts,
) -> Result<(RoomIndex, Changes), SelectReason> {
	let index = precheck(mode, held, facts, Instant::now())?;
	let journal = &services.sync.journal;

	let changes =
		journal.changes(sender_user, index.at, facts.next_batch, limit(index.members.len()));

	let changes = classify(changes, || index.at < journal.horizon())?;

	Ok((index, changes))
}

/// Why a pass cannot revise the index before it asks the journal, or the index
/// when it can.
///
/// The reasons come in the order the checks are made, so a pass that fails more
/// than one is counted under the first.
fn precheck(
	mode: Mode,
	held: Option<RoomIndex>,
	facts: Facts,
	now: Instant,
) -> Result<RoomIndex, SelectReason> {
	if mode == Mode::Off {
		return Err(SelectReason::Off);
	}

	let index = held.ok_or(SelectReason::NoIndex)?;

	if facts.replay || index.at > facts.next_batch {
		return Err(SelectReason::Replay);
	}

	if index.digest != facts.digest {
		return Err(SelectReason::Filters);
	}

	if facts.spaces {
		return Err(SelectReason::Spaces);
	}

	if now.saturating_duration_since(index.built) > REFRESH {
		return Err(SelectReason::Refresh);
	}

	Ok(index)
}

/// Why a pass cannot revise the index with what the journal answered, or what
/// it may revise it with.
///
/// A change of the global account data of the user is not revised: it moves
/// the direct rooms and the invite permissions every room is judged by.
fn classify(
	changes: Option<Changes>,
	below_horizon: impl FnOnce() -> bool,
) -> Result<Changes, SelectReason> {
	match changes {
		| None if below_horizon() => Err(SelectReason::Horizon),
		| None => Err(SelectReason::Overflow),
		| Some(changes) if changes.global_account_data => Err(SelectReason::AccountData),
		| Some(changes) => Ok(changes),
	}
}

/// The most rooms the journal may name before the pass looks at every room.
fn limit(members: usize) -> usize { LIMIT_FLOOR.max(members / 2) }

/// A digest of everything about the lists that decides which rooms they take.
///
/// The ranges and the details of the rooms do not: they shape the window, which
/// is built anew every pass. The digest lives as long as the process, so it
/// hashes the debug form of the filters instead of requiring them to be
/// hashable.
fn digest(lists: &Lists) -> u64 {
	let mut hasher = DefaultHasher::new();

	for (id, list) in lists {
		id.hash(&mut hasher);
		format!("{:?}", list.filters).hash(&mut hasher);
	}

	hasher.finish()
}

/// Whether a list takes the rooms of a space, which no change of the journal
/// names.
fn spaces(lists: &Lists) -> bool {
	lists.values().any(|list| {
		list.filters
			.as_ref()
			.is_some_and(|filters| !filters.spaces.is_empty())
	})
}

/// Revises the index, answers from it, and keeps it.
async fn candidates(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	invite_filter: &InviteFilter,
	index: RoomIndex,
	changes: &Changes,
) -> Result<(Window, ResponseLists, Dirty)> {
	let Revised { mut index, mut rooms, revised } =
		revise(index, changes, conn, sync_info, invite_filter).await?;

	let (window, lists) = finish(conn, sync_info, &mut rooms, invite_filter).await;

	let dirty = owed(conn, &window, &index, changes);

	record(&mut index, &window, dirty.iter().cloned());
	index.at = conn.next_batch;
	conn.index = Some(Box::new(index));

	counted(sync_info.services, SelectPath::Candidates, SelectReason::None, Some(revised));

	Ok((window, lists, Dirty::Rooms(dirty)))
}

/// Answers from every room of the account, and compares the index revised
/// beside it with the answer.
///
/// The index is built again from the full pass whatever the comparison found, so
/// a difference does not carry over to the next pass. It owes the ranges the
/// revised index owed, since a pass that goes on to collect nothing leaves them
/// to the next one, which the comparison then judges as the index would have.
async fn verified(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	invite_filter: &InviteFilter,
	(index, changes): (RoomIndex, &Changes),
	digest: u64,
) -> (Window, ResponseLists, Dirty) {
	let SyncInfo { services, sender_user, .. } = sync_info;

	let at = index.at;
	let limit = limit(index.members.len());
	let shadow = revise(index, changes, conn, sync_info, invite_filter).await;

	let (mut rooms, members) = full_path(conn, sync_info, invite_filter, true).await;
	let (window, lists) = finish(conn, sync_info, &mut rooms, invite_filter).await;
	let next_batch = conn.next_batch;

	let (dirty, owing) = match shadow {
		| Ok(Revised {
			index: shadow_index,
			rooms: mut shadow,
			revised,
		}) => {
			shadow.sort_unstable_by(room_sort);

			let (shadow_window, shadow_lists) =
				view(conn, sync_info, &shadow, invite_filter).await;

			let differences = compare((&window, &lists), (&shadow_window, &shadow_lists));

			counted(services, SelectPath::Candidates, SelectReason::None, Some(revised));
			judge(services, sender_user, differences, (at, next_batch, limit), |room_id| {
				shadow_index.members.contains(room_id)
					|| shadow_index.selected.contains_key(room_id)
					|| members.contains(room_id)
			});

			let owing = owed(conn, &shadow_window, &shadow_index, changes);
			let clean = shadow_window
				.keys()
				.filter(|room_id| !owing.contains(*room_id))
				.cloned()
				.collect();

			(Dirty::Shadow(Shadow { clean, at, limit }), owing)
		},
		| Err(error) => {
			warn!(
				user_id = %sender_user,
				%error,
				"sliding sync index could not be revised beside the full pass"
			);

			counted(services, SelectPath::Full, SelectReason::Error, None);

			(Dirty::Window, window.keys().cloned().collect())
		},
	};

	let mut index = build(next_batch, digest, members, &rooms);

	record(&mut index, &window, owing.into_iter());
	conn.index = Some(Box::new(index));

	(window, lists, dirty)
}

/// Answers from every room of the account, and keeps an index of what that
/// found for the lists of this `digest`, unless there is none because the
/// switches are off.
async fn full(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	invite_filter: &InviteFilter,
	digest: Option<u64>,
	reason: SelectReason,
) -> (Window, ResponseLists, Dirty) {
	counted(sync_info.services, SelectPath::Full, reason, None);

	let (mut rooms, members) = full_path(conn, sync_info, invite_filter, digest.is_some()).await;
	let (window, lists) = finish(conn, sync_info, &mut rooms, invite_filter).await;

	conn.index = digest.map(|digest| {
		let mut index = build(conn.next_batch, digest, members, &rooms);

		record(&mut index, &window, window.keys().cloned());

		Box::new(index)
	});

	(window, lists, Dirty::Window)
}

/// Revises the rooms of the index the journal names, and gives the rooms the
/// index selects now.
///
/// A room the journal names is looked at again only when the user has to do with
/// it: it is in the index, or the user is or was in it, or the user's own
/// membership of it changed. It is judged as the full pass would judge it, so
/// that a pass over the index and a pass over every room agree. A read that
/// fails fails the revision, and the index is not kept.
async fn revise(
	mut index: RoomIndex,
	changes: &Changes,
	conn: &Connection,
	sync_info: SyncInfo<'_>,
	invite_filter: &InviteFilter,
) -> Result<Revised> {
	let touched = touched(&index, changes);

	let revisions = touched
		.iter()
		.copied()
		.stream()
		.broad_then(|room_id| revision(sync_info, conn, invite_filter, room_id))
		.collect::<Vec<_>>()
		.await
		.into_iter()
		.collect::<Result<Vec<_>>>()?;

	let revised = revisions.len();
	for revision in revisions {
		apply(&mut index, revision);
	}

	let rooms = rooms_of(&index);

	Ok(Revised { index, rooms, revised })
}

/// The rooms of a window of the journal that the index has to look at again.
fn touched<'a>(index: &RoomIndex, changes: &'a Changes) -> Vec<&'a OwnedRoomId> {
	changes
		.rooms
		.iter()
		.chain(
			changes
				.own_membership
				.iter()
				.filter(|room_id| !changes.rooms.contains(*room_id)),
		)
		.filter(|room_id| {
			changes.own_membership.contains(*room_id)
				|| index.members.contains(*room_id)
				|| index.selected.contains_key(*room_id)
		})
		.collect()
}

/// Judges one room as the full pass does.
///
/// The user's own membership gives an active one, or none. An invite the filter
/// refuses is none. A room the matcher takes under an active membership is the
/// room. Otherwise a room the connection tracks that the user has left is a
/// retraction.
async fn revision(
	sync_info: SyncInfo<'_>,
	conn: &Connection,
	invite_filter: &InviteFilter,
	room_id: &OwnedRoomId,
) -> Result<Revision> {
	let SyncInfo { services, sender_user, .. } = sync_info;

	let active = active_membership(services, sender_user, room_id).await?;

	let member = active.is_some();
	let permitted = match active {
		| Some(MembershipState::Invite) =>
			invite_permitted_room(services, sender_user, room_id, invite_filter).await,
		| _ => true,
	};

	let selected = match active.filter(|_| permitted) {
		| Some(membership) => matcher(sync_info, conn, room_id.clone(), Some(membership)).await,
		| None => None,
	};

	let room = match selected {
		| Some(room) => Some(room),
		| None => retraction(sync_info, conn, room_id).await?,
	};

	Ok(Revision {
		room_id: room_id.clone(),
		member,
		entry: room.as_ref().map(indexed),
	})
}

/// The membership the user holds in a room that the full pass lists: joined,
/// invited or knocking, in that order.
async fn active_membership(
	services: &Services,
	sender_user: &UserId,
	room_id: &OwnedRoomId,
) -> Result<Option<MembershipState>> {
	let state_cache = &services.state_cache;

	if state_cache
		.try_is_joined(sender_user, room_id)
		.await?
	{
		return Ok(Some(MembershipState::Join));
	}

	if state_cache
		.try_is_invited(sender_user, room_id)
		.await?
	{
		return Ok(Some(MembershipState::Invite));
	}

	Ok(state_cache
		.is_knocked(sender_user, room_id)
		.await
		.then_some(MembershipState::Knock))
}

/// The room as a retraction, when the connection tracks it and the user has a
/// departure from it recorded.
async fn retraction(
	sync_info: SyncInfo<'_>,
	conn: &Connection,
	room_id: &OwnedRoomId,
) -> Result<Option<WindowRoom>> {
	let SyncInfo { services, sender_user, .. } = sync_info;

	if !conn.rooms.contains_key(room_id) {
		return Ok(None);
	}

	match services
		.state_cache
		.get_departure_count(room_id, sender_user)
		.await
	{
		| Ok(_) =>
			Ok(matcher(sync_info, conn, room_id.clone(), Some(MembershipState::Leave)).await),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

/// The rooms of the window a pass owes the ranges of.
///
/// A room is owed when a pass failed to collect it, when the journal names it
/// since the index was exact, when it entered the window, when it is a retraction,
/// and when collecting it would send a payload anyway: its payload is newer than
/// what was delivered, or the configuration it is delivered under changed. For any
/// other room the window had nothing since the pass that collected it.
fn owed(
	conn: &Connection,
	window: &Window,
	index: &RoomIndex,
	changes: &Changes,
) -> HashSet<OwnedRoomId> {
	window
		.iter()
		.filter(|(_, room)| owes(conn, index, changes, room))
		.map(|(room_id, _)| room_id.clone())
		.collect()
}

fn owes(conn: &Connection, index: &RoomIndex, changes: &Changes, room: &WindowRoom) -> bool {
	let room_id = &room.room_id;
	let (roomsince, config_hash) = conn
		.rooms
		.get(room_id)
		.map_or((0, 0), |delivered| (delivered.roomsince, delivered.config_hash));

	index.owed.contains(room_id)
		|| changes.rooms.contains(room_id)
		|| changes.own_membership.contains(room_id)
		|| !index.window.contains(room_id)
		|| departed(room.membership.as_ref())
		|| room.payload_is_fresh(roomsince)
		|| room_config(&merged_room_details(conn, &room.lists, room_id)).0 != config_hash
}

/// Records the window a pass chose, and adds the rooms it is to collect to the
/// rooms owed.
fn record(index: &mut RoomIndex, window: &Window, rooms: impl Iterator<Item = OwnedRoomId>) {
	index.owed.extend(rooms);
	index.window = window.keys().cloned().collect();
}

/// Takes the rooms a pass collected off the rooms its connection owes ranges, and
/// leaves every other.
///
/// A room that failed is not among them, so it is collected again.
pub(super) fn pay<'a>(
	conn: &mut Connection,
	collected: impl Iterator<Item = &'a OwnedRoomId> + 'a,
) {
	let Some(index) = conn.index.as_mut() else {
		return;
	};

	for room_id in collected {
		index.owed.remove(room_id);
	}
}

/// The rooms the full pass found something to send in that the index left out.
fn unowed<'a>(
	clean: &HashSet<OwnedRoomId>,
	content: impl Iterator<Item = &'a OwnedRoomId> + 'a,
) -> Vec<Difference> {
	content
		.filter(|room_id| clean.contains(*room_id))
		.map(|room_id| Difference {
			part: Mismatch::Range,
			subject: room_id.to_string(),
		})
		.collect()
}

fn apply(index: &mut RoomIndex, revision: Revision) {
	let Revision { room_id, member, entry } = revision;

	if member {
		index.members.insert(room_id.clone());
	} else {
		index.members.remove(&room_id);
	}

	match entry {
		| Some(entry) => {
			index.selected.insert(room_id, entry);
		},
		| None => {
			index.selected.remove(&room_id);
		},
	}
}

fn indexed(room: &WindowRoom) -> IndexedRoom {
	IndexedRoom {
		membership: room.membership.clone(),
		lists: room.lists.clone(),
		event_count: room.event_count,
		payload_count: room.payload_count,
	}
}

fn rooms_of(index: &RoomIndex) -> Vec<WindowRoom> {
	index
		.selected
		.iter()
		.map(|(room_id, room)| WindowRoom {
			room_id: room_id.clone(),
			membership: room.membership.clone(),
			forgotten: false,
			lists: room.lists.clone(),
			event_count: room.event_count,
			payload_count: room.payload_count,
		})
		.collect()
}

/// Builds the index of what a full pass selected.
///
/// A room the full pass lists twice, under an active membership and as a
/// retraction, is kept under the active one, as the window keeps it.
fn build(at: u64, digest: u64, members: HashSet<OwnedRoomId>, rooms: &[WindowRoom]) -> RoomIndex {
	let mut selected = HashMap::with_capacity(rooms.len());

	for room in rooms {
		match selected.entry(room.room_id.clone()) {
			| Entry::Vacant(slot) => {
				slot.insert(indexed(room));
			},
			| Entry::Occupied(mut held) =>
				if departed(held.get().membership.as_ref()) && !departed(room.membership.as_ref())
				{
					held.insert(indexed(room));
				},
		}
	}

	RoomIndex {
		at,
		built: Instant::now(),
		digest,
		members,
		selected,
		window: HashSet::new(),
		owed: HashSet::new(),
	}
}

/// Compares what the client would see of two passes: the rooms of the window
/// with the fields that place them, and the count of every list.
fn compare(
	(window, lists): (&Window, &ResponseLists),
	(shadow_window, shadow_lists): (&Window, &ResponseLists),
) -> Vec<Difference> {
	let rooms = window
		.iter()
		.filter(|(room_id, room)| {
			shadow_window
				.get(*room_id)
				.is_none_or(|shadow| !same_room(room, shadow))
		})
		.map(|(room_id, _)| room_id)
		.chain(
			shadow_window
				.keys()
				.filter(|room_id| !window.contains_key(*room_id)),
		)
		.map(|room_id| Difference {
			part: Mismatch::Window,
			subject: room_id.to_string(),
		});

	let counts = lists
		.iter()
		.filter(|(id, list)| {
			shadow_lists
				.get(*id)
				.is_none_or(|shadow| shadow.count != list.count)
		})
		.map(|(id, _)| id)
		.chain(
			shadow_lists
				.keys()
				.filter(|id| !lists.contains_key(*id)),
		)
		.map(|id| Difference {
			part: Mismatch::Count,
			subject: id.to_string(),
		});

	rooms.chain(counts).collect()
}

fn same_room(a: &WindowRoom, b: &WindowRoom) -> bool {
	a.membership == b.membership
		&& a.lists == b.lists
		&& a.event_count == b.event_count
		&& a.payload_count == b.payload_count
}

/// Judges the differences between the index and the full pass, and reports those
/// that no write after the pass explains.
///
/// The index was revised and the full pass read live one after the other, so a
/// write that landed after the position the pass reads up to can show in the full
/// pass alone. The journal is asked what was written after that position, once
/// the full pass has read, up to the latest number dispatched: a write the full
/// pass could read may not have retired yet. When the journal cannot say, nothing
/// is judged.
fn judge(
	services: &Services,
	sender_user: &UserId,
	differences: Vec<Difference>,
	(at, next_batch, limit): (u64, u64, usize),
	known: impl Fn(&OwnedRoomId) -> bool,
) {
	if differences.is_empty() {
		return;
	}

	let Some(written) = services.sync.journal.changes(
		sender_user,
		next_batch,
		services.globals.pending_count().end,
		limit,
	) else {
		skipped(services, Skipped::Journal);

		return;
	};

	let (differences, explained) = settle(differences, &written, known);

	if explained > 0 {
		skipped(services, Skipped::Explained);
	}

	report(services, sender_user, &differences, (at, next_batch));
}

/// Sets aside the differences a write after the pass accounts for, and returns
/// the others with the number set aside.
///
/// A difference of a room is set aside when the write named that room. A
/// difference of a count is set aside when the write named a room the user has to
/// do with, since the count may have moved with it.
fn settle(
	differences: Vec<Difference>,
	written: &Changes,
	known: impl Fn(&OwnedRoomId) -> bool,
) -> (Vec<Difference>, usize) {
	let total = differences.len();

	let counts_moved = written
		.rooms
		.iter()
		.chain(&written.own_membership)
		.any(known);

	let unexplained = differences
		.into_iter()
		.filter(|difference| match difference.part {
			| Mismatch::Count => !counts_moved,
			| Mismatch::Window | Mismatch::Range => match RoomId::parse(&difference.subject) {
				| Ok(room_id) =>
					!written.rooms.contains(&room_id)
						&& !written.own_membership.contains(&room_id),
				| Err(_) => true,
			},
		})
		.collect::<Vec<_>>();

	let explained = total.saturating_sub(unexplained.len());

	(unexplained, explained)
}

fn skipped(services: &Services, reason: Skipped) {
	if let Some(registry) = services.server.metrics.prometheus() {
		registry.sync.v5_candidates_skip(reason);
	}
}

/// Counts and logs the differences of one pass.
fn report(
	services: &Services,
	sender_user: &UserId,
	differences: &[Difference],
	(at, next_batch): (u64, u64),
) {
	for Difference { part, subject } in differences.iter().take(MISMATCH_LOGS) {
		error!(
			user_id = %sender_user,
			%subject,
			?part,
			index_at = at,
			next_batch,
			"sliding sync index differs from the full pass"
		);
	}

	if differences.len() > MISMATCH_LOGS {
		error!(
			user_id = %sender_user,
			differences = differences.len(),
			index_at = at,
			next_batch,
			"sliding sync index differs from the full pass in more places than are logged"
		);
	}

	if let Some(registry) = services.server.metrics.prometheus() {
		for difference in differences {
			registry
				.sync
				.v5_candidates_mismatch(difference.part);
		}
	}
}

/// Counts a pass by the path it took, and the rooms it revised when it took the
/// index.
fn counted(services: &Services, path: SelectPath, reason: SelectReason, revised: Option<usize>) {
	let Some(registry) = services.server.metrics.prometheus() else {
		return;
	};

	registry.sync.v5_selected(path, reason);

	if let Some(revised) = revised {
		registry
			.sync
			.v5_candidates(u64::try_from(revised).unwrap_or(u64::MAX));
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{HashMap, HashSet},
		time::{Duration, Instant},
	};

	use ruma::{
		OwnedRoomId,
		api::client::sync::sync_events::v5::{
			ListId,
			request::{List, ListConfig, ListFilters},
			response,
		},
		events::room::member::MembershipState,
		room_id, uint,
	};
	use tuwunel_core::metrics::prom::{Mismatch, SelectReason};
	use tuwunel_service::sync::{Changes, Connection, IndexedRoom, Lists, Room, RoomIndex};

	use super::{
		super::{
			ListIds, ResponseLists, Window, WindowRoom,
			rooms::{merged_room_details, room_config},
		},
		Difference, Dirty, Facts, LIMIT_FLOOR, Mode, REFRESH, Revision, apply, build, classify,
		compare, digest, limit, owed, owes, pay, precheck, record, settle, spaces, touched,
		unowed,
	};

	const AT: u64 = 10;
	const DIGEST: u64 = 77;

	fn index() -> RoomIndex {
		RoomIndex {
			at: AT,
			built: Instant::now(),
			digest: DIGEST,
			members: HashSet::new(),
			selected: HashMap::new(),
			window: HashSet::new(),
			owed: HashSet::new(),
		}
	}

	fn facts() -> Facts {
		Facts {
			replay: false,
			digest: DIGEST,
			spaces: false,
			next_batch: AT + 5,
		}
	}

	fn fresh(facts: Facts) -> Result<RoomIndex, SelectReason> {
		precheck(Mode::Candidates, Some(index()), facts, Instant::now())
	}

	fn room(room_id: &str, membership: MembershipState, event_count: u64) -> WindowRoom {
		WindowRoom {
			room_id: OwnedRoomId::try_from(room_id).expect("room id must parse"),
			membership: Some(membership),
			forgotten: false,
			lists: ListIds::from_iter([ListId::from("all")]),
			event_count,
			payload_count: event_count,
		}
	}

	fn indexed_room(event_count: u64) -> IndexedRoom {
		IndexedRoom {
			membership: Some(MembershipState::Join),
			lists: ListIds::from_iter([ListId::from("all")]),
			event_count,
			payload_count: event_count,
		}
	}

	fn window(rooms: &[WindowRoom]) -> Window {
		rooms
			.iter()
			.map(|room| (room.room_id.clone(), room.clone()))
			.collect()
	}

	fn counts(counts: &[(&str, u32)]) -> ResponseLists {
		counts
			.iter()
			.map(|(id, count)| (ListId::from(*id), response::List { count: (*count).into() }))
			.collect()
	}

	#[test]
	fn the_keys_select_a_mode_only_with_the_journal_on() {
		assert_eq!(Mode::select(true, false, false), Mode::Off);
		assert_eq!(Mode::select(true, true, false), Mode::Candidates);
		assert_eq!(Mode::select(true, false, true), Mode::Verify);
		assert_eq!(Mode::select(true, true, true), Mode::Verify);
		assert_eq!(Mode::select(false, true, false), Mode::Off);
		assert_eq!(Mode::select(false, true, true), Mode::Off);
	}

	#[test]
	fn an_index_that_is_fresh_and_in_step_is_revised() {
		fresh(facts()).expect("an index that is fresh and in step must be revised");
	}

	#[test]
	fn the_reasons_for_the_full_pass_are_told_in_order() {
		let now = Instant::now();

		assert_eq!(
			precheck(Mode::Off, Some(index()), facts(), now).err(),
			Some(SelectReason::Off)
		);
		assert_eq!(
			precheck(Mode::Candidates, None, facts(), now).err(),
			Some(SelectReason::NoIndex)
		);
		assert_eq!(fresh(Facts { replay: true, ..facts() }).err(), Some(SelectReason::Replay));
		assert_eq!(
			fresh(Facts { next_batch: AT - 1, ..facts() }).err(),
			Some(SelectReason::Replay)
		);
		assert_eq!(
			fresh(Facts { digest: DIGEST + 1, ..facts() }).err(),
			Some(SelectReason::Filters)
		);
		assert_eq!(fresh(Facts { spaces: true, ..facts() }).err(), Some(SelectReason::Spaces));

		assert_eq!(
			fresh(Facts {
				replay: true,
				digest: DIGEST + 1,
				spaces: true,
				..facts()
			})
			.err(),
			Some(SelectReason::Replay)
		);
		assert_eq!(
			fresh(Facts {
				digest: DIGEST + 1,
				spaces: true,
				..facts()
			})
			.err(),
			Some(SelectReason::Filters)
		);
	}

	#[test]
	fn an_index_older_than_the_refresh_interval_is_built_again() {
		let built = Instant::now();
		let edge = built
			.checked_add(REFRESH)
			.expect("instant must fit");
		let past = edge
			.checked_add(Duration::from_secs(1))
			.expect("instant must fit");

		precheck(Mode::Candidates, Some(RoomIndex { built, ..index() }), facts(), edge)
			.expect("an index at the edge of the interval must be revised");
		assert_eq!(
			precheck(Mode::Candidates, Some(RoomIndex { built, ..index() }), facts(), past).err(),
			Some(SelectReason::Refresh)
		);
	}

	#[test]
	fn the_journal_answers_are_sorted_into_reasons() {
		let changes = Changes::default();

		classify(Some(changes), || false).expect("a window with no news must be revised");
		assert_eq!(classify(None, || true).err(), Some(SelectReason::Horizon));
		assert_eq!(classify(None, || false).err(), Some(SelectReason::Overflow));
		assert_eq!(
			classify(
				Some(Changes {
					global_account_data: true,
					..Default::default()
				}),
				|| { false }
			)
			.err(),
			Some(SelectReason::AccountData)
		);
	}

	#[test]
	fn the_limit_has_a_floor_and_grows_with_the_account() {
		assert_eq!(limit(0), LIMIT_FLOOR);
		assert_eq!(limit(2 * LIMIT_FLOOR), LIMIT_FLOOR);
		assert_eq!(limit(10_000), 5_000);
	}

	#[test]
	fn the_digest_follows_the_lists_and_their_filters_alone() {
		let list = |filters: Option<ListFilters>| List { filters, ..Default::default() };
		let lists = |entries: &[(&str, List)]| -> Lists {
			entries
				.iter()
				.map(|(id, list)| (ListId::from(*id), list.clone()))
				.collect()
		};

		let direct = ListFilters { is_dm: Some(true), ..Default::default() };
		let base = lists(&[("all", list(None))]);

		assert_eq!(digest(&base), digest(&lists(&[("all", list(None))])));

		let mut ranged = list(None);
		ranged.ranges = vec![(uint!(0), uint!(9))].into();

		assert_eq!(digest(&base), digest(&lists(&[("all", ranged)])));
		assert_ne!(digest(&base), digest(&lists(&[("all", list(Some(direct.clone())))])));
		assert_ne!(digest(&base), digest(&lists(&[("all", list(None)), ("dm", list(None))])));
		assert_ne!(digest(&base), digest(&lists(&[("other", list(None))])));
		assert_ne!(
			digest(&lists(&[("all", list(Some(direct)))])),
			digest(&lists(&[(
				"all",
				list(Some(ListFilters { is_dm: Some(false), ..Default::default() }))
			)]))
		);
	}

	#[test]
	fn a_filter_by_space_is_seen_in_any_list() {
		let spaced = ListFilters {
			spaces: vec![room_id!("!space:example.com").to_owned()],
			..Default::default()
		};

		let plain: Lists = [(ListId::from("all"), List::default())].into();
		let mixed: Lists = [
			(ListId::from("all"), List::default()),
			(ListId::from("spaced"), List {
				filters: Some(spaced),
				..Default::default()
			}),
			(ListId::from("empty"), List {
				filters: Some(ListFilters::default()),
				..Default::default()
			}),
		]
		.into();

		assert!(!spaces(&plain));
		assert!(spaces(&mixed));
	}

	#[test]
	fn only_the_rooms_the_user_has_to_do_with_are_looked_at_again() {
		let member = room_id!("!member:example.com").to_owned();
		let selected = room_id!("!selected:example.com").to_owned();
		let foreign = room_id!("!foreign:example.com").to_owned();
		let entered = room_id!("!entered:example.com").to_owned();

		let mut held = index();
		held.members.insert(member.clone());
		held.selected
			.insert(selected.clone(), indexed_room(3));

		let changes = Changes {
			rooms: [member.clone(), selected.clone(), foreign, entered.clone()].into(),
			own_membership: [entered.clone()].into(),
			global_account_data: false,
		};

		let mut found = touched(&held, &changes);
		found.sort();

		assert_eq!(found, [&entered, &member, &selected]);
	}

	#[test]
	fn a_room_named_for_both_reasons_is_looked_at_once() {
		let both = room_id!("!both:example.com").to_owned();
		let changes = Changes {
			rooms: [both.clone()].into(),
			own_membership: [both.clone()].into(),
			global_account_data: false,
		};

		assert_eq!(touched(&index(), &changes), [&both]);
	}

	#[test]
	fn a_room_that_no_list_takes_stays_a_member_and_a_departure_ends_both() {
		let outside = room_id!("!outside:example.com").to_owned();
		let mut held = index();

		apply(&mut held, Revision {
			room_id: outside.clone(),
			member: true,
			entry: None,
		});

		assert!(held.members.contains(&outside));
		assert!(!held.selected.contains_key(&outside));

		apply(&mut held, Revision {
			room_id: outside.clone(),
			member: true,
			entry: Some(indexed_room(4)),
		});

		assert_eq!(held.selected[&outside].event_count, 4);

		apply(&mut held, Revision {
			room_id: outside.clone(),
			member: false,
			entry: None,
		});

		assert!(!held.members.contains(&outside));
		assert!(!held.selected.contains_key(&outside));
	}

	#[test]
	fn a_retraction_selects_a_room_the_user_is_not_a_member_of() {
		let left = room_id!("!left:example.com").to_owned();
		let mut held = index();
		held.members.insert(left.clone());

		apply(&mut held, Revision {
			room_id: left.clone(),
			member: false,
			entry: Some(IndexedRoom {
				membership: Some(MembershipState::Leave),
				..indexed_room(6)
			}),
		});

		assert!(!held.members.contains(&left));
		assert_eq!(held.selected[&left].membership, Some(MembershipState::Leave));
	}

	#[test]
	fn an_active_membership_wins_over_a_retraction_of_the_same_room() {
		let rooms = |first: MembershipState, second: MembershipState| {
			[room("!twice:example.com", first, 5), room("!twice:example.com", second, 9)]
		};

		let twice = room_id!("!twice:example.com");

		for rooms in [
			rooms(MembershipState::Join, MembershipState::Leave),
			rooms(MembershipState::Leave, MembershipState::Join),
		] {
			let built = build(AT, DIGEST, HashSet::new(), &rooms);

			assert_eq!(built.selected.len(), 1);
			assert_eq!(built.selected[twice].membership, Some(MembershipState::Join));
		}
	}

	#[test]
	fn a_built_index_keeps_the_members_no_list_takes() {
		let outside = room_id!("!outside:example.com").to_owned();
		let built = build(AT + 2, DIGEST, [outside.clone()].into(), &[room(
			"!inside:example.com",
			MembershipState::Join,
			2,
		)]);

		assert_eq!(built.at, AT + 2);
		assert_eq!(built.digest, DIGEST);
		assert!(built.members.contains(&outside));
		assert!(!built.selected.contains_key(&outside));
		assert_eq!(built.selected.len(), 1);
	}

	fn written(rooms: &[&str], own_membership: &[&str]) -> Changes {
		let ids = |names: &[&str]| -> HashSet<OwnedRoomId> {
			names
				.iter()
				.map(|name| OwnedRoomId::try_from(*name).expect("room id must parse"))
				.collect()
		};

		Changes {
			rooms: ids(rooms),
			own_membership: ids(own_membership),
			global_account_data: false,
		}
	}

	fn window_difference(subject: &str) -> Difference {
		Difference {
			part: Mismatch::Window,
			subject: subject.to_owned(),
		}
	}

	fn count_difference(subject: &str) -> Difference {
		Difference {
			part: Mismatch::Count,
			subject: subject.to_owned(),
		}
	}

	#[test]
	fn a_room_the_later_write_named_is_set_aside_and_another_is_not() {
		let known = |_: &OwnedRoomId| false;
		let named = written(&["!rewritten:example.com"], &["!joined:example.com"]);

		let (left, explained) = settle(
			vec![
				window_difference("!rewritten:example.com"),
				window_difference("!joined:example.com"),
				window_difference("!untouched:example.com"),
			],
			&named,
			known,
		);

		assert_eq!(left, [window_difference("!untouched:example.com")]);
		assert_eq!(explained, 2);
	}

	#[test]
	fn a_count_is_set_aside_only_when_the_write_named_a_room_the_user_has_to_do_with() {
		let known = |room_id: &OwnedRoomId| room_id == room_id!("!member:example.com");

		let (left, explained) =
			settle(vec![count_difference("all")], &written(&["!member:example.com"], &[]), known);

		assert!(left.is_empty());
		assert_eq!(explained, 1);

		let (left, explained) =
			settle(vec![count_difference("all")], &written(&[], &["!member:example.com"]), known);

		assert!(left.is_empty());
		assert_eq!(explained, 1);

		let (left, explained) = settle(
			vec![count_difference("all")],
			&written(&["!foreign:example.com"], &[]),
			known,
		);

		assert_eq!(left, [count_difference("all")]);
		assert_eq!(explained, 0);

		let (left, explained) = settle(vec![count_difference("all")], &written(&[], &[]), known);

		assert_eq!(left, [count_difference("all")]);
		assert_eq!(explained, 0);
	}

	#[test]
	fn a_difference_about_a_room_that_is_no_room_id_is_never_set_aside() {
		let (left, explained) = settle(
			vec![window_difference("not a room")],
			&written(&["!rewritten:example.com"], &[]),
			|_| true,
		);

		assert_eq!(left, [window_difference("not a room")]);
		assert_eq!(explained, 0);
	}

	#[test]
	fn passes_that_agree_have_no_difference() {
		let rooms = [
			room("!a:example.com", MembershipState::Join, 3),
			room("!b:example.com", MembershipState::Invite, 2),
		];

		let lists = counts(&[("all", 2)]);

		assert!(compare((&window(&rooms), &lists), (&window(&rooms), &lists)).is_empty());
	}

	#[test]
	fn a_room_missing_from_either_window_or_placed_otherwise_is_a_window_difference() {
		let a = room("!a:example.com", MembershipState::Join, 3);
		let b = room("!b:example.com", MembershipState::Join, 2);
		let lists = counts(&[("all", 2)]);

		assert_eq!(
			compare(
				(&window(&[a.clone(), b.clone()]), &lists),
				(&window(std::slice::from_ref(&a)), &lists)
			),
			[window_difference("!b:example.com")]
		);
		assert_eq!(
			compare(
				(&window(std::slice::from_ref(&a)), &lists),
				(&window(&[a.clone(), b]), &lists)
			),
			[window_difference("!b:example.com")]
		);

		let variants = [
			WindowRoom { event_count: 4, ..a.clone() },
			WindowRoom { payload_count: 4, ..a.clone() },
			WindowRoom {
				membership: Some(MembershipState::Invite),
				..a.clone()
			},
			WindowRoom { lists: ListIds::new(), ..a.clone() },
		];

		for variant in variants {
			assert_eq!(
				compare(
					(&window(std::slice::from_ref(&a)), &lists),
					(&window(&[variant]), &lists)
				),
				[window_difference("!a:example.com")]
			);
		}
	}

	#[test]
	fn a_list_counted_otherwise_or_on_one_side_alone_is_a_count_difference() {
		let empty = Window::new();

		assert_eq!(
			compare((&empty, &counts(&[("all", 2)])), (&empty, &counts(&[("all", 3)]))),
			[count_difference("all")]
		);
		assert_eq!(compare((&empty, &counts(&[("all", 2)])), (&empty, &counts(&[]))), [
			count_difference("all")
		]);
		assert_eq!(compare((&empty, &counts(&[])), (&empty, &counts(&[("dm", 0)]))), [
			count_difference("dm")
		]);
	}

	type Change = fn(&mut Calm);

	struct Calm {
		conn: Connection,
		index: RoomIndex,
		changes: Changes,
		room: WindowRoom,
	}

	fn calm() -> Calm {
		let room = room("!calm:example.com", MembershipState::Join, 3);
		let mut conn = Connection::default();

		conn.lists.insert(ListId::from("all"), List {
			room_details: ListConfig {
				timeline_limit: uint!(1),
				..Default::default()
			},
			..Default::default()
		});

		let (config_hash, _) =
			room_config(&merged_room_details(&conn, &room.lists, &room.room_id));

		conn.rooms.insert(room.room_id.clone(), Room {
			roomsince: 5,
			config_hash,
			..Default::default()
		});

		let mut index = index();
		index.window.insert(room.room_id.clone());

		Calm {
			conn,
			index,
			changes: Changes::default(),
			room,
		}
	}

	fn owes_now(calm: &Calm) -> bool { owes(&calm.conn, &calm.index, &calm.changes, &calm.room) }

	fn delivered(calm: &mut Calm) -> &mut Room {
		calm.conn
			.rooms
			.get_mut(&calm.room.room_id)
			.expect("the calm room must have been delivered")
	}

	#[test]
	fn a_room_with_nothing_new_is_owed_nothing() {
		assert!(!owes_now(&calm()));
	}

	#[test]
	fn a_room_is_owed_for_each_of_its_reasons_alone() {
		let reasons: [(&str, Change); 11] = [
			("a pass failed to collect it", |calm| {
				calm.index.owed.insert(calm.room.room_id.clone());
			}),
			("the journal names it", |calm| {
				calm.changes
					.rooms
					.insert(calm.room.room_id.clone());
			}),
			("the journal names the user's membership of it", |calm| {
				calm.changes
					.own_membership
					.insert(calm.room.room_id.clone());
			}),
			("it entered the window", |calm| calm.index.window.clear()),
			("it is a retraction", |calm| {
				calm.room.membership = Some(MembershipState::Leave);
			}),
			("it was never delivered", |calm| delivered(calm).roomsince = 0),
			("its payload is newer than its delivery", |calm| calm.room.payload_count = 9),
			("its configuration was delivered otherwise", |calm| {
				delivered(calm).config_hash += 1;
			}),
			("a list asks for another timeline limit", |calm| {
				calm.conn
					.lists
					.get_mut(&ListId::from("all"))
					.expect("the list must exist")
					.room_details
					.timeline_limit = uint!(7);
			}),
			("the connection subscribed to it", |calm| {
				calm.conn
					.subscriptions
					.insert(calm.room.room_id.clone(), ListConfig {
						timeline_limit: uint!(9),
						..Default::default()
					});
			}),
			("it left the list that asks for its timeline", |calm| {
				calm.room.lists = ListIds::new();
			}),
		];

		for (reason, change) in reasons {
			let mut calm = calm();
			change(&mut calm);

			assert!(owes_now(&calm), "a room must be owed when {reason}");
		}
	}

	#[test]
	fn only_the_rooms_that_are_owed_are_collected() {
		let quiet = calm();
		let named = room("!named:example.com", MembershipState::Join, 3);
		let entered = room("!entered:example.com", MembershipState::Join, 3);
		let mut conn = quiet.conn;
		let (config_hash, _) =
			room_config(&merged_room_details(&conn, &named.lists, &named.room_id));

		for owed_room in [&named, &entered] {
			conn.rooms
				.insert(owed_room.room_id.clone(), Room {
					roomsince: 5,
					config_hash,
					..Default::default()
				});
		}

		let mut index = quiet.index;
		index.window.insert(named.room_id.clone());

		let changes = written(&["!named:example.com"], &[]);
		let window = window(&[quiet.room, named.clone(), entered.clone()]);

		let found = owed(&conn, &window, &index, &changes);

		assert_eq!(found, HashSet::from([named.room_id, entered.room_id]));
	}

	#[test]
	fn a_pass_takes_off_the_rooms_it_collected_and_no_others() {
		let [a, b, c] = ["!a:example.com", "!b:example.com", "!c:example.com"]
			.map(|room_id| OwnedRoomId::try_from(room_id).expect("room id must parse"));

		let mut conn = Connection {
			index: Some(Box::new(RoomIndex {
				owed: HashSet::from([a.clone(), b.clone(), c.clone()]),
				..index()
			})),
			..Default::default()
		};

		pay(&mut conn, [&a, &c].into_iter());

		let held = conn
			.index
			.as_ref()
			.expect("the index must be kept");

		assert_eq!(held.owed, HashSet::from([b]));

		let mut bare = Connection::default();

		pay(&mut bare, [&a].into_iter());

		assert!(bare.index.is_none());
	}

	#[test]
	fn a_pass_records_its_window_and_keeps_what_it_owed_before() {
		let [old, kept, taken] = ["!old:example.com", "!kept:example.com", "!taken:example.com"]
			.map(|room_id| OwnedRoomId::try_from(room_id).expect("room id must parse"));

		let mut held = index();
		held.window.insert(old.clone());
		held.owed.insert(old.clone());

		let window = window(&[
			room("!kept:example.com", MembershipState::Join, 1),
			room("!taken:example.com", MembershipState::Join, 2),
		]);

		record(&mut held, &window, [taken.clone()].into_iter());

		assert_eq!(held.window, HashSet::from([kept, taken.clone()]));
		assert_eq!(held.owed, HashSet::from([old, taken]));
	}

	#[test]
	fn a_full_pass_with_something_to_send_in_a_room_the_index_left_out_is_a_range_difference() {
		let [left_out, owed_room, silent] =
			["!left_out:example.com", "!owed:example.com", "!silent:example.com"]
				.map(|room_id| OwnedRoomId::try_from(room_id).expect("room id must parse"));

		let clean = HashSet::from([left_out.clone(), silent]);
		let content = [&owed_room, &left_out];

		assert_eq!(unowed(&clean, content.into_iter()), [Difference {
			part: Mismatch::Range,
			subject: left_out.to_string(),
		}]);
		assert!(unowed(&clean, std::iter::empty()).is_empty());
	}

	#[test]
	fn a_range_difference_is_set_aside_only_when_the_write_named_the_room() {
		let range_difference = |subject: &str| Difference {
			part: Mismatch::Range,
			subject: subject.to_owned(),
		};

		let named = written(&["!named:example.com"], &[]);
		let (left, explained) = settle(
			vec![range_difference("!named:example.com"), range_difference("!other:example.com")],
			&named,
			|_| true,
		);

		assert_eq!(left, [range_difference("!other:example.com")]);
		assert_eq!(explained, 1);
	}

	#[test]
	fn only_the_rooms_of_the_dirty_set_are_asked_for() {
		let room_id = room_id!("!asked:example.com").to_owned();

		assert!(Dirty::Window.only().is_none());
		assert!(
			Dirty::Rooms(HashSet::from([room_id.clone()]))
				.only()
				.is_some_and(|rooms| rooms.contains(&room_id))
		);
	}
}
