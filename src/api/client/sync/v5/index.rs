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
#[tracing::instrument(level = "debug", skip_all)]
pub(super) async fn select(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	replay: bool,
) -> (Window, ResponseLists) {
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
) -> Result<(Window, ResponseLists)> {
	let Revised { mut index, mut rooms, revised } =
		revise(index, changes, conn, sync_info, invite_filter).await?;

	let selection = finish(conn, sync_info, &mut rooms, invite_filter).await;

	index.at = conn.next_batch;
	conn.index = Some(Box::new(index));

	counted(sync_info.services, SelectPath::Candidates, SelectReason::None, Some(revised));

	Ok(selection)
}

/// Answers from every room of the account, and compares the index revised
/// beside it with the answer.
///
/// The index is built again from the full pass whatever the comparison found, so
/// a difference does not carry over to the next pass.
async fn verified(
	conn: &mut Connection,
	sync_info: SyncInfo<'_>,
	invite_filter: &InviteFilter,
	(index, changes): (RoomIndex, &Changes),
	digest: u64,
) -> (Window, ResponseLists) {
	let SyncInfo { services, sender_user, .. } = sync_info;

	let at = index.at;
	let limit = limit(index.members.len());
	let shadow = revise(index, changes, conn, sync_info, invite_filter).await;

	let (mut rooms, members) = full_path(conn, sync_info, invite_filter, true).await;
	let (window, lists) = finish(conn, sync_info, &mut rooms, invite_filter).await;
	let next_batch = conn.next_batch;

	match shadow {
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
		},
		| Err(error) => {
			warn!(
				user_id = %sender_user,
				%error,
				"sliding sync index could not be revised beside the full pass"
			);

			counted(services, SelectPath::Full, SelectReason::Error, None);
		},
	}

	conn.index = Some(Box::new(build(next_batch, digest, members, &rooms)));

	(window, lists)
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
) -> (Window, ResponseLists) {
	counted(sync_info.services, SelectPath::Full, reason, None);

	let (mut rooms, members) = full_path(conn, sync_info, invite_filter, digest.is_some()).await;
	let selection = finish(conn, sync_info, &mut rooms, invite_filter).await;

	conn.index = digest.map(|digest| Box::new(build(conn.next_batch, digest, members, &rooms)));

	selection
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
			| Mismatch::Window => match RoomId::parse(&difference.subject) {
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
			request::{List, ListFilters},
			response,
		},
		events::room::member::MembershipState,
		room_id, uint,
	};
	use tuwunel_core::metrics::prom::{Mismatch, SelectReason};
	use tuwunel_service::sync::{Changes, IndexedRoom, Lists, RoomIndex};

	use super::{
		super::{ListIds, ResponseLists, Window, WindowRoom},
		Difference, Facts, LIMIT_FLOOR, Mode, REFRESH, Revision, apply, build, classify, compare,
		digest, limit, precheck, settle, spaces, touched,
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
}
