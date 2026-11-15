use std::{
	collections::{BTreeMap, HashMap, HashSet},
	ops::Bound::{Excluded, Included, Unbounded},
	sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use ruma::{DeviceId, OwnedDeviceId, OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::{
	Result,
	metrics::{Metrics, prom::JournalKind},
	result::NotFound,
	smallvec::SmallVec,
	utils::u64_from_bytes,
};
use tuwunel_database::Map;

use crate::globals::Data;

/// A bounded in-memory log of the changes the sync endpoints report, keyed by
/// the global sequence number of the write.
///
/// Every writer of a stream the sync fast path probe reads records its number
/// here under the permit that holds it and before the write reaches the
/// database, so a row a reader finds has an entry here. The log is complete
/// for numbers above its horizon: it starts at the counter the server started
/// from and rises when the oldest entries are dropped to stay within capacity.
/// The lock is held only for the in-memory work of one call, never across an
/// await and never while calling another service; calls that only read share
/// it.
pub struct Journal {
	metrics: Arc<Metrics>,
	capacity: usize,
	state: RwLock<State>,
}

#[derive(Default)]
struct State {
	entries: BTreeMap<u64, Changes>,
	len: usize,
	horizon: u64,
}

type Changes = SmallVec<[Change; 1]>;

struct Change {
	kind: JournalKind,
	room: Option<OwnedRoomId>,
	user: Option<OwnedUserId>,
	device: Option<OwnedDeviceId>,
}

/// Which rooms one wait has found the user to be in.
///
/// A room maps to whether the user is in it, and a room the memo lacks is
/// unknown, unless the memo is complete, which means every room the user is in
/// is in the memo.
#[derive(Debug, Default)]
pub struct RoomMemo {
	rooms: HashMap<OwnedRoomId, bool>,
	complete: bool,
}

/// The verdict of [`Journal::scan`] on a window of changes.
#[derive(Debug, Eq, PartialEq)]
pub enum Scan {
	/// A change in the window concerns the user, of this kind.
	Wake(JournalKind),
	/// The window holds changes of rooms the memo does not know. The caller
	/// records whether the user is in each room, or fills the memo with all of
	/// them when `overflow` is set, and scans the same window again. With
	/// `overflow` the rooms are only those found before the limit was passed.
	Resolve {
		rooms: Vec<OwnedRoomId>,
		overflow: bool,
	},
	/// No change in the window concerns the user.
	Quiet,
	/// The journal no longer covers the start of the window; it is complete
	/// from this sequence number.
	Horizon(u64),
}

impl RoomMemo {
	/// Returns whether the user is in `room`, or `None` when the memo does not
	/// know.
	#[must_use]
	pub fn get(&self, room: &RoomId) -> Option<bool> { self.rooms.get(room).copied() }

	/// Notes whether the user is in `room`.
	pub fn insert(&mut self, room: OwnedRoomId, joined: bool) { self.rooms.insert(room, joined); }

	/// Notes that the memo holds every room the user is in, so a room it lacks
	/// is one the user is not in.
	pub fn mark_complete(&mut self) { self.complete = true; }
}

impl Journal {
	/// Creates a journal holding at most `capacity` changes, complete for
	/// sequence numbers above `horizon`. A capacity of 0 turns it off.
	pub(super) fn new(capacity: usize, horizon: u64, metrics: Arc<Metrics>) -> Self {
		Self {
			metrics,
			capacity,
			state: RwLock::new(State { horizon, ..State::default() }),
		}
	}

	/// Notes that the write numbered `count` changed `room`, for every user
	/// when `user` is `None` and for that user alone otherwise.
	pub fn record(&self, count: u64, kind: JournalKind, room: &RoomId, user: Option<&UserId>) {
		self.insert(count, Change {
			kind,
			room: Some(room.to_owned()),
			user: user.map(ToOwned::to_owned),
			device: None,
		});
	}

	/// Notes that the write numbered `count` changed something of `user` that
	/// belongs to no room, for the one `device` when it is given and for every
	/// device of the user otherwise.
	pub fn record_user(
		&self,
		count: u64,
		kind: JournalKind,
		user: &UserId,
		device: Option<&DeviceId>,
	) {
		self.insert(count, Change {
			kind,
			room: None,
			user: Some(user.to_owned()),
			device: device.map(ToOwned::to_owned),
		});
	}

	fn insert(&self, count: u64, change: Change) {
		if self.capacity == 0 {
			return;
		}

		let kind = change.kind;
		let mut state = self.write();

		state
			.entries
			.entry(count)
			.or_default()
			.push(change);
		state.len = state.len.saturating_add(1);

		let evicted = state.evict(self.capacity);

		if let Some(registry) = self.metrics.prometheus() {
			registry.sync.journal_recorded(kind);
			registry
				.sync
				.set_journal_entries(to_u64(state.len));

			if evicted > 0 {
				registry.sync.journal_evicted(to_u64(evicted));
			}
		}
	}

	/// Returns the rooms that changed for `user` after `since`, with no upper
	/// bound, or `None` when the journal cannot say: it is off, or `since`
	/// lies below its horizon.
	///
	/// One pass over the entries after `since` under the lock, so a caller
	/// asks once for a whole build.
	#[must_use]
	pub fn changed_rooms(&self, user: &UserId, since: u64) -> Option<HashSet<OwnedRoomId>> {
		if self.capacity == 0 {
			return None;
		}

		let state = self.read();

		if since < state.horizon {
			return None;
		}

		let rooms = state
			.entries
			.range((Excluded(since), Unbounded))
			.flat_map(|(_, changes)| changes)
			.filter(|change| is_room_kind(change.kind))
			.filter(|change| {
				change
					.user
					.as_deref()
					.is_none_or(|owner| owner == user)
			})
			.filter_map(|change| change.room.clone())
			.collect();

		Some(rooms)
	}

	/// Returns whether `room` changed for `user` after `since`, with no upper
	/// bound, or `None` when the journal cannot say: it is off, or `since`
	/// lies below its horizon.
	///
	/// One pass over the entries after `since` under the lock, for the rare
	/// room a snapshot of [`Self::changed_rooms`] does not hold.
	#[must_use]
	pub fn changed_since(&self, room: &RoomId, user: &UserId, since: u64) -> Option<bool> {
		if self.capacity == 0 {
			return None;
		}

		let state = self.read();

		if since < state.horizon {
			return None;
		}

		let changed = state
			.entries
			.range((Excluded(since), Unbounded))
			.flat_map(|(_, changes)| changes)
			.any(|change| {
				is_room_kind(change.kind)
					&& change.room.as_deref() == Some(room)
					&& change
						.user
						.as_deref()
						.is_none_or(|owner| owner == user)
			});

		Some(changed)
	}

	/// Counts a miss of `kind` for a room that the fast path found changed
	/// and the journal holds no change for, and returns whether it did.
	///
	/// `snapshot` is an answer of [`Self::changed_rooms`] taken earlier, and a
	/// room it holds is no miss. A room it lacks may have changed after the
	/// snapshot was taken, so the journal is asked once more for that room
	/// alone, and only a live answer of no change is a miss. A journal that
	/// cannot answer is no miss.
	pub fn audit(
		&self,
		kind: JournalKind,
		room: &RoomId,
		user: &UserId,
		since: u64,
		snapshot: Option<&HashSet<OwnedRoomId>>,
	) -> bool {
		if snapshot.is_none_or(|rooms| rooms.contains(room)) {
			return false;
		}

		if self.changed_since(room, user, since) != Some(false) {
			return false;
		}

		if let Some(registry) = self.metrics.prometheus() {
			registry.sync.journal_miss(kind);
		}

		true
	}

	/// Looks at the changes numbered in `(after, upto]`, in order, for one that
	/// concerns `user` on `device`, and returns the verdict.
	///
	/// A change of `user` wakes unless it names another device. A change of a
	/// room with no user wakes when `memo` says the user is in the room, is
	/// passed over when it says the user is not, and when it does not know, is
	/// passed over if `memo` is complete and otherwise left for the caller to
	/// find out, up to `limit` rooms. The first wake ends the scan. A `since`
	/// below the horizon is answered with the horizon, before any change is
	/// looked at.
	///
	/// A journal that is off records nothing, so a scan of it would find every
	/// window quiet; the caller does not scan a journal whose capacity is 0.
	///
	/// One pass under the shared lock, over the entries of the window only. The
	/// entries it looked at are counted, except when the verdict came before the
	/// window was looked at, for a `since` below the horizon or an empty window.
	#[must_use]
	pub fn scan(
		&self,
		user: &UserId,
		device: Option<&DeviceId>,
		after: u64,
		upto: u64,
		memo: &RoomMemo,
		limit: usize,
	) -> Scan {
		let state = self.read();

		if after < state.horizon {
			return Scan::Horizon(state.horizon);
		}

		if after >= upto {
			return Scan::Quiet;
		}

		let mut unknown: Vec<OwnedRoomId> = Vec::new();
		let mut seen: HashSet<&RoomId> = HashSet::new();
		let mut walked = 0_u64;

		let window = state
			.entries
			.range((Excluded(after), Included(upto)))
			.flat_map(|(_, changes)| changes);

		for change in window {
			walked = walked.saturating_add(1);

			if let Some(owner) = change.user.as_deref() {
				let other_device = change
					.device
					.as_deref()
					.is_some_and(|target| Some(target) != device);

				if owner == user && !other_device {
					self.walked(walked);

					return Scan::Wake(change.kind);
				}

				continue;
			}

			let Some(room) = change.room.as_deref() else {
				continue;
			};

			match memo.get(room) {
				| Some(true) => {
					self.walked(walked);

					return Scan::Wake(change.kind);
				},
				| Some(false) => {},
				| None if memo.complete => {},
				| None =>
					if seen.insert(room) {
						unknown.push(room.to_owned());

						if unknown.len() > limit {
							self.walked(walked);

							return Scan::Resolve { rooms: unknown, overflow: true };
						}
					},
			}
		}

		self.walked(walked);

		if unknown.is_empty() {
			Scan::Quiet
		} else {
			Scan::Resolve { rooms: unknown, overflow: false }
		}
	}

	fn walked(&self, entries: u64) {
		if let Some(registry) = self.metrics.prometheus() {
			registry.sync.wait_scanned(entries);
		}
	}

	/// Returns the sequence number from which the journal is complete.
	#[must_use]
	pub fn horizon(&self) -> u64 { self.read().horizon }

	/// Returns the number of changes held.
	#[must_use]
	pub fn len(&self) -> usize { self.read().len }

	/// Returns whether the journal holds no change.
	#[must_use]
	pub fn is_empty(&self) -> bool { self.len() == 0 }

	fn read(&self) -> RwLockReadGuard<'_, State> {
		self.state
			.read()
			.unwrap_or_else(PoisonError::into_inner)
	}

	fn write(&self) -> RwLockWriteGuard<'_, State> {
		self.state
			.write()
			.unwrap_or_else(PoisonError::into_inner)
	}
}

impl State {
	fn evict(&mut self, capacity: usize) -> usize {
		let mut evicted = 0_usize;

		while self.len > capacity {
			let Some((count, changes)) = self.entries.pop_first() else {
				debug_assert!(false, "journal length {} with no entries", self.len);

				self.len = 0;
				break;
			};

			self.len = self.len.saturating_sub(changes.len());
			self.horizon = self.horizon.max(count);
			evicted = evicted.saturating_add(changes.len());
		}

		evicted
	}
}

/// Reads the sequence number the server started from, which the global
/// counter stored before it dispatched any number.
///
/// A missing counter is a database that never dispatched one; any other
/// failure is an error, since a horizon set too low would claim a history the
/// journal never saw.
pub(super) fn counter_at_start(global: &Map) -> Result<u64> {
	global
		.get_blocking(Data::COUNTER)
		.optional()?
		.as_deref()
		.map_or(Ok(0), u64_from_bytes)
}

fn is_room_kind(kind: JournalKind) -> bool {
	match kind {
		| JournalKind::Timeline
		| JournalKind::Typing
		| JournalKind::PrivateRead
		| JournalKind::AccountData
		| JournalKind::Receipts
		| JournalKind::Notifications
		| JournalKind::DeviceLists => true,
		| JournalKind::Membership
		| JournalKind::Profile
		| JournalKind::GlobalAccountData
		| JournalKind::OwnKeys
		| JournalKind::OneTimeKeys
		| JournalKind::ToDevice => false,
	}
}

fn to_u64(len: usize) -> u64 { u64::try_from(len).unwrap_or(u64::MAX) }
