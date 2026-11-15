use std::{
	collections::{BTreeMap, HashSet},
	ops::Bound::{Excluded, Unbounded},
	sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use ruma::{OwnedRoomId, OwnedUserId, RoomId, UserId};
use tuwunel_core::{
	Result,
	metrics::{Metrics, prom::JournalKind},
	result::NotFound,
	smallvec::SmallVec,
	utils::u64_from_bytes,
};
use tuwunel_database::Map;

use crate::globals::Data;

/// A bounded in-memory log of the rooms that changed, keyed by the global
/// sequence number of the write.
///
/// Every writer of a stream the sync fast path probe reads records its number
/// here under the permit that holds it and before the write reaches the
/// database, so a row a reader finds has an entry here. The log is complete
/// for numbers above its horizon: it starts at the counter the server started
/// from and rises when the oldest entries are dropped to stay within capacity.
/// The lock is held only for the in-memory work of one call, never across an
/// await and never while calling another service.
pub struct Journal {
	metrics: Arc<Metrics>,
	capacity: usize,
	state: Mutex<State>,
}

#[derive(Default)]
struct State {
	entries: BTreeMap<u64, Changes>,
	len: usize,
	horizon: u64,
}

type Changes = SmallVec<[Change; 1]>;

struct Change {
	room: OwnedRoomId,
	user: Option<OwnedUserId>,
}

impl Journal {
	/// Creates a journal holding at most `capacity` changes, complete for
	/// sequence numbers above `horizon`. A capacity of 0 turns it off.
	pub(super) fn new(capacity: usize, horizon: u64, metrics: Arc<Metrics>) -> Self {
		Self {
			metrics,
			capacity,
			state: Mutex::new(State { horizon, ..State::default() }),
		}
	}

	/// Notes that the write numbered `count` changed `room`, for every user
	/// when `user` is `None` and for that user alone otherwise.
	pub fn record(&self, count: u64, room: &RoomId, user: Option<&UserId>) {
		if self.capacity == 0 {
			return;
		}

		let change = Change {
			room: room.to_owned(),
			user: user.map(ToOwned::to_owned),
		};

		let mut state = self.lock();

		state
			.entries
			.entry(count)
			.or_default()
			.push(change);
		state.len = state.len.saturating_add(1);

		let evicted = state.evict(self.capacity);

		if let Some(registry) = self.metrics.prometheus() {
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

		let state = self.lock();

		if since < state.horizon {
			return None;
		}

		let rooms = state
			.entries
			.range((Excluded(since), Unbounded))
			.flat_map(|(_, changes)| changes)
			.filter(|change| {
				change
					.user
					.as_deref()
					.is_none_or(|owner| owner == user)
			})
			.map(|change| change.room.clone())
			.collect();

		Some(rooms)
	}

	/// Counts a miss of `kind` when `changed`, the answer of
	/// [`Self::changed_rooms`], holds no change for a room that the fast path
	/// found changed, and returns whether it did.
	///
	/// A journal that cannot answer is no miss.
	pub fn audit(
		&self,
		kind: JournalKind,
		room: &RoomId,
		changed: Option<&HashSet<OwnedRoomId>>,
	) -> bool {
		if changed.is_none_or(|rooms| rooms.contains(room)) {
			return false;
		}

		if let Some(registry) = self.metrics.prometheus() {
			registry.sync.journal_miss(kind);
		}

		true
	}

	/// Returns the sequence number from which the journal is complete.
	#[must_use]
	pub fn horizon(&self) -> u64 { self.lock().horizon }

	/// Returns the number of changes held.
	#[must_use]
	pub fn len(&self) -> usize { self.lock().len }

	/// Returns whether the journal holds no change.
	#[must_use]
	pub fn is_empty(&self) -> bool { self.len() == 0 }

	fn lock(&self) -> MutexGuard<'_, State> {
		self.state
			.lock()
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

fn to_u64(len: usize) -> u64 { u64::try_from(len).unwrap_or(u64::MAX) }
