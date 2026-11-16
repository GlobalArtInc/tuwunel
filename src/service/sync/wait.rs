use std::pin::pin;

use futures::StreamExt;
use ruma::{DeviceId, OwnedRoomId, RoomId, UserId};
use tokio::select;
use tuwunel_core::{
	implement,
	metrics::prom::{JournalKind, Lookup},
	utils::stream::ReadyExt,
};

use super::{RoomMemo, Scan};

/// Most rooms a scan of the journal hands back to be looked up one by one.
///
/// A window with more rooms the wait does not know sends it to load the list of
/// every room the user is in instead.
const LIMIT: usize = 32;

/// What ended a wait on the sync journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Woken {
	/// The journal holds a change of this kind that concerns the user.
	Journal(JournalKind),

	/// The journal dropped changes after the point the wait started from, so it
	/// can no longer say whether one concerned the user. The wait ended once the
	/// counter had retired every number up to the journal's horizon, so a sync
	/// that starts from the counter next finds no such gap.
	Horizon,

	/// The server is stopping.
	Shutdown,
}

/// Waits until the sync journal holds a change after `after` that concerns the
/// user on `device`.
///
/// `after` is the position the caller's last build ended at, so a change that
/// landed between that build and this call lies in the window the first scan
/// looks at. The wait sleeps until the retirement frontier of the global counter
/// moves, then scans the journal between the position it has reached and the
/// highest number the counter has retired, which is read after the frontier is
/// marked as seen. A change is in the journal before its number retires, and a
/// retirement after the read wakes the wait again, so no change is missed. A
/// change concerns the user when it is theirs, for their device or for all of
/// them, or when it is of a room they are joined to or invited to.
///
/// It returns [`Woken::Horizon`] when the journal no longer covers `after`, and
/// [`Woken::Shutdown`] when the server stops. A caller that wants a time limit
/// drops the future, which is safe at every await: it keeps nothing but its own
/// receiver and memo.
///
/// A journal that is off records nothing and would never wake the wait, so the
/// caller does not wait on a journal whose capacity is 0.
#[implement(super::Service)]
pub async fn wait_journal(
	&self,
	user: &UserId,
	device: Option<&DeviceId>,
	mut after: u64,
) -> Woken {
	let globals = &self.services.globals;
	let mut shutdown = pin!(self.services.server.until_shutdown());
	let mut frontier = globals.watch_retired();
	let mut memo = RoomMemo::default();

	loop {
		let retired = *frontier.borrow_and_update();
		let front = retired.max(globals.current_count());

		while front > after {
			match self
				.journal
				.scan(user, device, after, front, &memo, LIMIT)
			{
				| Scan::Wake(kind) => return Woken::Journal(kind),
				| Scan::Horizon(horizon) => {
					let reached = select! {
						reached = globals.wait_count(&horizon) => reached.is_ok(),
						() = &mut shutdown => false,
					};

					return if reached { Woken::Horizon } else { Woken::Shutdown };
				},
				| Scan::Quiet => after = front,
				| Scan::Resolve { rooms, overflow } =>
					self.resolve(user, &mut memo, rooms, overflow, true)
						.await,
			}
		}

		select! {
			changed = frontier.changed() => if changed.is_err() {
				return Woken::Shutdown;
			},
			() = &mut shutdown => return Woken::Shutdown,
		}
	}
}

/// Returns whether the journal holds a change after `after` that concerns the
/// user on `device`, up to the frontier the counter has retired now, or `None`
/// when the journal no longer covers `after`.
///
/// It reads what [`Self::wait_journal`] would find in one look and does not
/// wait. The journal must be on, as for a wait. It is the shadow comparison of a
/// wait and counts neither the entries it passes over nor the lookups it makes,
/// so those counters measure the waits alone.
#[implement(super::Service)]
pub async fn journal_relevant(
	&self,
	user: &UserId,
	device: Option<&DeviceId>,
	after: u64,
) -> Option<bool> {
	let upto = self.services.globals.current_count();

	self.journal_relevant_upto(user, device, after, upto)
		.await
}

/// Returns whether the journal holds a change in `(after, upto]` that concerns
/// the user on `device`, or `None` when the journal no longer covers `after`.
///
/// It is [`Self::journal_relevant`] with the upper end of the window given, for
/// a caller whose own read stopped at `upto`: a change numbered above it is one
/// the read could not have seen, so it does not count.
#[implement(super::Service)]
pub async fn journal_relevant_upto(
	&self,
	user: &UserId,
	device: Option<&DeviceId>,
	after: u64,
	upto: u64,
) -> Option<bool> {
	let mut memo = RoomMemo::default();

	loop {
		match self
			.journal
			.scan_silent(user, device, after, upto, &memo, LIMIT)
		{
			| Scan::Wake(_) => return Some(true),
			| Scan::Quiet => return Some(false),
			| Scan::Horizon(_) => return None,
			| Scan::Resolve { rooms, overflow } =>
				self.resolve(user, &mut memo, rooms, overflow, false)
					.await,
		}
	}
}

/// Finds out which of the rooms the user is in and notes it in `memo`, or when
/// `overflow` is set, loads every room the user is in and marks `memo` complete.
///
/// Each pass leaves the memo knowing at least one room it did not, or complete,
/// so the passes over one window are bounded by the rooms in it.
///
/// The lookups are counted when `counted` is set, which a wait sets and the
/// shadow comparison does not.
#[implement(super::Service)]
async fn resolve(
	&self,
	user: &UserId,
	memo: &mut RoomMemo,
	rooms: Vec<OwnedRoomId>,
	overflow: bool,
	counted: bool,
) {
	let registry = counted
		.then(|| self.services.server.metrics.prometheus())
		.flatten();

	if overflow {
		let state_cache = &self.services.state_cache;

		state_cache
			.rooms_joined(user)
			.chain(state_cache.rooms_invited(user))
			.ready_for_each(|room| memo.insert(room.to_owned(), true))
			.await;

		memo.mark_complete();

		if let Some(registry) = registry {
			registry.sync.wait_lookup(Lookup::Set);
		}

		return;
	}

	for room in rooms {
		let present = self.is_present(user, &room).await;

		memo.insert(room, present);

		if let Some(registry) = registry {
			registry.sync.wait_lookup(Lookup::Room);
		}
	}
}

#[implement(super::Service)]
async fn is_present(&self, user: &UserId, room: &RoomId) -> bool {
	let state_cache = &self.services.state_cache;

	state_cache.is_joined(user, room).await || state_cache.is_invited(user, room).await
}
