//! Test hooks on the disk syncs of the log store.

use std::{
	io,
	sync::{
		Condvar, Mutex, MutexGuard, PoisonError,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

const HELD_AT_MOST: Duration = Duration::from_secs(20);

#[derive(Default)]
struct Gate {
	permits: Option<usize>,
	failing: bool,
	waiting: usize,
}

/// Counts the syncs of the log store, and holds back or fails the syncs that
/// make appended entries durable.
#[derive(Default)]
pub(crate) struct Probe {
	gate: Mutex<Gate>,
	open: Condvar,
	syncs: AtomicU64,
}

impl Probe {
	/// Holds back every sync of appended entries until it is released.
	pub(crate) fn hold(&self) { self.gate().permits = Some(0); }

	/// Lets the given number of held syncs of appended entries through.
	pub(crate) fn release(&self, syncs: usize) {
		let mut gate = self.gate();
		gate.permits = gate
			.permits
			.map(|permits| permits.saturating_add(syncs));

		self.open.notify_all();
	}

	/// Lets every sync of appended entries through.
	pub(crate) fn release_all(&self) {
		self.gate().permits = None;
		self.open.notify_all();
	}

	/// Makes the syncs of appended entries fail, or succeed again.
	pub(crate) fn fail(&self, failing: bool) { self.gate().failing = failing; }

	/// The number of syncs of appended entries waiting to be let through.
	pub(crate) fn waiting(&self) -> usize { self.gate().waiting }

	/// The number of syncs of the write-ahead log so far.
	pub(crate) fn syncs(&self) -> u64 { self.syncs.load(Ordering::SeqCst) }

	pub(super) fn count(&self) { self.syncs.fetch_add(1, Ordering::SeqCst); }

	pub(super) fn pass(&self) -> io::Result<()> {
		let deadline = Instant::now()
			.checked_add(HELD_AT_MOST)
			.unwrap_or_else(Instant::now);

		let mut gate = self.gate();
		gate.waiting = gate.waiting.saturating_add(1);
		while gate.permits == Some(0) {
			let Some(left) = deadline.checked_duration_since(Instant::now()) else {
				gate.waiting = gate.waiting.saturating_sub(1);

				return Err(io::Error::other("a held sync was never released"));
			};

			gate = self
				.open
				.wait_timeout(gate, left)
				.unwrap_or_else(PoisonError::into_inner)
				.0;
		}

		gate.waiting = gate.waiting.saturating_sub(1);
		gate.permits = gate
			.permits
			.map(|permits| permits.saturating_sub(1));

		if gate.failing {
			return Err(io::Error::other("injected sync failure"));
		}

		Ok(())
	}

	fn gate(&self) -> MutexGuard<'_, Gate> {
		self.gate
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
	}
}
