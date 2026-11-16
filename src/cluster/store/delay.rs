//! A delay on the disk syncs of the log store, for rehearsals of a slow disk.

use std::{
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	thread::sleep,
	time::Duration,
};

/// The longest delay a sync can be given.
pub const MAX_SYNC_DELAY: Duration = Duration::from_secs(10);

/// Handle to the delay every sync of the log store waits before it starts.
///
/// The delay is zero unless something sets it, and a zero delay costs one
/// atomic load per sync. The handle is cheap to clone; all clones steer the
/// same store.
#[derive(Clone, Debug, Default)]
pub struct SyncDelay {
	millis: Arc<AtomicU64>,
}

impl SyncDelay {
	/// Sets the delay, at most [`MAX_SYNC_DELAY`]; a longer one is clamped.
	pub fn set(&self, delay: Duration) {
		let delay = delay.min(MAX_SYNC_DELAY);
		let millis = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX);

		self.millis.store(millis, Ordering::Relaxed);
	}

	/// The delay now in force.
	#[must_use]
	pub fn get(&self) -> Duration { Duration::from_millis(self.millis.load(Ordering::Relaxed)) }

	pub(super) fn wait(&self) {
		let delay = self.get();
		if !delay.is_zero() {
			sleep(delay);
		}
	}
}
