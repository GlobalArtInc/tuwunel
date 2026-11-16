//! Keeps a repeating event from filling the log.

use std::time::{Duration, Instant};

/// Lets one event through per interval and counts the ones it held back.
pub(super) struct Limiter {
	every: Duration,
	last: Option<Instant>,
	held_back: u64,
}

impl Limiter {
	pub(super) const fn new(every: Duration) -> Self { Self { every, last: None, held_back: 0 } }

	/// Asks to let an event through at `now`. It answers with the number of
	/// events held back since the last one it let through, or none when this
	/// one is held back too.
	pub(super) fn take(&mut self, now: Instant) -> Option<u64> {
		let due = self
			.last
			.is_none_or(|last| now.saturating_duration_since(last) >= self.every);

		if !due {
			self.held_back = self.held_back.saturating_add(1);

			return None;
		}

		self.last = Some(now);

		Some(std::mem::take(&mut self.held_back))
	}
}
