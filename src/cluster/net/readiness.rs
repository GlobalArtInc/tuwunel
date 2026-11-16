//! What a follower has heard from its leader, and whether that makes it ready.
//!
//! Kept apart from the Raft core and the clock so the decision can be checked
//! against a table: every function takes the time it is asked at.

use std::time::{Duration, Instant};

/// How far behind its leader a follower may be, in log entries, and still be
/// ready.
pub(super) const READY_LAG_ENTRIES: u64 = 1000;

/// How long a follower may have been behind its leader, and how long it may
/// have heard nothing from it, and still be ready.
pub(super) const READY_LAG_AGE: Duration = Duration::from_secs(5);

/// What a follower knows of its leader.
///
/// The leader's commit index comes from the AppendEntries requests, not from
/// the Raft core: a follower caps the commit index it reports at the end of its
/// own log, so from the core alone a follower that is far behind looks as
/// current as its log.
pub(super) struct Contact {
	commit: Option<u64>,
	heard: Option<Instant>,
	at_par: Instant,
}

/// How far a follower is from its leader at one moment.
pub(super) struct Lag {
	/// Committed log entries not yet applied.
	pub(super) entries: u64,

	/// How long since the follower had applied everything committed.
	pub(super) behind_for: Duration,

	/// How long since the last request from the leader, or none if there has
	/// been none.
	pub(super) silent_for: Option<Duration>,
}

impl Contact {
	pub(super) const fn new(now: Instant) -> Self {
		Self { commit: None, heard: None, at_par: now }
	}

	/// Records a request from the leader that carried commit index `commit`.
	pub(super) fn heard(&mut self, commit: Option<u64>, now: Instant) {
		self.heard = Some(now);
		self.commit = self.commit.max(commit);
	}

	/// Measures the lag. `known` is the commit index the Raft core reports and
	/// `applied` the index of the last entry applied; the larger commit index
	/// of this and the requests is the one that counts.
	pub(super) fn measure(
		&mut self,
		known: Option<u64>,
		applied: Option<u64>,
		now: Instant,
	) -> Lag {
		let entries = entries_behind(self.commit.max(known), applied);
		if entries == 0 {
			self.at_par = now;
		}

		Lag {
			entries,
			behind_for: now.saturating_duration_since(self.at_par),
			silent_for: self
				.heard
				.map(|heard| now.saturating_duration_since(heard)),
		}
	}
}

impl Lag {
	/// Decides whether a follower with this lag is ready: it has heard from
	/// its leader within [`READY_LAG_AGE`], is at most [`READY_LAG_ENTRIES`]
	/// entries behind, and has not been behind for longer than
	/// [`READY_LAG_AGE`]. The error says which does not hold.
	pub(super) fn verdict(&self) -> Result<(), String> {
		match self.silent_for {
			| None => return Err("the leader has not been heard from yet".to_owned()),
			| Some(silence) if silence > READY_LAG_AGE =>
				return Err(format!("the leader has been silent for {}s", silence.as_secs())),
			| Some(_) => {},
		}

		if self.entries > READY_LAG_ENTRIES {
			return Err(format!("{} log entries behind the leader", self.entries));
		}

		if self.entries > 0 && self.behind_for > READY_LAG_AGE {
			return Err(format!("behind the leader for {}s", self.behind_for.as_secs()));
		}

		Ok(())
	}
}

/// The number of entries committed and not yet applied.
pub(super) fn entries_behind(commit: Option<u64>, applied: Option<u64>) -> u64 {
	match (commit, applied) {
		| (None, _) => 0,
		| (Some(commit), None) => commit.saturating_add(1),
		| (Some(commit), Some(applied)) => commit.saturating_sub(applied),
	}
}
