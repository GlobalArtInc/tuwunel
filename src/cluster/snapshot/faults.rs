//! Hooks tests use to break a node in a chosen place. A build that is not a
//! test has none of them, and the calls to them compile to nothing.

#[cfg(test)]
use std::sync::{Arc, MutexGuard};

use super::{SnapshotStore, replace::Step};

#[cfg(test)]
pub(crate) type StepHook = Arc<dyn Fn(Step) + Send + Sync>;

#[cfg(test)]
pub(crate) type LookupHook = Arc<dyn Fn() + Send + Sync>;

#[cfg(test)]
pub(crate) type BoundsHook = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// What a test has switched on.
#[cfg(test)]
#[derive(Default)]
pub(super) struct Faults {
	step: Option<StepHook>,
	lookup: Option<LookupHook>,
	bounds: Option<BoundsHook>,
	no_link: bool,
	free_space: Option<u64>,
}

#[cfg(test)]
impl SnapshotStore {
	/// Runs `hook` at every step of a replacement.
	pub(crate) fn set_hook(&self, hook: Option<StepHook>) { self.faults().step = hook; }

	/// Runs `hook` in the lookup of the current snapshot, once the metadata of
	/// the candidate is read and before its files are listed.
	pub(crate) fn set_lookup_hook(&self, hook: Option<LookupHook>) {
		self.faults().lookup = hook;
	}

	/// Runs `hook` once a checkpoint is taken, with the database sequence read
	/// before it and the one read after it.
	pub(crate) fn set_bounds_hook(&self, hook: Option<BoundsHook>) {
		self.faults().bounds = hook;
	}

	/// Makes the check for hard links fail the way a second filesystem does.
	pub(crate) fn refuse_links(&self, refuse: bool) { self.faults().no_link = refuse; }

	/// Replaces the free space of the database directory.
	pub(crate) fn set_free_space(&self, free: Option<u64>) { self.faults().free_space = free; }

	pub(super) fn fire(&self, step: Step) {
		let hook = self.faults().step.clone();
		if let Some(hook) = hook {
			hook(step);
		}
	}

	pub(super) fn glimpse(&self) {
		let hook = self.faults().lookup.clone();
		if let Some(hook) = hook {
			hook();
		}
	}

	pub(super) fn checkpointed(&self, before: u64, after: u64) {
		let hook = self.faults().bounds.clone();
		if let Some(hook) = hook {
			hook(before, after);
		}
	}

	pub(super) fn links_refused(&self) -> bool { self.faults().no_link }

	pub(super) fn free_space_replaced(&self) -> Option<u64> { self.faults().free_space }

	fn faults(&self) -> MutexGuard<'_, Faults> {
		self.inner.faults.lock().expect("faults locked")
	}
}

#[cfg(not(test))]
#[expect(clippy::unused_self)]
impl SnapshotStore {
	#[inline(always)]
	pub(super) fn fire(&self, _step: Step) {}

	#[inline(always)]
	pub(super) fn glimpse(&self) {}

	#[inline(always)]
	pub(super) fn checkpointed(&self, _before: u64, _after: u64) {}

	#[inline(always)]
	pub(super) fn links_refused(&self) -> bool { false }

	#[inline(always)]
	pub(super) fn free_space_replaced(&self) -> Option<u64> { None }
}
