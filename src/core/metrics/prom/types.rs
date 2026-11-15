use std::{
	sync::atomic::{
		AtomicBool, AtomicI64, AtomicU64,
		Ordering::{Acquire, Relaxed, Release},
	},
	time::{Duration, Instant},
};

use super::write::{Kind, Le, Scale, Value, Writer};

/// A kind of metric that knows how to write its own series.
pub trait Metric {
	/// Type announced for the family this metric belongs to.
	const KIND: Kind;

	/// Writes every series of this metric under `name`, with `labels` as the
	/// label body shared by all of them.
	fn write(&self, w: &mut Writer, name: &str, labels: &str);

	/// Upper bound on the number of series [`Self::write`] can produce.
	fn series(&self) -> usize;
}

/// Monotonic counter.
#[derive(Debug)]
pub struct Counter {
	value: AtomicU64,
	scale: Scale,
}

/// Value that moves in both directions.
#[derive(Debug, Default)]
pub struct Gauge {
	value: AtomicI64,
}

/// Wraps a metric that is exposed only after it was first written.
///
/// Used for readings that a collector fills in and that may be unavailable on
/// a platform or before the first scan, where a zero would be misleading. The
/// mark is released after the value is written and acquired before it is read,
/// whatever ordering the metric itself uses.
#[derive(Debug, Default)]
pub struct Seen<M> {
	seen: AtomicBool,
	inner: M,
}

/// Distribution over fixed buckets.
///
/// Bounds and the sum are integers in the unit the caller observes in, for
/// example nanoseconds or bytes; `scale` converts them for exposition. Bucket
/// counts are stored per bucket and accumulated when written, so `+Inf` and
/// `_count` always agree.
#[derive(Debug)]
pub struct Histogram<const N: usize> {
	bounds: &'static [u64; N],
	scale: Scale,
	counts: [AtomicU64; N],
	inf: AtomicU64,
	sum: AtomicU64,
}

/// Records the time since its creation into a histogram when dropped.
#[derive(Debug)]
#[must_use = "the duration is recorded when the timer is dropped"]
pub struct Timer<'a, const N: usize> {
	histogram: &'a Histogram<N>,
	started: Instant,
}

impl Counter {
	#[must_use]
	/// Creates a counter exposed as stored.
	pub const fn new() -> Self { Self::scaled(Scale::One) }

	#[must_use]
	/// Creates a counter whose stored integer is divided by `scale` when
	/// exposed.
	pub const fn scaled(scale: Scale) -> Self { Self { value: AtomicU64::new(0), scale } }

	#[inline]
	/// Adds one.
	pub fn inc(&self) { self.add(1); }

	#[inline]
	/// Adds `count`. The stored integer wraps around on overflow; a counter
	/// that gets near `u64::MAX` is not guarded.
	pub fn add(&self, count: u64) { self.value.fetch_add(count, Relaxed); }

	#[inline]
	/// Raises the counter to `total`, for a source that reports a running
	/// total rather than increments. A smaller `total` is ignored, so the
	/// counter never moves backwards.
	pub fn raise_to(&self, total: u64) { self.value.fetch_max(total, Relaxed); }

	#[inline]
	#[must_use]
	/// Returns the stored integer.
	pub fn get(&self) -> u64 { self.value.load(Relaxed) }
}

impl<M> Seen<M> {
	#[must_use]
	/// Wraps `inner`, which stays unexposed until [`Self::publish`].
	pub const fn new(inner: M) -> Self { Self { seen: AtomicBool::new(false), inner } }

	#[inline]
	/// Applies a write to the metric and then marks it as written, so a
	/// reader that sees the mark also sees the value and never a zero between
	/// the two.
	pub fn publish<R, W>(&self, write: W) -> R
	where
		W: FnOnce(&M) -> R,
	{
		let written = write(&self.inner);
		self.seen.store(true, Release);

		written
	}

	#[inline]
	#[must_use]
	/// Returns whether the metric was ever written.
	pub fn is_seen(&self) -> bool { self.seen.load(Acquire) }
}

impl<M: Metric> Metric for Seen<M> {
	const KIND: Kind = M::KIND;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		if self.is_seen() {
			self.inner.write(w, name, labels);
		}
	}

	fn series(&self) -> usize { self.inner.series() }
}

impl Default for Counter {
	fn default() -> Self { Self::new() }
}

impl Metric for Counter {
	const KIND: Kind = Kind::Counter;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		w.sample(name, "", labels, None, Value::Scaled(self.get(), self.scale));
	}

	fn series(&self) -> usize { 1 }
}

impl Gauge {
	#[must_use]
	/// Creates a gauge at zero.
	pub const fn new() -> Self { Self { value: AtomicI64::new(0) } }

	#[inline]
	/// Replaces the value.
	pub fn set(&self, value: i64) { self.value.store(value, Relaxed); }

	#[inline]
	/// Replaces the value with an unsigned one, saturating at `i64::MAX`.
	pub fn set_unsigned(&self, value: u64) { self.set(i64::try_from(value).unwrap_or(i64::MAX)); }

	#[inline]
	/// Adds one.
	pub fn inc(&self) { self.value.fetch_add(1, Relaxed); }

	#[inline]
	/// Subtracts one.
	pub fn dec(&self) { self.value.fetch_sub(1, Relaxed); }

	#[inline]
	#[must_use]
	/// Returns the value.
	pub fn get(&self) -> i64 { self.value.load(Relaxed) }
}

impl Metric for Gauge {
	const KIND: Kind = Kind::Gauge;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		w.sample(name, "", labels, None, Value::Signed(self.get()));
	}

	fn series(&self) -> usize { 1 }
}

impl<const N: usize> Histogram<N> {
	#[must_use]
	/// Creates a histogram over strictly ascending `bounds`, exposed as stored.
	pub fn new(bounds: &'static [u64; N]) -> Self { Self::scaled(bounds, Scale::One) }

	#[must_use]
	/// Creates a histogram that observes nanoseconds and exposes seconds.
	pub fn seconds(bounds: &'static [u64; N]) -> Self { Self::scaled(bounds, Scale::Nano) }

	#[must_use]
	/// Creates a histogram over strictly ascending `bounds` whose integers are
	/// divided by `scale` when exposed.
	pub fn scaled(bounds: &'static [u64; N], scale: Scale) -> Self {
		debug_assert!(
			bounds.windows(2).all(|pair| pair[0] < pair[1]),
			"histogram bounds must be strictly ascending"
		);

		Self {
			bounds,
			scale,
			counts: [const { AtomicU64::new(0) }; N],
			inf: AtomicU64::new(0),
			sum: AtomicU64::new(0),
		}
	}

	#[inline]
	/// Records one observation in the unit of the bounds. A value above the
	/// last bound lands in `+Inf`, and the sum wraps around on overflow, so a
	/// value near `u64::MAX` corrupts the sum.
	pub fn observe(&self, value: u64) {
		let index = self
			.bounds
			.partition_point(|bound| *bound < value);
		let slot = self.counts.get(index).unwrap_or(&self.inf);

		slot.fetch_add(1, Relaxed);
		self.sum.fetch_add(value, Relaxed);
	}

	#[inline]
	/// Records a duration in nanoseconds, saturating at `u64::MAX`. The
	/// histogram must have been created with [`Self::seconds`]; the unit is not
	/// checked, so any other histogram records nanoseconds against its own
	/// bounds.
	pub fn observe_duration(&self, duration: Duration) {
		self.observe(u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX));
	}

	#[inline]
	/// Starts a timer that records into this histogram when dropped.
	pub fn start(&self) -> Timer<'_, N> { Timer { histogram: self, started: Instant::now() } }

	#[must_use]
	/// Returns the number of observations.
	pub fn count(&self) -> u64 {
		self.counts
			.iter()
			.chain(std::iter::once(&self.inf))
			.fold(0, |total, count| total.saturating_add(count.load(Relaxed)))
	}
}

impl<const N: usize> Metric for Histogram<N> {
	const KIND: Kind = Kind::Histogram;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		let mut cumulative: u64 = 0;
		for (bound, count) in self.bounds.iter().zip(&self.counts) {
			cumulative = cumulative.saturating_add(count.load(Relaxed));
			w.sample(
				name,
				"_bucket",
				labels,
				Some(Le::Bound(*bound, self.scale)),
				Value::Unsigned(cumulative),
			);
		}

		let total = cumulative.saturating_add(self.inf.load(Relaxed));
		w.sample(name, "_bucket", labels, Some(Le::Inf), Value::Unsigned(total));
		w.sample(name, "_sum", labels, None, Value::Scaled(self.sum.load(Relaxed), self.scale));
		w.sample(name, "_count", labels, None, Value::Unsigned(total));
	}

	fn series(&self) -> usize { N.saturating_add(3) }
}

impl<const N: usize> Drop for Timer<'_, N> {
	fn drop(&mut self) {
		self.histogram
			.observe_duration(self.started.elapsed());
	}
}

#[cfg(test)]
mod tests;
