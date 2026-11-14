//! Labelled families: closed sets indexed by enum, open sets with a ceiling.

use std::{
	collections::HashMap,
	marker::PhantomData,
	sync::{Arc, PoisonError, RwLock, RwLockReadGuard},
};

use smallstr::SmallString;

use super::{
	types::{Counter, Metric},
	write::{Kind, Writer, push_label},
};

/// Label value of the series that absorbs keys beyond the ceiling.
pub const OTHER: &str = "_other";

/// Longest key an open family accepts; longer ones go to `_other`.
pub const MAX_KEY_LEN: usize = 256;

const SEPARATOR: char = '\u{1f}';

/// A closed set of label values that maps to a dense index.
///
/// Implemented by [`label_set!`](crate::label_set) enums and by pairs of
/// label sets. `index` is below `COUNT`, so the number of series is known
/// statically.
pub trait LabelSet: Copy {
	/// Number of distinct values.
	const COUNT: usize;

	/// Position of this value, below `COUNT`.
	fn index(self) -> usize;

	/// Appends the labels of the value at `index` to a label body.
	fn write_labels(index: usize, out: &mut String);
}

/// Defines an enum implementing [`LabelSet`] for a single label name.
///
/// ```ignore
/// label_set! {
///     pub enum Since("since") { Initial => "initial", Incremental => "incremental" }
/// }
/// ```
#[macro_export]
macro_rules! label_set {
	(
		$(#[$meta:meta])*
		$vis:vis enum $name:ident($label:literal) {
			$($(#[$vmeta:meta])* $variant:ident => $value:literal),+ $(,)?
		}
	) => {
		$(#[$meta])*
		#[derive(Clone, Copy, Debug, Eq, PartialEq)]
		$vis enum $name {
			$($(#[$vmeta])* $variant),+
		}

		impl $crate::metrics::prom::LabelSet for $name {
			const COUNT: usize = [$($value),+].len();

			#[expect(clippy::as_conversions)]
			fn index(self) -> usize { self as usize }

			fn write_labels(index: usize, out: &mut ::std::string::String) {
				const VALUES: &[&str] = &[$($value),+];

				let value = VALUES.get(index).copied().unwrap_or($crate::metrics::prom::OTHER);
				$crate::metrics::prom::push_label(out, $label, value);
			}
		}
	};
}

impl<A, B> LabelSet for (A, B)
where
	A: LabelSet,
	B: LabelSet,
{
	const COUNT: usize = A::COUNT.saturating_mul(B::COUNT);

	fn index(self) -> usize {
		self.0
			.index()
			.saturating_mul(B::COUNT)
			.saturating_add(self.1.index())
	}

	fn write_labels(index: usize, out: &mut String) {
		A::write_labels(index.checked_div(B::COUNT).unwrap_or(0), out);
		B::write_labels(index.checked_rem(B::COUNT).unwrap_or(0), out);
	}
}

/// One metric per value of a closed label set, in a flat array.
///
/// Lookup is an index with no hashing. Every series is written, including
/// those that were never touched.
#[derive(Debug)]
pub struct EnumFamily<K, M> {
	slots: Box<[M]>,
	_labels: PhantomData<fn(K)>,
}

impl<K, M> EnumFamily<K, M>
where
	K: LabelSet,
{
	/// Creates the family, building each metric with `make`.
	pub fn new<F>(make: F) -> Self
	where
		F: FnMut() -> M,
	{
		Self {
			slots: std::iter::repeat_with(make)
				.take(K::COUNT.max(1))
				.collect(),
			_labels: PhantomData,
		}
	}

	#[inline]
	/// Returns the metric for a label value.
	pub fn get(&self, key: K) -> &M {
		let last = self.slots.len().saturating_sub(1);

		&self.slots[key.index().min(last)]
	}
}

impl<K, M> Metric for EnumFamily<K, M>
where
	K: LabelSet,
	M: Metric,
{
	const KIND: Kind = M::KIND;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		let mut scratch = String::new();
		for (index, slot) in self.slots.iter().enumerate() {
			scratch.clear();
			scratch.push_str(labels);
			K::write_labels(index, &mut scratch);
			slot.write(w, name, &scratch);
		}
	}

	fn series(&self) -> usize {
		self.slots
			.iter()
			.fold(0, |total, slot| total.saturating_add(slot.series()))
	}
}

/// One metric per key of an open label set, with a ceiling on the number of
/// keys.
///
/// The key is made of one value per label name. Up to `cap` distinct keys get
/// their own series. Any further key, and any key that is too long, has the
/// wrong number of parts, contains the internal separator or is itself `_other`,
/// is counted in a shared `_other` series and bumps the overflow counter reported as
/// `tuwunel_metrics_series_overflow_total{family}`. Lookups of known keys take
/// a read lock; the write lock is held only to insert a new key.
pub struct DynFamily<M> {
	labels: &'static [&'static str],
	cap: usize,
	make: fn() -> M,
	map: RwLock<HashMap<Box<str>, Arc<M>>>,
	other: Arc<M>,
	overflow: Counter,
}

impl<M> DynFamily<M> {
	#[must_use]
	/// Creates a family keyed by one value per name in `labels`, holding at
	/// most `cap` keys besides `_other`.
	pub fn new(labels: &'static [&'static str], cap: usize, make: fn() -> M) -> Self {
		Self {
			labels,
			cap,
			make,
			map: RwLock::new(HashMap::new()),
			other: Arc::new(make()),
			overflow: Counter::new(),
		}
	}

	#[must_use]
	/// Returns the metric for a key, creating it when there is room.
	///
	/// `parts` holds one value per label name, in order.
	pub fn get(&self, parts: &[&str]) -> Arc<M> {
		let mut joined = SmallString::<[u8; 160]>::new();
		let Some(key) = self.key(parts, &mut joined) else {
			return self.divert();
		};

		{
			let map = self.read();
			if let Some(metric) = map.get(key) {
				return Arc::clone(metric);
			}

			if map.len() >= self.cap {
				drop(map);
				return self.divert();
			}
		}

		let mut map = self
			.map
			.write()
			.unwrap_or_else(PoisonError::into_inner);

		if let Some(metric) = map.get(key) {
			return Arc::clone(metric);
		}

		if map.len() >= self.cap {
			drop(map);
			return self.divert();
		}

		let metric = Arc::new((self.make)());
		map.insert(key.into(), Arc::clone(&metric));

		metric
	}

	#[must_use]
	/// Returns the number of keys with their own series.
	pub fn len(&self) -> usize { self.read().len() }

	#[must_use]
	/// Returns whether no key has its own series yet.
	pub fn is_empty(&self) -> bool { self.read().is_empty() }

	#[must_use]
	/// Returns the number of lookups diverted to `_other`.
	pub fn overflowed(&self) -> u64 { self.overflow.get() }

	#[must_use]
	/// Returns the ceiling on keys with their own series.
	pub fn cap(&self) -> usize { self.cap }

	fn key<'a>(
		&self,
		parts: &[&'a str],
		joined: &'a mut SmallString<[u8; 160]>,
	) -> Option<&'a str> {
		if parts.len() != self.labels.len()
			|| parts
				.iter()
				.any(|part| part.contains(SEPARATOR) || *part == OTHER)
		{
			return None;
		}

		let length = parts.iter().map(|part| part.len()).fold(
			parts
				.len()
				.saturating_sub(1)
				.saturating_mul(SEPARATOR.len_utf8()),
			usize::saturating_add,
		);

		if length > MAX_KEY_LEN {
			return None;
		}

		let key = match parts {
			| [one] => *one,
			| _ => {
				for (index, part) in parts.iter().enumerate() {
					if index > 0 {
						joined.push(SEPARATOR);
					}

					joined.push_str(part);
				}

				joined.as_str()
			},
		};

		Some(key)
	}

	fn divert(&self) -> Arc<M> {
		self.overflow.inc();

		Arc::clone(&self.other)
	}

	fn read(&self) -> RwLockReadGuard<'_, HashMap<Box<str>, Arc<M>>> {
		self.map
			.read()
			.unwrap_or_else(PoisonError::into_inner)
	}

	fn entries(&self) -> Vec<(Box<str>, Arc<M>)> {
		let mut entries: Vec<_> = self
			.read()
			.iter()
			.map(|(key, metric)| (key.clone(), Arc::clone(metric)))
			.collect();

		entries.sort_by(|(a, _), (b, _)| a.cmp(b));

		entries
	}
}

impl<M> Metric for DynFamily<M>
where
	M: Metric,
{
	const KIND: Kind = M::KIND;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) {
		let mut scratch = String::new();
		for (key, metric) in self.entries() {
			scratch.clear();
			scratch.push_str(labels);
			for (label, value) in self.labels.iter().zip(key.split(SEPARATOR)) {
				push_label(&mut scratch, label, value);
			}

			metric.write(w, name, &scratch);
		}

		let overflowed = self.overflowed();
		if overflowed > 0 {
			scratch.clear();
			scratch.push_str(labels);
			for label in self.labels {
				push_label(&mut scratch, label, OTHER);
			}

			self.other.write(w, name, &scratch);
		}

		w.overflow(name, overflowed);
	}

	fn series(&self) -> usize {
		self.cap
			.saturating_add(1)
			.saturating_mul(self.other.series())
			.saturating_add(1)
	}
}
