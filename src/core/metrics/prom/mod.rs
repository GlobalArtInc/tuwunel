//! Prometheus registry on relaxed atomics.
//!
//! The registry is a static structure of named groups, one file per group.
//! Metrics are counters, gauges and histograms with fixed buckets; labels come
//! from closed enums or from open sets with a ceiling on the number of series.
//! Reading takes no locks beyond short read locks on open families, and the
//! text format is Prometheus 0.0.4.

mod family;
mod meta;
#[cfg(test)]
mod tests;
mod types;
mod write;

pub use self::{
	family::{DynFamily, EnumFamily, LabelSet, MAX_KEY_LEN, OTHER},
	meta::Meta,
	types::{Counter, Gauge, Histogram, Metric, Timer},
	write::{Kind, Le, Scale, Value, Writer, push_label},
};

/// Content type of the text exposition.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// All metric groups of the server.
///
/// Names, buckets and rendering of every metric are defined under this module
/// and nowhere else; the rest of the code only calls methods of the groups.
#[derive(Debug, Default)]
#[expect(missing_copy_implementations)]
pub struct Registry {
	/// Build information and registry bookkeeping.
	pub meta: Meta,
}

impl Registry {
	#[must_use]
	/// Creates a registry with every metric at zero.
	pub fn new() -> Self { Self::default() }

	#[must_use]
	/// Renders every metric in the Prometheus text format.
	pub fn render(&self) -> String {
		let mut w = Writer::new();
		Meta::write(&mut w);

		w.finish()
	}

	#[must_use]
	/// Upper bound on the number of series [`Self::render`] can produce.
	///
	/// Closed families count every value of their label set, open families
	/// count their ceiling plus `_other`, and a histogram counts its buckets,
	/// `+Inf`, sum and count.
	pub fn max_series(&self) -> usize { Meta::series() }
}
