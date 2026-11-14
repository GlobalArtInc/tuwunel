//! Metrics about the build and about the registry itself.

use super::write::{Kind, Value, Writer, push_label};
use crate::info::version::version;

/// Build information. The overflow counters of open families are written by
/// the [`Writer`] when it finishes.
#[derive(Clone, Copy, Debug, Default)]
pub struct Meta;

impl Meta {
	pub(super) fn write(w: &mut Writer) {
		let mut labels = String::new();
		push_label(&mut labels, "version", version());

		w.family(
			"tuwunel_build_info",
			"Constant 1, labelled with the version of the running build.",
			Kind::Gauge,
		);
		w.sample("tuwunel_build_info", "", &labels, None, Value::Unsigned(1));
	}

	pub(super) fn series() -> usize { 1 }
}
