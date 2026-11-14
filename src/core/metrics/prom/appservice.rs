//! Metrics of delivery to appservices.
//!
//! Series are labelled by the id of the appservice registration. Ids are chosen
//! by the operator, but the number of series is capped regardless, and
//! appservices beyond the cap share the `_other` series.

use std::time::Duration;

use super::{
	family::{DynFamily, EnumFamily},
	types::{Counter, Gauge, Histogram, Metric},
	write::Writer,
};
use crate::label_set;

/// Ceiling on the appservices that get a series of their own.
pub const APPSERVICE_CAP: usize = 64;

static SECONDS: [u64; 11] = [
	10_000_000,
	25_000_000,
	50_000_000,
	100_000_000,
	250_000_000,
	500_000_000,
	1_000_000_000,
	2_500_000_000,
	5_000_000_000,
	10_000_000_000,
	30_000_000_000,
];

label_set! {
	/// How a request to an appservice ended.
	pub enum SendResult("result") {
		/// The appservice accepted the request.
		Ok => "ok",
		/// The request timed out.
		Timeout => "timeout",
		/// The connection or the transfer failed.
		Network => "network",
		/// The appservice answered, but not with a usable success: a status
		/// outside 2xx, or a body that could not be read or understood.
		BadResponse => "bad_response",
		/// Any other failure.
		Other => "other",
	}
}

/// Metrics of delivery to appservices.
pub struct Appservice {
	transactions: DynFamily<EnumFamily<SendResult, Counter>>,
	duration: DynFamily<Histogram<11>>,
	queued: DynFamily<Gauge>,
	pings: DynFamily<EnumFamily<SendResult, Counter>>,
}

impl Appservice {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self {
		Self {
			transactions: DynFamily::new(&["appservice"], APPSERVICE_CAP, || {
				EnumFamily::new(Counter::new)
			}),
			duration: DynFamily::new(&["appservice"], APPSERVICE_CAP, || {
				Histogram::seconds(&SECONDS)
			}),
			queued: DynFamily::new(&["appservice"], APPSERVICE_CAP, Gauge::new),
			pings: DynFamily::new(&["appservice"], APPSERVICE_CAP, || {
				EnumFamily::new(Counter::new)
			}),
		}
	}

	/// Records one attempt to send a transaction, successful or not, and how
	/// long it took.
	pub fn transaction(&self, appservice: &str, result: SendResult, elapsed: Duration) {
		let key = [appservice];
		self.transactions.get(&key).get(result).inc();
		self.duration.get(&key).observe_duration(elapsed);
	}

	/// Records one ping.
	pub fn ping(&self, appservice: &str, result: SendResult) {
		self.pings.get(&[appservice]).get(result).inc();
	}

	/// Stores the number of events waiting in the queue of an appservice.
	pub fn set_queued(&self, appservice: &str, events: u64) {
		self.queued
			.get(&[appservice])
			.set_unsigned(events);
	}

	#[must_use]
	/// Returns the number of transaction attempts of an appservice that ended
	/// as `result`.
	pub fn transactions(&self, appservice: &str, result: SendResult) -> u64 {
		self.transactions
			.get(&[appservice])
			.get(result)
			.get()
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_appservice_transactions_total",
			"Attempts to send a transaction to an appservice, by how they ended; a retry is \
			 another attempt.",
			&self.transactions,
		);
		w.metric(
			"tuwunel_appservice_transaction_seconds",
			"Time to send a transaction to an appservice and get its answer.",
			&self.duration,
		);
		w.metric(
			"tuwunel_appservice_queued_events",
			"Events waiting in the queue of an appservice, counted up to a ceiling by a \
			 periodic scan; events in a transaction in flight are not counted.",
			&self.queued,
		);
		w.metric(
			"tuwunel_appservice_ping_total",
			"Pings of an appservice, by how they ended.",
			&self.pings,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.transactions.series(),
			self.duration.series(),
			self.queued.series(),
			self.pings.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Appservice {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for Appservice {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Appservice")
			.finish_non_exhaustive()
	}
}
