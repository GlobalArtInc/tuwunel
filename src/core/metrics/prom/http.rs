//! Metrics of the HTTP server.
//!
//! Requests are counted and timed per route and method, handlers in flight are
//! counted per route, and the size of sync responses is recorded per API. A
//! route is the template the router matched, never the path of a request, so
//! identifiers in a path cannot become labels.

#[cfg(test)]
mod tests;

use std::{sync::Arc, time::Instant};

use ::http::Method as HttpMethod;
use smallstr::SmallString;

use super::{
	family::{DynFamily, EnumFamily},
	types::{Counter, Gauge, Histogram, Metric},
	write::{Kind, Writer},
};
use crate::label_set;

/// Route label of a request that matched no route.
pub const UNMATCHED: &str = "_unmatched";

/// Ceiling on the (route, method) pairs that get a series of their own.
///
/// The pair of a request is chosen when its status is known, and a request the
/// router rejected or answered without a handler is counted under
/// [`UNMATCHED`] and `other`. Only pairs a handler served reach the ceiling, so
/// a client cannot fill it by probing methods.
pub const ROUTE_CAP: usize = 512;

const ROUTE_INLINE: usize = 128;

const METHOD_NOT_ALLOWED: u16 = 405;

static SECONDS: [u64; 14] = [
	5_000_000,
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
	60_000_000_000,
	120_000_000_000,
];

static SYNC_BYTES: [u64; 8] =
	[1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20, 1 << 22, 1 << 24];

label_set! {
	/// Class of a response status.
	///
	/// `Aborted` stands for a request dropped before it produced a response,
	/// as when the client goes away.
	pub enum StatusClass("status") {
		/// Informational, 1xx.
		Informational => "1xx",
		/// Success, 2xx.
		Success => "2xx",
		/// Redirection, 3xx.
		Redirection => "3xx",
		/// Client error, 4xx.
		ClientError => "4xx",
		/// Server error, 5xx.
		ServerError => "5xx",
		/// Dropped before a response.
		Aborted => "aborted",
	}
}

label_set! {
	/// Version of the sync API.
	pub enum Api("api") {
		/// The v3 endpoints.
		V3 => "v3",
		/// The sliding sync endpoint.
		V5 => "v5",
	}
}

/// Request method, folded into a closed set so that a client cannot mint
/// label values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Method {
	/// GET.
	Get,
	/// POST.
	Post,
	/// PUT.
	Put,
	/// DELETE.
	Delete,
	/// PATCH.
	Patch,
	/// HEAD.
	Head,
	/// OPTIONS.
	Options,
	/// Any other method.
	Other,
}

/// Metrics of the HTTP server.
pub struct Http {
	requests: Arc<DynFamily<EnumFamily<StatusClass, Counter>>>,
	duration: Arc<DynFamily<Histogram<14>>>,
	in_flight: DynFamily<Gauge>,
	sync_size: EnumFamily<Api, Shared<Histogram<8>>>,
}

/// A request in flight.
///
/// Created when a request arrives and resolved by [`Self::finish`]. Dropping
/// it without finishing, as happens when the future serving the request is
/// dropped, records the request as aborted. Either way the request leaves the
/// in-flight gauge and its time is observed.
///
/// The series of the request is looked up when it ends, because only then is
/// it known whether a handler served it.
#[must_use = "dropping the guard records the request as aborted"]
pub struct InFlight {
	requests: Arc<DynFamily<EnumFamily<StatusClass, Counter>>>,
	duration: Arc<DynFamily<Histogram<14>>>,
	in_flight: Arc<Gauge>,
	sync_size: Option<Arc<Histogram<8>>>,
	route: SmallString<[u8; ROUTE_INLINE]>,
	method: Method,
	started: Instant,
	status: StatusClass,
	rejected: bool,
}

struct Shared<M>(Arc<M>);

impl Method {
	#[must_use]
	/// Returns whether routes register handlers for this method.
	///
	/// `HEAD` is answered from the handler of `GET` and `OPTIONS` by the CORS
	/// layer, so a client can send either to any route.
	pub const fn is_routed(self) -> bool {
		matches!(self, Self::Get | Self::Post | Self::Put | Self::Delete | Self::Patch)
	}

	#[must_use]
	/// Returns the label value.
	pub const fn as_str(self) -> &'static str {
		match self {
			| Self::Get => "GET",
			| Self::Post => "POST",
			| Self::Put => "PUT",
			| Self::Delete => "DELETE",
			| Self::Patch => "PATCH",
			| Self::Head => "HEAD",
			| Self::Options => "OPTIONS",
			| Self::Other => "other",
		}
	}
}

impl From<&HttpMethod> for Method {
	fn from(method: &HttpMethod) -> Self {
		match *method {
			| HttpMethod::GET => Self::Get,
			| HttpMethod::POST => Self::Post,
			| HttpMethod::PUT => Self::Put,
			| HttpMethod::DELETE => Self::Delete,
			| HttpMethod::PATCH => Self::Patch,
			| HttpMethod::HEAD => Self::Head,
			| HttpMethod::OPTIONS => Self::Options,
			| _ => Self::Other,
		}
	}
}

impl StatusClass {
	#[must_use]
	/// Returns the class of a status code. Codes outside 100 to 599 fold into
	/// the nearest class.
	pub const fn from_code(code: u16) -> Self {
		match code {
			| ..=199 => Self::Informational,
			| 200..=299 => Self::Success,
			| 300..=399 => Self::Redirection,
			| 400..=499 => Self::ClientError,
			| _ => Self::ServerError,
		}
	}
}

impl Http {
	#[must_use]
	/// Creates the group with every metric at zero.
	pub fn new() -> Self {
		Self {
			requests: Arc::new(DynFamily::new(&["route", "method"], ROUTE_CAP, || {
				EnumFamily::new(Counter::new)
			})),
			duration: Arc::new(DynFamily::new(&["route", "method"], ROUTE_CAP, || {
				Histogram::seconds(&SECONDS)
			})),
			in_flight: DynFamily::new(&["route"], ROUTE_CAP, Gauge::new),
			sync_size: EnumFamily::new(|| Shared(Arc::new(Histogram::new(&SYNC_BYTES)))),
		}
	}

	/// Starts accounting for a request.
	///
	/// `route` is the matched route template, or `None` when no route matched.
	/// `sync` names the sync API when the route is one of its endpoints, which
	/// records the size of the response too.
	///
	/// Only the in-flight gauge is looked up here, by route; the request and
	/// duration series are chosen when the request ends.
	pub fn begin(&self, route: Option<&str>, method: Method, sync: Option<Api>) -> InFlight {
		let route = route.unwrap_or(UNMATCHED);

		let in_flight = self.in_flight.get(&[route]);
		in_flight.inc();

		InFlight {
			requests: Arc::clone(&self.requests),
			duration: Arc::clone(&self.duration),
			in_flight,
			sync_size: sync.map(|api| Arc::clone(&self.sync_size.get(api).0)),
			route: SmallString::from_str(route),
			method,
			started: Instant::now(),
			status: StatusClass::Aborted,
			rejected: false,
		}
	}

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_http_requests_total",
			"Requests by route template, method and status class; aborted counts requests \
			 dropped before a response; requests no handler served count under route _unmatched \
			 and method other.",
			self.requests.as_ref(),
		);
		w.metric(
			"tuwunel_http_request_duration_seconds",
			"Time from arrival to the response, or to the drop of an aborted request.",
			self.duration.as_ref(),
		);
		w.metric(
			"tuwunel_http_requests_in_flight",
			"Requests being handled by route template.",
			&self.in_flight,
		);
		w.metric(
			"tuwunel_sync_response_size_bytes",
			"Size of sync responses by API version.",
			&self.sync_size,
		);
	}

	pub(super) fn series(&self) -> usize {
		[
			self.requests.series(),
			self.duration.series(),
			self.in_flight.series(),
			self.sync_size.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}

impl Default for Http {
	fn default() -> Self { Self::new() }
}

impl std::fmt::Debug for Http {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Http").finish_non_exhaustive()
	}
}

impl InFlight {
	/// Resolves the request with the status code of its response.
	///
	/// `response_bytes` is the size of the response body when it is known
	/// exactly; it is recorded for sync endpoints only.
	pub fn finish(mut self, code: u16, response_bytes: Option<u64>) {
		self.status = StatusClass::from_code(code);
		self.rejected = code == METHOD_NOT_ALLOWED;

		if let (Some(size), Some(bytes)) = (&self.sync_size, response_bytes) {
			size.observe(bytes);
		}
	}
}

impl Drop for InFlight {
	fn drop(&mut self) {
		self.in_flight.dec();

		let (route, method) = if self.rejected || !self.method.is_routed() {
			(UNMATCHED, Method::Other)
		} else {
			(self.route.as_str(), self.method)
		};

		let key = [route, method.as_str()];
		self.requests.get(&key).get(self.status).inc();
		self.duration
			.get(&key)
			.observe_duration(self.started.elapsed());
	}
}

impl<M: Metric> Metric for Shared<M> {
	const KIND: Kind = M::KIND;

	fn write(&self, w: &mut Writer, name: &str, labels: &str) { self.0.write(w, name, labels); }

	fn series(&self) -> usize { self.0.series() }
}
