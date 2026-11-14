use ::http::Method as HttpMethod;

use super::{Api, Http, Method, ROUTE_CAP, StatusClass, UNMATCHED};

fn count(http: &Http, route: &str, method: Method, class: StatusClass) -> u64 {
	http.requests
		.get(&[route, method.as_str()])
		.get(class)
		.get()
}

#[test]
fn status_codes_fold_into_their_class() {
	for (code, class) in [
		(100, StatusClass::Informational),
		(199, StatusClass::Informational),
		(200, StatusClass::Success),
		(204, StatusClass::Success),
		(301, StatusClass::Redirection),
		(404, StatusClass::ClientError),
		(499, StatusClass::ClientError),
		(500, StatusClass::ServerError),
		(599, StatusClass::ServerError),
		(999, StatusClass::ServerError),
	] {
		assert_eq!(StatusClass::from_code(code), class, "{code}");
	}
}

#[test]
fn unknown_methods_share_one_label() {
	let custom = HttpMethod::from_bytes(b"PURGE").expect("a custom method");

	assert_eq!(Method::from(&HttpMethod::GET), Method::Get);
	assert_eq!(Method::from(&HttpMethod::OPTIONS).as_str(), "OPTIONS");
	assert_eq!(Method::from(&custom).as_str(), "other");
}

#[test]
fn a_request_leaves_the_gauge_and_is_counted_once() {
	let http = Http::new();

	let finished = http.begin(Some("/a"), Method::Get, None);
	let aborted = http.begin(Some("/a"), Method::Get, None);
	assert_eq!(http.in_flight.get(&["/a"]).get(), 2);

	finished.finish(200, None);
	assert_eq!(http.in_flight.get(&["/a"]).get(), 1);

	drop(aborted);

	assert_eq!(http.in_flight.get(&["/a"]).get(), 0);
	assert_eq!(count(&http, "/a", Method::Get, StatusClass::Success), 1);
	assert_eq!(count(&http, "/a", Method::Get, StatusClass::Aborted), 1);
	assert_eq!(http.duration.get(&["/a", "GET"]).count(), 2);
}

#[test]
fn a_missing_route_is_unmatched_and_size_is_kept_for_sync_only() {
	let http = Http::new();

	http.begin(None, Method::Post, None)
		.finish(404, Some(10));
	http.begin(Some("/sync"), Method::Get, Some(Api::V5))
		.finish(200, Some(2_000));
	http.begin(Some("/sync"), Method::Get, Some(Api::V5))
		.finish(200, None);

	assert_eq!(count(&http, UNMATCHED, Method::Post, StatusClass::ClientError), 1);
	assert_eq!(http.sync_size.get(Api::V5).0.count(), 1);
	assert_eq!(http.sync_size.get(Api::V3).0.count(), 0);
}

#[test]
fn probing_methods_does_not_fill_the_ceiling() {
	let http = Http::new();
	let custom = HttpMethod::from_bytes(b"PURGE").expect("a custom method");
	let methods = [
		Method::Delete,
		Method::Patch,
		Method::Head,
		Method::Options,
		Method::from(&custom),
		Method::Put,
	];

	for route in 0..100 {
		let route = format!("/r{route}");
		for method in methods {
			http.begin(Some(&route), method, None)
				.finish(405, None);
		}
	}

	assert_eq!(http.requests.len(), 1);
	assert_eq!(http.duration.len(), 1);
	assert_eq!(count(&http, UNMATCHED, Method::Other, StatusClass::ClientError), 600);

	for route in 0..ROUTE_CAP.saturating_sub(1) {
		http.begin(Some(&format!("/legit{route}")), Method::Get, None)
			.finish(200, None);
	}

	assert_eq!(http.requests.overflowed(), 0);
	assert_eq!(http.duration.overflowed(), 0);
	assert_eq!(count(&http, "/legit0", Method::Get, StatusClass::Success), 1);
}

#[test]
fn a_method_with_no_handler_of_its_own_is_counted_as_other() {
	let http = Http::new();

	http.begin(Some("/a"), Method::Head, None)
		.finish(200, None);
	http.begin(Some("/a"), Method::Options, None)
		.finish(200, None);
	http.begin(Some("/a"), Method::Post, None)
		.finish(405, None);
	http.begin(Some("/a"), Method::Delete, None)
		.finish(200, None);

	assert_eq!(count(&http, UNMATCHED, Method::Other, StatusClass::Success), 2);
	assert_eq!(count(&http, UNMATCHED, Method::Other, StatusClass::ClientError), 1);
	assert_eq!(count(&http, "/a", Method::Delete, StatusClass::Success), 1);
	assert_eq!(http.requests.len(), 2);
}
