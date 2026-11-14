use std::sync::Arc;

use axum::{
	Router,
	body::Body,
	routing::{get, post},
};
use futures::poll;
use http::{Method, Request, StatusCode};
use ruma::api::{
	Metadata,
	client::sync::sync_events::{v3, v5},
	path_builder::PathBuilder,
};
use tower::ServiceExt;
use tuwunel_core::metrics::{Metrics, prom::Api};

use super::{MetricsLayer, sync_api};

const ROUTE: &str = "/rooms/{room_id}/x";

async fn ok() -> &'static str { "ok" }

async fn big() -> String { "x".repeat(2_000) }

async fn not_found() -> StatusCode { StatusCode::NOT_FOUND }

fn app(metrics: &Arc<Metrics>) -> Router {
	Router::new()
		.route(ROUTE, get(ok))
		.route("/rooms/{room_id}/y", post(ok))
		.route("/slow", get(std::future::pending::<&'static str>))
		.route("/sync", get(big))
		.route("/_matrix/client/v3/sync", get(big))
		.fallback(not_found)
		.layer(MetricsLayer::new(Arc::clone(metrics)))
}

fn enabled() -> Arc<Metrics> {
	let metrics = Metrics::new(None);
	metrics.enable_prometheus();

	metrics
}

fn render(metrics: &Metrics) -> String {
	metrics
		.prometheus()
		.expect("the registry is enabled")
		.render()
}

fn request(method: Method, uri: &str) -> Request<Body> {
	Request::builder()
		.method(method)
		.uri(uri)
		.body(Body::empty())
		.expect("the request builds")
}

async fn send(app: &Router, method: Method, uri: &str) -> StatusCode {
	app.clone()
		.oneshot(request(method, uri))
		.await
		.expect("the router is infallible")
		.status()
}

#[tokio::test]
async fn requests_are_counted_timed_and_leave_the_gauge() {
	let metrics = enabled();
	let app = app(&metrics);

	for _ in 0..2 {
		assert_eq!(send(&app, Method::GET, "/rooms/!a:b/x").await, StatusCode::OK);
	}

	let text = render(&metrics);

	for line in [
		r#"tuwunel_http_requests_total{route="/rooms/{room_id}/x",method="GET",status="2xx"} 2"#,
		r#"tuwunel_http_request_duration_seconds_count{route="/rooms/{room_id}/x",method="GET"} 2"#,
		r#"tuwunel_http_request_duration_seconds_bucket{route="/rooms/{room_id}/x",method="GET",le="+Inf"} 2"#,
		r#"tuwunel_http_requests_in_flight{route="/rooms/{room_id}/x"} 0"#,
	] {
		assert!(text.lines().any(|found| found == line), "{line}\n{text}");
	}
}

#[tokio::test]
async fn routes_are_templates_and_unknown_paths_share_one_label() {
	let metrics = enabled();
	let app = app(&metrics);

	send(&app, Method::GET, "/rooms/!secret:room/x").await;
	send(&app, Method::GET, "/nothing/user-id-1").await;
	send(&app, Method::GET, "/nothing/user-id-2").await;
	send(&app, Method::GET, "/rooms/!secret:room/y").await;

	let text = render(&metrics);

	assert!(!text.contains("secret"), "{text}");
	assert!(!text.contains("user-id"), "{text}");
	assert!(text.contains(r#"route="/rooms/{room_id}/x""#), "{text}");
	assert!(
		text.lines().any(|line| line
			== r#"tuwunel_http_requests_total{route="_unmatched",method="GET",status="4xx"} 2"#),
		"{text}"
	);
	assert!(
		text.lines().any(|line| line
			== r#"tuwunel_http_requests_total{route="_unmatched",method="other",status="4xx"} 1"#),
		"{text}"
	);
}

#[tokio::test]
async fn a_dropped_request_is_aborted_and_leaves_the_gauge() {
	let metrics = enabled();
	let app = app(&metrics);

	let mut pending = Box::pin(app.clone().oneshot(request(Method::GET, "/slow")));

	assert!(poll!(pending.as_mut()).is_pending());
	assert!(
		render(&metrics)
			.lines()
			.any(|line| line == r#"tuwunel_http_requests_in_flight{route="/slow"} 1"#)
	);

	drop(pending);

	let text = render(&metrics);

	for line in [
		r#"tuwunel_http_requests_total{route="/slow",method="GET",status="aborted"} 1"#,
		r#"tuwunel_http_requests_total{route="/slow",method="GET",status="2xx"} 0"#,
		r#"tuwunel_http_request_duration_seconds_count{route="/slow",method="GET"} 1"#,
		r#"tuwunel_http_requests_in_flight{route="/slow"} 0"#,
	] {
		assert!(text.lines().any(|found| found == line), "{line}\n{text}");
	}
}

#[tokio::test]
async fn nothing_is_recorded_while_the_registry_is_disabled() {
	let metrics = Metrics::new(None);
	let app = app(&metrics);

	assert_eq!(send(&app, Method::GET, "/rooms/a/x").await, StatusCode::OK);
	assert!(metrics.prometheus().is_none());
}

#[tokio::test]
async fn sync_responses_are_sized_and_other_routes_are_not() {
	let metrics = enabled();
	let app = app(&metrics);

	send(&app, Method::GET, "/_matrix/client/v3/sync").await;
	send(&app, Method::GET, "/sync").await;

	let text = render(&metrics);

	for line in [
		r#"tuwunel_sync_response_size_bytes_count{api="v3"} 1"#,
		r#"tuwunel_sync_response_size_bytes_sum{api="v3"} 2000"#,
		r#"tuwunel_sync_response_size_bytes_bucket{api="v3",le="4096"} 1"#,
		r#"tuwunel_sync_response_size_bytes_bucket{api="v3",le="1024"} 0"#,
		r#"tuwunel_sync_response_size_bytes_count{api="v5"} 0"#,
	] {
		assert!(text.lines().any(|found| found == line), "{line}\n{text}");
	}
}

#[test]
fn every_sync_path_of_the_api_has_its_version() {
	for path in v3::Request::PATH_BUILDER.all_paths() {
		assert_eq!(sync_api(path), Some(Api::V3), "{path}");
	}

	for path in v5::Request::PATH_BUILDER.all_paths() {
		assert_eq!(sync_api(path), Some(Api::V5), "{path}");
	}

	assert_eq!(sync_api("/_matrix/client/v3/rooms/{room_id}/messages"), None);
}
