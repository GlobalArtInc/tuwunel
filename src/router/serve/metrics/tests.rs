use std::{
	net::TcpListener,
	path::{Path, PathBuf},
	sync::Arc,
	time::Duration,
};

use axum::{Router, body::Body};
use axum_server::Handle;
use http::{
	HeaderMap, Method, Request, StatusCode,
	header::{ALLOW, AUTHORIZATION, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use http_body_util::BodyExt;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
};
use tower::ServiceExt;
use tuwunel_core::{Server, config::Figment};

use super::{
	bind, router, serve,
	token::{Auth, Denied},
};
use crate::test_server::{config, server as test_server};

const HOUR: Duration = Duration::from_hours(1);

fn enabled() -> Figment {
	Figment::new()
		.merge(("metrics.enable", true))
		.merge(("metrics.port", 0))
}

fn server(settings: Figment) -> Arc<Server> {
	let server = test_server(settings);
	server.metrics.enable_prometheus();

	Arc::new(server)
}

struct TokenFile(PathBuf);

impl TokenFile {
	fn new(name: &str, content: &str) -> Self {
		let path =
			std::env::temp_dir().join(format!("tuwunel-metrics-{}-{name}", std::process::id()));

		std::fs::write(&path, content).expect("the token file is written");

		Self(path)
	}

	fn path(&self) -> &Path { &self.0 }

	fn write(&self, content: &str) {
		std::fs::write(&self.0, content).expect("the token file is rewritten");
	}
}

impl Drop for TokenFile {
	fn drop(&mut self) { std::fs::remove_file(&self.0).ok(); }
}

async fn send(
	router: &Router,
	method: Method,
	uri: &str,
	authorization: Option<&str>,
) -> (StatusCode, HeaderMap, String) {
	let mut request = Request::builder().method(method).uri(uri);
	if let Some(authorization) = authorization {
		request = request.header(AUTHORIZATION, authorization);
	}

	let response = router
		.clone()
		.oneshot(
			request
				.body(Body::empty())
				.expect("the request builds"),
		)
		.await
		.expect("the router is infallible");

	let (parts, body) = response.into_parts();
	let body = body
		.collect()
		.await
		.expect("the body is read")
		.to_bytes();

	(parts.status, parts.headers, String::from_utf8_lossy(&body).into_owned())
}

async fn status(router: &Router, authorization: Option<&str>) -> StatusCode {
	send(router, Method::GET, "/metrics", authorization)
		.await
		.0
}

#[test]
fn disabled_metrics_bind_nothing() {
	let server = server(Figment::new().merge(("metrics.port", 0)));

	assert!(bind(&server).expect("nothing to bind").is_none());
}

#[test]
fn a_listener_is_not_bound_without_a_registry() {
	let server = test_server(enabled());

	assert!(server.metrics.prometheus().is_none());
	assert!(bind(&server).expect("nothing to bind").is_none());
}

#[test]
fn enabled_metrics_bind_the_configured_address() {
	let listener = bind(&server(enabled()))
		.expect("the address binds")
		.expect("the listener exists");

	let addr = listener.local_addr().expect("the bound address");

	assert!(addr.ip().is_loopback());
	assert_ne!(addr.port(), 0);
}

#[test]
fn a_taken_port_is_a_configuration_error_naming_the_port() {
	let taken = TcpListener::bind("127.0.0.1:0").expect("an ephemeral port binds");
	let port = taken.local_addr().expect("bound address").port();
	let server = server(
		Figment::new()
			.merge(("metrics.enable", true))
			.merge(("metrics.port", port)),
	);

	let error = bind(&server).expect_err("the port is taken");

	assert!(error.to_string().contains("metrics.port"), "{error}");
}

#[tokio::test]
async fn scrape_returns_the_exposition() {
	let router = router(&server(enabled()), HOUR);
	let (status, headers, body) = send(&router, Method::GET, "/metrics", None).await;

	assert_eq!(status, StatusCode::OK);
	assert!(
		headers[CONTENT_TYPE]
			.to_str()
			.expect("a visible content type")
			.starts_with("text/plain; version=0.0.4")
	);
	assert!(body.contains("tuwunel_build_info{version=\""), "{body}");
}

#[tokio::test]
async fn only_get_metrics_is_served() {
	let router = router(&server(enabled()), HOUR);

	assert_eq!(send(&router, Method::GET, "/", None).await.0, StatusCode::NOT_FOUND);
	assert_eq!(
		send(&router, Method::GET, "/metrics/extra", None)
			.await
			.0,
		StatusCode::NOT_FOUND
	);
	assert_eq!(
		send(&router, Method::POST, "/metrics", None)
			.await
			.0,
		StatusCode::METHOD_NOT_ALLOWED
	);
	assert_eq!(
		send(&router, Method::DELETE, "/metrics", None)
			.await
			.0,
		StatusCode::METHOD_NOT_ALLOWED
	);

	let (status, headers, _) = send(&router, Method::HEAD, "/metrics", None).await;

	assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
	assert_eq!(headers[ALLOW], "GET");
}

#[tokio::test]
async fn a_configured_token_is_required_and_compared_exactly() {
	let server = server(enabled().merge(("metrics.bearer_token", "scrape-secret")));
	let router = router(&server, HOUR);

	for refused in [
		None,
		Some("Bearer wrong-secret"),
		Some("Bearer scrape-secre"),
		Some("Bearer scrape-secrets"),
		Some("Bearer "),
		Some("Basic scrape-secret"),
		Some("scrape-secret"),
	] {
		assert_eq!(status(&router, refused).await, StatusCode::UNAUTHORIZED, "{refused:?}");
	}

	let (_, headers, body) = send(&router, Method::GET, "/metrics", None).await;

	assert_eq!(headers[WWW_AUTHENTICATE], "Bearer");
	assert!(!body.contains("scrape-secret"));

	for accepted in ["Bearer scrape-secret", "bearer scrape-secret", "Bearer   scrape-secret"] {
		assert_eq!(status(&router, Some(accepted)).await, StatusCode::OK, "{accepted}");
	}
}

#[tokio::test]
async fn two_authorization_headers_are_refused() {
	let server = server(enabled().merge(("metrics.bearer_token", "scrape-secret")));
	let router = router(&server, HOUR);

	let response = router
		.oneshot(
			Request::builder()
				.uri("/metrics")
				.header(AUTHORIZATION, "Bearer scrape-secret")
				.header(AUTHORIZATION, "Bearer scrape-secret")
				.body(Body::empty())
				.expect("the request builds"),
		)
		.await
		.expect("the router is infallible");

	assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_token_file_that_hangs_is_refused_within_the_timeout() {
	let fifo = std::env::temp_dir().join(format!("tuwunel-metrics-{}-hang", std::process::id()));
	std::fs::remove_file(&fifo).ok();
	assert!(
		std::process::Command::new("mkfifo")
			.arg(&fifo)
			.status()
			.expect("mkfifo runs")
			.success()
	);

	let server = server(enabled().merge(("metrics.bearer_token_file", &fifo)));
	let auth = Auth::new(HOUR).with_read_timeout(Duration::from_millis(100));
	let mut headers = HeaderMap::new();
	headers.insert(AUTHORIZATION, "Bearer x".parse().expect("a header value"));

	let started = std::time::Instant::now();
	let denied = auth.check(&server.config, &headers).await;

	assert_eq!(denied, Err(Denied::Unverifiable));
	assert!(started.elapsed() < Duration::from_secs(2));

	let again = auth.check(&server.config, &headers).await;
	assert_eq!(again, Err(Denied::Unverifiable));

	std::fs::OpenOptions::new()
		.write(true)
		.open(&fifo)
		.expect("the blocked reader is released");
	std::fs::remove_file(&fifo).ok();
}

#[tokio::test]
async fn a_reloaded_token_applies_without_a_restart() {
	let server = server(enabled());
	let router = router(&server, HOUR);

	assert_eq!(status(&router, None).await, StatusCode::OK);

	server
		.config
		.update(config(enabled().merge(("metrics.bearer_token", "scrape-secret"))))
		.expect("the configuration is replaced");

	assert_eq!(status(&router, None).await, StatusCode::UNAUTHORIZED);
	assert_eq!(status(&router, Some("Bearer scrape-secret")).await, StatusCode::OK);
}

#[tokio::test]
async fn a_token_file_is_read_again_and_failures_are_refused() {
	let file = TokenFile::new("rotating", "first-token\n");
	let settings = enabled().merge(("metrics.bearer_token_file", file.path()));
	let router = router(&server(settings), Duration::ZERO);

	assert_eq!(status(&router, Some("Bearer first-token")).await, StatusCode::OK);
	assert_eq!(status(&router, None).await, StatusCode::UNAUTHORIZED);

	file.write("  second-token  \n");

	assert_eq!(status(&router, Some("Bearer first-token")).await, StatusCode::UNAUTHORIZED);
	assert_eq!(status(&router, Some("Bearer second-token")).await, StatusCode::OK);

	file.write(" \n");

	assert_eq!(
		status(&router, Some("Bearer ")).await,
		StatusCode::INTERNAL_SERVER_ERROR,
		"an empty file must never turn into an empty token"
	);

	std::fs::remove_file(file.path()).expect("the token file is removed");

	assert_eq!(
		status(&router, Some("Bearer second-token")).await,
		StatusCode::INTERNAL_SERVER_ERROR
	);
	assert_eq!(status(&router, None).await, StatusCode::UNAUTHORIZED);

	file.write("third-token");

	assert_eq!(status(&router, Some("Bearer third-token")).await, StatusCode::OK);
}

#[tokio::test]
async fn a_token_file_is_not_read_on_every_request() {
	let file = TokenFile::new("cached", "first-token");
	let settings = enabled().merge(("metrics.bearer_token_file", file.path()));
	let router = router(&server(settings), HOUR);

	assert_eq!(status(&router, Some("Bearer first-token")).await, StatusCode::OK);

	file.write("second-token");

	assert_eq!(status(&router, Some("Bearer first-token")).await, StatusCode::OK);
	assert_eq!(status(&router, Some("Bearer second-token")).await, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn the_listener_serves_over_a_socket_until_the_handle_stops_it() {
	let server = server(enabled());
	let listener = bind(&server)
		.expect("the address binds")
		.expect("the listener exists");

	let addr = listener.local_addr().expect("the bound address");
	let handle = Handle::new();
	let serving = tokio::spawn(serve(&server, &handle, listener).expect("the listener starts"));

	let mut stream = TcpStream::connect(addr)
		.await
		.expect("the listener accepts");

	stream
		.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
		.await
		.expect("the request is sent");

	let mut response = String::new();
	stream
		.read_to_string(&mut response)
		.await
		.expect("the response is read");

	assert!(response.starts_with("HTTP/1.1 200"), "{response}");
	assert!(response.contains("tuwunel_build_info"), "{response}");

	handle.graceful_shutdown(Some(Duration::from_secs(5)));

	serving
		.await
		.expect("the listener task joins")
		.expect("the listener stops cleanly");
}
