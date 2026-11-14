mod token;

#[cfg(test)]
mod tests;

use std::{
	io,
	net::{SocketAddr, TcpListener},
	sync::Arc,
	time::Duration,
};

use axum::{
	Router,
	extract::State,
	response::{IntoResponse, Response},
	routing::get,
};
use axum_server::{Handle, from_tcp};
use futures::{FutureExt, future::BoxFuture};
use http::{
	HeaderMap, StatusCode,
	header::{ALLOW, CONTENT_TYPE, WWW_AUTHENTICATE},
};
use tower_http::timeout::TimeoutLayer;
use tuwunel_core::{Result, Server, err, error, info, metrics::prom, warn};

use self::token::{Auth, Denied, FILE_TTL};
#[cfg(any(
	feature = "zstd_compression",
	feature = "gzip_compression",
	feature = "brotli_compression"
))]
use crate::layers::compression_layer;

struct Endpoint {
	server: Arc<Server>,
	auth: Auth,
}

pub(super) fn bind(server: &Server) -> Result<Option<TcpListener>> {
	let metrics = &server.config.metrics;

	if !metrics.enable {
		return Ok(None);
	}

	if server.metrics.prometheus().is_none() {
		warn!(
			"The metrics listener is enabled in the configuration but the registry was not \
			 created at startup; it stays down until a restart"
		);

		return Ok(None);
	}

	let addr = SocketAddr::new(metrics.address, metrics.port);
	let listener = TcpListener::bind(addr).map_err(|e| match e.kind() {
		| io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied =>
			err!(Config("metrics.port", "Failed to bind the metrics listener to {addr}: {e}")),
		| _ =>
			err!(Config("metrics.address", "Failed to bind the metrics listener to {addr}: {e}")),
	})?;

	listener.set_nonblocking(true)?;

	Ok(Some(listener))
}

pub(super) fn serve<'a>(
	server: &Arc<Server>,
	handle: &Handle<SocketAddr>,
	listener: TcpListener,
) -> Result<BoxFuture<'a, Result<(), io::Error>>> {
	let addr = listener.local_addr()?;
	let router = router(server, FILE_TTL).into_make_service();

	let acceptor = from_tcp(listener)?
		.handle(handle.clone())
		.serve(router)
		.boxed();

	info!(%addr, "Serving metrics");

	Ok(acceptor)
}

fn router(server: &Arc<Server>, token_ttl: Duration) -> Router {
	let endpoint = Arc::new(Endpoint {
		server: Arc::clone(server),
		auth: Auth::new(token_ttl),
	});

	let router = Router::new()
		.route("/metrics", get(scrape).head(head_refused))
		.fallback(not_found)
		.with_state(endpoint);

	#[cfg(any(
		feature = "zstd_compression",
		feature = "gzip_compression",
		feature = "brotli_compression"
	))]
	let router = router.layer(compression_layer(server));

	router.layer(TimeoutLayer::with_status_code(
		StatusCode::REQUEST_TIMEOUT,
		Duration::from_secs(server.config.http.request_timeout),
	))
}

async fn scrape(State(endpoint): State<Arc<Endpoint>>, headers: HeaderMap) -> Response {
	let config = Arc::clone(&endpoint.server.config);

	if let Err(denied) = endpoint.auth.check(&config, &headers).await {
		return denied.into_response();
	}

	let Some(registry) = endpoint.server.metrics.prometheus() else {
		error!("The metrics listener is up but the registry was never enabled");
		return StatusCode::SERVICE_UNAVAILABLE.into_response();
	};

	([(CONTENT_TYPE, prom::CONTENT_TYPE)], registry.render()).into_response()
}

async fn not_found() -> StatusCode { StatusCode::NOT_FOUND }

async fn head_refused() -> impl IntoResponse {
	(StatusCode::METHOD_NOT_ALLOWED, [(ALLOW, "GET")])
}

impl IntoResponse for Denied {
	fn into_response(self) -> Response {
		match self {
			| Self::Unauthorized => (
				StatusCode::UNAUTHORIZED,
				[(WWW_AUTHENTICATE, "Bearer")],
				"A bearer token is required\n",
			)
				.into_response(),
			| Self::Unverifiable =>
				(StatusCode::INTERNAL_SERVER_ERROR, "The bearer token cannot be verified\n")
					.into_response(),
		}
	}
}
