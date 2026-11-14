#[cfg(test)]
mod tests;

use std::{
	convert::Infallible,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll, ready},
};

use axum::{
	body::HttpBody,
	extract::{MatchedPath, Request},
	response::Response,
};
use pin_project_lite::pin_project;
use tower::{Layer, Service};
use tuwunel_core::metrics::{
	Metrics,
	prom::{Api, InFlight, Method, Registry},
};

#[derive(Clone)]
pub(crate) struct MetricsLayer {
	metrics: Arc<Metrics>,
}

#[derive(Clone)]
pub(crate) struct MetricsService<S> {
	metrics: Arc<Metrics>,
	inner: S,
}

pin_project! {
	pub(crate) struct MetricsFuture<F> {
		#[pin]
		inner: F,
		guard: Option<InFlight>,
	}
}

impl MetricsLayer {
	pub(crate) fn new(metrics: Arc<Metrics>) -> Self { Self { metrics } }
}

impl<S> Layer<S> for MetricsLayer {
	type Service = MetricsService<S>;

	fn layer(&self, inner: S) -> Self::Service {
		MetricsService {
			metrics: Arc::clone(&self.metrics),
			inner,
		}
	}
}

impl<S> Service<Request> for MetricsService<S>
where
	S: Service<Request, Response = Response, Error = Infallible>,
{
	type Error = Infallible;
	type Future = MetricsFuture<S::Future>;
	type Response = Response;

	fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		self.inner.poll_ready(cx)
	}

	fn call(&mut self, request: Request) -> Self::Future {
		let guard = self
			.metrics
			.prometheus()
			.map(|registry| begin(registry, &request));

		MetricsFuture { inner: self.inner.call(request), guard }
	}
}

impl<F> Future for MetricsFuture<F>
where
	F: Future<Output = Result<Response, Infallible>>,
{
	type Output = F::Output;

	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		let this = self.project();
		let result = ready!(this.inner.poll(cx));

		if let (Some(guard), Ok(response)) = (this.guard.take(), &result) {
			guard.finish(response.status().as_u16(), response.body().size_hint().exact());
		}

		Poll::Ready(result)
	}
}

fn begin(registry: &Registry, request: &Request) -> InFlight {
	let route = request
		.extensions()
		.get::<MatchedPath>()
		.map(MatchedPath::as_str);

	registry
		.http
		.begin(route, Method::from(request.method()), route.and_then(sync_api))
}

fn sync_api(route: &str) -> Option<Api> {
	match route {
		| "/_matrix/client/r0/sync" | "/_matrix/client/v3/sync" => Some(Api::V3),
		| "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync" => Some(Api::V5),
		| _ => None,
	}
}
