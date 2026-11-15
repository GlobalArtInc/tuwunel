use std::{
	future::poll_fn,
	pin::pin,
	time::{Duration, Instant},
};

use futures::future::Either;
use tuwunel_core::metrics::prom::{Phase, Registry};

/// Records the time `fut` spends inside its own polls as `phase`, leaving out
/// the time its task spends on other parts. Without a registry the future is
/// returned untouched.
pub(super) fn timed<'a, F>(
	registry: Option<&'a Registry>,
	phase: Phase,
	fut: F,
) -> impl Future<Output = F::Output> + Send + 'a
where
	F: Future + Send + 'a,
{
	let Some(registry) = registry else {
		return Either::Left(fut);
	};

	Either::Right(async move {
		let mut inner = pin!(fut);
		let mut busy = Duration::ZERO;
		let output = poll_fn(|cx| {
			let started = Instant::now();
			let polled = inner.as_mut().poll(cx);
			busy = busy.saturating_add(started.elapsed());

			polled
		})
		.await;

		registry.sync.v3_phase(phase, busy);

		output
	})
}

#[cfg(test)]
mod tests {
	use futures::FutureExt;
	use tuwunel_core::metrics::prom::{Phase, Registry};

	use super::timed;

	#[test]
	fn a_finished_part_returns_its_output_and_records_one_observation() {
		let registry = Registry::new();

		let output = timed(Some(&registry), Phase::Joined, async { 7_u64 }).now_or_never();

		assert_eq!(output, Some(7));
		assert_eq!(registry.sync.v3_phases(Phase::Joined), 1);
	}

	#[test]
	fn without_metrics_the_output_is_returned_and_nothing_is_recorded() {
		let registry = Registry::new();

		let output = timed(None, Phase::Joined, async { 7_u64 }).now_or_never();

		assert_eq!(output, Some(7));
		assert_eq!(registry.sync.v3_phases(Phase::Joined), 0);
	}
}
