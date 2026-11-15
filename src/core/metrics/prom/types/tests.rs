use super::{Counter, Gauge, Metric, Seen};
use crate::metrics::prom::Writer;

fn rendered(seen: &Seen<Gauge>) -> String {
	let mut w = Writer::new();
	seen.write(&mut w, "tuwunel_probe", "");

	w.finish()
}

#[test]
fn a_counter_raised_to_a_smaller_total_keeps_its_value() {
	let counter = Counter::new();

	counter.raise_to(10);
	counter.raise_to(3);
	assert_eq!(counter.get(), 10);

	counter.raise_to(12);
	assert_eq!(counter.get(), 12);
}

#[test]
fn a_seen_metric_writes_nothing_until_it_is_published() {
	let seen = Seen::new(Gauge::new());

	assert!(!seen.is_seen());
	assert!(!rendered(&seen).contains("tuwunel_probe"));

	seen.publish(|gauge| gauge.set(5));

	assert!(seen.is_seen());
	assert!(rendered(&seen).contains("tuwunel_probe 5\n"));
}
