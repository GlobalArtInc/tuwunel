use crate::metrics::prom::{ProcessSample, Registry};

const FAMILIES: [&str; 6] = [
	"process_resident_memory_bytes",
	"process_cpu_seconds_total",
	"process_threads",
	"process_open_fds",
	"process_max_fds",
	"process_start_time_seconds",
];

#[test]
fn a_reading_is_absent_until_taken_and_kept_when_unavailable() {
	let registry = Registry::new();
	let text = registry.render();

	for family in FAMILIES {
		assert!(!text.contains(family), "{family}\n{text}");
	}

	registry.process.record(&ProcessSample {
		threads: Some(4),
		cpu_micros: Some(2_500_000),
		..ProcessSample::default()
	});

	let text = registry.render();

	assert!(text.contains("# HELP process_threads "), "{text}");
	assert!(text.contains("# TYPE process_threads gauge\n"), "{text}");
	assert!(text.contains("process_threads 4\n"), "{text}");
	assert!(text.contains("process_cpu_seconds_total 2.5\n"), "{text}");
	assert!(!text.contains("process_open_fds"), "{text}");

	registry.process.record(&ProcessSample::default());

	let text = registry.render();

	assert!(text.contains("process_threads 4\n"), "{text}");
	assert!(text.contains("process_cpu_seconds_total 2.5\n"), "{text}");
	assert!(!text.contains("process_max_fds"), "{text}");
}
