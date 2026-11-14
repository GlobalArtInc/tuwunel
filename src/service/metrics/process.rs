use tuwunel_core::metrics::prom::ProcessSample;

pub(super) struct Sampled {
	pub(super) sample: ProcessSample,
	pub(super) failed: Vec<&'static str>,
}

#[cfg(target_os = "linux")]
const USER_HZ: u64 = 100;

#[cfg(target_os = "linux")]
pub(super) fn sample() -> Sampled {
	use std::fs::{read_dir, read_to_string};

	use tuwunel_core::utils::sys::{max_file_descriptors, page_size};

	let mut failed = Vec::new();
	let mut note = |name: &'static str| failed.push(name);

	let stat = read_to_string("/proc/self/stat")
		.ok()
		.and_then(|text| parse_stat(&text));

	let resident_bytes = read_to_string("/proc/self/statm")
		.ok()
		.and_then(|text| parse_statm(&text))
		.zip(
			page_size()
				.ok()
				.and_then(|size| u64::try_from(size).ok()),
		)
		.map(|(pages, size)| pages.saturating_mul(size));

	let open_fds = read_dir("/proc/self/fd").ok().map(|entries| {
		let open = entries.flatten().count();

		u64::try_from(open.saturating_sub(1)).unwrap_or(u64::MAX)
	});

	let max_fds = max_file_descriptors()
		.ok()
		.and_then(|(soft, _)| u64::try_from(soft).ok());

	if stat.is_none() {
		note("/proc/self/stat");
	}

	if resident_bytes.is_none() {
		note("/proc/self/statm");
	}

	if open_fds.is_none() {
		note("/proc/self/fd");
	}

	if max_fds.is_none() {
		note("getrlimit");
	}

	let sample = ProcessSample {
		resident_bytes,
		cpu_micros: stat.map(|(ticks, _)| ticks.saturating_mul(1_000_000 / USER_HZ)),
		threads: stat.map(|(_, threads)| threads),
		open_fds,
		max_fds,
		start_time_seconds: None,
	};

	Sampled { sample, failed }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn sample() -> Sampled {
	Sampled {
		sample: ProcessSample::default(),
		failed: Vec::new(),
	}
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn parse_stat(text: &str) -> Option<(u64, u64)> {
	let mut fields = text.rsplit_once(')')?.1.split_ascii_whitespace();

	let user: u64 = fields.nth(11)?.parse().ok()?;
	let system: u64 = fields.next()?.parse().ok()?;
	let threads: u64 = fields.nth(4)?.parse().ok()?;

	Some((user.saturating_add(system), threads))
}

#[cfg(any(target_os = "linux", test))]
pub(super) fn parse_statm(text: &str) -> Option<u64> {
	text.split_ascii_whitespace().nth(1)?.parse().ok()
}
