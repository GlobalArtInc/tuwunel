use std::{
	path::{Path, PathBuf},
	sync::Arc,
	time::{Duration, Instant},
};

use aws_lc_rs::constant_time::verify_slices_are_equal;
use http::{HeaderMap, header::AUTHORIZATION};
use tokio::{fs::read_to_string, sync::Mutex, time::timeout};
use tuwunel_core::{config::Config, error};

pub(super) const FILE_TTL: Duration = Duration::from_secs(1);

pub(super) const FILE_READ_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Eq, PartialEq)]
pub(super) enum Denied {
	Unauthorized,
	Unverifiable,
}

pub(super) struct Auth {
	ttl: Duration,
	read_timeout: Duration,
	file: Mutex<Option<Cached>>,
}

struct Cached {
	path: PathBuf,
	read: Instant,
	token: Option<Arc<str>>,
}

impl Auth {
	pub(super) fn new(ttl: Duration) -> Self {
		Self {
			ttl,
			read_timeout: FILE_READ_TIMEOUT,
			file: Mutex::new(None),
		}
	}

	#[cfg(test)]
	pub(super) fn with_read_timeout(mut self, read_timeout: Duration) -> Self {
		self.read_timeout = read_timeout;
		self
	}

	pub(super) async fn check(&self, config: &Config, headers: &HeaderMap) -> Result<(), Denied> {
		let metrics = &config.metrics;

		match (&metrics.bearer_token, &metrics.bearer_token_file) {
			| (Some(expected), _) => verify(expected, headers),
			| (None, Some(path)) => {
				let presented = bearer(headers).ok_or(Denied::Unauthorized)?;
				let expected = self.file_token(path).await?;

				compare(&expected, presented)
			},
			| (None, None) => Ok(()),
		}
	}

	async fn file_token(&self, path: &Path) -> Result<Arc<str>, Denied> {
		let mut cached = self.file.lock().await;

		let fresh = cached
			.as_ref()
			.filter(|entry| entry.path == path && entry.read.elapsed() < self.ttl);

		if let Some(entry) = fresh {
			return entry.token.clone().ok_or(Denied::Unverifiable);
		}

		let token = timeout(self.read_timeout, read_token(path))
			.await
			.unwrap_or_else(|_| {
				error!(path = %path.display(), "Timed out reading the metrics bearer token file");
				None
			});

		*cached = Some(Cached {
			path: path.to_owned(),
			read: Instant::now(),
			token: token.clone(),
		});

		token.ok_or(Denied::Unverifiable)
	}
}

async fn read_token(path: &Path) -> Option<Arc<str>> {
	let text = read_to_string(path)
		.await
		.inspect_err(|e| {
			error!(path = %path.display(), "Failed to read the metrics bearer token file: {e}");
		})
		.ok()?;

	let token = text.trim();
	if token.is_empty() {
		error!(path = %path.display(), "The metrics bearer token file is empty");
		return None;
	}

	Some(Arc::from(token))
}

fn verify(expected: &str, headers: &HeaderMap) -> Result<(), Denied> {
	let presented = bearer(headers).ok_or(Denied::Unauthorized)?;

	compare(expected, presented)
}

fn compare(expected: &str, presented: &str) -> Result<(), Denied> {
	if expected.is_empty() {
		error!("The configured metrics bearer token is empty");
		return Err(Denied::Unverifiable);
	}

	ct_eq(expected, presented)
		.then_some(())
		.ok_or(Denied::Unauthorized)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
	let mut values = headers.get_all(AUTHORIZATION).iter();
	let value = values.next()?;
	if values.next().is_some() {
		return None;
	}

	let (scheme, token) = value.to_str().ok()?.split_once(' ')?;

	scheme
		.eq_ignore_ascii_case("Bearer")
		.then(|| token.trim_start_matches(' '))
}

fn ct_eq(a: &str, b: &str) -> bool { verify_slices_are_equal(a.as_bytes(), b.as_bytes()).is_ok() }
