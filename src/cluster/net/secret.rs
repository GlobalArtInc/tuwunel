//! The shared secret that authenticates the traffic between nodes.

use std::{fmt, io, path::Path};

use aws_lc_rs::{
	constant_time::verify_slices_are_equal,
	digest::{SHA256, digest},
};
use axum::http::HeaderValue;

/// The shortest secret accepted, in bytes.
pub const MIN_SECRET_LEN: usize = 16;

/// The bearer token every node of the cluster presents and requires.
///
/// The token is compared by the digests of both sides, so the comparison takes
/// the same time whatever the length of what was presented and wherever it
/// first differs. The secret never appears in `Debug` output, in errors or in
/// logs, and the header it is sent in is marked sensitive.
#[derive(Clone)]
pub struct Secret {
	header: HeaderValue,
	digest: [u8; 32],
}

impl Secret {
	/// Builds the secret from its text, ignoring surrounding whitespace.
	///
	/// A secret that is empty, shorter than [`MIN_SECRET_LEN`], or holds a
	/// byte that is not a printable ASCII character (a space included) is
	/// refused. The error does not repeat the text.
	pub fn new(text: &str) -> io::Result<Self> {
		let token = text.trim();
		if token.is_empty() {
			return Err(invalid("the cluster secret is empty"));
		}

		if token.len() < MIN_SECRET_LEN {
			return Err(invalid(&format!(
				"the cluster secret is shorter than {MIN_SECRET_LEN} bytes"
			)));
		}

		if !token.bytes().all(|byte| byte.is_ascii_graphic()) {
			return Err(invalid("the cluster secret must be printable ASCII without spaces"));
		}

		let mut header = HeaderValue::from_str(&format!("Bearer {token}"))
			.map_err(|_| invalid("the cluster secret holds a character a header cannot carry"))?;

		header.set_sensitive(true);

		Ok(Self { header, digest: hash(token.as_bytes()) })
	}

	/// Reads the secret from a file.
	pub async fn load(path: &Path) -> io::Result<Self> {
		let text = tokio::fs::read_to_string(path)
			.await
			.map_err(|error| {
				io::Error::new(
					error.kind(),
					format!("cannot read the cluster secret file {}: {error}", path.display()),
				)
			})?;

		Self::new(&text)
	}

	/// The value of the `Authorization` header that presents the secret.
	#[must_use]
	pub fn authorization(&self) -> HeaderValue { self.header.clone() }

	/// Tells whether an `Authorization` header value presents the secret.
	#[must_use]
	pub fn accepts(&self, header: Option<&HeaderValue>) -> bool {
		let Some(presented) = header.and_then(|value| value.to_str().ok()) else {
			return false;
		};

		let Some((scheme, token)) = presented.split_once(' ') else {
			return false;
		};

		let known = hash(token.trim().as_bytes());

		scheme.eq_ignore_ascii_case("bearer")
			&& verify_slices_are_equal(&known, &self.digest).is_ok()
	}
}

impl fmt::Debug for Secret {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("Secret(..)") }
}

fn hash(token: &[u8]) -> [u8; 32] {
	let mut out = [0_u8; 32];
	out.copy_from_slice(digest(&SHA256, token).as_ref());

	out
}

fn invalid(message: &str) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, message) }
