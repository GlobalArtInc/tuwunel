//! Helpers shared by the modules of the crate.

use std::io;

/// Runs a blocking operation off the async workers.
pub(crate) async fn blocking<T, F>(operation: F) -> io::Result<T>
where
	T: Send + 'static,
	F: FnOnce() -> io::Result<T> + Send + 'static,
{
	tokio::task::spawn_blocking(operation)
		.await
		.map_err(io::Error::other)?
}

/// An error for data that is present and wrong.
pub(crate) fn invalid(message: String) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidData, message)
}
