use std::fmt::Debug;

use tuwunel_core::implement;

/// Deletes a raw key from this map.
///
/// The removal uses the map's configured options, flushes immediately when the
/// engine is uncorked, and then notifies matching watchers. When the engine is
/// corked, the surrounding cork controls flushing.
///
/// # Panics
///
/// Panics if RocksDB rejects the deletion or an uncorked flush fails.
#[implement(super::Map)]
#[tracing::instrument(skip(self, key), fields(%self), level = "trace")]
pub fn remove<K>(&self, key: &K)
where
	K: AsRef<[u8]> + ?Sized + Debug,
{
	let write_options = &self.write_options;
	self.engine
		.write_delete(&self.cf(), key, write_options)
		.expect("database remove error");

	if !self.engine.corked() {
		self.engine.flush().expect("database flush error");
	}

	self.notify(key.as_ref());
}
