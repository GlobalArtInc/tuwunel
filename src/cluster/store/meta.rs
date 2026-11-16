//! The identity and the markers kept beside the log.

use std::{io, sync::Arc};

use super::db::{Db, LEADER_MARKER, REPLACE_MARKER};
use crate::util::blocking;

/// Handle to what the log store keeps besides the log.
///
/// The incarnation is created once, when the store is created, and never
/// changes. A node whose volume is lost starts a new store, with a new
/// incarnation, and so can be told from the node it replaces.
///
/// The leader marker and the replace marker are opaque byte strings. The store
/// persists them and knows nothing of their contents; whoever sets one defines
/// its format. Both are synced to disk before the call returns.
#[derive(Clone)]
pub struct Meta {
	db: Arc<Db>,
}

impl Meta {
	pub(super) fn new(db: Arc<Db>) -> Self { Self { db } }

	/// The incarnation of this node's storage. It is never zero.
	#[must_use]
	pub fn incarnation(&self) -> u128 { self.db.incarnation() }

	/// The node this store was bound to, or none while it is not bound.
	pub async fn node_id(&self) -> io::Result<Option<u64>> {
		let db = Arc::clone(&self.db);

		blocking(move || db.node()).await
	}

	/// Binds the store to a node, or checks that it is bound to that node.
	///
	/// The first call records `node` in the store, synced to disk. Later calls
	/// succeed only for the same node: a volume that held node 0 and is
	/// mounted for node 1 is refused instead of acting as node 1 with the
	/// incarnation of node 0. Call it before the store is handed to Raft.
	pub async fn bind_node(&self, node: u64) -> io::Result<()> {
		let db = Arc::clone(&self.db);

		blocking(move || db.bind_node(node)).await
	}

	/// The leader marker, or none when it is not set.
	pub async fn leader_marker(&self) -> io::Result<Option<Vec<u8>>> {
		self.get(LEADER_MARKER).await
	}

	/// Sets the leader marker, replacing any earlier one.
	pub async fn set_leader_marker(&self, marker: &[u8]) -> io::Result<()> {
		self.set(LEADER_MARKER, marker).await
	}

	/// Removes the leader marker. Removing a marker that is not set succeeds.
	pub async fn clear_leader_marker(&self) -> io::Result<()> { self.clear(LEADER_MARKER).await }

	/// The replace marker, or none when it is not set.
	pub async fn replace_marker(&self) -> io::Result<Option<Vec<u8>>> {
		self.get(REPLACE_MARKER).await
	}

	/// Sets the replace marker, replacing any earlier one.
	pub async fn set_replace_marker(&self, marker: &[u8]) -> io::Result<()> {
		self.set(REPLACE_MARKER, marker).await
	}

	/// Removes the replace marker. Removing a marker that is not set succeeds.
	pub async fn clear_replace_marker(&self) -> io::Result<()> {
		self.clear(REPLACE_MARKER).await
	}

	async fn get(&self, key: &'static [u8]) -> io::Result<Option<Vec<u8>>> {
		let db = Arc::clone(&self.db);

		blocking(move || db.get_meta(key)).await
	}

	async fn set(&self, key: &'static [u8], marker: &[u8]) -> io::Result<()> {
		let db = Arc::clone(&self.db);
		let marker = marker.to_vec();

		blocking(move || db.put_meta(key, &marker)).await
	}

	async fn clear(&self, key: &'static [u8]) -> io::Result<()> {
		let db = Arc::clone(&self.db);

		blocking(move || db.delete_meta(key)).await
	}
}
