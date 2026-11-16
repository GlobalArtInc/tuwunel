//! Quorum replication of the database between cluster nodes.
//!
//! The cluster runs Raft through `openraft` and replicates the physical write
//! batches of the database leader. This crate holds the types the protocol is
//! instantiated with and the durable storage of the Raft log. Nothing here
//! starts on its own: the server only builds the pieces when the cluster is
//! enabled in its configuration.
//!
//! The state machine is the database itself. It applies the committed log to
//! the database of a follower, and builds, serves and installs snapshots of the
//! database; the snapshot directory and the in-place replacement of a database
//! by a snapshot are in [`SnapshotStore`].

#![deny(missing_docs)]

extern crate rust_rocksdb as rocksdb;

mod machine;
mod snapshot;
mod store;
#[cfg(test)]
mod testing;
#[cfg(test)]
mod tests;
mod types;
mod util;

pub use self::{
	machine::{Machine, decode_marker, encode_marker},
	snapshot::{
		Recovery, Reopen, SnapshotDir, SnapshotFile, SnapshotStore, Snapshots,
		decode_leader_marker, encode_leader_marker,
	},
	store::{LogReader, LogStore, Meta},
	types::{Node, NodeId, Request, TypeConfig},
};
