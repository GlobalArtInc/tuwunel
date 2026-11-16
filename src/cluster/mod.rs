//! Quorum replication of the database between cluster nodes.
//!
//! The cluster runs Raft through `openraft` and replicates the physical write
//! batches of the database leader. This crate holds the types the protocol is
//! instantiated with, the durable storage of the Raft log, the state machine
//! over the database and the network between the nodes. Nothing here starts on
//! its own: the server only builds the pieces when the cluster is enabled in
//! its configuration.

#![deny(missing_docs)]

extern crate rust_rocksdb as rocksdb;

mod machine;
pub mod net;
mod store;
#[cfg(test)]
mod tests;
mod types;
mod util;

pub use self::{
	machine::{Machine, Snapshots, decode_marker, encode_marker},
	store::{LogReader, LogStore, MAX_SYNC_DELAY, Meta, SyncDelay},
	types::{Node, NodeId, Request, TypeConfig},
};
