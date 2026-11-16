//! Quorum replication of the database between cluster nodes.
//!
//! The cluster runs Raft through `openraft` and replicates the physical write
//! batches of the database leader. This crate holds the types the protocol is
//! instantiated with and the durable storage of the Raft log. Nothing here
//! starts on its own: the server only builds the pieces when the cluster is
//! enabled in its configuration.

#![deny(missing_docs)]

extern crate rust_rocksdb as rocksdb;

mod store;
#[cfg(test)]
mod tests;
mod types;

pub use self::{
	store::{LogReader, LogStore, Meta},
	types::{Node, NodeId, Request, TypeConfig},
};
