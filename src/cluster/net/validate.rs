//! Checks of what arrives from the network before it can reach the log.
//!
//! A peer is authenticated, but a peer can still send a record that cannot be
//! applied, and a log entry once appended is replayed by every node for as long
//! as it stays in the log. Each check here stands between the wire and the log.

use std::collections::BTreeSet;

use openraft::{
	entry::RaftEntry,
	impls::{EntryPayload, Membership},
	raft::AppendEntriesRequest,
};
use reqwest::Url;

use crate::types::{Node, NodeId, Request, TypeConfig};

/// Checks every entry of an AppendEntries request.
///
/// A replicated write must pass [`tuwunel_database::Entry::validate`]. A
/// membership change must pass [`membership`]. The error names the entry.
pub fn append(request: &AppendEntriesRequest<TypeConfig>) -> Result<(), String> {
	for entry in &request.entries {
		let index = entry.log_id().index;

		match &entry.payload {
			| EntryPayload::Blank => {},
			| EntryPayload::Normal(Request(write)) => write
				.validate()
				.map_err(|error| format!("entry {index} is not a valid write: {error}"))?,
			| EntryPayload::Membership(membership) => self::membership(membership)
				.map_err(|error| format!("entry {index} holds a bad membership: {error}"))?,
		}
	}

	Ok(())
}

/// Checks a membership as it arrives from the network.
///
/// Decoding a membership does not check what the type guarantees when it is
/// built: the voter sets and the member records travel apart. Raft looks up
/// every voter in the member records to reach it, so a voter without a record
/// would stop every node that logs the entry. A membership must therefore hold
/// at least one voter configuration, none of its voter sets may be empty, every
/// voter must have a member record, and every record must pass [`node`].
pub fn membership(membership: &Membership<NodeId, Node>) -> Result<(), String> {
	let configs = membership.get_joint_config();

	if configs.is_empty() {
		return Err("no voter configuration".to_owned());
	}

	if configs.iter().any(BTreeSet::is_empty) {
		return Err("a voter configuration is empty".to_owned());
	}

	if let Some(voter) = membership
		.voter_ids()
		.find(|voter| membership.get_node(voter).is_none())
	{
		return Err(format!("voter {voter} has no member record"));
	}

	for (id, record) in membership.nodes() {
		node(record).map_err(|error| format!("member {id}: {error}"))?;
	}

	Ok(())
}

/// Checks a member record: its address is the base URL of an `http` or `https`
/// server, without credentials, a path, a query or a fragment, and its
/// incarnation is not zero.
///
/// The address is where requests carrying the cluster secret are sent, so an
/// address that smuggles a different destination or credentials is refused.
pub fn node(node: &Node) -> Result<(), String> {
	if node.incarnation == 0 {
		return Err("incarnation is zero".to_owned());
	}

	let url = Url::parse(&node.addr).map_err(|error| format!("address is not a URL: {error}"))?;

	if !matches!(url.scheme(), "http" | "https") {
		return Err(format!("address scheme {:?} is not http or https", url.scheme()));
	}

	if url.host_str().is_none_or(str::is_empty) {
		return Err("address has no host".to_owned());
	}

	if !url.username().is_empty() || url.password().is_some() {
		return Err("address carries credentials".to_owned());
	}

	if !matches!(url.path(), "" | "/") || url.query().is_some() || url.fragment().is_some() {
		return Err("address is more than a base URL".to_owned());
	}

	Ok(())
}
