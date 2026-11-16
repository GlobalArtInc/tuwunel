//! The Raft state machine of the cluster: the replicated database itself.
//!
//! The database leader writes first and captures its writes as log entries,
//! so the leader's state machine applies nothing. For an entry of its own it
//! only publishes how far the cluster has confirmed the database sequence,
//! which is what the commit barrier waits for. A follower writes every entry
//! of another origin into its database, together with a marker naming the log
//! position reached, in one atomic write.
//!
//! The marker lives in the database column `raft_applied`. Its first byte is
//! the format version, [`MARKER_VERSION`]; the rest is the postcard encoding of
//! the pair of the applied log id and the membership in force at that position.
//! Both are needed to answer `applied_state` after a restart.

#[cfg(test)]
mod tests;

use std::{io, ops::Range, sync::Arc};

use futures::{Stream, StreamExt};
use openraft::{
	OptionalSend, StoredMembership,
	entry::RaftEntry,
	impls::EntryPayload,
	storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine},
	type_config::alias::{
		EntryPayloadOf, LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf,
	},
};
use tokio::sync::watch;
use tuwunel_database::{Database, Role};

use crate::{
	types::{Request, TypeConfig},
	util::blocking,
};

/// Format version of the position marker, its first byte.
const MARKER_VERSION: u8 = 1;

type Applied = (Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>);

/// State machine over the cluster database.
///
/// Raft is its only caller: the trait methods take `&mut self`, so entries are
/// applied one at a time and in log order. The database must not be changed
/// from anywhere else on a follower, and its role must not change while an
/// apply runs.
///
/// Snapshots are not implemented yet. A planned snapshot is deferred, so the
/// Raft core carries on; a forced one, which a lagging follower or a restart
/// after a purge would need, fails with an error. Until snapshots exist the
/// cluster runtime still runs Raft with `SnapshotPolicy::Never`.
pub struct Machine {
	db: Arc<Database>,
	committed: watch::Sender<u64>,
}

/// Snapshot builder of the state machine until it can snapshot: every build
/// fails.
#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshots;

impl Machine {
	/// Creates the state machine over `db`.
	///
	/// `committed` is the cluster-confirmed database sequence of a leader. An
	/// entry the leader captured itself moves it up to the sequence the entry
	/// ends at, and it never moves down. The barrier of the leader waits on the
	/// receiving side. The sender is not touched on a follower.
	#[must_use]
	pub fn new(db: Arc<Database>, committed: watch::Sender<u64>) -> Self {
		Self { db, committed }
	}

	async fn stored(&self) -> io::Result<Applied> {
		let db = Arc::clone(&self.db);

		blocking(move || {
			match db
				.engine
				.applied_marker()
				.map_err(io::Error::other)?
			{
				| None => Ok((None, StoredMembership::default())),
				| Some(marker) => decode_marker(&marker),
			}
		})
		.await
	}

	fn lead(
		&self,
		log_id: &LogIdOf<TypeConfig>,
		payload: &EntryPayloadOf<TypeConfig>,
	) -> io::Result<()> {
		match payload {
			| EntryPayload::Normal(Request(entry)) if entry.origin == self.db.engine.origin() =>
				self.confirm(&entry.seq),
			| EntryPayload::Normal(Request(entry)) => Err(io::Error::other(format!(
				"leader cannot apply entry {log_id} of origin {:016x}: only its own entries are \
				 committed here",
				entry.origin
			))),
			| EntryPayload::Blank | EntryPayload::Membership(_) => Ok(()),
		}
	}

	fn confirm(&self, seq: &Range<u64>) -> io::Result<()> {
		let head = self.db.engine.current_sequence();
		if seq.start > seq.end || seq.end > head {
			return Err(invalid(format!(
				"own entry covers sequence {}..{} of a database at {head}",
				seq.start, seq.end
			)));
		}

		self.committed.send_if_modified(|committed| {
			let advance = seq.end > *committed;
			if advance {
				*committed = seq.end;
			}

			advance
		});

		Ok(())
	}

	async fn follow(
		&self,
		log_id: LogIdOf<TypeConfig>,
		payload: EntryPayloadOf<TypeConfig>,
		membership: StoredMembershipOf<TypeConfig>,
	) -> io::Result<StoredMembershipOf<TypeConfig>> {
		let (entries, membership) = match payload {
			| EntryPayload::Normal(Request(entry)) => (vec![entry], membership),
			| EntryPayload::Blank => (Vec::new(), membership),
			| EntryPayload::Membership(members) =>
				(Vec::new(), StoredMembership::new(Some(log_id), members)),
		};

		let marker = encode_marker(&log_id, &membership)?;
		let db = Arc::clone(&self.db);

		blocking(move || {
			db.engine
				.apply_replicated(&entries, &marker)
				.map_err(io::Error::other)
		})
		.await?;

		Ok(membership)
	}
}

impl RaftSnapshotBuilder<TypeConfig> for Snapshots {
	type SnapshotData = Vec<u8>;

	async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TypeConfig, Vec<u8>>> {
		Err(unavailable())
	}
}

impl RaftStateMachine<TypeConfig> for Machine {
	type SnapshotBuilder = Snapshots;
	type SnapshotData = Vec<u8>;

	async fn applied_state(&mut self) -> io::Result<Applied> { self.stored().await }

	async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
	where
		Strm: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
	{
		let mut membership: Option<StoredMembershipOf<TypeConfig>> = None;

		while let Some(item) = entries.next().await {
			let (entry, responder) = item?;
			let log_id = entry.log_id();

			if self.db.engine.role() == Role::Leader {
				self.lead(&log_id, &entry.payload)?;
			} else {
				let current = match membership.take() {
					| Some(current) => current,
					| None => self.stored().await?.1,
				};

				membership = Some(
					self.follow(log_id, entry.payload, current)
						.await?,
				);
			}

			if let Some(responder) = responder {
				responder.send(());
			}
		}

		Ok(())
	}

	async fn try_create_snapshot_builder(
		&mut self,
		force: bool,
	) -> Option<Self::SnapshotBuilder> {
		force.then_some(Snapshots)
	}

	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder { Snapshots }

	async fn install_snapshot(
		&mut self,
		_meta: &SnapshotMetaOf<TypeConfig>,
		_snapshot: Vec<u8>,
	) -> io::Result<()> {
		Err(unavailable())
	}

	async fn get_current_snapshot(
		&mut self,
	) -> io::Result<Option<SnapshotOf<TypeConfig, Vec<u8>>>> {
		Ok(None)
	}
}

/// Encodes the position marker the state machine keeps in the database, for
/// [`tuwunel_database::Engine::apply_replicated`] and
/// [`tuwunel_database::Engine::write_marker`].
///
/// The first byte is the format version, 1; the rest is the postcard encoding
/// of the pair of the applied log id and the membership in force at that log
/// id. [`decode_marker`] reads it back. Any change of `Node`, of the log id or
/// membership types, or of the openraft version that alters these bytes must
/// raise the marker version.
pub fn encode_marker(
	applied: &LogIdOf<TypeConfig>,
	membership: &StoredMembershipOf<TypeConfig>,
) -> io::Result<Vec<u8>> {
	postcard::to_extend(&(applied, membership), vec![MARKER_VERSION])
		.map_err(|error| invalid(format!("raft_applied marker is not encodable: {error}")))
}

/// Decodes a position marker written by [`encode_marker`].
///
/// An empty marker means nothing was applied: no log id and the empty
/// membership. A marker of an unknown version, a damaged one, and one with
/// bytes after its end are errors of kind `InvalidData`.
pub fn decode_marker(
	marker: &[u8],
) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
	let Some((&version, body)) = marker.split_first() else {
		return Ok((None, StoredMembership::default()));
	};

	if version != MARKER_VERSION {
		return Err(invalid(format!(
			"raft_applied marker version {version} is not supported, expected {MARKER_VERSION}"
		)));
	}

	let ((applied, membership), rest) =
		postcard::take_from_bytes::<(LogIdOf<TypeConfig>, StoredMembershipOf<TypeConfig>)>(body)
			.map_err(|error| invalid(format!("raft_applied marker is damaged: {error}")))?;

	if !rest.is_empty() {
		return Err(invalid(format!(
			"raft_applied marker has {} bytes after its end",
			rest.len()
		)));
	}

	Ok((Some(applied), membership))
}

fn invalid(message: String) -> io::Error { io::Error::new(io::ErrorKind::InvalidData, message) }

fn unavailable() -> io::Error {
	io::Error::new(io::ErrorKind::Unsupported, "snapshots are not available yet")
}
