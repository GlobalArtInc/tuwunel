use std::{
	io,
	sync::{Arc, Mutex},
};

use futures::{Stream, StreamExt};
use openraft::{
	OptionalSend, StoredMembership,
	entry::RaftEntry,
	storage::{EntryResponder, RaftSnapshotBuilder, RaftStateMachine, Snapshot, SnapshotMeta},
	type_config::alias::{LogIdOf, SnapshotMetaOf, SnapshotOf, StoredMembershipOf},
};

use crate::TypeConfig;

#[derive(Clone, Default)]
struct State {
	applied: Option<LogIdOf<TypeConfig>>,
	membership: StoredMembershipOf<TypeConfig>,
}

type Current = Arc<Mutex<Option<SnapshotOf<TypeConfig, Vec<u8>>>>>;

/// The smallest state machine the storage conformance suite accepts. It keeps
/// the applied position and the membership and nothing else.
#[derive(Default)]
pub(crate) struct Machine {
	state: State,
	current: Current,
}

pub(crate) struct Snapshots {
	state: State,
	current: Current,
}

impl RaftSnapshotBuilder<TypeConfig> for Snapshots {
	type SnapshotData = Vec<u8>;

	async fn build_snapshot(&mut self) -> io::Result<SnapshotOf<TypeConfig, Vec<u8>>> {
		let snapshot = Snapshot {
			meta: SnapshotMeta {
				last_log_id: self.state.applied,
				last_membership: self.state.membership.clone(),
			},
			snapshot: Vec::new(),
		};

		*self.current.lock().expect("snapshot lock") = Some(snapshot.clone());

		Ok(snapshot)
	}
}

impl RaftStateMachine<TypeConfig> for Machine {
	type SnapshotBuilder = Snapshots;
	type SnapshotData = Vec<u8>;

	async fn applied_state(
		&mut self,
	) -> io::Result<(Option<LogIdOf<TypeConfig>>, StoredMembershipOf<TypeConfig>)> {
		Ok((self.state.applied, self.state.membership.clone()))
	}

	async fn apply<Strm>(&mut self, mut entries: Strm) -> io::Result<()>
	where
		Strm: Stream<Item = io::Result<EntryResponder<TypeConfig>>> + Unpin + OptionalSend,
	{
		while let Some(item) = entries.next().await {
			let (entry, responder) = item?;
			let log_id = entry.log_id();
			if let Some(membership) = entry.get_membership() {
				self.state.membership = StoredMembership::new(Some(log_id), membership);
			}

			self.state.applied = Some(log_id);
			if let Some(responder) = responder {
				responder.send(());
			}
		}

		Ok(())
	}

	async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
		Snapshots {
			state: self.state.clone(),
			current: Arc::clone(&self.current),
		}
	}

	async fn install_snapshot(
		&mut self,
		meta: &SnapshotMetaOf<TypeConfig>,
		snapshot: Vec<u8>,
	) -> io::Result<()> {
		self.state = State {
			applied: meta.last_log_id,
			membership: meta.last_membership.clone(),
		};

		*self.current.lock().expect("snapshot lock") =
			Some(Snapshot { meta: meta.clone(), snapshot });

		Ok(())
	}

	async fn get_current_snapshot(
		&mut self,
	) -> io::Result<Option<SnapshotOf<TypeConfig, Vec<u8>>>> {
		Ok(self
			.current
			.lock()
			.expect("snapshot lock")
			.clone())
	}
}
