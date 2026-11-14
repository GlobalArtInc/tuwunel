//! Sizes of what the server holds: cached rooms and counted records.
//!
//! The counts come from scans that run every few minutes or not at all, so a
//! count is left out of the exposition until its first scan.

use super::{
	types::{Gauge, Metric, Seen},
	write::Writer,
};

/// Sizes of caches and counts of records.
#[derive(Debug, Default)]
pub struct Inventory {
	last_normal_entries: Gauge,
	users: Seen<Gauge>,
	rooms: Seen<Gauge>,
	devices: Seen<Gauge>,
}

impl Inventory {
	#[must_use]
	/// Creates the group with nothing counted yet.
	pub fn new() -> Self { Self::default() }

	/// Stores the number of rooms in the timeline cache of newest counts.
	pub fn set_last_normal_entries(&self, entries: u64) {
		self.last_normal_entries.set_unsigned(entries);
	}

	/// Stores the number of local user accounts.
	pub fn set_users(&self, users: u64) { self.users.touch().set_unsigned(users); }

	/// Stores the number of rooms the server knows.
	pub fn set_rooms(&self, rooms: u64) { self.rooms.touch().set_unsigned(rooms); }

	/// Stores the number of devices of local users.
	pub fn set_devices(&self, devices: u64) { self.devices.touch().set_unsigned(devices); }

	pub(super) fn write(&self, w: &mut Writer) {
		w.metric(
			"tuwunel_timeline_last_normal_entries",
			"Rooms held in the cache of newest timeline counts.",
			&self.last_normal_entries,
		);

		if self.users.is_seen() {
			w.metric(
				"tuwunel_users_registered",
				"Local user accounts, counted by a periodic scan.",
				&self.users,
			);
		}

		if self.rooms.is_seen() {
			w.metric(
				"tuwunel_rooms_known",
				"Rooms known to the server, counted by a periodic scan.",
				&self.rooms,
			);
		}

		if self.devices.is_seen() {
			w.metric(
				"tuwunel_devices",
				"Devices of local users, counted by a periodic scan.",
				&self.devices,
			);
		}
	}

	pub(super) fn series(&self) -> usize {
		[
			self.last_normal_entries.series(),
			self.users.series(),
			self.rooms.series(),
			self.devices.series(),
		]
		.into_iter()
		.fold(0, usize::saturating_add)
	}
}
