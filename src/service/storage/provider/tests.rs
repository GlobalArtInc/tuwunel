//! Tests for storage-provider payload chunking.
//!
//! The cases cover aligned, partial, empty, and maximum-sized payloads. They
//! verify chunk boundaries without requiring a live storage backend.

use bytes::Bytes;
use object_store::PutPayload;
use tuwunel_core::{Result, config::Figment, metrics::prom::Op};

use super::chunked;
use crate::test_utils::fixture;

#[test]
fn chunked_splits_into_part_sized_chunks() {
	let payload = PutPayload::from(vec![0_u8; 25]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 3);
	assert_eq!(chunks[0].content_length(), 10);
	assert_eq!(chunks[1].content_length(), 10);
	assert_eq!(chunks[2].content_length(), 5);
}

#[test]
fn chunked_aligned_size_yields_no_remainder() {
	let payload = PutPayload::from(vec![0_u8; 30]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 3);
	assert!(chunks.iter().all(|c| c.content_length() == 10));
}

#[test]
fn chunked_smaller_than_part_size_yields_one() {
	let payload = PutPayload::from(vec![0_u8; 5]);
	let chunks: Vec<PutPayload> = chunked(payload, 10).collect();

	assert_eq!(chunks.len(), 1);
	assert_eq!(chunks[0].content_length(), 5);
}

#[test]
fn chunked_empty_payload_yields_nothing() {
	let payload = PutPayload::from(Vec::<u8>::new());

	assert!(chunked(payload, 10).next().is_none());
}

#[test]
fn chunked_usize_max_yields_one_part() {
	let payload = PutPayload::from(vec![0_u8; 1024]);
	let chunks: Vec<PutPayload> = chunked(payload, usize::MAX).collect();

	assert_eq!(chunks.len(), 1);
	assert_eq!(chunks[0].content_length(), 1024);
}

#[tokio::test]
async fn provider_operations_are_counted_by_outcome() -> Result {
	let Some(fixture) = fixture(Figment::new()).await? else {
		return Ok(());
	};

	let services = &fixture.services;
	let registry = services.server.metrics.enable_prometheus();
	let provider = services.storage.provider("media")?;
	let count = |op, ok| registry.storage.operations("media", op, ok);
	let before = [
		count(Op::Put, true),
		count(Op::Head, true),
		count(Op::Get, true),
		count(Op::Get, false),
		count(Op::Delete, true),
	];

	provider
		.put_one("metrics/object", Bytes::from_static(b"payload"))
		.await?;

	provider.head("metrics/object").await?;
	provider.get("metrics/object").await?;
	provider
		.get("metrics/absent")
		.await
		.expect_err("the object does not exist");

	provider.delete_one("metrics/object").await?;

	let after = [
		count(Op::Put, true),
		count(Op::Head, true),
		count(Op::Get, true),
		count(Op::Get, false),
		count(Op::Delete, true),
	];

	assert_eq!(after.map(|n| n.saturating_sub(1)), before);

	let text = registry.render();

	assert!(text.contains(
		"tuwunel_storage_operations_total{provider=\"media\",op=\"get\",result=\"error\"} "
	));
	assert!(
		text.contains("tuwunel_storage_operation_seconds_count{provider=\"media\",op=\"head\"} ")
	);

	Ok(())
}
