#![cfg(test)]

use std::sync::{
	Arc,
	atomic::{AtomicUsize, Ordering::SeqCst},
};

use reqwest::StatusCode;
use serde_json::{Value, json};
use tuwunel_core::{
	Result,
	log::{Capture, capture::Data},
	ruma::RoomId,
};
use tuwunel_service::Services;

use self::{
	client::{Client, field, register},
	fixture::boot,
};

#[expect(dead_code)]
mod client;
mod fixture;

const TOKEN: &str = "join-unknown-local-room-access-token";
const EXHAUSTED: &str = "Several servers failed";
const FOREIGN_SERVER: &str = "127.0.0.1:9";

#[test]
fn unknown_local_room_is_refused_without_retry() -> Result {
	boot("join-unknown-local-room", ["rooms.joins.max_attempts_per_request=3"], exercise)
}

async fn exercise(services: &Services, base: &str) -> Result {
	register(services, "unknown-room-joiner", TOKEN).await?;

	let client = Client { services, base, token: TOKEN };
	let room_id = RoomId::parse(format!("!never-created:{}", services.globals.server_name()))?;

	let exhausted = Arc::new(AtomicUsize::new(0));
	let counter = exhausted.clone();
	let capture = Capture::new(
		&services.server.log.capture,
		Some(|data: Data<'_>| data.our_modules()),
		move |data: Data<'_>| {
			if data.message().starts_with(EXHAUSTED) {
				counter.fetch_add(1, SeqCst);
			}
		},
	);

	let _guard = capture.start();

	assert_not_found(&client, &format!("rooms/{room_id}/join")).await?;
	assert_eq!(exhausted.load(SeqCst), 0, "join by id retried a refused join");

	assert_not_found(&client, &format!("join/{room_id}")).await?;
	assert_eq!(exhausted.load(SeqCst), 0, "join by alias retried a refused join");

	assert!(!services.metadata.exists(&room_id).await, "the refused join appended an event");

	assert_retried(&client, &format!("join/{room_id}?server_name={FOREIGN_SERVER}")).await?;
	assert_eq!(exhausted.load(SeqCst), 1, "a join through a foreign server was not retried");

	Ok(())
}

async fn assert_not_found(client: &Client<'_>, path: &str) -> Result {
	let response = client
		.post_url(&client.url(path), &json!({}))
		.await?;

	let status = response.status();
	let body: Value = response.json().await?;

	assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
	assert_eq!(field(&body, "errcode")?, "M_NOT_FOUND", "{body}");

	Ok(())
}

async fn assert_retried(client: &Client<'_>, path: &str) -> Result {
	let response = client
		.post_url(&client.url(path), &json!({}))
		.await?;

	let status = response.status();
	let body: Value = response.json().await?;

	assert_ne!(status, StatusCode::NOT_FOUND, "{body}");

	Ok(())
}
