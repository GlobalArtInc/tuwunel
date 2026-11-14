#![cfg(test)]

use reqwest::Method;
use serde_json::{Value, json};
use tuwunel_core::Result;
use tuwunel_service::Services;

use self::{
	client::{Client, register},
	fixture::boot,
};

mod client;
mod fixture;

#[path = "client/admin_api.rs"]
mod admin_api;

const ADMIN_TOKEN: &str = "admin-list-rooms-page-admin-access-token";

/// Drives the room list over the admin API with a page narrower than the set.
///
/// The ordering, the batch tokens and the filters read only their own columns
/// of every room, while the rows of the page carry the full details.
#[test]
fn lists_a_page_in_order_with_full_details() -> Result {
	let options = ["admin.room.create=true", "registration.grant_admin_to_first_user=false"];

	boot("admin-list-rooms-page", options, exercise)
}

async fn exercise(services: &Services, base: &str) -> Result {
	let admin_id = register(services, "listroomsadmin", ADMIN_TOKEN).await?;

	services.admin.make_user_admin(&admin_id).await?;

	let admin = Client { services, base, token: ADMIN_TOKEN };

	for name in ["pageprobe-b", "pageprobe-a", "pageprobe-c"] {
		admin
			.create_room(&json!({ "preset": "private_chat", "name": name }))
			.await?;
	}

	let first = list(&admin, "search_term=pageprobe&order_by=name&limit=2").await?;

	assert_eq!(names(&first), ["pageprobe-a", "pageprobe-b"]);
	assert_eq!(first["total_rooms"], 3);
	assert_eq!(first["offset"], 0);
	assert_eq!(first["next_batch"], 2);
	assert!(first.get("prev_batch").is_none_or(Value::is_null));

	for room in first["rooms"].as_array().unwrap() {
		assert_eq!(room["joined_members"], 1);
		assert_eq!(room["joined_local_members"], 1);
		assert_eq!(room["creator"], admin_id.as_str());
		assert_eq!(room["join_rules"], "invite");
		assert!(room["version"].is_string());
		assert!(room["state_events"].as_u64().unwrap() > 0);
	}

	let last = list(&admin, "search_term=pageprobe&order_by=name&limit=2&from=2").await?;

	assert_eq!(names(&last), ["pageprobe-c"]);
	assert_eq!(last["total_rooms"], 3);
	assert_eq!(last["prev_batch"], 0);
	assert!(last.get("next_batch").is_none_or(Value::is_null));

	let backward = list(&admin, "search_term=pageprobe&order_by=name&dir=b&limit=1").await?;

	assert_eq!(names(&backward), ["pageprobe-c"]);
	assert_eq!(backward["next_batch"], 1);

	let by_members =
		list(&admin, "search_term=pageprobe&order_by=joined_members&limit=1").await?;

	assert_eq!(by_members["total_rooms"], 3);
	assert_eq!(names(&by_members).len(), 1);
	assert_eq!(by_members["next_batch"], 1);

	let public = list(&admin, "search_term=pageprobe&public_rooms=true").await?;

	assert_eq!(public["total_rooms"], 0);
	assert!(public["rooms"].as_array().unwrap().is_empty());

	let empty = list(&admin, "search_term=pageprobe&empty_rooms=true").await?;

	assert_eq!(empty["total_rooms"], 0);

	let private =
		list(&admin, "search_term=pageprobe&public_rooms=false&empty_rooms=false").await?;

	assert_eq!(private["total_rooms"], 3);

	Ok(())
}

async fn list(admin: &Client<'_>, query: &str) -> Result<Value> {
	let path = format!("v1/rooms?{query}");
	let response = admin
		.admin_api(Method::GET, &path, &json!({}), 200)
		.await?;

	Ok(response.parse()?)
}

fn names(response: &Value) -> Vec<&str> {
	response["rooms"]
		.as_array()
		.unwrap()
		.iter()
		.map(|room| room["name"].as_str().unwrap())
		.collect()
}
