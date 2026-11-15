use bytes::BytesMut;
use reqwest::Request;
use ruma::api::{
	OutgoingRequestExt,
	appservice::{Registration, ping::send_ping},
	auth_scheme::SendAccessToken,
	error::{BadStatusErrorData, ErrorKind},
};
use tuwunel_core::{
	Error, Result, err, http::StatusCode, implement, metrics::prom::SendResult,
	utils::string_from_bytes, warn,
};

use super::request::add_access_token_query;
use crate::client::read_response_capped;

/// Pings an appservice per MSC2659, surfacing the spec's failure error codes.
///
/// Returns `Ok(())` without pinging when the registration has no URL set.
#[implement(super::Service)]
pub async fn ping(
	&self,
	registration: Registration,
	request: send_ping::v1::Request,
) -> Result<()> {
	let Some(dest) = registration.url else {
		return Ok(());
	};

	if dest == *"null" || dest.is_empty() {
		return Ok(());
	}

	let (base, client) = self.transport(&dest)?;
	let hs_token = registration.hs_token.as_str();
	let mut http_request = request
		.try_into_http_request::<BytesMut>(base, SendAccessToken::IfRequired(hs_token), ())
		.map_err(|e| {
			self.record_ping(&registration.id, SendResult::Other);

			err!(Request(ConnectionFailed(warn!(
				appservice = %registration.id,
				%dest,
				?e,
				"Failed to find appservice destination"
			))))
		})?
		.map(BytesMut::freeze);

	add_access_token_query(&mut http_request, hs_token);

	let reqwest_request = Request::try_from(http_request).inspect_err(|_| {
		self.record_ping(&registration.id, SendResult::Other);
	})?;

	let response = client
		.execute(reqwest_request)
		.await
		.map_err(|e| {
			// The URL carries the hs_token in its access_token query.
			let e = e.without_url();

			if e.is_timeout() {
				self.record_ping(&registration.id, SendResult::Timeout);

				err!(Request(ConnectionTimeout(warn!(
					appservice = %registration.id,
					%dest,
					?e,
					"Connection to appservice timed out"
				))))
			} else {
				self.record_ping(&registration.id, SendResult::Network);

				err!(Request(ConnectionFailed(warn!(
					appservice = %registration.id,
					%dest,
					?e,
					"Could not send request to appservice"
				))))
			}
		})?;

	let status = response.status();
	if status.is_success() {
		self.record_ping(&registration.id, SendResult::Ok);

		return Ok(());
	}

	self.record_ping(&registration.id, SendResult::BadResponse);

	let limit = self
		.services
		.config
		.network
		.http
		.max_response_size;

	let body = read_response_capped(response, limit)
		.await
		.ok()
		.and_then(|body| string_from_bytes(&body).ok());

	warn!(
		appservice = %registration.id,
		%status,
		%dest,
		"Appservice returned unsuccessful HTTP response to ping"
	);

	Err(Error::Request(
		ErrorKind::BadStatus(BadStatusErrorData { status: Some(status), body }),
		format!("Appservice returned status {status}").into(),
		StatusCode::BAD_REQUEST,
	))
}

#[implement(super::Service)]
fn record_ping(&self, id: &str, result: SendResult) {
	if let Some(registry) = self.services.server.metrics.prometheus() {
		registry.appservice.ping(id, result);
	}
}
