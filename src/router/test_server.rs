use std::sync::Arc;

use tuwunel_core::{
	Config, Server,
	config::{Figment, Sources},
	log::{LogLevelReloadHandles, Logging, capture},
	metrics::Metrics,
	tracing::subscriber::NoSubscriber,
};

pub(crate) fn config(settings: Figment) -> Config {
	let raw = Figment::new()
		.merge(("server_name", "test.example"))
		.merge(settings);

	Config::new(&raw).expect("the test configuration parses")
}

pub(crate) fn server(settings: Figment) -> Server {
	let logging = Logging {
		subscriber: Arc::new(NoSubscriber::new()),
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(capture::State::new()),
	};

	Server::new(config(settings), Sources::default(), None, logging, Metrics::new(None))
}
