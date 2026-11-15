use super::APPSERVICE_CAP;
use crate::metrics::prom::Registry;

#[test]
fn queues_beyond_the_cap_add_up_in_the_shared_series() {
	let registry = Registry::new();
	let ids: Vec<String> = (0..APPSERVICE_CAP.saturating_add(3))
		.map(|index| format!("as{index}"))
		.collect();

	let queues: Vec<_> = ids.iter().map(|id| (id.as_str(), 10)).collect();

	registry.appservice.set_queued(&queues);

	let text = registry.render();

	assert!(
		text.contains("tuwunel_appservice_queued_events{appservice=\"as0\"} 10\n"),
		"{text}"
	);
	assert!(
		text.contains("tuwunel_appservice_queued_events{appservice=\"_other\"} 30\n"),
		"{text}"
	);

	let queues: Vec<_> = ids.iter().map(|id| (id.as_str(), 0)).collect();

	registry.appservice.set_queued(&queues);

	let text = registry.render();

	assert!(
		text.contains("tuwunel_appservice_queued_events{appservice=\"as0\"} 0\n"),
		"{text}"
	);
	assert!(
		text.contains("tuwunel_appservice_queued_events{appservice=\"_other\"} 0\n"),
		"{text}"
	);
}
