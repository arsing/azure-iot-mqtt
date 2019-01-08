pub(crate) fn new(
	iothub_hostname: String,
	client_id: String,
	username: String,
	authentication: crate::Authentication,
	transport: crate::Transport,

	will: Option<(String, Vec<u8>)>,

	max_back_off: std::time::Duration,
	keep_alive: std::time::Duration,

	default_subscriptions: impl IntoIterator<Item = mqtt::proto::SubscribeTo>,
) -> Result<mqtt::Client<crate::IoSource>, crate::CreateClientError> {
	let (password, certificate) = match authentication {
		crate::Authentication::SasToken(sas_token) => (Some(sas_token), None),
		crate::Authentication::Certificate { der, password } => (None, Some((der, password))),
	};

	let will = will.map(|(topic_name, payload)| mqtt::proto::Publication {
		topic_name,
		qos: mqtt::proto::QoS::AtMostOnce,
		retain: false,
		payload,
	});

	let io_source = crate::IoSource::new(
		iothub_hostname.into(),
		certificate.into(),
		2 * keep_alive,
		transport,
	)?;

	let mut inner = mqtt::Client::new(
		Some(client_id),
		Some(username),
		password,
		will,
		io_source,
		max_back_off,
		keep_alive,
	);

	for subscribe_to in default_subscriptions {
		match inner.subscribe(subscribe_to) {
			Ok(()) => (),

			// The subscription can only fail if `inner` has shut down, which is not the case here
			Err(mqtt::UpdateSubscriptionError::ClientDoesNotExist) => unreachable!(),
		}
	}

	Ok(inner)
}
