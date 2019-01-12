use futures::Future;

#[derive(Debug)]
pub(crate) struct State {
	max_back_off: std::time::Duration,
	current_back_off: std::time::Duration,

	keep_alive: std::time::Duration,

	inner: Inner,
}

enum Inner {
	BeginBackOff,

	EndBackOff(tokio::timer::Delay),

	SendRequest,

	WaitingForResponse {
		request_id: u8,
		timeout: tokio::timer::Delay,
	},

	HaveResponse {
		version: usize,
	},
}

impl State {
	pub(crate) fn new(
		max_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Self {
		State {
			max_back_off,
			current_back_off: std::time::Duration::from_secs(0),

			keep_alive,

			inner: Default::default(),
		}
	}

	#[allow(
		clippy::unneeded_field_pattern, // Clippy wants wildcard pattern for the `if let Some(Response)` pattern below,
		                                // which would silently allow fields to be added to the variant without adding them here
	)]
	pub(crate) fn poll(
		&mut self,
		client: &mut mqtt::Client<crate::IoSource>,

		message: &mut Option<super::InternalTwinStateMessage>,
		previous_request_id: &mut u8,
	) -> Result<super::Response<Message>, super::MessageParseError> {
		loop {
			log::trace!("    {:?}", self.inner);

			match &mut self.inner {
				Inner::BeginBackOff => match self.current_back_off {
					back_off if back_off.as_secs() == 0 => {
						self.current_back_off = std::time::Duration::from_secs(1);
						self.inner = Inner::SendRequest;
					},

					back_off => {
						log::debug!("Backing off for {:?}", back_off);
						let back_off_deadline = std::time::Instant::now() + back_off;
						self.current_back_off = std::cmp::min(self.max_back_off, self.current_back_off * 2);
						self.inner = Inner::EndBackOff(tokio::timer::Delay::new(back_off_deadline));
					},
				},

				Inner::EndBackOff(back_off_timer) => match back_off_timer.poll().expect("could not poll back-off timer") {
					futures::Async::Ready(()) => self.inner = Inner::SendRequest,
					futures::Async::NotReady => (),
				},

				Inner::SendRequest => {
					let request_id = previous_request_id.wrapping_add(1);
					*previous_request_id = request_id;

					// We don't care about the response since this is a QoS 0 publication.
					// We don't even need to `poll()` the future because `mqtt::Client::publish` puts it in the send queue *synchronously*.
					// But we do need to tell the caller client to poll the `mqtt::Client` at least once more so that it attempts to send the message,
					// so return `Response::Continue`.
					let _ = client.publish(mqtt::proto::Publication {
						topic_name: format!("$iothub/twin/GET/?$rid={}", request_id),
						qos: mqtt::proto::QoS::AtMostOnce,
						retain: false,
						payload: vec![],
					});

					let deadline = std::time::Instant::now() + 2 * self.keep_alive;
					let timeout = tokio::timer::Delay::new(deadline);
					self.inner = Inner::WaitingForResponse { request_id, timeout };
					return Ok(super::Response::Continue);
				},

				Inner::WaitingForResponse { request_id, timeout } => {
					if let Some(super::InternalTwinStateMessage::Response { status, request_id: message_request_id, payload, version: _ }) = message {
						if *message_request_id == *request_id {
							match status {
								crate::Status::Ok => {
									let twin_state: crate::TwinState = serde_json::from_slice(payload).map_err(super::MessageParseError::Json)?;

									let _ = message.take();

									self.inner = Inner::HaveResponse { version: twin_state.desired.version };
									return Ok(super::Response::Message(Message::Initial(twin_state)));
								},

								status @ crate::Status::TooManyRequests |
								status @ crate::Status::Error(_) => {
									log::warn!("getting initial twin state failed with status {}", status);

									let _ = message.take();

									self.inner = Inner::BeginBackOff;
									continue;
								},

								status => {
									let status = *status;
									let _ = message.take();
									return Err(super::MessageParseError::IotHubStatus(status));
								},
							}
						}
					}

					match timeout.poll().expect("could not poll initial twin state response timeout timer") {
						futures::Async::Ready(()) => {
							log::warn!("timed out waiting for initial twin state response");
							self.inner = Inner::SendRequest;
						},

						futures::Async::NotReady => return Ok(super::Response::NotReady),
					}
				},

				Inner::HaveResponse { version } => match message.take() {
					Some(super::InternalTwinStateMessage::TwinPatch(twin_properties)) => {
						if twin_properties.version != *version + 1 {
							log::warn!("expected PATCH response with version {} but received version {}", *version + 1, twin_properties.version);
							self.inner = Inner::SendRequest;
							continue;
						}

						*version = twin_properties.version;

						return Ok(super::Response::Message(Message::Patch(twin_properties)));
					},

					other => {
						*message = other;
						return Ok(super::Response::NotReady);
					},
				},
			}
		}
	}

	pub (crate) fn new_connection(&mut self) {
		self.inner = Inner::SendRequest;
	}
}

impl Default for Inner {
	fn default() -> Self {
		Inner::SendRequest
	}
}

impl std::fmt::Debug for Inner {
	#[allow(
		clippy::unneeded_field_pattern, // Clippy wants wildcard pattern for the WaitingForResponse arm,
		                                // which would silently allow fields to be added to the variant without adding them here
	)]
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Inner::BeginBackOff => f.debug_struct("BeginBackOff").finish(),

			Inner::EndBackOff(_) => f.debug_struct("EndBackOff").finish(),

			Inner::SendRequest => f.debug_struct("SendRequest").finish(),

			Inner::WaitingForResponse { request_id, timeout: _ } =>
				f.debug_struct("WaitingForResponse")
				.field("request_id", request_id)
				.finish(),

			Inner::HaveResponse { version } =>
				f.debug_struct("HaveResponse")
				.field("version", version)
				.finish(),
		}
	}
}

#[derive(Debug)]
pub(crate) enum Message {
	Initial(crate::TwinState),

	Patch(crate::TwinProperties),
}
