//! This module contains the device client and device message types.

use futures::{ Future, Sink, Stream };

/// A client for the Azure IoT Hub MQTT protocol. This client receives device-level messages.
///
/// A `Client` is a [`Stream`] of [`Message`]s. These messages contain twin properties and cloud-to-device communications.
///
/// It automatically reconnects if the connection to the server is broken. Each reconnection will yield one [`Message::TwinInitial`] message.
pub struct Client {
	inner: mqtt::Client<crate::IoSource>,

	keep_alive: std::time::Duration,
	max_back_off: std::time::Duration,
	current_back_off: std::time::Duration,
	c2d_prefix: String,

	state: State,

	direct_method_response_send: futures::sync::mpsc::Sender<DirectMethodResponse>,
	direct_method_response_recv: futures::sync::mpsc::Receiver<DirectMethodResponse>,
}

enum State {
	BeginBackOff,
	EndBackOff(tokio::timer::Delay),
	BeginSendingGetRequest,
	EndSendingGetRequest(Box<dyn Future<Item = (), Error = mqtt::PublishError> + Send>),
	WaitingForGetResponse(tokio::timer::Delay),
	ReceivedTwinInitial(TwinState),
	HaveGetResponse {
		previous_version: usize,
	},
}

impl Client {
	/// Creates a new `Client`
	///
	/// * `iothub_hostname`
	///
	///     The hostname of the Azure IoT Hub. Eg "foo.azure-devices.net"
	///
	/// * `device_id`
	///
	///     The ID of the device.
	///
	/// * `authentication`
	///
	///     The method this client should use to authorize with the Azure IoT Hub.
	///
	/// * `transport`
	///
	///     The transport to use for the connection to the Azure IoT Hub.
	///
	/// * `will`
	///
	///     If set, this message will be published by the server if this client disconnects uncleanly.
	///     Use the handle from `.inner().shutdown_handle()` to disconnect cleanly.
	///
	/// * `max_back_off`
	///
	///     Every connection failure or server error will double the back-off period, to a maximum of this value.
	///
	/// * `keep_alive`
	///
	///     The keep-alive time advertised to the server. The client will ping the server at half this interval.
	#[allow(clippy::new_ret_no_self)]
	pub fn new(
		iothub_hostname: String,
		device_id: String,
		authentication: crate::Authentication,
		transport: crate::Transport,

		will: Option<Vec<u8>>,

		max_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Result<Self, crate::CreateClientError> {
		let username = format!("{}/{}/api-version=2018-06-30", iothub_hostname, device_id);

		let will = will.map(|payload| (format!("devices/{}/messages/events/", device_id), payload));

		let c2d_prefix = format!("devices/{}/messages/devicebound/", device_id);

		let inner = crate::client_common::new(
			iothub_hostname,
			device_id,
			username,
			authentication,
			transport,

			will,

			max_back_off,
			keep_alive,

			vec![
				// Twin initial GET response
				mqtt::proto::SubscribeTo {
					topic_filter: "$iothub/twin/res/#".to_string(),
					qos: mqtt::proto::QoS::AtMostOnce,
				},

				// Twin patches
				mqtt::proto::SubscribeTo {
					topic_filter: "$iothub/twin/PATCH/properties/desired/#".to_string(),
					qos: mqtt::proto::QoS::AtMostOnce,
				},

				// C2D messages
				mqtt::proto::SubscribeTo {
					topic_filter: format!("{}#", c2d_prefix),
					qos: mqtt::proto::QoS::AtLeastOnce,
				},

				// Direct methods
				mqtt::proto::SubscribeTo {
					topic_filter: "$iothub/methods/POST/#".to_string(),
					qos: mqtt::proto::QoS::AtLeastOnce,
				},
			],
		)?;

		let (direct_method_response_send, direct_method_response_recv) = futures::sync::mpsc::channel(0);

		Ok(Client {
			inner,

			keep_alive,
			max_back_off,
			current_back_off: std::time::Duration::from_secs(0),
			c2d_prefix,

			state: State::BeginSendingGetRequest,

			direct_method_response_send,
			direct_method_response_recv,
		})
	}

	/// Gets a reference to the inner `mqtt::Client`
	pub fn inner(&self) -> &mqtt::Client<crate::IoSource> {
		&self.inner
	}

	/// Returns a handle that can be used to respond to direct methods
	pub fn direct_method_response_handle(&self) -> DirectMethodResponseHandle {
		DirectMethodResponseHandle(self.direct_method_response_send.clone())
	}
}

impl Stream for Client {
	type Item = Message;
	type Error = mqtt::Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			log::trace!("    {:?}", self.state);

			while let futures::Async::Ready(Some(direct_method_response)) = self.direct_method_response_recv.poll().expect("Receiver::poll cannot fail") {
				let DirectMethodResponse { request_id, status, payload, ack_sender } = direct_method_response;
				let payload = serde_json::to_vec(&payload).expect("cannot fail to serialize serde_json::Value");
				let publication = mqtt::proto::Publication {
					topic_name: format!("$iothub/methods/res/{}/?$rid={}", status, request_id),
					qos: mqtt::proto::QoS::AtLeastOnce,
					retain: false,
					payload,
				};

				if ack_sender.send(Box::new(self.inner.publish(publication))).is_err() {
					log::debug!("could not send ack for direct method response because ack receiver has been dropped");
				}
			}

			match &mut self.state {
				State::BeginBackOff => match self.current_back_off {
					back_off if back_off.as_secs() == 0 => {
						self.current_back_off = std::time::Duration::from_secs(1);
						self.state = State::BeginSendingGetRequest;
					},

					back_off => {
						log::debug!("Backing off for {:?}", back_off);
						let back_off_deadline = std::time::Instant::now() + back_off;
						self.current_back_off = std::cmp::min(self.max_back_off, self.current_back_off * 2);
						self.state = State::EndBackOff(tokio::timer::Delay::new(back_off_deadline));
					},
				},

				State::EndBackOff(back_off_timer) => match back_off_timer.poll().expect("could not poll back-off timer") {
					futures::Async::Ready(()) => self.state = State::BeginSendingGetRequest,
					futures::Async::NotReady => match self.inner.poll()? {
						// Inner client will resubscribe as necessary, so there's nothing for this client to do
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match Message::parse(publication, &self.c2d_prefix) {
							Ok(message @ Message::CloudToDevice(_)) |
							Ok(message @ Message::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message))),

							Ok(message @ Message::TwinInitial(_)) |
							Ok(message @ Message::TwinPatch(_)) =>
								log::debug!("Discarding message {:?} because we're still backing off before making a GET request", message),

							Err(err) =>
								log::warn!("Discarding message that could not be parsed: {}", err),
						},

						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},
				},

				State::BeginSendingGetRequest =>
					self.state = State::EndSendingGetRequest(Box::new(self.inner.publish(mqtt::proto::Publication {
						topic_name: format!("$iothub/twin/GET/?$rid={}", 1),
						qos: mqtt::proto::QoS::AtMostOnce,
						retain: false,
						payload: vec![],
					}))),

				State::EndSendingGetRequest(f) => match f.poll() {
					Ok(futures::Async::Ready(())) => {
						let deadline = std::time::Instant::now() + 2 * self.keep_alive;
						self.state = State::WaitingForGetResponse(tokio::timer::Delay::new(deadline));
					},

					Ok(futures::Async::NotReady) => match self.inner.poll()? {
						// Inner client hasn't sent the GET request yet, so there's nothing for this client to do
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match Message::parse(publication, &self.c2d_prefix) {
							Ok(message @ Message::CloudToDevice(_)) |
							Ok(message @ Message::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message))),

							Ok(message @ Message::TwinInitial(_)) |
							Ok(message @ Message::TwinPatch(_)) =>
								log::debug!("Discarding message {:?} because we haven't sent the GET request yet", message),

							Err(err) =>
								log::warn!("Discarding message that could not be parsed: {}", err),
						},

						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},

					// The publish can only fail if `self.inner` has shut down, which is not the case here
					Err(mqtt::PublishError::ClientDoesNotExist) => unreachable!(),
				},

				State::WaitingForGetResponse(timer) => {
					match self.inner.poll()? {
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => {
							log::warn!("Connection reset while waiting for GET response. Resending GET request...");
							self.state = State::BeginSendingGetRequest;
							continue;
						},

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match Message::parse(publication, &self.c2d_prefix) {
							Ok(message @ Message::CloudToDevice(_)) |
							Ok(message @ Message::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message))),

							Ok(Message::TwinInitial(twin_state)) => {
								self.state = State::ReceivedTwinInitial(twin_state);
								continue;
							},

							Ok(Message::TwinPatch(_)) => {
								log::debug!("Discarding patch response because we haven't received the initial response yet");
								continue;
							},

							Err(err @ MessageParseError::IotHubStatus(_)) => {
								log::warn!("{}", err);
								self.state = State::BeginBackOff;
								continue;
							},

							Err(err) => {
								log::warn!("Discarding message that could not be parsed: {}", err);
								continue;
							},
						},

						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

						futures::Async::NotReady => (),
					}

					match timer.poll().expect("could not poll GET response timer") {
						futures::Async::Ready(()) => {
							log::warn!("timed out waiting for GET response");
							self.state = State::BeginSendingGetRequest;
						},

						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					}
				},

				State::ReceivedTwinInitial(twin_state) => {
					let twin_state = std::mem::replace(twin_state, TwinState {
						desired: TwinProperties {
							version: 0,
							properties: Default::default(),
						},
						reported: TwinProperties {
							version: 0,
							properties: Default::default(),
						},
					});

					self.current_back_off = std::time::Duration::from_secs(0);
					self.state = State::HaveGetResponse {
						previous_version: twin_state.desired.version,
					};

					return Ok(futures::Async::Ready(Some(Message::TwinInitial(twin_state))));
				},

				State::HaveGetResponse { previous_version } => match self.inner.poll()? {
					futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => {
						log::warn!("Connection reset. Resending GET request...");
						self.state = State::BeginSendingGetRequest;
					},

					futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match Message::parse(publication, &self.c2d_prefix) {
						Ok(message @ Message::CloudToDevice(_)) |
						Ok(message @ Message::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message))),

						Ok(Message::TwinInitial(twin_state)) =>
							self.state = State::ReceivedTwinInitial(twin_state),

						Ok(Message::TwinPatch(twin_properties)) => {
							if twin_properties.version != *previous_version + 1 {
								log::warn!("expected PATCH response with version {} but received version {}", *previous_version + 1, twin_properties.version);
								self.state = State::BeginSendingGetRequest;
								continue;
							}

							*previous_version = twin_properties.version;

							return Ok(futures::Async::Ready(Some(Message::TwinPatch(twin_properties))));
						},

						Err(err) =>
							log::warn!("Discarding message that could not be parsed: {}", err),
					},

					futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

					futures::Async::NotReady => return Ok(futures::Async::NotReady),
				},
			}
		}
	}
}

impl std::fmt::Debug for State {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			State::BeginBackOff => f.debug_struct("BeginBackOff").finish(),
			State::EndBackOff(_) => f.debug_struct("EndBackOff").finish(),
			State::BeginSendingGetRequest => f.debug_struct("BeginSendingGetRequest").finish(),
			State::EndSendingGetRequest(_) => f.debug_struct("EndSendingGetRequest").finish(),
			State::WaitingForGetResponse(_) => f.debug_struct("WaitingForGetResponse").finish(),
			State::ReceivedTwinInitial(twin_state) => f.debug_tuple("ReceivedTwinInitial").field(twin_state).finish(),
			State::HaveGetResponse { previous_version } => f.debug_struct("HaveGetResponse").field("previous_version", previous_version).finish(),
		}
	}
}

/// A message generated by a [`Client`]
#[derive(Debug)]
pub enum Message {
	/// A cloud-to-device message
	CloudToDevice(CloudToDeviceMessage),

	/// A direct method invocation
	DirectMethod {
		name: String,
		payload: serde_json::Value,
		request_id: String,
	},

	/// The full twin state, as currently stored in the Azure IoT Hub.
	TwinInitial(TwinState),

	/// A patch to the twin state that should be applied to the current state to get the new state.
	TwinPatch(TwinProperties),
}

impl Message {
	fn parse(publication: mqtt::ReceivedPublication, c2d_prefix: &str) -> Result<Self, MessageParseError> {
		if let Some(captures) = RESPONSE_REGEX.captures(&publication.topic_name) {
			let status = &captures[1];
			let status: crate::Status = status.parse().map_err(|err| MessageParseError::TwinInitialCouldNotParseStatus(status.to_string(), err))?;

			match status {
				crate::Status::Ok => {
					let twin_state = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;
					Ok(Message::TwinInitial(twin_state))
				},

				status => Err(MessageParseError::IotHubStatus(status)),
			}
		}
		else if publication.topic_name.starts_with("$iothub/twin/PATCH/properties/desired/") {
			let twin_properties = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;

			Ok(Message::TwinPatch(twin_properties))
		}
		else if publication.topic_name.starts_with(c2d_prefix) {
			let mut system_properties = crate::system_properties::SystemPropertiesBuilder::new();
			let mut properties: std::collections::HashMap<_, _> = Default::default();

			for (key, value) in url::form_urlencoded::parse(publication.topic_name[c2d_prefix.len()..].as_bytes()) {
				if let Some(value) = system_properties.try_property(&*key, value) {
					properties.insert(key.into_owned(), value.into_owned());
				}
			}

			let system_properties = system_properties.build().map_err(MessageParseError::C2DMessageMissingRequiredProperty)?;
			Ok(Message::CloudToDevice(CloudToDeviceMessage {
				system_properties,
				properties,
				data: publication.payload,
			}))
		}
		else if let Some(captures) = DIRECT_METHOD_REGEX.captures(&publication.topic_name) {
			let name = captures[1].to_string();
			let payload = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;
			let request_id = captures[2].to_string();

			Ok(Message::DirectMethod {
				name,
				payload,
				request_id,
			})
		}
		else {
			Err(MessageParseError::UnrecognizedMessage)
		}
	}
}

#[derive(Debug)]
enum MessageParseError {
	C2DMessageMissingRequiredProperty(&'static str),
	IotHubStatus(crate::Status),
	Json(serde_json::Error),
	TwinInitialCouldNotParseStatus(String, std::num::ParseIntError),
	UnrecognizedMessage,
}

impl std::fmt::Display for MessageParseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			MessageParseError::C2DMessageMissingRequiredProperty(property) => write!(f, "C2D message does not contain required property {}", property),
			MessageParseError::IotHubStatus(status) => write!(f, "IoT Hub failed request with status {}", status),
			MessageParseError::Json(err) => write!(f, "could not parse payload as valid JSON: {}", err),
			MessageParseError::TwinInitialCouldNotParseStatus(status, err) => write!(f, "could not parse {:?} as status code: {}", status, err),
			MessageParseError::UnrecognizedMessage => write!(f, "message could not be recognized"),
		}
	}
}

impl std::error::Error for MessageParseError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		#[allow(clippy::match_same_arms)]
		match self {
			MessageParseError::C2DMessageMissingRequiredProperty(_) => None,
			MessageParseError::IotHubStatus(_) => None,
			MessageParseError::Json(err) => Some(err),
			MessageParseError::TwinInitialCouldNotParseStatus(_, err) => Some(err),
			MessageParseError::UnrecognizedMessage => None,
		}
	}
}

/// The full twin state stored in the Azure IoT Hub.
#[derive(Debug, serde_derive::Deserialize)]
pub struct TwinState {
	/// The desired twin state
	pub desired: TwinProperties,

	/// The twin state reported by the device
	pub reported: TwinProperties,
}

/// A collection of twin properties, including a version number
#[derive(Debug, serde_derive::Deserialize)]
pub struct TwinProperties {
	#[serde(rename = "$version")]
	pub version: usize,

	#[serde(flatten)]
	pub properties: std::collections::HashMap<String, serde_json::Value>,
}

#[derive(Debug)]
pub struct CloudToDeviceMessage {
	pub system_properties: crate::SystemProperties,
	pub properties: std::collections::HashMap<String, String>,
	pub data: Vec<u8>,
}

/// Used to respond to direct methods
pub struct DirectMethodResponseHandle(futures::sync::mpsc::Sender<DirectMethodResponse>);

impl DirectMethodResponseHandle {
	/// Send a direct method response with the given parameters
	pub fn respond(&self, request_id: String, status: crate::Status, payload: serde_json::Value) -> impl Future<Item = (), Error = DirectMethodResponseError> {
		let (ack_sender, ack_receiver) = futures::sync::oneshot::channel();
		let ack_receiver = ack_receiver.map_err(|_| DirectMethodResponseError::ClientDoesNotExist);

		self.0.clone()
			.send(DirectMethodResponse {
				request_id,
				status,
				payload,
				ack_sender,
			})
			.then(|result| match result {
				Ok(_) => Ok(()),
				Err(_) => Err(DirectMethodResponseError::ClientDoesNotExist),
			})
			.and_then(|()| ack_receiver)
			.and_then(|publish| publish.map_err(|_| DirectMethodResponseError::ClientDoesNotExist))
	}
}

#[derive(Debug)]
pub enum DirectMethodResponseError {
	ClientDoesNotExist,
}

impl std::fmt::Display for DirectMethodResponseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			DirectMethodResponseError::ClientDoesNotExist => write!(f, "client does not exist"),
		}
	}
}

impl std::error::Error for DirectMethodResponseError {
}

struct DirectMethodResponse {
	request_id: String,
	status: crate::Status,
	payload: serde_json::Value,
	ack_sender: futures::sync::oneshot::Sender<Box<dyn Future<Item = (), Error = mqtt::PublishError> + Send>>,
}

lazy_static::lazy_static! {
	static ref RESPONSE_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/twin/res/(\d+)/\?\$rid=1$").expect("could not compile regex");
	static ref DIRECT_METHOD_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/methods/POST/([^/]+)/\?\$rid=(.+)$").expect("could not compile regex");
}
