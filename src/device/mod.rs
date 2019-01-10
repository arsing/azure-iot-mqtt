//! This module contains the device client and device message types.

use futures::{ Future, Sink, Stream };

mod desired_properties;

mod reported_properties;
pub use self::reported_properties::{ ReportTwinStateHandle, ReportTwinStateRequest };

/// A client for the Azure IoT Hub MQTT protocol. This client receives device-level messages.
///
/// A `Client` is a [`Stream`] of [`Message`]s. These messages contain twin properties and cloud-to-device communications.
///
/// It automatically reconnects if the connection to the server is broken. Each reconnection will yield one [`Message::TwinInitial`] message.
pub struct Client {
	inner: mqtt::Client<crate::IoSource>,

	c2d_prefix: String,

	state: State,
	previous_request_id: u8,

	desired_properties: self::desired_properties::State,
	reported_properties: self::reported_properties::State,

	direct_method_response_send: futures::sync::mpsc::Sender<DirectMethodResponse>,
	direct_method_response_recv: futures::sync::mpsc::Receiver<DirectMethodResponse>,
}

enum State {
	WaitingForSubscriptions {
		reset_session: bool,
	},

	Idle,
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

			c2d_prefix,

			state: State::WaitingForSubscriptions { reset_session: true },
			previous_request_id: u8::max_value(),

			desired_properties: self::desired_properties::State::new(max_back_off, keep_alive),
			reported_properties: self::reported_properties::State::new(max_back_off, keep_alive),

			direct_method_response_send,
			direct_method_response_recv,
		})
	}

	/// Gets a reference to the inner `mqtt::Client`
	pub fn inner(&self) -> &mqtt::Client<crate::IoSource> {
		&self.inner
	}

	/// Returns a handle that can be used to publish reported twin state to the Azure IoT Hub
	pub fn report_twin_state_handle(&self) -> ReportTwinStateHandle {
		self.reported_properties.report_twin_state_handle()
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
				State::WaitingForSubscriptions { reset_session } =>
					if *reset_session {
						match self.inner.poll()? {
							futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

							futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match InternalMessage::parse(publication, &self.c2d_prefix) {
								Ok(message @ InternalMessage::CloudToDevice(_)) |
								Ok(message @ InternalMessage::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message.into()))),

								Ok(message @ InternalMessage::Response { .. }) |
								Ok(message @ InternalMessage::TwinPatch(_)) =>
									log::debug!("Discarding message {:?} because we haven't sent the requests yet", message),

								Err(err) =>
									log::warn!("Discarding message that could not be parsed: {}", err),
							},

							futures::Async::Ready(Some(mqtt::Event::SubscriptionUpdates(_))) => {
								log::debug!("subscriptions acked by server");
								self.state = State::Idle;
							},

							futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

							futures::Async::NotReady => return Ok(futures::Async::NotReady),
						}
					}
					else {
						self.state = State::Idle;
					},

				State::Idle => {
					let mut continue_loop = false;

					let mut message = match self.inner.poll()? {
						futures::Async::Ready(Some(mqtt::Event::NewConnection { reset_session })) => {
							self.state = State::WaitingForSubscriptions { reset_session };
							self.desired_properties.new_connection();
							self.reported_properties.new_connection();
							continue;
						},

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match InternalMessage::parse(publication, &self.c2d_prefix) {
							Ok(message @ InternalMessage::CloudToDevice(_)) |
							Ok(message @ InternalMessage::DirectMethod { .. }) => return Ok(futures::Async::Ready(Some(message.into()))),

							Ok(message @ InternalMessage::Response { .. }) |
							Ok(message @ InternalMessage::TwinPatch(_)) => {
								// There may be more messages, so continue the loop
								continue_loop = true;

								Some(message)
							},

							Err(err) => {
								log::warn!("Discarding message that could not be parsed: {}", err);
								continue;
							},
						},

						// Don't expect any subscription updates at this point
						futures::Async::Ready(Some(mqtt::Event::SubscriptionUpdates(_))) => unreachable!(),

						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

						futures::Async::NotReady => None,
					};

					match self.desired_properties.poll(&mut self.inner, &mut message, &mut self.previous_request_id) {
						Ok(Response::Message(message)) => return Ok(futures::Async::Ready(Some(message))),
						Ok(Response::Continue) => continue_loop = true,
						Ok(Response::NotReady) => (),
						Err(err) => log::warn!("Discarding message that could not be parsed: {}", err),
					}

					match self.reported_properties.poll(&mut self.inner, &mut message, &mut self.previous_request_id) {
						Ok(Response::Message(message)) => return Ok(futures::Async::Ready(Some(message))),
						Ok(Response::Continue) => continue_loop = true,
						Ok(Response::NotReady) => (),
						Err(err) => log::warn!("Discarding message that could not be parsed: {}", err),
					}

					if let Some(message) = message {
						// This can happen if the Azure IoT Hub responded to a reported property request that we aren't waiting for
						// because we have since sent a new one
						log::debug!("unconsumed packet {:?}", message);
					}

					if !continue_loop {
						return Ok(futures::Async::NotReady);
					}
				},
			}
		}
	}
}

impl std::fmt::Debug for State {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			State::WaitingForSubscriptions { reset_session } => f.debug_struct("WaitingForSubscriptions").field("reset_session", reset_session).finish(),
			State::Idle => f.debug_struct("Idle").finish(),
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

	/// The server acknowledged a report of the twin state. Contains the version number of the updated section.
	ReportedTwinState(usize),

	/// The full twin state, as currently stored in the Azure IoT Hub.
	TwinInitial(TwinState),

	/// A patch to the twin state that should be applied to the current state to get the new state.
	TwinPatch(TwinProperties),
}

#[derive(Debug)]
enum MessageParseError {
	C2DMessageMissingRequiredProperty(&'static str),
	IotHubStatus(crate::Status),
	Json(serde_json::Error),
	MissingResponseVersion,
	ParseResponseVersion(String, std::num::ParseIntError),
	ParseResponseRequestId(String, std::num::ParseIntError),
	ParseResponseStatus(String, std::num::ParseIntError),
	UnrecognizedMessage,
}

impl std::fmt::Display for MessageParseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			MessageParseError::C2DMessageMissingRequiredProperty(property) => write!(f, "C2D message does not contain required property {}", property),
			MessageParseError::IotHubStatus(status) => write!(f, "IoT Hub failed request with status {}", status),
			MessageParseError::Json(err) => write!(f, "could not parse payload as valid JSON: {}", err),
			MessageParseError::MissingResponseVersion => write!(f, r#"could not parse response: missing "$version" property"#),
			MessageParseError::ParseResponseVersion(version, err) => write!(f, "could not parse {:?} as version number of reported twin state: {}", version, err),
			MessageParseError::ParseResponseRequestId(request_id, err) => write!(f, "could not parse {:?} as request ID: {}", request_id, err),
			MessageParseError::ParseResponseStatus(status, err) => write!(f, "could not parse {:?} as status code: {}", status, err),
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
			MessageParseError::MissingResponseVersion => None,
			MessageParseError::ParseResponseVersion(_, err) => Some(err),
			MessageParseError::ParseResponseRequestId(_, err) => Some(err),
			MessageParseError::ParseResponseStatus(_, err) => Some(err),
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

#[derive(Debug)]
enum InternalMessage {
	CloudToDevice(CloudToDeviceMessage),

	DirectMethod {
		name: String,
		payload: serde_json::Value,
		request_id: String,
	},

	Response {
		status: crate::Status,
		request_id: u8,
		version: Option<usize>,
		payload: Vec<u8>,
	},

	TwinPatch(TwinProperties),
}

impl InternalMessage {
	fn parse(
		publication: mqtt::ReceivedPublication,
		c2d_prefix: &str,
	) -> Result<Self, MessageParseError> {
		if let Some(captures) = RESPONSE_REGEX.captures(&publication.topic_name) {
			let status = &captures[1];
			let status: crate::Status = status.parse().map_err(|err| MessageParseError::ParseResponseStatus(status.to_string(), err))?;

			let query_string = &captures[2];

			let mut request_id = None;
			let mut version = None;

			for (key, value) in url::form_urlencoded::parse(query_string.as_bytes()) {
				match &*key {
					"$rid" => request_id = Some(value.parse().map_err(|err| MessageParseError::ParseResponseRequestId(status.to_string(), err))?),
					"$version" => version = Some(value.parse().map_err(|err| MessageParseError::ParseResponseVersion(status.to_string(), err))?),
					_ => (),
				}
			}

			let request_id = request_id.ok_or(MessageParseError::UnrecognizedMessage)?;

			Ok(InternalMessage::Response { status, request_id, version, payload: publication.payload })
		}
		else if publication.topic_name.starts_with("$iothub/twin/PATCH/properties/desired/") {
			let twin_properties = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;
			Ok(InternalMessage::TwinPatch(twin_properties))
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
			Ok(InternalMessage::CloudToDevice(CloudToDeviceMessage {
				system_properties,
				properties,
				data: publication.payload,
			}))
		}
		else if let Some(captures) = DIRECT_METHOD_REGEX.captures(&publication.topic_name) {
			let name = captures[1].to_string();
			let payload = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;
			let request_id = captures[2].to_string();

			Ok(InternalMessage::DirectMethod {
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

impl From<InternalMessage> for Message {
	fn from(message: InternalMessage) -> Self {
		match message {
			InternalMessage::CloudToDevice(message) => Message::CloudToDevice(message),
			InternalMessage::DirectMethod { name, payload, request_id } => Message::DirectMethod { name, payload, request_id },
			InternalMessage::Response { .. } => panic!("InternalMessage::Response cannot be directly converted to Message"),
			InternalMessage::TwinPatch(properties) => Message::TwinPatch(properties),
		}
	}
}

enum Response {
	Message(crate::device::Message),
	Continue,
	NotReady,
}

lazy_static::lazy_static! {
	static ref RESPONSE_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/twin/res/(\d+)/\?(.+)$").expect("could not compile regex");
	static ref DIRECT_METHOD_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/methods/POST/([^/]+)/\?\$rid=(.+)$").expect("could not compile regex");
}
