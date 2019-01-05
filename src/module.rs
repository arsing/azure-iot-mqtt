//! This module contains the module client and module message types.

use futures::{ Future, Sink, Stream };

/// A client for the Azure IoT Hub MQTT protocol. This client receives module-level messages.
///
/// A `Client` is a [`Stream`] of [`Message`]s. These messages contain module method requests.
///
/// It automatically reconnects if the connection to the server is broken.
pub struct Client {
	inner: mqtt::Client<crate::IoSource>,

	module_method_response_send: futures::sync::mpsc::Sender<ModuleMethodResponse>,
	module_method_response_recv: futures::sync::mpsc::Receiver<ModuleMethodResponse>,
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
	/// * `module_id`
	///
	///     The ID of the module.
	///
	/// * `sas_token`
	///
	///     The SAS token to use for authorizing this client with the Azure IoT Hub.
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
		device_id: &str,
		module_id: &str,
		sas_token: String,
		transport: crate::Transport,

		will: Option<Vec<u8>>,

		max_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Result<Self, crate::Error> {
		let iothub_hostname = iothub_hostname.into();

		let will = will.map(|payload| mqtt::proto::Publication {
			topic_name: format!("devices/{}/modules/{}/messages/events/", device_id, module_id),
			qos: mqtt::proto::QoS::AtMostOnce,
			retain: false,
			payload,
		});

		let username = format!("{}/{}/{}/?api-version=2018-06-30", iothub_hostname, device_id, module_id);
		let password = sas_token;

		let client_id = format!("{}/{}", device_id, module_id);

		let io_source = crate::IoSource::new(
			iothub_hostname,
			2 * keep_alive,
			transport,
		)?;

		let mut inner = mqtt::Client::new(
			Some(client_id),
			Some(username),
			Some(password),
			will,
			io_source,
			max_back_off,
			keep_alive,
		);

		let module_method_subscription = inner.subscribe(mqtt::proto::SubscribeTo {
			topic_filter: "$iothub/methods/POST/#".to_string(),
			qos: mqtt::proto::QoS::AtLeastOnce,
		});

		match module_method_subscription {
			Ok(()) => (),

			// The subscription can only fail if `self.inner` has shut down, which is not the case here
			Err(mqtt::UpdateSubscriptionError::ClientDoesNotExist) => unreachable!(),
		}

		let (module_method_response_send, module_method_response_recv) = futures::sync::mpsc::channel(0);

		Ok(Client {
			inner,

			module_method_response_send,
			module_method_response_recv,
		})
	}

	/// Gets a reference to the inner `mqtt::Client`
	pub fn inner(&self) -> &mqtt::Client<crate::IoSource> {
		&self.inner
	}

	/// Returns a handle that can be used to respond to module methods
	pub fn module_method_response_handle(&self) -> ModuleMethodResponseHandle {
		ModuleMethodResponseHandle(self.module_method_response_send.clone())
	}
}

impl Stream for Client {
	type Item = Message;
	type Error = crate::Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			while let futures::Async::Ready(Some(module_method_response)) = self.module_method_response_recv.poll().expect("Receiver::poll cannot fail") {
				let ModuleMethodResponse { request_id, status, payload, ack_sender } = module_method_response;
				let payload = serde_json::to_vec(&payload).expect("cannot fail to serialize serde_json::Value");
				let publication = mqtt::proto::Publication {
					topic_name: format!("$iothub/methods/res/{}/?$rid={}", status, request_id),
					qos: mqtt::proto::QoS::AtLeastOnce,
					retain: false,
					payload,
				};

				if ack_sender.send(Box::new(self.inner.publish(publication))).is_err() {
					log::debug!("could not send ack for module method response because ack receiver has been dropped");
				}
			}

			match self.inner.poll().map_err(crate::Error::MqttClient)? {
				futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) =>
					log::debug!("new connection established"),

				futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => match Message::parse(&publication) {
					Ok(message @ Message::ModuleMethod { .. }) => return Ok(futures::Async::Ready(Some(message))),

					Err(err) =>
						log::warn!("Discarding message that could not be parsed: {}", err),
				},

				futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

				futures::Async::NotReady => return Ok(futures::Async::NotReady),
			}
		}
	}
}

/// A message generated by a [`Client`]
#[derive(Debug)]
pub enum Message {
	/// A module method invocation
	ModuleMethod {
		name: String,
		payload: serde_json::Value,
		request_id: String,
	},
}

impl Message {
	fn parse(publication: &mqtt::ReceivedPublication) -> Result<Self, MessageParseError> {
		if let Some(captures) = MODULE_METHOD_REGEX.captures(&publication.topic_name) {
			let name = captures[1].to_string();
			let payload = serde_json::from_slice(&publication.payload).map_err(MessageParseError::Json)?;
			let request_id = captures[2].to_string();

			Ok(Message::ModuleMethod {
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
	Json(serde_json::Error),
	UnrecognizedMessage,
}

impl std::fmt::Display for MessageParseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			MessageParseError::Json(err) => write!(f, "could not parse payload as valid JSON: {}", err),
			MessageParseError::UnrecognizedMessage => write!(f, "message could not be recognized"),
		}
	}
}

impl std::error::Error for MessageParseError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		#[allow(clippy::match_same_arms)]
		match self {
			MessageParseError::Json(err) => Some(err),
			MessageParseError::UnrecognizedMessage => None,
		}
	}
}

/// Used to respond to module methods
pub struct ModuleMethodResponseHandle(futures::sync::mpsc::Sender<ModuleMethodResponse>);

impl ModuleMethodResponseHandle {
	/// Send a module method response with the given parameters
	pub fn respond(&self, request_id: String, status: crate::Status, payload: serde_json::Value) -> impl Future<Item = (), Error = ModuleMethodResponseError> {
		let (ack_sender, ack_receiver) = futures::sync::oneshot::channel();
		let ack_receiver = ack_receiver.map_err(|_| ModuleMethodResponseError::ClientDoesNotExist);

		self.0.clone()
			.send(ModuleMethodResponse {
				request_id,
				status,
				payload,
				ack_sender,
			})
			.then(|result| match result {
				Ok(_) => Ok(()),
				Err(_) => Err(ModuleMethodResponseError::ClientDoesNotExist),
			})
			.and_then(|()| ack_receiver)
			.and_then(|publish| publish.map_err(|_| ModuleMethodResponseError::ClientDoesNotExist))
	}
}

#[derive(Debug)]
pub enum ModuleMethodResponseError {
	ClientDoesNotExist,
}

impl std::fmt::Display for ModuleMethodResponseError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ModuleMethodResponseError::ClientDoesNotExist => write!(f, "client does not exist"),
		}
	}
}

impl std::error::Error for ModuleMethodResponseError {
}

struct ModuleMethodResponse {
	request_id: String,
	status: crate::Status,
	payload: serde_json::Value,
	ack_sender: futures::sync::oneshot::Sender<Box<dyn Future<Item = (), Error = mqtt::PublishError> + Send>>,
}

lazy_static::lazy_static! {
	static ref MODULE_METHOD_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/methods/POST/([^/]+)/\?\$rid=(.+)$").expect("could not compile regex");
}