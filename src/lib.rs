/*!
 * This crate contains types related to the Azure IoT MQTT server.
 */

#![deny(rust_2018_idioms, warnings)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(
	clippy::default_trait_access,
	clippy::doc_markdown,
	clippy::large_enum_variant,
	clippy::pub_enum_variant_names,
	clippy::similar_names,
	clippy::single_match_else,
	clippy::stutter,
	clippy::too_many_arguments,
	clippy::use_self,
)]

use futures::{ Future, Stream };

/// Error type
#[derive(Debug)]
pub enum Error {
	MqttClient(mqtt::Error),
	ResolveIotHubHostname(Option<std::io::Error>),
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::MqttClient(err) => write!(f, "MQTT client encountered an error: {}", err),
			Error::ResolveIotHubHostname(Some(err)) => write!(f, "could not resolve Azure IoT Hub hostname: {}", err),
			Error::ResolveIotHubHostname(None) => write!(f, "could not resolve Azure IoT Hub hostname: no addresses found"),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::MqttClient(err) => Some(err),
			Error::ResolveIotHubHostname(Some(err)) => Some(err),
			Error::ResolveIotHubHostname(None) => None,
		}
	}
}

/// A client for getting device twin properties.
///
/// A `TwinClient` is a [`Stream`] of [`TwinMessage`]s.
///
/// It automatically reconnects if the connection to the server is broken. Each reconnection will yield one [`TwinMessage::Initial`] message
/// and zero or more [`TwinMessage::Patch`] messages.
pub struct TwinClient {
	inner: mqtt::Client<StreamSource>,
	publish_handle: mqtt::PublishHandle,
	update_subscription_handle: mqtt::UpdateSubscriptionHandle,

	max_back_off: std::time::Duration,
	current_back_off: std::time::Duration,

	state: State,
}

enum State {
	BeginSubscription,
	WaitingForSubscription(Box<dyn Future<Item = ((), ()), Error = mqtt::UpdateSubscriptionError> + Send>),
	BeginBackOff,
	EndBackOff(tokio::timer::Delay),
	BeginSendingGetRequest,
	EndSendingGetRequest(Box<dyn Future<Item = (), Error = mqtt::PublishError> + Send>),
	WaitingForGetResponse,
	HaveGetResponse,
}

impl TwinClient {
	/// Creates a new `TwinClient`
	///
	/// * `iothub_hostname`
	///
	///     The hostname of the Azure IoT Hub. Eg "foo.azure-devices.net"
	///
	/// * `device_id`
	///
	///     The ID of the device.
	///
	/// * `sas_token`
	///
	///     The SAS token to use for authorizing this client with the Azure IoT Hub.
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
		sas_token: String,

		will: Option<Vec<u8>>,

		max_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Result<Self, Error> {
		let iothub_host =
			std::net::ToSocketAddrs::to_socket_addrs(&(&*iothub_hostname, 8883))
			.map_err(|err| Error::ResolveIotHubHostname(Some(err)))?
			.next()
			.ok_or(Error::ResolveIotHubHostname(None))?;

		let username = format!("{}/{}/api-version=2018-06-30", iothub_hostname, device_id);
		let password = sas_token;

		let will = mqtt::proto::Publication {
			topic_name: format!("devices/{}/messages/events/", device_id),
			qos: mqtt::proto::QoS::AtMostOnce,
			retain: false,
			payload: will.unwrap_or_else(Vec::new),
		};
		let client_id = device_id;

		let inner = mqtt::Client::new(
			Some(client_id),
			Some(username),
			Some(password),
			Some(will),
			StreamSource {
				timeout: 2 * keep_alive,
				iothub_hostname,
				iothub_host,
			},
			max_back_off,
			keep_alive,
		);

		let publish_handle = match inner.publish_handle() {
			Ok(publish_handle) => publish_handle,

			// This can only happen if `inner` has been shut down
			Err(mqtt::PublishError::ClientDoesNotExist) => unreachable!(),
		};

		let update_subscription_handle = match inner.update_subscription_handle() {
			Ok(update_subscription_handle) => update_subscription_handle,

			// This can only happen if `inner` has been shut down
			Err(mqtt::UpdateSubscriptionError::ClientDoesNotExist) => unreachable!(),
		};

		Ok(TwinClient {
			inner,
			publish_handle,
			update_subscription_handle,

			max_back_off,
			current_back_off: std::time::Duration::from_secs(0),

			state: State::BeginSubscription,
		})
	}

	/// Gets a reference to the inner `mqtt::Client`
	pub fn inner(&self) -> &mqtt::Client<StreamSource> {
		&self.inner
	}
}

impl Stream for TwinClient {
	type Item = TwinMessage;
	type Error = Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			log::trace!("    {:?}", self.state);

			match &mut self.state {
				State::BeginSubscription => {
					let response_subscription = self.update_subscription_handle.subscribe(mqtt::proto::SubscribeTo {
						topic_filter: "$iothub/twin/res/#".to_string(),
						qos: mqtt::proto::QoS::AtMostOnce,
					});
					let patches_subscription = self.update_subscription_handle.subscribe(mqtt::proto::SubscribeTo {
						topic_filter: "$iothub/twin/PATCH/properties/desired/#".to_string(),
						qos: mqtt::proto::QoS::AtMostOnce,
					});
					self.state = State::WaitingForSubscription(Box::new(response_subscription.join(patches_subscription)));
				},

				State::WaitingForSubscription(f) => match f.poll() {
					Ok(futures::Async::Ready(((), ()))) => self.state = State::BeginSendingGetRequest,

					Ok(futures::Async::NotReady) => match self.inner.poll().map_err(Error::MqttClient)? {
						futures::Async::Ready(Some(_)) => (), // Ignore all events until subscription succeeds
						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),
						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},

					// The subscription can only fail if `self.inner` has shut down, which is not the case here
					Err(mqtt::UpdateSubscriptionError::ClientDoesNotExist) => unreachable!(),
				},

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
					futures::Async::NotReady => match self.inner.poll().map_err(Error::MqttClient)? {
						futures::Async::Ready(Some(_)) => (), // Ignore all events
						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),
						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},
				},

				State::BeginSendingGetRequest =>
					self.state = State::EndSendingGetRequest(Box::new(self.publish_handle.publish(mqtt::proto::Publication {
						topic_name: format!("$iothub/twin/GET/?$rid={}", 1),
						qos: mqtt::proto::QoS::AtMostOnce,
						retain: false,
						payload: vec![],
					}))),

				State::EndSendingGetRequest(f) => match f.poll() {
					Ok(futures::Async::Ready(())) => self.state = State::WaitingForGetResponse,

					Ok(futures::Async::NotReady) => match self.inner.poll().map_err(Error::MqttClient)? {
						futures::Async::Ready(Some(_)) => (), // Ignore all events until publish succeeds
						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),
						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},

					// The publish can only fail if `self.inner` has shut down, which is not the case here
					Err(mqtt::PublishError::ClientDoesNotExist) => unreachable!(),
				},

				State::WaitingForGetResponse => match self.inner.poll().map_err(Error::MqttClient)? {
					futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) =>
						self.state = State::BeginSendingGetRequest,

					futures::Async::Ready(Some(mqtt::Event::Publication(publication))) =>
						if let Some(captures) = RESPONSE_REGEX.captures(&publication.topic_name) {
							let status: usize = match captures[1].parse() {
								Ok(status) => status,
								Err(err) => {
									log::warn!("could not parse status from publication topic {:?}: {}", publication.topic_name, err);
									self.state = State::BeginBackOff;
									continue;
								},
							};

							match status {
								200 => {
									self.current_back_off = std::time::Duration::from_secs(0);

									let twin_state = match serde_json::from_slice(&publication.payload) {
										Ok(twin_state) => twin_state,
										Err(err) => {
											log::warn!("could not deserialize GET response: {}", err);
											self.state = State::BeginBackOff;
											continue;
										},
									};

									self.state = State::HaveGetResponse;
									return Ok(futures::Async::Ready(Some(TwinMessage::Initial(twin_state))));
								},

								status => {
									log::warn!("IoT Hub returned status {} in response to initial GET request.", status);
									self.state = State::BeginBackOff;
								},
							}
						},

					futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

					futures::Async::NotReady => return Ok(futures::Async::NotReady),
				},

				State::HaveGetResponse => match self.inner.poll().map_err(Error::MqttClient)? {
					futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => self.state = State::BeginSendingGetRequest,

					futures::Async::Ready(Some(mqtt::Event::Publication(publication))) => {
						let twin_properties = match serde_json::from_slice(&publication.payload) {
							Ok(twin_properties) => twin_properties,
							Err(err) => {
								log::warn!("could not deserialize PATCH response: {}", err);
								self.state = State::BeginBackOff;
								continue;
							},
						};

						return Ok(futures::Async::Ready(Some(TwinMessage::Patch(twin_properties))));
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
			State::BeginSubscription => f.debug_struct("BeginSubscription").finish(),
			State::WaitingForSubscription(_) => f.debug_struct("WaitingForSubscription").finish(),
			State::BeginBackOff => f.debug_struct("BeginBackOff").finish(),
			State::EndBackOff(_) => f.debug_struct("EndBackOff").finish(),
			State::BeginSendingGetRequest => f.debug_struct("BeginSendingGetRequest").finish(),
			State::EndSendingGetRequest(_) => f.debug_struct("EndSendingGetRequest").finish(),
			State::WaitingForGetResponse => f.debug_struct("WaitingForGetResponse").finish(),
			State::HaveGetResponse => f.debug_struct("HaveGetResponse").finish(),
		}
	}
}

/// A [`mqtt::IoSource`] implementation used by [`TwinClient`]
pub struct StreamSource {
	timeout: std::time::Duration,
	iothub_hostname: String,
	iothub_host: std::net::SocketAddr,
}

impl mqtt::IoSource for StreamSource {
	type Io = tokio_tls::TlsStream<tokio_io_timeout::TimeoutStream<tokio::net::TcpStream>>;
	type Future = Box<dyn Future<Item = Self::Io, Error = std::io::Error> + Send>;

	fn connect(&mut self) -> Self::Future {
		let iothub_hostname = self.iothub_hostname.clone();
		let timeout = self.timeout;

		Box::new(
			tokio::net::TcpStream::connect(&self.iothub_host)
			.and_then(move |stream| {
				let mut stream = tokio_io_timeout::TimeoutStream::new(stream);
				stream.set_read_timeout(Some(timeout));
				let connector = native_tls::TlsConnector::new().unwrap();
				let connector: tokio_tls::TlsConnector = connector.into();
				connector.connect(&iothub_hostname, stream)
				.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
			}))
	}
}

/// A message generated by a [`TwinClient`]
#[derive(Debug)]
pub enum TwinMessage {
	/// The full twin state, as currently stored in the Azure IoT Hub.
	Initial(TwinState),

	/// A patch to the twin state that should be applied to the current state to get the new state.
	Patch(TwinProperties),
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

lazy_static::lazy_static! {
	static ref RESPONSE_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/twin/res/(\d+)/\?\$rid=1$").expect("could not compile regex");
}
