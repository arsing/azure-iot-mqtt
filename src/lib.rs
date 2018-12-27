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
	WebSocketUrl(url::ParseError),
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::MqttClient(err) => write!(f, "MQTT client encountered an error: {}", err),
			Error::ResolveIotHubHostname(Some(err)) => write!(f, "could not resolve Azure IoT Hub hostname: {}", err),
			Error::ResolveIotHubHostname(None) => write!(f, "could not resolve Azure IoT Hub hostname: no addresses found"),
			Error::WebSocketUrl(err) => write!(f, "could not construct a valid URL for the Azure IoT Hub: {}", err),
		}
	}
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::MqttClient(err) => Some(err),
			Error::ResolveIotHubHostname(Some(err)) => Some(err),
			Error::ResolveIotHubHostname(None) => None,
			Error::WebSocketUrl(err) => Some(err),
		}
	}
}

/// A client for the Azure IoT Hub MQTT protocol.
///
/// A `Client` is a [`Stream`] of [`Message`]s. These messages contain twin properties and cloud-to-device communications.
///
/// It automatically reconnects if the connection to the server is broken. Each reconnection will yield one [`Message::TwinInitial`] message,
/// zero or more [`Message::TwinPatch`] messages and zero or more [`Message::CloudToDevice`] messages.
pub struct Client {
	inner: mqtt::Client<IoSource>,
	publish_handle: mqtt::PublishHandle,
	update_subscription_handle: mqtt::UpdateSubscriptionHandle,

	keep_alive: std::time::Duration,
	max_back_off: std::time::Duration,
	current_back_off: std::time::Duration,
	c2d_prefix: String,

	state: State,
}

enum State {
	BeginSubscription,
	WaitingForSubscription(Box<dyn Future<Item = ((), (), ()), Error = mqtt::UpdateSubscriptionError> + Send>),
	BeginBackOff,
	EndBackOff(tokio::timer::Delay),
	BeginSendingGetRequest,
	EndSendingGetRequest(Box<dyn Future<Item = (), Error = mqtt::PublishError> + Send>),
	WaitingForGetResponse(tokio::timer::Delay),
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
		device_id: String,
		sas_token: String,
		transport: Transport,

		will: Option<Vec<u8>>,

		max_back_off: std::time::Duration,
		keep_alive: std::time::Duration,
	) -> Result<Self, Error> {
		let port = match transport {
			Transport::Tcp => 8883,
			Transport::WebSocket => 443,
		};

		let iothub_host =
			std::net::ToSocketAddrs::to_socket_addrs(&(&*iothub_hostname, port))
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

		let c2d_prefix = format!("devices/{}/messages/devicebound/", device_id);

		let client_id = device_id;

		let io_source_extra = match transport {
			Transport::Tcp => IoSourceExtra::Raw,

			Transport::WebSocket => {
				let url = match format!("ws://{}/$iothub/websocket", iothub_hostname).parse() {
					Ok(url) => url,
					Err(err) => return Err(Error::WebSocketUrl(err)),
				};

				IoSourceExtra::WebSocket {
					url,
				}
			},
		};

		let io_source = IoSource {
			iothub_hostname,
			iothub_host,
			timeout: 2 * keep_alive,
			extra: io_source_extra,
		};

		let inner = mqtt::Client::new(
			Some(client_id),
			Some(username),
			Some(password),
			Some(will),
			io_source,
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

		Ok(Client {
			inner,
			publish_handle,
			update_subscription_handle,

			keep_alive,
			max_back_off,
			current_back_off: std::time::Duration::from_secs(0),
			c2d_prefix,

			state: State::BeginSubscription,
		})
	}

	/// Gets a reference to the inner `mqtt::Client`
	pub fn inner(&self) -> &mqtt::Client<IoSource> {
		&self.inner
	}
}

impl Stream for Client {
	type Item = Message;
	type Error = Error;

	fn poll(&mut self) -> futures::Poll<Option<Self::Item>, Self::Error> {
		loop {
			log::trace!("    {:?}", self.state);

			match &mut self.state {
				State::BeginSubscription => {
					let twin_get_subscription = self.update_subscription_handle.subscribe(mqtt::proto::SubscribeTo {
						topic_filter: "$iothub/twin/res/#".to_string(),
						qos: mqtt::proto::QoS::AtMostOnce,
					});
					let twin_patches_subscription = self.update_subscription_handle.subscribe(mqtt::proto::SubscribeTo {
						topic_filter: "$iothub/twin/PATCH/properties/desired/#".to_string(),
						qos: mqtt::proto::QoS::AtMostOnce,
					});
					let c2d_subscription = self.update_subscription_handle.subscribe(mqtt::proto::SubscribeTo {
						topic_filter: format!("{}#", self.c2d_prefix),
						qos: mqtt::proto::QoS::AtLeastOnce,
					});
					self.state = State::WaitingForSubscription(Box::new(twin_get_subscription.join3(twin_patches_subscription, c2d_subscription)));
				},

				State::WaitingForSubscription(f) => match f.poll() {
					Ok(futures::Async::Ready(((), (), ()))) => self.state = State::BeginSendingGetRequest,

					Ok(futures::Async::NotReady) => match self.inner.poll().map_err(Error::MqttClient)? {
						// Inner client will resubscribe as necessary, so there's nothing for this client to do
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) =>
							log::debug!("Discarding PUBLISH packet with topic {:?} because we haven't subscribed yet", publication.topic_name),

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
						// Inner client will resubscribe as necessary, so there's nothing for this client to do
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) =>
							log::debug!("Discarding PUBLISH packet with topic {:?} because we're still backing off before making a GET request", publication.topic_name),

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
					Ok(futures::Async::Ready(())) => {
						let deadline = std::time::Instant::now() + 2 * self.keep_alive;
						self.state = State::WaitingForGetResponse(tokio::timer::Delay::new(deadline));
					},

					Ok(futures::Async::NotReady) => match self.inner.poll().map_err(Error::MqttClient)? {
						// Inner client hasn't sent the GET request yet, so there's nothing for this client to do
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => (),

						futures::Async::Ready(Some(mqtt::Event::Publication(publication))) =>
							log::debug!("Discarding PUBLISH packet with topic {:?} because we haven't sent the GET request yet", publication.topic_name),

						futures::Async::Ready(None) => return Ok(futures::Async::Ready(None)),

						futures::Async::NotReady => return Ok(futures::Async::NotReady),
					},

					// The publish can only fail if `self.inner` has shut down, which is not the case here
					Err(mqtt::PublishError::ClientDoesNotExist) => unreachable!(),
				},

				State::WaitingForGetResponse(timer) => {
					match self.inner.poll().map_err(Error::MqttClient)? {
						futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => {
							log::warn!("Connection reset while waiting for GET response. Resending GET request...");
							self.state = State::BeginSendingGetRequest;
							continue;
						},

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

										let twin_state: TwinState = match serde_json::from_slice(&publication.payload) {
											Ok(twin_state) => twin_state,
											Err(err) => {
												log::warn!("could not deserialize GET response: {}", err);
												self.state = State::BeginBackOff;
												continue;
											},
										};

										self.state = State::HaveGetResponse {
											previous_version: twin_state.desired.version,
										};

										return Ok(futures::Async::Ready(Some(Message::TwinInitial(twin_state))));
									},

									status => {
										log::warn!("IoT Hub returned status {} in response to initial GET request.", status);
										self.state = State::BeginBackOff;
										continue;
									},
								}
							}
							else {
								log::debug!("Discarding PUBLISH packet with topic {:?} because it's not the GET response", publication.topic_name);
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

				State::HaveGetResponse { previous_version } => match self.inner.poll().map_err(Error::MqttClient)? {
					futures::Async::Ready(Some(mqtt::Event::NewConnection { .. })) => {
						log::warn!("Connection reset. Resending GET request...");
						self.state = State::BeginSendingGetRequest;
					},

					futures::Async::Ready(Some(mqtt::Event::Publication(publication))) =>
						if publication.topic_name.starts_with("$iothub/twin/PATCH/properties/desired/") {
							let twin_properties: TwinProperties = match serde_json::from_slice(&publication.payload) {
								Ok(twin_properties) => twin_properties,
								Err(err) => {
									log::warn!("could not deserialize PATCH response: {}", err);
									self.state = State::BeginBackOff;
									continue;
								},
							};

							if twin_properties.version == *previous_version + 1 {
								*previous_version = twin_properties.version;
							}
							else {
								log::warn!("expected PATCH response with version {} but received version {}", *previous_version + 1, twin_properties.version);
								self.state = State::BeginSendingGetRequest;
								continue;
							}

							return Ok(futures::Async::Ready(Some(Message::TwinPatch(twin_properties))));
						}
						else if publication.topic_name.starts_with(&self.c2d_prefix) {
							let mut message_id = None;
							let mut correlation_id = None;
							let mut to = None;
							let mut iothub_ack = None;
							let mut properties: std::collections::HashMap<_, _> = Default::default();

							for (key, value) in url::form_urlencoded::parse(publication.topic_name[self.c2d_prefix.len()..].as_bytes()) {
								if key == "$.mid" {
									message_id = Some(value.into_owned());
								}
								else if key == "$.cid" {
									correlation_id = Some(value.into_owned());
								}
								else if key == "$.to" {
									to = Some(value.into_owned());
								}
								else if key == "iothub-ack" {
									iothub_ack = Some(value.into_owned());
								}
								else {
									properties.insert(key.into_owned(), value.into_owned());
								}
							}

							match (message_id, to, iothub_ack) {
								(Some(message_id), Some(to), Some(iothub_ack)) =>
									return Ok(futures::Async::Ready(Some(Message::CloudToDevice(CloudToDeviceMessage {
										message_id,
										correlation_id,
										to,
										iothub_ack,
										properties,
										data: publication.payload,
									})))),

								(None, _, _) =>
									log::debug!(r#"Discarding PUBLISH packet with topic {:?} because it does not contain the "mid" property"#, publication.topic_name),

								(Some(_), None, _) =>
									log::debug!(r#"Discarding PUBLISH packet with topic {:?} because it does not contain the "to" property"#, publication.topic_name),

								(Some(_), Some(_), None) =>
									log::debug!(r#"Discarding PUBLISH packet with topic {:?} because it does not contain the "iothub-ack" property"#, publication.topic_name),
							}
						}
						else {
							log::debug!("Discarding PUBLISH packet with topic {:?}", publication.topic_name);
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
			State::WaitingForGetResponse(_) => f.debug_struct("WaitingForGetResponse").finish(),
			State::HaveGetResponse { previous_version } => f.debug_struct("HaveGetResponse").field("previous_version", previous_version).finish(),
		}
	}
}

/// A [`mqtt::IoSource`] implementation used by [`Client`]
pub struct IoSource {
	iothub_hostname: String,
	iothub_host: std::net::SocketAddr,
	timeout: std::time::Duration,
	extra: IoSourceExtra,
}

#[derive(Clone, Debug)]
enum IoSourceExtra {
	Raw,

	WebSocket {
		url: url::Url,
	},
}

impl mqtt::IoSource for IoSource {
	type Io = Io<tokio_tls::TlsStream<tokio_io_timeout::TimeoutStream<tokio::net::TcpStream>>>;
	type Future = Box<dyn Future<Item = Self::Io, Error = std::io::Error> + Send>;

	fn connect(&mut self) -> Self::Future {
		let iothub_hostname = self.iothub_hostname.clone();
		let timeout = self.timeout;
		let extra = self.extra.clone();

		Box::new(
			tokio::net::TcpStream::connect(&self.iothub_host)
			.and_then(move |stream| {
				stream.set_nodelay(true)?;

				let mut stream = tokio_io_timeout::TimeoutStream::new(stream);
				stream.set_read_timeout(Some(timeout));

				let connector = native_tls::TlsConnector::new().unwrap();
				let connector: tokio_tls::TlsConnector = connector.into();

				Ok(
					connector.connect(&iothub_hostname, stream)
					.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)))
			})
			.flatten()
			.and_then(move |stream| match extra {
				IoSourceExtra::Raw => futures::future::Either::A(futures::future::ok(Io::Raw(stream))),

				IoSourceExtra::WebSocket { url } => {
					let request = tungstenite::handshake::client::Request {
						url,
						extra_headers: Some(vec![
							("sec-websocket-protocol".into(), "mqtt".into()),
						]),
					};

					let handshake = tungstenite::ClientHandshake::start(stream, request, None);

					futures::future::Either::B(WsConnect::Handshake(handshake).map(|stream| Io::WebSocket {
						inner: stream,
						pending_read: std::io::Cursor::new(vec![]),
					}))
				},
			}))
	}
}

/// The transport to use for the connection to the Azure IoT Hub
#[derive(Clone, Copy, Debug)]
pub enum Transport {
	Tcp,
	WebSocket,
}

/// A message generated by a [`Client`]
#[derive(Debug)]
pub enum Message {
	/// The full twin state, as currently stored in the Azure IoT Hub.
	TwinInitial(TwinState),

	/// A patch to the twin state that should be applied to the current state to get the new state.
	TwinPatch(TwinProperties),

	/// A cloud-to-device message
	CloudToDevice(CloudToDeviceMessage),
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
	pub message_id: String,
	pub correlation_id: Option<String>,
	pub to: String,
	pub iothub_ack: String,
	pub properties: std::collections::HashMap<String, String>,
	pub data: Vec<u8>,
}

lazy_static::lazy_static! {
	static ref RESPONSE_REGEX: regex::Regex = regex::Regex::new(r"^\$iothub/twin/res/(\d+)/\?\$rid=1$").expect("could not compile regex");
}

enum WsConnect<S> where S: std::io::Read + std::io::Write {
	Handshake(tungstenite::handshake::MidHandshake<tungstenite::ClientHandshake<S>>),
	Invalid,
}

impl<S> std::fmt::Debug for WsConnect<S> where S: std::io::Read + std::io::Write {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			WsConnect::Handshake(_) => f.debug_struct("Handshake").finish(),
			WsConnect::Invalid => f.debug_struct("Invalid").finish(),
		}
	}
}

impl<S> Future for WsConnect<S> where S: std::io::Read + std::io::Write {
	type Item = tungstenite::WebSocket<S>;
	type Error = std::io::Error;

	fn poll(&mut self) -> futures::Poll<Self::Item, Self::Error> {
		match std::mem::replace(self, WsConnect::Invalid) {
			WsConnect::Handshake(handshake) => match handshake.handshake() {
				Ok((stream, _)) =>
					Ok(futures::Async::Ready(stream)),

				Err(tungstenite::HandshakeError::Interrupted(handshake)) => {
					*self = WsConnect::Handshake(handshake);
					Ok(futures::Async::NotReady)
				},

				Err(tungstenite::HandshakeError::Failure(err)) =>
					poll_from_tungstenite_error(err),
			},

			WsConnect::Invalid =>
				panic!("future polled after completion"),
		}
	}
}

/// A wrapper around an inner I/O object
pub enum Io<S> {
	Raw(S),

	WebSocket {
		inner: tungstenite::WebSocket<S>,
		pending_read: std::io::Cursor<Vec<u8>>,
	},
}

impl<S> std::io::Read for Io<S> where S: tokio::io::AsyncRead + std::io::Write {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		use tokio::io::AsyncRead;

		match self.poll_read(buf)? {
			futures::Async::Ready(read) => Ok(read),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}
}

impl<S> tokio::io::AsyncRead for Io<S> where S: tokio::io::AsyncRead + std::io::Write {
	fn poll_read(&mut self, buf: &mut [u8]) -> futures::Poll<usize, std::io::Error> {
		use std::io::Read;

		let (inner, pending_read) = match self {
			Io::Raw(stream) => return stream.poll_read(buf),
			Io::WebSocket { inner, pending_read } => (inner, pending_read),
		};

		if buf.is_empty() {
			return Ok(futures::Async::Ready(0));
		}

		loop {
			if pending_read.position() != pending_read.get_ref().len() as u64 {
				return Ok(futures::Async::Ready(pending_read.read(buf).expect("Cursor::read cannot fail")));
			}

			let message = match inner.read_message() {
				Ok(tungstenite::Message::Binary(b)) => b,

				Ok(message) => {
					log::warn!("ignoring unexpected message: {:?}", message);
					continue;
				},

				Err(err) => return poll_from_tungstenite_error(err),
			};

			*pending_read = std::io::Cursor::new(message);
		}
	}
}

impl<S> std::io::Write for Io<S> where S: std::io::Read + tokio::io::AsyncWrite {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		use tokio::io::AsyncWrite;

		match self.poll_write(buf)? {
			futures::Async::Ready(written) => Ok(written),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		use tokio::io::AsyncWrite;

		match self.poll_flush()? {
			futures::Async::Ready(()) => Ok(()),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}
}

impl<S> tokio::io::AsyncWrite for Io<S> where S: std::io::Read + tokio::io::AsyncWrite {
	fn shutdown(&mut self) -> futures::Poll<(), std::io::Error> {
		let inner = match self {
			Io::Raw(stream) => return stream.shutdown(),
			Io::WebSocket { inner, .. } => inner,
		};

		match inner.close(None) {
			Ok(()) => Ok(futures::Async::Ready(())),
			Err(err) => poll_from_tungstenite_error(err),
		}
	}

	fn poll_write(&mut self, buf: &[u8]) -> futures::Poll<usize, std::io::Error> {
		let inner = match self {
			Io::Raw(stream) => return stream.poll_write(buf),
			Io::WebSocket { inner, .. } => inner,
		};

		if buf.is_empty() {
			return Ok(futures::Async::Ready(0));
		}

		let message = tungstenite::Message::Binary(buf.to_owned());

		match inner.write_message(message) {
			Ok(()) => Ok(futures::Async::Ready(buf.len())),
			Err(tungstenite::Error::SendQueueFull(_)) => Ok(futures::Async::NotReady), // Hope client calls `poll_flush()` before retrying
			Err(err) => poll_from_tungstenite_error(err),
		}
	}

	fn poll_flush(&mut self) -> futures::Poll<(), std::io::Error> {
		let inner = match self {
			Io::Raw(stream) => return stream.poll_flush(),
			Io::WebSocket { inner, .. } => inner,
		};

		match inner.write_pending() {
			Ok(()) => Ok(futures::Async::Ready(())),
			Err(err) => poll_from_tungstenite_error(err),
		}
	}
}

fn poll_from_tungstenite_error<T>(err: tungstenite::Error) -> futures::Poll<T, std::io::Error> {
	match err {
		tungstenite::Error::Io(ref err) if err.kind() == std::io::ErrorKind::WouldBlock => Ok(futures::Async::NotReady),
		tungstenite::Error::Io(err) => Err(err),
		err => Err(std::io::Error::new(std::io::ErrorKind::Other, err)),
	}
}
