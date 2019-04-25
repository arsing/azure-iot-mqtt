//! This module contains the I/O types used by the clients.

use futures::Future;

/// A [`mqtt::IoSource`] implementation used by the clients.
pub struct IoSource {
	iothub_hostname: std::sync::Arc<str>,
	iothub_host: std::net::SocketAddr,
	authentication: crate::Authentication,
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

impl IoSource {
	#[allow(clippy::new_ret_no_self)] // Clippy bug
	pub(crate) fn new(
		iothub_hostname: std::sync::Arc<str>,
		authentication: crate::Authentication,
		timeout: std::time::Duration,
		transport: crate::Transport,
	) -> Result<Self, crate::CreateClientError> {
		let port = match transport {
			crate::Transport::Tcp => 8883,
			crate::Transport::WebSocket => 443,
		};

		let iothub_host =
			std::net::ToSocketAddrs::to_socket_addrs(&(&*iothub_hostname, port))
			.map_err(|err| crate::CreateClientError::ResolveIotHubHostname(Some(err)))?
			.next()
			.ok_or(crate::CreateClientError::ResolveIotHubHostname(None))?;

		let extra = match transport {
			crate::Transport::Tcp => crate::io::IoSourceExtra::Raw,

			crate::Transport::WebSocket => {
				let url = match format!("ws://{}/$iothub/websocket", iothub_hostname).parse() {
					Ok(url) => url,
					Err(err) => return Err(crate::CreateClientError::WebSocketUrl(err)),
				};

				crate::io::IoSourceExtra::WebSocket {
					url,
				}
			},
		};

		Ok(IoSource {
			iothub_hostname,
			iothub_host,
			authentication,
			timeout,
			extra,
		})
	}
}

url::define_encode_set! {
    pub IOTHUB_ENCODE_SET = [url::percent_encoding::PATH_SEGMENT_ENCODE_SET] | { '=' }
}

impl mqtt::IoSource for IoSource {
	type Io = Io<tokio_tls::TlsStream<tokio_io_timeout::TimeoutStream<tokio_tcp::TcpStream>>>;
	type Future = Box<dyn Future<Item = (Self::Io, Option<String>), Error = std::io::Error> + Send>;

	fn connect(&mut self) -> Self::Future {
		#[allow(clippy::identity_op)]
		const DEFAULT_MAX_TOKEN_VALID_DURATION: std::time::Duration = std::time::Duration::from_secs(1 * 60 * 60);

		let iothub_hostname = self.iothub_hostname.clone();
		let timeout = self.timeout;
		let extra = self.extra.clone();

		let authentication = match &self.authentication {
			crate::Authentication::SasKey { device_id, key, max_token_valid_duration, server_root_certificate } =>
				match prepare_sas_token_request(&*iothub_hostname, device_id, None, *max_token_valid_duration) {
					Ok((signature_data, make_sas_token)) => {
						use hmac::Mac;

						let mut mac = hmac::Hmac::<sha2::Sha256>::new_varkey(key).expect("HMAC can have invalid key length");
						mac.input(signature_data.as_bytes());
						let signature = mac.result().code();
						let signature = base64::encode(signature.as_slice());

						let sas_token = make_sas_token(&signature);

						futures::future::Either::A(futures::future::ok((Some(sas_token), None, server_root_certificate.clone())))
					},

					Err(err) => futures::future::Either::A(futures::future::err(err)),
				},

			crate::Authentication::SasToken { token, server_root_certificate } =>
				futures::future::Either::A(futures::future::ok((Some(token.to_owned()), None, server_root_certificate.clone()))),

			crate::Authentication::Certificate { der, password, server_root_certificate } =>
				match native_tls::Identity::from_pkcs12(der, password) {
					Ok(identity) => futures::future::Either::A(futures::future::ok((None, Some(identity), server_root_certificate.clone()))),
					Err(err) => futures::future::Either::A(futures::future::err(
						std::io::Error::new(std::io::ErrorKind::Other, format!("could not parse client certificate: {}", err)))),
				},

			crate::Authentication::IotEdge { device_id, module_id, generation_id, iothub_hostname, workload_url } =>
				match crate::iotedge_client::Client::new(workload_url) {
					Ok(iotedge_client) => {
						match prepare_sas_token_request(iothub_hostname, device_id, None, DEFAULT_MAX_TOKEN_VALID_DURATION) {
							Ok((signature_data, make_sas_token)) => {
								let signature = iotedge_client.hmac_sha256(module_id, generation_id, &signature_data);

								let server_root_certificate = iotedge_client.get_server_root_certificate();

								futures::future::Either::B(signature.join(server_root_certificate)
									.then(move |result| match result {
										Ok((signature, server_root_certificate)) => {
											let sas_token = make_sas_token(&signature);
											Ok((Some(sas_token), None, Some(server_root_certificate)))
										},

										Err(err) => Err(std::io::Error::new(std::io::ErrorKind::Other, err)),
									}))
							},

							Err(err) => futures::future::Either::A(futures::future::err(err)),
						}
					},

					Err(err) => futures::future::Either::A(futures::future::err(
						std::io::Error::new(std::io::ErrorKind::Other, format!("could not initialize iotedge client: {}", err)))),
				},
		};

		let stream =
			tokio_timer::Timeout::new(tokio_tcp::TcpStream::connect(&self.iothub_host), timeout)
			.map_err(|err|
				if err.is_inner() {
					err.into_inner().unwrap()
				}
				else if err.is_elapsed() {
					std::io::ErrorKind::TimedOut.into()
				}
				else if err.is_timer() {
					panic!("could not poll connect timer: {}", err);
				}
				else {
					panic!("unreachable error: {}", err);
				});

		Box::new(
			authentication.join(stream)
			.and_then(move |((password, identity, server_root_certificate), stream)| {
				stream.set_nodelay(true)?;

				let mut stream = tokio_io_timeout::TimeoutStream::new(stream);
				stream.set_read_timeout(Some(timeout));

				let mut tls_connector_builder = native_tls::TlsConnector::builder();
				if let Some(identity) = identity {
					tls_connector_builder.identity(identity);
				}
				if let Some(server_root_certificate) = server_root_certificate {
					tls_connector_builder.add_root_certificate(server_root_certificate);
				}
				let connector =
					tls_connector_builder.build()
					.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, format!("could not create TLS connector: {}", err)))?;
				let connector: tokio_tls::TlsConnector = connector.into();

				Ok(
					connector.connect(&iothub_hostname, stream)
					.then(|stream| {
						let stream = stream.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
						Ok((stream, password))
					})
				)
			})
			.flatten()
			.and_then(|(stream, password)| match extra {
				IoSourceExtra::Raw => futures::future::Either::A(futures::future::ok((Io::Raw(stream), password))),

				IoSourceExtra::WebSocket { url } => {
					let request = tungstenite::handshake::client::Request {
						url,
						extra_headers: Some(vec![
							("sec-websocket-protocol".into(), "mqtt".into()),
						]),
					};

					let handshake = tungstenite::ClientHandshake::start(stream, request, None);

					futures::future::Either::B(WsConnect::Handshake(handshake).map(|stream| (Io::WebSocket {
						inner: stream,
						pending_read: std::io::Cursor::new(vec![]),
					}, password)))
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

impl<S> std::io::Read for Io<S> where S: tokio_io::AsyncRead + std::io::Write {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		use tokio_io::AsyncRead;

		match self.poll_read(buf)? {
			futures::Async::Ready(read) => Ok(read),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}
}

impl<S> tokio_io::AsyncRead for Io<S> where S: tokio_io::AsyncRead + std::io::Write {
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

impl<S> std::io::Write for Io<S> where S: std::io::Read + tokio_io::AsyncWrite {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		use tokio_io::AsyncWrite;

		match self.poll_write(buf)? {
			futures::Async::Ready(written) => Ok(written),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		use tokio_io::AsyncWrite;

		match self.poll_flush()? {
			futures::Async::Ready(()) => Ok(()),
			futures::Async::NotReady => Err(std::io::ErrorKind::WouldBlock.into()),
		}
	}
}

impl<S> tokio_io::AsyncWrite for Io<S> where S: std::io::Read + tokio_io::AsyncWrite {
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

fn prepare_sas_token_request(
	iothub_hostname: &str,
	device_id: &str,
	module_id: Option<&str>,
	max_token_valid_duration: std::time::Duration,
) -> std::io::Result<(String, impl FnOnce(&str) -> String)> {
	url::define_encode_set! {
		pub IOTHUB_ENCODE_SET = [url::percent_encoding::PATH_SEGMENT_ENCODE_SET] | { '=' }
	}

	let since_unix_epoch =
		std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
		.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, format!("could not get current time: {}", err)))?;

	let resource_uri =
		if let Some(module_id) = module_id {
			format!("{}/devices/{}/modules/{}", iothub_hostname, device_id, module_id)
		}
		else {
			format!("{}/devices/{}", iothub_hostname, device_id)
		};
	let resource_uri: String = url::percent_encoding::utf8_percent_encode(&resource_uri, IOTHUB_ENCODE_SET).collect();

	let expiry = since_unix_epoch + max_token_valid_duration;
	let expiry = expiry.as_secs().to_string();

	let signature_data = format!("{}\n{}", resource_uri, expiry);

	Ok((signature_data, move |signature: &str| {
		let mut serializer = url::form_urlencoded::Serializer::new(format!("SharedAccessSignature sr={}", resource_uri));
		serializer.append_pair("se", &expiry);
		serializer.append_pair("sig", signature);
		serializer.finish()
	}))
}
