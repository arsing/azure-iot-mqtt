use futures::{Future, Stream};

pub(crate) struct Client {
	scheme: Scheme,
	base: String,
	inner: hyper::Client<Connector>,
}

impl Client {
	pub(crate) fn new(workload_url: &url::Url) -> Result<Self, Error> {
		let (scheme, base, connector) =
			match workload_url.scheme() {
				"http" => (Scheme::Http, workload_url.to_string(), Connector::Http(hyper::client::HttpConnector::new(4))),
				"unix" =>
					if cfg!(windows) {
						// We get better handling of Windows file syntax if we parse a
						// unix:// URL as a file:// URL. Specifically:
						// - On Unix, `Url::parse("unix:///path")?.to_file_path()` succeeds and
						//   returns "/path".
						// - On Windows, `Url::parse("unix:///C:/path")?.to_file_path()` fails
						//   with Err(()).
						// - On Windows, `Url::parse("file:///C:/path")?.to_file_path()` succeeds
						//   and returns "C:\\path".
						let mut workload_url = workload_url.to_string();
						workload_url.replace_range(..4, "file");
						let workload_url = url::Url::parse(&workload_url).map_err(Error::ParseWorkloadUrl)?;
						let base = workload_url.to_file_path().map_err(|()| Error::ParseWorkloadUrlUnixFilePath)?;
						let base = base.to_str().ok_or_else(|| Error::ParseWorkloadUrlUnixFilePath)?;
						(Scheme::Unix, base.to_owned(), Connector::Unix(hyperlocal::UnixConnector::new()))
					}
					else {
						(Scheme::Unix, workload_url.path().to_owned(), Connector::Unix(hyperlocal::UnixConnector::new()))
					},
				scheme => return Err(Error::UnrecognizedWorkloadUrlScheme(scheme.to_owned())),
			};

		let inner = hyper::Client::builder().build(connector);

		Ok(Client {
			scheme,
			base,
			inner,
		})
	}

	pub(crate) fn get_server_root_certificate(&self) -> impl Future<Item = native_tls::Certificate, Error = Error> {
		let request =
			make_hyper_uri(self.scheme, &*self.base, "/trust-bundle?api-version=2019-01-30")
			.map_err(|err| Error::GetServerRootCertificate(ApiErrorReason::ConstructRequestUrl(err)))
			.and_then(|url|
				http::Request::get(url).body(Default::default())
					.map_err(|err| Error::GetServerRootCertificate(ApiErrorReason::ConstructRequest(err))));
		let request = match request {
			Ok(request) => request,
			Err(err) => return futures::future::Either::A(futures::future::err(err)),
		};

		let response =
			self.inner.request(request)
			.then(|response| match response {
				Ok(response) => {
					let (response_parts, response_body) = response.into_parts();
					let response =
						response_body
						.concat2()
						.then(move |response| match response {
							Ok(response) => Ok((response_parts.status, response)),
							Err(err) => Err(Error::GetServerRootCertificate(ApiErrorReason::ReadResponse(err))),
						});
					Ok(response)
				},

				Err(err) => Err(Error::GetServerRootCertificate(ApiErrorReason::ExecuteRequest(err))),
			})
			.flatten()
			.and_then(|(status, response)| {
				if status != http::StatusCode::OK {
					return Err(Error::GetServerRootCertificate(ApiErrorReason::UnsuccessfulResponse(status)));
				}

				serde_json::from_slice(&*response).map_err(|err| Error::GetServerRootCertificate(ApiErrorReason::ParseResponseBody(Box::new(err))))
			})
			.and_then(|TrustBundleResponse { certificate }|
				native_tls::Certificate::from_pem(certificate.as_bytes())
				.map_err(|err| Error::GetServerRootCertificate(ApiErrorReason::ParseResponseBody(Box::new(err)))));

		futures::future::Either::B(response)
	}

	pub(crate) fn hmac_sha256(&self, module_id: &str, generation_id: &str, data: &str) -> impl Future<Item = String, Error = Error> {
		let data = base64::encode(data.as_bytes());

		let request =
			make_hyper_uri(self.scheme, &*self.base, &format!("/modules/{}/genid/{}/sign?api-version=2019-01-30", module_id, generation_id))
			.map_err(|err| Error::SignSasToken(ApiErrorReason::ConstructRequestUrl(err)))
			.and_then(|url| {
				let sign_request = SignRequest {
					key_id: "primary",
					algorithm: "HMACSHA256",
					data: &data,
				};

				match serde_json::to_vec(&sign_request) {
					Ok(body) => Ok((url, body)),
					Err(err) => Err(Error::SignSasToken(ApiErrorReason::SerializeRequestBody(err))),
				}
			})
			.and_then(|(url, body)|
				http::Request::post(url).body(body.into())
					.map_err(|err| Error::SignSasToken(ApiErrorReason::ConstructRequest(err))));
		let request = match request {
			Ok(request) => request,
			Err(err) => return futures::future::Either::A(futures::future::err(err)),
		};

		let response =
			self.inner.request(request)
			.then(|response| match response {
				Ok(response) => {
					let (response_parts, response_body) = response.into_parts();
					let response =
						response_body
						.concat2()
						.then(move |response| match response {
							Ok(response) => Ok((response_parts.status, response)),
							Err(err) => Err(Error::SignSasToken(ApiErrorReason::ReadResponse(err))),
						});
					Ok(response)
				},

				Err(err) => Err(Error::SignSasToken(ApiErrorReason::ExecuteRequest(err))),
			})
			.flatten()
			.and_then(|(status, response)| {
				if status != http::StatusCode::OK {
					return Err(Error::SignSasToken(ApiErrorReason::UnsuccessfulResponse(status)));
				}

				serde_json::from_slice(&*response).map_err(|err| Error::SignSasToken(ApiErrorReason::ParseResponseBody(Box::new(err))))
			})
			.map(|SignResponse { digest }| digest);

		futures::future::Either::B(response)
	}
}

#[derive(Clone, Copy, Debug)]
enum Scheme {
	Http,
	Unix,
}

fn make_hyper_uri(scheme: Scheme, base: &str, path: &str) -> Result<hyper::Uri, Box<dyn std::error::Error + Send + Sync>> {
	match scheme {
		Scheme::Http => {
			let base = url::Url::parse(base)?;
			let url = base.join(path)?;
			let url = url.as_str().parse()?;
			Ok(url)
		},

		Scheme::Unix => Ok(hyperlocal::Uri::new(base, path).into()),
	}
}

enum Connector {
	Http(hyper::client::HttpConnector),
	Unix(hyperlocal::UnixConnector),
}

impl hyper::client::connect::Connect for Connector {
	type Transport = Transport;
	type Error = std::io::Error;
	type Future = Box<dyn Future<Item = (Self::Transport, hyper::client::connect::Connected), Error = Self::Error> + Send>;

	fn connect(&self, dst: hyper::client::connect::Destination) -> Self::Future {
		match self {
			Connector::Http(connector) => Box::new(
				connector.connect(dst)
				.map(|(transport, connected)| (Transport::Http(transport), connected))) as Self::Future,
			Connector::Unix(connector) => Box::new(
				connector.connect(dst)
				.map(|(transport, connected)| (Transport::Unix(transport), connected))),
		}
	}
}

enum Transport {
	Http(<hyper::client::HttpConnector as hyper::client::connect::Connect>::Transport),
	Unix(<hyperlocal::UnixConnector as hyper::client::connect::Connect>::Transport),
}

impl std::io::Read for Transport {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		match self {
			Transport::Http(transport) => transport.read(buf),
			Transport::Unix(transport) => transport.read(buf),
		}
	}
}

impl std::io::Write for Transport {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		match self {
			Transport::Http(transport) => transport.write(buf),
			Transport::Unix(transport) => transport.write(buf),
		}
	}

	fn flush(&mut self) -> std::io::Result<()> {
		match self {
			Transport::Http(transport) => transport.flush(),
			Transport::Unix(transport) => transport.flush(),
		}
	}
}

impl tokio_io::AsyncRead for Transport {
	unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
		match self {
			Transport::Http(transport) => transport.prepare_uninitialized_buffer(buf),
			Transport::Unix(transport) => transport.prepare_uninitialized_buffer(buf),
		}
	}
}

impl tokio_io::AsyncWrite for Transport {
	fn shutdown(&mut self) -> futures::Poll<(), std::io::Error> {
		match self {
			Transport::Http(transport) => transport.shutdown(),
			Transport::Unix(transport) => transport.shutdown(),
		}
	}
}

#[derive(serde_derive::Deserialize)]
struct TrustBundleResponse {
	certificate: String,
}

#[derive(serde_derive::Serialize)]
struct SignRequest<'a> {
	#[serde(rename = "keyId")]
	key_id: &'static str,
	#[serde(rename = "algo")]
	algorithm: &'static str,
	data: &'a str,
}

#[derive(serde_derive::Deserialize)]
struct SignResponse {
	digest: String,
}

#[derive(Debug)]
pub(super) enum Error {
	GetServerRootCertificate(ApiErrorReason),
	ParseWorkloadUrl(url::ParseError),
	ParseWorkloadUrlUnixFilePath,
	SignSasToken(ApiErrorReason),
	UnrecognizedWorkloadUrlScheme(String),
}

impl std::fmt::Display for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Error::GetServerRootCertificate(reason) => write!(f, "could not get server root certificate: {}", reason),
			Error::ParseWorkloadUrl(reason) => write!(f, "could not parse workload URL: {}", reason),
			Error::ParseWorkloadUrlUnixFilePath => write!(f, "could not parse workload URL as UDS file path"),
			Error::SignSasToken(reason) => write!(f, "could not create SAS token: {}", reason),
			Error::UnrecognizedWorkloadUrlScheme(scheme) => write!(f, "unrecognized scheme {:?}", scheme),
		}
	}
}

impl std::error::Error for Error {
	#[allow(clippy::match_same_arms)]
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::GetServerRootCertificate(reason) => reason.source(),
			Error::ParseWorkloadUrl(_) => None,
			Error::ParseWorkloadUrlUnixFilePath => None,
			Error::SignSasToken(reason) => reason.source(),
			Error::UnrecognizedWorkloadUrlScheme(_) => None,
		}
	}
}

#[derive(Debug)]
pub(super) enum ApiErrorReason {
	ConstructRequestUrl(Box<dyn std::error::Error + Send + Sync>),
	ConstructRequest(http::Error),
	ExecuteRequest(hyper::Error),
	ParseResponseBody(Box<dyn std::error::Error + Send + Sync>),
	ReadResponse(hyper::Error),
	SerializeRequestBody(serde_json::Error),
	UnsuccessfulResponse(http::StatusCode),
}

impl std::fmt::Display for ApiErrorReason {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ApiErrorReason::ConstructRequestUrl(err) => write!(f, "could not construct request URL: {}", err),
			ApiErrorReason::ConstructRequest(err) => write!(f, "could not construct request: {}", err),
			ApiErrorReason::ExecuteRequest(err) => write!(f, "could not execute request: {}", err),
			ApiErrorReason::ParseResponseBody(err) => write!(f, "could not deserialize response: {}", err),
			ApiErrorReason::ReadResponse(err) => write!(f, "could not read response: {}", err),
			ApiErrorReason::SerializeRequestBody(err) => write!(f, "could not serialize request: {}", err),
			ApiErrorReason::UnsuccessfulResponse(status) => write!(f, "response has status code {}", status),
		}
	}
}

impl std::error::Error for ApiErrorReason {
	#[allow(clippy::match_same_arms)]
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			ApiErrorReason::ConstructRequestUrl(err) => Some(&**err),
			ApiErrorReason::ConstructRequest(err) => Some(err),
			ApiErrorReason::ExecuteRequest(err) => Some(err),
			ApiErrorReason::ParseResponseBody(err) => Some(&**err),
			ApiErrorReason::ReadResponse(err) => Some(err),
			ApiErrorReason::SerializeRequestBody(err) => Some(err),
			ApiErrorReason::UnsuccessfulResponse(_) => None,
		}
	}
}
