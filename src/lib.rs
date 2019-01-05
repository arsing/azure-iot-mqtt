//! This crate contains types related to the Azure IoT MQTT server.

#![deny(rust_2018_idioms, warnings)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(
	clippy::cyclomatic_complexity,
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

pub mod device;

mod io;
pub use self::io::{ Io, IoSource, Transport };

pub mod module;

mod system_properties;
pub use self::system_properties::{ IotHubAck, SystemProperties };

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

/// Represents the status code used in initial twin responses and device method responses
#[derive(Clone, Copy, Debug)]
pub enum Status {
	/// 200
	Ok,

	/// 400
	BadRequest,

	/// 429
	TooManyRequests,

	/// 5xx
	Error(u32),

	/// Other
	Other(u32),
}

impl std::fmt::Display for Status {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		#[allow(clippy::match_same_arms)]
		match self {
			Status::Ok => write!(f, "200"),
			Status::BadRequest => write!(f, "400"),
			Status::TooManyRequests => write!(f, "429"),
			Status::Error(raw) => write!(f, "{}", raw),
			Status::Other(raw) => write!(f, "{}", raw),
		}
	}
}

impl std::str::FromStr for Status {
	type Err = std::num::ParseIntError;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(match s.parse()? {
			200 => Status::Ok,
			400 => Status::BadRequest,
			429 => Status::TooManyRequests,
			raw if raw >= 500 && raw < 600 => Status::Error(raw),
			raw => Status::Other(raw),
		})
	}
}
