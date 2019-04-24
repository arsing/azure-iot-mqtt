// An example device client.
//
// - Connects to an Azure IoT Hub using bare TLS or WebSockets.
// - Responds to direct method requests by returning the same payload.
// - Reports twin state once at start, then updates it periodically after.
//
//
// Example:
//
//     cargo run --example device -- \
//         --device-id <> --iothub-hostname <> --sas-key <> --sas-key-token-valid-time 600 --use-websocket --will 'azure-iot-mqtt client unexpectedly disconnected'

use futures::{ Future, Stream };

mod common;

#[derive(Debug, structopt_derive::StructOpt)]
#[structopt(raw(group = "common::authentication_group()"))]
struct Options {
	#[structopt(help = "Device ID", long = "device-id")]
	device_id: String,

	#[structopt(help = "IoT Hub hostname (eg foo.azure-devices.net)", long = "iothub-hostname")]
	iothub_hostname: String,

	#[structopt(help = "SAS key for token authentication, in base64 format", long = "sas-key", group = "authentication", requires = "sas_key_token_valid_time")]
	sas_key: Option<String>,

	#[structopt(
		help = "The maximum time that a SAS token generated from the SAS key should be valid for, in seconds",
		long = "sas-key-token-valid-time",
		requires = "sas_key",
		parse(try_from_str = "common::duration_from_secs_str"),
	)]
	sas_key_token_valid_time: Option<std::time::Duration>,

	#[structopt(help = "SAS token for token authentication", long = "sas-token", group = "authentication")]
	sas_token: Option<String>,

	#[structopt(
		help = "Path of certificate file (PKCS #12) for certificate authentication",
		long = "certificate-file",
		group = "authentication",
		requires = "certificate_file_password",
		parse(from_os_str),
	)]
	certificate_file: Option<std::path::PathBuf>,

	#[structopt(
		help = "Password to decrypt certificate file for certificate authentication",
		long = "certificate-file-password",
		requires = "certificate_file",
	)]
	certificate_file_password: Option<String>,

	#[structopt(help = "Whether to use websockets or bare TLS to connect to the Iot Hub", long = "use-websocket")]
	use_websocket: bool,

	#[structopt(help = "Will message to publish if this client disconnects unexpectedly", long = "will")]
	will: Option<String>,

	#[structopt(
		help = "Maximum back-off time between reconnections to the server, in seconds.",
		long = "max-back-off",
		default_value = "30",
		parse(try_from_str = "common::duration_from_secs_str"),
	)]
	max_back_off: std::time::Duration,

	#[structopt(
		help = "Keep-alive time advertised to the server, in seconds.",
		long = "keep-alive",
		default_value = "5",
		parse(try_from_str = "common::duration_from_secs_str"),
	)]
	keep_alive: std::time::Duration,

	#[structopt(
		help = "Interval at which the client reports its twin state to the server, in seconds.",
		long = "report-twin-state-period",
		default_value = "5",
		parse(try_from_str = "common::duration_from_secs_str"),
	)]
	report_twin_state_period: std::time::Duration,
}

fn main() {
	env_logger::Builder::from_env(env_logger::Env::new().filter_or("AZURE_IOT_MQTT_LOG", "mqtt=debug,mqtt::logging=trace,azure_iot_mqtt=debug,device=info")).init();

	let Options {
		device_id,
		iothub_hostname,
		sas_key,
		sas_key_token_valid_time,
		sas_token,
		certificate_file,
		certificate_file_password,
		use_websocket,
		will,
		max_back_off,
		keep_alive,
		report_twin_state_period,
	} = structopt::StructOpt::from_args();

	let authentication =
		common::parse_authentication(
			&device_id,
			sas_key,
			sas_key_token_valid_time,
			sas_token,
			certificate_file,
			certificate_file_password);

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");
	let executor = runtime.executor();

	let client = azure_iot_mqtt::device::Client::new(
		iothub_hostname,
		&device_id,
		authentication,
		if use_websocket { azure_iot_mqtt::Transport::WebSocket } else { azure_iot_mqtt::Transport::Tcp },

		will.map(String::into_bytes),

		max_back_off,
		keep_alive,
	).expect("could not create client");

	let shutdown_handle = client.inner().shutdown_handle().expect("couldn't get shutdown handle");
	runtime.spawn(
		tokio_signal::ctrl_c()
		.flatten_stream()
		.into_future()
		.then(move |_| shutdown_handle.shutdown())
		.then(|result| {
			result.expect("couldn't send shutdown notification");
			Ok(())
		}));

	let direct_method_response_handle = client.direct_method_response_handle();

	let report_twin_state_handle = client.report_twin_state_handle();

	runtime.spawn(
		report_twin_state_handle.report_twin_state(azure_iot_mqtt::ReportTwinStateRequest::Replace(
			vec![("start-time".to_string(), chrono::Utc::now().to_string().into())].into_iter().collect()
		))
		.then(|result| {
			let _ = result.expect("couldn't report initial twin state");
			Ok(())
		})
		.and_then(move |()|
			tokio::timer::Interval::new(std::time::Instant::now(), report_twin_state_period)
			.then(move |result| {
				let _ = result.expect("timer failed");

				report_twin_state_handle.report_twin_state(azure_iot_mqtt::ReportTwinStateRequest::Patch(
					vec![("current-time".to_string(), chrono::Utc::now().to_string().into())].into_iter().collect()
				))
				.then(|result| {
					let _ = result.expect("couldn't report twin state patch");
					Ok(())
				})
			})
			.for_each(Ok)));

	let f = client.for_each(move |message| {
		log::info!("received message {:?}", message);

		if let azure_iot_mqtt::device::Message::DirectMethod { name, payload, request_id } = message {
			log::info!("direct method {:?} invoked with payload {:?}", name, payload);

			// Respond with status 200 and same payload
			executor.spawn(direct_method_response_handle
				.respond(request_id.clone(), azure_iot_mqtt::Status::Ok, payload)
				.then(move |result| {
					let () = result.expect("couldn't send direct method response");
					log::info!("Responded to request {}", request_id);
					Ok(())
				}));
		}

		Ok(())
	});

	runtime.block_on(f).expect("azure-iot-mqtt client failed");
}
