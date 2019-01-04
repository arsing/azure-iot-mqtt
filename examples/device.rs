// Example:
//
//     cargo run --example device -- --device-id <> --iothub-hostname <> --sas-token <> --use-websocket --will 'azure-iot-mqtt client unexpectedly disconnected'

use futures::{ Future, Stream };

#[derive(Debug, structopt_derive::StructOpt)]
struct Options {
	#[structopt(help = "Device ID", long = "device-id")]
	device_id: String,

	#[structopt(help = "IoT Hub hostname (eg foo.azure-devices.net)", long = "iothub-hostname")]
	iothub_hostname: String,

	#[structopt(help = "SAS token", long = "sas-token")]
	sas_token: String,

	#[structopt(help = "Whether to use websockets or bare TLS to connect to the Iot Hub", long = "use-websocket")]
	use_websocket: bool,

	#[structopt(help = "Will message to publish if this client disconnects unexpectedly", long = "will")]
	will: Option<String>,

	#[structopt(
		help = "Maximum back-off time between reconnections to the server, in seconds.",
		long = "max-back-off",
		default_value = "30",
		parse(try_from_str = "duration_from_secs_str"),
	)]
	max_back_off: std::time::Duration,

	#[structopt(
		help = "Keep-alive time advertised to the server, in seconds.",
		long = "keep-alive",
		default_value = "5",
		parse(try_from_str = "duration_from_secs_str"),
	)]
	keep_alive: std::time::Duration,
}

fn main() {
	env_logger::Builder::from_env(env_logger::Env::new().filter_or("AZURE_IOT_MQTT_LOG", "mqtt=debug,mqtt::logging=trace,azure_iot_mqtt=debug,device=info")).init();

	let Options {
		device_id,
		iothub_hostname,
		sas_token,
		use_websocket,
		will,
		max_back_off,
		keep_alive,
	} = structopt::StructOpt::from_args();

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");
	let executor = runtime.executor();

	let client = azure_iot_mqtt::Client::new(
		iothub_hostname,
		device_id,
		sas_token,
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

	let f = client.for_each(move |message| {
		log::info!("received message {:?}", message);

		if let azure_iot_mqtt::Message::DirectMethod { name, payload, request_id } = message {
			log::info!("direct method {:?} invoked with payload {:?}", name, payload);

			// Respond with status 200 and same payload
			executor.spawn(direct_method_response_handle
				.respond(request_id.clone(), 200, payload)
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

fn duration_from_secs_str(s: &str) -> Result<std::time::Duration, <u64 as std::str::FromStr>::Err> {
	Ok(std::time::Duration::from_secs(s.parse()?))
}
