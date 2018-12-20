// Example:
//
//     cargo run --example twin -- --device-id <> --iothub-hostname <> --sas-token <> --will 'azure-iot-mqtt-twin-client client unexpectedly disconnected'

use futures::{ Future, Stream };

#[derive(Debug, structopt_derive::StructOpt)]
struct Options {
	#[structopt(help = "Device ID", long = "device-id")]
	device_id: String,

	#[structopt(help = "IoT Hub hostname (eg foo.azure-devices.net)", long = "iothub-hostname")]
	iothub_hostname: String,

	#[structopt(help = "SAS token", long = "sas-token")]
	sas_token: String,

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
	env_logger::Builder::from_env(env_logger::Env::new().filter_or("AZURE_IOT_MQTT_LOG", "mqtt=debug,mqtt::logging=trace,twin=info")).init();

	let Options {
		device_id,
		iothub_hostname,
		sas_token,
		will,
		max_back_off,
		keep_alive,
	} = structopt::StructOpt::from_args();

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");

	let twin_client = azure_iot_mqtt::TwinClient::new(
		iothub_hostname,
		device_id,
		sas_token,

		will.map(String::into_bytes),

		max_back_off,
		keep_alive,
	).expect("could not create twin client");

	let shutdown_handle = twin_client.inner().shutdown_handle().expect("couldn't get shutdown handle");
	runtime.spawn(
		tokio_signal::ctrl_c()
		.flatten_stream()
		.into_future()
		.then(move |_| shutdown_handle.shutdown())
		.then(|result| {
			result.expect("couldn't send shutdown notification");
			Ok(())
		}));

	let f = twin_client.for_each(|twin_message| {
		log::info!("received twin message {:?}", twin_message);

		Ok(())
	});

	runtime.block_on(f).expect("azure-iot-mqtt-twin-client client failed");
}

fn duration_from_secs_str(s: &str) -> Result<std::time::Duration, <u64 as std::str::FromStr>::Err> {
	Ok(std::time::Duration::from_secs(s.parse()?))
}
