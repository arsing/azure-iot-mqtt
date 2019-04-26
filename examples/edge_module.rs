// An example Edge module client.
//
// - Connects to Azure IoT Edge Hub using bare TLS or WebSockets.
// - Responds to direct method requests by returning the same payload.
// - Reports twin state once at start, then updates it periodically after.
//
//
// Example:
//
//     cargo run --example edge_module -- --use-websocket --will 'azure-iot-mqtt client unexpectedly disconnected'

use futures::{ Future, Stream };

#[allow(unused)]
mod common;

#[derive(Debug, structopt_derive::StructOpt)]
struct Options {
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
	env_logger::Builder::from_env(env_logger::Env::new().filter_or("AZURE_IOT_MQTT_LOG", "mqtt=debug,mqtt::logging=trace,azure_iot_mqtt=debug,module=info")).init();

	let Options {
		use_websocket,
		will,
		max_back_off,
		keep_alive,
		report_twin_state_period,
	} = structopt::StructOpt::from_args();

	let device_id = std::env::var("IOTEDGE_DEVICEID").expect("IOTEDGE_DEVICEID env var is not set");

	let module_id = std::env::var("IOTEDGE_MODULEID").expect("IOTEDGE_MODULEID env var is not set");

	let generation_id = std::env::var("IOTEDGE_MODULEGENERATIONID").expect("IOTEDGE_MODULEGENERATIONID env var is not set");

	let edgehub_hostname = std::env::var("IOTEDGE_GATEWAYHOSTNAME").expect("IOTEDGE_GATEWAYHOSTNAME env var is not set");

	let iothub_hostname = std::env::var("IOTEDGE_IOTHUBHOSTNAME").expect("IOTEDGE_IOTHUBHOSTNAME env var is not set");

	let workload_url = std::env::var("IOTEDGE_WORKLOADURI").expect("IOTEDGE_WORKLOADURI env var is not set");
	let workload_url = workload_url.parse().expect("could not parse IOTEDGE_WORKLOADURI");

	let authentication = azure_iot_mqtt::Authentication::IotEdge {
		device_id: device_id.clone(),
		module_id: module_id.clone(),
		generation_id,
		iothub_hostname,
		workload_url,
	};

	let mut runtime = tokio::runtime::Runtime::new().expect("couldn't initialize tokio runtime");
	let executor = runtime.executor();

	let client = azure_iot_mqtt::module::Client::new(
		edgehub_hostname,
		&device_id,
		&module_id,
		authentication,
		if use_websocket { azure_iot_mqtt::Transport::WebSocket } else { azure_iot_mqtt::Transport::Tcp },

		will.map(Into::into),

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

		if let azure_iot_mqtt::module::Message::DirectMethod { name, payload, request_id } = message {
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
