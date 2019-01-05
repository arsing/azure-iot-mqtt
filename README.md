An Azure IoT client library


# Features

- Device client
	- Receive initial twin state and updates
	- Receive and respond to direct method requests

- Module client
	- Receive and respond to direct method requests

- Supports MQTT and MQTT-over-WebSocket protocols.

- Transparently reconnects when connection is broken or protocol errors, with back-off.

- Standard futures 0.1 and tokio 0.1 interface. The client is just a `futures::Stream` of events received from the server.


# Documentation

The crate is not published to crates.io yet. Please generate docs locally with `cargo doc`.


# Examples

See the `examples/` directory for examples of a device application using the device client, and a module application using the module client.


# License

TBD. Will be resolved before publishing.
