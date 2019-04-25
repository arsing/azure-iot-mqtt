use std::io::Read;

pub(crate) fn authentication_group() -> structopt::clap::ArgGroup<'static> {
	structopt::clap::ArgGroup::with_name("authentication").required(true)
}

pub(crate) fn parse_authentication(
	device_id: &str,
	sas_key: Option<String>,
	sas_key_token_valid_time: Option<std::time::Duration>,
	sas_token: Option<String>,
	certificate_file: Option<std::path::PathBuf>,
	certificate_file_password: Option<String>,
) -> azure_iot_mqtt::Authentication {
	match (sas_key, sas_key_token_valid_time, sas_token, certificate_file, certificate_file_password) {
		(Some(sas_key), Some(sas_key_token_valid_time), None, None, None) => azure_iot_mqtt::Authentication::SasKey {
			device_id: device_id.to_owned(),
			key: base64::decode(&sas_key).expect("could not parse SAS key"),
			max_token_valid_duration: sas_key_token_valid_time,
			server_root_certificate: None,
		},

		(None, None, Some(sas_token), None, None) => azure_iot_mqtt::Authentication::SasToken {
			token: sas_token,
			server_root_certificate: None,
		},

		(None, None, None, Some(certificate_file), Some(certificate_file_password)) => {
			let certificate_file_display = certificate_file.display().to_string();

			let mut certificate_file = match std::fs::File::open(certificate_file) {
				Ok(certificate_file) => certificate_file,
				Err(err) => panic!("could not open certificate file {}: {}", certificate_file_display, err),
			};

			let mut certificate = vec![];
			if let Err(err) = certificate_file.read_to_end(&mut certificate) {
				panic!("could not read certificate file {}: {}", certificate_file_display, err);
			}

			azure_iot_mqtt::Authentication::Certificate {
				der: certificate,
				password: certificate_file_password,
				server_root_certificate: None,
			}
		},

		_ => unreachable!(),
	}
}

pub(crate) fn duration_from_secs_str(s: &str) -> Result<std::time::Duration, <u64 as std::str::FromStr>::Err> {
	Ok(std::time::Duration::from_secs(s.parse()?))
}
