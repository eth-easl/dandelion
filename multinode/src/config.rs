use log::warn;
use std::{collections::BTreeMap, fs::File};

#[derive(serde::Deserialize)]
pub struct MultinodeConfig {
    pub queue_server: NodeUrl,
    pub data_servers: Vec<NodeUrl>,
}

#[derive(serde::Deserialize)]
pub struct NodeUrl {
    pub node_id: u64,
    pub url: String,
}

impl MultinodeConfig {
    pub fn load(path: Option<&str>) -> Option<Self> {
        path.map(|path| match File::open(path) {
            Ok(config_file) => serde_json::from_reader(config_file)
                .map_err(|err| warn!("Could not parse multinode config {}: {}", path, err))
                .ok(),
            Err(err) => {
                warn!("Could not load multinode config {}: {}", path, err);
                None
            }
        })
        .flatten()
    }

    pub fn data_server_urls(&self) -> BTreeMap<u64, String> {
        self.data_servers
            .iter()
            .map(|server| (server.node_id, server.url.clone()))
            .collect()
    }
}

fn address_authority(address: &str) -> &str {
    let address = address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address);
    address.split('/').next().unwrap_or(address)
}

fn address_port(address: &str, description: &str) -> Result<u16, String> {
    let authority = address_authority(address);
    let (_, port) = authority.rsplit_once(':').ok_or_else(|| {
        format!(
            "{} address {} does not include a port",
            description, address
        )
    })?;
    port.parse::<u16>()
        .map_err(|err| format!("invalid {} port in {}: {}", description, address, err))
}

pub fn data_server_port(address: &str) -> Result<u16, String> {
    address_port(address, "data server")
}

pub fn queue_server_port(address: &str) -> Result<u16, String> {
    address_port(address, "queue server")
}

pub fn tcp_address(address: &str) -> String {
    address_authority(address).to_string()
}
