use core::panic;
use log::{debug, error, warn};
use std::fs::File;
use std::net::Ipv4Addr;
use std::path::Path;
use std::process::Command;

use clap::Parser;

const DEFAULT_CONFIG_PATH: &str = "./dandelion.config";
const DEFAULT_PORT: u16 = 8080;
const DEFAULT_SINGLE_CORE: bool = false;
const DEFAULT_TIMESTAMP_COUNT: usize = 1000;
const DEFAULT_MULTINODE_ENABLED: bool = false;
const DEFAULT_MULTINODE_PORT: u16 = 8081;

#[derive(serde::Deserialize, Debug)]
pub struct PreloadFunc {
    #[serde(rename = "name")]
    pub name: String,
    #[serde(rename = "engineType")]
    pub engine_type_id: String,
    #[serde(rename = "ctxSize")]
    pub ctx_size: usize,
    #[serde(rename = "binaryPath")]
    pub bin_path: String,
    #[serde(rename = "metadata")]
    pub metadata: FuncMetadata,
}

#[derive(serde::Deserialize, Debug)]
pub struct FuncMetadata {
    #[serde(rename = "inputSets")]
    pub input_sets: Vec<String>,
    #[serde(rename = "outputSets")]
    pub output_sets: Vec<String>,
}

#[derive(serde::Deserialize, Parser, Debug)]
pub struct DandelionConfig {
    #[arg(long, env, default_value_t = String::from(DEFAULT_CONFIG_PATH))]
    #[serde(default)]
    pub config_path: String,

    #[arg(long, env, default_value_t = DEFAULT_PORT)]
    #[serde(default)]
    pub port: u16,
    #[arg(long,env, default_value_t = DEFAULT_SINGLE_CORE)]
    #[serde(default)]
    pub single_core_mode: bool,
    #[arg(long, env)]
    pub total_cores: Option<usize>,
    #[arg(long, env)]
    pub dispatcher_cores: Option<usize>,
    #[arg(long, env)]
    pub frontend_cores: Option<usize>,
    #[arg(long, env)]
    pub io_cores: Option<usize>,
    #[arg(long, env, default_value_t = DEFAULT_TIMESTAMP_COUNT)]
    #[serde(default)]
    pub timestamp_count: usize,

    // (optional) multinode configuration
    #[arg(long, env, default_value_t = DEFAULT_MULTINODE_ENABLED)]
    #[serde(default)]
    pub multinode_enabled: bool,
    #[arg(long, env, default_value_t = DEFAULT_MULTINODE_PORT)]
    #[serde(default)]
    pub multinode_port: u16,
    #[arg(long, env, default_value = "")]
    #[serde(default)]
    pub multinode_local_ip: String,
    #[arg(long, env, default_value = "")]
    #[serde(default)]
    pub multinode_leader_ip: String,

    // (optional) preload config
    #[arg(long, env, default_value = "")]
    #[serde(default)]
    pub bin_preload_path: String,
}

fn check_and_resolve_ip(ip: &mut String) -> Result<(), String> {
    if ip.is_empty() {
        return Ok(());
    }
    if ip.parse::<Ipv4Addr>().is_ok() {
        return Ok(());
    }
    let pattern = match ip.as_str() {
        "dynamic_r630" => "10.233.0",
        "dynamic_cloudlab" => "10.0.1",
        _ => return Err(format!("Got invalid ip address: {}", ip)),
    };
    let mut resolved_ip = match Command::new("bash")
        .arg("-c")
        .arg(format!("ifconfig | grep {} | awk '{{print $2}}'", pattern))
        .output()
    {
        Ok(out) => String::from_utf8(out.stdout).unwrap(),
        Err(err) => return Err(format!("Failed to resolve {} ip: {}", pattern, err)),
    };
    resolved_ip.pop(); // remove trailing newline
    debug!("Resolved '{}' to '{}'", ip, resolved_ip);
    *ip = resolved_ip;
    Ok(())
}

impl DandelionConfig {
    /// Merge config generated from args into config read from serde, overwrite serde with non args value.
    /// If both serde and args give default values use the one from args
    fn merge_serde_into_args(&mut self, serde_config: &Self) {
        let default: Self = serde_json::from_slice("{}".as_bytes())
            .expect("Should have default values for all values in config");

        // define merging macros
        macro_rules! merge {
            ($field:ident, $default:expr) => {
                if self.$field == $default && serde_config.$field != default.$field {
                    self.$field = serde_config.$field;
                }
            };
        }
        macro_rules! merge_clone {
            ($field:ident, $default:expr) => {
                if self.$field == $default && serde_config.$field != default.$field {
                    self.$field = serde_config.$field.clone();
                }
            };
        }
        macro_rules! merge_option {
            ($field:ident) => {
                if let Some(serde_val) = serde_config.$field {
                    self.$field.get_or_insert(serde_val);
                }
            };
        }

        // merge serde config into args config
        // -> any args defaults are overwritten by serde non-default values
        // NOTE: config path is no further useful an can be ignored
        merge!(port, DEFAULT_PORT);
        merge!(single_core_mode, DEFAULT_SINGLE_CORE);
        merge_option!(total_cores);
        merge_option!(dispatcher_cores);
        merge_option!(frontend_cores);
        merge_option!(io_cores);
        merge!(timestamp_count, DEFAULT_TIMESTAMP_COUNT);
        merge!(multinode_enabled, DEFAULT_MULTINODE_ENABLED);
        merge!(multinode_port, DEFAULT_MULTINODE_PORT);
        merge_clone!(multinode_local_ip, String::from(""));
        merge_clone!(multinode_leader_ip, String::from(""));
        merge_clone!(bin_preload_path, String::from(""));
    }

    /// Get the config from the arguments, environment and possibly config file
    pub fn get_config() -> Self {
        // parse arguments from the command line and environent first
        let mut cli_config: DandelionConfig = DandelionConfig::parse();

        // if a config path is given -> read + parse it and merge into args config
        if !cli_config.config_path.is_empty() {
            match File::open(Path::new(&cli_config.config_path)) {
                Err(err) => warn!(
                    "Could not load config file {}: {}",
                    cli_config.config_path, err
                ),
                Ok(config_file) => match serde_json::from_reader(config_file) {
                    Ok(file_config) => cli_config.merge_serde_into_args(&file_config),
                    Err(err) => warn!("Could not load config file: {}", err),
                },
            };
        }

        cli_config
            .total_cores
            .get_or_insert(num_cpus::get_physical());

        check_and_resolve_ip(&mut cli_config.multinode_leader_ip)
            .expect("Invalid multinode leader ip in config");
        check_and_resolve_ip(&mut cli_config.multinode_local_ip)
            .expect("Invalid multinode local ip in config");

        return cli_config;
    }

    pub fn multinode_enabled(&self) -> bool {
        return self.multinode_enabled;
    }
    pub fn is_multinode_leader(&self) -> bool {
        return self.multinode_enabled && self.multinode_local_ip == self.multinode_leader_ip;
    }
    pub fn is_multinode_worker(&self) -> bool {
        return self.multinode_enabled && self.multinode_local_ip != self.multinode_leader_ip;
    }

    pub fn get_preload_functions(&self) -> Vec<PreloadFunc> {
        if self.bin_preload_path.is_empty() {
            return vec![];
        }

        // read + parse json file
        let reader = match File::open(Path::new(&self.bin_preload_path)) {
            Err(err) => {
                error!("Failed to read preload json file: {}", err);
                return vec![];
            }
            Ok(f) => f,
        };
        let json: Vec<PreloadFunc> = match serde_json::from_reader(reader) {
            Err(err) => {
                error!("Failed to read preload json file: {}", err);
                return vec![];
            }
            Ok(json) => json,
        };

        // sanity checks
        json.into_iter()
            .filter(|pf| {
                let valid = !pf.name.is_empty()
                    && pf.ctx_size > 0
                    && !pf.engine_type_id.is_empty()
                    && !pf.bin_path.is_empty();
                if !valid {
                    warn!(
                        "Ignoring preload function {}: does not match specification!",
                        pf.name
                    )
                };
                valid
            })
            .collect()
    }

    /// TODO depricate, and move as we move to single dispatcher core
    pub fn get_dispatcher_cores(&self) -> Vec<u8> {
        if self
            .total_cores
            .expect("Expect total cores to be set after init")
            < 1
        {
            panic!("Less than 1 core to run system");
        }
        if let Some(disp_cores) = self.dispatcher_cores {
            if disp_cores != 1 {
                panic!("trying to allocate more than 1 dispatcher core");
            }
        }
        return vec![0];
    }
    /// TODO depricate as we move to dynamic allocation
    pub fn get_frontend_cores(&self) -> Vec<u8> {
        let total_cores = self
            .total_cores
            .expect("total_cores should be set after init");
        let core_vec = if self.single_core_mode {
            vec![0]
        } else {
            if let Some(num_cores) = self.frontend_cores {
                (1u8..(1 + num_cores as u8)).collect()
            } else {
                vec![0]
            }
        };
        let max_core = core_vec
            .iter()
            .max()
            .expect("should have at least 1 frontend core in core vec");
        if *max_core as usize >= total_cores {
            panic!("allocated core with higher number than total cores");
        }
        return core_vec;
    }
    /// TODO depricate as we move to dynamic allocation
    pub fn get_communication_cores(&self) -> Vec<u8> {
        let core_vec = if self.single_core_mode {
            vec![0]
        } else if let Some(comm_cores) = self.io_cores {
            let lower_end = self
                .frontend_cores
                .and_then(|frontend_cores| Some(1 + frontend_cores as u8))
                .unwrap_or(1u8);
            (lower_end..lower_end + comm_cores as u8).collect()
        } else {
            vec![]
        };
        let total_cores = self
            .total_cores
            .expect("total_cores should be set after init");
        if let Some(max_core) = core_vec.iter().max() {
            if *max_core as usize >= total_cores {
                panic!("allocated more cores than given in total");
            }
        };
        return core_vec;
    }
    /// TODO depricate as we move to dynamic allocation
    pub fn get_computation_cores(&self) -> Vec<u8> {
        let core_vec = if self.single_core_mode {
            vec![0]
        } else {
            let max_core = self
                .total_cores
                .expect("total_cores should be set after init");
            // 1 other core for dispatcher is fixed
            let other_cores = 1 + self.frontend_cores.unwrap_or(0) + self.io_cores.unwrap_or(0);
            if other_cores + 1 >= max_core {
                panic!("no cores for engines left");
            }
            (other_cores as u8..max_core as u8).collect()
        };
        return core_vec;
    }
}
