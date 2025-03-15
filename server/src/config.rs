use core::panic;

use clap::Parser;

const DEFAULT_CONFIG_PATH: &str = "./dandelion.config";
const DEFAULT_PORT: u16 = 8080;
const DEFAULT_SINGLE_CORE: bool = false;
const DEFAULT_TIMESTAMP_COUNT: usize = 1000;
const DEFAULT_THREAD_PER_CORE: usize = 1;
const DEFAULT_CPU_PINNING: bool = true;


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
    #[arg(long, env, default_value_t = 1.0)]
    #[serde(default)]
    pub control_delta: f64,
    #[arg(long, env, default_value_t = 200)]
    #[serde(default)]
    pub control_interval: u64, // in milliseconds
    #[arg(long, env, default_value_t = DEFAULT_THREAD_PER_CORE)]
    #[serde(default)]
    pub threads_per_core: usize,
    #[arg(long, env, default_value_t = DEFAULT_CPU_PINNING)]
    #[serde(default)]
    pub cpu_pinning: bool,
}

impl DandelionConfig {
    /// Merge config generated from args into config read from serde, overwrite serde with non args value.
    /// If both serde and args give default values use the one from args
    fn merge_serde_into_args(&mut self, other: &Self) {
        let default: Self = serde_json::from_slice(&[])
            .expect("Should have default values for all values in config");
        // config path can be ignored, as it is not meaning full afterwards
        // handle port
        if self.port != DEFAULT_PORT && other.port != default.port {
            self.port = other.port;
        }
        // handle single core mode
        if self.single_core_mode != DEFAULT_SINGLE_CORE
            && self.single_core_mode != default.single_core_mode
        {
            self.single_core_mode = other.single_core_mode;
        }
        // handle options
        if let Some(other_val) = other.total_cores {
            self.total_cores.get_or_insert(other_val);
        }
        if let Some(other_val) = other.dispatcher_cores {
            self.dispatcher_cores.get_or_insert(other_val);
        }
        if let Some(other_val) = other.frontend_cores {
            self.frontend_cores.get_or_insert(other_val);
        }
        if let Some(other_val) = other.io_cores {
            self.io_cores.get_or_insert(other_val);
        }
        // timestamp count
        if other.timestamp_count != DEFAULT_TIMESTAMP_COUNT
            && self.timestamp_count != default.timestamp_count
        {
            self.timestamp_count = other.timestamp_count;
        }
        // threads per core
        if other.threads_per_core != DEFAULT_THREAD_PER_CORE
            && self.threads_per_core != default.threads_per_core
        {
            self.threads_per_core = other.threads_per_core;
        }
        // cpu pinning
        if other.cpu_pinning != DEFAULT_CPU_PINNING
            && self.cpu_pinning != default.cpu_pinning
        {
            self.cpu_pinning = other.cpu_pinning;
        }
    }

    /// Get the config from the arguments, environment and possibly config file
    pub fn get_config() -> Self {
        // parse arguments from the command line and environent first
        let mut cli_config: DandelionConfig = DandelionConfig::parse();
        // get dandelion config path if it exists and if not use current working directory
        // check if the user specified a config file or if there is one in the default location
        if let Ok(config_buff) = std::fs::read(cli_config.config_path.clone()) {
            if let Ok(file_config) = serde_json::from_slice::<DandelionConfig>(&config_buff) {
                cli_config.merge_serde_into_args(&file_config);
            }
        }
        cli_config
            .total_cores
            .get_or_insert(num_cpus::get_physical());
        return cli_config;
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
