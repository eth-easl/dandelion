use core::{fmt, panic};
use std::{fs::File, path::Path, str::FromStr};

use clap::Parser;
use log::{error, warn};

const DEFAULT_CONFIG_PATH: &str = "./dandelion.config";
const DEFAULT_FOLDER_PATH: &str = "/tmp/dandelion_server";
const DEFAULT_PORT: u16 = 8080;
const DEFAULT_TIMESTAMP_COUNT: usize = 1000;
const DEFAULT_VIRTUAL_MAX_RAM_MULTIPLIER: usize = 2;
const DEFAULT_MULTINODE_TIMEOUT: u64 = 50;
use machine_interface::composition::DEFAULT_AUTOSHARDING_OFFLOAD_CONST;
use machine_interface::function_driver::system_driver::reqwest::DEFAULT_CONCURRENCY_LIMIT;

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
struct PreloadFile {
    functions: Vec<PreloadFunc>,
    compositions: Vec<String>,
}

#[derive(serde::Deserialize, Debug)]
pub struct FuncMetadata {
    #[serde(rename = "inputSets")]
    pub input_sets: Vec<String>,
    #[serde(rename = "outputSets")]
    pub output_sets: Vec<String>,
    #[serde(rename = "minSetBytes", default)]
    pub min_set_bytes: Vec<usize>,
}

#[derive(Clone, Copy, serde::Deserialize, Debug, clap::ValueEnum)]
pub enum TestMode {
    SingleCore,
    NoCompute,
    NoEngine,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum AnyShardingMode {
    MaxSharding,
    FixedSharding(usize),
    AutoSharding(usize),
}

impl Default for AnyShardingMode {
    fn default() -> Self {
        Self::AutoSharding(DEFAULT_AUTOSHARDING_OFFLOAD_CONST)
    }
}

impl FromStr for AnyShardingMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "max" => Ok(Self::MaxSharding),
            s => {
                if s.starts_with("fixed:") {
                    let parts: Vec<&str> = s.split(':').collect();
                    let num = parts
                        .get(1)
                        .and_then(|val| val.parse::<usize>().ok())
                        .ok_or_else(|| {
                            "Invalid number for fixed sharding (e.g., 'fixed:4')".to_string()
                        })?;
                    Ok(Self::FixedSharding(num))
                } else if s.starts_with("auto:") {
                    let parts: Vec<&str> = s.split(':').collect();
                    let num = parts
                        .get(1)
                        .and_then(|val| val.parse::<usize>().ok())
                        .ok_or_else(|| {
                            "Invalid number for auto sharding (e.g., 'auto:2')".to_string()
                        })?;
                    Ok(Self::AutoSharding(num))
                } else {
                    Err(format!("Unknown AnyShardingMode {}", s))
                }
            }
        }
    }
}

impl fmt::Display for AnyShardingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MaxSharding => write!(f, "max"),
            Self::AutoSharding(n) => write!(f, "auto:{}", n),
            Self::FixedSharding(n) => write!(f, "fixed:{}", n),
        }
    }
}

impl TryFrom<String> for AnyShardingMode {
    type Error = String;
    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl From<AnyShardingMode> for String {
    fn from(mode: AnyShardingMode) -> Self {
        mode.to_string()
    }
}

#[derive(serde::Deserialize, Parser, Debug)]
pub struct DandelionConfig {
    #[arg(long, env, default_value_t = String::from(DEFAULT_CONFIG_PATH))]
    #[serde(default)]
    pub config_path: String,

    // general configuration parameters
    #[arg(long, env, default_value_t = DEFAULT_PORT)]
    #[serde(default)]
    pub port: u16,
    #[arg(long, env)]
    pub total_cores: Option<usize>,
    #[arg(long, env)]
    pub dispatcher_cores: Option<usize>,
    #[arg(long, env)]
    pub frontend_cores: Option<usize>,
    #[arg(long, env)]
    pub io_cores: Option<usize>,
    /// Number of concurrent requests to run per IO core
    #[arg(long, env, default_value_t = DEFAULT_CONCURRENCY_LIMIT)]
    #[serde(default)]
    pub io_concurrency: usize,
    #[arg(long, env, default_value_t = DEFAULT_TIMESTAMP_COUNT)]
    #[serde(default)]
    pub timestamp_count: usize,
    #[arg(long, env, default_value_t = DEFAULT_VIRTUAL_MAX_RAM_MULTIPLIER)]
    #[serde(default)]
    pub virtual_max_ram_multiplier: usize,
    #[arg(long, env, default_value_t = AnyShardingMode::AutoSharding(DEFAULT_AUTOSHARDING_OFFLOAD_CONST))]
    #[serde(default)]
    pub any_sharding_mode: AnyShardingMode,

    // (optional) preload config
    #[arg(long, env, default_value = "")]
    #[serde(default)]
    pub bin_preload_path: String,

    /// Folder for dandelion to use to store data
    #[arg(long, env, default_value_t = String::from(DEFAULT_FOLDER_PATH))]
    #[serde(default)]
    pub folder_path: String,

    /// Static identifier for this Dandelion node
    #[arg(long, env, default_value_t = 0)]
    #[serde(default)]
    pub node_id: u64,

    /// JSON config describing multinode data servers and queue server
    #[arg(long, env)]
    #[serde(default)]
    pub multinode_config: Option<String>,

    /// Timeout for how long to try to establish a connection to another node
    #[arg(long, env, default_value_t = DEFAULT_MULTINODE_TIMEOUT)]
    #[serde(default)]
    pub multinode_timeout_ms: u64,

    /// Special modes for testing
    #[arg(long, env, value_enum)]
    #[serde(default)]
    pub test_mode: Option<TestMode>,
}

impl DandelionConfig {
    /// Merge config generated from args into config read from serde, overwrite serde with non args value.
    /// If both serde and args give default values use the one from args
    fn merge_serde_into_args(&mut self, mut serde_config: Self) {
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
                if let Some(serde_val) = serde_config.$field.take() {
                    self.$field.get_or_insert(serde_val);
                }
            };
        }

        // merge serde config into args config
        // -> any args defaults are overwritten by serde non-default values
        // NOTE: config path is no further useful an can be ignored
        merge!(port, DEFAULT_PORT);
        merge_option!(test_mode);
        merge_option!(total_cores);
        merge_option!(dispatcher_cores);
        merge_option!(frontend_cores);
        merge_option!(io_cores);
        merge!(io_concurrency, DEFAULT_CONCURRENCY_LIMIT);
        merge!(timestamp_count, DEFAULT_TIMESTAMP_COUNT);
        merge!(
            virtual_max_ram_multiplier,
            DEFAULT_VIRTUAL_MAX_RAM_MULTIPLIER
        );
        merge!(
            any_sharding_mode,
            AnyShardingMode::AutoSharding(DEFAULT_AUTOSHARDING_OFFLOAD_CONST)
        );
        merge_clone!(bin_preload_path, String::from(""));
        merge_clone!(folder_path, String::from(DEFAULT_FOLDER_PATH));
        merge!(node_id, 0);
        merge_option!(multinode_config);
        merge!(multinode_timeout_ms, DEFAULT_MULTINODE_TIMEOUT);
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
                    Ok(file_config) => cli_config.merge_serde_into_args(file_config),
                    Err(err) => warn!("Could not load config file: {}", err),
                },
            };
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
        let no_test_mode_config = if let Some(num_cores) = self.frontend_cores {
            (1u8..(1 + num_cores as u8)).collect()
        } else {
            vec![0]
        };
        let core_vec = match self.test_mode {
            None | Some(TestMode::NoCompute) => no_test_mode_config,
            Some(TestMode::SingleCore) => vec![0],
            Some(TestMode::NoEngine) => vec![],
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
        let no_test_mode_cores = if let Some(comm_cores) = self.io_cores {
            let lower_end = self
                .frontend_cores
                .and_then(|frontend_cores| Some(1 + frontend_cores as u8))
                .unwrap_or(1u8);
            (lower_end..lower_end + comm_cores as u8).collect()
        } else {
            vec![]
        };
        let core_vec = match &self.test_mode {
            None | Some(TestMode::NoCompute) => no_test_mode_cores,
            Some(TestMode::SingleCore) => vec![0],
            Some(TestMode::NoEngine) => vec![],
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
        let core_vec = if let Some(test_mode) = &self.test_mode {
            return match test_mode {
                TestMode::SingleCore => vec![0],
                TestMode::NoCompute => vec![],
                TestMode::NoEngine => vec![],
            };
        } else {
            let max_core = self
                .total_cores
                .expect("total_cores should be set after init");
            // 1 other core for dispatcher is fixed
            let other_cores = self.dispatcher_cores.unwrap_or(1)
                + self.frontend_cores.unwrap_or(0)
                + self.io_cores.unwrap_or(0);
            if other_cores >= max_core {
                panic!("no cores for engines left");
            }
            (other_cores as u8..max_core as u8).collect()
        };
        return core_vec;
    }

    pub fn get_preload_functions(&self) -> (Vec<PreloadFunc>, Vec<String>) {
        let default_value = (vec![], vec![]);
        if self.bin_preload_path.is_empty() {
            return default_value;
        }

        // read + parse json file
        let reader = match File::open(Path::new(&self.bin_preload_path)) {
            Err(err) => {
                error!("Failed to read preload json file: {}", err);
                return default_value;
            }
            Ok(f) => f,
        };
        let PreloadFile {
            functions,
            compositions,
        } = match serde_json::from_reader(reader) {
            Err(err) => {
                error!("Failed to read preload json file: {}", err);
                return default_value;
            }
            Ok(json) => json,
        };

        // sanity checks
        if !functions.iter().all(|pf| {
            let valid = !pf.name.is_empty()
                && pf.ctx_size > 0
                && !pf.engine_type_id.is_empty()
                && !pf.bin_path.is_empty();
            if !valid {
                warn!(
                    "Ignoring preload function {}: Does not match specification!",
                    pf.name
                )
            };
            valid
        }) || !compositions.iter().all(|composition| {
            let valid = !composition.is_empty();
            if !valid {
                warn!("Ignoring empty composition preload!");
            }
            valid
        }) {
            return default_value;
        }
        (functions, compositions)
    }
}
