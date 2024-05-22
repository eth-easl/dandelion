use std::str::FromStr;

use log::{warn, LevelFilter};

#[derive(serde::Deserialize)]
pub struct DandelionConfig {
    #[serde(default = "total_cores_default")]
    pub total_cores: usize,
    #[serde(default = "dispatcher_cores_default")]
    pub dispatcher_cores: usize,
    #[serde(default = "timestamp_count_default")]
    pub timestamp_count: usize,
    #[serde(default = "loglevel_default")]
    pub log_level: LevelFilter,
}

fn total_cores_default() -> usize {
    return std::env::var("NUM_TOTAL_CORES").map_or_else(
        |_e| num_cpus::get_physical(),
        |n| n.parse::<usize>().unwrap(),
    );
}

fn dispatcher_cores_default() -> usize {
    return std::env::var("NUM_DISP_CORES").map_or(1, |n| n.parse::<usize>().unwrap());
}

fn timestamp_count_default() -> usize {
    return std::env::var("DANDELION_TIMESTAMP_COUNT")
        .map_or(1000, |n| n.parse::<usize>().unwrap());
}

fn loglevel_default() -> LevelFilter {
    return std::env::var("RUST_LOG").map_or(LevelFilter::Debug, |env_string| {
        LevelFilter::from_str(&env_string).unwrap()
    });
}

pub fn get_config() -> DandelionConfig {
    // get dandelion config path if it exists and if not use current working directory
    let config_path = std::env::var("DANDELION_CONFIG").unwrap_or(String::from("./dandelion.json"));
    let config_buff = std::fs::read(&config_path)
        .map_or_else(
            |err| {
                match err.kind() {
                    std::io::ErrorKind::NotFound => (),
                    err => {
                        warn!(
                            "Encountered error while searching for config file at {}: {:?}",
                            config_path, err
                        );
                        return None;
                    }
                };
                if let Ok(mut exec_path) = std::env::current_exe() {
                    exec_path.pop();
                    exec_path.push("dandelion.json");
                    match std::fs::read(&exec_path) {
                        Ok(buffer) => return Some(buffer),
                        Err(err) => warn!(
                        "Encountered error while searching config file in exec path at {:?}: {:?}",
                        exec_path, err
                    ),
                    };
                }
                return None;
            },
            |ok| Some(ok),
        )
        .unwrap_or(String::from("{}").into_bytes());
    return serde_json::from_slice(&config_buff).unwrap();
}
