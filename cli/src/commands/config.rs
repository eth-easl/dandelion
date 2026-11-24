use anyhow::{bail, Result};
use clap::Subcommand;

use crate::config::{self, ConfigFile};

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Print one key, or all keys that are set.
    Get { key: Option<String> },
    /// Set a key.
    Set { key: String, value: String },
    /// Remove a key.
    Unset { key: String },
    /// Print the config file location.
    Path,
}

pub fn run(cmd: ConfigCmd) -> Result<()> {
    let path = config::config_path()?;

    match cmd {
        ConfigCmd::Path => println!("{}", path.display()),

        ConfigCmd::Get { key } => {
            let file = ConfigFile::load(&path)?;
            match key {
                Some(key) => match (file.get(&key)?, config::default_for(&key)) {
                    (Some(v), _) => println!("{v}"),
                    (None, Some(d)) => println!("{d}"),
                    (None, None) => bail!("`{key}` is not set"),
                },
                None => {
                    for (k, v) in file.entries() {
                        println!("{k} = {v}");
                    }
                }
            }
        }

        ConfigCmd::Set { key, value } => {
            let mut file = ConfigFile::load(&path)?;
            file.set(&key, &value)?;
            file.save(&path)?;
        }

        ConfigCmd::Unset { key } => {
            let mut file = ConfigFile::load(&path)?;
            file.unset(&key)?;
            file.save(&path)?;
        }
    }
    Ok(())
}
