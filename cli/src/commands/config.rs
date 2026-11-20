use anyhow::{bail, Result};
use clap::Subcommand;

use crate::config::{self, ConfigFile, Profile};
use crate::Globals;

/// Name of the profile created by `config set` when no profile exists yet.
pub const DEFAULT_PROFILE: &str = "default";

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Print one key, or all keys, of the active profile.
    Get { key: Option<String> },
    /// Set a key in the active profile.
    Set { key: String, value: String },
    /// Remove a key from the active profile.
    Unset { key: String },
    /// Print every profile and its settings.
    List,
    /// Print the config file location.
    Path,
    /// Manage profiles.
    Profile {
        #[command(subcommand)]
        cmd: ProfileCmd,
    },
}

#[derive(Subcommand, Debug)]
pub enum ProfileCmd {
    /// List profile names; the active one is marked with `*`.
    List,
    /// Add a profile. Use the global `--url` to set its URL.
    Add { name: String },
    /// Make a profile the active one.
    Use { name: String },
    /// Remove a profile.
    Remove { name: String },
}

pub fn run(globals: &Globals, cmd: ConfigCmd) -> Result<()> {
    let path = config::config_path()?;
    let env = |k: &str| std::env::var(k).ok();

    match cmd {
        ConfigCmd::Path => println!("{}", path.display()),

        ConfigCmd::Get { key } => {
            let file = ConfigFile::load(&path)?;
            let name = file.selected_profile(globals.profile.as_deref(), &env)?;
            let empty = Profile::default();
            let profile = name.as_ref().map(|n| &file.profiles[n]).unwrap_or(&empty);
            match key {
                Some(key) => match (profile.get(&key)?, config::default_for(&key)) {
                    (Some(v), _) => println!("{v}"),
                    (None, Some(d)) => println!("{d}"),
                    (None, None) => bail!("`{key}` is not set"),
                },
                None => {
                    for (k, v) in profile.entries() {
                        println!("{k} = {v}");
                    }
                }
            }
        }

        ConfigCmd::Set { key, value } => {
            let mut file = ConfigFile::load(&path)?;
            let name = match file.selected_profile(globals.profile.as_deref(), &env)? {
                Some(n) => n,
                None => {
                    file.profiles
                        .insert(DEFAULT_PROFILE.into(), Profile::default());
                    file.active_profile = Some(DEFAULT_PROFILE.into());
                    eprintln!("created profile `{DEFAULT_PROFILE}` and made it active");
                    DEFAULT_PROFILE.into()
                }
            };
            file.profiles
                .get_mut(&name)
                .expect("profile exists")
                .set(&key, &value)?;
            file.save(&path)?;
        }

        ConfigCmd::Unset { key } => {
            let mut file = ConfigFile::load(&path)?;
            let Some(name) = file.selected_profile(globals.profile.as_deref(), &env)? else {
                bail!("no profile is active");
            };
            file.profiles
                .get_mut(&name)
                .expect("profile exists")
                .unset(&key)?;
            file.save(&path)?;
        }

        ConfigCmd::List => {
            let file = ConfigFile::load(&path)?;
            if file.profiles.is_empty() {
                println!("no profiles in {}", path.display());
            }
            for (i, (name, profile)) in file.profiles.iter().enumerate() {
                if i > 0 {
                    println!();
                }
                let active = file.active_profile.as_deref() == Some(name);
                println!("[{name}]{}", if active { " (active)" } else { "" });
                for (k, v) in profile.entries() {
                    println!("{k} = {v}");
                }
            }
        }

        ConfigCmd::Profile { cmd } => profile(globals, cmd, &path)?,
    }
    Ok(())
}

fn profile(globals: &Globals, cmd: ProfileCmd, path: &std::path::Path) -> Result<()> {
    let mut file = ConfigFile::load(path)?;
    match cmd {
        ProfileCmd::List => {
            for name in file.profiles.keys() {
                let mark = if file.active_profile.as_deref() == Some(name) {
                    "*"
                } else {
                    " "
                };
                println!("{mark} {name}");
            }
            return Ok(());
        }
        ProfileCmd::Add { name } => {
            if file.profiles.contains_key(&name) {
                bail!("profile `{name}` already exists");
            }
            let mut p = Profile::default();
            if let Some(url) = &globals.url {
                p.set("url", url)?;
            }
            file.profiles.insert(name.clone(), p);
            if file.active_profile.is_none() {
                file.active_profile = Some(name.clone());
                eprintln!("added profile `{name}` and made it active");
            }
        }
        ProfileCmd::Use { name } => {
            if !file.profiles.contains_key(&name) {
                bail!("profile `{name}` does not exist");
            }
            file.active_profile = Some(name);
        }
        ProfileCmd::Remove { name } => {
            if file.profiles.remove(&name).is_none() {
                bail!("profile `{name}` does not exist");
            }
            if file.active_profile.as_deref() == Some(name.as_str()) {
                file.active_profile = None;
                eprintln!("removed the active profile; no profile is active now");
            }
        }
    }
    file.save(path)
}
