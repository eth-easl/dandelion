use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::parse;
use crate::Globals;

pub const DEFAULT_URL: &str = "http://localhost:8080";
pub const DEFAULT_HOT_PATH: &str = "/hot/matmul";
pub const DEFAULT_COLD_PATH: &str = "/cold/matmul";

pub const ENV_URL: &str = "DANDELION_URL";
/// Overrides the config file location (mainly for tests and scripting).
pub const ENV_CONFIG: &str = "DANDELION_CONFIG";

pub const KEYS: [&str; 6] = [
    "url",
    "engine",
    "context_size",
    "timeout",
    "hot_path",
    "cold_path",
];

/// The config file: a flat table of the keys in [`KEYS`].
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_size: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cold_path: Option<String>,
}

impl ConfigFile {
    pub fn get(&self, key: &str) -> Result<Option<&String>> {
        Ok(match key {
            "url" => self.url.as_ref(),
            "engine" => self.engine.as_ref(),
            "context_size" => self.context_size.as_ref(),
            "timeout" => self.timeout.as_ref(),
            "hot_path" => self.hot_path.as_ref(),
            "cold_path" => self.cold_path.as_ref(),
            _ => bail!(unknown_key(key)),
        })
    }

    fn slot(&mut self, key: &str) -> Result<&mut Option<String>> {
        Ok(match key {
            "url" => &mut self.url,
            "engine" => &mut self.engine,
            "context_size" => &mut self.context_size,
            "timeout" => &mut self.timeout,
            "hot_path" => &mut self.hot_path,
            "cold_path" => &mut self.cold_path,
            _ => bail!(unknown_key(key)),
        })
    }

    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let value = validate(key, value)?;
        *self.slot(key)? = Some(value);
        Ok(())
    }

    pub fn unset(&mut self, key: &str) -> Result<()> {
        *self.slot(key)? = None;
        Ok(())
    }

    pub fn entries(&self) -> Vec<(&'static str, &String)> {
        KEYS.iter()
            .filter_map(|k| self.get(k).ok().flatten().map(|v| (*k, v)))
            .collect()
    }
}

fn unknown_key(key: &str) -> String {
    format!(
        "unknown config key `{key}`; valid keys are {}",
        KEYS.join(", ")
    )
}

pub fn default_for(key: &str) -> Option<&'static str> {
    match key {
        "url" => Some(DEFAULT_URL),
        "hot_path" => Some(DEFAULT_HOT_PATH),
        "cold_path" => Some(DEFAULT_COLD_PATH),
        _ => None,
    }
}

fn validate(key: &str, value: &str) -> Result<String> {
    match key {
        "url" => {
            reqwest::Url::parse(value).with_context(|| format!("invalid URL `{value}`"))?;
            Ok(value.to_string())
        }
        "engine" => parse::engine(value).map_err(|e| anyhow!(e)),
        "context_size" => parse::size(value)
            .map(|_| value.to_string())
            .map_err(|e| anyhow!(e)),
        "timeout" => parse::duration(value)
            .map(|_| value.to_string())
            .map_err(|e| anyhow!(e)),
        "hot_path" | "cold_path" => {
            if !value.starts_with('/') {
                bail!("`{key}` must start with `/`");
            }
            Ok(value.to_string())
        }
        _ => bail!(unknown_key(key)),
    }
}

/// `$DANDELION_CONFIG`, otherwise `$XDG_CONFIG_HOME/dandelion/config.toml`, otherwise
/// `~/.config/dandelion/config.toml`.
pub fn config_path() -> Result<PathBuf> {
    if let Some(p) = std::env::var_os(ENV_CONFIG).filter(|p| !p.is_empty()) {
        return Ok(PathBuf::from(p));
    }
    let base = match std::env::var_os("XDG_CONFIG_HOME").filter(|p| !p.is_empty()) {
        Some(p) => PathBuf::from(p),
        None => directories::BaseDirs::new()
            .ok_or_else(|| anyhow!("cannot determine the home directory"))?
            .home_dir()
            .join(".config"),
    };
    Ok(base.join("dandelion").join("config.toml"))
}

impl ConfigFile {
    pub fn load(path: &Path) -> Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(text) => toml::from_str(&text)
                .with_context(|| format!("invalid config file {}", path.display())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("cannot read {}", path.display())),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("cannot create {}", dir.display()))?;
        }
        let text = toml::to_string_pretty(self)?;
        std::fs::write(path, text).with_context(|| format!("cannot write {}", path.display()))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Settings {
    pub url: String,
    pub engine: Option<String>,
    pub context_size: Option<u64>,
    pub timeout: Option<Duration>,
    pub hot_path: String,
    pub cold_path: String,
    pub verbose: bool,
}

impl Settings {
    pub fn load(globals: &Globals) -> Result<Self> {
        let file = ConfigFile::load(&config_path()?)?;
        Self::resolve(globals, &file, &|k| std::env::var(k).ok())
    }

    pub fn resolve(
        globals: &Globals,
        file: &ConfigFile,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let ctx = |what: &str| format!("invalid `{what}` in config");

        let url = globals
            .url
            .clone()
            .or_else(|| env(ENV_URL).filter(|u| !u.is_empty()))
            .or_else(|| file.url.clone())
            .unwrap_or_else(|| DEFAULT_URL.to_string());

        let engine = file
            .engine
            .as_deref()
            .map(parse::engine)
            .transpose()
            .map_err(|e| anyhow!(e).context(ctx("engine")))?;
        let context_size = file
            .context_size
            .as_deref()
            .map(parse::size)
            .transpose()
            .map_err(|e| anyhow!(e).context(ctx("context_size")))?;
        let timeout = match globals.timeout {
            Some(t) => Some(t),
            None => file
                .timeout
                .as_deref()
                .map(parse::duration)
                .transpose()
                .map_err(|e| anyhow!(e).context(ctx("timeout")))?,
        };

        Ok(Settings {
            url,
            engine,
            context_size,
            timeout,
            hot_path: file
                .hot_path
                .clone()
                .unwrap_or_else(|| DEFAULT_HOT_PATH.into()),
            cold_path: file
                .cold_path
                .clone()
                .unwrap_or_else(|| DEFAULT_COLD_PATH.into()),
            verbose: globals.verbose,
        })
    }
}
