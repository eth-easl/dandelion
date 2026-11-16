use std::collections::BTreeMap;
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
pub const ENV_PROFILE: &str = "DANDELION_PROFILE";
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

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
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

impl Profile {
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
/// `~/.config/dandelion/config.toml` (on every platform, as in the spec).
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

    /// The profile name selected by `--profile`, or `DANDELION_PROFILE`, or `active_profile`.
    /// Returns `None` when no profile is selected at all.
    pub fn selected_profile(
        &self,
        flag: Option<&str>,
        env: &dyn Fn(&str) -> Option<String>,
    ) -> Result<Option<String>> {
        let (name, source) = if let Some(f) = flag {
            (f.to_string(), "--profile")
        } else if let Some(e) = env(ENV_PROFILE).filter(|e| !e.is_empty()) {
            (e, ENV_PROFILE)
        } else if let Some(a) = &self.active_profile {
            (a.clone(), "active_profile")
        } else {
            return Ok(None);
        };
        if !self.profiles.contains_key(&name) {
            bail!("profile `{name}` (from {source}) does not exist");
        }
        Ok(Some(name))
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
        let empty = Profile::default();
        let profile_name = file.selected_profile(globals.profile.as_deref(), env)?;
        let profile = profile_name
            .as_ref()
            .map(|n| &file.profiles[n])
            .unwrap_or(&empty);
        let ctx = |what: &str| match &profile_name {
            Some(n) => format!("invalid `{what}` in profile `{n}`"),
            None => format!("invalid `{what}`"),
        };

        let url = globals
            .url
            .clone()
            .or_else(|| env(ENV_URL).filter(|u| !u.is_empty()))
            .or_else(|| profile.url.clone())
            .unwrap_or_else(|| DEFAULT_URL.to_string());

        let engine = profile
            .engine
            .as_deref()
            .map(parse::engine)
            .transpose()
            .map_err(|e| anyhow!(e).context(ctx("engine")))?;
        let context_size = profile
            .context_size
            .as_deref()
            .map(parse::size)
            .transpose()
            .map_err(|e| anyhow!(e).context(ctx("context_size")))?;
        let timeout = match globals.timeout {
            Some(t) => Some(t),
            None => profile
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
            hot_path: profile
                .hot_path
                .clone()
                .unwrap_or_else(|| DEFAULT_HOT_PATH.into()),
            cold_path: profile
                .cold_path
                .clone()
                .unwrap_or_else(|| DEFAULT_COLD_PATH.into()),
            verbose: globals.verbose,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ConfigFile {
        toml::from_str(
            r#"
            active_profile = "local"

            [profiles.local]
            url = "http://localhost:8080"
            engine = "Process"
            context_size = "64M"

            [profiles.remote]
            url = "http://dandelion-host:8080"
            engine = "Kvm"
            timeout = "5m"
            hot_path = "/hot/v2"
            "#,
        )
        .unwrap()
    }

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |k| pairs.iter().find(|(pk, _)| pk == k).map(|(_, v)| v.clone())
    }

    #[test]
    fn defaults_without_config() {
        let s =
            Settings::resolve(&Globals::default(), &ConfigFile::default(), &env_of(&[])).unwrap();
        assert_eq!(s.url, DEFAULT_URL);
        assert_eq!(s.engine, None);
        assert_eq!(s.context_size, None);
        assert_eq!(s.timeout, None);
        assert_eq!(s.hot_path, DEFAULT_HOT_PATH);
        assert_eq!(s.cold_path, DEFAULT_COLD_PATH);
    }

    #[test]
    fn active_profile_is_used() {
        let s = Settings::resolve(&Globals::default(), &sample(), &env_of(&[])).unwrap();
        assert_eq!(s.url, "http://localhost:8080");
        assert_eq!(s.engine.as_deref(), Some("Process"));
        assert_eq!(s.context_size, Some(64 << 20));
    }

    #[test]
    fn env_profile_beats_active_profile() {
        let s = Settings::resolve(
            &Globals::default(),
            &sample(),
            &env_of(&[(ENV_PROFILE, "remote")]),
        )
        .unwrap();
        assert_eq!(s.url, "http://dandelion-host:8080");
        assert_eq!(s.engine.as_deref(), Some("Kvm"));
        assert_eq!(s.context_size, None);
        assert_eq!(s.timeout, Some(Duration::from_secs(300)));
        assert_eq!(s.hot_path, "/hot/v2");
        assert_eq!(s.cold_path, DEFAULT_COLD_PATH);
    }

    #[test]
    fn profile_flag_beats_env_profile() {
        let g = Globals {
            profile: Some("local".into()),
            ..Default::default()
        };
        let s = Settings::resolve(&g, &sample(), &env_of(&[(ENV_PROFILE, "remote")])).unwrap();
        assert_eq!(s.engine.as_deref(), Some("Process"));
    }

    #[test]
    fn env_url_beats_profile_and_flag_beats_env() {
        let env = env_of(&[(ENV_URL, "http://env:1")]);
        let s = Settings::resolve(&Globals::default(), &sample(), &env).unwrap();
        assert_eq!(s.url, "http://env:1");

        let g = Globals {
            url: Some("http://flag:2".into()),
            ..Default::default()
        };
        let s = Settings::resolve(&g, &sample(), &env).unwrap();
        assert_eq!(s.url, "http://flag:2");
    }

    #[test]
    fn timeout_flag_beats_profile() {
        let g = Globals {
            profile: Some("remote".into()),
            timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let s = Settings::resolve(&g, &sample(), &env_of(&[])).unwrap();
        assert_eq!(s.timeout, Some(Duration::from_secs(1)));
    }

    #[test]
    fn missing_profile_is_an_error() {
        let g = Globals {
            profile: Some("nope".into()),
            ..Default::default()
        };
        assert!(Settings::resolve(&g, &sample(), &env_of(&[])).is_err());
        assert!(Settings::resolve(
            &Globals::default(),
            &sample(),
            &env_of(&[(ENV_PROFILE, "nope")])
        )
        .is_err());
    }

    #[test]
    fn set_validates_and_normalises() {
        let mut p = Profile::default();
        p.set("engine", "kvm").unwrap();
        assert_eq!(p.engine.as_deref(), Some("Kvm"));
        assert!(p.set("engine", "wasm").is_err());
        assert!(p.set("context_size", "lots").is_err());
        assert!(p.set("timeout", "soon").is_err());
        assert!(p.set("hot_path", "hot").is_err());
        assert!(p.set("url", "not a url").is_err());
        assert!(p.set("colour", "blue").is_err());
        p.set("hot_path", "/hot/x").unwrap();
        p.unset("hot_path").unwrap();
        assert_eq!(p.hot_path, None);
    }

    #[test]
    fn round_trip() {
        let c = sample();
        let text = toml::to_string_pretty(&c).unwrap();
        assert_eq!(toml::from_str::<ConfigFile>(&text).unwrap(), c);
    }
}
