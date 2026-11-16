use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use serde::Deserialize;

use crate::client::{Client, Executable, FunctionRegistration, InputSet, Item};
use crate::config::Settings;
use crate::parse;
use crate::Globals;

#[derive(Subcommand, Debug)]
pub enum FunctionCmd {
    /// Register a function binary.
    Register(FunctionRegisterArgs),
}

#[derive(Args, Debug, Default)]
pub struct FunctionRegisterArgs {
    /// Function binary to upload.
    #[arg(value_name = "BINARY", conflicts_with = "server_path")]
    pub binary: Option<PathBuf>,

    /// Function name [default: file stem of BINARY].
    #[arg(long, value_name = "N")]
    pub name: Option<String>,

    /// Engine: Process, Kvm or Cheri [default: from config].
    #[arg(long, value_name = "ENGINE", value_parser = parse::engine)]
    pub engine: Option<String>,

    /// Default context size, e.g. `64M` or `1G` [default: from config].
    #[arg(long, value_name = "SIZE", value_parser = parse::size)]
    pub context_size: Option<u64>,

    /// Path to the binary on the server's disk; BINARY is then not needed.
    #[arg(long, value_name = "PATH")]
    pub server_path: Option<String>,

    /// Input set name (repeatable, in order).
    #[arg(long = "input-set", value_name = "NAME")]
    pub input_sets: Vec<String>,

    /// Static item for an input set: `<SET>:<ITEM>[#KEY]=@<FILE>` (repeatable).
    #[arg(long = "static", value_name = "SET:ITEM[#KEY]=@FILE", value_parser = parse::static_item)]
    pub statics: Vec<parse::StaticItemArg>,

    /// Output set name (repeatable, in order).
    #[arg(long = "output-set", value_name = "NAME")]
    pub output_sets: Vec<String>,

    /// Minimum set size in bytes (repeatable).
    #[arg(long = "min-set-bytes", value_name = "N", value_parser = parse::size)]
    pub min_set_bytes: Vec<u64>,

    /// Read the registration from a TOML manifest. Flags override its values.
    #[arg(long, value_name = "FILE")]
    pub manifest: Option<PathBuf>,
}

pub const REGISTER_PATH: &str = "/register/function";

pub fn run(globals: &Globals, cmd: FunctionCmd) -> Result<()> {
    match cmd {
        FunctionCmd::Register(args) => {
            let settings = Settings::load(globals)?;
            let reg = build_registration(&args, &settings)?;
            let client = Client::new(&settings)?;
            let message = client.post_for_status(REGISTER_PATH, reg.encode())?;
            super::print_message(&message);
            Ok(())
        }
    }
}

/// The TOML manifest accepted by `--manifest`. Paths are relative to the manifest file.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub name: Option<String>,
    pub engine: Option<String>,
    pub context_size: Option<SizeValue>,
    pub binary: Option<PathBuf>,
    pub server_path: Option<String>,
    #[serde(default)]
    pub input_sets: Vec<ManifestInputSet>,
    #[serde(default)]
    pub output_sets: Vec<String>,
    #[serde(default)]
    pub min_set_bytes: Vec<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestInputSet {
    pub name: String,
    #[serde(rename = "static")]
    pub static_items: Option<Vec<ManifestStaticItem>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestStaticItem {
    pub item: String,
    pub key: Option<u32>,
    pub file: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum SizeValue {
    Bytes(u64),
    Text(String),
}

impl SizeValue {
    fn bytes(&self) -> Result<u64> {
        match self {
            SizeValue::Bytes(n) => Ok(*n),
            SizeValue::Text(s) => parse::size(s).map_err(|e| anyhow!(e)),
        }
    }
}

fn load_manifest(path: &Path) -> Result<(Manifest, PathBuf)> {
    let text = super::read_text(path)?;
    let manifest: Manifest =
        toml::from_str(&text).with_context(|| format!("invalid manifest {}", path.display()))?;
    let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    Ok((manifest, dir))
}

/// Where the function binary comes from, before it is read.
enum ExecutablePath {
    /// A path on the server's disk.
    Server(String),
    /// A file on this machine, uploaded with the registration.
    Local(PathBuf),
}

impl ExecutablePath {
    fn path(&self) -> &Path {
        match self {
            ExecutablePath::Server(p) => Path::new(p),
            ExecutablePath::Local(p) => p,
        }
    }
}

fn file_stem(p: &Path) -> Option<String> {
    p.file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .filter(|s| !s.is_empty())
}

/// Merges command-line flags (highest priority), the manifest, and the config profile (lowest
/// priority), then reads the binary and static files.
pub fn build_registration(
    args: &FunctionRegisterArgs,
    settings: &Settings,
) -> Result<FunctionRegistration> {
    let (manifest, base) = match &args.manifest {
        Some(p) => load_manifest(p)?,
        None => (Manifest::default(), PathBuf::new()),
    };
    let from_manifest = |p: &Path| base.join(p);

    // Flags win over the manifest; within each, BINARY and --server-path are exclusive.
    let executable_path = match (&args.server_path, &args.binary) {
        (Some(path), _) => Some(ExecutablePath::Server(path.clone())),
        (None, Some(path)) => Some(ExecutablePath::Local(path.clone())),
        (None, None) => match (&manifest.server_path, &manifest.binary) {
            (Some(_), Some(_)) => {
                bail!("the manifest sets both `binary` and `server_path`; keep one")
            }
            (Some(path), None) => Some(ExecutablePath::Server(path.clone())),
            (None, Some(path)) => Some(ExecutablePath::Local(from_manifest(path))),
            (None, None) => None,
        },
    };
    let Some(executable_path) = executable_path else {
        bail!("a BINARY is required unless --server-path (or a manifest) provides one");
    };

    let name = args
        .name
        .clone()
        .or(manifest.name.clone())
        .or_else(|| file_stem(executable_path.path()))
        .ok_or_else(|| anyhow!("cannot derive a function name; pass --name"))?;

    let executable = match executable_path {
        ExecutablePath::Server(path) => Executable::LocalPath(path),
        ExecutablePath::Local(path) => Executable::Binary(super::read_bytes(&path)?),
    };

    let engine_type = match (&args.engine, &manifest.engine) {
        (Some(e), _) => e.clone(),
        (None, Some(e)) => parse::engine(e).map_err(|e| anyhow!(e).context("in manifest"))?,
        (None, None) => settings.engine.clone().ok_or_else(|| {
            anyhow!("no engine given; pass --engine or set one with `dandelion config set engine <Process|Kvm|Cheri>`")
        })?,
    };

    let context_size = match (args.context_size, &manifest.context_size) {
        (Some(c), _) => c,
        (None, Some(c)) => c.bytes().context("invalid `context_size` in manifest")?,
        (None, None) => settings.context_size.ok_or_else(|| {
            anyhow!("no context size given; pass --context-size or set one with `dandelion config set context_size 64M`")
        })?,
    };

    let mut input_sets: Vec<InputSet> = if !args.input_sets.is_empty() {
        args.input_sets
            .iter()
            .map(|n| InputSet {
                name: n.clone(),
                static_items: None,
            })
            .collect()
    } else {
        manifest
            .input_sets
            .iter()
            .map(|s| {
                let static_items = s
                    .static_items
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|i| {
                                Ok(Item {
                                    identifier: i.item.clone(),
                                    key: i.key.unwrap_or(0),
                                    data: super::read_bytes(&from_manifest(&i.file))?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?;
                Ok(InputSet {
                    name: s.name.clone(),
                    static_items,
                })
            })
            .collect::<Result<_>>()?
    };

    for st in &args.statics {
        let set = input_sets
            .iter_mut()
            .find(|s| s.name == st.set)
            .ok_or_else(|| {
                anyhow!(
                    "--static refers to input set `{}`, which is not declared with --input-set",
                    st.set
                )
            })?;
        let data = super::read_bytes(&st.file)?;
        let items = set.static_items.get_or_insert_with(Vec::new);
        let key = st.key.unwrap_or(0);
        items.push(Item {
            identifier: st.item.clone(),
            key,
            data,
        });
    }

    let output_sets = if args.output_sets.is_empty() {
        manifest.output_sets
    } else {
        args.output_sets.clone()
    };
    let min_set_bytes = if args.min_set_bytes.is_empty() {
        manifest.min_set_bytes
    } else {
        args.min_set_bytes.clone()
    };

    Ok(FunctionRegistration {
        name,
        context_size,
        engine_type,
        executable,
        input_sets,
        output_sets,
        min_set_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConfigFile;
    use crate::Cli;
    use clap::Parser;

    fn settings() -> Settings {
        Settings::resolve(&Globals::default(), &ConfigFile::default(), &|_| None).unwrap()
    }

    #[test]
    fn manifest_with_overrides() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/matmul"), b"ELF").unwrap();
        std::fs::write(dir.path().join("params.bin"), b"P").unwrap();
        let manifest = dir.path().join("fn.toml");
        std::fs::write(
            &manifest,
            r#"
            name = "matmul"
            engine = "Kvm"
            context_size = "64M"
            binary = "./target/matmul"
            output_sets = ["result"]
            min_set_bytes = []

            [[input_sets]]
            name = "matrices"

            [[input_sets]]
            name = "config"
            static = [{ item = "params", file = "./params.bin" }, { item = "extra", key = 9, file = "./params.bin" }]
            "#,
        )
        .unwrap();

        let args = FunctionRegisterArgs {
            manifest: Some(manifest),
            ..Default::default()
        };
        let reg = build_registration(&args, &settings()).unwrap();
        assert_eq!(reg.name, "matmul");
        assert_eq!(reg.engine_type, "Kvm");
        assert_eq!(reg.context_size, 64 << 20);
        assert_eq!(reg.executable, Executable::Binary(b"ELF".to_vec()));
        assert_eq!(
            reg.input_sets[0],
            InputSet {
                name: "matrices".into(),
                static_items: None
            }
        );
        assert_eq!(
            reg.input_sets[1],
            InputSet {
                name: "config".into(),
                static_items: Some(vec![
                    Item {
                        identifier: "params".into(),
                        key: 0,
                        data: b"P".to_vec()
                    },
                    Item {
                        identifier: "extra".into(),
                        key: 9,
                        data: b"P".to_vec()
                    },
                ]),
            }
        );
        assert_eq!(reg.output_sets, ["result"]);

        let args = FunctionRegisterArgs {
            manifest: args.manifest.clone(),
            name: Some("other".into()),
            engine: Some("Process".into()),
            output_sets: vec!["a".into(), "b".into()],
            ..Default::default()
        };
        let reg = build_registration(&args, &settings()).unwrap();
        assert_eq!(reg.name, "other");
        assert_eq!(reg.engine_type, "Process");
        assert_eq!(reg.output_sets, ["a", "b"]);
        assert_eq!(reg.input_sets.len(), 2);
    }

    #[test]
    fn server_path_needs_no_binary() {
        let args = FunctionRegisterArgs {
            server_path: Some("/opt/fns/matmul".into()),
            engine: Some("Kvm".into()),
            context_size: Some(4096),
            ..Default::default()
        };
        let reg = build_registration(&args, &settings()).unwrap();
        assert_eq!(reg.name, "matmul");
        assert_eq!(
            reg.executable,
            Executable::LocalPath("/opt/fns/matmul".into())
        );
    }

    #[test]
    fn manifest_with_binary_and_server_path_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = dir.path().join("fn.toml");
        std::fs::write(
            &manifest,
            "binary = \"f\"\nserver_path = \"/f\"\nengine = \"Kvm\"\ncontext_size = 1\n",
        )
        .unwrap();
        let args = FunctionRegisterArgs {
            manifest: Some(manifest.clone()),
            ..Default::default()
        };
        assert!(build_registration(&args, &settings()).is_err());
        // A flag picks one and overrides the manifest.
        let args = FunctionRegisterArgs {
            manifest: Some(manifest),
            server_path: Some("/g".into()),
            ..Default::default()
        };
        assert_eq!(
            build_registration(&args, &settings()).unwrap().executable,
            Executable::LocalPath("/g".into())
        );
    }

    #[test]
    fn static_keys_default_to_zero() {
        let dir = tempfile::tempdir().unwrap();
        let f = dir.path().join("s");
        std::fs::write(&f, b"S").unwrap();
        let args = FunctionRegisterArgs {
            server_path: Some("/x".into()),
            engine: Some("Kvm".into()),
            context_size: Some(1),
            input_sets: vec!["cfg".into()],
            statics: vec![
                parse::static_item(&format!("cfg:a=@{}", f.display())).unwrap(),
                parse::static_item(&format!("cfg:b#7=@{}", f.display())).unwrap(),
                parse::static_item(&format!("cfg:c=@{}", f.display())).unwrap(),
            ],
            ..Default::default()
        };
        let reg = build_registration(&args, &settings()).unwrap();
        let keys: Vec<u32> = reg.input_sets[0]
            .static_items
            .as_ref()
            .unwrap()
            .iter()
            .map(|i| i.key)
            .collect();
        assert_eq!(keys, [0, 7, 0]);
    }

    #[test]
    fn engine_and_size_fall_back_to_config_or_fail() {
        let args = FunctionRegisterArgs {
            server_path: Some("/x".into()),
            ..Default::default()
        };
        let err = build_registration(&args, &settings()).unwrap_err();
        assert!(err.to_string().contains("engine"));

        let mut s = settings();
        s.engine = Some("Cheri".into());
        s.context_size = Some(1 << 20);
        let reg = build_registration(&args, &s).unwrap();
        assert_eq!(reg.engine_type, "Cheri");
        assert_eq!(reg.context_size, 1 << 20);
    }

    #[test]
    fn static_needs_declared_set() {
        let args = FunctionRegisterArgs {
            server_path: Some("/x".into()),
            engine: Some("Kvm".into()),
            context_size: Some(1),
            statics: vec![parse::static_item("nope:a=@/dev/null").unwrap()],
            ..Default::default()
        };
        assert!(build_registration(&args, &settings()).is_err());
    }

    #[test]
    fn binary_required() {
        let args = FunctionRegisterArgs {
            engine: Some("Kvm".into()),
            context_size: Some(1),
            ..Default::default()
        };
        assert!(build_registration(&args, &settings()).is_err());
    }

    #[test]
    fn binary_and_server_path_conflict() {
        let r = Cli::try_parse_from([
            "dandelion",
            "function",
            "register",
            "f.elf",
            "--server-path",
            "/f",
        ]);
        assert!(r.is_err());
    }
}
