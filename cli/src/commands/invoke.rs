use std::collections::HashSet;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use clap::Args;

use crate::client::{
    decode_invoke_response, encode_invoke, format_size, Client, InvokeResponse, InvokeTarget, Item,
    Set,
};
use crate::config::Settings;
use crate::parse::{self, InputItemArg, InputValue};
use crate::Globals;

/// Items up to this many bytes are shown inline in the summary (if they are UTF-8).
const INLINE_LIMIT: usize = 256;

#[derive(Args, Debug, Default)]
pub struct InvokeArgs {
    /// Name of the registered function or composition.
    #[arg(
        value_name = "NAME",
        required_unless_present = "composition",
        conflicts_with = "composition"
    )]
    pub name: Option<String>,

    /// Run an unregistered composition from a file, or from stdin with `-`.
    #[arg(long, value_name = "FILE | -")]
    pub composition: Option<String>,

    /// Input item: `<SET>:<ITEM>[#KEY]=@<FILE>` or `<SET>:<ITEM>[#KEY]=<literal>`.
    #[arg(long = "input", value_name = "SET:ITEM[#KEY]=VALUE", value_parser = parse::input_item)]
    pub inputs: Vec<parse::InputItemArg>,

    /// Directory with one subdirectory per set and one file per item (`<ITEM>[#KEY]`).
    #[arg(long, value_name = "DIR", conflicts_with = "inputs")]
    pub input_dir: Option<PathBuf>,

    /// Use the cold invoke path.
    #[arg(long)]
    pub no_cache: bool,

    /// Write each output item to DIR/<set>/<item>.
    #[arg(long, value_name = "DIR", group = "output_mode")]
    pub out_dir: Option<PathBuf>,

    /// Print the response as one JSON document.
    #[arg(long, group = "output_mode")]
    pub json: bool,

    /// Write only this item's bytes to stdout. SET is the set index; ITEM is the identifier or index.
    #[arg(long, value_name = "SET:ITEM", group = "output_mode")]
    pub raw: Option<String>,
}

pub fn run(globals: &Globals, args: InvokeArgs) -> Result<()> {
    let target = match (&args.name, &args.composition) {
        (Some(n), None) => InvokeTarget::Name(n.clone()),
        (None, Some(c)) => InvokeTarget::Composition(super::read_text_arg(c)?),
        _ => bail!("give either a NAME or --composition"),
    };
    let sets = match &args.input_dir {
        Some(dir) => sets_from_dir(dir)?,
        None => sets_from_inputs(&args.inputs)?,
    };

    let settings = Settings::load(globals)?;
    let path = if args.no_cache {
        &settings.cold_path
    } else {
        &settings.hot_path
    };
    let client = Client::new(&settings)?;
    let body = client.post(path, encode_invoke(&target, &sets))?;
    let resp = decode_invoke_response(&body)?;

    if let Some(spec) = &args.raw {
        let mut stdout = std::io::stdout().lock();
        stdout.write_all(&resp.find_item(spec)?.data)?;
        stdout.flush()?;
    } else if args.json {
        println!("{}", serde_json::to_string_pretty(&resp.to_json())?);
    } else if let Some(dir) = &args.out_dir {
        let written = resp.write_out_dir(dir)?;
        println!("wrote {} file(s) to {}", written.len(), dir.display());
    } else {
        print!("{resp}");
    }
    Ok(())
}

pub fn sets_from_inputs(inputs: &[InputItemArg]) -> Result<Vec<Set>> {
    let mut names: Vec<&str> = Vec::new();
    let mut sets: Vec<Set> = Vec::new();
    for input in inputs {
        let idx = match names.iter().position(|n| *n == input.set) {
            Some(i) => i,
            None => {
                names.push(&input.set);
                sets.push(Set::default());
                sets.len() - 1
            }
        };
        let set = &mut sets[idx];
        let data = match &input.value {
            InputValue::File(p) => super::read_bytes(p)?,
            InputValue::Literal(s) => s.clone().into_bytes(),
        };
        let key = input.key.unwrap_or(0u32);
        set.items.push(Item {
            identifier: input.item.clone(),
            key,
            data,
        });
    }
    Ok(sets)
}

fn sorted_entries(dir: &Path) -> Result<Vec<std::fs::DirEntry>> {
    let mut entries = std::fs::read_dir(dir)
        .with_context(|| format!("cannot read {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("cannot read {}", dir.display()))?;
    // ignore hidden files (starting with '.')
    entries.retain(|e| !e.file_name().to_string_lossy().starts_with('.'));
    entries.sort_by_key(|e| e.file_name());
    Ok(entries)
}

pub fn sets_from_dir(dir: &Path) -> Result<Vec<Set>> {
    let mut sets = Vec::new();
    for set_entry in sorted_entries(dir)? {
        let set_path = set_entry.path();
        if !set_path.is_dir() {
            bail!(
                "{} is not a directory; --input-dir must contain only one subdirectory per set",
                set_path.display()
            );
        }
        let mut set = Set::default();
        for item_entry in sorted_entries(&set_path)? {
            let item_path = item_entry.path();
            if !item_path.is_file() {
                bail!("{} is not a file", item_path.display());
            }
            let file_name = item_entry.file_name();
            let file_name = file_name
                .to_str()
                .with_context(|| format!("{} is not valid UTF-8", item_path.display()))?;
            let (identifier, key) = parse::item_and_key(file_name)
                .map_err(|e| anyhow::anyhow!("{}: {e}", item_path.display()))?;
            let key = key.unwrap_or(0);
            set.items.push(Item {
                identifier,
                key,
                data: super::read_bytes(&item_path)?,
            });
        }
        sets.push(set);
    }
    Ok(sets)
}

fn inline_text(data: &[u8]) -> Option<&str> {
    if data.len() > INLINE_LIMIT {
        return None;
    }
    let s = std::str::from_utf8(data).ok()?;
    let printable = s
        .chars()
        .all(|c| !c.is_control() || c == '\n' || c == '\t' || c == '\r');
    printable.then_some(s)
}

/// Turns an item identifier into a safe file name.
fn file_name_for(item: &Item, index: usize) -> String {
    let cleaned: String = item
        .identifier
        .chars()
        .map(|c| {
            if c == '/' || c == '\\' || c == '\0' {
                '_'
            } else {
                c
            }
        })
        .collect();
    if cleaned.is_empty() || cleaned == "." || cleaned == ".." {
        format!("item-{index}")
    } else {
        cleaned
    }
}

/// Human-readable summary of every set and item, with short text items and the trace inline.
impl fmt::Display for InvokeResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.sets.is_empty() {
            writeln!(f, "(no output sets)")?;
        }
        for (si, set) in self.sets.iter().enumerate() {
            let n = set.items.len();
            writeln!(f, "set {si}: {n} item{}", if n == 1 { "" } else { "s" })?;
            for item in &set.items {
                writeln!(
                    f,
                    "  {:?}  key={}  size={}",
                    item.identifier,
                    item.key,
                    format_size(item.data.len() as u64)
                )?;
                if let Some(text) = inline_text(&item.data) {
                    for line in text.trim_end_matches('\n').lines() {
                        writeln!(f, "    | {line}")?;
                    }
                }
            }
        }
        if let Some(trace) = &self.trace {
            writeln!(f, "trace:")?;
            let pretty = serde_json::from_str::<serde_json::Value>(trace)
                .ok()
                .and_then(|v| serde_json::to_string_pretty(&v).ok())
                .unwrap_or_else(|| trace.clone());
            for line in pretty.lines() {
                writeln!(f, "  {line}")?;
            }
        }
        Ok(())
    }
}

impl InvokeResponse {
    /// `{ sets: [...], trace: <parsed JSON | null> }` with base64 item data.
    pub fn to_json(&self) -> serde_json::Value {
        let b64 = base64::engine::general_purpose::STANDARD;
        let sets: Vec<serde_json::Value> = self
            .sets
            .iter()
            .map(|s| {
                let items: Vec<serde_json::Value> = s
                    .items
                    .iter()
                    .map(|i| {
                        serde_json::json!({
                            "identifier": i.identifier,
                            "key": i.key,
                            "data": b64.encode(&i.data),
                        })
                    })
                    .collect();
                serde_json::json!({ "items": items })
            })
            .collect();
        let trace = match &self.trace {
            None => serde_json::Value::Null,
            // Fall back to the raw string if the server sends something that is not JSON.
            Some(trace) => serde_json::from_str(trace)
                .unwrap_or_else(|_| serde_json::Value::String(trace.clone())),
        };
        serde_json::json!({ "sets": sets, "trace": trace })
    }

    /// Writes each item to `DIR/<set index>/<item>`, and the trace to `DIR/trace.json`.
    /// Returns the paths written.
    pub fn write_out_dir(&self, dir: &Path) -> Result<Vec<PathBuf>> {
        let mut written = Vec::new();
        std::fs::create_dir_all(dir).with_context(|| format!("cannot create {}", dir.display()))?;
        for (si, set) in self.sets.iter().enumerate() {
            let set_dir = dir.join(si.to_string());
            std::fs::create_dir_all(&set_dir)
                .with_context(|| format!("cannot create {}", set_dir.display()))?;
            let mut used = HashSet::new();
            for (ii, item) in set.items.iter().enumerate() {
                let mut name = file_name_for(item, ii);
                if !used.insert(name.clone()) {
                    name = format!("{name}#{ii}");
                    used.insert(name.clone());
                }
                let path = set_dir.join(&name);
                std::fs::write(&path, &item.data)
                    .with_context(|| format!("cannot write {}", path.display()))?;
                written.push(path);
            }
        }
        if let Some(trace) = &self.trace {
            let path = dir.join("trace.json");
            std::fs::write(&path, trace)
                .with_context(|| format!("cannot write {}", path.display()))?;
            written.push(path);
        }
        Ok(written)
    }

    /// Finds the item for `--raw <SET>:<ITEM>` (SET is the set index, ITEM is matched against the
    /// identifiers first and is otherwise taken as the item index).
    pub fn find_item(&self, spec: &str) -> Result<&Item> {
        let (set, item) = spec
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid --raw `{spec}`; expected `<SET>:<ITEM>`"))?;
        let si: usize = set
            .parse()
            .map_err(|_| anyhow!("invalid --raw `{spec}`: the set must be an index (0, 1, ...)"))?;
        let set_ref = self.sets.get(si).ok_or_else(|| {
            anyhow!(
                "response has {} set(s); there is no set {si}",
                self.sets.len()
            )
        })?;
        if let Some(found) = set_ref.items.iter().find(|i| i.identifier == item) {
            return Ok(found);
        }
        if let Some(found) = item
            .parse::<usize>()
            .ok()
            .and_then(|ii| set_ref.items.get(ii))
        {
            return Ok(found);
        }
        let ids: Vec<String> = set_ref
            .items
            .iter()
            .map(|i| format!("{:?}", i.identifier))
            .collect();
        bail!("set {si} has no item `{item}`; items: [{}]", ids.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Cli;
    use clap::Parser;

    #[test]
    fn inputs_grouped_in_first_appearance_order() {
        let inputs: Vec<InputItemArg> = ["b:x=1", "a:y#9=2", "b:z=3", "a:w=4"]
            .iter()
            .map(|s| parse::input_item(s).unwrap())
            .collect();
        let sets = sets_from_inputs(&inputs).unwrap();
        assert_eq!(sets.len(), 2);
        let ids: Vec<(&str, u32)> = sets[0]
            .items
            .iter()
            .map(|i| (i.identifier.as_str(), i.key))
            .collect();
        assert_eq!(ids, [("x", 0), ("z", 0)]);
        let ids: Vec<(&str, u32)> = sets[1]
            .items
            .iter()
            .map(|i| (i.identifier.as_str(), i.key))
            .collect();
        assert_eq!(ids, [("y", 9), ("w", 0)]);
        assert_eq!(sets[1].items[1].data, b"4");
    }

    #[test]
    fn dir_layout() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("b_set")).unwrap();
        std::fs::create_dir_all(d.path().join("a_set")).unwrap();
        std::fs::write(d.path().join("a_set/second#5"), b"2").unwrap();
        std::fs::write(d.path().join("a_set/first"), b"1").unwrap();
        std::fs::write(d.path().join("a_set/.DS_Store"), b"").unwrap();
        std::fs::write(d.path().join("b_set/only"), b"x").unwrap();
        let sets = sets_from_dir(d.path()).unwrap();
        assert_eq!(sets.len(), 2);
        assert_eq!(
            sets[0].items[0],
            Item {
                identifier: "first".into(),
                key: 0,
                data: b"1".to_vec()
            }
        );
        assert_eq!(
            sets[0].items[1],
            Item {
                identifier: "second".into(),
                key: 5,
                data: b"2".to_vec()
            }
        );
        assert_eq!(sets[1].items[0].identifier, "only");

        std::fs::write(d.path().join("stray.txt"), b"").unwrap();
        assert!(sets_from_dir(d.path()).is_err());
    }

    #[test]
    fn name_and_composition_conflict() {
        let r = Cli::try_parse_from(["dandelion", "invoke", "f", "--composition", "c.txt"]);
        assert!(r.is_err());
        let r = Cli::try_parse_from(["dandelion", "invoke"]);
        assert!(r.is_err());
    }

    #[test]
    fn output_modes_are_exclusive() {
        let r = Cli::try_parse_from(["dandelion", "invoke", "f", "--json", "--raw", "0:a"]);
        assert!(r.is_err());
    }

    fn resp() -> InvokeResponse {
        InvokeResponse {
            sets: vec![
                Set {
                    items: vec![
                        Item {
                            identifier: "msg".into(),
                            key: 0,
                            data: b"hello\n".to_vec(),
                        },
                        Item {
                            identifier: "bin".into(),
                            key: 1,
                            data: vec![0, 159, 146, 150],
                        },
                    ],
                },
                Set {
                    items: vec![Item {
                        identifier: "../x".into(),
                        key: 0,
                        data: b"a".to_vec(),
                    }],
                },
            ],
            trace: Some("{\"t\":[1,2]}".into()),
        }
    }

    #[test]
    fn summary_shows_text_but_not_binary() {
        let s = resp().to_string();
        assert!(s.contains("set 0: 2 items"));
        assert!(s.contains("\"msg\"  key=0  size=6 B"));
        assert!(s.contains("    | hello"));
        assert!(s.contains("\"bin\"  key=1  size=4 B"));
        assert!(s.contains("trace:"));
        assert!(s.contains("\"t\": ["));
    }

    #[test]
    fn json_output() {
        let j = resp().to_json();
        assert_eq!(j["sets"][0]["items"][0]["data"], "aGVsbG8K");
        assert_eq!(j["trace"]["t"][1], 2);
        let no_trace = InvokeResponse {
            trace: None,
            ..resp()
        };
        assert!(no_trace.to_json()["trace"].is_null());
    }

    #[test]
    fn find_item_for_raw() {
        let r = resp();
        assert_eq!(r.find_item("0:msg").unwrap().data, b"hello\n");
        assert_eq!(r.find_item("0:1").unwrap().identifier, "bin");
        assert!(r.find_item("2:msg").is_err());
        assert!(r.find_item("0:nope").is_err());
        assert!(r.find_item("msg").is_err());
    }

    #[test]
    fn out_dir_is_safe() {
        let dir = tempfile::tempdir().unwrap();
        let written = resp().write_out_dir(dir.path()).unwrap();
        assert_eq!(std::fs::read(dir.path().join("0/msg")).unwrap(), b"hello\n");
        assert_eq!(std::fs::read(dir.path().join("1/.._x")).unwrap(), b"a");
        assert!(dir.path().join("trace.json").exists());
        assert_eq!(written.len(), 4);
    }
}
