use std::path::PathBuf;
use std::time::Duration;

pub const ENGINES: [&str; 3] = ["Process", "Kvm", "Cheri"];

/// Parses a duration such as `30s`, `5m` or `1h 30m`.
pub fn duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s.trim()).map_err(|e| format!("invalid duration `{s}`: {e}"))
}

/// Accepts an engine name in any letter case and returns its canonical spelling.
pub fn engine(s: &str) -> Result<String, String> {
    ENGINES
        .iter()
        .find(|e| e.eq_ignore_ascii_case(s.trim()))
        .map(|e| e.to_string())
        .ok_or_else(|| {
            format!(
                "invalid engine `{s}`; expected one of {}",
                ENGINES.join(", ")
            )
        })
}

/// Parses a byte size.
/// A plain number is bytes. A single-letter suffix (`K`, `M`, `G`, `T`) is binary (e.g. `64M` =
/// 64 MiB). Explicit units (`MB`, `MiB`, ...) are passed to `bytesize`.
pub fn size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let err = || format!("invalid size `{s}`; expected e.g. `4096`, `64M` or `1G`");
    if t.is_empty() {
        return Err(err());
    }
    if let Ok(n) = t.parse::<u64>() {
        return Ok(n);
    }
    let (num, unit) = t.split_at(t.find(|c: char| c.is_ascii_alphabetic()).ok_or_else(err)?);
    let shift = match unit {
        "k" | "K" => Some(10),
        "m" | "M" => Some(20),
        "g" | "G" => Some(30),
        "t" | "T" => Some(40),
        _ => None,
    };
    match shift {
        Some(shift) => {
            let n: u64 = num.trim().parse().map_err(|_| err())?;
            n.checked_mul(1u64 << shift).ok_or_else(err)
        }
        None => t
            .parse::<bytesize::ByteSize>()
            .map(|b| b.as_u64())
            .map_err(|_| err()),
    }
}

/// `--static <SET>:<ITEM>[#KEY]=@<FILE>`
#[derive(Debug, Clone, PartialEq)]
pub struct StaticItemArg {
    pub set: String,
    pub item: String,
    pub key: Option<u32>,
    pub file: PathBuf,
}

pub fn static_item(s: &str) -> Result<StaticItemArg, String> {
    let err = || format!("invalid static item `{s}`; expected `<SET>:<ITEM>[#KEY]=@<FILE>`");
    let (lhs, rhs) = s.split_once('=').ok_or_else(err)?;
    let (set, item_key) = lhs.split_once(':').ok_or_else(err)?;
    let file = rhs.strip_prefix('@').ok_or_else(err)?;
    if set.is_empty() || file.is_empty() {
        return Err(err());
    }
    let (item, key) = item_and_key(item_key).map_err(|e| format!("{}: {e}", err()))?;
    Ok(StaticItemArg {
        set: set.to_string(),
        item,
        key,
        file: PathBuf::from(file),
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum InputValue {
    File(PathBuf),
    Literal(String),
}

/// `--input <SET>:<ITEM>[#KEY]=@<FILE>` or `--input <SET>:<ITEM>[#KEY]=<literal>`
#[derive(Debug, Clone, PartialEq)]
pub struct InputItemArg {
    pub set: String,
    pub item: String,
    pub key: Option<u32>,
    pub value: InputValue,
}

pub fn input_item(s: &str) -> Result<InputItemArg, String> {
    let err = |why: &str| {
        format!("invalid input `{s}`: {why}; expected `<SET>:<ITEM>[#KEY]=@<FILE>` or `<SET>:<ITEM>[#KEY]=<text>`")
    };
    let (lhs, rhs) = s.split_once('=').ok_or_else(|| err("missing `=`"))?;
    let (set, item_key) = lhs.split_once(':').ok_or_else(|| err("missing `:`"))?;
    if set.is_empty() {
        return Err(err("empty set name"));
    }
    let (item, key) = item_and_key(item_key).map_err(|e| err(&e))?;
    let value = match rhs.strip_prefix('@') {
        Some("") => return Err(err("empty file path")),
        Some(path) => InputValue::File(PathBuf::from(path)),
        None => InputValue::Literal(rhs.to_string()),
    };
    Ok(InputItemArg {
        set: set.to_string(),
        item,
        key,
        value,
    })
}

/// Splits `<ITEM>[#KEY]`. Used for `--input` and for file names in `--input-dir`.
pub fn item_and_key(s: &str) -> Result<(String, Option<u32>), String> {
    match s.rsplit_once('#') {
        Some((item, key)) => {
            let key = key
                .parse::<u32>()
                .map_err(|_| format!("key `{key}` is not an unsigned 32-bit integer"))?;
            Ok((item.to_string(), Some(key)))
        }
        None => Ok((s.to_string(), None)),
    }
}
