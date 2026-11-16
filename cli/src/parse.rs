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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(size("4096"), Ok(4096));
        assert_eq!(size("64M"), Ok(64 * 1024 * 1024));
        assert_eq!(size("64m"), Ok(64 * 1024 * 1024));
        assert_eq!(size("1G"), Ok(1 << 30));
        assert_eq!(size("2K"), Ok(2048));
        assert_eq!(size("64MiB"), Ok(64 * 1024 * 1024));
        assert_eq!(size("1 GiB"), Ok(1 << 30));
        assert_eq!(size("1MB"), Ok(1_000_000));
        assert!(size("").is_err());
        assert!(size("abc").is_err());
        assert!(size("12Q").is_err());
        assert!(size("99999999999T").is_err());
    }

    #[test]
    fn durations() {
        assert_eq!(duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(duration("5m"), Ok(Duration::from_secs(300)));
        assert!(duration("5").is_err());
    }

    #[test]
    fn engines() {
        assert_eq!(engine("kvm"), Ok("Kvm".into()));
        assert_eq!(engine("Process"), Ok("Process".into()));
        assert!(engine("wasm").is_err());
    }

    #[test]
    fn static_items() {
        assert_eq!(
            static_item("config:params=@./p.bin"),
            Ok(StaticItemArg {
                set: "config".into(),
                item: "params".into(),
                key: None,
                file: "./p.bin".into()
            })
        );
        assert_eq!(static_item("config:params#4=@p.bin").unwrap().key, Some(4));
        assert!(static_item("config:params#x=@p.bin").is_err());
        assert!(static_item("config:params=./p.bin").is_err());
        assert!(static_item("params=@p.bin").is_err());
    }

    #[test]
    fn input_items() {
        assert_eq!(
            input_item("m:a=@a.bin"),
            Ok(InputItemArg {
                set: "m".into(),
                item: "a".into(),
                key: None,
                value: InputValue::File("a.bin".into())
            })
        );
        assert_eq!(
            input_item("m:b#7=hello=world").unwrap(),
            InputItemArg {
                set: "m".into(),
                item: "b".into(),
                key: Some(7),
                value: InputValue::Literal("hello=world".into())
            }
        );
        assert_eq!(
            input_item("m:c=").unwrap().value,
            InputValue::Literal(String::new())
        );
        assert_eq!(input_item("m:=x").unwrap().item, "");
        assert!(input_item("m:a#x=1").is_err());
        assert!(input_item("m:a#-1=1").is_err());
        assert!(input_item("m:a=@").is_err());
        assert!(input_item("a=1").is_err());
        assert!(input_item(":a=1").is_err());
        assert!(input_item("m:a").is_err());
    }
}
