use anyhow::{Context, Result};
use clap::Subcommand;

use crate::client::{check_status, decode_status, Client};
use crate::config::Settings;
use crate::Globals;

#[derive(Subcommand, Debug)]
pub enum TraceCmd {
    /// List the names of the timestamps a trace records, in the server's order.
    Timestamps {
        /// Print the names as the JSON array the server sends.
        #[arg(long)]
        json: bool,
    },
}

pub const TIMESTAMPS_PATH: &str = "/trace/timestamps";

pub fn run(globals: &Globals, cmd: TraceCmd) -> Result<()> {
    match cmd {
        TraceCmd::Timestamps { json } => {
            let settings = Settings::load(globals)?;
            let client = Client::new(&settings)?;
            let (status, body, _) = client.get(TIMESTAMPS_PATH)?;
            check_status(status, &body)?;
            let message = decode_status(&body)?;
            let names = parse_names(&message)?;
            if json {
                println!("{}", serde_json::to_string(&names)?);
            } else {
                print!("{}", format_names(&names));
            }
            Ok(())
        }
    }
}

pub fn parse_names(message: &str) -> Result<Vec<String>> {
    serde_json::from_str(message).with_context(|| {
        format!("expected a JSON array of timestamp names from {TIMESTAMPS_PATH}, got `{message}`")
    })
}

pub fn format_names(names: &[String]) -> String {
    // one `<index>  <name>` line per timestamp`
    let width = names.len().saturating_sub(1).to_string().len();
    names
        .iter()
        .enumerate()
        .map(|(index, name)| format!("{index:>width$}  {name}\n"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_indexed() {
        let names = parse_names("[\"A\",\"B\"]").unwrap();
        assert_eq!(format_names(&names), "0  A\n1  B\n");
        let many: Vec<String> = (0..11).map(|i| format!("T{i}")).collect();
        assert!(format_names(&many).starts_with(" 0  T0\n"));
        assert!(format_names(&many).ends_with("10  T10\n"));
    }

    #[test]
    fn non_json_message_is_an_error() {
        assert!(parse_names("Hello, World\n").is_err());
        assert!(parse_names("{\"a\": 1}").is_err());
    }
}
