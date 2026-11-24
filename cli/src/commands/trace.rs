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
