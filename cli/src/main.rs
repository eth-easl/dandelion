mod client;
mod commands;
mod config;
mod error;
mod parse;
mod protocol;

use std::time::Duration;

use clap::Parser;

use crate::commands::Command;
use crate::error::{exit_code, ParseError};

#[derive(Parser, Debug)]
#[command(
    name = "dandelion",
    version,
    about = "Command-line client for the Dandelion runtime API"
)]
pub struct Cli {
    /// Server URL. Overrides DANDELION_URL and the config.
    #[arg(long, global = true, value_name = "URL")]
    pub url: Option<String>,

    /// Config profile to use. Overrides DANDELION_PROFILE.
    #[arg(long, global = true, value_name = "NAME")]
    pub profile: Option<String>,

    /// Request timeout, e.g. `30s` or `5m`.
    #[arg(long, global = true, value_name = "DURATION", value_parser = parse::duration)]
    pub timeout: Option<Duration>,

    /// Print the request path, status and timings to stderr.
    #[arg(short, long, global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Command,
}

/// Global options, separated from the subcommand so they can be passed around.
#[derive(Debug, Clone, Default)]
pub struct Globals {
    pub url: Option<String>,
    pub profile: Option<String>,
    pub timeout: Option<Duration>,
    pub verbose: bool,
}

impl Cli {
    pub fn globals(&self) -> Globals {
        Globals {
            url: self.url.clone(),
            profile: self.profile.clone(),
            timeout: self.timeout,
            verbose: self.verbose,
        }
    }
}

fn main() {
    std::process::exit(run());
}

/// Parses the command line, runs the command and returns the process exit code.
fn run() -> i32 {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            let _ = e.print();
            // Help and version go to stdout and are not errors.
            return if e.use_stderr() { 1 } else { 0 };
        }
    };

    let globals = cli.globals();
    match commands::run(&globals, cli.command) {
        Ok(()) => 0,
        Err(e) => {
            match e.downcast_ref::<ParseError>() {
                // The report already says what is wrong and where.
                Some(report) => eprintln!("{report}"),
                None => eprintln!("error: {e:#}"),
            }
            exit_code(&e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn global_flags_after_subcommand() {
        let cli = Cli::try_parse_from([
            "dandelion",
            "config",
            "profile",
            "add",
            "remote",
            "--url",
            "http://h:1",
        ])
        .unwrap();
        assert_eq!(cli.url.as_deref(), Some("http://h:1"));
    }
}
