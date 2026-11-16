mod completions;
mod composition;
mod config;
mod function;
mod invoke;
mod ping;
mod trace;

use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result};
use clap::Subcommand;

use crate::Globals;

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Manage functions.
    Function {
        #[command(subcommand)]
        cmd: function::FunctionCmd,
    },
    /// Manage compositions.
    Composition {
        #[command(subcommand)]
        cmd: composition::CompositionCmd,
    },
    /// Invoke a registered function or composition, or an inline composition.
    Invoke(invoke::InvokeArgs),
    /// Read and change the config file.
    Config {
        #[command(subcommand)]
        cmd: config::ConfigCmd,
    },
    /// Check that a Dandelion server is answering at the configured URL.
    Ping,
    /// Query the server's tracing metadata.
    Trace {
        #[command(subcommand)]
        cmd: trace::TraceCmd,
    },
    /// Generate a shell completion script.
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

pub fn run(globals: &Globals, command: Command) -> Result<()> {
    match command {
        Command::Function { cmd } => function::run(globals, cmd),
        Command::Composition { cmd } => composition::run(globals, cmd),
        Command::Invoke(args) => invoke::run(globals, args),
        Command::Config { cmd } => config::run(globals, cmd),
        Command::Ping => ping::run(globals),
        Command::Trace { cmd } => trace::run(globals, cmd),
        Command::Completions { shell } => {
            completions::run(shell);
            Ok(())
        }
    }
}

/// Reads a file, or stdin when `arg` is `-`.
fn read_text_arg(arg: &str) -> Result<String> {
    if arg == "-" {
        let mut s = String::new();
        std::io::stdin()
            .read_to_string(&mut s)
            .context("cannot read stdin")?;
        Ok(s)
    } else {
        read_text(Path::new(arg))
    }
}

fn read_text(path: &Path) -> Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("cannot read {}", path.display()))
}

fn read_bytes(path: &Path) -> Result<Vec<u8>> {
    std::fs::read(path).with_context(|| format!("cannot read {}", path.display()))
}

/// Prints the server's status message, e.g. `Function registered`.
fn print_message(message: &str) {
    let text = message.trim_end();
    if !text.is_empty() {
        println!("{text}");
    }
}
