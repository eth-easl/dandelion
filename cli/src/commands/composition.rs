use anyhow::{anyhow, Result};
use clap::Subcommand;
use composition::{CompositionTemplate, Registry};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};

use crate::client::{encode_composition, Client};
use crate::config::Settings;
use crate::error::ParseError;
use crate::Globals;

#[derive(Subcommand, Debug)]
pub enum CompositionCmd {
    /// Register composition(s) from a file, or from stdin with `-`.
    Register {
        #[arg(value_name = "FILE | -")]
        file: String,
    },
    /// Check locally that composition(s) parse, without contacting a server.
    Check {
        #[arg(value_name = "FILE | -", default_value = "-")]
        file: String,
    },
}

pub const REGISTER_PATH: &str = "/register/composition";

pub fn run(globals: &Globals, cmd: CompositionCmd) -> Result<()> {
    match cmd {
        CompositionCmd::Register { file } => {
            let text = super::read_text_arg(&file)?;
            let settings = Settings::load(globals)?;
            let client = Client::new(&settings)?;
            let message = client.post_for_status(REGISTER_PATH, encode_composition(&text))?;
            super::print_message(&message);
            Ok(())
        }
        CompositionCmd::Check { file } => {
            let text = super::read_text_arg(&file)?;
            let compositions = check(&text)?;
            if compositions.is_empty() {
                eprintln!("no compositions defined");
            }
            for (id, functions) in compositions {
                println!("composition {id} parsed ({functions} function applications)");
            }
            Ok(())
        }
    }
}

/// Registry that accepts every function declaration and knows no registered functions, so
/// compositions can be parsed without a server.
struct AcceptAllReg;

impl Registry for AcceptAllReg {
    fn check_declaration(
        &self,
        _id: &str,
        _params: &[&str],
        _rets: &[&str],
    ) -> DandelionResult<()> {
        Ok(())
    }

    fn id_exists(&self, _id: &str) -> bool {
        false
    }

    fn get_min_set_bytes(&self, _id: &FunctionId) -> DandelionResult<Vec<usize>> {
        Ok(Vec::new())
    }
}

/// Parses compositions with the same parser the server uses.
pub fn check(text: &str) -> Result<Vec<(String, usize)>> {
    match CompositionTemplate::parse(text, &AcceptAllReg) {
        Ok(compositions) => Ok(compositions
            .into_iter()
            .map(|(id, template, _)| (id.to_string(), template.num_functions()))
            .collect()),
        Err(err) => match err.error {
            DandelionError::Parsing(report) => Err(anyhow::Error::new(ParseError(report))),
            other => Err(anyhow!("cannot parse composition: {other:?}")),
        },
    }
}
