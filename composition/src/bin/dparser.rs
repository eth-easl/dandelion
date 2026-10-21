//! Checks whether a composition parses.
//!
//! Reads the composition from the file given as the first argument, or from stdin if no argument
//! is given. Function declarations are not checked against a registry, every declaration is
//! accepted as is.

use composition::{CompositionTemplate, Registry};
use dandelion_commons::{DandelionError, DandelionResult, FunctionId};

/// Registry that accepts all function declarations and knows no registered functions.
struct NoRegistry;

impl Registry for NoRegistry {
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
    
    fn composition_signature(&self, _id: &str) -> Option<(usize, usize)> {
        None
    }
}

fn main() {
    let input = match std::env::args().nth(1) {
        Some(path) => std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read input file {}: {}", path, e)),
        None => std::io::read_to_string(std::io::stdin().lock())
            .map_err(|e| format!("cannot read input: {}", e)),
    };
    let input = input.unwrap_or_else(|msg| {
        eprintln!("{}", msg);
        std::process::exit(1);
    });

    match CompositionTemplate::parse(&input, &NoRegistry) {
        Ok(compositions) => {
            for (id, template, _) in compositions {
                println!(
                    "composition {} parsed ({} function applications)",
                    id,
                    template.num_functions()
                );
            }
        }
        Err(err) => {
            match err.error {
                DandelionError::Parsing(report) => eprint!("{}", report),
                other => eprintln!("{:?}", other),
            }
            std::process::exit(1);
        }
    }
}
