use clap::CommandFactory;

use crate::Cli;

pub fn run(shell: clap_complete::Shell) {
    let mut cmd = Cli::command();
    clap_complete::generate(shell, &mut cmd, "dcli", &mut std::io::stdout());
}
