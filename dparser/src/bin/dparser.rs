use ariadne::Report;
use dparser::{print_errors, Span};

fn main() {
    let stdin = std::io::stdin().lock();
    let input = match std::io::read_to_string(stdin) {
        Ok(input) => input,
        Err(e) => {
            Report::<Span>::build(ariadne::ReportKind::Error, (), 0)
                .with_message(format!("cannot read input: {}", e))
                .finish()
                .eprint(ariadne::Source::from(""))
                .unwrap();
            std::process::exit(1);
        }
    };
    let program = match dparser::parse(&input).map_err(|errs| print_errors(&input, errs)) {
        Ok(program) => program,
        Err(()) => {
            std::process::exit(1);
        }
    };
    dbg!(&program);
}
