use ariadne::Report;
use dparser::{print_errors, Span};

trait ToReport<T> {
    fn rep(self) -> Result<T, Report<'static>>;
}

impl<T> ToReport<T> for Result<T, String> {
    fn rep(self) -> Result<T, Report<'static>> {
        self.map_err(|e| {
            Report::<Span>::build(ariadne::ReportKind::Error, (), 0)
                .with_message(e)
                .finish()
        })
    }
}

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
