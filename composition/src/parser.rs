mod composition_builder;

use crate::{
    parser::composition_builder::CompositionBuilder,
    sharding::{JoinStrategy, Sharding},
    CompositionTemplate,
};
use ariadne::{Color, Config, Label, Report, ReportKind, Source};
use chumsky::{prelude::*, Parser as ChumskyParser};
use dandelion_commons::{DandelionResult, FunctionId};
use std::ops::Range;

/// Functions from the function registry side required to check the semantic correctness.
pub trait Registry {
    /// Confirms the declared function is registered with matching params and returns.
    fn check_declaration(
        &self,
        id: &str,
        params: &Vec<&str>,
        rets: &Vec<&str>,
    ) -> DandelionResult<()>;
    /// Simple lookup whether an identifier is already registered.
    fn id_exists(&self, id: &str) -> bool;
}

/// A byte-offset range into the source that produced a node.
type Span = Range<usize>;

#[derive(Clone, Debug)]
struct Spanned<T> {
    span: Span,
    v: T,
}

impl<T> Spanned<T> {
    fn new(v: T, span: Span) -> Self {
        Spanned { v, span }
    }
}

/// An simple string slice identifier in the input.
type Ident<'src> = &'src str;

/// Wrapper to get back a Ident<'src> alias.
fn ident<'src>(
) -> impl ChumskyParser<'src, &'src str, Ident<'src>, extra::Err<Rich<'src, char>>> + Clone {
    text::ident()
}

#[derive(Debug)]
struct FunctionDecl<'src> {
    name: Ident<'src>,
    params: Vec<Ident<'src>>,
    returns: Vec<Ident<'src>>,
}
type SpannedFunctionDecl<'src> = Spanned<FunctionDecl<'src>>;

#[derive(Debug)]
struct InputDescriptor<'src> {
    name: Ident<'src>,
    ident: Ident<'src>,
    sharding: Sharding,
    optional: bool,
    // TODO loop_cond: LoopCond,
}
type SpannedInputDescriptor<'src> = Spanned<InputDescriptor<'src>>;

#[derive(Debug)]
struct OutputDescriptor<'src> {
    ident: Ident<'src>,
    name: Ident<'src>,
    // TODO feedback: bool,
}
type SpannedOutputDescriptor<'src> = Spanned<OutputDescriptor<'src>>;

#[derive(Debug, Clone)]
struct FunctionApplicationJoinStrategy<'src> {
    join_strategy_order: Vec<Ident<'src>>,
    join_strategies: Vec<JoinStrategy>,
}

#[derive(Debug)]
struct FunctionApplication<'src> {
    name: Ident<'src>,
    args: Vec<SpannedInputDescriptor<'src>>,
    rets: Vec<SpannedOutputDescriptor<'src>>,
    join_strategy: Option<FunctionApplicationJoinStrategy<'src>>,
}
type SpannedFunctionApplication<'src> = Spanned<FunctionApplication<'src>>;

// TODO: not constructed yet - loop_ parser below is not implemented, same as in dparser.
#[derive(Debug)]
#[allow(dead_code)]
enum LoopCond {
    None,
    UntilEmpty,
    UntilItemEmpty,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Loop<'src> {
    args: Vec<SpannedInputDescriptor<'src>>,
    rets: Vec<SpannedOutputDescriptor<'src>>,
    statements: Vec<SpannedFunctionApplication<'src>>,
}
#[allow(dead_code)]
type SpannedLoop<'src> = Spanned<Loop<'src>>;

#[derive(Debug)]
enum Statement<'src> {
    FunctionApplication(SpannedFunctionApplication<'src>),
    #[allow(dead_code)]
    Loop(SpannedLoop<'src>),
}

#[derive(Debug)]
struct CompositionDecl<'src> {
    name: Ident<'src>,
    params: Vec<Ident<'src>>,
    returns: Vec<Ident<'src>>,
    statements: Vec<Statement<'src>>,
}
type SpannedCompositionDecl<'src> = Spanned<CompositionDecl<'src>>;

#[derive(Debug)]
enum Item<'src> {
    FunctionDecl(SpannedFunctionDecl<'src>),
    CompositionDecl(SpannedCompositionDecl<'src>),
}

#[derive(Debug)]
struct Module<'src>(Vec<Item<'src>>);

pub(crate) struct Parser<'src, R: Registry> {
    parser: Boxed<'src, 'src, &'src str, Module<'src>, extra::Err<Rich<'src, char>>>,
    registry: &'src R,
}

impl<'src, R: Registry> Parser<'src, R> {
    pub(crate) fn new(registry: &'src R) -> Self {
        let function_decl = just("function")
            .padded()
            .ignore_then(ident().padded())
            .then(
                ident()
                    .padded()
                    .separated_by(just(','))
                    .collect::<Vec<_>>()
                    .delimited_by(just('('), just(')')),
            )
            .then_ignore(just("=>").padded())
            .then(
                ident()
                    .padded()
                    .separated_by(just(','))
                    .collect::<Vec<_>>()
                    .delimited_by(just('('), just(')')),
            )
            .then_ignore(just(";").padded())
            .map(|((name, params), returns)| FunctionDecl {
                name,
                params,
                returns,
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let input_descriptor = ident()
            .padded()
            .then_ignore(just('=').padded())
            .then(just("optional").padded().or_not())
            .then(
                (just("all")
                    .or(just("keyed"))
                    .or(just("each"))
                    .or(just("anyKeyed"))
                    .or(just("anyEach")))
                .map(|sharding| match sharding {
                    "all" => Sharding::All,
                    "keyed" => Sharding::Keyed(JoinStrategy::Cross),
                    "each" => Sharding::Each,
                    "anyKeyed" => Sharding::AnyKeyed(JoinStrategy::Cross),
                    "anyEach" => Sharding::AnyEach,
                    _ => unreachable!(),
                })
                .padded(),
            )
            .then(ident().padded())
            .map(|(((name, optional), sharding), ident)| InputDescriptor {
                name,
                ident,
                sharding,
                optional: optional.is_some(),
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let output_descriptor = ident()
            .padded()
            .then_ignore(just("=").padded())
            .then(ident().padded())
            .map(|(ident, name)| OutputDescriptor {
                ident,
                name,
                // TODO feedback: false,
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let name_followed_by_strategy = ident()
            .padded()
            .then(
                just("cross")
                    .or(just("inner"))
                    .or(just("left"))
                    .or(just("right"))
                    .or(just("full"))
                    .map(|sharding| match sharding {
                        "cross" => JoinStrategy::Cross,
                        "inner" => JoinStrategy::Inner,
                        "left" => JoinStrategy::Left,
                        "right" => JoinStrategy::Right,
                        "full" => JoinStrategy::Outer,
                        _ => unreachable!(),
                    }),
            )
            .padded();

        let by_join_strategy = just("by").padded().ignore_then(
            name_followed_by_strategy
                .repeated()
                .collect::<Vec<(Ident<'src>, JoinStrategy)>>()
                .padded()
                .then(ident())
                .map(|(xs, x)| {
                    let (mut names, strats): (Vec<_>, Vec<_>) = xs.into_iter().unzip();
                    names.push(x);
                    FunctionApplicationJoinStrategy {
                        join_strategy_order: names,
                        join_strategies: strats,
                    }
                }),
        );

        let function_application = ident()
            .then(
                input_descriptor
                    .separated_by(just(',').padded())
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded(), just(')').padded()),
            )
            .padded()
            .then_ignore(just("=>").padded())
            .then(
                output_descriptor
                    .separated_by(just(',').padded())
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded(), just(')').padded()),
            )
            .padded()
            .then(by_join_strategy.or_not())
            .map(
                |(((name, args), rets), join_strategy)| FunctionApplication {
                    name,
                    args,
                    rets,
                    join_strategy,
                },
            )
            .map_with(|v, e| Spanned::new(v, e.span().into_range()))
            .map(Statement::FunctionApplication);

        let statement = function_application.then_ignore(just(';').padded());

        // TODO: loop syntax (`loop { input_descriptor* => function_application* => output_descriptor* }`)
        // is not parsed yet.

        let composition = just("composition")
            .padded()
            .ignore_then(ident().padded())
            .then(
                ident()
                    .padded()
                    .separated_by(just(',').padded())
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded(), just(')').padded())
                    .padded(),
            )
            .then_ignore(just("=>").padded())
            .then(
                ident()
                    .padded()
                    .separated_by(just(',').padded())
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded(), just(')').padded()),
            )
            .padded()
            .then(
                statement
                    .repeated()
                    .collect::<Vec<_>>()
                    .delimited_by(just('{').padded(), just('}').padded()),
            )
            .map(|(((name, params), returns), statements)| CompositionDecl {
                name,
                params,
                returns,
                statements,
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let parser = (function_decl
            .map(Item::FunctionDecl)
            .or(composition.map(Item::CompositionDecl)))
        .repeated()
        .collect::<Vec<_>>()
        .padded()
        .then_ignore(end())
        .map(Module)
        .boxed();

        Parser { parser, registry }
    }

    pub(crate) fn parse(
        &self,
        src: &'src str,
    ) -> Result<Vec<(FunctionId, CompositionTemplate)>, Vec<ErrorDiagnostic>> {
        // parse module
        let module = self.parser.parse(src).into_result().map_err(|errs| {
            errs.into_iter()
                .map(ErrorDiagnostic::from_syntax_error)
                .collect::<Vec<_>>()
        })?;

        // convert to composition
        let mut builder = CompositionBuilder::new(self.registry);
        for item in module.0.iter() {
            match item {
                Item::FunctionDecl(fdecl) => builder.add_declaration(fdecl).map_err(|e| vec![e])?,
                Item::CompositionDecl(cdecl) => {
                    builder.add_composition(cdecl).map_err(|e| vec![e])?
                }
            }
        }
        Ok(builder.collect())
    }
}

pub(crate) struct ErrorDiagnostic {
    span: Span,
    message: String,
}

impl ErrorDiagnostic {
    fn new(span: Span, message: String) -> Self {
        ErrorDiagnostic { span, message }
    }

    fn from_syntax_error(e: Rich<'_, char>) -> Self {
        ErrorDiagnostic {
            span: e.span().into_range(),
            message: e.to_string(),
        }
    }
}

/// Renders a list of diagnostics into a single string carrying ANSI color escapes.
pub(crate) fn render_diagnostics(src: &str, diagnostics: &[ErrorDiagnostic]) -> String {
    let source = Source::from(src);
    let mut buf = Vec::new();
    for diagnostic in diagnostics {
        Report::build(ReportKind::Error, diagnostic.span.clone())
            .with_config(Config::default())
            .with_message(&diagnostic.message)
            .with_label(
                Label::new(diagnostic.span.clone())
                    .with_message(&diagnostic.message)
                    .with_color(Color::Red),
            )
            .finish()
            .write(&source, &mut buf)
            .expect("writing to an in-memory buffer never fails");
    }
    String::from_utf8(buf).expect("ariadne only ever emits valid utf-8")
}

fn print_errors(src: &str, diagnostics: &[ErrorDiagnostic]) {
    eprint!("{}", render_diagnostics(src, diagnostics));
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn function_test() {
//         let src = r#"
//         function Access( AccessToken) => (HTTPRequest);
//         function FanOut(HTTPResponse , Topic) => (HTTPRequests);
//         function Render(HTTPResponses )   => (HTMLOutput);

//         function HTTP(Request) => ( Response) ;
//         "#;
//         match Parser::new().parse_module(src) {
//             Ok(_) => (),
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//     }

//     #[test]
//     fn composition_test() {
//         let src = r#"
//         function Access( AccessToken) => (HTTPRequest);
//         function FanOut(HTTPResponse , Topic) => (HTTPRequests);
//         function Render(HTTPResponses )   => (HTMLOutput);

//         function HTTP(Request) => ( Response) ;

//         composition RenderLogs(InputAccessToken, InputTopic) => (OutHTMLOutput) {
//             Access(AccessToken = all InputAccessToken) => (AuthRequest = HTTPRequest);
//             HTTP(Request = each AuthRequest) => (AuthResponse = Response);
//             FanOut(HTTPResponse = all AuthResponse, Topic = all InputTopic) => (LogRequests = HTTPRequests);
//             HTTP(Request = each LogRequests) => (LogResponses = Response);
//             Render(HTTPResponses = all LogResponses) => (OutHTMLOutput = HTMLOutput);
//         }
//     "#;
//         match Parser::new().parse_module(src) {
//             Ok(_) => (),
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//     }

//     #[test]
//     fn simple_test() {
//         let src = r#"
//         function HTTP(Request) => (Response);
//         function MakePNGGrayscaleS3 (S3GetResponse) => (S3PutRequest);

//         composition MakePNGGrayscale (S3GetRequest) => () {
//             HTTP(Request = keyed S3GetRequest) => (ToProcess = Response);
//             MakePNGGrayscaleS3 (  S3GetResponse = keyed ToProcess ) => (PutRequest = S3PutRequest) ;
//             HTTP ( Request = keyed PutRequest) => ( );
//         }
//     "#;
//         let _ = match Parser::new().parse_module(src) {
//             Ok(m) => m,
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//         // TODO add checks on module
//     }

//     #[test]
//     fn optional_test() {
//         let src = r#"
//         function HTTP(Request) => (Response);
//         function MakePNGGrayscaleS3 (S3GetResponse) => (S3PutRequest);

//         composition MakePNGGrayscale (S3GetRequest) => () {
//             HTTP(Request = optional keyed S3GetRequest) => (ToProcess = Response);
//             MakePNGGrayscaleS3 (  S3GetResponse =optional      keyed ToProcess ) => (PutRequest = S3PutRequest) ;
//             HTTP ( Request = keyed PutRequest) => ( );
//         }
//     "#;
//         let _ = match Parser::new().parse_module(src) {
//             Ok(m) => m,
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//         // TODO add checks on module
//     }

//     #[test]
//     fn sharding_test() {
//         let src = r#"
//         function FunA (A, B) => (C);
//         function FunB (A, B,C) => (D);
//         function FunC (D) => (E);

//         composition Test (InputA, InputB) => (OutputE) {
//             FunA (
//                 A = keyed InputA,
//                 B = keyed InputB
//             ) =>
//                 (InterC = C)
//             ;

//             FunB (
//                 A = keyed InputA,
//                 B = anyKeyed InputB,
//                 C = anyEach InputC
//             ) => (
//                 InterD = D
//             );

//             FunC
//                 ( D = all InterD ) =>
//                 ( OutputE = E);
//         }
//     "#;
//         let module = match Parser::new().parse_module(src) {
//             Ok(m) => m,
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//         dbg!(&module);
//         let Module(items) = module;
//         let mut functions: Vec<SpannedFunctionDecl<'_>> = Vec::new();
//         let mut compositions: Vec<SpannedCompositionDecl<'_>> = Vec::new();
//         for i in items.into_iter() {
//             match i {
//                 Item::FunctionDecl(d) => functions.push(d),
//                 Item::CompositionDecl(c) => compositions.push(c),
//             }
//         }
//         // TODO: here and in join_test, check the data structure is what we expect
//     }

//     #[test]
//     fn join_test() {
//         let src = r#"
//         function FunA (A, B) => (C);
//         function FunB (A, B,C) => (D);
//         function FunC (D) => (E);

//         composition Test (InputA, InputB) => (OutputE) {
//             FunA (
//                 A = keyed InputA,
//                 B = keyed InputB
//             ) =>
//                 (InterC = C)
//             by A right B;

//             FunB (
//                 A = keyed InputA,
//                 B = keyed InputB,
//                 C = keyed InputC
//             ) => (
//                 InterD = D
//             ) by A
//               left B  full C;

//             FunB (
//                 A = keyed InputA,
//                 B = keyed InputB,
//                 C = keyed InputC
//             ) => (
//                 InterD = D
//             ) by A cross B inner C;

//             FunC
//                 ( D = all InterD ) =>
//                 ( OutputE = E) ;
//         }
//     "#;
//         let module = match Parser::new().parse_module(src) {
//             Ok(m) => m,
//             Err(e) => {
//                 print_errors(src, &e);
//                 panic!("parse error");
//             }
//         };
//         dbg!(&module);
//         let Module(items) = module;
//         let mut functions: Vec<SpannedFunctionDecl<'_>> = Vec::new();
//         let mut compositions: Vec<SpannedCompositionDecl<'_>> = Vec::new();
//         for i in items.into_iter() {
//             match i {
//                 Item::FunctionDecl(d) => functions.push(d),
//                 Item::CompositionDecl(c) => compositions.push(c),
//             }
//         }
//     }
// }
