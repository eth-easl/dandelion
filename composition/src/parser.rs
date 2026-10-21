mod composition_builder;

use crate::{
    parser::composition_builder::CompositionBuilder,
    sharding::{JoinStrategy, Sharding},
    CompositionTemplate, Registry,
};
use ariadne::{Color, Config, Label, Report, ReportKind, Source};
use chumsky::{prelude::*, Parser as ChumskyParser};
use dandelion_commons::{dandelion_err, DError, DandelionError, FunctionId};
use memory::data::Metadata;
use std::ops::Range;

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

/// Skips any mix of whitespace and `//` line comments.
fn pad<'src>() -> impl ChumskyParser<'src, &'src str, (), extra::Err<Rich<'src, char>>> + Clone {
    let comment = just("//")
        .then(any().and_is(text::newline().not()).repeated())
        .ignored();
    text::whitespace()
        .at_least(1)
        .ignored()
        .or(comment)
        .repeated()
        .ignored()
}

/// Wrapper to get back a Ident<'src> alias.
fn ident<'src>(
) -> impl ChumskyParser<'src, &'src str, Ident<'src>, extra::Err<Rich<'src, char>>> + Clone {
    text::ident()
}

#[derive(Debug)]
struct ExpansionDecl<'src> {
    /// input sets of the expansion target, in order
    sets: Vec<Ident<'src>>,
    /// allowed targets, None if any composition with a matching signature is allowed
    targets: Option<Vec<Spanned<Ident<'src>>>>,
}

impl<'src> FunctionDecl<'src> {
    /// Names an application can bind: the function's own params, followed by the sets that are
    /// forwarded to the expansion without passing through the function.
    fn application_params(&self) -> Vec<Ident<'src>> {
        let mut params = self.params.clone();
        if let Some(expansion) = &self.expansion {
            params.extend(
                expansion
                    .sets
                    .iter()
                    .filter(|set| !self.returns.contains(set))
                    .copied(),
            );
        }
        params
    }
}

#[derive(Debug)]
struct FunctionDecl<'src> {
    name: Ident<'src>,
    params: Vec<Ident<'src>>,
    returns: Vec<Ident<'src>>,
    expansion: Option<ExpansionDecl<'src>>
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
        // expands (...) [into (...)]
        let expansion = just("expands")
            .padded_by(pad())
            .ignore_then(
                ident()
                .padded_by(pad())
                .separated_by(just(','))
                .collect::<Vec<_>>()
                .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad()))
            )
            .then(
                just("into")
                    .padded_by(pad())
                    .ignore_then(ident()
                        .map_with(|v, e| Spanned::new(v, e.span().into_range()))
                        .padded_by(pad())
                        .separated_by(just(','))
                        .collect::<Vec<_>>()
                        .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad())))
                    .or_not(),
            )
            .map(|(sets, targets)| ExpansionDecl { sets, targets });

        let function_decl = just("function")
            .padded_by(pad())
            .ignore_then(ident().padded_by(pad()))
            .then(
                ident()
                    .padded_by(pad())
                    .separated_by(just(','))
                    .collect::<Vec<_>>()
                    .delimited_by(just('('), just(')')),
            )
            .then_ignore(just("=>").padded_by(pad()))
            .then(
                ident()
                    .padded_by(pad())
                    .separated_by(just(','))
                    .collect::<Vec<_>>()
                    .delimited_by(just('('), just(')')),
            )
            .then(expansion.or_not())
            .then_ignore(just(";").padded_by(pad()))
            .map(|(((name, params), returns), expansion)| FunctionDecl {
                name,
                params,
                returns,
                expansion
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let input_descriptor = ident()
            .padded_by(pad())
            .then_ignore(just('=').padded_by(pad()))
            .then(just("optional").padded_by(pad()).or_not())
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
                .padded_by(pad()),
            )
            .then(ident().padded_by(pad()))
            .map(|(((name, optional), sharding), ident)| InputDescriptor {
                name,
                ident,
                sharding,
                optional: optional.is_some(),
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let output_descriptor = ident()
            .padded_by(pad())
            .then_ignore(just("=").padded_by(pad()))
            .then(ident().padded_by(pad()))
            .map(|(ident, name)| OutputDescriptor {
                ident,
                name,
                // TODO feedback: false,
            })
            .map_with(|v, e| Spanned::new(v, e.span().into_range()));

        let name_followed_by_strategy = ident()
            .padded_by(pad())
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
            .padded_by(pad());

        let by_join_strategy = just("by").padded_by(pad()).ignore_then(
            name_followed_by_strategy
                .repeated()
                .collect::<Vec<(Ident<'src>, JoinStrategy)>>()
                .padded_by(pad())
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
                    .separated_by(just(',').padded_by(pad()))
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad())),
            )
            .padded_by(pad())
            .then_ignore(just("=>").padded_by(pad()))
            .then(
                output_descriptor
                    .separated_by(just(',').padded_by(pad()))
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad())),
            )
            .padded_by(pad())
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

        let statement = function_application.then_ignore(just(';').padded_by(pad()));

        // TODO: loop syntax (`loop { input_descriptor* => function_application* => output_descriptor* }`)
        // is not parsed yet.

        let composition = just("composition")
            .padded_by(pad())
            .ignore_then(ident().padded_by(pad()))
            .then(
                ident()
                    .padded_by(pad())
                    .separated_by(just(',').padded_by(pad()))
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad()))
                    .padded_by(pad()),
            )
            .then_ignore(just("=>").padded_by(pad()))
            .then(
                ident()
                    .padded_by(pad())
                    .separated_by(just(',').padded_by(pad()))
                    .collect::<Vec<_>>()
                    .delimited_by(just('(').padded_by(pad()), just(')').padded_by(pad())),
            )
            .padded_by(pad())
            .then(
                statement
                    .repeated()
                    .collect::<Vec<_>>()
                    .delimited_by(just('{').padded_by(pad()), just('}').padded_by(pad())),
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
        .padded_by(pad())
        .then_ignore(end())
        .map(Module)
        .boxed();

        Parser { parser, registry }
    }

    pub(crate) fn parse(
        &self,
        src: &'src str,
    ) -> Result<Vec<(FunctionId, CompositionTemplate, Metadata)>, Vec<ErrorDiagnostic>> {
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
        // check if all expansion types
        builder.check_expansion_targets().map_err(|e| vec![e])?;

        Ok(builder.collect())
    }
}

pub(crate) struct ErrorDiagnostic {
    span: Span,
    error: DError,
}

impl ErrorDiagnostic {
    fn new(span: Span, error: DError) -> Self {
        ErrorDiagnostic { span, error }
    }

    fn from_syntax_error(e: Rich<'_, char>) -> Self {
        ErrorDiagnostic {
            span: e.span().into_range(),
            error: dandelion_err!(DandelionError::Parsing(e.to_string())),
        }
    }

    /// Renders the diagnostic for logging, with ANSI colors and the origin of the error.
    pub(crate) fn render_log(&self, src: &str) -> String {
        self.render(src, &self.error.to_string(), true)
    }

    /// Renders the diagnostic as plain text for the request response, without the origin of the error.
    pub(crate) fn render_response(&self, src: &str) -> String {
        self.render(src, &self.error.error.to_string(), false)
    }

    fn render(&self, src: &str, header: &str, color: bool) -> String {
        // name shown for the source in the report location (e.g. `composition:2:1`)
        const SOURCE_NAME: &str = "composition";
        let mut buf = Vec::new();
        Report::build(ReportKind::Error, (SOURCE_NAME, self.span.clone()))
            .with_config(Config::default().with_color(color))
            .with_message(header)
            .with_label(
                Label::new((SOURCE_NAME, self.span.clone()))
                    .with_message(self.error.message())
                    .with_color(Color::Red),
            )
            .finish()
            .write((SOURCE_NAME, Source::from(src)), &mut buf)
            .expect("writing to an in-memory buffer never fails");
        String::from_utf8(buf).expect("ariadne only ever emits valid utf-8")
    }
}
