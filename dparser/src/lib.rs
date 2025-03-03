use ariadne::{Color, Config, Fmt, Label, Report, ReportKind, Source};
use chumsky::prelude::*;
use std::rc::Rc;

pub type Span = std::ops::Range<usize>;

pub trait SpannedExt<T> {
    fn news(v: T, span: Span) -> Self;
}

impl<T> SpannedExt<T> for Rc<Spanned<T>> {
    fn news(v: T, span: Span) -> Self {
        Rc::new(Spanned { span, v })
    }
}

#[derive(Clone, Debug)]
pub struct Spanned<T> {
    pub span: Span,
    pub v: T,
}

fn span_at(at: usize) -> Span {
    at..at + 1
}

fn aspanned<T>(v: T, span: Span) -> std::rc::Rc<Spanned<T>> {
    std::rc::Rc::new(Spanned { v, span })
}

pub type Ident = String;

pub type AFunctionDecl = Rc<Spanned<FunctionDecl>>;

#[derive(Debug)]
pub struct FunctionDecl {
    pub name: String,
    pub params: Vec<String>,
    pub returns: Vec<String>,
}

pub type ASharding = Rc<Spanned<Sharding>>;

#[derive(Debug)]
pub enum Sharding {
    All,
    Keyed,
    Each,
}

pub type ALoopCond = Rc<Spanned<LoopCond>>;

#[derive(Debug)]
pub enum LoopCond {
    None,
    UntilEmpty,
    UntilItemEmpty,
}

pub type AInputDescriptor = Rc<Spanned<InputDescriptor>>;

#[derive(Debug)]
pub struct InputDescriptor {
    pub name: String,
    pub ident: String,
    pub sharding: Sharding,
    // TODO pub loop_cond: LoopCond,
}

#[derive(Debug, Clone)]
pub enum JoinFilterStrategy {
    Cross,
    Inner,
    Left,
    Right,
    Full,
}

pub type AOutputDescriptor = Rc<Spanned<OutputDescriptor>>;

#[derive(Debug)]
pub struct OutputDescriptor {
    pub ident: String,
    pub name: String,
    // TODO pub feedback: bool,
}

pub type AFunctionApplication = Rc<Spanned<FunctionApplication>>;

#[derive(Debug, Clone)]
pub struct FunctionApplicationJoinStrategy {
    pub join_strategy_order: Vec<String>,
    pub join_strategies: Vec<JoinFilterStrategy>,
}

#[derive(Debug)]
pub struct FunctionApplication {
    pub name: String,
    pub args: Vec<AInputDescriptor>,
    pub rets: Vec<AOutputDescriptor>,
    pub join_strategy: Option<FunctionApplicationJoinStrategy>,
}

pub type ALoop = Rc<Spanned<Loop>>;

#[derive(Debug)]
pub struct Loop {
    pub args: Vec<AInputDescriptor>,
    pub rets: Vec<AOutputDescriptor>,
    pub statements: Vec<AFunctionApplication>,
}

#[derive(Debug)]
pub enum Statement {
    FunctionApplication(AFunctionApplication),
    Loop(ALoop),
}

pub type AComposition = Rc<Spanned<Composition>>;

#[derive(Debug)]
pub struct Composition {
    pub name: String,
    pub params: Vec<String>,
    pub returns: Vec<String>,
    pub statements: Vec<Statement>,
}

#[derive(Debug)]
pub enum Item {
    FunctionDecl(AFunctionDecl),
    Composition(AComposition),
}

#[derive(Debug)]
pub struct Module(pub Vec<Item>);

fn parser() -> impl Parser<char, Module, Error = Simple<char>> {
    let function_decl = just("function")
        .padded()
        .ignore_then(text::ident().padded())
        .then(
            text::ident()
                .padded()
                .separated_by(just(','))
                .delimited_by(just('('), just(')')),
        )
        .then_ignore(just("=>").padded())
        .then(
            text::ident()
                .padded()
                .separated_by(just(','))
                .delimited_by(just('('), just(')')),
        )
        .then_ignore(just(";").padded())
        .map(|((name, params), returns)| FunctionDecl {
            name,
            params,
            returns,
        })
        .map_with_span(aspanned);

    //     let loop_cond =
    //         select! {
    //             Token::Atom(Atom::Symbol(symbol)) if symbol == "until_empty" || symbol == "until_empty_item" => symbol,
    //         }.or_not()
    //         .map(|loop_cond| match loop_cond {
    //             Some(x) if x == "until_empty" => LoopCond::UntilEmpty,
    //             Some(x) if x == "until_empty_item" => LoopCond::UntilItemEmpty,
    //             Some(_) => unreachable!(),
    //             None => LoopCond::None,
    //         });

    //     loop_cond
    //         .then(sharding)
    //         .then(ident)
    //         .then_ignore(just(Token::Atom(Atom::Mark(Mark::From))))
    //         .then(ident)
    //         .delimited_by(delim.clone(), delim.clone())
    //         .map(|(((loop_cond, sharding), name), ident)| InputDescriptor {
    //             name,
    //             ident,
    //             sharding,
    //             loop_cond,
    //         })
    //         .map_with_span(aspanned)
    // };

    // let loop_ = symbol("loop")
    //     .ignore_then({
    //         input_descriptor
    //             .clone()
    //             .repeated()
    //             .delimited_by(delim.clone(), delim.clone())
    //     })
    //     .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
    //     .then({
    //         function_application
    //             .clone()
    //             .repeated()
    //             .delimited_by(delim.clone(), delim.clone())
    //     })
    //     .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
    //     .then({
    //         output_descriptor
    //             .clone()
    //             .repeated()
    //             .delimited_by(delim.clone(), delim.clone())
    //     })
    //     .delimited_by(delim.clone(), delim.clone())
    //     .map(|((args, statements), rets)| Loop {
    //         args,
    //         rets,
    //         statements,
    //     })
    //     .map_with_span(aspanned);

    let input_descriptor = text::ident()
        .padded()
        .then_ignore(just('=').padded())
        .then(
            (just("all").or(just("keyed")).or(just("each")))
                .map(|sharding| match sharding {
                    "all" => Sharding::All,
                    "keyed" => Sharding::Keyed,
                    "each" => Sharding::Each,
                    _ => unreachable!(),
                })
                .padded(),
        )
        .then(text::ident().padded())
        .map(|((name, sharding), ident)| InputDescriptor {
            name,
            ident,
            sharding,
        })
        .map_with_span(aspanned);

    let output_descriptor = text::ident()
        .padded()
        .then_ignore(just("=").padded())
        .then(text::ident().padded())
        .map(|(ident, name)| OutputDescriptor {
            ident,
            name,
            // TODO feedback: false,
        })
        .map_with_span(aspanned);
    
    let name_followed_by_strategy = text::ident().padded()
        .then
            (just("cross").or(just("inner")).or(just("left")).or(just("right")).or(just("full"))
                .map(|sharding| match sharding {
                    "cross" => JoinFilterStrategy::Cross,
                    "inner" => JoinFilterStrategy::Inner,
                    "left" => JoinFilterStrategy::Left,
                    "right" => JoinFilterStrategy::Right,
                    "full" => JoinFilterStrategy::Full,
                    _ => unreachable!(),
                }))
                .padded();

    let by_join_strategy = just("by").padded().ignore_then(
        name_followed_by_strategy.repeated().padded().then(text::ident()).map(|(xs, x): (Vec<(String, JoinFilterStrategy)>, String)| {
            let (mut names, strats): (Vec<_>, Vec<_>) = xs.iter().cloned().unzip();
            names.push(x);
            FunctionApplicationJoinStrategy { join_strategy_order: names, join_strategies: strats }
        }));

    let function_application = text::ident()
        .then(
            input_descriptor
                .separated_by(just(',').padded())
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .padded()
        .then_ignore(just("=>").padded())
        .then(
            output_descriptor
                .separated_by(just(',').padded())
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .padded()
        .then(by_join_strategy.or_not())
        .map(|(((name, args), rets), join_strategy)| FunctionApplication {
            name, args, rets, join_strategy
        })
        .map_with_span(aspanned)
        .map(Statement::FunctionApplication);

    let statement = function_application.then_ignore(just(';').padded());

    let composition = just("composition")
        .padded()
        .ignore_then(text::ident().padded())
        .then(
            text::ident()
                .padded()
                .separated_by(just(',').padded())
                .delimited_by(just('(').padded(), just(')').padded())
                .padded(),
        )
        .then_ignore(just("=>").padded())
        .then(
            text::ident()
                .padded()
                .separated_by(just(',').padded())
                .delimited_by(just('(').padded(), just(')').padded()),
        )
        .padded()
        .then(
            statement
                .repeated()
                .delimited_by(just('{').padded(), just('}').padded()),
        )
        .map(|(((name, params), returns), statements)| Composition {
            name,
            params,
            returns,
            statements,
        })
        .map_with_span(aspanned);

    (function_decl
        .map(|f| Item::FunctionDecl(f))
        .or(composition.map(|c| Item::Composition(c))))
    .repeated()
    .padded()
    .then_ignore(end())
    .map(|i| Module(i))
}

pub fn parse(src: &str) -> Result<Module, Vec<Simple<char>>> {
    parser().then_ignore(end()).parse(src)
}

#[test]
fn function_test() {
    let src = r#"
    function Access( AccessToken) => (HTTPRequest);
    function FanOut(HTTPResponse , Topic) => (HTTPRequests);
    function Render(HTTPResponses )   => (HTMLOutput);
    
    function HTTP(Request) => ( Response) ;
    "#;
    match parse(src) {
        Ok(_) => (),
        Err(e) => {
            print_errors(src, e);
            panic!("parse error");
        }
    };
}

#[test]
fn composition_test() {
    let src = r#"
    function Access( AccessToken) => (HTTPRequest);
    function FanOut(HTTPResponse , Topic) => (HTTPRequests);
    function Render(HTTPResponses )   => (HTMLOutput);
    
    function HTTP(Request) => ( Response) ;
    
    composition RenderLogs(InputAccessToken, InputTopic) => (OutHTMLOutput) {
        Access(AccessToken = all InputAccessToken) => (AuthRequest = HTTPRequest);
        HTTP(Request = each AuthRequest) => (AuthResponse = Response);
        FanOut(HTTPResponse = all AuthResponse, Topic = all InputTopic) => (LogRequests = HTTPRequests);
        HTTP(Request = each LogRequests) => (LogResponses = Response);
        Render(HTTPResponses = all LogResponses) => (OutHTMLOutput = HTMLOutput);
    }
"#;
    match parse(src) {
        Ok(_) => (),
        Err(e) => {
            print_errors(src, e);
            panic!("parse error");
        }
    };
}

#[test]
fn simple_test() {
    let src = r#"
    function HTTP(Request) => (Response);
    function MakePNGGrayscaleS3 (S3GetResponse) => (S3PutRequest);
    
    composition MakePNGGrayscale (S3GetRequest) => () {
        HTTP(Request = keyed S3GetRequest) => (ToProcess = Response);
        MakePNGGrayscaleS3 (  S3GetResponse = keyed ToProcess ) => (PutRequest = S3PutRequest) ;
        HTTP ( Request = keyed PutRequest) => ( );
    }
"#;
    let _ = match parse(src) {
        Ok(m) => m,
        Err(e) => {
            print_errors(src, e);
            panic!("parse error");
        }
    };
    // TODO add checks on module
}

#[test]
fn sharding_test() {
    let src = r#"
    function FunA (A, B) => (C);
    function FunB (A, B,C) => (D);
    function FunC (D) => (E);
    
    composition Test (InputA, InputB) => (OutputE) { 
        FunA (
            A = keyed InputA,
            B = keyed InputB
        ) => 
            (InterC = C)
        ;
    
        FunB (
            A = keyed InputA,
            B = keyed InputB,
            C = keyed InputC
        ) => (
            InterD = D
        );
        
        FunC
            ( D = all InterD ) =>
            ( OutputE = E);
    }
"#;
    let module = match parse(src) {
        Ok(m) => m,
        Err(e) => {
            print_errors(src, e);
            panic!("parse error");
        }
    };
    dbg!(&module);
    let Module(items) = module;
    let mut functions: Vec<AFunctionDecl> = Vec::new();
    let mut compositions: Vec<AComposition> = Vec::new();
    for i in items.into_iter() {
        match i {
            Item::FunctionDecl(d) => functions.push(d),
            Item::Composition(c) => compositions.push(c),
        }
    }
}

pub fn print_errors(src: &str, errs: Vec<Simple<char>>) {
    errs.into_iter().for_each(|e| {
        let msg = format!(
            "Unexpected {}",
            e.found()
                .map(|c| format!("{:?}", c).fg(Color::Red).to_string())
                .unwrap_or_else(|| "end of input".to_string())
        );
        let report = Report::build(ReportKind::Error, (), e.span().start)
            .with_config(Config::default())
            .with_message(msg)
            .with_label(
                Label::new(e.span())
                    .with_message({
                        let mut expected: Box<dyn Iterator<Item = String>> = Box::new(
                            e.expected()
                                .filter_map(|e| e.as_ref().map(|e| format!("{:?}", e))),
                        );
                        if let Some(l) = e.label() {
                            expected = Box::new(expected.chain(std::iter::once(format!("{}", l))));
                        }
                        let expected = expected
                            .map(|e| e.fg(Color::White).to_string())
                            .collect::<Vec<_>>();
                        format!("Expected one of {}", expected.join(", "))
                    })
                    .with_color(Color::Red),
            );
        report.finish().eprint(Source::from(&src)).unwrap();
    });
}
