use ariadne::{Color, Config, Fmt, Label, Report, ReportKind, Source};
use chumsky::prelude::*;
// use chumsky::{BoxStream, Flat, Stream};
use std::rc::Rc;

pub type Span = std::ops::Range<usize>;

//fn s_parser() -> impl Parser<char, Vec<(Node, Span)>, Error = Simple<char>> {
//    let comment = just(' ')
//        .repeated()
//        .then(just(';'))
//        .padded()
//        .ignore_then(text::newline().not().repeated())
//        .ignore_then(text::newline())
//        .ignored()
//        .labelled("comment")
//        .repeated();
//    let atom = just(":=")
//        .to(Node::Atom(Atom::Mark(Mark::Gets)))
//        .or(just("<-").to(Node::Atom(Atom::Mark(Mark::From))))
//        .or(just("->").to(Node::Atom(Atom::Mark(Mark::ToSlim))))
//        .or(just("=>").to(Node::Atom(Atom::Mark(Mark::ToFat))))
//        .or(just(':')
//            .ignore_then(text::ident())
//            .map(|x| Node::Atom(Atom::Symbol(x))))
//        .or(text::int(10).map(|l: String| Node::Atom(Atom::Int(l.parse().unwrap()))))
//        .or(just("true")
//            .map(|_| true)
//            .or(just("false").map(|_| false))
//            .map(|b| Node::Atom(Atom::Bool(b))))
//        .or(text::ident().map(|x| Node::Atom(Atom::Ident(x))))
//        .labelled("atom");

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

// fn flatten_tts(eoi: Span, token_trees: Vec<(Node, Span)>) -> BoxStream<'static, Token, Span> {
//     use std::iter::once;
//     // Currently, this is quite an explicit process: it will likely become easier in future versions of Chumsky.
//     Stream::from_nested(eoi, token_trees.into_iter(), |(tt, span)| match tt {
//         Node::Token(token) => Flat::Single((token, span)),
//         Node::List(list) => Flat::Many(
//             once((Node::Token(Token::ListDelim), span_at(span.start)))
//                 .chain(list.into_iter())
//                 .chain(once((Node::Token(Token::ListDelim), span_at(span.end - 1)))),
//         ),
//         Node::Atom(atom) => Flat::Single((Token::Atom(atom), span)),
//     })
// }

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

pub type AOutputDescriptor = Rc<Spanned<OutputDescriptor>>;

#[derive(Debug)]
pub struct OutputDescriptor {
    pub ident: String,
    pub name: String,
    // TODO pub feedback: bool,
}

pub type AFunctionApplication = Rc<Spanned<FunctionApplication>>;

#[derive(Debug)]
pub struct FunctionApplication {
    pub name: String,
    pub args: Vec<AInputDescriptor>,
    pub rets: Vec<AOutputDescriptor>,
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
    // let delim = just(Token::ListDelim);
    // let symbol = |s: &'static str| {
    //     select! {
    //         Token::Atom(Atom::Symbol(symbol)) if symbol == s => s,
    //     }
    // };
    //
    //

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

    // let output_descriptor = {
    //     symbol("feedback")
    //         .or_not()
    //         .map(|f| f.is_some())
    //         .then(ident)
    //         .then_ignore(just(Token::Atom(Atom::Mark(Mark::Gets))))
    //         .then(ident)
    //         .delimited_by(delim.clone(), delim.clone())
    //         .map(|((feedback, ident), name)| OutputDescriptor {
    //             ident,
    //             name,
    //             feedback,
    //         })
    //         .map_with_span(aspanned)
    // };

    // let function_application = ident
    //     .then(
    //         {
    //             input_descriptor
    //                 .clone()
    //                 .repeated()
    //                 .delimited_by(delim.clone(), delim.clone())
    //         }
    //         .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
    //         .then({
    //             output_descriptor
    //                 .clone()
    //                 .repeated()
    //                 .delimited_by(delim.clone(), delim.clone())
    //         }),
    //     )
    //     .delimited_by(delim.clone(), delim.clone())
    //     .map(|(name, (args, rets))| FunctionApplication { name, args, rets })
    //     .map_with_span(aspanned);

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

    let function_application = text::ident()
        .then(
            input_descriptor
                .separated_by(just(',').padded())
                .delimited_by(just('('), just(')')),
        )
        .padded()
        .then_ignore(just("=>").padded())
        .then(
            output_descriptor
                .separated_by(just(',').padded())
                .delimited_by(just('('), just(')')),
        )
        .padded()
        .map(|((name, args), rets)| FunctionApplication { name, args, rets })
        .map_with_span(aspanned)
        .map(Statement::FunctionApplication);

    let statement = function_application.then_ignore(just(';').padded());

    let composition = just("composition")
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
        .padded()
        .then(statement.repeated().delimited_by(just('{'), just('}')))
        .map(|(((name, params), returns), statements)| Composition {
            name,
            params,
            returns,
            statements,
        })
        .map_with_span(aspanned);

    //     .ignore_then(ident)
    //     .then(
    //         {
    //             ident
    //                 .clone()
    //                 .repeated()
    //                 .delimited_by(delim.clone(), delim.clone())
    //         }
    //         .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToSlim))))
    //         .then({
    //             ident
    //                 .clone()
    //                 .repeated()
    //                 .delimited_by(delim.clone(), delim.clone())
    //         }),
    //     )
    //     .then(
    //         function_application
    //             .map(|f| Statement::FunctionApplication(f))
    //             .or(loop_.map(|l| Statement::Loop(l)))
    //             .repeated()
    //             .delimited_by(delim.clone(), delim.clone()),
    //     )
    //     .delimited_by(delim.clone(), delim.clone())

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
    let module = parse(src).expect("successful parse");
    dbg!(&module);
}

#[test]
fn composition_test() {
    let src = r#"
    function Access( AccessToken) => (HTTPRequest);
    function FanOut(HTTPResponse , Topic) => (HTTPRequests);
    function Render(HTTPResponses )   => (HTMLOutput);
    
    function HTTP(Request) => ( Response) ;
    
    composition RenderLogs(InputAccessToken, InputTopic) => (HTMLOutput) {
        Access(AccessToken = all InputAccessToken) => (AuthRequest = HTTPRequest);
        HTTP(Request = each AuthRequest) => (AuthResponse = Response);
        FanOut(HTTPResponse = all AuthResponse, Topic = all InputTopic) => (LogRequests = HTTPRequests);
        HTTP(Request = each LogRequests) => (LogResponses = Response);
        Render(HTTPResponses = all LogResponses) => (HTMLOutput = HTMLOutput);
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
    // ...
}

#[test]
fn simple_test() {
    let src = r#"
    (:function MakePNGGrayscaleS3 (S3GetResponse) -> (S3PutRequest))
    
    (:composition MakePNGGrayscale (S3GetRequest) -> () (
        (DandelionHTTPGet ( (:keyed Request <- S3GetRequest) ) => ( (ToProcess := Response) ))
        (MakePNGGrayscale ( (:keyed S3GetResponse <- ToProcess) ) => ( (S3PutRequest := S3PutRequest) ))
        (DandelionHTTPPut ( (:keyed Request <- S3PutRequest) ) => ( ))
    ))
"#;
    let module = parse(src).expect("successful parse");
    dbg!(&module);
}

#[test]
fn sharding_test() {
    let src = r#"
    (:function FunA (A B) -> (C))
    (:function FunB (A B C) -> (D))
    (:function FunC (D) -> (E))
    
    (:composition Test (InputA InputB) -> (OutputE) (
        (FunA (
            (:keyed A <- InputA)
            (:keyed B <- InputB)
        ) => (
            (InterC := C)
        ))
    
        (FunB (
            (:keyed A <- InputA)
            (:keyed B <- InputB)
            (:keyed C <- InputC)
        ) => (
            (InterD := D)
        ))
        
        (FunC (
            (:all D <- InterD)
        ) => (
            (OutputE := E)
        ))
    ))
"#;
    let module = parse(src).expect("successful parse");
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
