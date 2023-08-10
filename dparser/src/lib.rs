use ariadne::{Color, Config, Fmt, Label, Report, ReportKind, Source};
use chumsky::{prelude::*, text::whitespace};
use chumsky::{BoxStream, Flat, Stream};
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Mark {
    Gets,
    From,
    ToSlim,
    ToFat,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Atom {
    Ident(String),
    Int(u64),
    Bool(bool),
    Symbol(String),
    Mark(Mark),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Token {
    Atom(Atom),
    ListDelim,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Node {
    Atom(Atom),
    List(Vec<(Self, Span)>),
    Token(Token),
}

fn s_parser() -> impl Parser<char, Vec<(Node, Span)>, Error = Simple<char>> {
    let comment = just(' ')
        .repeated()
        .then(just(';'))
        .padded()
        .ignore_then(text::newline().not().repeated())
        .ignore_then(text::newline())
        .ignored()
        .labelled("comment")
        .repeated();
    let atom = just(":=")
        .to(Node::Atom(Atom::Mark(Mark::Gets)))
        .or(just("<-").to(Node::Atom(Atom::Mark(Mark::From))))
        .or(just("->").to(Node::Atom(Atom::Mark(Mark::ToSlim))))
        .or(just("=>").to(Node::Atom(Atom::Mark(Mark::ToFat))))
        .or(just(':')
            .ignore_then(text::ident())
            .map(|x| Node::Atom(Atom::Symbol(x))))
        .or(text::int(10).map(|l: String| Node::Atom(Atom::Int(l.parse().unwrap()))))
        .or(just("true")
            .map(|_| true)
            .or(just("false").map(|_| false))
            .map(|b| Node::Atom(Atom::Bool(b))))
        .or(text::ident().map(|x| Node::Atom(Atom::Ident(x))))
        .labelled("atom");
    comment
        .padded()
        .ignore_then(recursive(|list| {
            let node = atom.or(list).labelled("atom or list").then_ignore(comment);
            node.map_with_span(|node, span| (node, span))
                .separated_by(whitespace().at_least(1))
                .then_ignore(comment)
                .padded()
                .delimited_by(just('('), just(')'))
                .collect::<Vec<(Node, Span)>>()
                .map(Node::List)
                .labelled("list")
                .then_ignore(comment)
        }))
        .labelled("node")
        .map_with_span(|node, span| (node, span))
        .separated_by(comment.or(whitespace().at_least(1).labelled("whitespace")))
        .padded()
        .then_ignore(comment)
        .padded()
}

#[test]
fn s_parser_test() {
    let srcs = [
        r#"
(a)
    "#,
        r#"
; comment
(a) ; comment
(b)
    "#,
        r#"
(a) ; comment
(a ; comment
)
    "#,
        r#"
(a) ; comment
(
    (a) ; comment
)
    "#,
        r#"
; this currently fails because support for comments is incomplete

(:function CompileFiles (Source) -> (Out)) ; the key for Out is the binary this file will end up in
(:function LinkObjects (ObjectFile Library) -> (Binary))
    "#,
        r#"
(:composition CompileMulti (SourceFiles Libraries) -> (Binaries) (
    (CompileFiles (
        (:shard Source <- SourceFile)
    ) => (
        (ObjectFiles := Out) ; declares a collection ObjectFiles and merges all outputs from the shards of CompileFile into it
    ))

    (LinkObjects (
        (:shard Objects <- ObjectFiles)
        (Libraries <- Libraries) ; no sharding modifier: broadcast libraries to all funciton calls
    ) => (
        (Binaries := Binary) ; declares a collection Binaries and merges all outputs from the shards of CompileFile into it
    ))
))
    "#,
    ];
    for src in srcs {
        // eprintln!("{}", src);
        let nodes = s_parser()
            .then_ignore(end())
            .parse(&*src)
            .expect("parse failed");
        // dbg!(&nodes);
        let eoi = 0..src.chars().count();
        let _token_stream = flatten_tts(eoi, nodes);
    }
}

fn span_at(at: usize) -> Span {
    at..at + 1
}

fn flatten_tts(eoi: Span, token_trees: Vec<(Node, Span)>) -> BoxStream<'static, Token, Span> {
    use std::iter::once;
    // Currently, this is quite an explicit process: it will likely become easier in future versions of Chumsky.
    Stream::from_nested(eoi, token_trees.into_iter(), |(tt, span)| match tt {
        Node::Token(token) => Flat::Single((token, span)),
        Node::List(list) => Flat::Many(
            once((Node::Token(Token::ListDelim), span_at(span.start)))
                .chain(list.into_iter())
                .chain(once((Node::Token(Token::ListDelim), span_at(span.end - 1)))),
        ),
        Node::Atom(atom) => Flat::Single((Token::Atom(atom), span)),
    })
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
    No,
    Shard,
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
    pub loop_cond: LoopCond,
}

pub type AOutputDescriptor = Rc<Spanned<OutputDescriptor>>;

#[derive(Debug)]
pub struct OutputDescriptor {
    pub ident: String,
    pub name: String,
    pub feedback: bool,
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
pub struct Module(Vec<Item>);

fn parser() -> impl Parser<Token, Module, Error = Simple<Token>> {
    let delim = just(Token::ListDelim);
    let symbol = |s: &'static str| {
        select! {
            Token::Atom(Atom::Symbol(symbol)) if symbol == s => s,
        }
    };
    let ident = select! {
        Token::Atom(Atom::Ident(atom)) => atom,
    };
    let function_decl = symbol("function")
        .ignore_then(ident)
        .then(ident.repeated().delimited_by(delim.clone(), delim.clone()))
        .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToSlim))))
        .then(ident.repeated().delimited_by(delim.clone(), delim.clone()))
        .delimited_by(delim.clone(), delim.clone())
        .map(|((name, params), returns)| FunctionDecl {
            name,
            params,
            returns,
        })
        .map_with_span(aspanned);

    let input_descriptor = {
        let sharding = symbol("shard").or_not().map(|sharding| match sharding {
            Some("shard") => Sharding::Shard,
            Some(_) => unreachable!(),
            None => Sharding::No,
        });

        let loop_cond =
            select! {
                Token::Atom(Atom::Symbol(symbol)) if symbol == "until_empty" || symbol == "until_empty_item" => symbol,
            }.or_not()
            .map(|loop_cond| match loop_cond {
                Some(x) if x == "until_empty" => LoopCond::UntilEmpty,
                Some(x) if x == "until_empty_item" => LoopCond::UntilItemEmpty,
                Some(_) => unreachable!(),
                None => LoopCond::None,
            });

        loop_cond
            .then(sharding)
            .then(ident)
            .then_ignore(just(Token::Atom(Atom::Mark(Mark::From))))
            .then(ident)
            .delimited_by(delim.clone(), delim.clone())
            .map(|(((loop_cond, sharding), name), ident)| InputDescriptor {
                name,
                ident,
                sharding,
                loop_cond,
            })
            .map_with_span(aspanned)
    };

    let output_descriptor = {
        symbol("feedback")
            .or_not()
            .map(|f| f.is_some())
            .then(ident)
            .then_ignore(just(Token::Atom(Atom::Mark(Mark::Gets))))
            .then(ident)
            .delimited_by(delim.clone(), delim.clone())
            .map(|((feedback, ident), name)| OutputDescriptor {
                ident,
                name,
                feedback,
            })
            .map_with_span(aspanned)
    };

    let function_application = ident
        .then(
            {
                input_descriptor
                    .clone()
                    .repeated()
                    .delimited_by(delim.clone(), delim.clone())
            }
            .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
            .then({
                output_descriptor
                    .clone()
                    .repeated()
                    .delimited_by(delim.clone(), delim.clone())
            }),
        )
        .delimited_by(delim.clone(), delim.clone())
        .map(|(name, (args, rets))| FunctionApplication { name, args, rets })
        .map_with_span(aspanned);

    let loop_ = symbol("loop")
        .ignore_then({
            input_descriptor
                .clone()
                .repeated()
                .delimited_by(delim.clone(), delim.clone())
        })
        .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
        .then({
            function_application
                .clone()
                .repeated()
                .delimited_by(delim.clone(), delim.clone())
        })
        .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToFat))))
        .then({
            output_descriptor
                .clone()
                .repeated()
                .delimited_by(delim.clone(), delim.clone())
        })
        .delimited_by(delim.clone(), delim.clone())
        .map(|((args, statements), rets)| Loop {
            args,
            rets,
            statements,
        })
        .map_with_span(aspanned);

    let composition = symbol("composition")
        .ignore_then(ident)
        .then(
            {
                ident
                    .clone()
                    .repeated()
                    .delimited_by(delim.clone(), delim.clone())
            }
            .then_ignore(just(Token::Atom(Atom::Mark(Mark::ToSlim))))
            .then({
                ident
                    .clone()
                    .repeated()
                    .delimited_by(delim.clone(), delim.clone())
            }),
        )
        .then(
            function_application
                .map(|f| Statement::FunctionApplication(f))
                .or(loop_.map(|l| Statement::Loop(l)))
                .repeated()
                .delimited_by(delim.clone(), delim.clone()),
        )
        .delimited_by(delim.clone(), delim.clone())
        .map(|((name, (params, returns)), statements)| Composition {
            name,
            params,
            returns,
            statements,
        })
        .map_with_span(aspanned);

    function_decl
        .map(|f| Item::FunctionDecl(f))
        .or(composition.map(|c| Item::Composition(c)))
        .repeated()
        .map(|i| Module(i))
}

#[test]
fn basic_test() {
    let src = r#"
    (:function CompileFiles (Source) -> (Out))
    (:function LinkObjects (ObjectFile Library) -> (Binary))
    
    (:composition CompileMulti (SourceFiles Libraries) -> (Binaries) (
        (CompileFiles (
            (:shard Source <- SourceFile)
        ) => (
            (ObjectFiles := Out)
        ))
    
        (LinkObjects (
            (:shard Objects <- ObjectFiles)
            (Libraries <- Libraries)
        ) => (
            (Binaries := Binary)
        ))
    ))
    
    (:function CompileOneFile (SourcesBefore) -> (SourcesAfter Out))
    
    (:composition CompileFixpoint (SourceFiles Libraries) -> (Binary) (
        (:loop (
            (:until_empty Sources <- SourceFiles) ; until_empty (until the collection is empty), until_empty_item (until the collection's only item has length zero)
        ) => (
            (CompileOneFile (
                (SourcesBefore <- Sources) ; no sharding modifier: run a single function instance with all the collection's inputs
            ) => (
                (SourcesAfter := SourcesAfter)
                (Out := Out)
            ))
        ) => (
            (:feedback Sources := SourcesAfter) ; replaces Sources at each iteration
            (ObjectFiles := Out)
        ))
        
        (LinkObjects (
            (Objects <- ObjectFiles)
            (Libraries <- Libraries) ; no sharding modifier: broadcast libraries to all funciton calls
        ) => (
            (Binary := Binary)
        ))
    ))
"#;
    let nodes = s_parser().then_ignore(end()).parse(src).unwrap();
    let eoi = 0..src.chars().count();
    let token_stream = flatten_tts(eoi, nodes);
    let _module = parser().then_ignore(end()).parse(token_stream).unwrap();
    dbg!(&_module);
    // ...
}

pub fn parse(src: &str) -> Result<Module, ()> {
    let nodes = match s_parser().then_ignore(end()).parse(&*src) {
        Ok(nodes) => nodes,
        Err(errs) => {
            errs.into_iter().for_each(|e| {
                let msg = format!(
                    "Unexpected {}",
                    e.found()
                        .map(|c| format!("token \'{}\'", c.fg(Color::Red)))
                        .unwrap_or_else(|| "end of input".to_string())
                );
                let report = Report::build(ReportKind::Error, (), e.span().start)
                    .with_config(Config::default())
                    .with_message(msg)
                    .with_label(
                        Label::new(e.span())
                            .with_message({
                                let mut expected: Box<dyn Iterator<Item = String>> = Box::new(
                                    e.expected().filter_map(|e| e.map(|e| format!("'{}'", e))),
                                );
                                if let Some(l) = e.label() {
                                    expected =
                                        Box::new(expected.chain(std::iter::once(format!("{}", l))));
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
            return Err(());
        }
    };

    let eoi = 0..src.chars().count();
    let token_stream = flatten_tts(eoi, nodes);

    let problem = match parser().then_ignore(end()).parse(token_stream) {
        Ok(problem) => problem,
        Err(errs) => {
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
                                    expected =
                                        Box::new(expected.chain(std::iter::once(format!("{}", l))));
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
            return Err(());
        }
    };
    Ok(problem)
}
