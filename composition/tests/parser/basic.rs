use crate::common::TestRegistry;
use composition::CompositionTemplate;

#[test]
fn function_declarations_without_a_composition_parse() {
    let src = r#"
        function Access(AccessToken) => (HTTPRequest);
        function FanOut(HTTPResponse, Topic) => (HTTPRequests);
        function Render(HTTPResponses) => (HTMLOutput);
        function HTTP(Request) => (Response);
    "#;
    let registry = TestRegistry::new()
        .with_function("Access", &["AccessToken"], &["HTTPRequest"])
        .with_function("FanOut", &["HTTPResponse", "Topic"], &["HTTPRequests"])
        .with_function("Render", &["HTTPResponses"], &["HTMLOutput"])
        .with_function("HTTP", &["Request"], &["Response"]);

    let compositions = CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected the module to parse:\n{e}"));
    assert!(
        compositions.is_empty(),
        "the module declares no compositions"
    );
}

#[test]
fn multi_stage_pipeline_parses() {
    let src = r#"
        function Access(AccessToken) => (HTTPRequest);
        function FanOut(HTTPResponse, Topic) => (HTTPRequests);
        function Render(HTTPResponses) => (HTMLOutput);
        function HTTP(Request) => (Response);

        composition RenderLogs(InputAccessToken, InputTopic) => (OutHTMLOutput) {
            Access(AccessToken = all InputAccessToken) => (AuthRequest = HTTPRequest);
            HTTP(Request = each AuthRequest) => (AuthResponse = Response);
            FanOut(HTTPResponse = all AuthResponse, Topic = all InputTopic) => (LogRequests = HTTPRequests);
            HTTP(Request = each LogRequests) => (LogResponses = Response);
            Render(HTTPResponses = all LogResponses) => (OutHTMLOutput = HTMLOutput);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("Access", &["AccessToken"], &["HTTPRequest"])
        .with_function("FanOut", &["HTTPResponse", "Topic"], &["HTTPRequests"])
        .with_function("Render", &["HTTPResponses"], &["HTMLOutput"])
        .with_function("HTTP", &["Request"], &["Response"]);

    let compositions = CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected the composition to parse:\n{e}"));
    assert_eq!(compositions.len(), 1);
    assert_eq!(compositions[0].0.as_str(), "RenderLogs");
}

#[test]
fn optional_inputs_parse() {
    let src = r#"
        function HTTP(Request) => (Response);
        function MakePNGGrayscaleS3(S3GetResponse) => (S3PutRequest);

        composition MakePNGGrayscale(S3GetRequest) => () {
            HTTP(Request = optional keyed S3GetRequest) => (ToProcess = Response);
            MakePNGGrayscaleS3(S3GetResponse = optional keyed ToProcess) => (PutRequest = S3PutRequest);
            HTTP(Request = keyed PutRequest) => ();
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("HTTP", &["Request"], &["Response"])
        .with_function("MakePNGGrayscaleS3", &["S3GetResponse"], &["S3PutRequest"]);

    let compositions = CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected the composition to parse:\n{e}"));
    assert_eq!(compositions.len(), 1);
    assert_eq!(compositions[0].0.as_str(), "MakePNGGrayscale");
}

#[test]
fn multiple_compositions_in_one_module_all_parse() {
    let src = r#"
        function Identity(In) => (Out);

        composition First(A) => (B) {
            Identity(In = all A) => (B = Out);
        }

        composition Second(A) => (B) {
            Identity(In = all A) => (B = Out);
        }
    "#;
    let registry = TestRegistry::new().with_function("Identity", &["In"], &["Out"]);

    let compositions = CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected both compositions to parse:\n{e}"));
    let mut names: Vec<_> = compositions.iter().map(|(id, _, _)| id.as_str()).collect();
    names.sort();
    assert_eq!(names, vec!["First", "Second"]);
}

/// Functions may consume sets produced by later statements, and multiple paths may lead to the
/// same function, neither of which is a cycle.
#[test]
fn acyclic_composition_with_statements_out_of_order_and_a_diamond_parses() {
    let src = r#"
        function Emit(X) => (Y);
        function Combine(A, B) => (C);

        composition Diamond(In) => (Out) {
            Combine(A = each Left, B = each Right) => (Out = C);
            Emit(X = each Top) => (Left = Y);
            Emit(X = each Top) => (Right = Y);
            Emit(X = each In) => (Top = Y);
        }
    "#;
    let registry = TestRegistry::new()
        .with_function("Emit", &["X"], &["Y"])
        .with_function("Combine", &["A", "B"], &["C"]);

    let compositions = CompositionTemplate::parse(src, &registry)
        .unwrap_or_else(|e| panic!("expected the composition to parse:\n{e}"));
    assert_eq!(compositions.len(), 1);
}
