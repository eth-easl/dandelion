fn main() {
    // The wire format is defined once, by the server. Compiling the same schema here keeps the CLI
    // from drifting out of sync with it.
    const PROTO_DIR: &str = "../server/proto";
    const PROTO_FILE: &str = "../server/proto/protocol.proto";
    if let Err(err) = prost_build::compile_protos(&[PROTO_FILE], &[PROTO_DIR]) {
        panic!("Failed to build protobufs: {:?}", err);
    }
    println!("cargo::rerun-if-changed={}", PROTO_FILE);
}
