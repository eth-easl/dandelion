fn main() {
    match prost_build::compile_protos(&["proto/protocol.proto"], &["proto/"]) {
        Ok(_) => (),
        Err(err) => panic!("Failed to build protobufs: {:?}", err),
    }
}
