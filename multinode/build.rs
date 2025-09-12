fn main() {
    match prost_build::compile_protos(&["proto/multinode.proto"], &["proto/"]) {
        Ok(_) => (),
        Err(err) => panic!("Failed to build protobufs: {:?}", err),
    }
}
