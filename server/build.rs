fn main() {
    match prost_build::compile_protos(&["proto/protocol.proto"], &["proto/"]) {
        Ok(_) => (),
        Err(err) => panic!("Failed to build protobufs: {:?}", err),
    }

    let features = std::env::var("CARGO_CFG_FEATURE").unwrap();
    println!("cargo::rustc-env=ENABLED_FEATURES={}", features);
}
