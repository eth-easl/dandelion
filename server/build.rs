use std::process::Command;

fn main() {
    // detect cheri via uname (since cargo does not detect the feature yet)
    let output = Command::new("uname")
        .arg("-p")
        .output()
        .expect("Uname should be available");
    let processor_string = std::str::from_utf8(&output.stdout).unwrap();
    let is_cheri = processor_string == "aarch64c\n" || processor_string == "unknown\n";
    // TODO: the server cannot have both features activated at the same time
    if is_cheri && std::env::var("CARGO_FEATURE_MMU").is_err() {
        println!("cargo:rustc-cfg=feature=\"cheri\"");
    }
}
