use std::process::Command;

fn main() {
    // detect cheri via uname (since cargo does not detect the feature yet)
    let output = Command::new("uname")
        .arg("-p")
        .output()
        .expect("Uname should be available");
    let processor_string = std::str::from_utf8(&output.stdout).unwrap();
    let is_cheri = processor_string == "aarch64c\n";
    if is_cheri {
        println!("cargo:rustc-cfg=feature=\"cheri\"");
    }
}
