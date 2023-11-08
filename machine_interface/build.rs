use cmake::Config;
use std::process::Command;

fn cmake_libraries() -> () {
    // cmake configure and build all
    let _all = Config::new("c_machine_libraries")
        .define("FORCE_BUILD_CHERI", "")
        .build_target("all")
        .build();
    // run tests tests
    let _test = Config::new("c_machine_libraries")
        .build_target("test")
        .build();
    // install
    let install = Config::new("c_machine_libraries").build();
    // passing cmake information to cargo
    println!("cargo:rustc-link-search=native={}", install.display());
    println!("cargo:rustc-link-lib=static=cheri_lib");
}

fn main() {
    // detect cheri via uname (since cargo does not detect the feature yet)
    let output = Command::new("uname")
        .arg("-p")
        .output()
        .expect("Uname should be available");
    let processor_string = std::str::from_utf8(&output.stdout).unwrap();
    // morello linux has unkonw for now, check later if this is portability issue / better way to detect
    let is_cheri = processor_string == "aarch64c\n" || processor_string == "unknown\n";
    if is_cheri && std::env::var("CARGO_FEATURE_MMU").is_err() {
        println!("cargo:rustc-cfg=feature=\"cheri\"");
        cmake_libraries();
    }
}
