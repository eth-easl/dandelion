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
    // passing cmake information to c
    println!("cargo:rustc-link-search=native={}", install.display());
    println!("cargo:rustc-link-lib=static=cheri_lib");
}

fn main() {
    // check if cheri is enabled and build library if so
    #[cfg(features = "cheri")]
    cmake_libraries();
}