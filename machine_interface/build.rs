use cmake::Config;
use std::process::Command;

fn cmake_libraries_cheri() {
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

fn cmake_libraries_gpu() {
    let install = Config::new("hip_interface").build();
    println!("cargo:rustc-link-search=native={}/build", install.display());
    println!("cargo:rustc-link-lib=static=hip_interface_lib");
    // link with hip runtime TODO make less hard coded
    println!("cargo:rustc-link-search=/opt/rocm-5.7.1/lib");
    println!("cargo:rustc-link-lib=amdhip64");
}

fn main() {
    // check if cheri is enabled and build library if so
    // Jonathan: pretty sure this is a typo (features instead of feature, but don't want to touch it)
    #[cfg(features = "cheri")]
    cmake_libraries_cheri();

    #[cfg(feature = "gpu")]
    cmake_libraries_gpu();
}
