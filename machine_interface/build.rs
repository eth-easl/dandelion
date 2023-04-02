use cmake::Config;
use std::process::Command;

fn cmake_libraries() -> () {
    // cmake configure and build all
    let _all = Config::new("c_machine_libraries")
        // .define("CMAKE_TOOLCHAIN_FILE", "morello-toolchain.txt")
        // .define("CMAKE_C_COMPILER", "cc")
        .build_target("all")
        .build();
    // run tests tests
    let _test = Config::new("c_machine_libraries")
        .build_target("test")
        .build();
    // install
    let install = Config::new("c_machine_libraries").build();
    // passing cmake information to cargo
    println!("cargo:warning={}", install.display());
    println!("cargo:rustc-link-search=native={}", install.display());
    println!("cargo:rustc-link-lib=static=cheri_mem");
}

fn main() {
    // detect cheri via uname (since cargo does not detect the feature yet)
    let output = Command::new("uname")
        .arg("-p")
        .output()
        .expect("Uname should be available");
    let processor_string = std::str::from_utf8(&output.stdout).unwrap();
    println!("cargo:warning=Uname is : {}", processor_string);
    let is_cheri = processor_string == "aarch64c\n";
    if is_cheri {
        println!("cargo:rustc-cfg=feature=\"cheri\"");
        cmake_libraries();
    }
}
