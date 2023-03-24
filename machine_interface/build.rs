use cmake::Config;

fn main() {
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
