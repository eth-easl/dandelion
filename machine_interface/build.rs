use cmake::Config;

fn main() {
    // cmake build config and run
    let dst = Config::new("c_machine_libraries")
        // .define("CMAKE_TOOLCHAIN_FILE", "morello-toolchain.txt")
        // .define("CMAKE_C_COMPILER", "cc")
        .build();
    // passing cmake information to cargo
    println!("cargo:warning={}", dst.display());
    println!("cargo:rustc-link-search=native={}", dst.display());
    println!("cargo:rustc-link-lib=static=cheri_mem");
}
