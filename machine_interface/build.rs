use cmake::Config;

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

fn libraries_gpu() {
    #[cfg(feature = "hip")]
    {
        // Link with HIP Runtime 6.2.2
        println!("cargo:rustc-link-search=/opt/rocm-6.2.2/lib");
        println!("cargo:rustc-link-lib=amdhip64");
    }

    #[cfg(feature = "cuda")]
    {
        // Link with CUDA Device API
        println!("cargo:rustc-link-search=/usr/local/cuda/lib64/stubs");
        println!("cargo:rustc-link-lib=cuda");

        // Link with CUDA Runtime API
        println!("cargo:rustc-link-search=/usr/local/cuda/lib64");
        println!("cargo:rustc-link-lib=cudart");
    }
}

fn main() {
    // check if cheri is enabled and build library if so
    #[cfg(feature = "cheri")]
    cmake_libraries();

    #[cfg(feature = "gpu")]
    libraries_gpu();
}
