fn main() {
    // check if cheri is enabled and build library if so
    #[cfg(feature = "cheri")]
    {
        let morello_sdk = std::env::var("MORELLO_SDK").unwrap();
        let mut toolchainfile = std::env::var("CARGO_MANIFEST_DIR").unwrap();
        toolchainfile.push_str("/c_machine_libraries/morello-toolchain.txt");
        let mut new_toolchainfile = std::env::var("OUT_DIR").unwrap();
        new_toolchainfile.push_str("/morello-toolchain.txt");
        std::fs::copy(toolchainfile, &new_toolchainfile).unwrap();

        let mut replace_string = String::from("s|SDK_LOCATION|");
        replace_string.push_str(&morello_sdk);
        replace_string.push_str("|g");
        let sed_out = std::process::Command::new("sed")
            .arg("-i")
            .arg("-e")
            .arg(replace_string)
            .arg(&new_toolchainfile)
            .output()
            .unwrap();
        println!("Sed out: {:?}", sed_out);

        use cmake::Config;
        // cmake configure and build all
        let _all = Config::new("c_machine_libraries")
            .define("FORCE_BUILD_CHERI", "")
            .define("CMAKE_TOOLCHAIN_FILE", &new_toolchainfile)
            .build_target("all")
            .build();
        // run tests tests
        // let _test = Config::new("c_machine_libraries")
        //     .build_target("test")
        //     .build();
        // install
        let install = Config::new("c_machine_libraries").build();
        // passing cmake information to c
        println!("cargo:rustc-link-search=native={}", install.display());
        println!("cargo:rustc-link-lib=static=cheri_lib");
    }
}
