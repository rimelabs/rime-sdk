use std::{env, fs, path::Path};

fn main() {
    println!("cargo:rerun-if-changed=vendor");
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .std("c++11")
        .warnings(false)
        .define("NDEBUG", None)
        .define("HAVE_NO_SPECSTRINGS", None)
        .define("BLING_FIRE_NOAP", None)
        .include("vendor/blingfireclient.library/inc")
        .include("vendor/blingfireclient.library/src")
        .include("vendor/blingfirecompile.library/inc")
        .include("vendor/blingfiretools/blingfiretokdll");
    let os = env::var("CARGO_CFG_TARGET_OS").unwrap();
    if os != "windows" {
        build.define("BLING_FIRE_NOWINDOWS", None);
    }
    if os == "macos" {
        build.define("BLING_FIRE_MAC", None);
    }
    for directory in [
        "vendor/blingfireclient.library/src",
        "vendor/blingfiretools/blingfiretokdll",
    ] {
        let mut files: Vec<_> = fs::read_dir(directory)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|x| x == "cpp"))
            .collect();
        files.sort();
        for file in files {
            build.file(Path::new(&file));
        }
    }
    build.compile("rime_blingfire");
}
