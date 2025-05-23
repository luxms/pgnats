use std::path::PathBuf;

fn main() {
    if !std::path::Path::new("tests/certs/server.key").exists()
        || !std::path::Path::new("tests/certs/server.crt").exists()
        || !std::path::Path::new("tests/certs/ca.crt").exists()
    {
        println!("Generating test certificates...");
        let status = std::process::Command::new("sh")
            .arg("generate_test_certs.sh")
            .status()
            .expect("failed to run generate_test_certs.sh");
        assert!(status.success(), "generate_test_certs.sh failed");
    }

    if std::env::var("SKIP_PGNATS_JS_TESTS").is_ok() {
        println!("cargo:rustc-cfg=skip_pgnats_js_tests");
    }

    if std::env::var("SKIP_PGNATS_TESTS").is_ok() {
        println!("cargo:rustc-cfg=skip_pgnats_tests");
    }

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let cargo_toml_path = PathBuf::from(&manifest_dir).join("Cargo.toml");

    let cargo_toml_content =
        std::fs::read_to_string(&cargo_toml_path).expect("Failed to read Cargo.toml");

    let mut manifest =
        cargo_toml::Manifest::from_str(&cargo_toml_content).expect("Failed to parse Cargo.toml");

    let pgrx_version = manifest
        .dependencies
        .get_mut("pgrx")
        .and_then(|dep| dep.try_detail_mut().ok())
        .and_then(|dep| dep.version.as_ref())
        .expect("pgrx dependency not found in Cargo.toml");

    let version_file = PathBuf::from(&manifest_dir).join(".cargo-pgrx-version");
    std::fs::write(&version_file, pgrx_version).expect("Failed to write .cargo-pgrx-version file");

    println!("cargo:rerun-if-env-changed=SKIP_PGNATS_JS_TESTS");
    println!("cargo:rerun-if-env-changed=SKIP_PGNATS_TESTS");
    println!("cargo:rerun-if-changed={}", cargo_toml_path.display());
}
