use std::path::PathBuf;

fn main() {
  if std::env::var("SKIP_NATS_JS_TESTS").is_ok() {
    println!("cargo:rustc-cfg=skip_nats_js_tests");
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

  println!("cargo:rerun-if-changed={}", cargo_toml_path.display());
}
