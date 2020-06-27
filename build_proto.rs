//! To regenerate *.rs files in `src/proto/` rename this file to `build.rs`
//! and add this to `Cargo.toml`:
//! ```
//! [build-dependencies]
//! protoc-rust = "2.0"
//! ```

extern crate protoc_rust;

fn main() {
    let proto_files = ["src/proto/fileformat.proto", "src/proto/osmformat.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    protoc_rust::Codegen::new()
        .out_dir("src/proto")
        .inputs(&proto_files)
        .run()
        .expect("Running protoc failed.");
}
