extern crate protoc_rust;

fn main() {
    let proto_files = ["src/proto/fileformat.proto", "src/proto/osmformat.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    protoc_rust::run(protoc_rust::Args {
        out_dir: "src/proto",
        input: &proto_files,
        includes: &[],
    }).expect("protoc");
}
