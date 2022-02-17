fn main() {
    let proto_files = ["src/proto/fileformat.proto", "src/proto/osmformat.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={}", path);
    }

    protobuf_codegen_pure::Codegen::new()
        .out_dir("src/proto")
        .inputs(&proto_files)
        .include("src/proto")
        .run()
        .expect("Generating protobuf files failed.");
}
