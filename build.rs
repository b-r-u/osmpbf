use std::io::Write;

static MOD_RS: &[u8] = b"
/// Generated from protobuf.
pub mod fileformat;
/// Generated from protobuf.
pub mod osmformat;
";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_files = ["src/proto/fileformat.proto", "src/proto/osmformat.proto"];

    for path in &proto_files {
        println!("cargo:rerun-if-changed={path}");
    }

    let out_dir = std::env::var("OUT_DIR")?;

    let customizations = protobuf_codegen::Customize::default()
        .tokio_bytes(true)
        .tokio_bytes_for_string(true)
        .lite_runtime(true);

    protobuf_codegen::Codegen::new()
        .pure()
        .out_dir(&out_dir)
        .inputs(proto_files)
        .include("src/proto")
        .customize(customizations)
        .run()?;

    std::fs::File::create(out_dir + "/mod.rs")?.write_all(MOD_RS)?;

    Ok(())
}
