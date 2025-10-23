//! Build script for `lading` crate.

fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/");

    let includes = ["proto/"];

    // Compile agent_payload.proto (metrics) without gRPC services
    prost_build::Config::new()
        .out_dir("src/proto/")
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_protos(&["proto/agent_payload.proto"], &includes)?;

    // Compile stateful_encoding.proto with gRPC services
    tonic_build::configure()
        .out_dir("src/proto/")
        .compile_protos(&["proto/stateful_encoding.proto"], &includes)?;

    Ok(())
}
