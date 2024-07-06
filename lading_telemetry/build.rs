fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/");

    let includes = ["proto/"];
    prost_build::Config::new()
        .out_dir("src/proto/")
        .compile_protos(&["proto/telemetry/v1/telemetry.proto"], &includes)?;

    Ok(())
}
