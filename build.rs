fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../contracts/jafra.proto");
    let proto_dir = proto.parent().expect("contracts directory").to_path_buf();
    println!("cargo:rerun-if-changed={}", proto.display());
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[&proto], &[&proto_dir])?;
    Ok(())
}
