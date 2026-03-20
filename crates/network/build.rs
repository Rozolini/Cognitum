/// Build script to automatically generate Rust code from Protocol Buffer definitions.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Compiles the gRPC service definitions before the crate is built.
    tonic_build::compile_protos("proto/cognitum.proto")?;
    Ok(())
}