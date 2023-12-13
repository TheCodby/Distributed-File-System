fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/master_service.proto")?;
    tonic_build::compile_protos("../proto/slave_service.proto")?;
    Ok(())
}
