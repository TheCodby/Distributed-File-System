pub mod db;
pub mod master_service {
    tonic::include_proto!("masterservice");
}
pub mod slave_service {
    tonic::include_proto!("slaveservice");
}
