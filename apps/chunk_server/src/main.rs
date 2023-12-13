use dotenv::dotenv;
use futures::stream::StreamExt;
use libs::master_service::master_service_client::MasterServiceClient;
use libs::master_service::ConnectRequest;
use libs::slave_service::slave_service_server::{SlaveService, SlaveServiceServer};
use libs::slave_service::{
    ReadChunkRequest, ReadChunkResponse, UploadFileToSlaveRequest, UploadFileToSlaveResponse,
};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, BufReader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
#[derive(Debug, Default)]
pub struct Service {}
#[tonic::async_trait]
impl SlaveService for Service {
    type getChunkStream = ReceiverStream<Result<ReadChunkResponse, Status>>;
    async fn upload_file(
        &self,
        request: Request<Streaming<UploadFileToSlaveRequest>>,
    ) -> Result<Response<UploadFileToSlaveResponse>, Status> {
        let mut stream = request.into_inner();
        let timestamp = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_nanos().to_string(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        let path = format!("/files/{}", timestamp);
        File::create(&path).await?;
        while let Some(data) = stream.next().await {
            let data = data.map_err(|e| Status::internal(format!("Stream error: {}", e)))?;

            // add vector type string to store the file
            println!("Got chunk: {:?}", data);
            fs::write(&path, data.binary).await?;
        }
        Ok(Response::new(UploadFileToSlaveResponse { file_path: path }))
    }
    async fn get_chunk(
        &self,
        request: Request<ReadChunkRequest>,
    ) -> Result<Response<Self::getChunkStream>, Status> {
        let data = request.into_inner();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let file = File::open(data.file_path).await.unwrap();
            let mut reader = BufReader::new(file);
            let mut buffer = [0; 4096];
            loop {
                let n = reader.read(&mut buffer).await.unwrap();
                if n == 0 {
                    break;
                }
                let response = ReadChunkResponse {
                    binary: buffer[..n].to_vec().into(),
                };
                tx.send(Ok(response)).await.unwrap();
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
async fn add_server_to_slaves() -> Result<(), Box<dyn std::error::Error>> {
    println!("Adding server to slaves......");
    let self_address: String = std::env::var("SELF_ADDRESS")?.parse().unwrap();
    let assigned_port: i32 = std::env::var("PORT")?.parse().unwrap();
    println!("Assigned port: {}", assigned_port);
    let masters: Vec<String> = std::env::var("MASTERS")?
        .split(",")
        .map(|s| s.to_string())
        .collect();
    println!("Masters: {:?}", masters);
    let mut masters_iter = masters.iter();
    while let Some(master) = masters_iter.next() {
        let master_addr = format!("http://{}", master);
        let mut client = MasterServiceClient::connect(master_addr).await?;
        match client
            .connect_to_master(ConnectRequest {
                ip: self_address.clone(),
                port: assigned_port,
            })
            .await
        {
            Ok(_) => {
                println!("Added server to master: {}", master);
                continue;
            }
            Err(e) => {
                println!("Error adding server to master: {}", e);
                continue;
            }
        }
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let assigned_port: i32 = std::env::var("PORT")?.parse().unwrap();
    let addr = format!("0.0.0.0:{}", assigned_port).parse()?;
    let service = Service::default();
    let server = Server::builder()
        .add_service(SlaveServiceServer::new(service))
        .serve(addr);
    println!("Slave server listening on {}", addr);

    fs::create_dir_all("/files").await?;
    println!("Created files directory");
    match add_server_to_slaves().await {
        Ok(_) => println!("Added server to slaves"),
        Err(e) => println!("Error adding server to slaves: {}", e),
    }
    server.await?;

    Ok(())
}
