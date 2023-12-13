use futures::stream::{self, StreamExt};
use libs::db::{self, insert_file, Chunk};
use libs::master_service::master_service_server::{MasterService, MasterServiceServer};
use libs::master_service::{
    ConnectRequest, ConnectResponse, ReadFileRequest, ReadFileResponse, UploadFileToMasterRequest,
    UploadFileToMasterResponse,
};
use libs::slave_service::slave_service_client::SlaveServiceClient;
use libs::slave_service::{ReadChunkRequest, UploadFileToSlaveRequest};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{
        server::{TcpConnectInfo, TlsConnectInfo},
        Server, ServerTlsConfig,
    },
    Request, Response, Status, Streaming,
};

#[derive(Debug, Default)]
pub struct Service {
    slave_servers: Arc<RwLock<VecDeque<SlaveServer>>>,
}

#[derive(Debug)]
struct SlaveServer {
    ip: String,
    port: i32,
}

#[tonic::async_trait]
impl MasterService for Service {
    type readFileStream = ReceiverStream<Result<ReadFileResponse, Status>>;
    async fn upload_file(
        &self,
        request: Request<Streaming<UploadFileToMasterRequest>>,
    ) -> Result<Response<UploadFileToMasterResponse>, Status> {
        let mut stream = request.into_inner();

        let mut file: Vec<Chunk> = Vec::new();
        let file_id = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(n) => n.as_nanos().to_string(),
            Err(_) => panic!("SystemTime before UNIX EPOCH!"),
        };
        // Handling the slave server selection and upload
        let mut file_format = "txt".to_string();
        while let Some(data) = stream.next().await {
            let data = data.unwrap();
            file_format = data.file_format;
            let mut slave_servers = self.slave_servers.write().await;
            let slave_server = slave_servers
                .pop_front()
                .ok_or_else(|| Status::internal("No slave servers available"))?;

            let slave_server_addr = format!(
                "http://{}:{}",
                slave_server.ip.to_string(),
                slave_server.port.to_string()
            );
            println!("Slave server address: {}", slave_server_addr);
            let mut slave_client = SlaveServiceClient::connect(slave_server_addr)
                .await
                .map_err(|e| Status::internal(format!("Failed to connect to slave: {}", e)))?;

            // Creating the request stream for the slave server
            let request_stream = stream::iter(vec![UploadFileToSlaveRequest {
                binary: data.binary,
            }]);

            let upload_response = slave_client
                .upload_file(request_stream)
                .await
                .map_err(|e| Status::internal(format!("Failed to upload to slave: {}", e)))?;
            file.push(Chunk {
                pathname: upload_response.into_inner().file_path,
                server: slave_server.ip.to_string(),
            });
            // Re-add the slave server to the list
            slave_servers.push_back(slave_server);
        }
        let inserted_id = insert_file(file, file_id, file_format).await.map_err(|e| {
            Status::internal(format!(
                "Failed to save file metadata in the database: {}",
                e
            ))
        })?;
        Ok(Response::new(UploadFileToMasterResponse {
            url: inserted_id.to_string(),
        }))
    }
    async fn connect_to_master(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<ConnectResponse>, Status> {
        let data = request.into_inner();
        let mut slave_servers = self.slave_servers.write().await;
        slave_servers.push_front(SlaveServer {
            ip: data.ip,
            port: data.port,
        });
        println!("Slave servers: {:?}", slave_servers);

        Ok(Response::new(ConnectResponse { success: true }))
    }
    async fn read_file(
        &self,
        request: Request<ReadFileRequest>,
    ) -> Result<Response<Self::readFileStream>, Status> {
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let data = request.into_inner();
            let file_result = db::get_file(data.file_id)
                .await
                .map_err(|e| Status::internal(format!("Failed to get file: {}", e)))
                .unwrap();
            let mut chunks = file_result.chunks.iter();
            while let Some(chunk_data) = chunks.next() {
                let mut slave_client =
                    SlaveServiceClient::connect(format!("http://{}:{}", chunk_data.server, 50052))
                        .await
                        .map_err(|e| Status::internal(format!("Failed to connect to slave: {}", e)))
                        .unwrap();
                let response = slave_client
                    .get_chunk(ReadChunkRequest {
                        file_path: chunk_data.pathname.clone(),
                    })
                    .await
                    .map_err(|e| Status::internal(format!("Failed to get chunk: {}", e)))
                    .unwrap();
                let mut stream = response.into_inner();
                while let Some(chunk) = stream.next().await {
                    println!("Chunk: {:?}", chunk);
                    let chunk = chunk.unwrap();
                    tx.send(Ok(ReadFileResponse {
                        binary: chunk.binary,
                        file_name: file_result.file_id.clone(),
                        file_format: file_result.file_format.clone(),
                    }))
                    .await
                    .unwrap();
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse().unwrap();
    let service = Service::default();
    println!("Master server listening on: {}", addr);
    Server::builder()
        .add_service(MasterServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
