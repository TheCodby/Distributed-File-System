use dotenv::dotenv;
use mongodb::bson::{self, doc, Bson};
use mongodb::{bson::Document, Client, Collection};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, Debug)]
pub struct Chunk {
    pub pathname: String,
    pub server: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct File {
    pub chunks: Vec<Chunk>,
    pub file_id: String,
    pub file_format: String,
}
pub async fn insert_file(
    chunks: Vec<Chunk>,
    file_id: String,
    file_format: String,
) -> Result<Bson, Box<dyn std::error::Error>> {
    dotenv().ok();
    let database_uri: String = std::env::var("DATABASE_URL")?.parse().unwrap();
    let client = Client::with_uri_str(database_uri).await?;

    let database = client.database("dfs");
    let collection = database.collection("chunks");
    let bson_chunks = bson::to_bson(&chunks)?;
    let new_document = doc! {
        "fileId": file_id,
        "fileFormat": file_format,
        "chunks": bson_chunks,
    };
    let result = collection.insert_one(new_document, None).await?;
    Ok(result.inserted_id)
}
pub async fn get_file(file_id: String) -> Result<File, Box<dyn std::error::Error>> {
    dotenv().ok();
    let database_uri: String = std::env::var("DATABASE_URL")?.parse().unwrap();
    let client = Client::with_uri_str(database_uri).await?;

    let database = client.database("dfs");
    let collection: Collection<Document> = database.collection("chunks");
    let filter = doc! {
        "fileId": file_id,
    };
    let document = collection.find_one(filter, None).await?.unwrap();
    println!(" /// document: {:?}", document);
    let bson_chunks = document.get("chunks").unwrap().clone();
    let chunks: Vec<Chunk> = bson::from_bson(bson_chunks)?;
    let file = File {
        chunks: chunks,
        file_id: document.get("fileId").unwrap().to_string(),
        file_format: document.get("fileFormat").unwrap().to_string(),
    };
    Ok(file)
}
