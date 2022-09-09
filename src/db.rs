// src/db.rs

use mongodb::{bson::{doc, Document}, Client, error::Result};

pub struct Config {
    pub uri: String,  
}

impl Config {
    pub fn new() -> Self {
        let mongodb_uri = dotenv!("MONGODB_URI");      

        Self { uri: mongodb_uri.to_string() }
    }    
}

#[derive(Clone, Debug)]
pub struct DB {
    pub client: Client,
}

impl DB {
    pub async fn new(config: Config) -> Result<Self> {
        let client = Client::with_uri_str(&config.uri).await?;

        Ok(Self { client })
    }

    pub async fn add_documents(&self, collection_name: &str, documents: Vec<Document>) -> Result<()> {        
        let db = self.client.clone().database("busserebatetraces");
        let collection = db.collection(collection_name);

        // delete all documents
        collection.delete_many(doc! {}, None).await?;

        collection.insert_many(documents, None).await?;

        Ok(())
    }
}