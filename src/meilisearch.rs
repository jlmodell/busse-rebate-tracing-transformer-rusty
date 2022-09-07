// src/meilisearch.rs

use meilisearch_sdk::{client::Client as MeilisearchClient, indexes::Index};
//
use crate::roster::Roster;

pub struct Meilisearch {
    pub client: MeilisearchClient,
    pub index: Index,
}

impl Meilisearch {
    pub fn new() -> Self {
        let client = meilisearch_sdk::client::Client::new(dotenv!("MEILISEARCH_URL"), dotenv!("MEILISEARCH_KEY"));
        let index = client.index("rosters".to_string());
        Self { client, index }
    }

    pub fn add_documents(&self, documents: Vec<Roster>) {
        
        let _ = self.index.add_documents(&documents, None);
    }

    pub fn delete_documents(&self, documents: Vec<String>) {        
        let _ = self.index.delete_documents(&documents);
    }
}