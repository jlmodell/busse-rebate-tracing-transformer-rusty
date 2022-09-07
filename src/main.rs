// src/main.rs

pub mod db;
pub mod meilisearch;
pub mod medline;
pub mod contract;
pub mod roster;
pub mod tracing;

#[macro_use]
extern crate dotenv_codegen;

use std::collections::HashMap;
use std::error;
use tokio;
use mongodb::{bson::doc, Client, options::ClientOptions, options::FindOptions};
//
use crate::db::Config;
use crate::meilisearch::Meilisearch;
use crate::medline::{MedlineInit, build_license_map};
//

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn error::Error>> {
    let config = Config::new();
    let meilisearch = Meilisearch::new();

    // setup mongodb
    let client_options = ClientOptions::parse(&config.uri).await?;
    let client = Client::with_options(client_options)?;

    // medline - TODO: create a match function to handle different data sources
    let mut medline = MedlineInit {
        client: client,
        filter: doc! { "__file__": { "$regex": "2750-082022-Rebate_File.xlsx", "$options": "i" }, "__month__": "08", "__year__": "2022" },
        find_options: FindOptions::builder().sort(doc! { "CustName": 1 }).build(),
        search_client: meilisearch.index,
        license_map: HashMap::new(),
    }; 
    
    medline.license_map = build_license_map(&medline.client, &medline.search_client, &medline.filter).await?;

    for (key, value) in &medline.license_map {
        println!("{}: {:?}", key, value);
    }
    
    // let records = medline.get_documents().await?;

    // // debug
    // for record in records {
    //     println!("{:?}", record);
    // }
    // // !debug

    Ok(())
}
