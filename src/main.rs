// src/main.rs

pub mod db;
pub mod meilisearch;
pub mod medline;
pub mod contract;
pub mod roster;
pub mod tracing;

#[macro_use]
extern crate dotenv_codegen;

use std::{error, sync::Arc};
use tokio;
//
use crate::medline::{MedlineInit};
//

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn error::Error>> {
    let medline = MedlineInit::new().await?;
    
    // debug
    let tracings = Arc::try_unwrap(medline.tracings).unwrap();

    let tracings = tracings.into_inner();

    println!("{}", tracings.len());
    
    for tracing in tracings {
        println!("{:?}", tracing);
    }
    // !debug

    Ok(())
}
