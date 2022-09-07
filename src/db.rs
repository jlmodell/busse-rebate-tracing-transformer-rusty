// src/db.rs

pub struct Config {
    pub uri: String,  
}

impl Config {
    pub fn new() -> Self {
        let mongodb_uri = dotenv!("MONGODB_URI");      

        Self { uri: mongodb_uri.to_string() }
    }
}