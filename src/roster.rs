// main/roster.rs

use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Roster {
    pub id: String,
    pub member_id: String,
    pub group_name: String,
    pub alias: Vec<String>,
    pub name: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub postal: String,
    pub gln: Option<String>,
    pub hin: Option<String>,    
}

