// src/contracts.rs

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Contract {
    pub contract: String,
    pub gpo: String,
    pub valid: bool,
    pub agreement: HashMap<String, Option<f32>>,
}

