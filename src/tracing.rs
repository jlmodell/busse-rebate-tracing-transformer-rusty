// src/generic_tracing.rs

use chrono::{NaiveDate};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Tracing {
    // info for distributor optional
    pub claim_nbr: Option<String>,
    pub invoice_nbr: Option<String>,
    pub invoice_date: NaiveDate,
    //
    pub gpo: String,
    pub contract: Option<String>,
    pub license: Option<String>,
    // locate member id
    pub period: String,
    pub name: String,
    pub addr: String,
    pub city: String,
    pub state: String,
    // rebate and sales info
    pub part: String,
    pub unit_rebate: f64,    
    pub ship_qty: i32,
    pub ship_qty_as_cs: i32,
    pub uom: String,    
    pub rebate: f64,
    pub cost: f64,
}