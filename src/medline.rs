// src/medline.rs

use std::collections::HashMap;
use mongodb::bson::{doc, Document, Bson, self};
use serde::{Deserialize, Serialize};
use mongodb::{options::FindOptions, Client};
use meilisearch_sdk::{indexes::Index};
use futures::stream::TryStreamExt;
use chrono::{NaiveDate};
//
use crate::{roster::Roster, contract::Contract, tracing::Tracing};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Medline {
    #[serde(alias = "VendorCont")]
    pub contract: String,
    #[serde(alias = "Debit Memo")]
    pub claim_nbr: i64,
    #[serde(alias = "Invoice")]
    pub invoice_nbr: i64,
    #[serde(alias = "InvoiceDat")]
    pub invoice_date: i64,
    #[serde(alias = "VendorItm")]
    pub part: String,
    #[serde(alias = "Quantity")]
    pub ship_qty: i32,
    #[serde(alias = "RebateAmt")]
    pub rebate: f64,
    #[serde(alias = "CustName")]
    pub name: String,
    #[serde(alias = "CustStreet")]
    pub addr: String,
    #[serde(alias = "CustCity")]
    pub city: String,
    #[serde(alias = "CustState")]
    pub state: String,
    #[serde(alias = "UoM")]
    pub uom: String,
    #[serde(alias = "ContrCost")]    
    pub cost: f64,
    //
    pub unit_cost: Option<f64>,    
    pub ship_as_cs: Option<f64>,    
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MedlineSearchIndex {
    pub contract: String,
    pub name: String,
    pub addr: String,
    pub city: String,
    pub state: String,
    pub gpo: Option<String>,
    pub license: Option<String>, 
}

pub struct MedlineInit {
    pub client: Client,
    pub filter: Document,
    pub find_options: FindOptions,
    pub search_client: Index,
    pub license_map: HashMap<String, MedlineSearchIndex>
}

pub async fn find_license(search_client: &Index, query: &str) -> String {  
    let result = search_client.search()
        .with_query(query)
        // .with_filter(format!("group_name = {}", gpo))
        .with_limit(1)
        .execute::<Roster>().await.unwrap().hits;

    if result.len() == 0 {
        return String::from("0");
    } 
    
    // println!("result: {:?}", &result[0].result);

    result[0].result.member_id.clone()
}

pub async fn find_gpo(client: &Client, contract: &str) -> String {
    let collection = client.database("busserebatetraces").collection::<Contract>("contracts");

    let filter = doc! {
        "contract": contract
    };
    
    let result = collection.find_one(filter, None).await;

    match result {
        Ok(doc) => {
            match doc {
                Some(doc) => {
                    doc.gpo
                },
                None => {
                    String::from("MISSING CONTRACT")
                }
            }
        },
        Err(e) => {
            println!("Error: {:?}", e);
            String::from("MISSING CONTRACT")
        }
    }
}

pub async fn build_license_map(client: &Client, search_client: &Index, filter: &Document) -> mongodb::error::Result<HashMap<String, MedlineSearchIndex>> {
    let collection = client.database("busserebatetraces").collection::<Medline>("data_warehouse");

    let _distinct_names = collection.distinct("CustName", filter.clone(), None).await.unwrap();

    println!("count: {}", _distinct_names.len());

    let pipeline = vec![
        doc! {
            "$match": filter.clone()
        },
        doc! {
            "$group": {
                "_id": {
                    "VendorCont": "$VendorCont",
                    "CustName": "$CustName",
                    "CustStreet": "$CustStreet",
                    "CustCity": "$CustCity",
                    "CustState": "$CustState",                        
                }
            }
        },
        doc! {
            "$project": {
                "_id": 0,
                "contract": "$_id.VendorCont",                    
                "name": "$_id.CustName",
                "addr": "$_id.CustStreet",
                "city": "$_id.CustCity",
                "state": "$_id.CustState"                    
            }
        }
    ];
    
    let mut cursor = collection.aggregate(pipeline, None).await.unwrap();

    let mut license_map: HashMap<String, MedlineSearchIndex> = HashMap::new();
    
    while let Some(mut doc) = cursor.try_next().await? {
        let contract = doc.get_str("contract").unwrap();
        let name: &str = doc.get_str("name").unwrap();
        let addr: &str = doc.get_str("addr").unwrap();
        let city: &str = doc.get_str("city").unwrap();
        let state: &str = doc.get_str("state").unwrap();            
        
        let gpo = find_gpo(client, contract).await;

        let key = format!("{} {} {} {}", name, addr, city, state);
        let license = find_license(search_client, &key).await;
        
        doc.insert("gpo", Bson::String(gpo));
        doc.insert("license", Bson::String(license));
        
        // println!("doc: {:?}", doc);

        let medline_search_index: MedlineSearchIndex = bson::from_bson(Bson::Document(doc)).unwrap();

        license_map.insert(key, medline_search_index);
    }

    Ok(license_map)
}

impl MedlineInit {
    pub fn get_license_map(self) -> HashMap<String, MedlineSearchIndex> {
        self.license_map
    }

    pub async fn get_documents(self) -> mongodb::error::Result<Vec<Tracing>> {
        let collection = self.client.database("busserebatetraces").collection::<Medline>("data_warehouse");
                
        let mut cursor = collection.find(self.filter, self.find_options).await?;

        let mut tracings: Vec<Tracing> = Vec::new();

        while let Some(mut row) = cursor.try_next().await? {        
            let query = format!("{} {} {} {}", row.name, row.addr, row.city, row.state);

            // search self.license_map for row.name and get value if found else search meilisearch
            let license = &self.license_map.get(&query).unwrap().license.as_ref().unwrap();
            let gpo = &self.license_map.get(&query).unwrap().gpo.as_ref().unwrap();

            row.unit_cost = Some(row.cost / (row.ship_qty as f64).abs());
                        
            tracings.push(Tracing {
                contract: Some(row.contract),
                gpo: gpo.to_string(),
                license: Some(license.to_string()),
                ship_qty: row.ship_qty,
                rebate: row.rebate,
                name: row.name,
                addr: row.addr,
                city: row.city,
                state: row.state,
                uom: row.uom,
                cost: row.cost,
                //                                
                invoice_nbr: Some(row.invoice_nbr.to_string()),
                invoice_date: NaiveDate::parse_from_str(&row.invoice_date.to_string(), "%Y-%m-%d").unwrap(),
                claim_nbr: Some(row.claim_nbr.to_string()),
                period: format!("{}-{}", dotenv!("PERIOD"), "MEDLINE".to_string()),
                part: row.part,
                unit_rebate: row.rebate / (row.ship_qty as f64).abs(),
                ship_qty_as_cs: 0,
            });
            
        }

        Ok(tracings)
    }
}




// {
//     "file": "2750-082022-Rebate_File.xlsx",
//     "header": 0,
//     "period": "MEDLINE",
//     "filter": {
//         "__file__": {
//             "$regex": "2750-082022-Rebate_File.xlsx",
//             "$options": "i"
//         },
//         "__month__": "08",
//         "__year__": "2022"
//     },
//     "month": "august",
//     "year": "2022",
//     "contract": "VendorCont",
//     "cull_missing_contracts": false,
//     "claim_nbr": "Debit Memo",
//     "order_nbr": "Invoice",
//     "invoice_nbr": "Invoice",
//     "invoice_date": "InvoiceDat",
//     "part": "VendorItm",
//     "part_regex": "",
//     "ship_qty": "Quantity",
//     "unit_rebate": null,
//     "rebate": "RebateAmt",
//     "name": "CustName",
//     "addr": "CustStreet",
//     "city": "CustCity",
//     "state": "CustState",
//     "uom": "UoM",
//     "cost": "ContrCost",
//     "cost_calculation": "cost * ship_qty",
//     "addr1": null,
//     "addr2": null
// }