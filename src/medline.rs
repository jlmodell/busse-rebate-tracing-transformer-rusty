// src/medline.rs

use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use mongodb::{bson::{doc, Document, to_document}};
use serde::{Deserialize, Serialize};
use mongodb::{options::FindOptions, Client};
use meilisearch_sdk::{indexes::Index};
use futures::stream::{TryStreamExt};
use chrono::{NaiveDate};
//
use crate::{roster::Roster, contract::Contract, tracing::Tracing, meilisearch::Meilisearch, db::{Config, DB}};

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
    pub db: DB,
    pub client: Client,
    pub filter: Document,
    pub find_options: FindOptions,
    pub search_client: Index,
    pub contract_map: HashMap<String, String>,
    pub license_map: Arc<tokio::sync::Mutex<HashMap<String, MedlineSearchIndex>>>,
    pub tracings: Arc<Mutex<Vec<Tracing>>>
}

impl MedlineInit {
    // initialize MedlineInit
    pub async fn new() -> mongodb::error::Result<Self> {
        let config = Config::new();
        let meilisearch = Meilisearch::new();

        // setup mongodb
        let db = DB::new(config).await?;
        
        let mut medline = MedlineInit {
            db: db.clone(),
            client: db.client.clone(),
            filter: doc! { "__file__": { "$regex": "2750-082022-Rebate_File.xlsx", "$options": "i" }, "__month__": "08", "__year__": "2022" },
            find_options: FindOptions::builder().sort(doc! { "CustName": 1 }).build(),
            search_client: meilisearch.index,
            contract_map: HashMap::new(),
            license_map: Arc::new(Mutex::new(HashMap::new())),
            tracings: Arc::new(Mutex::new(Vec::new()))
        };

        medline.build_contract_map().await?;
        medline.build_license_map().await?;        
        medline.get_documents().await?;
        
        let documents = medline.tracings.lock().await.clone();
        let documents = documents.into_iter().map(|tracing| { to_document(&tracing).unwrap() }).collect::<Vec<_>>();
        
        db.add_documents("medline_09082022", documents).await?;

        Ok(medline)
    }

    // build contract map
    pub async fn build_contract_map(&mut self) -> mongodb::error::Result<()> {
        let collection = self.client.database("busserebatetraces").collection::<Contract>("contracts");

        let filter = None;
        let find_options = FindOptions::builder().sort(doc! { "contract": 1 }).build();
        
        let mut cursor = collection.find(filter, find_options).await?;

        while let Some(contract) = cursor.try_next().await? {            
            self.contract_map.insert(contract.contract.clone(), contract.gpo.clone());
        };

        Ok(())
    }

    // build license map
    pub async fn build_license_map(&self) -> mongodb::error::Result<()> {
        let collection = self.client.clone().database("busserebatetraces").collection::<Medline>("data_warehouse");

        let pipeline = vec![
            doc! {
                "$match": self.filter.clone()
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
        
        let cursor = collection.aggregate(pipeline, None).await.unwrap();

        cursor.try_for_each_concurrent(10 as usize, |doc| async move {

            let contract = doc.get_str("contract").unwrap();
            let name: &str = doc.get_str("name").unwrap();
            let addr: &str = doc.get_str("addr").unwrap();
            let city: &str = doc.get_str("city").unwrap();
            let state: &str = doc.get_str("state").unwrap();

            let gpo = find_gpo(&self.contract_map, contract);

            let query = format!("{} {} {} {}", name, addr, city, state);
            let filter: String = format!("group_name = {}", contract);

            let license = find_license(&self.search_client, &query, &filter).await;

            let medline_search_index = MedlineSearchIndex {
                contract: contract.to_string(),
                license: Some(license),
                gpo: Some(gpo.to_string()),
                name: name.to_string(),
                addr: addr.to_string(),
                city: city.to_string(),
                state: state.to_string(),
            };

            // println!("{:?}", medline_search_index);

            // add search index to self.license_map
            self.license_map.lock().await.insert(query, medline_search_index);                        

            Ok(())
        }).await?;

        // let mut local_license_map: HashMap<String, MedlineSearchIndex> = HashMap::new();

        // while let Some(doc) = cursor.try_next().await? {    
        //     let (key, medline_search_index) = handle_document_mutation(doc, client.clone(), search_client.clone()).await;
            
        //     local_license_map.insert(key, medline_search_index);
        // };

        Ok(())
    }

    // get documents from mongodb
    pub async fn get_documents(&self) -> mongodb::error::Result<()> {
        let collection = self.client.clone().database("busserebatetraces").collection::<Medline>("data_warehouse");
                
        let cursor = collection.find(self.filter.clone(), self.find_options.clone()).await?;    

        cursor.try_for_each_concurrent(100 as usize, |mut doc| async move {
            let query = format!("{} {} {} {}", doc.name, doc.addr, doc.city, doc.state);
            
            let license = self.license_map.lock().await.get(&query).unwrap().license.clone().unwrap();

            let gpo = self.license_map.lock().await.get(&query).unwrap().gpo.clone().unwrap();

            doc.unit_cost = Some(doc.cost / (doc.ship_qty as f64).abs());            

            let tracing: Tracing = Tracing {
                contract: Some(doc.contract),
                gpo: gpo.to_string(),
                license: Some(license.to_string()),
                //
                ship_qty: doc.ship_qty,
                rebate: doc.rebate,
                name: doc.name,
                addr: doc.addr,
                city: doc.city,
                state: doc.state,
                uom: doc.uom,
                cost: doc.cost,
                //                                
                invoice_nbr: Some(doc.invoice_nbr.to_string()),
                invoice_date: NaiveDate::parse_from_str(&doc.invoice_date.to_string(), "%Y%m%d").unwrap(),
                claim_nbr: Some(doc.claim_nbr.to_string()),
                period: format!("{}-{}", dotenv!("PERIOD"), "MEDLINE".to_string()),
                part: doc.part,
                unit_rebate: doc.rebate / (doc.ship_qty as f64).abs(),
                ship_qty_as_cs: 0,
            };            

            self.tracings.lock().await.push(tracing);         
            
            Ok(())
        }).await?;

        Ok(())
    }

}

pub async fn find_license(search_client: &Index, query: &str, filter: &str) -> String {  
    let result = search_client.search()
        .with_query(query)
        .with_filter(filter)
        .with_limit(1)
        .execute::<Roster>().await.unwrap().hits;

    if result.len() == 0 {
        return String::from("0");
    }     

    result[0].result.member_id.clone()
}

pub fn find_gpo(contract_map: &HashMap<String,String>, contract: &str) -> String {
    // check if contract is in contract_map
    // if so, return gpo
    // else, return "0"        
    contract_map.get(contract).unwrap_or(&String::from("MISSING CONTRACT")).to_string()
}

pub async fn count_distinct_documents(client: &Client, filter: &Document) {
    let collection = client.database("busserebatetraces").collection::<Medline>("data_warehouse");

    let _distinct_names = collection.distinct("CustName", filter.clone(), None).await.unwrap();

    println!("count: {}", _distinct_names.len());
}
