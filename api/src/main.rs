use actix_web::{web, App, HttpServer, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use actix::prelude::*;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use influxdb::Client;
use std::time::Duration;
use tokio::time;
use std::sync::Mutex;
use influxdb::ReadQuery;
use std::sync::Arc;
use actix::StreamHandler;
use chrono::DateTime;
use tokio::time::interval;
use chrono::Utc;
#[derive(Debug, Serialize, Deserialize)]
struct BondingAccount {
    base_mint: Pubkey,
    target_mint: Pubkey,
    general_authority: Option<Pubkey>,
    reserve_authority: Option<Pubkey>,
    curve_authority: Option<Pubkey>,
    base_storage: Pubkey,
    buy_base_royalties: Pubkey,
    buy_target_royalties: Pubkey,
    sell_base_royalties: Pubkey,
    sell_target_royalties: Pubkey,
    buy_base_royalty_percentage: u32,
    buy_target_royalty_percentage: u32,
    sell_base_royalty_percentage: u32,
    sell_target_royalty_percentage: u32,
    curve: Pubkey,
    mint_cap: Option<u64>,
    purchase_cap: Option<u64>,
    go_live_unix_time: i64,
    freeze_buy_unix_time: Option<i64>,
    created_at_unix_time: i64,
    buy_frozen: bool,
    sell_frozen: bool,
    index: u16,
    bump_seed: u8,
    base_storage_bump_seed: u8,
    target_mint_authority_bump_seed: u8,
    base_storage_authority_bump_seed: Option<u8>,
    reserve_balance_from_bonding: u64,
    supply_from_bonding: u64,
    ignore_external_reserve_changes: bool,
    ignore_external_supply_changes: bool,
}

struct BondingWebSocket {
    address: Pubkey,
    bonding_account: Arc<BondingAccount>,
    client: Arc<Mutex<Client>>
}


#[derive(Deserialize)]
struct BondingRequest {
    address: String,
    start_unix_time: DateTime<Utc>,
    stop_unix_time: DateTime<Utc>,
}

#[derive(Serialize)]
struct BondingChange {
    reserve_change: f64,
    supply_change: f64,
    insert_ts: i64,
}

impl BondingWebSocket {
 fn handle_message(&mut self, msg: ws::Message, ctx: &mut ws::WebsocketContext<Self>) {
        if let ws::Message::Text(text) = msg {
            if let Ok(request) = serde_json::from_str::<serde_json::Value>(&text) {
                if let Some(request_type) = request["type"].as_str() {
                    match request_type {
                        "getAccountInfo" => {
                            ctx.text(serde_json::to_string(&*self.bonding_account).unwrap());
                        },
                        _ => println!("Unknown request type"),
                    }
                }
            }
        }
    }
    async fn query_historical_data(&self, start_time: i64, stop_time: i64) -> Vec<BondingChange> {
        let client = self.client.lock().unwrap();
        let query = ReadQuery::new(format!(
            r#"
            from(bucket:"mybucket")
                |> range(start: {}, stop: {})
                |> filter(fn: (r) => r.address == "{}")
                |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
            "#,
            start_time, stop_time, self.address
        ));
    
        let result = client.query(query).await.expect("Failed to query InfluxDB");
        let rows: Vec<Vec<&str>> = result.lines()
        .skip(1) // Skip the header row
        .filter(|line| !line.is_empty())
        .map(|line| line.split(',').collect())
        .collect();

    rows.into_iter().filter_map(|row| {
        if row.len() >= 3 {
            Some(BondingChange {
                reserve_change: row[1].parse().unwrap_or(0.0),
                supply_change: row[2].parse().unwrap_or(0.0),
                insert_ts: DateTime::parse_from_rfc3339(row[0])
                    .map(|dt| dt.timestamp())
                    .unwrap_or(0),
            })
        } else {
            None
        }
    }).collect()
    }
    async fn query_latest_data(&self, address: &str) -> Option<BondingChange> {
        let query = {
            let self_lock = self.client.lock().unwrap();
            ReadQuery::new(format!(
                r#"
                from(bucket:"mybucket")
                    |> range(start: -5s)
                    |> filter(fn: (r) => r.address == "{}")
                    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
                    |> last()
                "#,
                address
            ))
        };
        let result = {
            let self_lock = self.client.lock().unwrap();
            self_lock.query(query).await.expect("Failed to query InfluxDB")
        };
        
        let rows: Vec<Vec<&str>> = result.lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .map(|line| line.split(',').collect())
        .collect();

    rows.into_iter().next().and_then(|row| {
        if row.len() >= 3 {
            Some(BondingChange {
                reserve_change: row[1].parse().unwrap_or(0.0),
                supply_change: row[2].parse().unwrap_or(0.0),
                insert_ts: DateTime::parse_from_rfc3339(row[0])
                    .map(|dt| dt.timestamp())
                    .unwrap_or(0),
            })
        } else {
            None
        }
    })
    }
}

impl Actor for BondingWebSocket {
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        let self_arc = Arc::new(Mutex::new(self.clone()));
        let addr = ctx.address();
        let address = self.address.clone();
        actix::spawn(async move {
            let start_time = Utc::now() - Duration::from_secs(24 * 60 * 60);
            let stop_time = Utc::now();
            let historical_data = {
                let self_lock = self_arc.lock().unwrap();
                self_lock.query_historical_data(start_time.timestamp(), stop_time.timestamp()).await
            };
    
            addr.do_send(SendHistoricalData(historical_data));
    
    
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let latest_data = {
                    let self_lock = self_arc.lock().unwrap();
                    self_lock.query_latest_data(&address.to_string()).await.expect("Failed to query latest data")
                };
                addr.do_send(SendLatestData(latest_data));
            }
        });
    }
    
    
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for BondingWebSocket {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => ctx.pong(&msg),
            Ok(ws::Message::Text(text)) => {
                self.handle_message(ws::Message::Text(text), ctx);
            },            _ => (),
        }
    }
}
async fn bonding_ws(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, actix_web::Error> {
    let address = Pubkey::from_str(
        req.match_info().get("address").unwrap()
    ).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
    let client = Arc::new(Mutex::new(Client::new("http://localhost:8086", "mybucket").with_token("myinfluxdbtoken")));
    // Here you would typically fetch the account data from Solana
    // For this example, we'll create a dummy account
    let bonding_account = Arc::new(BondingAccount {
        base_mint: Pubkey::new_unique(),
        target_mint: Pubkey::new_unique(),
        general_authority: Some(Pubkey::new_unique()),
        reserve_authority: None,
        curve_authority: None,
        base_storage: Pubkey::new_unique(),
        buy_base_royalties: Pubkey::new_unique(),
        buy_target_royalties: Pubkey::new_unique(),
        sell_base_royalties: Pubkey::new_unique(),
        sell_target_royalties: Pubkey::new_unique(),
        buy_base_royalty_percentage: 0,
        buy_target_royalty_percentage: 0,
        sell_base_royalty_percentage: 42_949_672,
        sell_target_royalty_percentage: 42_949_672,
        curve: Pubkey::new_unique(),
        mint_cap: None,
        purchase_cap: None,
        go_live_unix_time: 1_724_599_165,
        freeze_buy_unix_time: None,
        created_at_unix_time: 1_724_599_165,
        buy_frozen: false,
        sell_frozen: false,
        index: 0,
        bump_seed: 253,
        base_storage_bump_seed: 0,
        target_mint_authority_bump_seed: 0,
        base_storage_authority_bump_seed: None,
        reserve_balance_from_bonding: 15_490_578_321,
        supply_from_bonding: 27_589_013_699,
        ignore_external_reserve_changes: false,
        ignore_external_supply_changes: false,
    });

    ws::start(
        BondingWebSocket { address, bonding_account, client },
        &req,
        stream,
    )
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| {
        App::new()
            .route("/ws/{address}", web::get().to(bonding_ws))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

struct SendHistoricalData(Vec<BondingChange>);
struct SendLatestData(BondingChange);

// Implement Clone for BondingWebSocket
impl Clone for BondingWebSocket {
    fn clone(&self) -> Self {
        // Implement the clone method based on your struct fields
        Self {
            client: self.client.clone(),
            address: self.address.clone(),
            bonding_account: self.bonding_account.clone()
            // Clone other fields as necessary
        }
    }
}

impl Message for SendLatestData {
    type Result = ();
}

impl Handler<SendLatestData> for BondingWebSocket {
    type Result = ();

    fn handle(&mut self, msg: SendLatestData, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg.0).unwrap());
    }
}
impl Message for SendHistoricalData {
    type Result = ();
}

impl Handler<SendHistoricalData> for BondingWebSocket {
    type Result = ();

    fn handle(&mut self, msg: SendHistoricalData, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&msg.0).unwrap());
    }
}