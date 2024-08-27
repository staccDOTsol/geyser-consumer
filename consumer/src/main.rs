use influxdb::{Client, InfluxDbWriteable, WriteQuery};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::prelude::*;
use chrono::Utc;
use bs58;
use     futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::TransactionError};
use std::time::Duration;
use yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof;
use std::collections::HashMap;
use tonic::transport::channel::ClientTlsConfig;
#[derive(InfluxDbWriteable)]
struct AccountUpdate {
    time: influxdb::Timestamp,
    pubkey: String,
    lamports: u64,
    owner: String,
    executable: bool,
    rent_epoch: u64,
    data_len: String,
    write_version: u64,
}


#[derive(Debug)]
#[allow(dead_code)]
pub struct AccountPretty {
    is_startup: bool,
    slot: u64,
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: String,
    write_version: u64,
    txn_signature: String,
}

impl From<SubscribeUpdateAccount> for AccountPretty {
    fn from(
        SubscribeUpdateAccount {
            is_startup,
            slot,
            account,
        }: SubscribeUpdateAccount,
    ) -> Self {
        let account = account.expect("should be defined");
        Self {
            is_startup,
            slot,
            pubkey: Pubkey::try_from(account.pubkey).expect("valid pubkey"),
            lamports: account.lamports,
            owner: Pubkey::try_from(account.owner).expect("valid pubkey"),
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: hex::encode(account.data),
            write_version: account.write_version,
            txn_signature: bs58::encode(account.txn_signature.unwrap_or_default()).into_string(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let geyser_endpoint = std::env::var("GEYSER_ENDPOINT").expect("GEYSER_ENDPOINT must be set");
    let x_token = std::env::var("X_TOKEN").expect("X_TOKEN must be set");
    let influxdb_url = std::env::var("INFLUXDB_URL").unwrap_or_else(|_| "http://influxdb:8086".to_string());
    let pubkey = std::env::var("PUBKEY").expect("PUBKEY must be set");
    let client = GeyserGrpcClient::build_from_shared(geyser_endpoint)?
        .x_token(Some(x_token))?
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new())?
        .connect()
        .await?;


    let influxdb_client = Client::new(influxdb_url, "mybucket");

    let request = SubscribeRequest {
        accounts: [(
            "account".to_string(),
            SubscribeRequestFilterAccounts {
                account: (vec![pubkey]), // Wrap pubkey in a Vec
                ..Default::default()
            },
        )]
        .into(),
        ..Default::default()
    };

    let resub = 100; // Example resubscribe value
    geyser_subscribe(client, request, resub, influxdb_client).await?;

    Ok(())
}

async fn geyser_subscribe(
    mut client: GeyserGrpcClient<impl yellowstone_grpc_proto::tonic::service::Interceptor>,
    request: SubscribeRequest,
    resub: usize,
    influxdb_client: Client,
) -> anyhow::Result<()> {
    let (mut subscribe_tx, mut stream) = client.subscribe_with_request(Some(request)).await?;

    println!("stream opened");
    let mut counter = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {
                    Some(UpdateOneof::Account(account)) => {
                        let account: AccountPretty = account.into();
                        println!(
                            "new account update: filters {:?}, account: {:#?}",
                            msg.filters, account
                        );
                        let account_update = AccountUpdate {
                            time: Utc::now().into(),
                            pubkey: account.pubkey.to_string(),
                            lamports: account.lamports,
                            owner: account.owner.to_string(),
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                            data_len: account.data.len().to_string(),
                            write_version: account.write_version,
                        };
                        
                        let write_query = account_update.into_query("account_updates");
                        influxdb_client.query(write_query).await?;
                        continue;
                    }
                    _ => {}
                }
                println!("new message: {msg:?}")
            }
            Err(error) => {
                println!("error: {error:?}");
                break;
            }
        }

        // Example to illustrate how to resubscribe/update the subscription
        counter += 1;
    }
    println!("stream closed");
    Ok(())
}