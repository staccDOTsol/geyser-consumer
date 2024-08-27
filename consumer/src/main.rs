use influxdb::{Client, InfluxDbWriteable, WriteQuery};
use yellowstone_grpc_client::{GeyserGrpcClient, GeyserGrpcClientError};
use yellowstone_grpc_proto::prelude::*;
use chrono::Utc;
use bs58;
use     solana_transaction_status::{EncodedTransactionWithStatusMeta, UiTransactionEncoding};
use     futures::{future::TryFutureExt, sink::SinkExt, stream::StreamExt};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::TransactionError};
use std::time::Duration;
use spl_token_bonding::state::TokenBondingV0;
use anchor_lang::AccountDeserialize;
use std::collections::HashMap;
use tonic::transport::channel::ClientTlsConfig;
use yellowstone_grpc_proto::prelude::{
    subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
    subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterMemcmp,
    SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdateAccount,
    SubscribeUpdateTransaction, SubscribeUpdateTransactionStatus,
};
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;

#[derive(InfluxDbWriteable)]
struct AccountUpdate {
    time: influxdb::Timestamp,
    base_mint: String,
    target_mint: String,
    general_authority: Option<String>,
    reserve_authority: Option<String>,
    curve_authority: Option<String>,
    base_storage: String,
    buy_base_royalties: String,
    buy_target_royalties: String,
    sell_base_royalties: String,
    sell_target_royalties: String,
    buy_base_royalty_percentage: u32,
    buy_target_royalty_percentage: u32,
    sell_base_royalty_percentage: u32,
    sell_target_royalty_percentage: u32,
    curve: String,
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
use     std::{env, fmt, fs::File, sync::Arc};

#[allow(dead_code)]
pub struct TransactionPretty {
    slot: u64,
    signature: Signature,
    is_vote: bool,
    tx: EncodedTransactionWithStatusMeta,
}

impl fmt::Debug for TransactionPretty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct TxWrap<'a>(&'a EncodedTransactionWithStatusMeta);
        impl<'a> fmt::Debug for TxWrap<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let serialized = serde_json::to_string(self.0).expect("failed to serialize");
                fmt::Display::fmt(&serialized, f)
            }
        }

        f.debug_struct("TransactionPretty")
            .field("slot", &self.slot)
            .field("signature", &self.signature)
            .field("is_vote", &self.is_vote)
            .field("tx", &TxWrap(&self.tx))
            .finish()
    }
}

impl From<SubscribeUpdateTransaction> for TransactionPretty {
    fn from(SubscribeUpdateTransaction { transaction, slot }: SubscribeUpdateTransaction) -> Self {
        let tx = transaction.expect("should be defined");
        Self {
            slot,
            signature: Signature::try_from(tx.signature.as_slice()).expect("valid signature"),
            is_vote: tx.is_vote,
            tx: yellowstone_grpc_proto::convert_from::create_tx_with_meta(tx)
                .expect("valid tx with meta")
                .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
                .expect("failed to encode"),
        }
    }
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
    let client = GeyserGrpcClient::build_from_shared(geyser_endpoint)?.x_token(Some(x_token)).expect("oops").connect().await.expect("oops");


    let influxdb_client = Client::new(influxdb_url, "mybucket");
    let mut transactions: TransactionsFilterMap = HashMap::new();
    transactions.insert(
        "client".to_string(),
        SubscribeRequestFilterTransactions {
            vote: Some(false),
            failed: Some(false),
            signature: None,
            account_include: vec![pubkey.clone()],
            account_exclude: vec![],
            account_required: vec![],
        },
    );

    let request = SubscribeRequest {
        transactions: transactions.into(),
        ..Default::default()
    };

    let resub = 100; 
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
    let pubkey = std::env::var("PUBKEY").expect("PUBKEY must be set");
    let rpc_client = solana_client::nonblocking::rpc_client::RpcClient::new_with_commitment(
        "https://rpc.shyft.to?api_key=1y872euEMghE5flT".to_string(),
        solana_sdk::commitment_config::CommitmentConfig::confirmed(),
    );



    println!("stream opened");
    let mut counter = 0;
    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                match msg.update_oneof {

                    Some(UpdateOneof::Transaction(tx)) => {
                        //TransactionPretty { slot: 286155800, signature: 4HJadortBepQmuVYy9K8cy9Nkc54N16Aw8dde1qZtPVKmZbVtVEvUkZo317BVmjkuK7LjkRLGhMXSijy4wJj7adA, is_vote: false, tx: {"transaction":["AaQcNDBUQoU2fiuRMjo1IhNledDJ4jNxUsvjJ/iYkvznATcdvBvlRXJJkJMhzNdtkgzUMUplFST0rggqxGrS5AUBAAgPeQpnNG4nXfqyXl2/8SVo8hHthq6S6SahHQVoV2dVting+lJNsnVDNuKSPUZiUEAiYSSCEnMaHs68KALFo7TQaw3yhz2JDSLYllB/qFBeatBYeINsfF3AFY9eavzRHqvwMvY08+F6m6HsmVLYH9SOYig3xCbEOgzC690+iz1TXFPX5ALtD5SV22NTSmIZ/ZP5kbXB2E0rt1naikiL9tD+snURdlaeVu34uYFQ7pzzNtKeSJT2fOb4LVhCdDTKg8VyGSyJMxeJt/Vw9lbZVjFWEmVJH/UkDhOrmy8PE9pCDkIDBkZv5SEXMv/srbpyw5vnvIzlu8X3EmssQ5s6QAAAAAa1UfcDTtRKNcazhMaHWRZOMuxlFxdcIfzVvrIXVz0Zqwlb6uww+j6BPqki5YOSJCE4HDgFg06ew4mcSQTCyRIG3fbh12Whk9nL4UbO63msHLSF7V9bN5E6jPWFfv8AqQan1RcYx3TJKFZjmGkdXraLXrijm0ttXHNVWyEAAAAAirB6YPhh9DJLoFp4P5yU630paUWcpZu9gNfsedPWipUQouIYHEdvFEDB2N5tZhKGfh10G3s7f/PghvEiKcpI0QAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAALaPhFSPVgF7InUd8gy+9M8JOPDotf9C3bgfYZLgrvXECBwAJA1DDAAAAAAAACBABCQIDBAAFBQoLAAwCDQYOGqFR6t35418UAYCWmAAAAAAAILorAAAAAAAA","base64"],"meta":{"err":null,"status":{"Ok":null},"fee":15000,"preBalances":[272278265,4454400,1461600,1461600,2039280,2039280,1244279970071,1,1141440,4370880,934087680,1169280,2366400,0,1],"postBalances":[262263265,4454400,1461600,1461600,2039280,2039280,1244289970071,1,1141440,4370880,934087680,1169280,2366400,0,1],"innerInstructions":[{"index":1,"instructions":[{"programIdIndex":10,"accounts":[3,5,1],"data":"6K8LvD9bV6dd","stackHeight":2},{"programIdIndex":14,"accounts":[0,6],"data":"3Bxs4NN8M2Yn4TLb","stackHeight":2},{"programIdIndex":10,"accounts":[2,4,13],"data":"6YF7VVXZihvw","stackHeight":2}]}],"logMessages":["Program ComputeBudget111111111111111111111111111111 invoke [1]","Program ComputeBudget111111111111111111111111111111 success","Program TBondmkCYxaPCKG4CHYfVTcwQ8on31xnJrPzk8F8WsS invoke [1]","Program log: Instruction: BuyNativeV0","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]","Program log: Instruction: MintTo","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4492 of 77202 compute units","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success","Program 11111111111111111111111111111111 invoke [2]","Program 11111111111111111111111111111111 success","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA invoke [2]","Program log: Instruction: MintTo","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA consumed 4538 of 64575 compute units","Program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA success","Program TBondmkCYxaPCKG4CHYfVTcwQ8on31xnJrPzk8F8WsS consumed 145107 of 199850 compute units","Program TBondmkCYxaPCKG4CHYfVTcwQ8on31xnJrPzk8F8WsS success"],"preTokenBalances":[{"accountIndex":4,"mint":"wSo1i13zTR3wrdwvf5AMkQ8Xwa5X2X3XyUzqs5bvkBy","uiTokenAmount":{"uiAmount":13.868257116,"decimals":9,"amount":"13868257116","uiAmountString":"13.868257116"},"owner":"G9DiB75qGBeVJf1DojhrycLSUhnqmKjG4EYnYLEACk8a","programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},{"accountIndex":5,"mint":"4Rw8dGapeo6dviezobiQzsbAidDhix1biibGEyaapWHQ","uiTokenAmount":{"uiAmount":21.567875496,"decimals":9,"amount":"21567875496","uiAmountString":"21.567875496"},"owner":"99VXriv7RXJSypeJDBQtGRsak1n5o2NBzbtMXhHW2RNG","programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}],"postTokenBalances":[{"accountIndex":4,"mint":"wSo1i13zTR3wrdwvf5AMkQ8Xwa5X2X3XyUzqs5bvkBy","uiTokenAmount":{"uiAmount":13.878257116,"decimals":9,"amount":"13878257116","uiAmountString":"13.878257116"},"owner":"G9DiB75qGBeVJf1DojhrycLSUhnqmKjG4EYnYLEACk8a","programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},{"accountIndex":5,"mint":"4Rw8dGapeo6dviezobiQzsbAidDhix1biibGEyaapWHQ","uiTokenAmount":{"uiAmount":21.570770138,"decimals":9,"amount":"21570770138","uiAmountString":"21.570770138"},"owner":"99VXriv7RXJSypeJDBQtGRsak1n5o2NBzbtMXhHW2RNG","programId":"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}],"rewards":[],"loadedAddresses":{"writable":[],"readonly":[]},"computeUnitsConsumed":145257},"version":"legacy"} }


            let decoded_transaction = tx.transaction.clone().unwrap().transaction.unwrap();
            if let Some(meta) = &tx.transaction.unwrap().meta {
                for (i, account_key) in decoded_transaction.message.unwrap().account_keys.iter().enumerate() {
                    let pubkey = Pubkey::new(account_key.as_slice());
    let account_info = rpc_client.get_account(&pubkey).await?;
   
    if let Ok(token_bonding) = TokenBondingV0::try_deserialize(&mut account_info.data.as_slice()) {
                        
                        println!("Account Info: {:?}", account_info);
                        let account_update = AccountUpdate {
                            time: Utc::now().into(),
                            base_mint: token_bonding.base_mint.to_string(),
                            target_mint: token_bonding.target_mint.to_string(),
                            general_authority: token_bonding.general_authority.map(|auth| auth.to_string()),
                            reserve_authority: token_bonding.reserve_authority.map(|auth| auth.to_string()),
                            curve_authority: token_bonding.curve_authority.map(|auth| auth.to_string()),
                            base_storage: token_bonding.base_storage.to_string(),
                            buy_base_royalties: token_bonding.buy_base_royalties.to_string(),
                            buy_target_royalties: token_bonding.buy_target_royalties.to_string(),
                            sell_base_royalties: token_bonding.sell_base_royalties.to_string(),
                            sell_target_royalties: token_bonding.sell_target_royalties.to_string(),
                            buy_base_royalty_percentage: token_bonding.buy_base_royalty_percentage,
                            buy_target_royalty_percentage: token_bonding.buy_target_royalty_percentage,
                            sell_base_royalty_percentage: token_bonding.sell_base_royalty_percentage,
                            sell_target_royalty_percentage: token_bonding.sell_target_royalty_percentage,
                            curve: token_bonding.curve.to_string(),
                            mint_cap: token_bonding.mint_cap,
                            purchase_cap: token_bonding.purchase_cap,
                            go_live_unix_time: token_bonding.go_live_unix_time,
                            freeze_buy_unix_time: token_bonding.freeze_buy_unix_time,
                            created_at_unix_time: token_bonding.created_at_unix_time,
                            buy_frozen: token_bonding.buy_frozen,
                            sell_frozen: token_bonding.sell_frozen,
                            index: token_bonding.index,
                            bump_seed: token_bonding.bump_seed,
                            base_storage_bump_seed: token_bonding.base_storage_bump_seed,
                            target_mint_authority_bump_seed: token_bonding.target_mint_authority_bump_seed,
                            base_storage_authority_bump_seed: token_bonding.base_storage_authority_bump_seed,
                            reserve_balance_from_bonding: token_bonding.reserve_balance_from_bonding,
                            supply_from_bonding: token_bonding.supply_from_bonding,
                            ignore_external_reserve_changes: token_bonding.ignore_external_reserve_changes,
                            ignore_external_supply_changes: token_bonding.ignore_external_supply_changes,
                        };

                        let write_query = account_update.into_query("account_updates");
                        println!("Executing query: {:?}", write_query);
                        influxdb_client.query(write_query).await?;
                }
            }
    }
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