//use chrono::Utc;
//use proto::injective::oracle::v1beta1::ProviderInfo;
//use std::sync::Arc;
//use tokio::sync::mpsc;
use tonic::Request;//, Response, Status};
use tokio_stream::StreamExt;
//use uuid::Uuid;
//use crate::producer::MessageProducer;
// Here we get the data and put it in a message queue
// Why do i need mpsc - idk yet
mod proto;
use crate::proto::injective::stream::v1beta1::{
    stream_client::StreamClient, BankBalancesFilter, OraclePriceFilter, OrderbookFilter,
    OrdersFilter, PositionsFilter, StreamRequest, SubaccountDepositsFilter,
    TradesFilter,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StreamClient::connect("http://10.100.40.106:1900").await?;
    //stream entire state
    let req = Request::new(StreamRequest {
        bank_balances_filter: Some(BankBalancesFilter {
            accounts: vec!["*".to_string()],
        }),
        subaccount_deposits_filter: Some(SubaccountDepositsFilter {
            subaccount_ids: vec!["*".to_string()],
        }),
        spot_orders_filter: Some(OrdersFilter {
            subaccount_ids: vec!["*".to_string()],
            market_ids: vec!["*".to_string()],
        }),
        derivative_orders_filter: Some(OrdersFilter {
            subaccount_ids: vec!["*".to_string()],
            market_ids: vec!["*".to_string()],
        }),
        positions_filter: Some(PositionsFilter {
            subaccount_ids: vec!["*".to_string()],
            market_ids: vec!["*".to_string()],
        }),
        oracle_price_filter: Some(OraclePriceFilter {
            symbol: vec!["*".to_string()],
        }),
        derivative_orderbooks_filter: Some(OrderbookFilter {
            market_ids: vec!["*".to_string()],
        }),
        spot_orderbooks_filter: Some(OrderbookFilter {
            market_ids: vec!["*".to_string()],
        }),
        spot_trades_filter: Some(TradesFilter {
            subaccount_ids: vec!["*".to_string()],
            market_ids: vec!["*".to_string()],
        }),
        derivative_trades_filter: Some(TradesFilter {
            subaccount_ids: vec!["*".to_string()],
            market_ids: vec!["*".to_string()],
        }),
    });
    let mut stream = client.stream(req).await?.into_inner();
    while  let Some(reply) = stream.next().await {
        match reply {
            Ok(message) => println!("received stream message : {:?}", message),
            Err(e) => {
                println!(
                    "error in msg"
                );
                break;
            }
        }
    }
    Ok(())
}
