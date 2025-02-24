mod proto; 
use proto::injective::exchange::v1beta1::query_client::QueryClient;
use proto::injective::exchange::v1beta1::{
    QueryFullDerivativeOrderbookRequest, QueryFullDerivativeOrderbookResponse,
};
use tonic::{metadata::MetadataValue, transport::Channel, Request};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel: Channel = Channel::from_static("127.0.0.1:9900").connect().await?;
    let mut client = QueryClient::new(channel);
    loop {
        let market_id =
            "0x142d0fa4506b5f404bcfdd54567797ff6767dce07afaedc90d379665f09f0520".to_string();
        let response = send_request(&mut client, market_id.clone()).await?;
        println!("{:?}", response);
    }
}