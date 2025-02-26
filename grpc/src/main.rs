use tokio_stream::StreamExt;
use tonic::Request;
mod proto;
mod stream;
use proto::injective::stream::v1beta1::stream_client::StreamClient;
use stream::{create_stream_request, StreamFilterCondition};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = StreamClient::connect("http://localhost:1999").await?;
    //stream entire state

    let req = Request::new(create_stream_request(StreamFilterCondition::DerivativeTrades)?);
    let mut stream = client.stream(req).await?.into_inner();
    while let Some(reply) = stream.next().await {
        match reply {
            Ok(message) => println!("received stream message : {:?}", message.derivative_trades),
            Err(e) => {
                println!("error in msg");
                break;
            }
        }
    }
    Ok(())
}
