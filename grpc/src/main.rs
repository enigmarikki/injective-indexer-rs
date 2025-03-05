use futures::StreamExt;
use log::{error, info};
use std::env;
use std::error::Error;
use tokio::signal::ctrl_c;
use tokio::task;

mod config;
mod models;
mod producer;
mod proto;
mod query_client;

use config::Config;
use models::{build_stream_request, StreamRequest, StreamResponse};
use producer::BatchKafkaProducer;
use proto::injective::stream::v1beta1::stream_client::StreamClient;
use query_client::ExchangeHeartbeat;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting Injective data streaming service");

    // Load configuration
    let config = match env::var("CONFIG_FILE") {
        Ok(path) => Config::from_file(&path)?,
        Err(_) => Config::from_env()?,
    };

    info!("Configuration loaded");

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    // Clone config for heartbeat task
    let heartbeat_config = config.clone();

    // Start the heartbeat service in a separate task
    let heartbeat_handle = task::spawn(async move {
        match ExchangeHeartbeat::new(&heartbeat_config.grpc, &heartbeat_config.kafka, 30).await {
            Ok(mut heartbeat) => {
                info!("Starting heartbeat service");
                if let Err(e) = heartbeat.start().await {
                    error!("Heartbeat service error: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to create heartbeat service: {}", e);
            }
        }
    });

    // Create Kafka producer for streaming service
    let producer = BatchKafkaProducer::new(&config.kafka)?;
    info!("Connected to Kafka: {}", config.kafka.brokers.join(","));

    // Create the streaming client
    let stream_client = connect_to_stream_service(&config.grpc.stream_endpoint).await?;
    info!(
        "Connected to stream service: {}",
        config.grpc.stream_endpoint
    );

    // Create a stream request
    let request = create_stream_request();
    info!("Stream request created");

    // Handle Ctrl+C signal for graceful shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            error!("Error setting up Ctrl+C handler: {}", e);
        }
        info!("Received Ctrl+C, initiating shutdown");
        let _ = shutdown_tx_clone.send(()).await;
    });

    // Start streaming data
    let stream_handle = task::spawn(async move {
        stream_and_process(stream_client, request, producer, shutdown_rx).await
    });

    // Wait for both tasks to complete
    let _ = tokio::try_join!(
        async {
            let _ = heartbeat_handle.await;
            Ok::<(), Box<dyn Error + Send + Sync>>(())
        },
        async {
            let result = stream_handle.await;
            match result {
                Ok(inner_result) => inner_result,
                Err(e) => Err(format!("Stream task panicked: {}", e).into()),
            }
        }
    )?;

    info!("Application shutting down");
    Ok(())
}

async fn connect_to_stream_service(
    endpoint: &str,
) -> Result<StreamClient<tonic::transport::Channel>, Box<dyn Error + Send + Sync>> {
    let client = StreamClient::connect(endpoint.to_string()).await?;
    Ok(client)
}

fn create_stream_request() -> StreamRequest {
    let mut request = build_stream_request();
    let wild_card_match = vec!["*".to_string()];

    // Configure what data to receive
    request.bank_balances_filter = None;

    request.spot_trades_filter = Some(models::TradesFilter {
        market_ids: wild_card_match.clone(), // Wildcard to match all markets
        subaccount_ids: wild_card_match.clone(), // Wildcard to match all subaccounts
    });

    request.derivative_trades_filter = Some(models::TradesFilter {
        market_ids: wild_card_match.clone(), // Wildcard to match all markets
        subaccount_ids: wild_card_match.clone(), // Wildcard to match all subaccounts
    });

    request.spot_orderbooks_filter = Some(models::OrderbookFilter {
        market_ids: wild_card_match.clone(),
    });

    // Adding the remaining filters
    request.derivative_orderbooks_filter = Some(models::OrderbookFilter {
        market_ids: wild_card_match.clone(),
    });

    request.spot_orders_filter = Some(models::OrdersFilter {
        market_ids: wild_card_match.clone(),
        subaccount_ids: wild_card_match.clone(),
    });

    request.derivative_orders_filter = Some(models::OrdersFilter {
        market_ids: wild_card_match.clone(),
        subaccount_ids: wild_card_match.clone(),
    });

    request.subaccount_deposits_filter = Some(models::SubaccountDepositsFilter {
        subaccount_ids: wild_card_match.clone(),
    });
    // We're polling positions anyway so no need to do it again here
    request.positions_filter = None;

    request.oracle_price_filter = Some(models::OraclePriceFilter {
        symbol: wild_card_match.clone(),
    });

    request
}

async fn stream_and_process(
    mut client: StreamClient<tonic::transport::Channel>,
    request: StreamRequest,
    producer: BatchKafkaProducer,
    mut shutdown_rx: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Start streaming
    let mut stream = client.stream(request).await?.into_inner();
    info!("Stream established, waiting for data...");

    // Process stream until we get a shutdown signal
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => {
                info!("Received shutdown signal, stopping stream...");
                break;
            }
            message = stream.next() => {
                match message {
                    Some(Ok(response)) => {
                        process_stream_response(response, &producer).await?;
                    }
                    Some(Err(e)) => {
                        error!("Error from stream: {}", e);
                        // Consider reconnection strategy here
                        break;
                    }
                    None => {
                        info!("Stream ended, exiting...");
                        break;
                    }
                }
            }
        }
    }

    info!("Stream processing ended");
    Ok(())
}

async fn process_stream_response(
    response: StreamResponse,
    producer: &BatchKafkaProducer,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Convert to our Kafka messages
    let messages = Vec::<models::KafkaMessage>::from(response);

    if !messages.is_empty() {
        // Send to Kafka in a batch
        let results = producer.send_batch(messages).await;

        // Log errors if any
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                error!("Failed to send message {}: {}", i, e);
            }
        }

        let success_count = results.iter().filter(|r: &&Result<(), rdkafka::error::KafkaError>| r.is_ok()).count();
        info!(
            "Successfully sent {}/{} messages to Kafka",
            success_count,
            results.len()
        );
    }

    Ok(())
}
