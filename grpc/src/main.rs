use futures::StreamExt;
use log::{error, info};
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::signal::ctrl_c;
use tokio::task;

mod config;
mod models;
mod producer;
mod proto;
mod query_client;
mod query_profiler;

use config::Config;
use models::{build_stream_request, StreamRequest, StreamResponse};
use producer::BatchKafkaProducer;
use proto::injective::stream::v1beta1::stream_client::StreamClient;

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

    // Create Kafka producer for streaming service
    let producer = Arc::new(BatchKafkaProducer::new(&config.kafka)?);
    info!("Connected to Kafka: {}", config.kafka.brokers.join(","));

    // Initialize with current block height
    initialize_with_current_block(&producer, &config.grpc).await?;

    // Clone config and producer for heartbeat task
    let heartbeat_config = config.clone();
    let heartbeat_producer = Arc::clone(&producer);

    // Start the heartbeat service in a separate task
    let heartbeat_handle = task::spawn(async move {
        match query_profiler::create_profiled_exchange_heartbeat(
            &heartbeat_config.grpc,
            heartbeat_producer,
            200,
        )
        .await
        {
            Ok(mut heartbeat) => {
                info!("Starting profiled heartbeat service");
                if let Err(e) = heartbeat.start().await {
                    error!("Heartbeat service error: {}", e);
                }
            }
            Err(e) => {
                error!("Failed to create profiled heartbeat service: {}", e);
            }
        }
    });

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

async fn initialize_with_current_block(
    producer: &Arc<BatchKafkaProducer>,
    config: &config::GrpcConfig,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    // Create a temporary exchange client to get current block height
    let exchange_client = query_client::ExchangeQueryClient::connect(config).await?;

    // Get the current block height from the chain
    match exchange_client.get_current_block_height().await {
        Ok(height) => {
            info!("Initializing with current block height: {}", height);
            producer.update_latest_block(height);
            Ok(())
        }
        Err(e) => {
            error!("Failed to get initial block height: {}", e);
            // Continue anyway, the system will self-correct
            Ok(())
        }
    }
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

    request.spot_orders_filter = None;

    request.derivative_orders_filter = None;

    request.subaccount_deposits_filter = None;
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
    producer: Arc<BatchKafkaProducer>,
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
                        // Check if this is a new block before processing
                        let block_height = response.block_height;
                        let current_block = producer.get_latest_block();

                        if block_height >= current_block {
                            // Only process if it's current or new
                            process_stream_response(response, &producer).await?;
                        } else {
                            // Log that we're skipping old data
                            info!("Skipping outdated block data: {} (current: {})",
                                  block_height, current_block);
                        }
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
    let messages = Vec::<models::KafkaMessage>::from(response);

    if !messages.is_empty() {
        let max_block_height = messages
            .iter()
            .map(|msg| msg.block_height)
            .max()
            .unwrap_or(0);
        let latest_processed = producer.get_latest_block();

        let results = producer.send_batch_current_only(messages).await;

        // Log errors if any
        for (i, result) in results.iter().enumerate() {
            if let Err(e) = result {
                error!("Failed to send message {}: {}", i, e);
            }
        }

        let success_count = results.iter().filter(|r| r.is_ok()).count();

        // Enhanced logging to show block height information
        if success_count > 0 {
            info!(
                "Block height: {} (was {}). Sent {}/{} messages to Kafka",
                max_block_height,
                latest_processed,
                success_count,
                results.len()
            );
        } else if max_block_height < latest_processed {
            // Log when we're skipping old blocks
            info!(
                "Skipped {} messages from old block height {} (current: {})",
                results.len(),
                max_block_height,
                latest_processed
            );
        }
    }

    Ok(())
}
