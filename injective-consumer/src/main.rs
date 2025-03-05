use futures::future::join_all;
use log::{error, info};
use std::env;
use std::error::Error;
use tokio::signal::ctrl_c;
use tokio::sync::oneshot;
use tokio::task;
use tokio::time::{sleep, Duration};

mod compute;
mod config;
mod consumer;
mod market_preloader;
mod models;
mod pubsub;
mod redis_consumer;
mod scylladb_consumer;

use config::Config;
use consumer::KafkaConsumer;
use market_preloader::MarketPreloader;
use pubsub::{RedisPubSubConfig, RedisPubSubService};
use redis_consumer::RedisProcessor;
use scylladb_consumer::ScyllaDBProcessor;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    info!("Starting Injective data processing service");

    // Load configuration
    let config = match env::var("CONFIG_FILE") {
        Ok(path) => Config::from_file(&path)?,
        Err(_) => Config::from_env()?,
    };

    // Get additional configuration from environment
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let scylladb_nodes = env::var("SCYLLADB_NODES")
        .unwrap_or_else(|_| "127.0.0.1:9042".to_string())
        .split(',')
        .map(|s| s.to_string())
        .collect::<Vec<String>>();

    info!("Configuration loaded");

    // Initialize Redis PubSub service
    info!("Initializing Redis PubSub service");
    let pubsub_config = RedisPubSubConfig {
        redis_url: redis_url.clone(),
        // Customize other options as needed
        ..RedisPubSubConfig::default()
    };

    let pubsub_service = match RedisPubSubService::new(pubsub_config).await {
        Ok(service) => {
            info!("Redis PubSub service initialized");
            Arc::new(service)
        }
        Err(e) => {
            error!("Failed to initialize Redis PubSub service: {}", e);
            return Err(e.into());
        }
    };

    // Initialize Redis processor with PubSub service
    info!("Connecting to Redis at {}", redis_url);
    let redis_processor = match RedisProcessor::new(&redis_url) {
        Ok(processor) => {
            info!("Connected to Redis: {}", redis_url);
            processor.with_pubsub(pubsub_service.clone()) // Add PubSub service here
        }
        Err(e) => {
            error!("Failed to connect to Redis: {}", e);
            return Err(e.into());
        }
    };

    // Initialize ScyllaDB processor
    info!("Connecting to ScyllaDB at {}", scylladb_nodes.join(","));
    let scylladb_processor = match ScyllaDBProcessor::new(scylladb_nodes.clone()).await {
        Ok(processor) => {
            info!("Connected to ScyllaDB: {}", scylladb_nodes.join(","));
            processor
        }
        Err(e) => {
            error!("Failed to connect to ScyllaDB: {}", e);
            return Err(e.into());
        }
    };

    // Create a dedicated market preloader
    let market_preloader = MarketPreloader::new(&redis_url, pubsub_service.clone()).await?;

    // Create separate Kafka configs for market preloader, Redis, and ScyllaDB consumers
    let mut market_kafka_config = config.kafka.clone();
    market_kafka_config.consumer_group = format!("{}-markets", config.kafka.consumer_group);

    let mut redis_kafka_config = config.kafka.clone();
    redis_kafka_config.consumer_group = format!("{}-redis", config.kafka.consumer_group);

    let mut scylladb_kafka_config = config.kafka.clone();
    scylladb_kafka_config.consumer_group = format!("{}-scylladb", config.kafka.consumer_group);

    // Create market preloader consumer with its own consumer group
    info!(
        "Creating Market Preloader Kafka consumer with group: {}",
        market_kafka_config.consumer_group
    );
    let market_consumer = match KafkaConsumer::new(&market_kafka_config, market_preloader) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create Market Preloader consumer: {}", e);
            return Err(e.into());
        }
    };

    // Create Redis consumer with its own consumer group
    info!(
        "Creating Redis Kafka consumer with group: {}",
        redis_kafka_config.consumer_group
    );
    let redis_consumer = match KafkaConsumer::new(&redis_kafka_config, redis_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create Redis consumer: {}", e);
            return Err(e.into());
        }
    };

    // Create ScyllaDB consumer with its own consumer group
    info!(
        "Creating ScyllaDB Kafka consumer with group: {}",
        scylladb_kafka_config.consumer_group
    );
    let scylladb_consumer = match KafkaConsumer::new(&scylladb_kafka_config, scylladb_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create ScyllaDB consumer: {}", e);
            return Err(e.into());
        }
    };

    // Create shutdown channels
    let (market_shutdown_tx, market_shutdown_rx) = oneshot::channel::<()>();
    let (redis_shutdown_tx, redis_shutdown_rx) = oneshot::channel::<()>();
    let (scylladb_shutdown_tx, scylladb_shutdown_rx) = oneshot::channel::<()>();

    // Start market preloader first
    info!("Starting market preloader");
    let market_handle = task::spawn(async move {
        if let Err(e) = market_consumer
            .start_with_shutdown(market_shutdown_rx)
            .await
        {
            error!("Market preloader error: {}", e);
        }
    });

    // Allow time for market preloader to process initial markets
    // This is a simple approach - ideally we'd want a signal from the preloader
    info!("Waiting for initial market data to be processed...");
    sleep(Duration::from_secs(5)).await;

    // Start other consumers in separate tasks with shutdown receivers
    info!("Starting Redis and ScyllaDB consumers");
    let redis_handle = task::spawn(async move {
        if let Err(e) = redis_consumer.start_with_shutdown(redis_shutdown_rx).await {
            error!("Redis consumer error: {}", e);
        }
    });

    let scylladb_handle = task::spawn(async move {
        if let Err(e) = scylladb_consumer
            .start_with_shutdown(scylladb_shutdown_rx)
            .await
        {
            error!("ScyllaDB consumer error: {}", e);
        }
    });

    // Set up signal handler for graceful shutdown
    let shutdown_handle = task::spawn(async move {
        match ctrl_c().await {
            Ok(()) => {
                info!("Received shutdown signal, stopping consumers...");
                // Send shutdown signal to all consumers
                if let Err(e) = market_shutdown_tx.send(()) {
                    error!(
                        "Failed to send shutdown signal to Market preloader: {:?}",
                        e
                    );
                }
                if let Err(e) = redis_shutdown_tx.send(()) {
                    error!("Failed to send shutdown signal to Redis consumer: {:?}", e);
                }
                if let Err(e) = scylladb_shutdown_tx.send(()) {
                    error!(
                        "Failed to send shutdown signal to ScyllaDB consumer: {:?}",
                        e
                    );
                }
            }
            Err(e) => {
                error!("Error waiting for shutdown signal: {}", e);
            }
        }
    });

    // Wait for all tasks to complete
    let _ = join_all(vec![
        market_handle,
        redis_handle,
        scylladb_handle,
        shutdown_handle,
    ])
    .await;

    info!("Application shutting down");
    Ok(())
}
