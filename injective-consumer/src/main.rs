use futures::future::join_all;
use log::{error, info};
use std::env;
use std::error::Error;
use tokio::signal::ctrl_c;
use tokio::task;

mod config;
mod models;
mod consumer;
mod redis_consumer;
mod scylladb_consumer;

use config::Config;
use consumer::KafkaConsumer;
use redis_consumer::RedisProcessor;
use scylladb_consumer::ScyllaDBProcessor;

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
    
    // Create shutdown channel
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);
    
    // Clone config for consumers
    let redis_config = config.clone();
    let scylladb_config = config.clone();
    
    // Initialize Redis processor
    let redis_processor = match RedisProcessor::new(&redis_url) {
        Ok(processor) => {
            info!("Connected to Redis: {}", redis_url);
            processor
        },
        Err(e) => {
            error!("Failed to connect to Redis: {}", e);
            return Err(e);
        }
    };
    
    // Initialize ScyllaDB processor
    let scylladb_processor = match ScyllaDBProcessor::new(scylladb_nodes.clone()).await {
        Ok(processor) => {
            info!("Connected to ScyllaDB: {}", scylladb_nodes.join(","));
            processor
        },
        Err(e) => {
            error!("Failed to connect to ScyllaDB: {}", e);
            return Err(e);
        }
    };
    
    // Create Redis consumer
    let redis_consumer = match KafkaConsumer::new(&redis_config.kafka, redis_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create Redis consumer: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Create ScyllaDB consumer
    let scylladb_consumer = match KafkaConsumer::new(&scylladb_config.kafka, scylladb_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create ScyllaDB consumer: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Handle Ctrl+C signal for graceful shutdown
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            error!("Error setting up Ctrl+C handler: {}", e);
        }
        info!("Received Ctrl+C, initiating shutdown");
        let _ = shutdown_tx_clone.send(()).await;
    });
    
    // Start Redis consumer
    let redis_handle = task::spawn(async move {
        if let Err(e) = redis_consumer.start().await {
            error!("Redis consumer error: {}", e);
        }
    });
    
    // Start ScyllaDB consumer
    let scylladb_handle = task::spawn(async move {
        if let Err(e) = scylladb_consumer.start().await {
            error!("ScyllaDB consumer error: {}", e);
        }
    });
    
    // Wait for shutdown signal
    shutdown_rx.recv().await;
    info!("Shutting down consumers...");
    
    // Wait for consumers to finish (this is a simplification as we can't actually
    // cancel the consumer tasks cleanly without additional work)
    let _ = join_all(vec![redis_handle, scylladb_handle]).await;
    
    info!("Application shutting down");
    Ok(())
}