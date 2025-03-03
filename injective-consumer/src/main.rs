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
mod compute;

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
    
    // Initialize Redis processor
    info!("Connecting to Redis at {}", redis_url);
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
    info!("Connecting to ScyllaDB at {}", scylladb_nodes.join(","));
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
    info!("Creating Redis Kafka consumer");
    let redis_consumer = match KafkaConsumer::new(&config.kafka, redis_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create Redis consumer: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Create ScyllaDB consumer
    info!("Creating ScyllaDB Kafka consumer");
    let scylladb_consumer = match KafkaConsumer::new(&config.kafka, scylladb_processor) {
        Ok(consumer) => consumer,
        Err(e) => {
            error!("Failed to create ScyllaDB consumer: {}", e);
            return Err(Box::new(e));
        }
    };
    
    // Start consumers in separate tasks
    info!("Starting consumers");
    let redis_handle = task::spawn(async move {
        if let Err(e) = redis_consumer.start().await {
            error!("Redis consumer error: {}", e);
        }
    });
    
    let scylladb_handle = task::spawn(async move {
        if let Err(e) = scylladb_consumer.start().await {
            error!("ScyllaDB consumer error: {}", e);
        }
    });
    
    // Set up signal handler for graceful shutdown
    let shutdown_handle = task::spawn(async move {
        match ctrl_c().await {
            Ok(()) => {
                info!("Received shutdown signal, stopping consumers...");
            }
            Err(e) => {
                error!("Error waiting for shutdown signal: {}", e);
            }
        }
    });
    
    // Wait for all tasks to complete (or be cancelled)
    let _ = join_all(vec![redis_handle, scylladb_handle, shutdown_handle]).await;
    
    info!("Application shutting down");
    Ok(())
}