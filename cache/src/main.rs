// src/main.rs - Main entry point for injective-consumer-redis

use injective_consumer::{Config, KafkaConsumer};
use log::{error, info};
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::signal::ctrl_c;

mod processor;

use processor::RedisMessageProcessor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    info!("Starting Injective Redis Cache Consumer Service");
    
    // Load configuration
    let config = match env::var("CONFIG_FILE") {
        Ok(path) => Config::from_file(&path)?,
        Err(_) => Config::from_env()?,
    };
    
    info!("Configuration loaded");
    
    // Get consumer group ID and Redis URL from environment variables
    let group_id = env::var("KAFKA_CONSUMER_GROUP").unwrap_or_else(|_| "injective-redis-consumer-group".to_string());
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    
    // Get TTL in seconds for Redis keys (optional)
    let ttl_seconds = env::var("REDIS_TTL_SECONDS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok());
    
    if let Some(ttl) = ttl_seconds {
        info!("Redis data TTL set to {} seconds", ttl);
    } else {
        info!("Redis data will not expire (no TTL set)");
    }
    
    // Create Redis message processor
    let processor = Arc::new(RedisMessageProcessor::new(&redis_url, ttl_seconds)?);
    
    // Create Kafka consumer
    let consumer = KafkaConsumer::new(&config.kafka, &group_id, processor)?;
    info!("Connected to Kafka: {}", config.kafka.brokers.join(","));
    
    // Handle Ctrl+C signal for graceful shutdown
    tokio::spawn(async move {
        if let Err(e) = ctrl_c().await {
            error!("Error setting up Ctrl+C handler: {}", e);
        }
        info!("Received Ctrl+C, initiating shutdown");
        std::process::exit(0);
    });
    
    // Start consuming messages
    info!("Starting to consume messages and store in Redis...");
    consumer.start().await?;
    
    Ok(())
}