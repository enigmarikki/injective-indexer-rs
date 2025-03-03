// src/main.rs - Main entry point for injective-consumer-scylla

use injective_consumer::{Config, KafkaConsumer};
use log::{error, info};
use std::env;
use std::error::Error;
use std::sync::Arc;
use tokio::signal::ctrl_c;

mod processor;

use processor::ScyllaMessageProcessor;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    info!("Starting Injective ScyllaDB Persistent Storage Service");
    
    // Load configuration
    let config = match env::var("CONFIG_FILE") {
        Ok(path) => Config::from_file(&path)?,
        Err(_) => Config::from_env()?,
    };
    
    info!("Configuration loaded");
    
    // Get consumer group ID from environment variable
    let group_id = env::var("KAFKA_CONSUMER_GROUP").unwrap_or_else(|_| "injective-scylla-consumer-group".to_string());
    
    // Get ScyllaDB connection details
    let scylla_nodes = env::var("SCYLLA_NODES").unwrap_or_else(|_| "127.0.0.1".to_string());
    let scylla_keyspace = env::var("SCYLLA_KEYSPACE").unwrap_or_else(|_| "injective".to_string());
    
    // Parse ScyllaDB nodes
    let nodes: Vec<&str> = scylla_nodes.split(',').collect();
    
    // Create ScyllaDB message processor
    let processor = Arc::new(ScyllaMessageProcessor::new(nodes, &scylla_keyspace).await?);
    
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
    info!("Starting to consume messages and store in ScyllaDB...");
    consumer.start().await?;
    
    Ok(())
}