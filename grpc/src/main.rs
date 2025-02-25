use std::net::SocketAddr;
use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::{info, error};
use tracing_subscriber::fmt::format::FmtSpan;
mod proto;
mod config;
mod server;
mod producer;
mod models;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();

    // Load configuration
    let config = config::load().expect("Failed to load configuration");
    
    // Initialize message queue producer
    let producer = match config.queue_type.as_str() {
        "kafka" => {
            let kafka_config = config::KafkaConfig::from_env()?;
            Arc::new(producer::KafkaProducer::new(kafka_config).await?)
        },
        "rabbitmq" => {
            let rabbitmq_config = config::RabbitMqConfig::from_env()?;
            Arc::new(producer::RabbitMqProducer::new(rabbitmq_config).await?)
        },
        "redis" => {
            let redis_config = config::RedisConfig::from_env()?;
            Arc::new(producer::RedisProducer::new(redis_config).await?)
        },
        _ => panic!("Unsupported queue type: {}", config.queue_type),
    };
    
    // Create gRPC service
    let grpc_service = server::GrpcServer::new(producer);
    
    // Start server
    let addr = format!("{}:{}", config.host, config.port).parse::<SocketAddr>()?;
    info!("Starting gRPC server on {}", addr);
    
    let server = Server::builder()
        .add_service(grpc_service.create_service())
        .serve(addr);
    
    // Handle shutdown gracefully
    tokio::select! {
        result = server => {
            if let Err(e) = result {
                error!("Server error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            info!("Received shutdown signal");
        }
    }
    
    info!("Server shutting down");
    
    Ok(())
}
