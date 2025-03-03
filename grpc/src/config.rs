use serde::{Deserialize, Serialize};
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub grpc: GrpcConfig,
    pub kafka: KafkaConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    pub stream_endpoint: String,
    pub query_endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub client_id: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            grpc: GrpcConfig {
                stream_endpoint: "http://localhost:1999".to_string(),
                query_endpoint: "http://localhost:9900".to_string(),
            },
            kafka: KafkaConfig {
                brokers: vec!["localhost:9092".to_string()],
                topic: "injective-data".to_string(),
                client_id: "injective-client".to_string(),
            },
        }
    }
}

impl Config {
    pub fn from_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let config: Config = serde_json::from_str(&contents)?;
        Ok(config)
    }

    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut config = Config::default();

        if let Ok(stream_endpoint) = env::var("GRPC_STREAM_ENDPOINT") {
            config.grpc.stream_endpoint = stream_endpoint;
        }

        if let Ok(query_endpoint) = env::var("GRPC_QUERY_ENDPOINT") {
            config.grpc.query_endpoint = query_endpoint;
        }

        if let Ok(brokers) = env::var("KAFKA_BROKERS") {
            config.kafka.brokers = brokers.split(',').map(|s| s.to_string()).collect();
        }

        if let Ok(topic) = env::var("KAFKA_TOPIC") {
            config.kafka.topic = topic;
        }

        if let Ok(client_id) = env::var("KAFKA_CLIENT_ID") {
            config.kafka.client_id = client_id;
        }

        Ok(config)
    }
}
