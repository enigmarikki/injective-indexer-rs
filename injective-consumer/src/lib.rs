// This file exposes our library components for both internal use and external consumers

// Re-export the modules
pub mod compute;
pub mod config;
pub mod consumer;
pub mod models;
pub mod pubsub;
pub mod redis_consumer;
pub mod scylladb_consumer;
// Re-export the key components for easier use
pub use config::Config;
pub use consumer::{KafkaConsumer, MessageProcessor};
pub use redis_consumer::RedisProcessor;
pub use scylladb_consumer::ScyllaDBProcessor;
