// src/lib.rs - Main library file for injective-consumer-base

pub mod config;
pub mod consumer;
pub mod models;
pub mod proto;

// Re-export key components
pub use config::Config;
pub use config::KafkaConfig;
pub use consumer::KafkaConsumer;
pub use consumer::MessageProcessor;