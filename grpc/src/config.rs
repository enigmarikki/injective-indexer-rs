use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub queue_type: String,
}

#[derive(Debug, Deserialize)]
pub struct KafkaConfig {
    pub bootstrap_servers: String,
    pub topic: String,
}

#[derive(Debug, Deserialize)]
pub struct RabbitMqConfig {
    pub uri: String,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Debug, Deserialize)]
pub struct RedisConfig {
    pub uri: String,
    pub channel: String,
}

pub fn load() -> Result<Config, config::ConfigError> {
    let config = config::Config::builder()
        .add_source(config::Environment::default())
        .build()?;

    config.try_deserialize()
}

impl KafkaConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            bootstrap_servers: env::var("KAFKA_BOOTSTRAP_SERVERS")?,
            topic: env::var("KAFKA_TOPIC")?,
        })
    }
}

impl RabbitMqConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            uri: env::var("RABBITMQ_URI")?,
            exchange: env::var("RABBITMQ_EXCHANGE")?,
            routing_key: env::var("RABBITMQ_ROUTING_KEY")?,
        })
    }
}

impl RedisConfig {
    pub fn from_env() -> Result<Self, env::VarError> {
        Ok(Self {
            uri: env::var("REDIS_URI")?,
            channel: env::var("REDIS_CHANNEL")?,
        })
    }
}
