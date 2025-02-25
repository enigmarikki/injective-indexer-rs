use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use lapin::{
    Connection, ConnectionProperties, Channel,
    options::BasicPublishOptions,
    BasicProperties,
};
use redis::{Client, AsyncCommands};
use tokio::time::Duration;
use std::error::Error;
use tracing::{info, error};
use crate::models::Event;
use crate::config::{KafkaConfig, RabbitMqConfig, RedisConfig};

#[async_trait]
pub trait MessageProducer: Send + Sync {
    async fn publish(&self, event: &Event) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl KafkaProducer {
    pub async fn new(config: KafkaConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self {
            producer,
            topic: config.topic,
        })
    }
}

#[async_trait]
impl MessageProducer for KafkaProducer {
    async fn publish(&self, event: &Event) -> Result<(), Box<dyn Error + Send + Sync>> {
        let payload = serde_json::to_vec(event)?;
        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(&event.id);

        self.producer.send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| Box::new(e) as Box<dyn Error + Send + Sync>)?;

        info!("Published event {} to Kafka topic {}", event.id, self.topic);
        Ok(())
    }
}

pub struct RabbitMqProducer {
    channel: Channel,
    exchange: String,
    routing_key: String,
}

impl RabbitMqProducer {
    pub async fn new(config: RabbitMqConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let conn = Connection::connect(
            &config.uri,
            ConnectionProperties::default().with_tokio(),
        ).await?;
        
        let channel = conn.create_channel().await?;
        
        Ok(Self {
            channel,
            exchange: config.exchange,
            routing_key: config.routing_key,
        })
    }
}

#[async_trait]
impl MessageProducer for RabbitMqProducer {
    async fn publish(&self, event: &Event) -> Result<(), Box<dyn Error + Send + Sync>> {
        let payload = serde_json::to_vec(event)?;
        
        self.channel.basic_publish(
            &self.exchange,
            &self.routing_key,
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default()
                .with_message_id(event.id.clone().into())
                .with_content_type("application/json".into()),
        ).await?;
        
        info!("Published event {} to RabbitMQ exchange {}", event.id, self.exchange);
        Ok(())
    }
}

pub struct RedisProducer {
    client: Client,
    channel: String,
}

impl RedisProducer {
    pub async fn new(config: RedisConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(config.uri)?;
        
        Ok(Self {
            client,
            channel: config.channel,
        })
    }
}

#[async_trait]
impl MessageProducer for RedisProducer {
    async fn publish(&self, event: &Event) -> Result<(), Box<dyn Error + Send + Sync>> {
        let payload = serde_json::to_string(event)?;
        let mut conn = self.client.get_async_connection().await?;
        
        let _: i32 = conn.publish(&self.channel, payload).await?;
        
        info!("Published event {} to Redis channel {}", event.id, self.channel);
        Ok(())
    }
}
