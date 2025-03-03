use crate::models::KafkaMessage;
use crate::config::KafkaConfig;
use async_trait::async_trait;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use log::{error, info};
use std::error::Error;
use std::time::Duration;

#[async_trait]
pub trait MessageProcessor: Send + Sync {
    async fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct KafkaConsumer<P: MessageProcessor> {
    consumer: StreamConsumer,
    processor: P,
}

impl<P: MessageProcessor> KafkaConsumer<P> {
    pub fn new(kafka_config: &KafkaConfig, processor: P) -> Result<Self, rdkafka::error::KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", &kafka_config.consumer_group)
            .set("bootstrap.servers", &kafka_config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .create()?;
            
        consumer.subscribe(&[&kafka_config.topic])?;
        
        Ok(KafkaConsumer {
            consumer,
            processor,
        })
    }
    
    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Starting Kafka consumer for topic: {}", self.get_subscribed_topics().join(", "));
        
        loop {
            match self.consumer.recv().await {
                Ok(message) => {
                    match message.payload() {
                        Some(payload) => {
                            match serde_json::from_slice::<KafkaMessage>(payload) {
                                Ok(kafka_message) => {
                                    if let Err(e) = self.processor.process_message(kafka_message).await {
                                        error!("Error processing message: {}", e);
                                    }
                                },
                                Err(e) => {
                                    error!("Failed to deserialize message: {}", e);
                                }
                            }
                        },
                        None => {
                            error!("Received empty message");
                        }
                    }
                },
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
    
    fn get_subscribed_topics(&self) -> Vec<String> {
        self.consumer.subscription()
            .unwrap_or_default()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}