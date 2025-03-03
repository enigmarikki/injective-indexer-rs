use crate::config::KafkaConfig;
use crate::models::KafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use futures::stream::StreamExt;
use std::error::Error;
use std::time::Duration;
use log::{info, error};

pub trait MessageProcessor {
    async fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct KafkaConsumer<T: MessageProcessor> {
    consumer: StreamConsumer,
    processor: T,
}

impl<T: MessageProcessor> KafkaConsumer<T> {
    pub fn new(config: &KafkaConfig, processor: T) -> Result<Self, rdkafka::error::KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers.join(","))
            .set("group.id", "injective_data_processor")
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .create()?;

        consumer.subscribe(&[&config.topic])?;

        Ok(KafkaConsumer {
            consumer,
            processor,
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Starting Kafka consumer for topic: {}", self.consumer.subscription().unwrap_or_default().join(", "));
        
        let mut message_stream = self.consumer.stream();
        
        while let Some(message_result) = message_stream.next().await {
            match message_result {
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
                                    error!("Error deserializing message: {}", e);
                                }
                            }
                        },
                        None => {
                            error!("Received message with empty payload");
                        }
                    }
                },
                Err(e) => {
                    error!("Error receiving message: {}", e);
                }
            }
        }

        Ok(())
    }
}