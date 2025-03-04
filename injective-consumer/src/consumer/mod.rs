use crate::config::KafkaConfig;
use crate::models::KafkaMessage;
use async_trait::async_trait;
use log::{error, info};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message,
};
use std::error::Error;
use std::time::Duration;

#[async_trait]
pub trait MessageProcessor: Send + Sync {
    async fn process_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

pub struct KafkaConsumer<P: MessageProcessor> {
    consumer: StreamConsumer,
    processor: P,
}

impl<P: MessageProcessor> KafkaConsumer<P> {
    pub fn new(
        kafka_config: &KafkaConfig,
        processor: P,
    ) -> Result<Self, rdkafka::error::KafkaError> {
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
        info!(
            "Starting Kafka consumer for topic: {}",
            self.get_subscribed_topics().join(", ")
        );

        loop {
            match self.consumer.recv().await {
                Ok(message) => match message.payload() {
                    Some(payload) => match serde_json::from_slice::<KafkaMessage>(payload) {
                        Ok(kafka_message) => {
                            if let Err(e) = self.processor.process_message(kafka_message).await {
                                error!("Error processing message: {}", e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to deserialize message: {}", e);
                        }
                    },
                    None => {
                        error!("Received empty message");
                    }
                },
                Err(e) => {
                    error!("Error receiving message: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }

    // Add a new method that supports shutdown
    pub async fn start_with_shutdown(
        &self,
        mut shutdown_signal: tokio::sync::oneshot::Receiver<()>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "Starting Kafka consumer for topic: {}",
            self.get_subscribed_topics().join(", ")
        );

        loop {
            tokio::select! {
                _ = &mut shutdown_signal => {
                    info!("Received shutdown signal, stopping consumer");
                    break;
                }
                message_result = self.consumer.recv() => {
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
        }

        info!("Kafka consumer stopped");
        Ok(())
    }

    fn get_subscribed_topics(&self) -> Vec<String> {
        match self.consumer.subscription() {
            Ok(subscription) => subscription
                .elements()
                .iter()
                .map(|elem| elem.topic().to_string())
                .collect(),
            Err(e) => {
                error!("Failed to get subscription: {}", e);
                Vec::new()
            }
        }
    }
}
