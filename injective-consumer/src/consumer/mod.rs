
use crate::config::KafkaConfig;
use crate::models::KafkaMessage;
use log::{error, info};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message};
use rdkafka::topic_partition_list::TopicPartitionList;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// Custom consumer context that logs rebalance events
pub struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        info!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        info!("Post-rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!("Commit successful"),
            Err(e) => error!("Commit error: {}", e),
        }
    }
}

type LoggingConsumer = StreamConsumer<CustomContext>;

/// Trait for processing messages
pub trait MessageProcessor: Send + Sync {
    fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>>;
}

/// Kafka consumer that reads messages and passes them to a MessageProcessor
pub struct KafkaConsumer {
    consumer: LoggingConsumer,
    message_processor: Arc<dyn MessageProcessor + Send + Sync>,
}

impl KafkaConsumer {
    pub fn new(
        config: &KafkaConfig,
        group_id: &str,
        message_processor: Arc<dyn MessageProcessor + Send + Sync>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let context = CustomContext;

        let consumer: LoggingConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", &config.brokers.join(","))
            .set("enable.auto.commit", "true")
            .set("auto.offset.reset", "earliest") // Start from the beginning if no offset
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000") // 5 minutes
            .set_log_level(RDKafkaLogLevel::Debug)
            .create_with_context(context)?;

        Ok(KafkaConsumer {
            consumer,
            message_processor,
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Subscribe to the topic
        self.consumer.subscribe(&[&self.consumer.client().metadata(
            Some(&self.consumer.subscription()[0]),
            Duration::from_secs(10),
        )?.topics().iter().next().unwrap().name()])?;

        info!("Consumer started, waiting for messages...");

        // Create a channel to handle messages concurrently
        let (tx, mut rx) = mpsc::channel::<rdkafka::message::BorrowedMessage<'_>>(100);
        
        // Spawn a task to process messages
        let processor = self.message_processor.clone();
        let processor_handle = tokio::spawn(async move {
            while let Some(msg) = rx.recv().await {
                if let Some(payload) = msg.payload() {
                    match String::from_utf8(payload.to_vec()) {
                        Ok(json_str) => {
                            match serde_json::from_str::<KafkaMessage>(&json_str) {
                                Ok(kafka_message) => {
                                    if let Err(e) = processor.process_message(kafka_message) {
                                        error!("Error processing message: {}", e);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to parse Kafka message: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            error!("Message payload is not valid UTF-8: {}", e);
                        }
                    }
                }
            }
        });

        // Main consumer loop
        loop {
            match self.consumer.recv().await {
                Ok(msg) => {
                    if let Err(e) = tx.send(msg).await {
                        error!("Failed to send message to processor: {}", e);
                    }
                    
                    // Optionally commit the message
                    self.consumer.commit_message(&msg, CommitMode::Async).unwrap_or_else(|e| {
                        error!("Failed to commit message: {}", e);
                    });
                }
                Err(e) => {
                    error!("Kafka error: {}", e);
                }
            }
        }

        // This will never be reached in the current implementation,
        // but we'll keep it for completeness
        let _ = processor_handle.await;
        Ok(())
    }
}