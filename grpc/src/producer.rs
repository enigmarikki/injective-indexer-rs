use crate::config::KafkaConfig;
use crate::models::KafkaMessage;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;

pub struct KafkaProducer {
    producer: FutureProducer,
    topic: String,
    timeout: Duration,
}

impl KafkaProducer {
    pub fn new(config: &KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers.join(","))
            .set("client.id", &config.client_id)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .create()?;

        Ok(KafkaProducer {
            producer,
            topic: config.topic.clone(),
            timeout: Duration::from_secs(5),
        })
    }

    pub async fn send_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_string(&message)?;
        let key = format!("{}-{}", message.block_height, message.block_time);

        let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);

        self.producer
            .send(record, Timeout::After(self.timeout))
            .await
            .map_err(|(err, _)| err)?;

        Ok(())
    }

    pub async fn send_messages(
        &self,
        messages: Vec<KafkaMessage>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for message in messages {
            self.send_message(message).await?;
        }
        Ok(())
    }
}

// Batch mode implementation for higher throughput
pub struct BatchKafkaProducer {
    producer: FutureProducer,
    topic: String,
}

impl BatchKafkaProducer {
    pub fn new(config: &KafkaConfig) -> Result<Self, rdkafka::error::KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.brokers.join(","))
            .set("client.id", &config.client_id)
            // Throughput Optimizations
            .set("compression.type", "snappy") // Efficient compression
            .set("batch.size", "1048576") // 1MB batch size (increased from default)
            .set("linger.ms", "5") // Wait up to 5ms to accumulate messages
            .set("max.in.flight.requests.per.connection", "5") // Allow multiple parallel requests
            .set("queue.buffering.max.messages", "100000") // Increase message queue
            .set("queue.buffering.max.kbytes", "10240") // 10MB buffer
            .set("message.send.max.retries", "3") // Retry failed sends
            .set("retry.backoff.ms", "100") // Backoff between retries
            .set("acks", "1") // Balanced between performance and reliability
            .create()?;

        Ok(BatchKafkaProducer {
            producer,
            topic: config.topic.clone(),
        })
    }

    pub async fn send_batch(
        &self,
        messages: Vec<KafkaMessage>,
    ) -> Vec<Result<(), rdkafka::error::KafkaError>> {
        // Convert all messages to payload strings up front, keeping them alive for the whole function
        let message_payloads: Vec<(String, String)> = messages
            .into_iter()
            .filter_map(|message| {
                let key = format!("{}-{}", message.block_height, message.block_time);
                match serde_json::to_string(&message) {
                    Ok(payload) => Some((key, payload)),
                    Err(e) => {
                        log::error!("Failed to serialize message: {}", e);
                        None
                    }
                }
            })
            .collect();

        // Create futures for each message
        let mut futures = Vec::with_capacity(message_payloads.len());

        for (key, payload) in &message_payloads {
            let record = FutureRecord::to(&self.topic).payload(payload).key(key);

            let future = self.producer.send(record, Timeout::Never);
            futures.push(future);
        }

        // Wait for all futures to complete
        let mut results = Vec::with_capacity(futures.len());
        for future in futures {
            let result = future.await.map(|_| ()).map_err(|(err, _)| err);
            results.push(result);
        }

        results
    }
}
