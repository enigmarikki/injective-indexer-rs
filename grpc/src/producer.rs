use crate::config::KafkaConfig;
use crate::models::KafkaMessage;
use futures::future::join_all;
use log::error;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::util::Timeout;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
const MAX_CONCURRENT_REQUESTS: usize = 100;
const BATCH_SIZE: usize = 1000;

pub struct BatchKafkaProducer {
    producer: Arc<FutureProducer>,
    topic: String,
    request_limiter: Arc<Semaphore>,
    latest_processed_block: Arc<std::sync::atomic::AtomicU64>,
}
impl BatchKafkaProducer {
    pub fn new(config: &KafkaConfig) -> Result<Self, KafkaError> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", config.brokers.join(","))
            .set("client.id", &config.client_id)
            // Ultra-low latency optimizations
            .set("compression.type", "lz4") // Faster than snappy
            .set("batch.size", "32768") // Optimal batch size for throughput
            .set("linger.ms", "0") // No delay for low latency
            .set("max.in.flight.requests.per.connection", "5")
            .set("queue.buffering.max.messages", "5000000")
            .set("queue.buffering.max.kbytes", "2097152")
            .set("socket.send.buffer.bytes", "1048576")
            .set("socket.receive.buffer.bytes", "1048576")
            .set("message.send.max.retries", "2")
            .set("retry.backoff.ms", "1")
            .set("acks", "1")
            .set("delivery.timeout.ms", "30000")
            .set("request.timeout.ms", "1000")
            .create()?;

        Ok(BatchKafkaProducer {
            producer: Arc::new(producer),
            topic: config.topic.clone(),
            request_limiter: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            latest_processed_block: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        })
    }
    pub fn update_latest_block(&self, block_height: u64) {
        let current = self
            .latest_processed_block
            .load(std::sync::atomic::Ordering::Relaxed);
        if block_height > current {
            self.latest_processed_block
                .store(block_height, std::sync::atomic::Ordering::Relaxed);
        }
    }
    /// Get the latest processed block height
    pub fn get_latest_block(&self) -> u64 {
        self.latest_processed_block
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Filter and send only messages from current or newer blocks
    pub async fn send_batch_current_only(
        &self,
        messages: Vec<KafkaMessage>,
    ) -> Vec<Result<(), KafkaError>> {
        if messages.is_empty() {
            return Vec::new();
        }

        // Get current latest block
        let latest_block = self.get_latest_block();

        // Filter messages to only include current or newer blocks
        let filtered_messages: Vec<KafkaMessage> = messages
            .into_iter()
            .filter(|msg| {
                // Always accept messages with higher block height
                if msg.block_height > latest_block {
                    // Update our latest block tracker
                    self.update_latest_block(msg.block_height);
                    return true;
                }
                // Only include messages from latest block
                msg.block_height == latest_block
            })
            .collect();

        // If no messages remain after filtering, return empty results
        if filtered_messages.is_empty() {
            return Vec::new();
        }

        // Process the filtered messages as normal
        self.send_batch(filtered_messages).await
    }
    /// Sends a batch of messages with extreme throughput optimization
    pub async fn send_batch(&self, messages: Vec<KafkaMessage>) -> Vec<Result<(), KafkaError>> {
        if messages.is_empty() {
            return Vec::new();
        }

        // Pre-allocate results with the exact capacity needed
        let mut results = Vec::with_capacity(messages.len());

        // Split messages into chunks for batching
        let chunks = self.partition_messages(messages);

        // Process each chunk concurrently but not with separate tasks
        let futures = chunks.into_iter().map(|chunk| self.process_chunk(chunk));

        // Wait for all futures
        let chunk_results = join_all(futures).await;

        // Collect results
        for mut chunk_result in chunk_results {
            results.append(&mut chunk_result);
        }

        results
    }

    /// Process a chunk of messages
    async fn process_chunk(&self, chunk: Vec<KafkaMessage>) -> Vec<Result<(), KafkaError>> {
        let mut results = Vec::with_capacity(chunk.len());
        let futures = chunk.into_iter().map(|message| {
            let producer = Arc::clone(&self.producer);
            let topic = self.topic.clone();
            let request_limiter = Arc::clone(&self.request_limiter);

            async move {
                // Acquire semaphore permit to limit concurrent requests
                let _permit = request_limiter.acquire().await.unwrap();

                // Serialize message
                let key = format!("{}-{}", message.block_height, message.block_time);
                let result = match serde_json::to_string(&message) {
                    Ok(payload) => {
                        // Send message
                        let record = FutureRecord::to(&topic).payload(&payload).key(&key);

                        producer
                            .send(record, Timeout::Never)
                            .await
                            .map(|_| ())
                            .map_err(|(e, _)| e)
                    }
                    Err(e) => {
                        error!("Failed to serialize message: {}", e);
                        // Use a valid KafkaError variant
                        Err(KafkaError::MessageProduction(
                            rdkafka::types::RDKafkaErrorCode::InvalidRecord,
                        ))
                    }
                };
                result
            }
        });

        // Execute all futures in parallel with concurrency control
        results.extend(join_all(futures).await);
        results
    }

    /// Partition messages for optimal processing
    fn partition_messages(&self, messages: Vec<KafkaMessage>) -> Vec<Vec<KafkaMessage>> {
        // Split messages into chunks of BATCH_SIZE
        let mut chunks = Vec::new();
        for chunk in messages.chunks(BATCH_SIZE) {
            chunks.push(chunk.to_vec());
        }
        chunks
    }

    /// Version optimized for ultra-low latency (sacrifices some throughput)
    pub async fn send_batch_low_latency(
        &self,
        messages: Vec<KafkaMessage>,
    ) -> Vec<Result<(), KafkaError>> {
        if messages.is_empty() {
            return Vec::new();
        }
        let mut results = Vec::with_capacity(messages.len());
        for message in messages {
            let key = format!("{}-{}", message.block_height, message.block_time);
            let result = match serde_json::to_string(&message) {
                Ok(payload) => {
                    let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);
                    self.producer
                        .send(record, Timeout::After(Duration::from_micros(1)))
                        .await
                        .map(|_| ())
                        .map_err(|(e, _)| e)
                }
                Err(e) => {
                    error!("Failed to serialize message: {}", e);
                    Err(KafkaError::MessageProduction(
                        rdkafka::types::RDKafkaErrorCode::InvalidRecord,
                    ))
                }
            };
            results.push(result);
        }

        results
    }

    /// Method to flush all pending messages - important for graceful shutdown
    pub async fn flush(&self, timeout_ms: u64) -> Result<(), KafkaError> {
        self.producer
            .flush(Timeout::After(Duration::from_millis(timeout_ms)))
    }
}
