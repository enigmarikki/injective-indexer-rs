use futures::future::join_all;
use log::{debug, error, info, warn};
use redis::{aio::ConnectionManager, AsyncCommands, Client, RedisResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};
use tokio::{task, time};

// Stream event types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EventType {
    MarketUpdate = 0,
    PositionUpdate = 1,
    LiquidationAlert = 2,
    PriceUpdate = 3,
    OrderbookUpdate = 4,
    TradeUpdate = 5,
    SystemEvent = 6,
}

// Stream event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub event_type: EventType,
    pub timestamp: u64,
    pub payload: serde_json::Value,
}
#[derive(Clone)]
pub enum SerializationProtocol {
    Bincode,
    Json,
}

// Configuration for Redis PubSub
#[derive(Clone)]
pub struct RedisPubSubConfig {
    pub redis_url: String,
    pub connection_pool_size: usize,
    pub sharded_channels: bool,
    pub channel_prefix: String,
    pub protocol: SerializationProtocol,
    pub metrics_interval_secs: u64,
    pub publisher_queue_size: usize,
    pub publisher_workers: usize,
}

impl Default for RedisPubSubConfig {
    fn default() -> Self {
        RedisPubSubConfig {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            connection_pool_size: 32, // Dragonfly can handle many connections efficiently
            sharded_channels: true,   // Use multiple channels for better throughput
            channel_prefix: "inj:exchange".to_string(),
            protocol: SerializationProtocol::Json, // Default protocol
            metrics_interval_secs: 10,
            publisher_queue_size: 10000, // Large queue for handling spikes
            publisher_workers: 8,        // Multiple publisher workers
        }
    }
}

// Metrics for monitoring
#[derive(Default, Debug, Serialize)]
pub struct PubSubMetrics {
    pub messages_published: std::sync::atomic::AtomicU64,
    pub publish_errors: std::sync::atomic::AtomicU64,
    pub avg_publish_time_us: std::sync::atomic::AtomicU64,
    pub max_publish_time_us: std::sync::atomic::AtomicU64,
    pub queue_depth: std::sync::atomic::AtomicU64,
}

// The main Redis PubSub service optimized for Dragonfly
pub struct RedisPubSubService {
    config: RedisPubSubConfig,
    // Connection pool for publishers
    pub_connections: Arc<Mutex<Vec<ConnectionManager>>>,
    // Queue for publishing messages
    pub_queue: mpsc::Sender<(String, Vec<u8>)>,
    metrics: Arc<PubSubMetrics>,
}

impl RedisPubSubService {
    pub async fn new(config: RedisPubSubConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(config.redis_url.clone())?;

        // Create connection pool for publishers
        let mut connections = Vec::with_capacity(config.connection_pool_size);
        for _ in 0..config.connection_pool_size {
            let conn = ConnectionManager::new(client.clone()).await?;
            connections.push(conn);
        }

        // Create publishing queue
        let (tx, rx) = mpsc::channel(config.publisher_queue_size);

        let metrics = Arc::new(PubSubMetrics::default());

        let service = RedisPubSubService {
            config: config.clone(),
            pub_connections: Arc::new(Mutex::new(connections)),
            pub_queue: tx,
            metrics: metrics.clone(),
        };

        // Start publisher workers
        service.spawn_publisher_workers(rx).await;

        // Start metrics reporter
        service.spawn_metrics_reporter();

        Ok(service)
    }

    // Spawn workers to handle publishing messages
    async fn spawn_publisher_workers(&self, rx: mpsc::Receiver<(String, Vec<u8>)>) {
        let connections = self.pub_connections.clone();
        let metrics = self.metrics.clone();
        let worker_count = self.config.publisher_workers;

        let rx = Arc::new(Mutex::new(rx));

        for i in 0..worker_count {
            let connections = connections.clone();
            let metrics = metrics.clone();
            let rx = rx.clone();

            task::spawn(async move {
                info!("Starting Redis publisher worker #{}", i);

                loop {
                    let message = {
                        let mut rx_lock = rx.lock().await;
                        match rx_lock.recv().await {
                            Some(msg) => msg,
                            None => {
                                debug!("Publisher queue closed, exiting worker #{}", i);
                                break;
                            }
                        }
                    };

                    let (channel, payload) = message;
                    let start_time = Instant::now();

                    let conn_result = {
                        let mut conn_guard = connections.lock().await;
                        if conn_guard.is_empty() {
                            warn!("Redis connection pool exhausted, publish may be delayed");
                            metrics
                                .publish_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            None
                        } else {
                            Some(conn_guard.pop().unwrap())
                        }
                    };

                    if let Some(mut conn) = conn_result {
                        let result: RedisResult<()> = conn.publish(&channel, payload.clone()).await;

                        {
                            let mut conn_guard = connections.lock().await;
                            conn_guard.push(conn);
                        }

                        if result.is_ok() {
                            metrics
                                .messages_published
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            let elapsed_us = start_time.elapsed().as_micros() as u64;
                            let current_avg = metrics
                                .avg_publish_time_us
                                .load(std::sync::atomic::Ordering::Relaxed);
                            let new_avg = if current_avg == 0 {
                                elapsed_us
                            } else {
                                (current_avg * 9 + elapsed_us) / 10
                            };
                            metrics
                                .avg_publish_time_us
                                .store(new_avg, std::sync::atomic::Ordering::Relaxed);

                            let current_max = metrics
                                .max_publish_time_us
                                .load(std::sync::atomic::Ordering::Relaxed);
                            if elapsed_us > current_max {
                                metrics
                                    .max_publish_time_us
                                    .store(elapsed_us, std::sync::atomic::Ordering::Relaxed);
                            }
                        } else if let Err(e) = result {
                            error!("Error publishing to Redis: {}", e);
                            metrics
                                .publish_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    }

    // Spawn a task to report metrics periodically
    fn spawn_metrics_reporter(&self) {
        let metrics = self.metrics.clone();
        let interval = self.config.metrics_interval_secs;

        task::spawn(async move {
            let mut interval_timer = time::interval(Duration::from_secs(interval));

            loop {
                interval_timer.tick().await;

                let published = metrics
                    .messages_published
                    .load(std::sync::atomic::Ordering::Relaxed);
                let errors = metrics
                    .publish_errors
                    .load(std::sync::atomic::Ordering::Relaxed);
                let avg_us = metrics
                    .avg_publish_time_us
                    .load(std::sync::atomic::Ordering::Relaxed);
                let max_us = metrics
                    .max_publish_time_us
                    .load(std::sync::atomic::Ordering::Relaxed);
                let queue = metrics
                    .queue_depth
                    .load(std::sync::atomic::Ordering::Relaxed);

                info!(
                    "Redis PubSub metrics: published={}, errors={}, avg_time={}µs, max_time={}µs, queue={}",
                    published, errors, avg_us, max_us, queue
                );
            }
        });
    }

    // Get a channel name based on event type
    pub fn get_channel_for_event(&self, event_type: EventType) -> String {
        if self.config.sharded_channels {
            format!("{}:{:?}", self.config.channel_prefix, event_type)
        } else {
            self.config.channel_prefix.clone()
        }
    }

    // High-performance publish method
    pub async fn publish_event(
        &self,
        event: StreamEvent,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let channel = self.get_channel_for_event(event.event_type);

        // Serialize based on protocol choice using a match on self.config.protocol.
        let payload = match self.config.protocol {
            SerializationProtocol::Bincode => bincode::serialize(&event)?,
            SerializationProtocol::Json => serde_json::to_vec(&event)?,
        };

        // Update queue depth metric
        self.metrics
            .queue_depth
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        match self.pub_queue.send((channel, payload)).await {
            Ok(_) => {
                self.metrics
                    .queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                Ok(())
            }
            Err(e) => {
                self.metrics
                    .publish_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                self.metrics
                    .queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                Err(format!("Failed to send to publishing queue: {}", e).into())
            }
        }
    }

    // Batch publish method for higher throughput
    pub async fn publish_events_batch(
        &self,
        events: Vec<StreamEvent>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if events.is_empty() {
            return Ok(());
        }

        let mut channel_events: HashMap<String, Vec<StreamEvent>> = HashMap::new();
        for event in events {
            let channel = self.get_channel_for_event(event.event_type);
            channel_events
                .entry(channel)
                .or_insert_with(Vec::new)
                .push(event);
        }

        let mut publish_futures = Vec::new();

        for (channel, events) in channel_events {
            let payload = match self.config.protocol {
                SerializationProtocol::Bincode => bincode::serialize(&events)?,
                SerializationProtocol::Json => serde_json::to_vec(&events)?,
            };

            self.metrics
                .queue_depth
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let queue = self.pub_queue.clone();
            let metrics = self.metrics.clone();
            let future = async move {
                let result = queue.send((channel, payload)).await;
                metrics
                    .queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                result
            };
            publish_futures.push(future);
        }

        let results = join_all(publish_futures).await;

        for result in results {
            if result.is_err() {
                self.metrics
                    .publish_errors
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Err("Error publishing batch to Redis".into());
            }
        }

        Ok(())
    }

    // Helper methods to create common event types
    pub fn create_market_update(&self, data: serde_json::Value) -> StreamEvent {
        StreamEvent {
            event_type: EventType::MarketUpdate,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload: serde_json::json!(data),
        }
    }

    pub fn create_price_update(&self, market_id: &str, price: &str) -> StreamEvent {
        StreamEvent {
            event_type: EventType::PriceUpdate,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload: serde_json::json!({
                "market_id": market_id,
                "price": price
            }),
        }
    }

    pub fn create_liquidation_alert(&self, data: serde_json::Value) -> StreamEvent {
        StreamEvent {
            event_type: EventType::LiquidationAlert,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload: serde_json::json!(
                 data
            ),
        }
    }
}
