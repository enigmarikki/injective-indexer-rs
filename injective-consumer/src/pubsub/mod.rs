use async_trait::async_trait;
use futures::{future::join_all, StreamExt};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
// Fixed: Removed unused Commands import
use redis::{aio::ConnectionManager, AsyncCommands, Client, RedisResult};
use serde::{Deserialize, Serialize};
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
}

// Stream event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEvent {
    pub event_type: EventType,
    pub timestamp: u64,
    pub payload: serde_json::Value,
}

// Configuration for Redis PubSub
#[derive(Clone)]
pub struct RedisPubSubConfig {
    pub redis_url: String,
    pub connection_pool_size: usize,
    pub sharded_channels: bool,
    pub channel_prefix: String,
    pub pattern_subscribe: bool,
    pub binary_protocol: bool,
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
            pattern_subscribe: false, // Pattern subscribe is slower, avoid unless needed
            binary_protocol: true,    // Use binary for better performance
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
    client: Client,
    // Connection pool for publishers
    pub_connections: Arc<Mutex<Vec<ConnectionManager>>>,
    // Queue for publishing messages
    pub_queue: mpsc::Sender<(String, Vec<u8>)>,
    metrics: Arc<PubSubMetrics>,
}

impl RedisPubSubService {
    pub async fn new(config: RedisPubSubConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Fixed: Removed the & reference to string
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
            client,
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

        // Using a multi-producer single-consumer approach for better reliability
        let rx = Arc::new(Mutex::new(rx));

        for i in 0..worker_count {
            let connections = connections.clone();
            let metrics = metrics.clone();
            let rx = rx.clone();

            // Spawn dedicated worker
            task::spawn(async move {
                info!("Starting Redis publisher worker #{}", i);

                loop {
                    // Get a message from the shared queue
                    let message = {
                        let mut rx_lock = rx.lock().await;
                        match rx_lock.recv().await {
                            Some(msg) => msg,
                            None => {
                                // Channel closed, exit worker
                                debug!("Publisher queue closed, exiting worker #{}", i);
                                break;
                            }
                        }
                    };

                    let (channel, payload) = message;
                    let start_time = Instant::now();

                    // Get a connection from the pool
                    let conn_result = {
                        let mut conn_guard = connections.lock().await;
                        if conn_guard.is_empty() {
                            warn!("Redis connection pool exhausted, publish may be delayed");
                            metrics
                                .publish_errors
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            None
                        } else {
                            // Take a connection from the pool
                            Some(conn_guard.pop().unwrap())
                        }
                    };

                    if let Some(mut conn) = conn_result {
                        // Publish the message
                        let result: RedisResult<()> = conn.publish(&channel, payload.clone()).await;

                        // Return connection to pool
                        {
                            let mut conn_guard = connections.lock().await;
                            conn_guard.push(conn);
                        }

                        // Update metrics
                        if result.is_ok() {
                            metrics
                                .messages_published
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                            // Update timing metrics
                            let elapsed_us = start_time.elapsed().as_micros() as u64;
                            let current_avg = metrics
                                .avg_publish_time_us
                                .load(std::sync::atomic::Ordering::Relaxed);
                            let new_avg = if current_avg == 0 {
                                elapsed_us
                            } else {
                                (current_avg * 9 + elapsed_us) / 10 // Exponential moving average
                            };
                            metrics
                                .avg_publish_time_us
                                .store(new_avg, std::sync::atomic::Ordering::Relaxed);

                            // Update max time if applicable
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
    // Use this method to ensure consistent channel naming
    pub fn get_channel_for_event(&self, event_type: EventType) -> String {
        if self.config.sharded_channels {
            // Use dedicated channel per event type for better throughput
            format!("{}:{:?}", self.config.channel_prefix, event_type)
        } else {
            // Use single channel for all events
            self.config.channel_prefix.clone()
        }
    }

    // Get a channel for market-specific events if sharding by market
    pub fn get_channel_for_market(&self, event_type: EventType, market_id: &str) -> String {
        if self.config.sharded_channels {
            format!(
                "{}:{}:{}",
                self.config.channel_prefix, market_id, event_type as u8
            )
        } else {
            self.get_channel_for_event(event_type)
        }
    }

    // High-performance publish method
    pub async fn publish_event(
        &self,
        event: StreamEvent,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let channel = self.get_channel_for_event(event.event_type);

        // Serialize based on protocol choice
        let payload = if self.config.binary_protocol {
            // For maximum performance, we use bincode instead of JSON
            bincode::serialize(&event)?
        } else {
            // JSON for compatibility
            serde_json::to_vec(&event)?
        };

        // Update queue depth metric
        self.metrics
            .queue_depth
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Send to publishing queue
        // Better error handling for send operation
        match self.pub_queue.send((channel, payload)).await {
            Ok(_) => {
                // Update queue depth metric after successful send
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

        // Group events by channel for efficient publishing
        let mut channel_events: HashMap<String, Vec<StreamEvent>> = HashMap::new();

        for event in events {
            let channel = self.get_channel_for_event(event.event_type);
            channel_events
                .entry(channel)
                .or_insert_with(Vec::new)
                .push(event);
        }

        // Convert to parallel futures
        let mut publish_futures = Vec::new();

        for (channel, events) in channel_events {
            // Create one combined payload for each channel
            let payload = if self.config.binary_protocol {
                bincode::serialize(&events)?
            } else {
                serde_json::to_vec(&events)?
            };

            // Update queue metric
            self.metrics
                .queue_depth
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            // Clone queue for async move
            let queue = self.pub_queue.clone();
            let metrics = self.metrics.clone();

            // Create publish future
            let future = async move {
                let result = queue.send((channel, payload)).await;
                metrics
                    .queue_depth
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                result
            };

            publish_futures.push(future);
        }

        // Execute all publishes in parallel
        let results = join_all(publish_futures).await;

        // Check for errors
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

    // Create a subscriber for a specific event type
    pub async fn create_subscriber(
        &self,
        event_types: Vec<EventType>,
    ) -> Result<RedisPubSubSubscriber, Box<dyn Error + Send + Sync>> {
        RedisPubSubSubscriber::new(self.client.clone(), event_types, self.config.clone()).await
    }

    // Helper methods to create common event types
    pub fn create_market_update(&self, market_id: &str, data: serde_json::Value) -> StreamEvent {
        StreamEvent {
            event_type: EventType::MarketUpdate,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload: serde_json::json!({
                "market_id": market_id,
                "data": data
            }),
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

    pub fn create_liquidation_alert(
        &self,
        market_id: &str,
        subaccount_id: &str,
        data: serde_json::Value,
    ) -> StreamEvent {
        StreamEvent {
            event_type: EventType::LiquidationAlert,
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            payload: serde_json::json!({
                "market_id": market_id,
                "subaccount_id": subaccount_id,
                "data": data
            }),
        }
    }
}

// Subscriber for Redis PubSub
pub struct RedisPubSubSubscriber {
    client: Client,
    event_types: Vec<EventType>,
    config: RedisPubSubConfig,
    rx: mpsc::Receiver<StreamEvent>,
}

impl RedisPubSubSubscriber {
    pub async fn new(
        client: Client,
        event_types: Vec<EventType>,
        config: RedisPubSubConfig,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel(2000);

        // Clone what we need for async closure
        let client_clone = client.clone();
        let event_types_clone = event_types.clone();
        let config_clone = config.clone();

        // Spawn subscriber task
        task::spawn(async move {
            Self::run_subscriber(client_clone, event_types_clone, tx, config_clone).await;
        });

        Ok(RedisPubSubSubscriber {
            client,
            event_types,
            config,
            rx,
        })
    }

    // Main subscriber loop
    async fn run_subscriber(
        client: Client,
        event_types: Vec<EventType>,
        tx: mpsc::Sender<StreamEvent>,
        config: RedisPubSubConfig,
    ) {
        // Added retry logic for connection failures
        let mut retry_count = 0;
        let max_retries = 5;
        let mut retry_delay = Duration::from_millis(100);

        'connection_loop: loop {
            // Create pubsub connection
            match client.get_async_connection().await {
                Ok(conn) => {
                    // Reset retry counters on successful connection
                    retry_count = 0;
                    retry_delay = Duration::from_millis(100);

                    let mut pubsub = conn.into_pubsub();

                    // Subscribe to required channels
                    let mut channels = Vec::new();

                    if config.sharded_channels {
                        // Subscribe to specific channels for each event type
                        for event_type in &event_types {
                            let channel = format!("{}:{:?}", config.channel_prefix, event_type);
                            channels.push(channel);
                        }
                    } else {
                        // Subscribe to single channel for all events
                        channels.push(config.channel_prefix.clone());
                    }

                    // Subscribe to channels
                    let mut subscription_failed = false;
                    for channel in &channels {
                        if let Err(e) = pubsub.subscribe(channel).await {
                            error!("Error subscribing to channel {}: {}", channel, e);
                            subscription_failed = true;
                            break;
                        }
                        info!("Subscribed to Redis channel: {}", channel);
                    }

                    if subscription_failed {
                        // Try to reconnect if subscription failed
                        retry_count += 1;
                        if retry_count >= max_retries {
                            error!(
                                "Failed to subscribe after {} retries, giving up",
                                max_retries
                            );
                            break 'connection_loop;
                        }

                        time::sleep(retry_delay).await;
                        retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(5));
                        continue 'connection_loop;
                    }

                    // Process messages
                    let mut msg_stream = pubsub.on_message();

                    while let Some(msg) = msg_stream.next().await {
                        let payload: Vec<u8> = match msg.get_payload() {
                            Ok(payload) => payload,
                            Err(e) => {
                                error!("Error getting payload: {}", e);
                                continue;
                            }
                        };

                        // Parse the message
                        let event: StreamEvent = if config.binary_protocol {
                            match bincode::deserialize(&payload) {
                                Ok(event) => event,
                                Err(e) => {
                                    error!("Error deserializing binary event: {}", e);
                                    continue;
                                }
                            }
                        } else {
                            match serde_json::from_slice(&payload) {
                                Ok(event) => event,
                                Err(e) => {
                                    error!("Error deserializing JSON event: {}", e);
                                    continue;
                                }
                            }
                        };

                        // Check if we're interested in this event type
                        if !config.sharded_channels || event_types.contains(&event.event_type) {
                            // Handle backpressure with try_send
                            match tx.try_send(event) {
                                Ok(_) => {} // Successfully sent
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    // Channel full, log warning
                                    warn!("Subscriber channel full, dropping message");
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    // Channel closed, exit loop
                                    debug!("Subscriber channel closed, exiting");
                                    break 'connection_loop;
                                }
                            }
                        }
                    }

                    // If we get here, the connection was dropped - try to reconnect
                    warn!("Redis PubSub connection dropped, attempting to reconnect");
                    time::sleep(Duration::from_millis(100)).await;
                }
                Err(e) => {
                    error!("Error creating pubsub connection: {}", e);

                    // Implement retry with exponential backoff
                    retry_count += 1;
                    if retry_count >= max_retries {
                        error!("Failed to connect after {} retries, giving up", max_retries);
                        break 'connection_loop;
                    }

                    warn!(
                        "Retrying connection in {:?} (attempt {}/{})",
                        retry_delay, retry_count, max_retries
                    );
                    time::sleep(retry_delay).await;
                    retry_delay = std::cmp::min(retry_delay * 2, Duration::from_secs(5));
                }
            }
        }

        info!("Redis PubSub subscriber task exiting");
    }

    // Get the next event
    pub async fn next_event(&mut self) -> Option<StreamEvent> {
        self.rx.recv().await
    }

    // Convert to a stream for easier consumption
    pub fn into_stream(self) -> impl futures::Stream<Item = StreamEvent> {
        tokio_stream::wrappers::ReceiverStream::new(self.rx)
    }
}
