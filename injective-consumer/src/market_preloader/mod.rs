use crate::compute::{calculate_liquidation_price, is_liquidatable};
use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, KafkaPayload};
use crate::pubsub::{EventType, RedisPubSubService, StreamEvent};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use redis::{Client, Commands, Connection};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

const PRICE_DECIMAL: f64 = 1e24;
const CHAIN_DECIMAL: f64 = 1e18;

// A dedicated processor that only handles market data
pub struct MarketPreloader {
    _client: Client,
    connection: Arc<Mutex<Connection>>,
    pubsub: Option<Arc<RedisPubSubService>>,
    // Track processed markets for metrics
    markets_processed: Arc<Mutex<u64>>,
    // Set of market IDs to track which ones we've seen
    known_markets: Arc<Mutex<HashSet<String>>>,
}

impl MarketPreloader {
    pub async fn new(
        redis_url: &str,
        pubsub: Arc<RedisPubSubService>,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        let connection = client.get_connection()?;

        let preloader = MarketPreloader {
            _client: client,
            connection: Arc::new(Mutex::new(connection)),
            pubsub: Some(pubsub),
            markets_processed: Arc::new(Mutex::new(0)),
            known_markets: Arc::new(Mutex::new(HashSet::new())),
        };

        // Set initial state in Redis to show we're in markets phase
        {
            let mut conn = preloader.connection.lock().await;
            conn.set::<_, _, ()>("processing_phase", "markets")?;
            conn.set::<_, _, ()>("markets_ready", "false")?;
        }

        Ok(preloader)
    }

    // Process a derivative market update
    async fn process_derivative_market(
        &self,
        market: &crate::models::DerivativeMarketPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().await;

        // Extract cumulative funding
        let cumulative_funding =
            market.cumulative_funding.parse::<f64>().unwrap_or(0.0) / PRICE_DECIMAL;

        // Parse other market data safely
        let mark_price = market.mark_price.parse::<f64>().unwrap_or(0.0) / PRICE_DECIMAL;
        let maintenance_margin_ratio = market
            .maintenance_margin_ratio
            .parse::<f64>()
            .unwrap_or(5e16)
            / CHAIN_DECIMAL;

        // Store market data in Redis
        let key = format!("market:derivative:{}", market.market_id);

        conn.hset::<_, _, _, ()>(&key, "ticker", &market.ticker)?;
        conn.hset::<_, _, _, ()>(&key, "mark_price", mark_price.to_string())?;
        conn.hset::<_, _, _, ()>(
            &key,
            "maintenance_margin_ratio",
            maintenance_margin_ratio.to_string(),
        )?;
        conn.hset::<_, _, _, ()>(&key, "cumulative_funding", cumulative_funding.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "block_height", block_height.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "timestamp", timestamp.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "status", &market.status)?;

        // Add to markets set
        conn.sadd::<_, _, ()>("markets:derivative", &market.market_id)?;

        // Add to our known markets set
        {
            let mut markets = self.known_markets.lock().await;
            markets.insert(market.market_id.clone());
        }

        // Publish market update through high-performance PubSub
        if let Some(pubsub) = &self.pubsub {
            let market_data = serde_json::json!({
                "ticker": market.ticker,
                "mark_price": mark_price.to_string(),
                "maintenance_margin_ratio": maintenance_margin_ratio.to_string(),
                "cumulative_funding": cumulative_funding.to_string(),
                "block_height": block_height.to_string(),
                "timestamp": timestamp.to_string(),
                "status": market.status,
                "preloaded": true,
            });

            // Create market update event
            let market_event = pubsub.create_market_update(market_data);

            // Publish through HPC Redis PubSub
            if let Err(e) = pubsub.publish_event(market_event).await {
                warn!("Failed to publish market update through PubSub: {}", e);
            }

            // Also publish price update for clients only interested in prices
            let price_event =
                pubsub.create_price_update(&market.market_id, &mark_price.to_string());

            if let Err(e) = pubsub.publish_event(price_event).await {
                warn!("Failed to publish price update through PubSub: {}", e);
            }
        }
        Ok(())
    }

    // Check if we're done with the initial market loading phase
    async fn check_phase_transition(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // This is a simplified check - in a real system you might want to:
        // 1. Wait until you've seen markets for X consecutive blocks
        // 2. Wait until you've seen a minimum number of markets
        // 3. Wait until a certain amount of time has passed

        let processed_count = *self.markets_processed.lock().await;
        let known_markets_count = self.known_markets.lock().await.len();

        if processed_count >= 10 && known_markets_count > 0 {
            info!(
                "Market preloader has processed initial markets (count: {})",
                processed_count
            );

            // Signal that markets are ready
            let mut conn = self.connection.lock().await;
            conn.set::<_, _, ()>("processing_phase", "others")?;
            conn.set::<_, _, ()>("markets_ready", "true")?;

            // Publish a system event to notify other components
            if let Some(pubsub) = &self.pubsub {
                let event = StreamEvent {
                    event_type: EventType::SystemEvent,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    payload: serde_json::json!({
                        "event": "markets_ready",
                        "processed_count": processed_count,
                        "market_count": known_markets_count,
                    }),
                };

                if let Err(e) = pubsub.publish_event(event).await {
                    warn!("Failed to publish markets_ready event: {}", e);
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl MessageProcessor for MarketPreloader {
    async fn process_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let block_height = message.block_height;
        let timestamp = message.block_time;

        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                info!(
                    "Market preloader: Processing {} markets at block {}",
                    markets.len(),
                    block_height
                );

                for market in markets {
                    if let Err(e) = self
                        .process_derivative_market(market, block_height, timestamp)
                        .await
                    {
                        error!(
                            "Market preloader: Error processing derivative market: {}",
                            e
                        );
                    }
                }

                // Check if we should transition phases
                if let Err(e) = self.check_phase_transition().await {
                    warn!("Error checking phase transition: {}", e);
                }
            }
            _ => {
                // The market preloader ignores all other message types
                debug!("Market preloader: Ignoring non-market message");
            }
        }

        Ok(())
    }
}
