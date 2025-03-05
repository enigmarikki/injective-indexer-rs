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
const QUANTITY_DECIMAL: f64 = 1e18;

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
            .unwrap_or(0.0)
            / QUANTITY_DECIMAL;

        // Store market data in Redis
        let key = format!("market:derivative:{}", market.market_id);

        conn.hset::<_, _, _, ()>(&key, "ticker", &market.ticker)?;
        conn.hset::<_, _, _, ()>(&key, "mark_price", mark_price.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "maintenance_margin_ratio", maintenance_margin_ratio)?;
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
            let market_event = pubsub.create_market_update(&market.market_id, market_data);

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

        // Update all positions for this market with new liquidation prices
        let positions_key = format!("positions:market:{}", market.market_id);
        let subaccount_ids: Vec<String> = conn.smembers(&positions_key)?;

        // Batch liquidation updates for efficiency
        let mut liquidation_events = Vec::new();

        for subaccount_id in subaccount_ids {
            let position_key = format!("position:{}:{}", market.market_id, subaccount_id);

            // Get position data with better error handling
            let is_long: bool = match conn.hget::<_, _, Option<String>>(&position_key, "is_long") {
                Ok(Some(value)) => value.parse().unwrap_or(false),
                _ => {
                    warn!(
                        "Missing is_long value for position {}, skipping",
                        position_key
                    );
                    continue;
                }
            };

            let quantity: f64 = match conn.hget::<_, _, Option<String>>(&position_key, "quantity") {
                Ok(Some(value)) => value.parse().unwrap_or(0.0),
                _ => {
                    warn!(
                        "Missing quantity value for position {}, skipping",
                        position_key
                    );
                    continue;
                }
            };

            let entry_price: f64 =
                match conn.hget::<_, _, Option<String>>(&position_key, "entry_price") {
                    Ok(Some(value)) => value.parse().unwrap_or(0.0),
                    _ => {
                        warn!(
                            "Missing entry_price value for position {}, skipping",
                            position_key
                        );
                        continue;
                    }
                };

            let margin: f64 = match conn.hget::<_, _, Option<String>>(&position_key, "margin") {
                Ok(Some(value)) => value.parse().unwrap_or(0.0),
                _ => {
                    warn!(
                        "Missing margin value for position {}, skipping",
                        position_key
                    );
                    continue;
                }
            };

            let cumulative_funding_entry: f64 = match conn
                .hget::<_, _, Option<String>>(&position_key, "cumulative_funding_entry")
            {
                Ok(Some(value)) => value.parse().unwrap_or(0.0),
                _ => 0.0, // Default to 0 if missing
            };

            // Skip positions with invalid data
            if quantity <= 0.0 || entry_price <= 0.0 || margin <= 0.0 {
                warn!("Invalid position data for {}, skipping", position_key);
                continue;
            }

            // Recalculate liquidation price
            let liquidation_price = calculate_liquidation_price(
                is_long,
                entry_price,
                margin,
                quantity,
                maintenance_margin_ratio,
                cumulative_funding,
                cumulative_funding_entry,
            );

            // Update the liquidation price
            conn.hset::<_, _, _, ()>(
                &position_key,
                "liquidation_price",
                liquidation_price.to_string(),
            )?;

            // Check if liquidatable
            let is_liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);
            conn.hset::<_, _, _, ()>(
                &position_key,
                "is_liquidatable",
                is_liquidatable.to_string(),
            )?;

            // Create position update data for PubSub
            if let Some(pubsub) = &self.pubsub {
                let position_data = serde_json::json!({
                    "is_long": is_long,
                    "quantity": quantity.to_string(),
                    "entry_price": entry_price.to_string(),
                    "margin": margin.to_string(),
                    "liquidation_price": liquidation_price.to_string(),
                    "cumulative_funding_entry": cumulative_funding_entry.to_string(),
                    "mark_price": mark_price.to_string(),
                    "is_liquidatable": is_liquidatable,
                    "block_height": block_height.to_string(),
                    "timestamp": timestamp.to_string(),
                    "preloaded": true,  // Flag to indicate this came from preloader
                });

                // Create position update event
                let event = StreamEvent {
                    event_type: EventType::PositionUpdate,
                    timestamp,
                    payload: serde_json::json!({
                        "market_id": market.market_id,
                        "subaccount_id": subaccount_id,
                        "data": position_data
                    }),
                };

                // Publish position update
                if let Err(e) = pubsub.publish_event(event).await {
                    warn!("Failed to publish position update: {}", e);
                }

                // For liquidatable positions, also create a liquidation alert
                if is_liquidatable {
                    let alert_data = serde_json::json!({
                        "market_id": market.market_id,
                        "subaccount_id": subaccount_id,
                        "is_long": is_long,
                        "liquidation_price": liquidation_price,
                        "mark_price": mark_price,
                        "quantity": quantity.to_string(),
                        "entry_price": entry_price.to_string(),
                        "margin": margin.to_string(),
                        "preloaded": true,  // Flag to indicate this came from preloader
                    });

                    // Add to batch for efficient publishing
                    let liquidation_event = pubsub.create_liquidation_alert(alert_data);

                    liquidation_events.push(liquidation_event);
                }
            }

            // Update liquidatable positions set
            if is_liquidatable {
                conn.sadd::<_, _, ()>(
                    "liquidatable_positions",
                    format!("{}:{}", market.market_id, subaccount_id),
                )?;

                // Legacy Redis publish for backward compatibility
                let alert_data = serde_json::json!({
                    "market_id": market.market_id,
                    "subaccount_id": subaccount_id,
                    "is_long": is_long,
                    "liquidation_price": liquidation_price,
                    "mark_price": mark_price,
                    "quantity": quantity.to_string(),
                    "entry_price": entry_price.to_string(),
                    "margin": margin.to_string(),
                });

                conn.publish::<_, _, ()>("liquidation_alerts", alert_data.to_string())?;

                info!(
                    "Liquidatable position: market={}, subaccount={}, liq_price={}, mark_price={}",
                    market.market_id, subaccount_id, liquidation_price, mark_price
                );
            } else {
                conn.srem::<_, _, ()>(
                    "liquidatable_positions",
                    format!("{}:{}", market.market_id, subaccount_id),
                )?;
            }
        }

        // Batch publish liquidation events if any
        if let Some(pubsub) = &self.pubsub {
            if !liquidation_events.is_empty() {
                if let Err(e) = pubsub.publish_events_batch(liquidation_events).await {
                    warn!("Failed to publish liquidation alerts batch: {}", e);
                }
            }
        }

        // Increment processed markets counter
        {
            let mut count = self.markets_processed.lock().await;
            *count += 1;
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
