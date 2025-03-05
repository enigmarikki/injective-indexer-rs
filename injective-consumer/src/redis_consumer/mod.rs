use crate::compute::{calculate_liquidation_price, is_liquidatable};
use crate::consumer::MessageProcessor;
use crate::models::{
    DerivativeMarketPayload, KafkaMessage, KafkaPayload, MessageType, PositionPayload,
};
use crate::pubsub::{EventType, RedisPubSubService, StreamEvent};
use async_trait::async_trait;
use log::{error, info, warn};
use redis::{Client, Commands, Connection};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

// Enum to represent different processing phases
#[derive(Debug, Clone, Copy, PartialEq)]
enum ProcessingPhase {
    Markets,
    Others,
}

pub struct RedisProcessor {
    _client: Client,
    connection: Arc<Mutex<Connection>>,
    pubsub: Option<Arc<RedisPubSubService>>,
    // Processing phase tracker
    phase: Arc<Mutex<ProcessingPhase>>,
    // Queue for non-market messages
    deferred_messages: Arc<Mutex<Vec<KafkaMessage>>>,
    // Track market IDs we need to process
    market_ids: Arc<Mutex<HashSet<String>>>,
}

impl RedisProcessor {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        let connection = client.get_connection()?;

        Ok(RedisProcessor {
            _client: client,
            connection: Arc::new(Mutex::new(connection)),
            pubsub: None,
            phase: Arc::new(Mutex::new(ProcessingPhase::Markets)),
            deferred_messages: Arc::new(Mutex::new(Vec::new())),
            market_ids: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    // Add a method to set the PubSub service
    pub fn with_pubsub(mut self, pubsub: Arc<RedisPubSubService>) -> Self {
        self.pubsub = Some(pubsub);
        self
    }

    async fn process_derivative_market(
        &self,
        market: &DerivativeMarketPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("DEBUG-10: Starting to process market {}", market.market_id);
        let mut conn = self.connection.lock().await;

        // Extract cumulative funding
        let cumulative_funding = market.cumulative_funding.parse::<f64>().unwrap_or(0.0);

        // Parse other market data safely
        let mark_price = market.mark_price.parse::<f64>().unwrap_or(0.0);
        let maintenance_margin_ratio = market
            .maintenance_margin_ratio
            .parse::<f64>()
            .unwrap_or(0.05);

        // Store market data in Redis
        let key = format!("market:derivative:{}", market.market_id);

        // DEBUG-11: Log Redis operations
        info!("DEBUG-11: Storing market data to Redis key: {}", key);

        conn.hset::<_, _, _, ()>(&key, "ticker", &market.ticker)?;
        conn.hset::<_, _, _, ()>(&key, "mark_price", &market.mark_price)?;
        conn.hset::<_, _, _, ()>(
            &key,
            "maintenance_margin_ratio",
            &market.maintenance_margin_ratio,
        )?;
        conn.hset::<_, _, _, ()>(&key, "cumulative_funding", cumulative_funding.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "block_height", block_height.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "timestamp", timestamp.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "status", &market.status)?;

        // Add to markets set
        conn.sadd::<_, _, ()>("markets:derivative", &market.market_id)?;

        // Remove from pending markets set
        {
            info!(
                "DEBUG-12: About to remove market {} from pending",
                market.market_id
            );
            let mut market_ids = self.market_ids.lock().await;
            let was_removed = market_ids.remove(&market.market_id);
            info!(
                "DEBUG-13: Market {} removed from pending: {}",
                market.market_id, was_removed
            );

            // If this was the last market, update phase
            if market_ids.is_empty() {
                info!("DEBUG-14: All markets processed. Switching to Others phase.");
                *self.phase.lock().await = ProcessingPhase::Others;

                // Mark in Redis that markets are ready
                conn.set::<_, _, ()>("markets_ready", "true")?;

                // Process queued messages
                drop(conn); // Release connection lock before processing
                info!("DEBUG-15: About to process queued messages");
                match self.process_queued_messages().await {
                    Ok(_) => info!("DEBUG-16: Successfully processed queued messages"),
                    Err(e) => error!("DEBUG-16: Error processing queued messages: {}", e),
                }
            } else {
                info!("DEBUG-17: Still have {} markets pending", market_ids.len());
            }
        }

        // Publish market update through high-performance PubSub
        if let Some(pubsub) = &self.pubsub {
            let market_data = serde_json::json!({
                "ticker": market.ticker,
                "mark_price": market.mark_price,
                "maintenance_margin_ratio": market.maintenance_margin_ratio,
                "cumulative_funding": cumulative_funding.to_string(),
                "block_height": block_height.to_string(),
                "timestamp": timestamp.to_string(),
                "status": market.status,
            });

            // Create market update event
            let market_event = pubsub.create_market_update(&market.market_id, market_data);

            // Publish through HPC Redis PubSub
            if let Err(e) = pubsub.publish_event(market_event).await {
                warn!("Failed to publish market update through PubSub: {}", e);
            }

            // Also publish price update for clients only interested in prices
            let price_event = pubsub.create_price_update(&market.market_id, &market.mark_price);

            if let Err(e) = pubsub.publish_event(price_event).await {
                warn!("Failed to publish price update through PubSub: {}", e);
            }
        }

        // Reacquire connection lock for position updates
        let mut conn = self.connection.lock().await;

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
                    });

                    // Add to batch for efficient publishing
                    let liquidation_event = pubsub.create_liquidation_alert(
                        &market.market_id,
                        &subaccount_id,
                        alert_data,
                    );

                    liquidation_events.push(liquidation_event);
                }
            }

            // Update liquidatable positions set and publish alerts
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

        Ok(())
    }

    // Process any queued non-market messages
    async fn process_queued_messages(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let messages = {
            let mut deferred = self.deferred_messages.lock().await;
            info!("DEBUG-18: Taking {} messages from queue", deferred.len());
            std::mem::take(&mut *deferred)
        };

        info!("DEBUG-19: Processing {} queued messages", messages.len());

        for (i, message) in messages.iter().enumerate() {
            info!(
                "DEBUG-20: Processing queued message {} of {}",
                i + 1,
                messages.len()
            );
            if let Err(e) = self.process_non_market_message(&message).await {
                error!("DEBUG-21: Error processing queued message {}: {}", i + 1, e);
            }
        }

        Ok(())
    }

    // Process a position message
    async fn process_position(
        &self,
        position: &PositionPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "DEBUG-22: Processing position for market={}, subaccount={}",
            position.market_id, position.subaccount_id
        );
        let mut conn = self.connection.lock().await;

        // Check if market exists
        let market_key = format!("market:derivative:{}", position.market_id);
        let market_exists: bool = conn.exists(&market_key)?;

        if !market_exists {
            warn!(
                "DEBUG-23: Market {} not found for position {}. Skipping position processing.",
                position.market_id, position.subaccount_id
            );
            return Ok(());
        }

        info!("DEBUG-24: Market exists for position, continuing processing");

        // Parse position data safely
        let is_long = position.is_long;
        let quantity = position.quantity.parse::<f64>().unwrap_or(0.0);
        let entry_price = position.entry_price.parse::<f64>().unwrap_or(0.0);
        let margin = position.margin.parse::<f64>().unwrap_or(0.0);
        let cumulative_funding_entry = position
            .cumulative_funding_entry
            .parse::<f64>()
            .unwrap_or(0.0);

        // Skip positions with invalid data
        if quantity <= 0.0 || entry_price <= 0.0 || margin <= 0.0 {
            warn!(
                "DEBUG-25: Invalid position data (q={}, p={}, m={}) for market {} subaccount {}, skipping",
                quantity, entry_price, margin, position.market_id, position.subaccount_id
            );
            return Ok(());
        }

        // Get market data for calculating liquidation price
        let mark_price: f64 = match conn.hget::<_, _, Option<String>>(&market_key, "mark_price") {
            Ok(Some(value)) => value.parse().unwrap_or(0.0),
            _ => {
                warn!(
                    "Missing mark_price for market {}, using default",
                    position.market_id
                );
                0.0
            }
        };

        let maintenance_margin_ratio: f64 =
            match conn.hget::<_, _, Option<String>>(&market_key, "maintenance_margin_ratio") {
                Ok(Some(value)) => value.parse().unwrap_or(0.05),
                _ => {
                    warn!(
                        "Missing maintenance_margin_ratio for market {}, using default",
                        position.market_id
                    );
                    0.05
                }
            };

        let market_cumulative_funding: f64 =
            match conn.hget::<_, _, Option<String>>(&market_key, "cumulative_funding") {
                Ok(Some(value)) => value.parse().unwrap_or(0.0),
                _ => {
                    warn!(
                        "Missing cumulative_funding for market {}, using default",
                        position.market_id
                    );
                    0.0
                }
            };

        // Calculate liquidation price
        let liquidation_price = calculate_liquidation_price(
            is_long,
            entry_price,
            margin,
            quantity,
            maintenance_margin_ratio,
            market_cumulative_funding,
            cumulative_funding_entry,
        );

        // Store position data
        let key = format!("position:{}:{}", position.market_id, position.subaccount_id);
        info!("DEBUG-26: Storing position to Redis key: {}", key);

        conn.hset::<_, _, _, ()>(&key, "is_long", position.is_long.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "quantity", &position.quantity)?;
        conn.hset::<_, _, _, ()>(&key, "entry_price", &position.entry_price)?;
        conn.hset::<_, _, _, ()>(&key, "margin", &position.margin)?;
        conn.hset::<_, _, _, ()>(
            &key,
            "cumulative_funding_entry",
            &position.cumulative_funding_entry,
        )?;
        conn.hset::<_, _, _, ()>(&key, "liquidation_price", liquidation_price.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "block_height", block_height.to_string())?;
        conn.hset::<_, _, _, ()>(&key, "timestamp", timestamp.to_string())?;

        // Add to position sets
        conn.sadd::<_, _, ()>(
            format!("positions:market:{}", position.market_id),
            &position.subaccount_id,
        )?;
        conn.sadd::<_, _, ()>(
            format!("positions:subaccount:{}", position.subaccount_id),
            &position.market_id,
        )?;

        // Check if liquidatable
        let is_liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);
        conn.hset::<_, _, _, ()>(&key, "is_liquidatable", is_liquidatable.to_string())?;

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
            });

            // Create position update event
            let position_event = StreamEvent {
                event_type: EventType::PositionUpdate,
                timestamp,
                payload: serde_json::json!({
                    "market_id": position.market_id.clone(),
                    "subaccount_id": position.subaccount_id.clone(),
                    "data": position_data.clone()
                }),
            };

            let pubsub_clone = pubsub.clone();
            let _market_id = position.market_id.clone();
            let _subaccount_id = position.subaccount_id.clone();

            // Spawn a task to publish the position update
            tokio::spawn(async move {
                if let Err(e) = pubsub_clone.publish_event(position_event).await {
                    warn!("Failed to publish position update: {}", e);
                }
            });
        }

        // Update liquidatable positions and publish alerts
        if is_liquidatable {
            conn.sadd::<_, _, ()>(
                "liquidatable_positions",
                format!("{}:{}", position.market_id, position.subaccount_id),
            )?;

            // Create liquidation alert data
            let alert_data = serde_json::json!({
                "market_id": position.market_id,
                "subaccount_id": position.subaccount_id,
                "is_long": is_long,
                "liquidation_price": liquidation_price,
                "mark_price": mark_price,
                "quantity": quantity.to_string(),
                "entry_price": entry_price.to_string(),
                "margin": margin.to_string(),
            });

            // Legacy Redis publish for backward compatibility
            conn.publish::<_, _, ()>("liquidation_alerts", alert_data.to_string())?;

            // Publish through HPC Redis PubSub
            if let Some(pubsub) = &self.pubsub {
                let liquidation_event = pubsub.create_liquidation_alert(
                    &position.market_id,
                    &position.subaccount_id,
                    alert_data,
                );

                // Fixed: Use direct publish for liquidation events (higher priority)
                if let Err(e) = pubsub.publish_event(liquidation_event).await {
                    warn!("Failed to publish liquidation alert: {}", e);
                }
            }

            info!(
                "Liquidatable position: market={}, subaccount={}, liq_price={}, mark_price={}",
                position.market_id, position.subaccount_id, liquidation_price, mark_price
            );
        } else {
            conn.srem::<_, _, ()>(
                "liquidatable_positions",
                format!("{}:{}", position.market_id, position.subaccount_id),
            )?;
        }

        info!("DEBUG-27: Position successfully processed and stored in Redis");

        Ok(())
    }
    // Process non-market messages
    async fn process_non_market_message(
        &self,
        message: &KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let block_height = message.block_height;
        let timestamp = message.block_time;
        let msg_type = &message.message_type;

        info!(
            "DEBUG-28: Processing non-market message type {:?} at block {}",
            msg_type, block_height
        );

        // Direct handling based on message type instead of relying on payload variant
        if *msg_type == MessageType::ExchangePosition {
            // Position messages need special handling
            if let KafkaPayload::ExchangePositions(positions) = &message.payload {
                info!(
                    "DEBUG-29: Processing {} positions from message",
                    positions.len()
                );
                // Process positions
                for (i, position) in positions.iter().enumerate() {
                    info!(
                        "DEBUG-30: Processing position {} of {} for market={}, subaccount={}",
                        i + 1,
                        positions.len(),
                        position.market_id,
                        position.subaccount_id
                    );
                    if let Err(e) = self
                        .process_position(position, block_height, timestamp)
                        .await
                    {
                        error!(
                            "DEBUG-31: Error processing position {} of {}: {}",
                            i + 1,
                            positions.len(),
                            e
                        );
                    }
                }
                info!("DEBUG-32: Finished processing all positions");
                return Ok(());
            } else {
                // Fallback: try manual deserialization if payload variant doesn't match
                error!("Message type is ExchangePosition but payload isn't ExchangePositions!");

                // Print the raw payload for debugging
                info!(
                    "Raw payload for position message: {}",
                    serde_json::to_string(&message.payload).unwrap_or_default()
                );

                // Attempt manual deserialization
                match serde_json::from_value::<Vec<PositionPayload>>(
                    serde_json::to_value(&message.payload).unwrap_or_default(),
                ) {
                    Ok(positions) => {
                        info!(
                            "DEBUG-29: Manually deserialized {} positions from message",
                            positions.len()
                        );
                        for (i, position) in positions.iter().enumerate() {
                            info!("DEBUG-30: Processing position {} of {} for market={}, subaccount={}", 
                             i+1, positions.len(), position.market_id, position.subaccount_id);
                            if let Err(e) = self
                                .process_position(position, block_height, timestamp)
                                .await
                            {
                                error!(
                                    "DEBUG-31: Error processing position {} of {}: {}",
                                    i + 1,
                                    positions.len(),
                                    e
                                );
                            }
                        }
                        info!("DEBUG-32: Finished processing all positions");
                    }
                    Err(e) => {
                        error!("Failed to manually deserialize position data: {}", e);
                    }
                }
                return Ok(());
            }
        }

        // Continue with previous logic for other message types
        match &message.payload {
            KafkaPayload::DerivativeTrades(trades) => {
                info!("Processing {} derivative trades", trades.len());
                // Process trades
                if let Some(pubsub) = &self.pubsub {
                    let mut trade_events = Vec::with_capacity(trades.len());

                    for trade in trades {
                        let trade_data = serde_json::json!({
                            "market_id": trade.market_id,
                            "is_buy": trade.is_buy,
                            "execution_type": trade.execution_type,
                            "subaccount_id": trade.subaccount_id,
                            "execution_price": trade.position_delta.execution_price,
                            "execution_quantity": trade.position_delta.execution_quantity,
                            "fee": trade.fee,
                            "trade_id": trade.trade_id,
                            "timestamp": timestamp.to_string(),
                        });

                        let event = StreamEvent {
                            event_type: EventType::TradeUpdate,
                            timestamp,
                            payload: trade_data,
                        };

                        trade_events.push(event);
                    }

                    // Batch publish trades
                    if !trade_events.is_empty() {
                        if let Err(e) = pubsub.publish_events_batch(trade_events).await {
                            warn!("Failed to publish trade updates batch: {}", e);
                        }
                    }
                }
            }
            KafkaPayload::StreamDerivativeOrderbooks(orderbooks) => {
                info!("Processing {} derivative orderbooks", orderbooks.len());
                // Process orderbooks
                if let Some(pubsub) = &self.pubsub {
                    let mut orderbook_events = Vec::with_capacity(orderbooks.len());

                    for orderbook in orderbooks {
                        let orderbook_data = serde_json::json!({
                            "market_id": orderbook.market_id,
                            "sequence": orderbook.sequence,
                            "bids": orderbook.buy_levels,
                            "asks": orderbook.sell_levels,
                            "timestamp": timestamp.to_string(),
                        });

                        let event = StreamEvent {
                            event_type: EventType::OrderbookUpdate,
                            timestamp,
                            payload: orderbook_data,
                        };

                        orderbook_events.push(event);
                    }

                    // Batch publish orderbooks
                    if !orderbook_events.is_empty() {
                        if let Err(e) = pubsub.publish_events_batch(orderbook_events).await {
                            warn!("Failed to publish orderbook updates batch: {}", e);
                        }
                    }
                }
            }
            _ => {
                info!(
                    "DEBUG-33: Skipping unsupported message type: {:?}",
                    msg_type
                );
            }
        }

        Ok(())
    }
}
#[async_trait]
impl MessageProcessor for RedisProcessor {
    async fn process_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // DEBUG-1: Log message type and phase at entry
        let phase = *self.phase.lock().await;
        // Use a reference to message_type instead of moving it
        let msg_type = &message.message_type;
        info!(
            "DEBUG-1: Processing message type {:?} with phase {:?}",
            msg_type, phase
        );
        info!(
            "Raw message payload: {}",
            serde_json::to_string(&message.payload).unwrap_or_default()
        );

        match msg_type {
            MessageType::DerivativeMarket => {
                // Process market messages regardless of phase
                if let KafkaPayload::DerivativeMarkets(markets) = &message.payload {
                    info!(
                        "Processing {} derivative markets at block {}",
                        markets.len(),
                        message.block_height
                    );

                    // Register all markets we need to process
                    {
                        let mut market_ids = self.market_ids.lock().await;
                        for market in markets {
                            market_ids.insert(market.market_id.clone());
                        }
                        info!(
                            "BREAKPOINT 2: Added {} markets to pending, total pending: {}",
                            markets.len(),
                            market_ids.len()
                        );

                        // DEBUG-2: Log all market IDs we're tracking
                        info!("DEBUG-2: Current pending market IDs: {:?}", market_ids);
                    }

                    // Process each market
                    for market in markets {
                        // DEBUG-3: Log before processing each market
                        info!("DEBUG-3: About to process market {}", market.market_id);

                        if let Err(e) = self
                            .process_derivative_market(
                                market,
                                message.block_height,
                                message.block_time,
                            )
                            .await
                        {
                            error!("Error processing derivative market: {}", e);
                        }

                        // DEBUG-4: Check if this market is still in pending
                        {
                            let market_ids = self.market_ids.lock().await;
                            info!(
                                "DEBUG-4: After processing market {}, still in pending: {}",
                                market.market_id,
                                market_ids.contains(&market.market_id)
                            );
                        }
                    }
                } else {
                    error!("Received DerivativeMarket message type but payload is not DerivativeMarkets");
                }
            }
            _ => {
                // For all other message types (including positions)
                match phase {
                    ProcessingPhase::Markets => {
                        // In Markets phase, defer non-market messages
                        info!(
                            "Deferring message type {:?} at block {} (in Markets phase)",
                            msg_type, message.block_height
                        );
                        let mut deferred = self.deferred_messages.lock().await;
                        deferred.push(message);
                        info!(
                            "DEBUG-6: Deferred message, queue now has {} messages",
                            deferred.len()
                        );
                    }
                    ProcessingPhase::Others => {
                        // In Others phase, process non-market messages
                        info!(
                            "Processing message type {:?} at block {}",
                            msg_type, message.block_height
                        );

                        // Add special debug for positions
                        if *msg_type == MessageType::ExchangePosition {
                            if let KafkaPayload::ExchangePositions(positions) = &message.payload {
                                info!(
                                    "DEBUG-5: Received {} positions at block {}",
                                    positions.len(),
                                    message.block_height
                                );
                            }
                        }

                        if let Err(e) = self.process_non_market_message(&message).await {
                            error!("DEBUG-7: Error processing message: {}", e);
                            return Err(e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
