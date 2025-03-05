use crate::compute::{calculate_liquidation_price, is_liquidatable};
use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, KafkaPayload};
use crate::pubsub::{EventType, RedisPubSubService, StreamEvent};
use async_trait::async_trait;
use log::{error, info, warn};
use redis::{Client, Commands, Connection};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RedisProcessor {
    _client: Client,
    connection: Arc<Mutex<Connection>>,
    // HPC Redis PubSub service
    pubsub: Option<Arc<RedisPubSubService>>,
}

impl RedisProcessor {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        let connection = client.get_connection()?;

        Ok(RedisProcessor {
            _client: client,
            connection: Arc::new(Mutex::new(connection)),
            pubsub: None,
        })
    }

    // Add a method to set the PubSub service
    pub fn with_pubsub(mut self, pubsub: Arc<RedisPubSubService>) -> Self {
        self.pubsub = Some(pubsub);
        self
    }

    async fn process_derivative_market(
        &self,
        market: &crate::models::DerivativeMarketPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
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

        // Add to markets set
        conn.sadd::<_, _, ()>("markets:derivative", &market.market_id)?;

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

    // Position processing method with PubSub integration
    async fn process_position(
        &self,
        position: &crate::models::PositionPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().await;

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
                "Invalid position data for market {} subaccount {}, skipping",
                position.market_id, position.subaccount_id
            );
            return Ok(());
        }

        // Get market data for calculating liquidation price
        let market_key = format!("market:derivative:{}", position.market_id);

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

        Ok(())
    }

}
#[async_trait]
impl MessageProcessor for RedisProcessor {
    async fn process_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let block_height = message.block_height;
        let timestamp = message.block_time;

        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                for market in markets {
                    if let Err(e) = self
                        .process_derivative_market(market, block_height, timestamp)
                        .await
                    {
                        error!("Redis: Error processing derivative market: {}", e);
                    }
                }
            }
            KafkaPayload::ExchangePositions(positions) => {
                // Create a batch of position events for efficient publishing
                if let Some(pubsub) = &self.pubsub {
                    let mut position_events = Vec::with_capacity(positions.len());
                    let mut liquidation_events = Vec::new();
                    
                    // Lock connection once for efficient batch processing
                    let mut conn = self.connection.lock().await;
                    
                    // First, collect all position events for batch publishing
                    for position in positions {
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
                                "Invalid position data for market {} subaccount {}, skipping batch event",
                                position.market_id, position.subaccount_id
                            );
                            continue;
                        }
                        
                        // Get market data for calculating liquidation price
                        let market_key = format!("market:derivative:{}", position.market_id);

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
                        
                        // Check if liquidatable
                        let is_liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);
                        
                        // Create position update data
                        let position_data = serde_json::json!({
                            "is_long": is_long,
                            "quantity": quantity.to_string(),
                            "entry_price": entry_price.to_string(),
                            "margin": margin.to_string(),
                            "liquidation_price": liquidation_price.to_string(),
                            "cumulative_funding_entry": cumulative_funding_entry.to_string(),
                            "mark_price": mark_price.to_string(),
                            "is_liquidatable": is_liquidatable,
                            "maintenance_margin_ratio": maintenance_margin_ratio.to_string(),
                            "market_cumulative_funding": market_cumulative_funding.to_string(),
                            "block_height": block_height.to_string(),
                            "timestamp": timestamp.to_string(),
                        });
                        
                        // Create position event for batch publishing
                        let position_event = StreamEvent {
                            event_type: EventType::PositionUpdate,
                            timestamp,
                            payload: serde_json::json!({
                                "market_id": position.market_id,
                                "subaccount_id": position.subaccount_id,
                                "data": position_data
                            }),
                        };
                        
                        // Add to position events batch
                        position_events.push(position_event);
                        
                        // If liquidatable, also create a liquidation alert event
                        if is_liquidatable {
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
                            
                            // Create liquidation alert event
                            let liquidation_event = pubsub.create_liquidation_alert(
                                &position.market_id,
                                &position.subaccount_id,
                                alert_data,
                            );
                            
                            // Add to liquidation events batch
                            liquidation_events.push(liquidation_event);
                        }
                    }
                    
                    // Drop the connection lock before async operations
                    drop(conn);
                    
                    // Batch publish position events
                    if !position_events.is_empty() {
                        let pubsub_clone = pubsub.clone();
                        tokio::spawn(async move {
                            if let Err(e) = pubsub_clone.publish_events_batch(position_events).await {
                                warn!("Failed to publish position updates batch: {}", e);
                            }
                        });
                    }
                    else {
                        warn!("Empty position events!")
                    }
                    
                    // Batch publish liquidation events if any
                    if !liquidation_events.is_empty() {
                        let pubsub_clone = pubsub.clone();
                        tokio::spawn(async move {
                            if let Err(e) = pubsub_clone.publish_events_batch(liquidation_events).await {
                                warn!("Failed to publish liquidation alerts batch: {}", e);
                            }
                        });
                    }
                }
                
                
            }
            // Handle trade updates with PubSub
            KafkaPayload::DerivativeTrades(trades) => {
                if let Some(pubsub) = &self.pubsub {
                    // Batch trades for efficient publishing
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
            // Handle orderbook updates with PubSub
            KafkaPayload::StreamDerivativeOrderbooks(orderbooks) => {
                if let Some(pubsub) = &self.pubsub {
                    // Batch orderbook updates for efficient publishing
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
            // Handle other message types as needed
            _ => {
                // Skip unhandled message types
            }
        }

        Ok(())
    }
}