use crate::compute::{calculate_liquidation_price, is_liquidatable};
use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, KafkaPayload};
use async_trait::async_trait;
use log::{error, info, warn};
use redis::{Client, Commands, Connection};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct RedisProcessor {
    client: Client,
    // Replace simple Mutex with Tokio's async Mutex for better performance in async context
    connection: Arc<Mutex<Connection>>,
}

impl RedisProcessor {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        let connection = client.get_connection()?;

        Ok(RedisProcessor {
            client,
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    async fn process_derivative_market(
        &self,
        market: &crate::models::DerivativeMarketPayload,
        block_height: u64,
        timestamp: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().await;

        // Extract cumulative funding from perpetual info
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

        // Update all positions for this market with new liquidation prices
        let positions_key = format!("positions:market:{}", market.market_id);
        let subaccount_ids: Vec<String> = conn.smembers(&positions_key)?;

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

            // Update liquidatable positions set
            if is_liquidatable {
                conn.sadd::<_, _, ()>(
                    "liquidatable_positions",
                    format!("{}:{}", market.market_id, subaccount_id),
                )?;

                // Publish alert
                let alert_data = serde_json::json!({
                    "market_id": market.market_id,
                    "subaccount_id": subaccount_id,
                    "is_long": is_long,
                    "liquidation_price": liquidation_price,
                    "mark_price": mark_price
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

        Ok(())
    }

    // Change the process_position method to async as well
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

        // Update liquidatable positions set
        if is_liquidatable {
            conn.sadd::<_, _, ()>(
                "liquidatable_positions",
                format!("{}:{}", position.market_id, position.subaccount_id),
            )?;

            // Publish alert
            let alert_data = serde_json::json!({
                "market_id": position.market_id,
                "subaccount_id": position.subaccount_id,
                "is_long": is_long,
                "liquidation_price": liquidation_price,
                "mark_price": mark_price
            });

            conn.publish::<_, _, ()>("liquidation_alerts", alert_data.to_string())?;

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

    // Helper method to get all liquidatable positions
    pub async fn get_liquidatable_positions(
        &self,
    ) -> Result<Vec<(String, String, f64, f64)>, Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().await;

        let keys: Vec<String> = conn.smembers("liquidatable_positions")?;
        let mut result = Vec::new();

        for key in keys {
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() == 2 {
                let market_id = parts[0].to_string();
                let subaccount_id = parts[1].to_string();

                let position_key = format!("position:{}:{}", market_id, subaccount_id);
                let market_key = format!("market:derivative:{}", market_id);

                let liquidation_price =
                    match conn.hget::<_, _, Option<String>>(&position_key, "liquidation_price") {
                        Ok(Some(value)) => value.parse::<f64>().unwrap_or(0.0),
                        _ => continue, // Skip if missing
                    };

                let mark_price = match conn.hget::<_, _, Option<String>>(&market_key, "mark_price")
                {
                    Ok(Some(value)) => value.parse::<f64>().unwrap_or(0.0),
                    _ => continue, // Skip if missing
                };

                result.push((market_id, subaccount_id, liquidation_price, mark_price));
            }
        }

        Ok(result)
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
                for position in positions {
                    if let Err(e) = self
                        .process_position(position, block_height, timestamp)
                        .await
                    {
                        error!("Redis: Error processing position: {}", e);
                    }
                }
            }
            // Handle other message types as needed
            _ => {
                // info!("Redis: Skipping unhandled message type");
            }
        }

        Ok(())
    }
}
