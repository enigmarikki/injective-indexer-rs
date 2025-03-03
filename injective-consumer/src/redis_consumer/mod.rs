use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, KafkaPayload, PositionData};
use crate::compute::{calculate_liquidation_price, is_liquidatable};
use redis::{Client, Commands, Connection, RedisResult};
use std::error::Error;
use std::sync::Mutex;
use log::{info, error, warn};

use async_trait::async_trait;

pub struct RedisProcessor {
    client: Client,
    connection: Mutex<Connection>,
}

impl RedisProcessor {
    pub fn new(redis_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        let connection = client.get_connection()?;
        
        Ok(RedisProcessor {
            client,
            connection: Mutex::new(connection),
        })
    }
    
    fn process_derivative_market(&self, market: &crate::models::DerivativeMarketPayload, block_height: i64, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = match self.connection.lock() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to lock Redis connection: {}", e);
                return Err("Failed to lock Redis connection".into());
            }
        };
        
        // Extract cumulative funding from perpetual info
        let cumulative_funding = if let Some(perpetual_info) = &market.perpetual_info {
            perpetual_info.funding_info.cumulative_funding.parse::<f64>().unwrap_or_else(|e| {
                warn!("Invalid cumulative_funding format: {}. Error: {}", 
                     perpetual_info.funding_info.cumulative_funding, e);
                0.0
            })
        } else {
            0.0
        };
        
        // Parse other market data
        let mark_price = market.mark_price.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid mark_price format: {}. Error: {}", market.mark_price, e);
            0.0
        });
        
        let maintenance_margin_ratio = market.maintenance_margin_ratio.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid maintenance_margin_ratio format: {}. Error: {}", 
                 market.maintenance_margin_ratio, e);
            0.05
        });
        
        // Skip markets with invalid data
        if mark_price <= 0.0 || maintenance_margin_ratio <= 0.0 {
            warn!("Skipping market with invalid data: market={}, mark_price={}, maintenance_margin_ratio={}", 
                 market.market_id, mark_price, maintenance_margin_ratio);
            return Ok(());
        }
        
        // Store market data in Redis using a pipeline for efficiency
        let key = format!("market:derivative:{}", market.market_id);
        
        // Use a pipeline to efficiently execute multiple Redis commands
        let pipe_result: RedisResult<()> = redis::pipe()
            .hset(&key, "ticker", &market.ticker)
            .hset(&key, "mark_price", &market.mark_price)
            .hset(&key, "maintenance_margin_ratio", &market.maintenance_margin_ratio)
            .hset(&key, "cumulative_funding", cumulative_funding.to_string())
            .hset(&key, "block_height", block_height.to_string())
            .hset(&key, "timestamp", timestamp.to_string())
            .sadd("markets:derivative", &market.market_id)
            .query(&mut *conn);
        
        if let Err(e) = pipe_result {
            error!("Failed to store market data in Redis: {}", e);
            return Err(Box::new(e));
        }
        
        // Update all positions for this market with new liquidation prices
        let positions_key = format!("positions:market:{}", market.market_id);
        let subaccount_ids: Vec<String> = match conn.smembers(&positions_key) {
            Ok(ids) => ids,
            Err(e) => {
                error!("Failed to get subaccounts for market {}: {}", market.market_id, e);
                return Err(Box::new(e));
            }
        };
        
        for subaccount_id in subaccount_ids {
            let position_key = format!("position:{}:{}", market.market_id, subaccount_id);
            
            // Get position data with safer error handling
            let position_data = self.get_position_data_from_redis(&mut conn, &position_key)?;
            
            // Skip positions with invalid data
            if position_data.quantity <= 0.0 || position_data.entry_price <= 0.0 {
                warn!("Skipping position with invalid data: market={}, subaccount={}, quantity={}, entry_price={}", 
                     market.market_id, subaccount_id, position_data.quantity, position_data.entry_price);
                continue;
            }
            
            // Recalculate liquidation price
            let liquidation_price = calculate_liquidation_price(
                position_data.is_long, 
                position_data.entry_price, 
                position_data.margin, 
                position_data.quantity, 
                maintenance_margin_ratio, 
                cumulative_funding, 
                position_data.cumulative_funding_entry
            );
            
            // Ensure liquidation price is valid
            if !liquidation_price.is_finite() || liquidation_price <= 0.0 {
                warn!("Calculated invalid liquidation price {} for position {}/{}", 
                     liquidation_price, market.market_id, subaccount_id);
                continue;
            }
            
            // Update the liquidation price
            if let Err(e) = conn.hset::<_, _, _, ()>(&position_key, "liquidation_price", liquidation_price.to_string()) {
                error!("Failed to update liquidation price: {}", e);
                continue;
            }
            
            // Check if liquidatable
            let is_liquidatable = is_liquidatable(position_data.is_long, liquidation_price, mark_price);
            if let Err(e) = conn.hset::<_, _, _, ()>(&position_key, "is_liquidatable", is_liquidatable.to_string()) {
                error!("Failed to update is_liquidatable flag: {}", e);
                continue;
            }
            
            // Update liquidatable positions set
            if is_liquidatable {
                if let Err(e) = conn.sadd::<_, _, ()>("liquidatable_positions", format!("{}:{}", market.market_id, subaccount_id)) {
                    error!("Failed to add to liquidatable_positions set: {}", e);
                }
                
                // Publish alert
                let alert_data = serde_json::json!({
                    "market_id": market.market_id,
                    "subaccount_id": subaccount_id,
                    "is_long": position_data.is_long,
                    "liquidation_price": liquidation_price,
                    "mark_price": mark_price
                });
                
                if let Err(e) = conn.publish::<_, _, ()>("liquidation_alerts", alert_data.to_string()) {
                    error!("Failed to publish liquidation alert: {}", e);
                } else {
                    info!("Liquidatable position: market={}, subaccount={}, liq_price={}, mark_price={}", 
                          market.market_id, subaccount_id, liquidation_price, mark_price);
                }
            } else {
                if let Err(e) = conn.srem::<_, _, ()>("liquidatable_positions", format!("{}:{}", market.market_id, subaccount_id)) {
                    error!("Failed to remove from liquidatable_positions set: {}", e);
                }
            }
            
            // Emit updated position
            if let Err(e) = self.emit_position_update_internal(&mut conn, &market.market_id, &subaccount_id, position_data.is_long, 
                                                      position_data.quantity.to_string(), position_data.entry_price.to_string(), 
                                                      position_data.margin.to_string(), position_data.cumulative_funding_entry.to_string(), 
                                                      liquidation_price) {
                error!("Failed to emit position update: {}", e);
            }
        }
        
        Ok(())
    }
    
    fn process_position(&self, position: &crate::models::PositionPayload, block_height: i64, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = match self.connection.lock() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to lock Redis connection: {}", e);
                return Err("Failed to lock Redis connection".into());
            }
        };
        
        // Parse position data safely
        let is_long = position.is_long;
        let quantity = position.quantity.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid quantity format for position {}/{}: {}. Error: {}", 
                 position.market_id, position.subaccount_id, position.quantity, e);
            0.0
        });
        let entry_price = position.entry_price.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid entry_price format for position {}/{}: {}. Error: {}", 
                 position.market_id, position.subaccount_id, position.entry_price, e);
            0.0
        });
        let margin = position.margin.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid margin format for position {}/{}: {}. Error: {}", 
                 position.market_id, position.subaccount_id, position.margin, e);
            0.0
        });
        let cumulative_funding_entry = position.cumulative_funding_entry.parse::<f64>().unwrap_or_else(|e| {
            warn!("Invalid cumulative_funding_entry format for position {}/{}: {}. Error: {}", 
                 position.market_id, position.subaccount_id, position.cumulative_funding_entry, e);
            0.0
        });
        
        // Skip invalid positions
        if quantity <= 0.0 || entry_price <= 0.0 {
            warn!("Skipping position with invalid data: market={}, subaccount={}, quantity={}, entry_price={}", 
                 position.market_id, position.subaccount_id, quantity, entry_price);
            return Ok(());
        }
        
        // Get market data for calculating liquidation price
        let market_key = format!("market:derivative:{}", position.market_id);
        
        // Safely retrieve market data with proper error handling
        let mark_price: f64 = match conn.hget::<_, _, Result<String, redis::RedisError>>(&market_key, "mark_price") {
            Ok(Ok(s)) => s.parse().unwrap_or_else(|_| {
                warn!("Invalid mark_price format for market {}", position.market_id);
                0.0
            }),
            Ok(Err(_)) | Err(_) => {
                warn!("Failed to retrieve mark_price for market {}", position.market_id);
                0.0
            }
        };
        
        let maintenance_margin_ratio: f64 = match conn.hget::<_, _, Result<String, redis::RedisError>>(&market_key, "maintenance_margin_ratio") {
            Ok(Ok(s)) => s.parse().unwrap_or_else(|_| {
                warn!("Invalid maintenance_margin_ratio format for market {}", position.market_id);
                0.05 // Default value
            }),
            Ok(Err(_)) | Err(_) => {
                warn!("Failed to retrieve maintenance_margin_ratio for market {}", position.market_id);
                0.05 // Default value
            }
        };
        
        let market_cumulative_funding: f64 = match conn.hget::<_, _, Result<String, redis::RedisError>>(&market_key, "cumulative_funding") {
            Ok(Ok(s)) => s.parse().unwrap_or_else(|_| {
                warn!("Invalid cumulative_funding format for market {}", position.market_id);
                0.0
            }),
            Ok(Err(_)) | Err(_) => {
                warn!("Failed to retrieve cumulative_funding for market {}", position.market_id);
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
            cumulative_funding_entry
        );
        
        // Ensure liquidation price is valid
        if !liquidation_price.is_finite() || liquidation_price <= 0.0 {
            warn!("Calculated invalid liquidation price {} for position {}/{}", 
                 liquidation_price, position.market_id, position.subaccount_id);
            return Ok(());
        }
        
        // Store position data
        let key = format!("position:{}:{}", position.market_id, position.subaccount_id);
        
        // Store data with error handling using pipeline
        let store_result = redis::pipe()
            .hset(&key, "is_long", position.is_long.to_string())
            .hset(&key, "quantity", &position.quantity)
            .hset(&key, "entry_price", &position.entry_price)
            .hset(&key, "margin", &position.margin)
            .hset(&key, "cumulative_funding_entry", &position.cumulative_funding_entry)
            .hset(&key, "liquidation_price", liquidation_price.to_string())
            .hset(&key, "block_height", block_height.to_string())
            .hset(&key, "timestamp", timestamp.to_string())
            .query::<()>(&mut *conn);
        
        if let Err(e) = store_result {
            error!("Failed to store position data in Redis: {}", e);
            return Err(Box::new(e));
        }
        
        // Add to position sets
        let positions_market_result = conn.sadd::<_, _, ()>(format!("positions:market:{}", position.market_id), &position.subaccount_id);
        if let Err(e) = positions_market_result {
            error!("Failed to add position to market set: {}", e);
        }
        
        let positions_subaccount_result = conn.sadd::<_, _, ()>(format!("positions:subaccount:{}", position.subaccount_id), &position.market_id);
        if let Err(e) = positions_subaccount_result {
            error!("Failed to add position to subaccount set: {}", e);
        }
        
        // Check if liquidatable
        let is_liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);
        let liquidatable_result = conn.hset::<_, _, _, ()>(&key, "is_liquidatable", is_liquidatable.to_string());
        if let Err(e) = liquidatable_result {
            error!("Failed to set liquidatable flag: {}", e);
        }
        
        // Update liquidatable positions set
        if is_liquidatable {
            let add_result = conn.sadd::<_, _, ()>("liquidatable_positions", format!("{}:{}", position.market_id, position.subaccount_id));
            if let Err(e) = add_result {
                error!("Failed to add to liquidatable positions set: {}", e);
            }
            
            // Publish alert
            let alert_data = serde_json::json!({
                "market_id": position.market_id,
                "subaccount_id": position.subaccount_id,
                "is_long": is_long,
                "liquidation_price": liquidation_price,
                "mark_price": mark_price
            });
            
            let publish_result = conn.publish::<_, _, ()>("liquidation_alerts", alert_data.to_string());
            if let Err(e) = publish_result {
                error!("Failed to publish liquidation alert: {}", e);
            } else {
                info!("Liquidatable position: market={}, subaccount={}, liq_price={}, mark_price={}", 
                      position.market_id, position.subaccount_id, liquidation_price, mark_price);
            }
        } else {
            let rem_result = conn.srem::<_, _, ()>("liquidatable_positions", format!("{}:{}", position.market_id, position.subaccount_id));
            if let Err(e) = rem_result {
                error!("Failed to remove from liquidatable positions set: {}", e);
            }
        }
        
        // Emit updated position with liquidation price
        if let Err(e) = self.emit_updated_position(position, liquidation_price) {
            error!("Failed to emit updated position: {}", e);
        }
        
        Ok(())
    }
    
    // Helper method to get position data from Redis
    fn get_position_data_from_redis(&self, conn: &mut Connection, position_key: &str) -> Result<PositionData, Box<dyn Error + Send + Sync>> {
        // Get position data
        let is_long_result: Result<String, redis::RedisError> = conn.hget(position_key, "is_long");
        let is_long = match is_long_result {
            Ok(s) => s.parse::<bool>().unwrap_or(false),
            Err(e) => {
                warn!("Failed to get is_long for position {}: {}", position_key, e);
                false
            }
        };
        
        let quantity_result: Result<String, redis::RedisError> = conn.hget(position_key, "quantity");
        let quantity = match quantity_result {
            Ok(s) => s.parse::<f64>().unwrap_or(0.0),
            Err(e) => {
                warn!("Failed to get quantity for position {}: {}", position_key, e);
                0.0
            }
        };
        
        let entry_price_result: Result<String, redis::RedisError> = conn.hget(position_key, "entry_price");
        let entry_price = match entry_price_result {
            Ok(s) => s.parse::<f64>().unwrap_or(0.0),
            Err(e) => {
                warn!("Failed to get entry_price for position {}: {}", position_key, e);
                0.0
            }
        };
        
        let margin_result: Result<String, redis::RedisError> = conn.hget(position_key, "margin");
        let margin = match margin_result {
            Ok(s) => s.parse::<f64>().unwrap_or(0.0),
            Err(e) => {
                warn!("Failed to get margin for position {}: {}", position_key, e);
                0.0
            }
        };
        
        let cumulative_funding_entry_result: Result<String, redis::RedisError> = conn.hget(position_key, "cumulative_funding_entry");
        let cumulative_funding_entry = match cumulative_funding_entry_result {
            Ok(s) => s.parse::<f64>().unwrap_or(0.0),
            Err(e) => {
                warn!("Failed to get cumulative_funding_entry for position {}: {}", position_key, e);
                0.0
            }
        };
        
        Ok(PositionData {
            is_long,
            quantity,
            entry_price,
            margin,
            cumulative_funding_entry,
        })
    }
    
    // Helper method to get all liquidatable positions
    pub fn get_liquidatable_positions(&self) -> Result<Vec<(String, String, f64, f64)>, Box<dyn Error + Send + Sync>> {
        let mut conn = match self.connection.lock() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to lock Redis connection: {}", e);
                return Err("Failed to lock Redis connection".into());
            }
        };
        
        // Get all liquidatable position keys
        let keys: Vec<String> = match conn.smembers("liquidatable_positions") {
            Ok(keys) => keys,
            Err(e) => {
                error!("Failed to get liquidatable positions: {}", e);
                return Err(Box::new(e));
            }
        };
        
        let mut result = Vec::with_capacity(keys.len());
        
        for key in keys {
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() == 2 {
                let market_id = parts[0].to_string();
                let subaccount_id = parts[1].to_string();
                
                let position_key = format!("position:{}:{}", market_id, subaccount_id);
                let market_key = format!("market:derivative:{}", market_id);
                
                // Get liquidation price and mark price
                let liquidation_price_result: Result<String, redis::RedisError> = conn.hget(&position_key, "liquidation_price");
                let mark_price_result: Result<String, redis::RedisError> = conn.hget(&market_key, "mark_price");
                
                if let (Ok(liquidation_price_str), Ok(mark_price_str)) = (liquidation_price_result, mark_price_result) {
                    let liquidation_price = liquidation_price_str.parse::<f64>().unwrap_or_else(|_| {
                        warn!("Invalid liquidation_price format for position {}/{}", market_id, subaccount_id);
                        0.0
                    });
                    
                    let mark_price = mark_price_str.parse::<f64>().unwrap_or_else(|_| {
                        warn!("Invalid mark_price format for market {}", market_id);
                        0.0
                    });
                    
                    // Skip invalid prices
                    if liquidation_price <= 0.0 || mark_price <= 0.0 {
                        warn!("Skipping position with invalid prices: market={}, subaccount={}, liquidation_price={}, mark_price={}", 
                             market_id, subaccount_id, liquidation_price, mark_price);
                        continue;
                    }
                    
                    result.push((market_id, subaccount_id, liquidation_price, mark_price));
                }
            }
        }
        
        Ok(result)
    }
    
    // New method to emit position update
    pub fn emit_position_update(&self, market_id: &str, subaccount_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = match self.connection.lock() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to lock Redis connection: {}", e);
                return Err("Failed to lock Redis connection".into());
            }
        };
        
        let position_key = format!("position:{}:{}", market_id, subaccount_id);
        
        // Get position data with proper error handling
        let is_long_result: Result<String, redis::RedisError> = conn.hget(&position_key, "is_long");
        let is_long = match is_long_result {
            Ok(s) => s.parse::<bool>().unwrap_or(false),
            Err(e) => {
                warn!("Failed to get is_long for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        let quantity_result: Result<String, redis::RedisError> = conn.hget(&position_key, "quantity");
        let quantity = match quantity_result {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get quantity for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        let entry_price_result: Result<String, redis::RedisError> = conn.hget(&position_key, "entry_price");
        let entry_price = match entry_price_result {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get entry_price for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        let margin_result: Result<String, redis::RedisError> = conn.hget(&position_key, "margin");
        let margin = match margin_result {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get margin for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        let cumulative_funding_entry_result: Result<String, redis::RedisError> = conn.hget(&position_key, "cumulative_funding_entry");
        let cumulative_funding_entry = match cumulative_funding_entry_result {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get cumulative_funding_entry for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        let liquidation_price_result: Result<String, redis::RedisError> = conn.hget(&position_key, "liquidation_price");
        let liquidation_price = match liquidation_price_result {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get liquidation_price for position {}/{}: {}", market_id, subaccount_id, e);
                return Err(Box::new(e));
            }
        };
        
        // Create position payload with liquidation price
        let position_payload = serde_json::json!({
            "market_id": market_id,
            "subaccount_id": subaccount_id,
            "is_long": is_long,
            "quantity": quantity,
            "entry_price": entry_price,
            "margin": margin,
            "cumulative_funding_entry": cumulative_funding_entry,
            "liquidation_price": liquidation_price
        });
        
        // Publish position update
        let publish_result = conn.publish::<_, _, ()>("position_updates", position_payload.to_string());
        
        if let Err(e) = publish_result {
            error!("Failed to publish position update: {}", e);
            return Err(Box::new(e));
        }
        
        info!("Emitted position update for market={}, subaccount={} with liquidation_price={}", 
              market_id, subaccount_id, liquidation_price);
        
        Ok(())
    }
    
    // Helper method for emitting position updates from internal methods
    fn emit_position_update_internal(
        &self, 
        conn: &mut Connection, 
        market_id: &str, 
        subaccount_id: &str, 
        is_long: bool,
        quantity: String,
        entry_price: String,
        margin: String,
        cumulative_funding_entry: String,
        liquidation_price: f64
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create position payload with liquidation price
        let position_payload = serde_json::json!({
            "market_id": market_id,
            "subaccount_id": subaccount_id,
            "is_long": is_long,
            "quantity": quantity,
            "entry_price": entry_price,
            "margin": margin,
            "cumulative_funding_entry": cumulative_funding_entry,
            "liquidation_price": liquidation_price.to_string()
        });
        
        // Publish position update
        let publish_result = conn.publish::<_, _, ()>("position_updates", position_payload.to_string());
        
        if let Err(e) = publish_result {
            error!("Failed to publish position update: {}", e);
            return Err(Box::new(e));
        }
        
        info!("Emitted position update for market={}, subaccount={} with liquidation_price={}", 
              market_id, subaccount_id, liquidation_price);
        
        Ok(())
    }

    // Call this after liquidation price calculation in process_position
    fn emit_updated_position(&self, position: &crate::models::PositionPayload, liquidation_price: f64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = match self.connection.lock() {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to lock Redis connection: {}", e);
                return Err("Failed to lock Redis connection".into());
            }
        };
        
        let position_payload = serde_json::json!({
            "market_id": position.market_id,
            "subaccount_id": position.subaccount_id,
            "is_long": position.is_long,
            "quantity": position.quantity,
            "entry_price": position.entry_price,
            "margin": position.margin,
            "cumulative_funding_entry": position.cumulative_funding_entry,
            "liquidation_price": liquidation_price.to_string()
        });
        
        let publish_result = conn.publish::<_, _, ()>("position_updates", position_payload.to_string());
        
        if let Err(e) = publish_result {
            error!("Failed to publish position update: {}", e);
            return Err(Box::new(e));
        }
        
        info!("Emitted position update with calculated liquidation_price={}", liquidation_price);
        
        Ok(())
    }
}

#[async_trait]
impl MessageProcessor for RedisProcessor {
    async fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        let block_height = message.block_height as i64;
        let timestamp = message.block_time as i64;
        
        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                for market in markets {
                    if let Err(e) = self.process_derivative_market(market, block_height, timestamp) {
                        error!("Redis: Error processing derivative market: {}", e);
                    }
                }
            },
            KafkaPayload::Positions(positions) => {
                for position in positions {
                    if let Err(e) = self.process_position(position, block_height, timestamp) {
                        error!("Redis: Error processing position: {}", e);
                    }
                }
            },
            // Handle other message types as needed
            _ => {
                // info!("Redis: Skipping unhandled message type");
            }
        }
        
        Ok(())
    }
}