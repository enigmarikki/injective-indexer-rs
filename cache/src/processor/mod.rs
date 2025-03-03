// src/redis_processor/mod.rs - Redis implementation of MessageProcessor
use injective_consumer::{KafkaMessage, MessageProcessor};
use injective_consumer::models::{DerivativeMarketPayload, ExchangeBalancePayload, OrderbookL3Payload, PositionPayload};
use log::{error, info};
use redis::{AsyncCommands, Client, RedisResult};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub struct RedisMessageProcessor {
    client: Arc<Mutex<Client>>,
    ttl: Option<Duration>,
}

    // Helper to store orderbook in Redis
    async fn store_orderbook(&self, orderbook: &OrderbookL3Payload) -> Result<(), Box<dyn Error + Send + Sync>> {
        let client = self.client.lock().await;
        let mut conn = client.get_async_connection().await?;
        
        // Store the full orderbook as JSON
        let orderbook_key = format!("orderbook:{}:data", orderbook.market_id);
        let json = serde_json::to_string(orderbook)?;
        
        if let Some(ttl) = self.ttl {
            conn.set_ex(&orderbook_key, &json, ttl.as_secs() as usize).await?;
        } else {
            conn.set(&orderbook_key, &json).await?;
        }
        
        // Update latest timestamp
        conn.set(
            format!("orderbook:{}:timestamp", orderbook.market_id),
            orderbook.timestamp.to_string()
        ).await?;
        
        // Also store a summary with just best bid/ask
        if !orderbook.bids.is_empty() && !orderbook.asks.is_empty() {
            // Sort bids (desc) and asks (asc)
            let mut bids = orderbook.bids.clone();
            let mut asks = orderbook.asks.clone();
            
            bids.sort_by(|a, b| {
                let a_price = a.price.parse::<f64>().unwrap_or(0.0);
                let b_price = b.price.parse::<f64>().unwrap_or(0.0);
                b_price.partial_cmp(&a_price).unwrap()
            });
            
            asks.sort_by(|a, b| {
                let a_price = a.price.parse::<f64>().unwrap_or(0.0);
                let b_price = b.price.parse::<f64>().unwrap_or(0.0);
                a_price.partial_cmp(&b_price).unwrap()
            });
            
            // Get best bid and ask
            if let Some(best_bid) = bids.first() {
                let best_bid_key = format!("orderbook:{}:bestbid", orderbook.market_id);
                conn.set(&best_bid_key, &best_bid.price).await?;
            }
            
            if let Some(best_ask) = asks.first() {
                let best_ask_key = format!("orderbook:{}:bestask", orderbook.market_id);
                conn.set(&best_ask_key, &best_ask.price).await?;
            }
            
            // Calculate mid price
            if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
                let bid_price = best_bid.price.parse::<f64>().unwrap_or(0.0);
                let ask_price = best_ask.price.parse::<f64>().unwrap_or(0.0);
                
                if bid_price > 0.0 && ask_price > 0.0 {
                    let mid_price = (bid_price + ask_price) / 2.0;
                    let mid_price_key = format!("orderbook:{}:midprice", orderbook.market_id);
                    conn.set(&mid_price_key, mid_price.to_string()).await?;
                }
            }
        }
        
        Ok(())
    }

impl MessageProcessor for RedisMessageProcessor {
    fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Clone Arc for the async block
        let processor = self.clone();
        let timestamp = message.block_time;
        
        // Process different message types
        match message.payload {
            injective_consumer_base::models::KafkaPayload::DerivativeMarkets(markets) => {
                tokio::spawn(async move {
                    for market in markets {
                        if let Err(e) = processor.store_derivative_market(&market, timestamp).await {
                            error!("Failed to store derivative market in Redis: {}", e);
                        } else {
                            info!("Stored derivative market in Redis: {}", market.ticker);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::Positions(positions) => {
                tokio::spawn(async move {
                    for position in positions {
                        if let Err(e) = processor.store_position(&position, timestamp).await {
                            error!("Failed to store position in Redis: {}", e);
                        } else {
                            info!("Stored position in Redis for market: {}", position.market_id);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::ExchangeBalances(balances) => {
                tokio::spawn(async move {
                    for balance in balances {
                        if let Err(e) = processor.store_exchange_balance(&balance, timestamp).await {
                            error!("Failed to store exchange balance in Redis: {}", e);
                        } else {
                            info!("Stored exchange balance in Redis for subaccount: {}", balance.subaccount_id);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::DerivativeL3Orderbooks(orderbooks) => {
                tokio::spawn(async move {
                    for orderbook in orderbooks {
                        if let Err(e) = processor.store_orderbook(&orderbook).await {
                            error!("Failed to store orderbook in Redis: {}", e);
                        } else {
                            info!("Stored orderbook in Redis for market: {}", orderbook.market_id);
                        }
                    }
                });
            },
            // Add handlers for other message types as needed
            _ => {
                // For other message types that we're not handling yet
                info!("Received message type that's not being persisted to Redis");
            }
        }
        
        Ok(())
    }
}

// Implement Clone for RedisMessageProcessor
impl Clone for RedisMessageProcessor {
    fn clone(&self) -> Self {
        RedisMessageProcessor {
            client: self.client.clone(),
            ttl: self.ttl,
        }
    }
}

impl RedisMessageProcessor {
    pub fn new(redis_url: &str, ttl_seconds: Option<u64>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = Client::open(redis_url)?;
        
        // Test connection
        let mut conn = client.get_connection()?;
        redis::cmd("PING").query::<String>(&mut conn)?;
        
        info!("Connected to Redis at {}", redis_url);
        
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            ttl: ttl_seconds.map(Duration::from_secs),
        })
    }
    
    // Helper function to set value with optional TTL
    async fn set_with_ttl(&self, key: &str, value: &str) -> RedisResult<()> {
        let client = self.client.lock().await;
        let mut conn = client.get_async_connection().await?;
        
        if let Some(ttl) = self.ttl {
            conn.set_ex(key, value, ttl.as_secs() as usize).await?;
        } else {
            conn.set(key, value).await?;
        }
        
        Ok(())
    }
    
    // Helper to store a market in Redis
    async fn store_derivative_market(&self, market: &DerivativeMarketPayload, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Store market data as a hash
        let market_key = format!("market:{}:data", market.market_id);
        let json = serde_json::to_string(market)?;
        
        // Set the market data
        self.set_with_ttl(&market_key, &json).await?;
        
        // Also maintain a sorted set of all markets by timestamp
        let client = self.client.lock().await;
        let mut conn = client.get_async_connection().await?;
        conn.zadd("markets:derivative", &market.market_id, timestamp).await?;
        
        Ok(())
    }
    
    // Helper to store a position in Redis
    async fn store_position(&self, position: &PositionPayload, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create a composite key with market_id and subaccount_id
        let position_key = format!("position:{}:{}:data", position.market_id, position.subaccount_id);
        let json = serde_json::to_string(position)?;
        
        // Set the position data
        self.set_with_ttl(&position_key, &json).await?;
        
        // Also maintain a sorted set of all positions by timestamp for each market
        let client = self.client.lock().await;
        let mut conn = client.get_async_connection().await?;
        
        // Add to market positions index
        conn.zadd(
            format!("market:{}:positions", position.market_id), 
            &position.subaccount_id, 
            timestamp
        ).await?;
        
        // Add to subaccount positions index
        conn.zadd(
            format!("subaccount:{}:positions", position.subaccount_id), 
            &position.market_id, 
            timestamp
        ).await?;
        
        Ok(())
    }
    
    // Helper to store exchange balance in Redis
    async fn store_exchange_balance(&self, balance: &ExchangeBalancePayload, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create a composite key with subaccount_id and denom
        let balance_key = format!("balance:{}:{}:data", balance.subaccount_id, balance.denom);
        let json = serde_json::to_string(balance)?;
        
        // Set the balance data
        self.set_with_ttl(&balance_key, &json).await?;
        
        // Also maintain a sorted set of all balances by timestamp for each subaccount
        let client = self.client.lock().await;
        let mut conn = client.get_async_connection().await?;
        
        // Add to subaccount balances index
        conn.zadd(
            format!("subaccount:{}:balances", balance.subaccount_id), 
            &balance.denom, 
            timestamp
        ).await?;
        
        // Add to global denoms index
        conn.zadd("denoms:all", &balance.denom, timestamp).await?;
        
        Ok(())
    }
}