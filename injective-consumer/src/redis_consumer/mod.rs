use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, MessageType, KafkaPayload};
use redis::{Client, Commands, Connection};
use std::error::Error;
use std::sync::Mutex;
use log::{info, error};

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
    
    fn process_derivative_market(&self, market: &crate::models::DerivativeMarketPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().map_err(|_| "Failed to lock Redis connection")?;
        
        // Store market data in Redis
        let key = format!("market:derivative:{}", market.market_id);
        
        // Use a hash for each market
        conn.hset(&key, "ticker", &market.ticker)?;
        conn.hset(&key, "oracle_base", &market.oracle_base)?;
        conn.hset(&key, "oracle_quote", &market.oracle_quote)?;
        conn.hset(&key, "quote_denom", &market.quote_denom)?;
        conn.hset(&key, "maker_fee_rate", &market.maker_fee_rate)?;
        conn.hset(&key, "taker_fee_rate", &market.taker_fee_rate)?;
        conn.hset(&key, "initial_margin_ratio", &market.initial_margin_ratio)?;
        conn.hset(&key, "maintenance_margin_ratio", &market.maintenance_margin_ratio)?;
        conn.hset(&key, "is_perpetual", market.is_perpetual.to_string())?;
        conn.hset(&key, "status", &market.status)?;
        conn.hset(&key, "mark_price", &market.mark_price)?;
        
        // Add to markets set for easy listing
        conn.sadd("markets:derivative", &market.market_id)?;
        
        Ok(())
    }
    
    fn process_position(&self, position: &crate::models::PositionPayload) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().map_err(|_| "Failed to lock Redis connection")?;
        
        // Store position data in Redis
        let key = format!("position:{}:{}", position.market_id, position.subaccount_id);
        conn.hset(&key, "is_long", position.is_long.to_string())?;
        conn.hset(&key, "quantity", &position.quantity)?;
        conn.hset(&key, "entry_price", &position.entry_price)?;
        conn.hset(&key, "margin", &position.margin)?;
        conn.hset(&key, "cumulative_funding_entry", &position.cumulative_funding_entry)?;
        
        // Add to positions set for the market
        conn.sadd(format!("positions:market:{}", position.market_id), &position.subaccount_id)?;
        
        // Add to positions set for the subaccount
        conn.sadd(format!("positions:subaccount:{}", position.subaccount_id), &position.market_id)?;
        
        Ok(())
    }
    
    fn process_exchange_balance(&self, balance: &crate::models::ExchangeBalancePayload) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().map_err(|_| "Failed to lock Redis connection")?;
        
        // Store balance data in Redis
        let key = format!("balance:{}:{}", balance.subaccount_id, balance.denom);
        conn.hset(&key, "available_balance", &balance.available_balance)?;
        conn.hset(&key, "total_balance", &balance.total_balance)?;
        
        // Add to balances set for the subaccount
        conn.sadd(format!("balances:subaccount:{}", balance.subaccount_id), &balance.denom)?;
        
        Ok(())
    }
    
    fn process_orderbook(&self, orderbook: &crate::models::OrderbookL3Payload) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut conn = self.connection.lock().map_err(|_| "Failed to lock Redis connection")?;
        
        // Store best bid/ask in Redis for quick access
        let market_id = &orderbook.market_id;
        
        // Find best bid/ask
        let mut best_bid_price = "0".to_string();
        let mut best_bid_quantity = "0".to_string();
        let mut best_ask_price = "0".to_string();
        let mut best_ask_quantity = "0".to_string();
        
        if let Some(best_bid) = orderbook.bids.iter().max_by(|a, b| {
            a.price.parse::<f64>().unwrap_or(0.0).partial_cmp(&b.price.parse::<f64>().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            best_bid_price = best_bid.price.clone();
            best_bid_quantity = best_bid.quantity.clone();
        }
        
        if let Some(best_ask) = orderbook.asks.iter().min_by(|a, b| {
            a.price.parse::<f64>().unwrap_or(0.0).partial_cmp(&b.price.parse::<f64>().unwrap_or(0.0)).unwrap_or(std::cmp::Ordering::Equal)
        }) {
            best_ask_price = best_ask.price.clone();
            best_ask_quantity = best_ask.quantity.clone();
        }
        
        // Store best bid/ask
        let key = format!("orderbook:best:{}", market_id);
        conn.hset(&key, "bid_price", &best_bid_price)?;
        conn.hset(&key, "bid_quantity", &best_bid_quantity)?;
        conn.hset(&key, "ask_price", &best_ask_price)?;
        conn.hset(&key, "ask_quantity", &best_ask_quantity)?;
        conn.hset(&key, "timestamp", orderbook.timestamp.to_string())?;
        
        // We don't store the full orderbook in Redis as that would be too large
        // Instead just keep the top of book for quick access
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageProcessor for RedisProcessor {
    async fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                for market in markets {
                    if let Err(e) = self.process_derivative_market(market) {
                        error!("Error processing derivative market: {}", e);
                    }
                }
            },
            KafkaPayload::Positions(positions) => {
                for position in positions {
                    if let Err(e) = self.process_position(position) {
                        error!("Error processing position: {}", e);
                    }
                }
            },
            KafkaPayload::ExchangeBalances(balances) => {
                for balance in balances {
                    if let Err(e) = self.process_exchange_balance(balance) {
                        error!("Error processing exchange balance: {}", e);
                    }
                }
            },
            KafkaPayload::DerivativeL3Orderbooks(orderbooks) => {
                for orderbook in orderbooks {
                    if let Err(e) = self.process_orderbook(orderbook) {
                        error!("Error processing orderbook: {}", e);
                    }
                }
            },
            // Handle other payload types as needed
            _ => {
                // Log that we received a message type we don't handle
                info!("Received message type {:?} which is not handled by Redis processor", message.message_type);
            }
        }
        
        Ok(())
    }
}