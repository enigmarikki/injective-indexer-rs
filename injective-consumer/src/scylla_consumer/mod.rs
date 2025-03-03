use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, MessageType, KafkaPayload};
use chrono::{DateTime, NaiveDateTime, Utc};
use scylla::{Session, SessionBuilder};
use std::error::Error;
use std::sync::Arc;
use log::{info, error};

pub struct ScyllaDBProcessor {
    session: Arc<Session>,
}

impl ScyllaDBProcessor {
    pub async fn new(nodes: Vec<String>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Create a session to connect to ScyllaDB
        let session = SessionBuilder::new()
            .known_nodes(&nodes)
            .build()
            .await?;
            
        // Create keyspace and tables if they don't exist
        Self::initialize_schema(&session).await?;
        
        Ok(ScyllaDBProcessor {
            session: Arc::new(session),
        })
    }
    
    async fn initialize_schema(session: &Session) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create keyspace
        session.query(
            "CREATE KEYSPACE IF NOT EXISTS injective WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;
        
        // Create market prices table
        session.query(
            "CREATE TABLE IF NOT EXISTS injective.market_prices (
                market_id text,
                timestamp timestamp,
                mark_price text,
                bid_price text,
                ask_price text,
                PRIMARY KEY (market_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)",
            &[]
        ).await?;
        
        // Create positions table
        session.query(
            "CREATE TABLE IF NOT EXISTS injective.positions (
                market_id text,
                subaccount_id text,
                timestamp timestamp,
                is_long boolean,
                quantity text,
                entry_price text,
                margin text,
                PRIMARY KEY ((market_id, subaccount_id), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)",
            &[]
        ).await?;
        
        // Create balances table
        session.query(
            "CREATE TABLE IF NOT EXISTS injective.exchange_balances (
                subaccount_id text,
                denom text,
                timestamp timestamp,
                available_balance text,
                total_balance text,
                PRIMARY KEY ((subaccount_id, denom), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)",
            &[]
        ).await?;
        
        // Create orderbook snapshots table
        session.query(
            "CREATE TABLE IF NOT EXISTS injective.orderbook_snapshots (
                market_id text,
                timestamp timestamp,
                bid_price text,
                bid_quantity text,
                ask_price text,
                ask_quantity text,
                PRIMARY KEY (market_id, timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)",
            &[]
        ).await?;
        
        // Technical analysis tables
        session.query(
            "CREATE TABLE IF NOT EXISTS injective.ta_indicators (
                market_id text,
                indicator_type text,
                timeframe text,
                timestamp timestamp,
                value double,
                PRIMARY KEY ((market_id, indicator_type, timeframe), timestamp)
            ) WITH CLUSTERING ORDER BY (timestamp DESC)",
            &[]
        ).await?;
        
        Ok(())
    }
    
    async fn store_market_price(&self, market_id: &str, mark_price: &str, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let query = "INSERT INTO injective.market_prices (market_id, timestamp, mark_price) VALUES (?, ?, ?)";
        
        let datetime = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap_or_default(),
            Utc,
        );
        
        self.session.query(query, (market_id, datetime, mark_price)).await?;
        
        Ok(())
    }
    
    async fn store_position(&self, position: &crate::models::PositionPayload, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let query = "INSERT INTO injective.positions (market_id, subaccount_id, timestamp, is_long, quantity, entry_price, margin) VALUES (?, ?, ?, ?, ?, ?, ?)";
        
        let datetime = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap_or_default(),
            Utc,
        );
        
        self.session.query(
            query, 
            (
                &position.market_id, 
                &position.subaccount_id, 
                datetime,
                position.is_long,
                &position.quantity,
                &position.entry_price,
                &position.margin
            )
        ).await?;
        
        Ok(())
    }
    
    async fn store_balance(&self, balance: &crate::models::ExchangeBalancePayload, timestamp: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let query = "INSERT INTO injective.exchange_balances (subaccount_id, denom, timestamp, available_balance, total_balance) VALUES (?, ?, ?, ?, ?)";
        
        let datetime = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_opt(timestamp, 0).unwrap_or_default(),
            Utc,
        );
        
        self.session.query(
            query, 
            (
                &balance.subaccount_id, 
                &balance.denom, 
                datetime,
                &balance.available_balance,
                &balance.total_balance
            )
        ).await?;
        
        Ok(())
    }
    
    async fn store_orderbook_snapshot(&self, orderbook: &crate::models::OrderbookL3Payload) -> Result<(), Box<dyn Error + Send + Sync>> {
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
        
        let query = "INSERT INTO injective.orderbook_snapshots (market_id, timestamp, bid_price, bid_quantity, ask_price, ask_quantity) VALUES (?, ?, ?, ?, ?, ?)";
        
        let datetime = DateTime::<Utc>::from_utc(
            NaiveDateTime::from_timestamp_millis(orderbook.timestamp).unwrap_or_default(),
            Utc,
        );
        
        self.session.query(
            query, 
            (
                &orderbook.market_id, 
                datetime,
                &best_bid_price,
                &best_bid_quantity,
                &best_ask_price,
                &best_ask_quantity
            )
        ).await?;
        
        // We could calculate and store technical indicators here
        // For example, we could calculate and store SMAs, EMAs, etc.
        self.calculate_technical_indicators(&orderbook.market_id).await?;
        
        Ok(())
    }
    
    async fn calculate_technical_indicators(&self, market_id: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Calculate SMA, EMA, RSI, etc. for different timeframes
        // This is placeholder code - you would need to implement your TA logic here
        
        // Example: Calculate and store a 20-period SMA
        // First, fetch the last 20 prices from the market_prices table
        let query = "SELECT timestamp, mark_price FROM injective.market_prices WHERE market_id = ? ORDER BY timestamp DESC LIMIT 20";
        let rows = self.session.query(query, (market_id,)).await?;
        
        // Process the data and calculate indicators
        // This is just a placeholder - you'd implement actual TA logic
        if rows.rows.len() >= 20 {
            // Calculate SMA
            let sma_value = 0.0; // Replace with actual calculation
            
            // Store the indicator
            let now = Utc::now();
            let insert_query = "INSERT INTO injective.ta_indicators (market_id, indicator_type, timeframe, timestamp, value) VALUES (?, ?, ?, ?, ?)";
            self.session.query(insert_query, (market_id, "SMA", "20", now, sma_value)).await?;
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl MessageProcessor for ScyllaDBProcessor {
    async fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        let timestamp = message.block_time;
        
        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                for market in markets {
                    if let Err(e) = self.store_market_price(&market.market_id, &market.mark_price, timestamp).await {
                        error!("Error storing market price: {}", e);
                    }
                }
            },
            KafkaPayload::Positions(positions) => {
                for position in positions {
                    if let Err(e) = self.store_position(position, timestamp).await {
                        error!("Error storing position: {}", e);
                    }
                }
            },
            KafkaPayload::ExchangeBalances(balances) => {
                for balance in balances {
                    if let Err(e) = self.store_balance(balance, timestamp).await {
                        error!("Error storing balance: {}", e);
                    }
                }
            },
            KafkaPayload::DerivativeL3Orderbooks(orderbooks) => {
                for orderbook in orderbooks {
                    if let Err(e) = self.store_orderbook_snapshot(orderbook).await {
                        error!("Error storing orderbook snapshot: {}", e);
                    }
                }
            },
            // Handle other payload types as needed
            _ => {
                // Log that we received a message type we don't handle
                info!("Received message type {:?} which is not handled by ScyllaDB processor", message.message_type);
            }
        }
        
        Ok(())
    }
}