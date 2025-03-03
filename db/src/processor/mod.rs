
use injective_consumer_base::{KafkaMessage, MessageProcessor};
use injective_consumer_base::models::{DerivativeMarketPayload, ExchangeBalancePayload, OrderbookL3Payload, PositionPayload};
use log::{error, info};
use scylla::{Session, SessionBuilder};
use std::error::Error;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

impl MessageProcessor for ScyllaMessageProcessor {
    fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Clone Arc for the async block
        let processor = self.clone();
        let block_time = message.block_time;
        let block_height = message.block_height;
        
        // Process different message types
        match message.payload {
            injective_consumer_base::models::KafkaPayload::DerivativeMarkets(markets) => {
                tokio::spawn(async move {
                    for market in markets {
                        if let Err(e) = processor.store_derivative_market(&market, block_time, block_height).await {
                            error!("Failed to store derivative market in ScyllaDB: {}", e);
                        } else {
                            info!("Stored derivative market in ScyllaDB: {}", market.ticker);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::Positions(positions) => {
                tokio::spawn(async move {
                    for position in positions {
                        if let Err(e) = processor.store_position(&position, block_time, block_height).await {
                            error!("Failed to store position in ScyllaDB: {}", e);
                        } else {
                            info!("Stored position in ScyllaDB for market: {}", position.market_id);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::ExchangeBalances(balances) => {
                tokio::spawn(async move {
                    for balance in balances {
                        if let Err(e) = processor.store_exchange_balance(&balance, block_time, block_height).await {
                            error!("Failed to store exchange balance in ScyllaDB: {}", e);
                        } else {
                            info!("Stored exchange balance in ScyllaDB for subaccount: {}", balance.subaccount_id);
                        }
                    }
                });
            },
            injective_consumer_base::models::KafkaPayload::DerivativeL3Orderbooks(orderbooks) => {
                tokio::spawn(async move {
                    for orderbook in orderbooks {
                        if let Err(e) = processor.store_orderbook(&orderbook, block_time, block_height).await {
                            error!("Failed to store orderbook in ScyllaDB: {}", e);
                        } else {
                            info!("Stored orderbook in ScyllaDB for market: {}", orderbook.market_id);
                        }
                    }
                });
            },
            // Add handlers for other message types as needed
            _ => {
                // For other message types that we're not handling yet
                info!("Received message type that's not being persisted to ScyllaDB");
            }
        }
        
        Ok(())
    }
}

// Implement Clone for ScyllaMessageProcessor
impl Clone for ScyllaMessageProcessor {
    fn clone(&self) -> Self {
        ScyllaMessageProcessor {
            session: self.session.clone(),
        }
    }
}

pub struct ScyllaMessageProcessor {
    session: Arc<Mutex<Session>>,
}

impl ScyllaMessageProcessor {
    pub async fn new(nodes: Vec<&str>, keyspace: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Build session
        let session = SessionBuilder::new()
            .known_nodes(nodes)
            .build()
            .await?;
        
        // Create keyspace if it doesn't exist
        session
            .query(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : 3 }}",
                    keyspace
                ),
                &[],
            )
            .await?;
        
        // Use keyspace
        session.query(format!("USE {}", keyspace), &[]).await?;
        
        // Initialize tables
        Self::init_tables(&session).await?;
        
        info!("Connected to ScyllaDB at {:?} using keyspace {}", nodes, keyspace);
        
        Ok(Self {
            session: Arc::new(Mutex::new(session)),
        })
    }
    
    // Initialize ScyllaDB tables
    async fn init_tables(session: &Session) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Create derivative markets table
        session
            .query(
                "CREATE TABLE IF NOT EXISTS derivative_markets (
                    market_id text,
                    ticker text,
                    oracle_base text,
                    oracle_quote text,
                    quote_denom text,
                    maker_fee_rate text,
                    taker_fee_rate text,
                    initial_margin_ratio text,
                    maintenance_margin_ratio text,
                    is_perpetual boolean,
                    status text,
                    mark_price text,
                    block_time bigint,
                    block_height bigint,
                    PRIMARY KEY (market_id, block_time)
                ) WITH CLUSTERING ORDER BY (block_time DESC)",
                &[],
            )
            .await?;
        
        // Create positions table
        session
            .query(
                "CREATE TABLE IF NOT EXISTS positions (
                    market_id text,
                    subaccount_id text,
                    block_time bigint,
                    is_long boolean,
                    quantity text,
                    entry_price text,
                    margin text,
                    cumulative_funding_entry text,
                    block_height bigint,
                    PRIMARY KEY ((market_id, subaccount_id), block_time)
                ) WITH CLUSTERING ORDER BY (block_time DESC)",
                &[],
            )
            .await?;
        
        // Create positions by market table (for querying all positions for a market)
        session
            .query(
                "CREATE TABLE IF NOT EXISTS positions_by_market (
                    market_id text,
                    subaccount_id text,
                    block_time bigint,
                    is_long boolean,
                    quantity text,
                    entry_price text,
                    margin text,
                    cumulative_funding_entry text,
                    block_height bigint,
                    PRIMARY KEY (market_id, block_time, subaccount_id)
                ) WITH CLUSTERING ORDER BY (block_time DESC, subaccount_id ASC)",
                &[],
            )
            .await?;
        
        // Create positions by subaccount table (for querying all positions for a subaccount)
        session
            .query(
                "CREATE TABLE IF NOT EXISTS positions_by_subaccount (
                    subaccount_id text,
                    market_id text,
                    block_time bigint,
                    is_long boolean,
                    quantity text,
                    entry_price text,
                    margin text,
                    cumulative_funding_entry text,
                    block_height bigint,
                    PRIMARY KEY (subaccount_id, block_time, market_id)
                ) WITH CLUSTERING ORDER BY (block_time DESC, market_id ASC)",
                &[],
            )
            .await?;
        
        // Create exchange balances table
        session
            .query(
                "CREATE TABLE IF NOT EXISTS exchange_balances (
                    subaccount_id text,
                    denom text,
                    block_time bigint,
                    available_balance text,
                    total_balance text,
                    block_height bigint,
                    PRIMARY KEY ((subaccount_id, denom), block_time)
                ) WITH CLUSTERING ORDER BY (block_time DESC)",
                &[],
            )
            .await?;
        
        // Create exchange balances by subaccount table (for querying all balances for a subaccount)
        session
            .query(
                "CREATE TABLE IF NOT EXISTS exchange_balances_by_subaccount (
                    subaccount_id text,
                    denom text,
                    block_time bigint,
                    available_balance text,
                    total_balance text,
                    block_height bigint,
                    PRIMARY KEY (subaccount_id, block_time, denom)
                ) WITH CLUSTERING ORDER BY (block_time DESC, denom ASC)",
                &[],
            )
            .await?;
        
        // Create orderbook snapshots table
        session
            .query(
                "CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    market_id text,
                    timestamp bigint,
                    orderbook_id uuid,
                    block_height bigint,
                    block_time bigint,
                    PRIMARY KEY (market_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)",
                &[],
            )
            .await?;
        
        // Create orderbook orders table
        session
            .query(
                "CREATE TABLE IF NOT EXISTS orderbook_orders (
                    orderbook_id uuid,
                    price text,
                    quantity text,
                    order_hash text,
                    subaccount_id text,
                    is_bid boolean,
                    PRIMARY KEY (orderbook_id, is_bid, price, order_hash)
                ) WITH CLUSTERING ORDER BY (is_bid DESC, price DESC, order_hash ASC)",
                &[],
            )
            .await?;
        
        // Create market statistics table for time-series analysis
        session
            .query(
                "CREATE TABLE IF NOT EXISTS market_statistics (
                    market_id text,
                    date date,
                    hour int,
                    best_bid text,
                    best_ask text,
                    mid_price text,
                    mark_price text,
                    volume text,
                    PRIMARY KEY ((market_id, date), hour)
                ) WITH CLUSTERING ORDER BY (hour ASC)",
                &[],
            )
            .await?;
        
        info!("ScyllaDB tables initialized");
        Ok(())
    }
    
    // Helper function to store a derivative market
    async fn store_derivative_market(&self, market: &DerivativeMarketPayload, block_time: i64, block_height: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let session = self.session.lock().await;
        
        // Insert into derivative_markets table
        session
            .query(
                "INSERT INTO derivative_markets (
                    market_id, ticker, oracle_base, oracle_quote, quote_denom,
                    maker_fee_rate, taker_fee_rate, initial_margin_ratio,
                    maintenance_margin_ratio, is_perpetual, status, mark_price,
                    block_time, block_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &market.market_id, &market.ticker, &market.oracle_base, &market.oracle_quote, 
                    &market.quote_denom, &market.maker_fee_rate, &market.taker_fee_rate,
                    &market.initial_margin_ratio, &market.maintenance_margin_ratio, market.is_perpetual,
                    &market.status, &market.mark_price, block_time, block_height
                ),
            )
            .await?;
        
        Ok(())
    }
    
    // Helper function to store a position
    async fn store_position(&self, position: &PositionPayload, block_time: i64, block_height: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let session = self.session.lock().await;
        
        // Insert into positions table
        session
            .query(
                "INSERT INTO positions (
                    market_id, subaccount_id, block_time, is_long, quantity,
                    entry_price, margin, cumulative_funding_entry, block_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &position.market_id, &position.subaccount_id, block_time, position.is_long,
                    &position.quantity, &position.entry_price, &position.margin,
                    &position.cumulative_funding_entry, block_height
                ),
            )
            .await?;
        
        // Insert into positions_by_market table
        session
            .query(
                "INSERT INTO positions_by_market (
                    market_id, subaccount_id, block_time, is_long, quantity,
                    entry_price, margin, cumulative_funding_entry, block_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &position.market_id, &position.subaccount_id, block_time, position.is_long,
                    &position.quantity, &position.entry_price, &position.margin,
                    &position.cumulative_funding_entry, block_height
                ),
            )
            .await?;
        
        // Insert into positions_by_subaccount table
        session
            .query(
                "INSERT INTO positions_by_subaccount (
                    subaccount_id, market_id, block_time, is_long, quantity,
                    entry_price, margin, cumulative_funding_entry, block_height
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    &position.subaccount_id, &position.market_id, block_time, position.is_long,
                    &position.quantity, &position.entry_price, &position.margin,
                    &position.cumulative_funding_entry, block_height
                ),
            )
            .await?;
        
        Ok(())
    }
    
    // Helper function to store an exchange balance
    async fn store_exchange_balance(&self, balance: &ExchangeBalancePayload, block_time: i64, block_height: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let session = self.session.lock().await;
        
        // Insert into exchange_balances table
        session
            .query(
                "INSERT INTO exchange_balances (
                    subaccount_id, denom, block_time, available_balance,
                    total_balance, block_height
                ) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    &balance.subaccount_id, &balance.denom, block_time,
                    &balance.available_balance, &balance.total_balance, block_height
                ),
            )
            .await?;
        
        // Insert into exchange_balances_by_subaccount table
        session
            .query(
                "INSERT INTO exchange_balances_by_subaccount (
                    subaccount_id, denom, block_time, available_balance,
                    total_balance, block_height
                ) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    &balance.subaccount_id, &balance.denom, block_time,
                    &balance.available_balance, &balance.total_balance, block_height
                ),
            )
            .await?;
        
        Ok(())
    }
    
    // Helper function to store an orderbook
    async fn store_orderbook(&self, orderbook: &OrderbookL3Payload, block_time: i64, block_height: i64) -> Result<(), Box<dyn Error + Send + Sync>> {
        let session = self.session.lock().await;
        
        // Generate a unique orderbook ID
        let orderbook_id = Uuid::new_v4();
        
        // Insert into orderbook_snapshots table
        session
            .query(
                "INSERT INTO orderbook_snapshots (
                    market_id, timestamp, orderbook_id, block_height, block_time
                ) VALUES (?, ?, ?, ?, ?)",
                (
                    &orderbook.market_id, orderbook.timestamp, orderbook_id,
                    block_height, block_time
                ),
            )
            .await?;
        
        // Insert all bid orders
        for bid in &orderbook.bids {
            session
                .query(
                    "INSERT INTO orderbook_orders (
                        orderbook_id, price, quantity, order_hash, subaccount_id, is_bid
                    ) VALUES (?, ?, ?, ?, ?, true)",
                    (
                        orderbook_id, &bid.price, &bid.quantity, &bid.order_hash, &bid.subaccount_id
                    ),
                )
                .await?;
        }
        
        // Insert all ask orders
        for ask in &orderbook.asks {
            session
                .query(
                    "INSERT INTO orderbook_orders (
                        orderbook_id, price, quantity, order_hash, subaccount_id, is_bid
                    ) VALUES (?, ?, ?, ?, ?, false)",
                    (
                        orderbook_id, &ask.price, &ask.quantity, &ask.order_hash, &ask.subaccount_id
                    ),
                )
                .await?;
        }
        
        // Calculate and store market statistics if possible
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
            
            if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
                // Get best bid and ask
                let best_bid_price = &best_bid.price;
                let best_ask_price = &best_ask.price;
                
                // Calculate mid price
                let bid_price = best_bid.price.parse::<f64>().unwrap_or(0.0);
                let ask_price = best_ask.price.parse::<f64>().unwrap_or(0.0);
                
                if bid_price > 0.0 && ask_price > 0.0 {
                    let mid_price = (bid_price + ask_price) / 2.0;
                    
                    // Calculate hourly timestamp
                    let date = chrono::Utc::now().date_naive();
                    let hour = chrono::Utc::now().hour() as i32;
                    
                    // Insert or update market statistics
                    session
                        .query(
                            "INSERT INTO market_statistics (
                                market_id, date, hour, best_bid, best_ask, mid_price
                            ) VALUES (?, ?, ?, ?, ?, ?)",
                            (
                                &orderbook.market_id, date, hour, best_bid_price, 
                                best_ask_price, mid_price.to_string()
                            ),
                        )
                        .await?;
                }
            }
        }
        
        Ok(())
    }
}