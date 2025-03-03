use crate::proto::injective::exchange::v1beta1::{
    DerivativeMarket, DerivativePosition, FullDerivativeMarket, 
    QueryDerivativeMarketsRequest, QueryDerivativeMarketsResponse,
    QueryExchangeBalancesRequest, QueryExchangeBalancesResponse,
    QueryPositionsRequest, QueryPositionsResponse,
};
use crate::proto::injective::exchange::v1beta1::query_client::QueryClient;
use crate::config::GrpcConfig;
use log::{info, error};
use std::error::Error;
use tonic::Request;
use tokio::time::{Duration, interval};

pub struct ExchangeQueryClient {
    client: QueryClient<tonic::transport::Channel>,
}

impl ExchangeQueryClient {
    pub async fn connect(config: &GrpcConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = QueryClient::connect(config.query_endpoint.clone()).await?;
        info!("Connected to exchange query service: {}", config.query_endpoint);
        
        Ok(Self { client })
    }

    pub async fn get_derivative_markets(&mut self, status: Option<String>) -> Result<Vec<FullDerivativeMarket>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(QueryDerivativeMarketsRequest {
            status: status.unwrap_or_default(),
            market_ids: vec![],
            with_mid_price_and_tob: true,
        });

        let response = self.client.derivative_markets(request).await?;
        let markets = response.into_inner().markets;
        
        info!("Retrieved {} derivative markets", markets.len());
        Ok(markets)
    }

    pub async fn get_positions(&mut self) -> Result<Vec<DerivativePosition>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(QueryPositionsRequest {});
        
        let response = self.client.positions(request).await?;
        let positions = response.into_inner().state;
        
        info!("Retrieved {} positions", positions.len());
        Ok(positions)
    }

    pub async fn get_exchange_balances(&mut self) -> Result<Vec<crate::proto::injective::exchange::v1beta1::Balance>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(QueryExchangeBalancesRequest {});
        
        let response = self.client.exchange_balances(request).await?;
        let balances = response.into_inner().balances;
        
        info!("Retrieved {} exchange balances", balances.len());
        Ok(balances)
    }

    // Get full L3 orderbook for a derivative market
    pub async fn get_full_derivative_orderbook(&mut self, market_id: &str) -> Result<crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookResponse, Box<dyn Error + Send + Sync>> {
        let request = Request::new(crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookRequest {
            market_id: market_id.to_string(),
        });
        
        let response = self.client.l3_derivative_order_book(request).await?;
        info!("Retrieved full orderbook for market {}", market_id);
        
        Ok(response.into_inner())
    }
}

// A heartbeat service that periodically fetches data from the exchange
pub struct ExchangeHeartbeat {
    client: ExchangeQueryClient,
    producer: crate::producer::BatchKafkaProducer,
    interval_seconds: u64,
}

impl ExchangeHeartbeat {
    pub async fn new(
        config: &GrpcConfig,
        kafka_config: &crate::config::KafkaConfig,
        interval_seconds: u64,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = ExchangeQueryClient::connect(config).await?;
        let producer = crate::producer::BatchKafkaProducer::new(kafka_config)?;
        
        Ok(Self {
            client,
            producer,
            interval_seconds,
        })
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Starting exchange heartbeat service with interval of {}s", self.interval_seconds);
        
        let mut interval = interval(Duration::from_secs(self.interval_seconds));
        
        loop {
            interval.tick().await;
            info!("Executing heartbeat...");
            
            // Fetch derivative markets
            match self.client.get_derivative_markets(Some("Active".to_string())).await {
                Ok(markets) => {
                    // Handle markets data and send to Kafka
                    self.process_derivative_markets(markets).await?;
                },
                Err(e) => {
                    error!("Failed to fetch derivative markets: {}", e);
                }
            }
            
            // Fetch positions
            match self.client.get_positions().await {
                Ok(positions) => {
                    // Handle positions data and send to Kafka
                    self.process_positions(positions).await?;
                },
                Err(e) => {
                    error!("Failed to fetch positions: {}", e);
                }
            }
            
            // Fetch exchange balances
            match self.client.get_exchange_balances().await {
                Ok(balances) => {
                    // Handle balances data and send to Kafka
                    self.process_exchange_balances(balances).await?;
                },
                Err(e) => {
                    error!("Failed to fetch exchange balances: {}", e);
                }
            }
            
            // Now fetch orderbooks for each active derivative market
            if let Ok(markets) = self.client.get_derivative_markets(Some("Active".to_string())).await {
                for market in markets {
                    if let Some(market_data) = market.market {
                        if let Ok(orderbook) = self.client.get_full_derivative_orderbook(&market_data.market_id).await {
                            self.process_derivative_orderbook(&market_data.market_id, orderbook).await?;
                        }
                    }
                }
            }
        }
    }

    async fn process_derivative_markets(&self, markets: Vec<FullDerivativeMarket>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = markets.into_iter().map(|market| {
            crate::models::KafkaMessage {
                message_type: crate::models::MessageType::DerivativeMarket,
                block_height: 0, // We don't have block height in this context
                block_time: chrono::Utc::now().timestamp(),
                payload: crate::models::KafkaPayload::DerivativeMarkets(vec![self.convert_derivative_market(market)]),
            }
        }).collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!("Sent {}/{} derivative markets to Kafka", success_count, results.len());
        }

        Ok(())
    }

    fn convert_derivative_market(&self, market: FullDerivativeMarket) -> crate::models::DerivativeMarketPayload {
        // Extract market details and convert to our payload format
        let market_data = market.market.unwrap_or_default();
        crate::models::DerivativeMarketPayload {
            market_id: market_data.market_id,
            ticker: market_data.ticker,
            oracle_base: market_data.oracle_base,
            oracle_quote: market_data.oracle_quote,
            quote_denom: market_data.quote_denom,
            maker_fee_rate: market_data.maker_fee_rate,
            taker_fee_rate: market_data.taker_fee_rate,
            initial_margin_ratio: market_data.initial_margin_ratio,
            maintenance_margin_ratio: market_data.maintenance_margin_ratio,
            is_perpetual: market_data.is_perpetual,
            status: self.map_market_status(market_data.status),
            mark_price: market.mark_price,
        }
    }

    fn map_market_status(&self, status: i32) -> String {
        match status {
            1 => "Active".to_string(),
            2 => "Paused".to_string(),
            3 => "Demolished".to_string(),
            4 => "Expired".to_string(),
            _ => "Unknown".to_string(),
        }
    }

    async fn process_positions(&self, positions: Vec<DerivativePosition>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = positions.into_iter().map(|position| {
            crate::models::KafkaMessage {
                message_type: crate::models::MessageType::Position,
                block_height: 0, // We don't have block height in this context
                block_time: chrono::Utc::now().timestamp(),
                payload: crate::models::KafkaPayload::Positions(vec![self.convert_position(position)]),
            }
        }).collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!("Sent {}/{} positions to Kafka", success_count, results.len());
        }

        Ok(())
    }

    fn convert_position(&self, position: DerivativePosition) -> crate::models::PositionPayload {
        let position_data = position.position.unwrap_or_default();
        crate::models::PositionPayload {
            market_id: position.market_id,
            subaccount_id: position.subaccount_id,
            is_long: position_data.is_long,
            quantity: position_data.quantity,
            entry_price: position_data.entry_price,
            margin: position_data.margin,
            cumulative_funding_entry: position_data.cumulative_funding_entry,
        }
    }

    async fn process_exchange_balances(&self, balances: Vec<crate::proto::injective::exchange::v1beta1::Balance>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = balances.into_iter().map(|balance| {
            crate::models::KafkaMessage {
                message_type: crate::models::MessageType::ExchangeBalance,
                block_height: 0, // We don't have block height in this context
                block_time: chrono::Utc::now().timestamp(),
                payload: crate::models::KafkaPayload::ExchangeBalances(vec![self.convert_exchange_balance(balance)]),
            }
        }).collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!("Sent {}/{} exchange balances to Kafka", success_count, results.len());
        }

        Ok(())
    }

    fn convert_exchange_balance(&self, balance: crate::proto::injective::exchange::v1beta1::Balance) -> crate::models::ExchangeBalancePayload {
        let deposit = balance.deposits.unwrap_or_default();
        crate::models::ExchangeBalancePayload {
            subaccount_id: balance.subaccount_id,
            denom: balance.denom,
            available_balance: deposit.available_balance,
            total_balance: deposit.total_balance,
        }
    }

    async fn process_derivative_orderbook(&self, market_id: &str, orderbook: crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookResponse) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::DerivativeL3Orderbook,
            block_height: 0,
            block_time: chrono::Utc::now().timestamp(),
            payload: crate::models::KafkaPayload::DerivativeL3Orderbooks(vec![
                crate::models::OrderbookL3Payload {
                    market_id: market_id.to_string(),
                    bids: orderbook.bids.into_iter().map(|order| self.convert_limit_order(order)).collect(),
                    asks: orderbook.asks.into_iter().map(|order| self.convert_limit_order(order)).collect(),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                }
            ]),
        };

        // Send to Kafka
        let results = self.producer.send_batch(vec![message]).await;
        if let Some(Err(e)) = results.first() {
            error!("Failed to send orderbook to Kafka: {}", e);
        } else {
            info!("Sent orderbook for market {} to Kafka", market_id);
        }

        Ok(())
    }

    fn convert_limit_order(&self, order: crate::proto::injective::exchange::v1beta1::TrimmedLimitOrder) -> crate::models::TrimmedLimitOrderPayload {
        crate::models::TrimmedLimitOrderPayload {
            price: order.price,
            quantity: order.quantity,
            order_hash: order.order_hash,
            subaccount_id: order.subaccount_id,
        }
    }
}