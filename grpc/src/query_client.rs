use crate::config::GrpcConfig;
use crate::proto::injective::exchange::v1beta1::full_derivative_market::Info;
use crate::proto::injective::exchange::v1beta1::query_client::QueryClient;
use crate::proto::injective::exchange::v1beta1::{
    DerivativePosition, FullDerivativeMarket, QueryDerivativeMarketsRequest,
    QueryExchangeBalancesRequest, QueryPositionsRequest,
};
use log::{debug, error, info};
use std::error::Error;
use tokio::time::{interval, Duration};
use tonic::Request;

pub struct ExchangeQueryClient {
    client: QueryClient<tonic::transport::Channel>,
    http_client: reqwest::Client,
    tendermint_rpc_endpoint: String,
}

impl ExchangeQueryClient {
    pub async fn connect(config: &GrpcConfig) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Set larger message size limit through environment variables
        // These need to be set before creating the client
        std::env::set_var("GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH", "67108864"); // 64MB
        std::env::set_var("GRPC_ARG_MAX_SEND_MESSAGE_LENGTH", "16777216");    // 16MB
        
        // Create normal client without interceptor
        let client = QueryClient::connect(config.query_endpoint.clone()).await?;
        
        info!("Connected to exchange query service: {}", config.query_endpoint);
    
        // Rest of your connect method...
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
    
        // Derive Tendermint RPC endpoint from query endpoint
        let parsed_url = url::Url::parse(&config.query_endpoint)?;
        let host = parsed_url.host_str().unwrap_or("localhost");
        let tendermint_rpc_endpoint = format!("http://{}:26657", host);
        debug!("Using Tendermint RPC endpoint: {}", tendermint_rpc_endpoint);
    
        Ok(Self {
            client,
            http_client,
            tendermint_rpc_endpoint,
        })
    }

    // Properly async method to fetch current block height from Tendermint RPC
    pub async fn get_current_block_height(&self) -> Result<u64, Box<dyn Error + Send + Sync>> {
        #[derive(serde::Deserialize)]
        struct StatusResponse {
            result: StatusResult,
        }

        #[derive(serde::Deserialize)]
        struct StatusResult {
            sync_info: SyncInfo,
        }

        #[derive(serde::Deserialize)]
        struct SyncInfo {
            latest_block_height: String,
        }

        // Use the async HTTP client properly
        let response = self
            .http_client
            .get(&format!("{}/status", self.tendermint_rpc_endpoint))
            .send()
            .await?
            .json::<StatusResponse>()
            .await?;

        let height: u64 = response.result.sync_info.latest_block_height.parse()?;
        debug!("Current block height: {}", height);

        Ok(height)
    }

    pub async fn get_derivative_markets(
        &mut self,
        status: Option<String>,
    ) -> Result<Vec<FullDerivativeMarket>, Box<dyn Error + Send + Sync>> {
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

    pub async fn get_positions(
        &mut self,
    ) -> Result<Vec<DerivativePosition>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(QueryPositionsRequest {});

        let response = self.client.positions(request).await?;
        let positions = response.into_inner().state;

        info!("Retrieved {} positions", positions.len());
        Ok(positions)
    }

    pub async fn get_exchange_balances(
        &mut self,
    ) -> Result<
        Vec<crate::proto::injective::exchange::v1beta1::Balance>,
        Box<dyn Error + Send + Sync>,
    > {
        let request = Request::new(QueryExchangeBalancesRequest {});

        let response = self.client.exchange_balances(request).await?;
        let balances = response.into_inner().balances;

        info!("Retrieved {} exchange balances", balances.len());
        Ok(balances)
    }

    // Get full L3 orderbook for a derivative market
    pub async fn get_full_derivative_orderbook(
        &mut self,
        market_id: &str,
    ) -> Result<
        crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookResponse,
        Box<dyn Error + Send + Sync>,
    > {
        let request = Request::new(
            crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookRequest {
                market_id: market_id.to_string(),
            },
        );

        let response = self.client.l3_derivative_order_book(request).await?;
        info!("Retrieved full orderbook for market {}", market_id);

        Ok(response.into_inner())
    }
}

// A heartbeat service that periodically fetches data from the exchange

pub struct ExchangeHeartbeat {
    client: ExchangeQueryClient,
    producer: std::sync::Arc<crate::producer::BatchKafkaProducer>,
    interval_seconds: u64,
}

impl ExchangeHeartbeat {
    pub async fn new(
        config: &GrpcConfig,
        kafka_config: &crate::config::KafkaConfig,
        interval_seconds: u64,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = ExchangeQueryClient::connect(config).await?;
        let producer = std::sync::Arc::new(crate::producer::BatchKafkaProducer::new(kafka_config)?);

        Ok(Self {
            client,
            producer,
            interval_seconds,
        })
    }

    // Add the new method that accepts an Arc-wrapped producer
    pub async fn new_with_producer(
        config: &GrpcConfig,
        producer: std::sync::Arc<crate::producer::BatchKafkaProducer>,
        interval_seconds: u64,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let client = ExchangeQueryClient::connect(config).await?;

        Ok(Self {
            client,
            producer,
            interval_seconds,
        })
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "Starting exchange heartbeat service with interval of {}s",
            self.interval_seconds
        );

        let mut interval = interval(Duration::from_secs(self.interval_seconds));

        loop {
            interval.tick().await;
            info!("Executing heartbeat...");

            // Get current block height - fully async operation
            let block_height = match self.client.get_current_block_height().await {
                Ok(height) => {
                    // Update the producer with the latest block height
                    info!("Current chain block height: {}", height);
                    self.producer.update_latest_block(height);
                    height
                }
                Err(e) => {
                    error!("Failed to get current block height: {}", e);
                    // Use the existing latest height
                    self.producer.get_latest_block()
                }
            };

            // Fetch derivative markets
            match self
                .client
                .get_derivative_markets(Some("Active".to_string()))
                .await
            {
                Ok(markets) => {
                    // Handle markets data and send to Kafka using the batch_current_only
                    self.process_derivative_markets(markets, block_height)
                        .await?;
                }
                Err(e) => {
                    error!("Failed to fetch derivative markets: {}", e);
                }
            }

            // Fetch positions
            match self.client.get_positions().await {
                Ok(positions) => {
                    // Handle positions data and send to Kafka
                    self.process_positions(positions, block_height).await?;
                }
                Err(e) => {
                    error!("Failed to fetch positions: {}", e);
                }
            }

            // Fetch exchange balances
            // match self.client.get_exchange_balances().await {
            //     Ok(balances) => {
            //         // Handle balances data and send to Kafka
            //         self.process_exchange_balances(balances, block_height)
            //             .await?;
            //     }
            //     Err(e) => {
            //         error!("Failed to fetch exchange balances: {}", e);
            //     }
            // }

            // Now fetch orderbooks for each active derivative market
            if let Ok(markets) = self
                .client
                .get_derivative_markets(Some("Active".to_string()))
                .await
            {
                let mut all_orderbooks = Vec::new();

                for market in markets {
                    if let Some(market_data) = market.market {
                        if let Ok(orderbook) = self
                            .client
                            .get_full_derivative_orderbook(&market_data.market_id)
                            .await
                        {
                            all_orderbooks.push((market_data.market_id.clone(), orderbook));
                        }
                    }
                }

                // Process all orderbooks in one batch
                if !all_orderbooks.is_empty() {
                    self.process_all_derivative_orderbooks(all_orderbooks, block_height)
                        .await?;
                }
            }
        }
    }
    async fn process_all_derivative_orderbooks(
        &self,
        orderbooks: Vec<(
            String,
            crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookResponse,
        )>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let orderbook_payloads: Vec<crate::models::FullLimitOrderbookPayload> = orderbooks
            .into_iter()
            .map(
                |(market_id, orderbook)| crate::models::FullLimitOrderbookPayload {
                    market_id,
                    bids: orderbook
                        .bids
                        .into_iter()
                        .map(|order| self.convert_limit_order(order))
                        .collect(),
                    asks: orderbook
                        .asks
                        .into_iter()
                        .map(|order| self.convert_limit_order(order))
                        .collect(),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                },
            )
            .collect();
        let length = orderbook_payloads.len();
        // Single message with all orderbooks
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::DerivativeFullOrderbook,
            block_height,
            block_time: chrono::Utc::now().timestamp_millis() as u64,
            payload: crate::models::KafkaPayload::DerivativeFullOrderbooks(orderbook_payloads),
        };

        // Send the batch
        let results = self.producer.send_batch_current_only(vec![message]).await;
        let success = results.first().map_or(false, |r| r.is_ok());

        info!(
            "Sent {} orderbooks to Kafka as single batch at block height {}",
            length, block_height
        );

        Ok(())
    }
    async fn process_derivative_markets(
        &self,
        markets: Vec<FullDerivativeMarket>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if markets.is_empty() {
            return Ok(());
        }
        let length = markets.len();

        // Convert all markets to payloads
        let market_payloads = markets
            .into_iter()
            .map(|market| self.convert_derivative_market(market))
            .collect();

        // Create a single message containing all markets
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::DerivativeMarket,
            block_height: block_height,
            block_time: chrono::Utc::now().timestamp_millis() as u64,
            payload: crate::models::KafkaPayload::DerivativeMarkets(market_payloads),
        };

        // Send to Kafka
        let results = self.producer.send_batch_current_only(vec![message]).await;
        let success = results.first().map_or(false, |r| r.is_ok());
        info!(
            "Sent {} derivative markets to Kafka as single batch at block height {}",
            length, block_height
        );

        Ok(())
    }

    async fn process_positions(
        &self,
        positions: Vec<DerivativePosition>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if positions.is_empty() {
            return Ok(());
        }
        let length = positions.len();
        // Convert all positions into a single payload
        let position_payloads = positions
            .into_iter()
            .map(|position| self.convert_position(position))
            .collect();

        // Create just one message containing all positions
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::ExchangePosition,
            block_height: block_height,
            block_time: chrono::Utc::now().timestamp_millis() as u64,
            payload: crate::models::KafkaPayload::ExchangePositions(position_payloads),
        };

        // Send single message with all positions
        let results = self.producer.send_batch_current_only(vec![message]).await;
        let success = results.first().map_or(false, |r| r.is_ok());

        info!(
            "Sent {} positions to Kafka as single batch at block height {}",
            length, block_height
        );

        Ok(())
    }

    async fn process_exchange_balances(
        &self,
        balances: Vec<crate::proto::injective::exchange::v1beta1::Balance>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        if balances.is_empty() {
            return Ok(());
        }
        let length = balances.len();
        // Convert all balances into a single payload
        let balance_payloads = balances
            .into_iter()
            .map(|balance| self.convert_exchange_balance(balance))
            .collect();

        // Create one message containing all balances
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::ExchangeBalance,
            block_height: block_height,
            block_time: chrono::Utc::now().timestamp_millis() as u64,
            payload: crate::models::KafkaPayload::ExchangeBalances(balance_payloads),
        };

        // Send the single message
        let results = self.producer.send_batch_current_only(vec![message]).await;
        let success = results.first().map_or(false, |r| r.is_ok());

        info!(
            "Sent {} exchange balances to Kafka as single batch at block height {}",
            length, block_height
        );

        Ok(())
    }
    async fn process_derivative_orderbook(
        &self,
        market_id: &str,
        orderbook: crate::proto::injective::exchange::v1beta1::QueryFullDerivativeOrderbookResponse,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let message = crate::models::KafkaMessage {
            message_type: crate::models::MessageType::DerivativeFullOrderbook,
            block_height: block_height,
            block_time: chrono::Utc::now().timestamp_millis() as u64,
            payload: crate::models::KafkaPayload::DerivativeFullOrderbooks(vec![
                crate::models::FullLimitOrderbookPayload {
                    market_id: market_id.to_string(),
                    bids: orderbook
                        .bids
                        .into_iter()
                        .map(|order| self.convert_limit_order(order))
                        .collect(),
                    asks: orderbook
                        .asks
                        .into_iter()
                        .map(|order| self.convert_limit_order(order))
                        .collect(),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                },
            ]),
        };

        // Send to Kafka
        let results = self.producer.send_batch_current_only(vec![message]).await;
        if let Some(Err(e)) = results.first() {
            error!("Failed to send orderbook to Kafka: {}", e);
        } else {
            info!(
                "Sent orderbook for market {} to Kafka at block height {}",
                market_id, block_height
            );
        }

        Ok(())
    }

    fn convert_derivative_market(
        &self,
        market: FullDerivativeMarket,
    ) -> crate::models::DerivativeMarketPayload {
        // Extract market details
        let market_data = market.market.unwrap_or_default();

        // Extract perpetual market state
        let perp_state = match &market.info {
            Some(Info::PerpetualInfo(state)) => Some(state),
            _ => None,
        };

        // Extract market_info and funding_info separately
        let market_info = perp_state.and_then(|state| state.market_info.as_ref());
        let funding_info = perp_state.and_then(|state| state.funding_info.as_ref());

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

            min_price_tick: market_data.min_price_tick_size,
            min_quantity_tick: market_data.min_quantity_tick_size,
            min_notional: market_data.min_notional,

            // Get fields from market_info
            hfr: market_info
                .map(|info| info.hourly_funding_rate_cap.clone())
                .unwrap_or_default(),
            hir: market_info
                .map(|info| info.hourly_interest_rate.clone())
                .unwrap_or_default(),
            funding_interval: market_info
                .map(|info| info.funding_interval.to_string())
                .unwrap_or_default(),

            // Get fields from funding_info
            cumulative_funding: funding_info
                .map(|info| info.cumulative_funding.clone())
                .unwrap_or_default(),
            cumulative_price: funding_info
                .map(|info| info.cumulative_price.clone())
                .unwrap_or_default(),
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

    fn convert_exchange_balance(
        &self,
        balance: crate::proto::injective::exchange::v1beta1::Balance,
    ) -> crate::models::ExchangeBalancePayload {
        let deposit = balance.deposits.unwrap_or_default();
        crate::models::ExchangeBalancePayload {
            subaccount_id: balance.subaccount_id,
            denom: balance.denom,
            available_balance: deposit.available_balance,
            total_balance: deposit.total_balance,
        }
    }

    fn convert_limit_order(
        &self,
        order: crate::proto::injective::exchange::v1beta1::TrimmedLimitOrder,
    ) -> crate::models::TrimmedLimitOrderPayload {
        crate::models::TrimmedLimitOrderPayload {
            price: order.price,
            quantity: order.quantity,
            order_hash: order.order_hash,
            subaccount_id: order.subaccount_id,
        }
    }
}
