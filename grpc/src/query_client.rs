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
        let client = QueryClient::connect(config.query_endpoint.clone()).await?;
        info!(
            "Connected to exchange query service: {}",
            config.query_endpoint
        );

        // Create a properly configured HTTP client
        let http_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        // Derive Tendermint RPC endpoint from query endpoint
        // Assuming format "http://host:port" and we want to connect to port 36657
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
                Ok(height) => height,
                Err(e) => {
                    error!("Failed to get current block height: {}", e);
                    0 // Fallback to 0 if we can't get the height
                }
            };

            // Fetch derivative markets
            match self
                .client
                .get_derivative_markets(Some("Active".to_string()))
                .await
            {
                Ok(markets) => {
                    // Handle markets data and send to Kafka
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
            match self.client.get_exchange_balances().await {
                Ok(balances) => {
                    // Handle balances data and send to Kafka
                    self.process_exchange_balances(balances, block_height)
                        .await?;
                }
                Err(e) => {
                    error!("Failed to fetch exchange balances: {}", e);
                }
            }

            // Now fetch orderbooks for each active derivative market
            if let Ok(markets) = self
                .client
                .get_derivative_markets(Some("Active".to_string()))
                .await
            {
                for market in markets {
                    if let Some(market_data) = market.market {
                        if let Ok(orderbook) = self
                            .client
                            .get_full_derivative_orderbook(&market_data.market_id)
                            .await
                        {
                            self.process_derivative_orderbook(
                                &market_data.market_id,
                                orderbook,
                                block_height,
                            )
                            .await?;
                        }
                    }
                }
            }
        }
    }

    async fn process_derivative_markets(
        &self,
        markets: Vec<FullDerivativeMarket>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = markets
            .into_iter()
            .map(|market| {
                crate::models::KafkaMessage {
                    message_type: crate::models::MessageType::DerivativeMarket,
                    block_height: block_height, // Now using actual block height

                    block_time: chrono::Utc::now().timestamp_millis() as u64,
                    payload: crate::models::KafkaPayload::DerivativeMarkets(vec![
                        self.convert_derivative_market(market)
                    ]),
                }
            })
            .collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch_low_latency(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!(
                "Sent {}/{} derivative markets to Kafka at block height {}",
                success_count,
                results.len(),
                block_height
            );
        }

        Ok(())
    }

    async fn process_positions(
        &self,
        positions: Vec<DerivativePosition>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = positions
            .into_iter()
            .map(|position| crate::models::KafkaMessage {
                message_type: crate::models::MessageType::ExchangePosition,
                block_height: block_height,

                block_time: chrono::Utc::now().timestamp_millis() as u64,
                payload: crate::models::KafkaPayload::ExchangePositions(vec![
                    self.convert_position(position)
                ]),
            })
            .collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch_low_latency(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!(
                "Sent {}/{} positions to Kafka at block height {}",
                success_count,
                results.len(),
                block_height
            );
        }

        Ok(())
    }

    async fn process_exchange_balances(
        &self,
        balances: Vec<crate::proto::injective::exchange::v1beta1::Balance>,
        block_height: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Convert to Kafka message format
        let messages = balances
            .into_iter()
            .map(|balance| crate::models::KafkaMessage {
                message_type: crate::models::MessageType::ExchangeBalance,
                block_height: block_height,

                block_time: chrono::Utc::now().timestamp_millis() as u64,
                payload: crate::models::KafkaPayload::ExchangeBalances(vec![
                    self.convert_exchange_balance(balance)
                ]),
            })
            .collect::<Vec<_>>();

        // Send to Kafka
        if !messages.is_empty() {
            let results = self.producer.send_batch_low_latency(messages).await;
            let success_count = results.iter().filter(|r| r.is_ok()).count();
            info!(
                "Sent {}/{} exchange balances to Kafka at block height {}",
                success_count,
                results.len(),
                block_height
            );
        }

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
        let results = self.producer.send_batch_low_latency(vec![message]).await;
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
