// src/models/mod.rs - Data models for injective-consumer-base

use serde::{Deserialize, Serialize};

/// Main Kafka message type that wraps different kinds of payloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    pub message_type: MessageType,
    pub block_height: i64,
    pub block_time: i64,
    pub payload: KafkaPayload,
}

/// Enumeration of different message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    DerivativeMarket,
    Position,
    ExchangeBalance,
    DerivativeL3Orderbook,
    BankBalance,
    DerivativeTrade,
    SpotTrade,
    DerivativeOrder,
    SpotOrder,
    SubaccountDeposit,
    OraclePrice,
}

/// Enumeration of different payload types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KafkaPayload {
    DerivativeMarkets(Vec<DerivativeMarketPayload>),
    Positions(Vec<PositionPayload>),
    ExchangeBalances(Vec<ExchangeBalancePayload>),
    DerivativeL3Orderbooks(Vec<OrderbookL3Payload>),
    BankBalances(Vec<BankBalancePayload>),
    DerivativeTrades(Vec<DerivativeTradePayload>),
    SpotTrades(Vec<SpotTradePayload>),
    DerivativeOrders(Vec<DerivativeOrderPayload>),
    SpotOrders(Vec<SpotOrderPayload>),
    SubaccountDeposits(Vec<SubaccountDepositPayload>),
    OraclePrices(Vec<OraclePricePayload>),
}

/// Derivative market data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeMarketPayload {
    pub market_id: String,
    pub ticker: String,
    pub oracle_base: String,
    pub oracle_quote: String,
    pub quote_denom: String,
    pub maker_fee_rate: String,
    pub taker_fee_rate: String,
    pub initial_margin_ratio: String,
    pub maintenance_margin_ratio: String,
    pub is_perpetual: bool,
    pub status: String,
    pub mark_price: String,
}

/// Position data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionPayload {
    pub market_id: String,
    pub subaccount_id: String,
    pub is_long: bool,
    pub quantity: String,
    pub entry_price: String,
    pub margin: String,
    pub cumulative_funding_entry: String,
}

/// Exchange balance data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeBalancePayload {
    pub subaccount_id: String,
    pub denom: String,
    pub available_balance: String,
    pub total_balance: String,
}

/// Trimmed limit order data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrimmedLimitOrderPayload {
    pub price: String,
    pub quantity: String,
    pub order_hash: String,
    pub subaccount_id: String,
}

/// Full orderbook data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookL3Payload {
    pub market_id: String,
    pub bids: Vec<TrimmedLimitOrderPayload>,
    pub asks: Vec<TrimmedLimitOrderPayload>,
    pub timestamp: i64,
}

/// Bank balance data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankBalancePayload {
    pub account: String,
    pub denom: String,
    pub balance: String,
}

/// Derivative trade data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeTradePayload {
    pub market_id: String,
    pub is_buy: bool,
    pub price: String,
    pub quantity: String,
    pub execution_type: String,
    pub trade_id: String,
    pub subaccount_id: String,
    pub fee: String,
    pub executed_at: i64,
}

/// Spot trade data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotTradePayload {
    pub market_id: String,
    pub is_buy: bool,
    pub price: String,
    pub quantity: String,
    pub execution_type: String,
    pub trade_id: String,
    pub subaccount_id: String,
    pub fee: String,
    pub executed_at: i64,
}

/// Derivative order data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeOrderPayload {
    pub market_id: String,
    pub is_buy: bool,
    pub price: String,
    pub quantity: String,
    pub leverage: String,
    pub order_hash: String,
    pub subaccount_id: String,
    pub order_type: String,
    pub status: String,
    pub created_at: i64,
}

/// Spot order data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotOrderPayload {
    pub market_id: String,
    pub is_buy: bool,
    pub price: String,
    pub quantity: String,
    pub order_hash: String,
    pub subaccount_id: String,
    pub order_type: String,
    pub status: String,
    pub created_at: i64,
}

/// Subaccount deposit data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubaccountDepositPayload {
    pub subaccount_id: String,
    pub denom: String,
    pub deposit_type: String,
    pub amount: String,
}

/// Oracle price data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OraclePricePayload {
    pub symbol: String,
    pub price: String,
    pub type_field: String,
}

/// Stream filter types used for subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankBalancesFilter {
    pub accounts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradesFilter {
    pub market_ids: Vec<String>,
    pub subaccount_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookFilter {
    pub market_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrdersFilter {
    pub market_ids: Vec<String>,
    pub subaccount_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubaccountDepositsFilter {
    pub subaccount_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionsFilter {
    pub market_ids: Vec<String>,
    pub subaccount_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OraclePriceFilter {
    pub symbol: Vec<String>,
}

/// Stream request structure (simplified)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRequest {
    pub bank_balances_filter: Option<BankBalancesFilter>,
    pub spot_trades_filter: Option<TradesFilter>,
    pub derivative_trades_filter: Option<TradesFilter>,
    pub spot_orderbooks_filter: Option<OrderbookFilter>,
    pub derivative_orderbooks_filter: Option<OrderbookFilter>,
    pub spot_orders_filter: Option<OrdersFilter>,
    pub derivative_orders_filter: Option<OrdersFilter>,
    pub subaccount_deposits_filter: Option<SubaccountDepositsFilter>,
    pub positions_filter: Option<PositionsFilter>,
    pub oracle_price_filter: Option<OraclePriceFilter>,
}

// Helper function to build an empty stream request
pub fn build_stream_request() -> StreamRequest {
    StreamRequest {
        bank_balances_filter: None,
        spot_trades_filter: None,
        derivative_trades_filter: None,
        spot_orderbooks_filter: None,
        derivative_orderbooks_filter: None,
        spot_orders_filter: None,
        derivative_orders_filter: None,
        subaccount_deposits_filter: None,
        positions_filter: None,
        oracle_price_filter: None,
    }
}

/// Stream response structure (simplified)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamResponse {
    pub block_height: i64,
    pub block_time: i64,
    // Other fields would be added as needed
}

// Implement conversion from StreamResponse to Vec<KafkaMessage>
impl From<StreamResponse> for Vec<KafkaMessage> {
    fn from(response: StreamResponse) -> Self {
        // This would be implemented based on the actual structure of StreamResponse
        // For simplicity, returning an empty vector
        Vec::new()
    }
}