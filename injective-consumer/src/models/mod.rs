use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};


// Market data structure
#[derive(Clone, Debug)]
pub struct MarketData {
    pub market_id: String,
    pub ticker: String,
    pub mark_price: f64,
    pub maintenance_margin_ratio: f64,
    pub cumulative_funding: f64,
    pub block_height: i64,
    pub timestamp: DateTime<Utc>,
}

// Position data structure
#[derive(Clone, Debug)]
pub struct PositionData {
    pub market_id: String,
    pub subaccount_id: String,
    pub is_long: bool,
    pub quantity: f64,
    pub entry_price: f64,
    pub margin: f64,
    pub cumulative_funding_entry: f64,
    pub liquidation_price: f64,
    pub block_height: i64,
    pub timestamp: DateTime<Utc>,
}

/// Wrapper types for Kafka messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaMessage {
    pub message_type: MessageType,
    pub block_height: u64,
    pub block_time: u64,
    pub payload: KafkaPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageType {
    StreamBankBalance,
    StreamSubaccountDeposit,
    StreamPosition,
    StreamSpotOrderbook,
    StreamDerivativeOrderbook,
    StreamOraclePrice,
    SpotTrade,
    DerivativeTrade,
    SpotOrder,
    DerivativeOrder,
    DerivativeMarket,
    ExchangeBalance,
    ExchangePosition,
    DerivativeL3Orderbook,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum KafkaPayload {
    StreamBankBalances(Vec<BankBalancePayload>),
    StreamSubaccountDeposits(Vec<SubaccountDepositPayload>),
    StreamSpotOrderbooks(Vec<OrderbookPayload>),
    StreamDerivativeOrderbooks(Vec<OrderbookPayload>),
    StreamPositions(Vec<PositionPayload>),
    StreamOraclePrices(Vec<OraclePricePayload>),
    SpotTrades(Vec<SpotTradePayload>),
    DerivativeTrades(Vec<DerivativeTradePayload>),
    SpotOrders(Vec<SpotOrderPayload>),
    DerivativeOrders(Vec<DerivativeOrderPayload>),
    DerivativeMarkets(Vec<DerivativeMarketPayload>),
    ExchangePositions(Vec<PositionPayload>),
    ExchangeBalances(Vec<ExchangeBalancePayload>),
    DerivativeL3Orderbooks(Vec<OrderbookL3Payload>),
}

// Custom serializable structs for each message type
// These mirror the protobuf structs but are optimized for JSON serialization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BankBalancePayload {
    pub account: String,
    pub balances: Vec<CoinPayload>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinPayload {
    pub denom: String,
    pub amount: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubaccountDepositPayload {
    pub subaccount_id: String,
    pub denom: String,
    pub available_balance: String,
    pub total_balance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotTradePayload {
    pub market_id: String,
    pub is_buy: bool,
    pub execution_type: String,
    pub quantity: String,
    pub price: String,
    pub subaccount_id: String,
    pub fee: String,
    pub order_hash: String,
    pub fee_recipient_address: String,
    pub cid: String,
    pub trade_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeTradePayload {
    pub market_id: String,
    pub is_buy: bool,
    pub execution_type: String,
    pub subaccount_id: String,
    pub position_delta: PositionDeltaPayload,
    pub payout: String,
    pub fee: String,
    pub order_hash: String,
    pub fee_recipient_address: String,
    pub cid: String,
    pub trade_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionDeltaPayload {
    pub is_long: bool,
    pub execution_quantity: String,
    pub execution_margin: String,
    pub execution_price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpotOrderPayload {
    pub status: String,
    pub order_hash: String,
    pub cid: String,
    pub market_id: String,
    pub subaccount_id: String,
    pub price: String,
    pub quantity: String,
    pub fillable: String,
    pub is_buy: bool,
    pub order_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DerivativeOrderPayload {
    pub status: String,
    pub order_hash: String,
    pub cid: String,
    pub market_id: String,
    pub subaccount_id: String,
    pub price: String,
    pub quantity: String,
    pub margin: String,
    pub fillable: String,
    pub is_buy: bool,
    pub order_type: String,
    pub is_market: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookPayload {
    pub market_id: String,
    pub buy_levels: Vec<PriceLevelPayload>,
    pub sell_levels: Vec<PriceLevelPayload>,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevelPayload {
    pub price: String,
    pub quantity: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionPayload {
    pub market_id: String,
    pub subaccount_id: String,
    pub is_long: bool,
    pub quantity: String,
    pub entry_price: String,
    pub margin: String,
    pub cumulative_funding_entry: String,
    #[serde(default)]
    pub liquidation_price: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OraclePricePayload {
    pub symbol: String,
    pub price: String,
    pub oracle_type: String,
}

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
    pub min_price_tick: String,
    pub min_quantity_tick: String,
    pub min_notional: String,
    pub hfr: String,
    pub hir: String,
    pub funding_interval: String,
    pub cumulative_funding: String,
    pub cumulative_price : String
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeBalancePayload {
    pub subaccount_id: String,
    pub denom: String,
    pub available_balance: String,
    pub total_balance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookL3Payload {
    pub market_id: String,
    pub bids: Vec<TrimmedLimitOrderPayload>,
    pub asks: Vec<TrimmedLimitOrderPayload>,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrimmedLimitOrderPayload {
    pub price: String,
    pub quantity: String,
    pub order_hash: String,
    pub subaccount_id: String,
}
