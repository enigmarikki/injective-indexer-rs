use serde::{Deserialize, Serialize};
// Re-export proto generated types for ease of use
pub use crate::proto::injective::stream::v1beta1::{
    BankBalancesFilter, OraclePriceFilter, OrderbookFilter, OrdersFilter, PositionsFilter,
    StreamRequest, StreamResponse, SubaccountDepositsFilter, TradesFilter,
};

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OraclePricePayload {
    pub symbol: String,
    pub price: String,
    pub oracle_type: String,
}

// Additional data models for the new message types
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

// Functions to convert from proto types to our serializable types
impl From<crate::proto::injective::stream::v1beta1::StreamResponse> for Vec<KafkaMessage> {
    fn from(response: crate::proto::injective::stream::v1beta1::StreamResponse) -> Self {
        let mut messages = Vec::new();
        let block_height = response.block_height;
        let block_time = response.block_time as u64;

        // Process bank balances
        if !response.bank_balances.is_empty() {
            let bank_balances = response
                .bank_balances
                .into_iter()
                .map(|b| BankBalancePayload {
                    account: b.account,
                    balances: b
                        .balances
                        .into_iter()
                        .map(|c| CoinPayload {
                            denom: c.denom,
                            amount: c.amount,
                        })
                        .collect(),
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamBankBalance,
                block_height,
                block_time: block_time,
                payload: KafkaPayload::StreamBankBalances(bank_balances),
            });
        }

        // Process subaccount deposits
        if !response.subaccount_deposits.is_empty() {
            let subaccount_deposits = response
                .subaccount_deposits
                .into_iter()
                .flat_map(|sd| {
                    let subaccount_id = sd.subaccount_id.clone();
                    sd.deposits
                        .into_iter()
                        .map(move |d| SubaccountDepositPayload {
                            subaccount_id: subaccount_id.clone(),
                            denom: d.denom,
                            available_balance: d.deposit.as_ref().map_or_else(
                                || "0".to_string(),
                                |dep| dep.available_balance.clone(),
                            ),
                            total_balance: d
                                .deposit
                                .as_ref()
                                .map_or_else(|| "0".to_string(), |dep| dep.total_balance.clone()),
                        })
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamSubaccountDeposit,
                block_height,
                block_time: block_time,
                payload: KafkaPayload::StreamSubaccountDeposits(subaccount_deposits),
            });
        }

        // Process spot trades
        if !response.spot_trades.is_empty() {
            let spot_trades = response
                .spot_trades
                .into_iter()
                .map(|t| SpotTradePayload {
                    market_id: t.market_id,
                    is_buy: t.is_buy,
                    execution_type: t.execution_type,
                    quantity: t.quantity,
                    price: t.price,
                    subaccount_id: t.subaccount_id,
                    fee: t.fee,
                    order_hash: t.order_hash,
                    fee_recipient_address: t.fee_recipient_address,
                    cid: t.cid,
                    trade_id: t.trade_id,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::SpotTrade,
                block_height,
                block_time: block_time,
                payload: KafkaPayload::SpotTrades(spot_trades),
            });
        }

        // Process derivative trades
        if !response.derivative_trades.is_empty() {
            let derivative_trades = response
                .derivative_trades
                .into_iter()
                .map(|t| DerivativeTradePayload {
                    market_id: t.market_id,
                    is_buy: t.is_buy,
                    execution_type: t.execution_type,
                    subaccount_id: t.subaccount_id,
                    position_delta: PositionDeltaPayload {
                        is_long: t.position_delta.as_ref().map_or(false, |pd| pd.is_long),
                        execution_quantity: t
                            .position_delta
                            .as_ref()
                            .map_or_else(|| "0".to_string(), |pd| pd.execution_quantity.clone()),
                        execution_margin: t
                            .position_delta
                            .as_ref()
                            .map_or_else(|| "0".to_string(), |pd| pd.execution_margin.clone()),
                        execution_price: t
                            .position_delta
                            .as_ref()
                            .map_or_else(|| "0".to_string(), |pd| pd.execution_price.clone()),
                    },
                    payout: t.payout,
                    fee: t.fee,
                    order_hash: t.order_hash,
                    fee_recipient_address: t.fee_recipient_address,
                    cid: t.cid,
                    trade_id: t.trade_id,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::DerivativeTrade,
                block_height,
                block_time: block_time,
                payload: KafkaPayload::DerivativeTrades(derivative_trades),
            });
        }

        // Process spot orders
        if !response.spot_orders.is_empty() {
            let spot_orders = response
                .spot_orders
                .into_iter()
                .map(|o| {
                    let status_str = match o.status {
                        0 => "Unspecified",
                        1 => "Booked",
                        2 => "Matched",
                        3 => "Cancelled",
                        _ => "Unknown",
                    };

                    let order_info = o.order.as_ref().and_then(|order| order.order.as_ref());
                    let market_id = o
                        .order
                        .as_ref()
                        .map_or_else(|| "".to_string(), |order| order.market_id.clone());

                    SpotOrderPayload {
                        status: status_str.to_string(),
                        order_hash: o.order_hash,
                        cid: o.cid,
                        market_id,
                        subaccount_id: o
                            .order
                            .as_ref()
                            .and_then(|order| order.order.as_ref())
                            .and_then(|order| order.order_info.as_ref())
                            .map_or_else(|| "".to_string(), |info| info.subaccount_id.clone()),
                        price: order_info
                            .and_then(|order| order.order_info.as_ref())
                            .map_or_else(|| "0".to_string(), |info| info.price.clone()),
                        quantity: order_info
                            .and_then(|order| order.order_info.as_ref())
                            .map_or_else(|| "0".to_string(), |info| info.quantity.clone()),
                        fillable: o
                            .order
                            .as_ref()
                            .and_then(|order| order.order.as_ref())
                            .map_or_else(|| "0".to_string(), |o| o.fillable.clone()),
                        is_buy: order_info
                            .and_then(|order| order.order_info.as_ref())
                            .map_or(false, |_| {
                                match o
                                    .order
                                    .as_ref()
                                    .and_then(|o| o.order.as_ref())
                                    .map(|o| o.order_type)
                                {
                                    Some(1) | Some(3) | Some(5) | Some(7) | Some(9) => true, // BUY, STOP_BUY, TAKE_BUY, BUY_PO, BUY_ATOMIC
                                    _ => false,
                                }
                            }),
                        order_type: o.order.as_ref().and_then(|o| o.order.as_ref()).map_or(
                            "Unknown".to_string(),
                            |o| match o.order_type {
                                0 => "Unspecified".to_string(),
                                1 => "Buy".to_string(),
                                2 => "Sell".to_string(),
                                3 => "StopBuy".to_string(),
                                4 => "StopSell".to_string(),
                                5 => "TakeBuy".to_string(),
                                6 => "TakeSell".to_string(),
                                7 => "BuyPO".to_string(),
                                8 => "SellPO".to_string(),
                                9 => "BuyAtomic".to_string(),
                                10 => "SellAtomic".to_string(),
                                n => format!("Unknown({})", n),
                            },
                        ),
                    }
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::SpotOrder,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::SpotOrders(spot_orders),
            });
        }

        // Process derivative orders
        if !response.derivative_orders.is_empty() {
            let derivative_orders = response
                .derivative_orders
                .into_iter()
                .map(|o| {
                    let status_str = match o.status {
                        0 => "Unspecified",
                        1 => "Booked",
                        2 => "Matched",
                        3 => "Cancelled",
                        _ => "Unknown",
                    };

                    let order_info = o.order.as_ref().and_then(|order| order.order.as_ref());
                    let order_info_inner = order_info.and_then(|o| o.order_info.as_ref());
                    let market_id = o
                        .order
                        .as_ref()
                        .map_or_else(|| "".to_string(), |order| order.market_id.clone());
                    let is_market = o.order.as_ref().map_or(false, |order| order.is_market);

                    DerivativeOrderPayload {
                        status: status_str.to_string(),
                        order_hash: o.order_hash,
                        cid: o.cid,
                        market_id,
                        subaccount_id: order_info_inner
                            .map_or_else(|| "".to_string(), |info| info.subaccount_id.clone()),
                        price: order_info_inner
                            .map_or_else(|| "0".to_string(), |info| info.price.clone()),
                        quantity: order_info_inner
                            .map_or_else(|| "0".to_string(), |info| info.quantity.clone()),
                        margin: order_info
                            .map_or_else(|| "0".to_string(), |info| info.margin.clone()),
                        fillable: order_info
                            .map_or_else(|| "0".to_string(), |info| info.fillable.clone()),
                        is_buy: order_info_inner.map_or(false, |_| {
                            match order_info.map(|o| o.order_type) {
                                Some(1) | Some(3) | Some(5) | Some(7) | Some(9) => true, // BUY, STOP_BUY, TAKE_BUY, BUY_PO, BUY_ATOMIC
                                _ => false,
                            }
                        }),
                        order_type: order_info.map_or("Unknown".to_string(), |o| {
                            match o.order_type {
                                0 => "Unspecified".to_string(),
                                1 => "Buy".to_string(),
                                2 => "Sell".to_string(),
                                3 => "StopBuy".to_string(),
                                4 => "StopSell".to_string(),
                                5 => "TakeBuy".to_string(),
                                6 => "TakeSell".to_string(),
                                7 => "BuyPO".to_string(),
                                8 => "SellPO".to_string(),
                                9 => "BuyAtomic".to_string(),
                                10 => "SellAtomic".to_string(),
                                n => format!("Unknown({})", n),
                            }
                        }),
                        is_market,
                    }
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::DerivativeOrder,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::DerivativeOrders(derivative_orders),
            });
        }

        // Process spot orderbook updates
        if !response.spot_orderbook_updates.is_empty() {
            let spot_orderbooks = response
                .spot_orderbook_updates
                .into_iter()
                .map(|o| OrderbookPayload {
                    market_id: o
                        .orderbook
                        .as_ref()
                        .map_or_else(|| "".to_string(), |ob| ob.market_id.clone()),
                    buy_levels: o.orderbook.as_ref().map_or_else(Vec::new, |ob| {
                        ob.buy_levels
                            .iter()
                            .map(|l| PriceLevelPayload {
                                price: l.p.clone(),
                                quantity: l.q.clone(),
                            })
                            .collect()
                    }),
                    sell_levels: o.orderbook.as_ref().map_or_else(Vec::new, |ob| {
                        ob.sell_levels
                            .iter()
                            .map(|l| PriceLevelPayload {
                                price: l.p.clone(),
                                quantity: l.q.clone(),
                            })
                            .collect()
                    }),
                    sequence: o.seq,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamSpotOrderbook,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::StreamSpotOrderbooks(spot_orderbooks),
            });
        }

        // Process derivative orderbook updates
        if !response.derivative_orderbook_updates.is_empty() {
            let derivative_orderbooks = response
                .derivative_orderbook_updates
                .into_iter()
                .map(|o| OrderbookPayload {
                    market_id: o
                        .orderbook
                        .as_ref()
                        .map_or_else(|| "".to_string(), |ob| ob.market_id.clone()),
                    buy_levels: o.orderbook.as_ref().map_or_else(Vec::new, |ob| {
                        ob.buy_levels
                            .iter()
                            .map(|l| PriceLevelPayload {
                                price: l.p.clone(),
                                quantity: l.q.clone(),
                            })
                            .collect()
                    }),
                    sell_levels: o.orderbook.as_ref().map_or_else(Vec::new, |ob| {
                        ob.sell_levels
                            .iter()
                            .map(|l| PriceLevelPayload {
                                price: l.p.clone(),
                                quantity: l.q.clone(),
                            })
                            .collect()
                    }),
                    sequence: o.seq,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamDerivativeOrderbook,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::StreamDerivativeOrderbooks(derivative_orderbooks),
            });
        }

        // Process positions
        if !response.positions.is_empty() {
            let positions = response
                .positions
                .into_iter()
                .map(|p| PositionPayload {
                    market_id: p.market_id,
                    subaccount_id: p.subaccount_id,
                    is_long: p.is_long,
                    quantity: p.quantity,
                    entry_price: p.entry_price,
                    margin: p.margin,
                    cumulative_funding_entry: p.cumulative_funding_entry,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamPosition,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::StreamPositions(positions),
            });
        }

        // Process oracle prices
        if !response.oracle_prices.is_empty() {
            let oracle_prices = response
                .oracle_prices
                .into_iter()
                .map(|p| OraclePricePayload {
                    symbol: p.symbol,
                    price: p.price,
                    oracle_type: p.r#type,
                })
                .collect();

            messages.push(KafkaMessage {
                message_type: MessageType::StreamOraclePrice,
                block_height,
                block_time: block_time,

                payload: KafkaPayload::StreamOraclePrices(oracle_prices),
            });
        }

        messages
    }
}

// Define helper builder functions for StreamRequest
pub fn build_stream_request() -> StreamRequest {
    StreamRequest {
        bank_balances_filter: None,
        subaccount_deposits_filter: None,
        spot_trades_filter: None,
        derivative_trades_filter: None,
        spot_orders_filter: None,
        derivative_orders_filter: None,
        spot_orderbooks_filter: None,
        derivative_orderbooks_filter: None,
        positions_filter: None,
        oracle_price_filter: None,
    }
}
