use tokio_stream::StreamExt;
use tonic::Request;
use crate::proto::injective::stream::v1beta1::{
    BankBalancesFilter, OraclePriceFilter, OrderbookFilter,
    OrdersFilter, PositionsFilter, StreamRequest, SubaccountDepositsFilter, TradesFilter,
};
pub enum StreamFilterCondition {
    BankBalance,
    SubaccountDeposit,
    SpotOrders,
    DerivativeOrders,
    Positions,
    Oracles,
    DerivativeOrderbook,
    SpotOrderbook,
    SpotTrades,
    DerivativeTrades,
}

pub fn create_stream_request(filter_domain: StreamFilterCondition) -> Result<StreamRequest, Box<dyn std::error::Error>> {
    let wild_card_match = vec!["*".to_string()];
    let mut empty_stream_request: StreamRequest = StreamRequest {
        bank_balances_filter: None,
        subaccount_deposits_filter: None,
        spot_orders_filter: None,
        derivative_orders_filter: None,
        positions_filter: None,
        oracle_price_filter: None,
        derivative_orderbooks_filter: None,
        spot_orderbooks_filter: None,
        spot_trades_filter: None,
        derivative_trades_filter: None,
    };
    match filter_domain {
        StreamFilterCondition::BankBalance => {
            empty_stream_request.bank_balances_filter = Some(BankBalancesFilter {
                accounts: wild_card_match,
            })
        }
        StreamFilterCondition::SubaccountDeposit => {
            empty_stream_request.subaccount_deposits_filter = Some(SubaccountDepositsFilter {
                subaccount_ids: wild_card_match,
            })
        }
        StreamFilterCondition::SpotOrders => {
            empty_stream_request.spot_orders_filter = Some(OrdersFilter {
                subaccount_ids: wild_card_match.clone(),
                market_ids: wild_card_match,
            })
        }
        StreamFilterCondition::DerivativeOrders => {
            empty_stream_request.derivative_orders_filter = Some(OrdersFilter {
                subaccount_ids: wild_card_match.clone(),
                market_ids: wild_card_match,
            })
        }
        StreamFilterCondition::Positions => {
            empty_stream_request.positions_filter = Some(PositionsFilter {
                subaccount_ids: wild_card_match.clone(),
                market_ids: wild_card_match,
            })
        }
        StreamFilterCondition::Oracles => {
            empty_stream_request.oracle_price_filter = Some(OraclePriceFilter {
                symbol: wild_card_match,
            })
        }
        StreamFilterCondition::DerivativeOrderbook => {
            empty_stream_request.derivative_orderbooks_filter = Some(OrderbookFilter {
                market_ids: wild_card_match,
            })
        }
        StreamFilterCondition::SpotOrderbook => {
            empty_stream_request.spot_orderbooks_filter = Some(OrderbookFilter {
                market_ids: wild_card_match,
            })
        }
        StreamFilterCondition::SpotTrades => {
            empty_stream_request.spot_trades_filter = Some(TradesFilter {
                market_ids: wild_card_match.clone(),
                subaccount_ids: wild_card_match,
            })
        }
        StreamFilterCondition::DerivativeTrades => {
            empty_stream_request.derivative_trades_filter = Some(TradesFilter {
                market_ids: wild_card_match.clone(),
                subaccount_ids: wild_card_match,
            })
        }
    }
    Ok(empty_stream_request)
}

