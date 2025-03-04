use crate::compute::{calculate_liquidation_price, is_liquidatable};
use crate::consumer::MessageProcessor;
use crate::models::{KafkaMessage, KafkaPayload};
use async_trait::async_trait;
use chrono::{LocalResult, TimeZone, Utc};
use log::{error, info, warn};
use scylla::{Session, SessionBuilder};
use std::error::Error;
use std::sync::Arc;

pub struct ScyllaDBProcessor {
    session: Arc<Session>,
}

impl ScyllaDBProcessor {
    pub async fn new(nodes: Vec<String>) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let session = SessionBuilder::new().known_nodes(&nodes).build().await?;
        Self::initialize_schema(&session).await?;
        Ok(ScyllaDBProcessor {
            session: Arc::new(session),
        })
    }

    async fn initialize_schema(session: &Session) -> Result<(), Box<dyn Error + Send + Sync>> {
        session.query_unpaged(
            "CREATE KEYSPACE IF NOT EXISTS injective WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}",
            &[]
        ).await?;

        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS injective.markets (
                market_id text,
                block_height bigint,
                timestamp timestamp,
                ticker text,
                mark_price text,
                maintenance_margin_ratio text,
                cumulative_funding text,
                PRIMARY KEY (market_id, block_height)
            ) WITH CLUSTERING ORDER BY (block_height DESC)",
                &[],
            )
            .await?;

        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS injective.positions (
                market_id text,
                subaccount_id text,
                block_height bigint,
                timestamp timestamp,
                is_long boolean,
                quantity text,
                entry_price text,
                margin text,
                cumulative_funding_entry text,
                liquidation_price text,
                PRIMARY KEY ((market_id, subaccount_id), block_height)
            ) WITH CLUSTERING ORDER BY (block_height DESC)",
                &[],
            )
            .await?;

        session
            .query_unpaged(
                "CREATE TABLE IF NOT EXISTS injective.liquidatable_positions (
                market_id text,
                subaccount_id text,
                block_height bigint,
                timestamp timestamp,
                is_long boolean,
                quantity text,
                entry_price text,
                margin text,
                liquidation_price text,
                mark_price text,
                PRIMARY KEY (market_id, subaccount_id)
            )",
                &[],
            )
            .await?;

        Ok(())
    }

    async fn process_derivative_market(
        &self,
        market: &crate::models::DerivativeMarketPayload,
        block_height: i64,
        timestamp: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let cumulative_funding = market.cumulative_funding.parse::<f64>().unwrap_or(0.0);
        let mark_price = market.mark_price.parse::<f64>().unwrap_or(0.0);
        let maintenance_margin_ratio = market
            .maintenance_margin_ratio
            .parse::<f64>()
            .unwrap_or(0.05);

        if mark_price <= 0.0 || maintenance_margin_ratio <= 0.0 {
            warn!("Invalid market data for {}, skipping", market.market_id);
            return Ok(());
        }

        // Use timestamp_opt to avoid deprecation.
        let datetime_millis = match Utc.timestamp_opt(timestamp, 0) {
            LocalResult::Single(dt) => dt.timestamp_millis(),
            _ => 0,
        };

        let market_query = "INSERT INTO injective.markets (
            market_id, block_height, timestamp, ticker, mark_price, maintenance_margin_ratio, cumulative_funding
        ) VALUES (?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query_unpaged(
                market_query,
                (
                    &market.market_id,
                    block_height,
                    datetime_millis,
                    &market.ticker,
                    &market.mark_price,
                    &market.maintenance_margin_ratio,
                    cumulative_funding.to_string(),
                ),
            )
            .await
            .map_err(|e| {
                error!("Failed to insert market data: {}", e);
                e
            })?;

        // Fetch positions for this market.
        let positions_query = "SELECT subaccount_id, is_long, quantity, entry_price, margin, cumulative_funding_entry, block_height 
            FROM injective.positions 
            WHERE market_id = ? 
            PER PARTITION LIMIT 1";

        let positions_result = self
            .session
            .query_unpaged(positions_query, (&market.market_id,))
            .await
            .map_err(|e| {
                error!(
                    "Failed to fetch positions for market {}: {}",
                    market.market_id, e
                );
                e
            })?;

        // Convert the QueryResult into rows via into_rows_result()
        let rows_result = positions_result.into_rows_result().map_err(|e| {
            error!("Failed to convert query result into rows: {}", e);
            e
        })?;
        let mut rows_iter =
            rows_result.rows::<(String, bool, String, String, String, String, i64)>()?;
        while let Some(row) = rows_iter.next().transpose()? {
            let (
                subaccount_id,
                is_long,
                quantity,
                entry_price,
                margin,
                cumulative_funding_entry,
                pos_block_height,
            ) = row;

            let quantity_val = quantity.parse::<f64>().unwrap_or(0.0);
            let entry_price_val = entry_price.parse::<f64>().unwrap_or(0.0);
            let margin_val = margin.parse::<f64>().unwrap_or(0.0);
            let cumulative_funding_entry_val =
                cumulative_funding_entry.parse::<f64>().unwrap_or(0.0);

            if quantity_val <= 0.0 || entry_price_val <= 0.0 || margin_val <= 0.0 {
                warn!(
                    "Invalid position data for market {} subaccount {}, skipping",
                    market.market_id, subaccount_id
                );
                continue;
            }

            let liquidation_price = calculate_liquidation_price(
                is_long,
                entry_price_val,
                margin_val,
                quantity_val,
                maintenance_margin_ratio,
                cumulative_funding,
                cumulative_funding_entry_val,
            );

            let update_query = "UPDATE injective.positions 
                SET liquidation_price = ? 
                WHERE market_id = ? AND subaccount_id = ? AND block_height = ?";

            if let Err(e) = self
                .session
                .query_unpaged(
                    update_query,
                    (
                        liquidation_price.to_string(),
                        &market.market_id,
                        &subaccount_id,
                        pos_block_height,
                    ),
                )
                .await
            {
                error!("Failed to update liquidation price: {}", e);
                continue;
            }

            let liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);
            if liquidatable {
                let liquidatable_query = "INSERT INTO injective.liquidatable_positions (
                    market_id, subaccount_id, block_height, timestamp, is_long, quantity, 
                    entry_price, margin, liquidation_price, mark_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

                if let Err(e) = self
                    .session
                    .query_unpaged(
                        liquidatable_query,
                        (
                            &market.market_id,
                            &subaccount_id,
                            pos_block_height,
                            datetime_millis,
                            is_long,
                            quantity,
                            entry_price,
                            margin,
                            liquidation_price.to_string(),
                            mark_price.to_string(),
                        ),
                    )
                    .await
                {
                    error!("Failed to insert liquidatable position: {}", e);
                    continue;
                }

                info!("Liquidatable position inserted: market={}, subaccount={}, liq_price={}, mark_price={}",
                      market.market_id, subaccount_id, liquidation_price, mark_price);
            } else {
                let delete_query = "DELETE FROM injective.liquidatable_positions 
                    WHERE market_id = ? AND subaccount_id = ?";

                if let Err(e) = self
                    .session
                    .query_unpaged(delete_query, (&market.market_id, &subaccount_id))
                    .await
                {
                    error!("Failed to delete non-liquidatable position: {}", e);
                }
            }
        }

        Ok(())
    }

    async fn process_position(
        &self,
        position: &crate::models::PositionPayload,
        block_height: i64,
        timestamp: i64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let is_long = position.is_long;
        let quantity = position.quantity.parse::<f64>().unwrap_or(0.0);
        let entry_price = position.entry_price.parse::<f64>().unwrap_or(0.0);
        let margin = position.margin.parse::<f64>().unwrap_or(0.0);
        let cumulative_funding_entry = position
            .cumulative_funding_entry
            .parse::<f64>()
            .unwrap_or(0.0);

        if quantity <= 0.0 || entry_price <= 0.0 || margin <= 0.0 {
            warn!(
                "Invalid position data for market {} subaccount {}, skipping",
                position.market_id, position.subaccount_id
            );
            return Ok(());
        }

        let market_query = "SELECT mark_price, maintenance_margin_ratio, cumulative_funding 
            FROM injective.markets 
            WHERE market_id = ? 
            LIMIT 1";

        let market_result = self
            .session
            .query_unpaged(market_query, (&position.market_id,))
            .await
            .map_err(|e| {
                error!("Failed to fetch market data: {}", e);
                e
            })?;

        let (mark_price, maintenance_margin_ratio, market_cumulative_funding) = {
            // Convert the QueryResult into rows.
            if let Ok(rows_result) = market_result.into_rows_result() {
                // Use a tuple of &str for lifetime convenience.
                let mut rows_iter = rows_result.rows::<(&str, &str, &str)>()?;
                if let Some(row) = rows_iter.next().transpose()? {
                    let (mp, mmr, mcf) = row;
                    (
                        mp.parse::<f64>().unwrap_or(0.0),
                        mmr.parse::<f64>().unwrap_or(0.05),
                        mcf.parse::<f64>().unwrap_or(0.0),
                    )
                } else {
                    (0.0, 0.05, 0.0)
                }
            } else {
                (0.0, 0.05, 0.0)
            }
        };

        let liquidation_price = calculate_liquidation_price(
            is_long,
            entry_price,
            margin,
            quantity,
            maintenance_margin_ratio,
            market_cumulative_funding,
            cumulative_funding_entry,
        );

        let datetime_millis = match Utc.timestamp_opt(timestamp as i64, 0) {
            LocalResult::Single(dt) => dt.timestamp_millis(),
            _ => 0,
        };

        let position_query = "INSERT INTO injective.positions (
            market_id, subaccount_id, block_height, timestamp, is_long, quantity, 
            entry_price, margin, cumulative_funding_entry, liquidation_price
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        self.session
            .query_unpaged(
                position_query,
                (
                    &position.market_id,
                    &position.subaccount_id,
                    block_height,
                    datetime_millis,
                    is_long,
                    &position.quantity,
                    &position.entry_price,
                    &position.margin,
                    &position.cumulative_funding_entry,
                    liquidation_price.to_string(),
                ),
            )
            .await
            .map_err(|e| {
                error!("Failed to insert position: {}", e);
                e
            })?;

        let liquidatable = is_liquidatable(is_long, liquidation_price, mark_price);

        if liquidatable {
            let liquidatable_query = "INSERT INTO injective.liquidatable_positions (
                market_id, subaccount_id, block_height, timestamp, is_long, quantity, 
                entry_price, margin, liquidation_price, mark_price
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            if let Err(e) = self
                .session
                .query_unpaged(
                    liquidatable_query,
                    (
                        &position.market_id,
                        &position.subaccount_id,
                        block_height,
                        datetime_millis,
                        is_long,
                        &position.quantity,
                        &position.entry_price,
                        &position.margin,
                        liquidation_price.to_string(),
                        mark_price.to_string(),
                    ),
                )
                .await
            {
                error!("Failed to insert liquidatable position: {}", e);
            } else {
                info!("Liquidatable position updated: market={}, subaccount={}, liq_price={}, mark_price={}",
                    position.market_id, position.subaccount_id, liquidation_price, mark_price);
            }
        } else {
            let delete_query = "DELETE FROM injective.liquidatable_positions 
                WHERE market_id = ? AND subaccount_id = ?";
            if let Err(e) = self
                .session
                .query_unpaged(delete_query, (&position.market_id, &position.subaccount_id))
                .await
            {
                error!("Failed to delete non-liquidatable position: {}", e);
            }
        }

        Ok(())
    }

    pub async fn get_liquidatable_positions(
        &self,
    ) -> Result<Vec<(String, String, f64, f64)>, Box<dyn Error + Send + Sync>> {
        let query = "SELECT market_id, subaccount_id, liquidation_price, mark_price FROM injective.liquidatable_positions";
        let result = self.session.query_unpaged(query, &[]).await.map_err(|e| {
            error!("Failed to query liquidatable positions: {}", e);
            e
        })?;
        let mut positions = Vec::new();
        let rows_result = result.into_rows_result().map_err(|e| {
            error!("Failed to convert query result into rows: {}", e);
            e
        })?;
        let mut rows_iter = rows_result.rows::<(String, String, String, String)>()?;
        while let Some(row) = rows_iter.next().transpose()? {
            let (market_id, subaccount_id, liq_price_str, mark_price_str) = row;
            let liquidation_price = liq_price_str.parse::<f64>().unwrap_or(0.0);
            let mark_price = mark_price_str.parse::<f64>().unwrap_or(0.0);
            positions.push((market_id, subaccount_id, liquidation_price, mark_price));
        }
        Ok(positions)
    }
}

#[async_trait]
impl MessageProcessor for ScyllaDBProcessor {
    async fn process_message(
        &self,
        message: KafkaMessage,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let block_height = message.block_height as i64;
        let timestamp = message.block_time as i64;

        match &message.payload {
            KafkaPayload::DerivativeMarkets(markets) => {
                for market in markets {
                    if let Err(e) = self
                        .process_derivative_market(market, block_height, timestamp)
                        .await
                    {
                        error!("ScyllaDB: Error processing derivative market: {}", e);
                    }
                }
            }
            KafkaPayload::ExchangePositions(positions) => {
                for position in positions {
                    if let Err(e) = self
                        .process_position(position, block_height, timestamp)
                        .await
                    {
                        error!("ScyllaDB: Error processing position: {}", e);
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }
}
