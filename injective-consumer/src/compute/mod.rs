// Calculates the liquidation price for a position
pub fn calculate_liquidation_price(
    is_long: bool,
    entry_price: f64,
    margin: f64,
    quantity: f64,
    maintenance_margin_ratio: f64,
    market_cumulative_funding: f64,
    position_cumulative_funding_entry: f64,
) -> f64 {
    // Skip calculation if any required data is missing or invalid
    if quantity <= 0.0 || entry_price <= 0.0 || maintenance_margin_ratio <= 0.0 {
        return 0.0;
    }

    // Calculate funding-adjusted margin
    let unrealized_funding_payment =
        quantity * (market_cumulative_funding - position_cumulative_funding_entry);

    // For longs, Margin -= Funding
    // For shorts, Margin += Funding
    let adjusted_margin = if is_long {
        margin - unrealized_funding_payment
    } else {
        margin + unrealized_funding_payment
    };

    // Calculate unit margin (margin per contract)
    let unit_margin = adjusted_margin / quantity;

    // Calculate liquidation price
    if is_long {
        // For long positions: liquidation_price = (entry_price - unit_margin) / (1 - maintenance_margin_ratio)
        (entry_price - unit_margin) / (1.0 - maintenance_margin_ratio)
    } else {
        // For short positions: liquidation_price = (entry_price + unit_margin) / (1 + maintenance_margin_ratio)
        (entry_price + unit_margin) / (1.0 + maintenance_margin_ratio)
    }
}

/// Checks if a position is liquidatable
pub fn is_liquidatable(is_long: bool, liquidation_price: f64, mark_price: f64) -> bool {
    if is_long {
        // Long position gets liquidated when price falls to or below liquidation price
        mark_price <= liquidation_price
    } else {
        // Short position gets liquidated when price rises to or above liquidation price
        mark_price >= liquidation_price
    }
}
