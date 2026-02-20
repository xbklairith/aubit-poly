//! Slippage calculator for cross-platform arbitrage.
//!
//! Calculates the maximum profitable order size by walking through
//! orderbook depth on both platforms.

use common::{KalshiOrderbook, OrderbookDepth, OrderbookLevel};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

/// Convert Kalshi orderbook to OrderbookDepth.
/// Kalshi prices are in cents (0-100), we convert to dollars (0-1).
pub fn kalshi_orderbook_to_depth(orderbook: &KalshiOrderbook, side: &str) -> OrderbookDepth {
    let levels = match side {
        "yes" => &orderbook.yes,
        "no" => &orderbook.no,
        _ => return OrderbookDepth::default(),
    };

    // For buying, we need the ask side.
    // Kalshi's orderbook `yes` contains levels where people are willing to sell YES.
    // The price is what we pay, quantity is how many contracts.
    let asks: Vec<OrderbookLevel> = levels
        .iter()
        .map(|l| OrderbookLevel {
            price: Decimal::from(l.price) / Decimal::from(100), // cents to dollars
            size: Decimal::from(l.quantity),
        })
        .collect();

    OrderbookDepth::new(asks, vec![])
}

/// Parse Polymarket orderbook depth from JSONB.
/// The JSONB is stored as array of {price, size} objects.
pub fn parse_polymarket_depth(json_value: &serde_json::Value) -> OrderbookDepth {
    if let Some(arr) = json_value.as_array() {
        let asks: Vec<OrderbookLevel> = arr
            .iter()
            .filter_map(|level| {
                let price = level.get("price")?.as_str()?.parse::<Decimal>().ok()?;
                let size = level.get("size")?.as_str()?.parse::<Decimal>().ok()?;
                Some(OrderbookLevel { price, size })
            })
            .collect();
        OrderbookDepth::new(asks, vec![])
    } else {
        OrderbookDepth::default()
    }
}

/// Result of slippage calculation.
#[derive(Debug, Clone)]
pub struct SlippageResult {
    /// Maximum number of contracts that can be profitably traded
    pub max_contracts: u64,
    /// Total cost on platform A (YES side)
    pub total_cost_a: Decimal,
    /// Total cost on platform B (NO side)
    pub total_cost_b: Decimal,
    /// Total fees
    pub total_fees: Decimal,
    /// Average YES price across all levels used
    pub avg_yes_price: Decimal,
    /// Average NO price across all levels used
    pub avg_no_price: Decimal,
    /// Net profit in dollars
    pub net_profit: Decimal,
    /// Net profit percentage
    pub net_profit_pct: Decimal,
    /// Breakdown by price level
    pub levels: Vec<SlippageLevel>,
}

/// Single level in slippage breakdown.
#[derive(Debug, Clone)]
pub struct SlippageLevel {
    pub contracts: u64,
    pub yes_price: Decimal,
    pub no_price: Decimal,
    pub profit_pct: Decimal,
}

/// Calculate maximum profitable order size for cross-platform arbitrage.
///
/// # Arguments
/// * `yes_asks` - Orderbook asks for YES side (platform where we buy YES)
/// * `no_asks` - Orderbook asks for NO side (platform where we buy NO)
/// * `yes_fee_rate` - Fee rate for YES platform (0-1)
/// * `no_fee_rate` - Fee rate for NO platform (0-1)
/// * `min_profit_pct` - Minimum profit percentage threshold
///
/// # Returns
/// `SlippageResult` with max profitable size, or None if no profitable trade exists.
pub fn calculate_max_profitable_size(
    yes_asks: &OrderbookDepth,
    no_asks: &OrderbookDepth,
    yes_fee_rate: Decimal,
    no_fee_rate: Decimal,
    min_profit_pct: Decimal,
) -> Option<SlippageResult> {
    let yes_levels = &yes_asks.asks;
    let no_levels = &no_asks.asks;

    if yes_levels.is_empty() || no_levels.is_empty() {
        debug!("Missing orderbook depth for slippage calculation");
        return None;
    }

    let mut total_contracts: u64 = 0;
    let mut total_yes_cost = Decimal::ZERO;
    let mut total_no_cost = Decimal::ZERO;
    let mut levels = Vec::new();

    let mut y_idx = 0;
    let mut y_filled = Decimal::ZERO;
    let mut n_idx = 0;
    let mut n_filled = Decimal::ZERO;

    while y_idx < yes_levels.len() && n_idx < no_levels.len() {
        let y_level = &yes_levels[y_idx];
        let n_level = &no_levels[n_idx];

        let y_avail = y_level.size - y_filled;
        let n_avail = n_level.size - n_filled;

        // How many contracts can we fill at current levels
        let can_fill = y_avail.min(n_avail);
        if can_fill <= Decimal::ZERO {
            break;
        }

        // Calculate profit at these prices
        let yes_price = y_level.price;
        let no_price = n_level.price;
        let yes_fee = yes_price * yes_fee_rate;
        let no_fee = no_price * no_fee_rate;
        let total_cost_per_contract = yes_price + no_price + yes_fee + no_fee;
        let profit_per_contract = Decimal::ONE - total_cost_per_contract;
        let profit_pct = if total_cost_per_contract > Decimal::ZERO {
            (profit_per_contract / total_cost_per_contract) * dec!(100)
        } else {
            Decimal::ZERO
        };

        // Stop if no longer profitable
        if profit_pct < min_profit_pct {
            debug!(
                "Stopping at profit {:.2}% (below {:.2}% threshold)",
                profit_pct, min_profit_pct
            );
            break;
        }

        // Fill this level
        let contracts = can_fill.floor().to_string().parse::<u64>().unwrap_or(0);
        if contracts == 0 {
            break;
        }

        let contracts_dec = Decimal::from(contracts);
        total_contracts += contracts;
        total_yes_cost += contracts_dec * yes_price;
        total_no_cost += contracts_dec * no_price;

        levels.push(SlippageLevel {
            contracts,
            yes_price,
            no_price,
            profit_pct,
        });

        // Update filled amounts
        y_filled += contracts_dec;
        n_filled += contracts_dec;

        // Move to next level if current is exhausted
        if y_filled >= y_level.size {
            y_idx += 1;
            y_filled = Decimal::ZERO;
        }
        if n_filled >= n_level.size {
            n_idx += 1;
            n_filled = Decimal::ZERO;
        }
    }

    if total_contracts == 0 {
        return None;
    }

    let total_contracts_dec = Decimal::from(total_contracts);
    let total_fees = total_yes_cost * yes_fee_rate + total_no_cost * no_fee_rate;
    let total_investment = total_yes_cost + total_no_cost + total_fees;
    let payout = total_contracts_dec;
    let net_profit = payout - total_investment;
    let net_profit_pct = if total_investment > Decimal::ZERO {
        (net_profit / total_investment) * dec!(100)
    } else {
        Decimal::ZERO
    };

    Some(SlippageResult {
        max_contracts: total_contracts,
        total_cost_a: total_yes_cost,
        total_cost_b: total_no_cost,
        total_fees,
        avg_yes_price: total_yes_cost / total_contracts_dec,
        avg_no_price: total_no_cost / total_contracts_dec,
        net_profit,
        net_profit_pct,
        levels,
    })
}

/// Estimate average price for a given order size.
pub fn estimate_avg_price(depth: &OrderbookDepth, order_size_dollars: Decimal) -> Option<Decimal> {
    if depth.asks.is_empty() {
        return None;
    }

    let mut remaining = order_size_dollars;
    let mut total_cost = Decimal::ZERO;
    let mut total_contracts = Decimal::ZERO;

    for level in &depth.asks {
        if remaining <= Decimal::ZERO {
            break;
        }

        let level_value = level.size * level.price;
        let take_value = remaining.min(level_value);
        let take_contracts = take_value / level.price;

        total_cost += take_value;
        total_contracts += take_contracts;
        remaining -= take_value;
    }

    if total_contracts > Decimal::ZERO {
        Some(total_cost / total_contracts)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_depth(levels: Vec<(f64, f64)>) -> OrderbookDepth {
        let asks = levels
            .into_iter()
            .map(|(price, size)| OrderbookLevel {
                price: Decimal::try_from(price).unwrap(),
                size: Decimal::try_from(size).unwrap(),
            })
            .collect();
        OrderbookDepth::new(asks, vec![])
    }

    #[test]
    fn test_max_profitable_size() {
        // YES asks: $0.37 x 100, $0.38 x 200
        let yes_depth = create_test_depth(vec![(0.37, 100.0), (0.38, 200.0)]);
        // NO asks: $0.60 x 50, $0.61 x 100, $0.62 x 200
        let no_depth = create_test_depth(vec![(0.60, 50.0), (0.61, 100.0), (0.62, 200.0)]);

        let result = calculate_max_profitable_size(
            &yes_depth,
            &no_depth,
            Decimal::ZERO, // Polymarket 0% fee
            dec!(0.01),    // Kalshi 1% fee
            dec!(1.0),     // 1% min profit
        );

        assert!(result.is_some());
        let r = result.unwrap();
        assert!(r.max_contracts > 0);
        assert!(r.net_profit_pct >= dec!(1.0));
    }

    #[test]
    fn test_estimate_avg_price() {
        let depth = create_test_depth(vec![(0.50, 100.0), (0.51, 100.0), (0.52, 100.0)]);

        // $25 order at best price
        let avg = estimate_avg_price(&depth, dec!(25));
        assert!(avg.is_some());
        assert_eq!(avg.unwrap(), dec!(0.50));

        // $75 order spans first two levels
        let avg = estimate_avg_price(&depth, dec!(75));
        assert!(avg.is_some());
        // (50*0.50 + 25*0.51) / 75 contracts = ?
    }
}
