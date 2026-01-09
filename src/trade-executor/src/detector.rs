//! Spread opportunity detector.

use chrono::Utc;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use common::repository::MarketWithPrices;

use crate::models::{SpreadOpportunity, TradeDetails};

/// Spread detector for finding arbitrage opportunities.
pub struct SpreadDetector {
    /// Minimum profit percentage to trigger a trade
    min_profit: Decimal,
    /// Maximum price age in seconds
    max_price_age_secs: i64,
}

impl SpreadDetector {
    /// Create a new spread detector.
    pub fn new(min_profit: Decimal, max_price_age_secs: i64) -> Self {
        Self {
            min_profit,
            max_price_age_secs,
        }
    }

    /// Check a single market for arbitrage opportunity.
    ///
    /// An opportunity exists when:
    /// - YES_ask + NO_ask < $1.00 (can buy both for less than guaranteed payout)
    /// - Profit percentage >= minimum threshold
    ///
    /// Returns Some(opportunity) if profitable, None otherwise.
    #[inline]
    pub fn check_opportunity(&self, market: &MarketWithPrices) -> Option<SpreadOpportunity> {
        // Skip markets with invalid prices
        let yes_ask = market.yes_best_ask?;
        let no_ask = market.no_best_ask?;

        if yes_ask <= dec!(0) || no_ask <= dec!(0) {
            return None;
        }

        // Check price freshness
        let age_secs = (Utc::now() - market.captured_at).num_seconds();
        if age_secs > self.max_price_age_secs {
            return None;
        }

        // Calculate spread and profit
        // spread = cost to buy both YES and NO
        let spread = yes_ask + no_ask;

        // profit = guaranteed payout ($1) - cost (same formula as Python)
        // This is the absolute profit in dollars
        let profit_pct = dec!(1.00) - spread;

        // Log near-profitable opportunities for debugging
        if spread < dec!(1.05) {
            tracing::debug!(
                "Market {}: YES={}, NO={}, spread={}, profit_pct={}, min={}",
                &market.name[..market.name.len().min(40)],
                yes_ask, no_ask, spread, profit_pct, self.min_profit
            );
        }

        // Check minimum profit threshold
        if profit_pct < self.min_profit {
            return None;
        }

        Some(SpreadOpportunity {
            market_id: market.id,
            condition_id: market.condition_id.clone(),
            market_name: market.name.clone(),
            asset: market.asset.clone(),
            end_time: market.end_time,
            yes_token_id: market.yes_token_id.clone(),
            no_token_id: market.no_token_id.clone(),
            yes_price: yes_ask,
            no_price: no_ask,
            spread,
            profit_pct,
            detected_at: Utc::now(),
        })
    }

    /// Scan multiple markets and return sorted opportunities (best first).
    pub fn scan_markets(&self, markets: &[MarketWithPrices]) -> Vec<SpreadOpportunity> {
        let mut opportunities: Vec<_> = markets
            .iter()
            .filter_map(|m| self.check_opportunity(m))
            .collect();

        // Sort by profit percentage (highest first)
        opportunities.sort_by(|a, b| b.profit_pct.partial_cmp(&a.profit_pct).unwrap_or(std::cmp::Ordering::Equal));
        opportunities
    }

    /// Calculate trade details for an opportunity.
    ///
    /// For spread arbitrage, we buy both YES and NO tokens.
    /// The shares are equal to ensure guaranteed payout on settlement.
    pub fn calculate_trade_details(
        &self,
        opportunity: &SpreadOpportunity,
        investment: Decimal,
        fee_rate: Decimal,
    ) -> TradeDetails {
        let yes_price = opportunity.yes_price;
        let no_price = opportunity.no_price;
        let total_cost = yes_price + no_price;

        // Proportional allocation
        let yes_ratio = yes_price / total_cost;
        let no_ratio = no_price / total_cost;

        let yes_investment = investment * yes_ratio;
        let no_investment = investment * no_ratio;

        // Calculate shares
        let yes_shares = yes_investment / yes_price;
        let no_shares = no_investment / no_price;

        // Use minimum shares for spread arb (equal shares both sides)
        let shares = yes_shares.min(no_shares);

        // Recalculate actual costs
        let yes_cost = shares * yes_price;
        let no_cost = shares * no_price;
        let total_invested = yes_cost + no_cost;

        // Guaranteed payout is the number of shares
        let payout = shares;
        let gross_profit = payout - total_invested;
        let fee = total_invested * fee_rate;
        let net_profit = gross_profit - fee;
        let profit_pct = if total_invested > dec!(0) {
            (net_profit / total_invested) * dec!(100)
        } else {
            dec!(0)
        };

        TradeDetails {
            yes_shares: shares,
            no_shares: shares,
            yes_price,
            no_price,
            yes_cost,
            no_cost,
            total_invested,
            payout,
            gross_profit,
            fee,
            net_profit,
            profit_pct,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use uuid::Uuid;

    fn make_market(yes_ask: Decimal, no_ask: Decimal) -> MarketWithPrices {
        MarketWithPrices {
            id: Uuid::new_v4(),
            condition_id: "test-condition".to_string(),
            market_type: "up_down".to_string(),
            asset: "BTC".to_string(),
            timeframe: "1h".to_string(),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            name: "Test Market".to_string(),
            end_time: Utc::now() + Duration::hours(1),
            is_active: true,
            yes_best_ask: Some(yes_ask),
            yes_best_bid: Some(yes_ask - dec!(0.01)),
            no_best_ask: Some(no_ask),
            no_best_bid: Some(no_ask - dec!(0.01)),
            captured_at: Utc::now(),
        }
    }

    #[test]
    fn test_profitable_opportunity() {
        let detector = SpreadDetector::new(dec!(0.01), 60);

        // YES: $0.45, NO: $0.45 => Spread: $0.90, Profit: $0.10 (10%)
        let market = make_market(dec!(0.45), dec!(0.45));
        let opp = detector.check_opportunity(&market);

        assert!(opp.is_some());
        let opp = opp.unwrap();
        assert_eq!(opp.spread, dec!(0.90));
        assert_eq!(opp.profit_pct, dec!(0.10)); // $0.10 profit
    }

    #[test]
    fn test_no_opportunity_when_spread_too_high() {
        let detector = SpreadDetector::new(dec!(0.01), 60);

        // YES: $0.55, NO: $0.55 => Spread: $1.10, No profit
        let market = make_market(dec!(0.55), dec!(0.55));
        let opp = detector.check_opportunity(&market);

        assert!(opp.is_none());
    }

    #[test]
    fn test_no_opportunity_below_threshold() {
        let detector = SpreadDetector::new(dec!(0.05), 60); // 5% min profit

        // YES: $0.49, NO: $0.49 => Spread: $0.98, Profit: ~2%
        let market = make_market(dec!(0.49), dec!(0.49));
        let opp = detector.check_opportunity(&market);

        assert!(opp.is_none()); // Below 5% threshold
    }

    #[test]
    fn test_calculate_trade_details() {
        let detector = SpreadDetector::new(dec!(0.01), 60);

        let market = make_market(dec!(0.45), dec!(0.45));
        let opp = detector.check_opportunity(&market).unwrap();

        let details = detector.calculate_trade_details(&opp, dec!(100), dec!(0.001));

        assert!(details.total_invested <= dec!(100));
        assert!(details.net_profit > dec!(0));
        assert_eq!(details.yes_shares, details.no_shares);
    }
}
