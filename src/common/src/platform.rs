//! Platform abstraction for cross-platform arbitrage.
//!
//! Provides unified types for markets across Polymarket and Kalshi.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported prediction market platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Polymarket,
    Kalshi,
    Limitless,
}

impl Platform {
    /// Get the platform fee rate for trading.
    /// Polymarket: 0% (no taker fees)
    /// Kalshi: ~1% (varies by contract)
    /// Limitless: 0% (no fees)
    pub fn fee_rate(&self) -> Decimal {
        match self {
            Platform::Polymarket => Decimal::ZERO,
            Platform::Kalshi => dec!(0.01), // 1%
            Platform::Limitless => Decimal::ZERO, // 0% fees
        }
    }

    /// Get the platform name as a string for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            Platform::Polymarket => "polymarket",
            Platform::Kalshi => "kalshi",
            Platform::Limitless => "limitless",
        }
    }

    /// Parse platform from string.
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "polymarket" => Some(Platform::Polymarket),
            "kalshi" => Some(Platform::Kalshi),
            "limitless" => Some(Platform::Limitless),
            _ => None,
        }
    }

    /// Check if platform has WebSocket orderbook support.
    pub fn has_websocket_orderbook(&self) -> bool {
        match self {
            Platform::Polymarket => true,
            Platform::Kalshi => false, // REST polling only
            Platform::Limitless => true, // WebSocket orderbook support
        }
    }

    /// Get recommended price staleness threshold in seconds.
    pub fn max_price_staleness_secs(&self) -> i64 {
        match self {
            Platform::Polymarket => 5,  // WebSocket - expect fresh data
            Platform::Kalshi => 10,     // REST polling - allow more staleness
            Platform::Limitless => 5,   // WebSocket - expect fresh data
        }
    }

    /// Get the settlement chain for this platform.
    pub fn settlement_chain(&self) -> &'static str {
        match self {
            Platform::Polymarket => "polygon",
            Platform::Kalshi => "centralized",
            Platform::Limitless => "base",
        }
    }
}

impl fmt::Display for Platform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// Re-export OrderbookLevel from repository
pub use crate::repository::OrderbookLevel;

/// Orderbook depth for a market side.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OrderbookDepth {
    /// Ask levels (sorted by price ascending - best first)
    pub asks: Vec<OrderbookLevel>,
    /// Bid levels (sorted by price descending - best first)
    pub bids: Vec<OrderbookLevel>,
}

impl OrderbookDepth {
    /// Create from vectors, ensuring proper sorting.
    pub fn new(mut asks: Vec<OrderbookLevel>, mut bids: Vec<OrderbookLevel>) -> Self {
        asks.sort_by(|a, b| a.price.cmp(&b.price));
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        Self { asks, bids }
    }

    /// Check if depth data is available.
    pub fn has_depth(&self) -> bool {
        !self.asks.is_empty() || !self.bids.is_empty()
    }

    /// Get best ask price.
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.first().map(|l| l.price)
    }
}

/// Unified market representation for cross-platform comparison.
/// Contains only the fields needed for arbitrage detection.
#[derive(Debug, Clone)]
pub struct UnifiedMarket {
    /// Source platform
    pub platform: Platform,
    /// Platform-specific identifier (condition_id for Polymarket, ticker for Kalshi)
    pub market_id: String,
    /// Database UUID (if stored)
    pub db_id: Option<uuid::Uuid>,
    /// Market name/question
    pub name: String,
    /// Cryptocurrency asset (BTC, ETH, SOL, etc.)
    pub asset: String,
    /// Timeframe (15m, 1h, daily, etc.)
    pub timeframe: String,
    /// Market close/resolution time
    pub end_time: DateTime<Utc>,
    /// Best ask price for YES outcome (cost to buy YES)
    pub yes_best_ask: Option<Decimal>,
    /// Best bid price for YES outcome (price to sell YES)
    pub yes_best_bid: Option<Decimal>,
    /// Best ask price for NO outcome (cost to buy NO)
    pub no_best_ask: Option<Decimal>,
    /// Best bid price for NO outcome (price to sell NO)
    pub no_best_bid: Option<Decimal>,
    /// Market liquidity in dollars (if available)
    pub liquidity: Option<Decimal>,
    /// When the price was last updated
    pub price_updated_at: Option<DateTime<Utc>>,
    /// Market direction for above/below type (Some("above") or Some("below"))
    pub direction: Option<String>,
    /// Price strike target (for price threshold markets)
    pub strike_price: Option<f64>,
    /// YES side orderbook depth (optional, loaded on demand)
    pub yes_depth: Option<OrderbookDepth>,
    /// NO side orderbook depth (optional, loaded on demand)
    pub no_depth: Option<OrderbookDepth>,
}

impl UnifiedMarket {
    /// Check if this market has valid prices for arbitrage detection.
    pub fn has_valid_prices(&self) -> bool {
        self.yes_best_ask.is_some() && self.no_best_ask.is_some()
    }

    /// Check if prices are fresh enough for trading.
    pub fn is_price_fresh(&self, max_staleness_secs: i64) -> bool {
        match self.price_updated_at {
            Some(updated_at) => {
                let age = Utc::now() - updated_at;
                age.num_seconds() <= max_staleness_secs
            }
            None => false,
        }
    }

    /// Check if market has sufficient liquidity.
    pub fn has_sufficient_liquidity(&self, min_liquidity: Decimal) -> bool {
        self.liquidity.map_or(false, |l| l >= min_liquidity)
    }

    /// Check if market is expiring within the given number of seconds.
    pub fn expires_within_secs(&self, secs: i64) -> bool {
        let time_to_expiry = self.end_time - Utc::now();
        time_to_expiry.num_seconds() <= secs && time_to_expiry.num_seconds() > 0
    }

    /// Get time to expiry in seconds.
    pub fn time_to_expiry_secs(&self) -> i64 {
        (self.end_time - Utc::now()).num_seconds()
    }

    /// Calculate the spread cost (YES_ask + NO_ask).
    /// A spread < 1.0 indicates an internal arbitrage opportunity.
    pub fn spread(&self) -> Option<Decimal> {
        match (self.yes_best_ask, self.no_best_ask) {
            (Some(yes), Some(no)) => Some(yes + no),
            _ => None,
        }
    }
}

/// Matched pair of markets across platforms for arbitrage.
#[derive(Debug, Clone)]
pub struct MarketPair {
    /// Polymarket market
    pub polymarket: UnifiedMarket,
    /// Kalshi market
    pub kalshi: UnifiedMarket,
    /// Match confidence score (0.0 to 1.0)
    pub confidence: f64,
    /// Human-readable explanation of why these markets match
    pub match_reason: String,
}

impl MarketPair {
    /// Create a new market pair.
    pub fn new(
        polymarket: UnifiedMarket,
        kalshi: UnifiedMarket,
        confidence: f64,
        match_reason: String,
    ) -> Self {
        Self {
            polymarket,
            kalshi,
            confidence,
            match_reason,
        }
    }

    /// Check if both markets have valid prices.
    pub fn has_valid_prices(&self) -> bool {
        self.polymarket.has_valid_prices() && self.kalshi.has_valid_prices()
    }

    /// Get the minimum liquidity across both markets.
    pub fn min_liquidity(&self) -> Option<Decimal> {
        match (self.polymarket.liquidity, self.kalshi.liquidity) {
            (Some(p), Some(k)) => Some(p.min(k)),
            (Some(p), None) => Some(p),
            (None, Some(k)) => Some(k),
            (None, None) => None,
        }
    }

    /// Get the earlier expiration time.
    pub fn earliest_expiry(&self) -> DateTime<Utc> {
        self.polymarket.end_time.min(self.kalshi.end_time)
    }
}

/// Cross-platform arbitrage opportunity.
#[derive(Debug, Clone)]
pub struct CrossPlatformOpportunity {
    /// The matched market pair
    pub pair: MarketPair,
    /// Platform to buy YES on (lower YES ask price)
    pub buy_yes_on: Platform,
    /// Platform to buy NO on (lower NO ask price)
    pub buy_no_on: Platform,
    /// Price to buy YES (from the cheaper platform)
    pub yes_price: Decimal,
    /// Price to buy NO (from the cheaper platform)
    pub no_price: Decimal,
    /// Total cost to buy both YES and NO
    pub total_cost: Decimal,
    /// Gross profit percentage (before fees)
    pub gross_profit_pct: Decimal,
    /// Net profit percentage (after platform fees)
    pub net_profit_pct: Decimal,
    /// When this opportunity was detected
    pub detected_at: DateTime<Utc>,
    /// Maximum contracts that can be profitably traded (considering slippage)
    pub max_contracts: Option<u64>,
    /// Maximum investment size in dollars
    pub max_investment: Option<Decimal>,
}

impl CrossPlatformOpportunity {
    /// Calculate an arbitrage opportunity from a market pair.
    /// Returns None if no profitable opportunity exists.
    pub fn calculate(pair: MarketPair, min_profit_pct: Decimal) -> Option<Self> {
        // Need valid prices on both platforms
        if !pair.has_valid_prices() {
            return None;
        }

        let poly_yes_ask = pair.polymarket.yes_best_ask?;
        let poly_no_ask = pair.polymarket.no_best_ask?;
        let kalshi_yes_ask = pair.kalshi.yes_best_ask?;
        let kalshi_no_ask = pair.kalshi.no_best_ask?;

        // Find the best price for each side
        let (buy_yes_on, yes_price) = if poly_yes_ask <= kalshi_yes_ask {
            (Platform::Polymarket, poly_yes_ask)
        } else {
            (Platform::Kalshi, kalshi_yes_ask)
        };

        let (buy_no_on, no_price) = if poly_no_ask <= kalshi_no_ask {
            (Platform::Polymarket, poly_no_ask)
        } else {
            (Platform::Kalshi, kalshi_no_ask)
        };

        let total_cost = yes_price + no_price;

        // Gross profit: $1 payout - total cost
        let gross_profit = Decimal::ONE - total_cost;
        let gross_profit_pct = (gross_profit / total_cost) * dec!(100);

        // Calculate fees
        let yes_fee = yes_price * buy_yes_on.fee_rate();
        let no_fee = no_price * buy_no_on.fee_rate();
        let total_fees = yes_fee + no_fee;

        // Net profit after fees
        let net_profit = gross_profit - total_fees;
        let net_profit_pct = (net_profit / total_cost) * dec!(100);

        // Check if profitable enough
        if net_profit_pct < min_profit_pct {
            return None;
        }

        Some(Self {
            pair,
            buy_yes_on,
            buy_no_on,
            yes_price,
            no_price,
            total_cost,
            gross_profit_pct,
            net_profit_pct,
            detected_at: Utc::now(),
            max_contracts: None,
            max_investment: None,
        })
    }

    /// Set max profitable size from slippage calculation.
    pub fn with_max_size(mut self, contracts: u64, investment: Decimal) -> Self {
        self.max_contracts = Some(contracts);
        self.max_investment = Some(investment);
        self
    }

    /// Format the opportunity for logging/display.
    pub fn summary(&self) -> String {
        let size_info = match (self.max_contracts, self.max_investment) {
            (Some(c), Some(inv)) => format!(" | Max: {} contracts (${:.0})", c, inv),
            _ => String::new(),
        };
        format!(
            "{} vs {} | YES@{} ({}) + NO@{} ({}) = {} | Net: {:.2}%{}",
            self.pair.polymarket.name,
            self.pair.kalshi.name,
            self.yes_price,
            self.buy_yes_on,
            self.no_price,
            self.buy_no_on,
            self.total_cost,
            self.net_profit_pct,
            size_info
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_fee_rate() {
        assert_eq!(Platform::Polymarket.fee_rate(), Decimal::ZERO);
        assert_eq!(Platform::Kalshi.fee_rate(), dec!(0.01));
        assert_eq!(Platform::Limitless.fee_rate(), Decimal::ZERO);
    }

    #[test]
    fn test_platform_from_str() {
        assert_eq!(Platform::from_str("polymarket"), Some(Platform::Polymarket));
        assert_eq!(Platform::from_str("Kalshi"), Some(Platform::Kalshi));
        assert_eq!(Platform::from_str("limitless"), Some(Platform::Limitless));
        assert_eq!(Platform::from_str("Limitless"), Some(Platform::Limitless));
        assert_eq!(Platform::from_str("unknown"), None);
    }

    #[test]
    fn test_platform_settlement_chain() {
        assert_eq!(Platform::Polymarket.settlement_chain(), "polygon");
        assert_eq!(Platform::Kalshi.settlement_chain(), "centralized");
        assert_eq!(Platform::Limitless.settlement_chain(), "base");
    }

    #[test]
    fn test_unified_market_spread() {
        let market = UnifiedMarket {
            platform: Platform::Polymarket,
            market_id: "test".to_string(),
            db_id: None,
            name: "Test".to_string(),
            asset: "BTC".to_string(),
            timeframe: "15m".to_string(),
            end_time: Utc::now() + chrono::Duration::hours(1),
            yes_best_ask: Some(dec!(0.55)),
            yes_best_bid: Some(dec!(0.53)),
            no_best_ask: Some(dec!(0.47)),
            no_best_bid: Some(dec!(0.45)),
            liquidity: Some(dec!(1000)),
            price_updated_at: Some(Utc::now()),
            direction: None,
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        };

        assert_eq!(market.spread(), Some(dec!(1.02)));
        assert!(market.has_valid_prices());
    }

    #[test]
    fn test_cross_platform_opportunity_calculation() {
        let poly_market = UnifiedMarket {
            platform: Platform::Polymarket,
            market_id: "poly-test".to_string(),
            db_id: None,
            name: "BTC Up 15m".to_string(),
            asset: "BTC".to_string(),
            timeframe: "15m".to_string(),
            end_time: Utc::now() + chrono::Duration::hours(1),
            yes_best_ask: Some(dec!(0.50)),  // Cheaper YES
            yes_best_bid: Some(dec!(0.48)),
            no_best_ask: Some(dec!(0.52)),
            no_best_bid: Some(dec!(0.50)),
            liquidity: Some(dec!(1000)),
            price_updated_at: Some(Utc::now()),
            direction: Some("up".to_string()),
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        };

        let kalshi_market = UnifiedMarket {
            platform: Platform::Kalshi,
            market_id: "kalshi-test".to_string(),
            db_id: None,
            name: "BTC above X".to_string(),
            asset: "BTC".to_string(),
            timeframe: "15m".to_string(),
            end_time: Utc::now() + chrono::Duration::hours(1),
            yes_best_ask: Some(dec!(0.55)),
            yes_best_bid: Some(dec!(0.53)),
            no_best_ask: Some(dec!(0.44)),  // Cheaper NO
            no_best_bid: Some(dec!(0.42)),
            liquidity: Some(dec!(500)),
            price_updated_at: Some(Utc::now()),
            direction: Some("above".to_string()),
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        };

        let pair = MarketPair::new(
            poly_market,
            kalshi_market,
            0.95,
            "Asset and timeframe match".to_string(),
        );

        // Total cost: 0.50 (Poly YES) + 0.44 (Kalshi NO) = 0.94
        // Gross profit: 1.0 - 0.94 = 0.06 = 6.38%
        // Kalshi fee: 0.44 * 0.01 = 0.0044
        // Net profit: 0.06 - 0.0044 = 0.0556 = 5.91%
        let opp = CrossPlatformOpportunity::calculate(pair, dec!(1.0)).unwrap();

        assert_eq!(opp.buy_yes_on, Platform::Polymarket);
        assert_eq!(opp.buy_no_on, Platform::Kalshi);
        assert_eq!(opp.yes_price, dec!(0.50));
        assert_eq!(opp.no_price, dec!(0.44));
        assert_eq!(opp.total_cost, dec!(0.94));
        assert!(opp.gross_profit_pct > dec!(6.0));
        assert!(opp.net_profit_pct > dec!(5.0));
    }
}
