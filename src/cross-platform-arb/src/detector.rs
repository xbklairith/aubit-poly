//! Cross-platform arbitrage detector.
//!
//! Scans for profitable arbitrage opportunities between Polymarket and Kalshi.
//! Arbitrage formula: YES_price(Platform A) + NO_price(Platform B) < $1.00

use chrono::{DateTime, Utc};
use common::{CrossPlatformOpportunity, MarketPair, UnifiedMarket};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info};

/// Configuration for arbitrage detection.
#[derive(Debug, Clone)]
pub struct DetectorConfig {
    /// Minimum net profit percentage (after fees) to consider an opportunity
    pub min_profit_pct: Decimal,
    /// Minimum net profit for 15-minute markets (more lenient)
    pub min_profit_pct_15m: Decimal,
    /// Minimum liquidity in dollars
    pub min_liquidity: Decimal,
    /// Minimum liquidity for 15-minute markets
    pub min_liquidity_15m: Decimal,
    /// Minimum time to market resolution (seconds)
    pub min_time_to_resolution: i64,
    /// Minimum time for 15-minute markets (seconds)
    pub min_time_to_resolution_15m: i64,
    /// Maximum orderbook staleness (seconds)
    pub max_price_staleness: i64,
    /// Match confidence threshold
    pub min_match_confidence: f64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            min_profit_pct: dec!(3.5),         // 3.5% for standard markets
            min_profit_pct_15m: dec!(1.0),     // 1% for 15-minute markets
            min_liquidity: dec!(500),          // $500 minimum
            min_liquidity_15m: dec!(100),      // $100 for 15-minute markets
            min_time_to_resolution: 3600,      // 1 hour minimum
            min_time_to_resolution_15m: 30,    // 30 seconds for 15-minute markets
            max_price_staleness: 30,           // 30 seconds max price age
            min_match_confidence: 0.90,        // 90% match confidence
        }
    }
}

/// Cross-platform arbitrage detector.
pub struct CrossPlatformDetector {
    config: DetectorConfig,
}

impl CrossPlatformDetector {
    /// Create a new detector with default configuration.
    pub fn new() -> Self {
        Self::with_config(DetectorConfig::default())
    }

    /// Create a new detector with custom configuration.
    pub fn with_config(config: DetectorConfig) -> Self {
        Self { config }
    }

    /// Scan for arbitrage opportunities across matched market pairs.
    pub fn scan(&self, pairs: &[MarketPair]) -> Vec<CrossPlatformOpportunity> {
        let mut opportunities = Vec::new();

        info!("Scanning {} market pairs for arbitrage opportunities", pairs.len());

        for pair in pairs {
            // Apply filters
            if !self.passes_filters(pair) {
                continue;
            }

            // Calculate opportunity
            let min_profit = self.get_min_profit(&pair.polymarket.timeframe);
            if let Some(opp) = CrossPlatformOpportunity::calculate(pair.clone(), min_profit) {
                info!(
                    "OPPORTUNITY FOUND: {} | Net profit: {:.2}%",
                    opp.summary(),
                    opp.net_profit_pct
                );
                opportunities.push(opp);
            }
        }

        // Sort by profit descending
        opportunities.sort_by(|a, b| b.net_profit_pct.partial_cmp(&a.net_profit_pct).unwrap());

        info!(
            "Found {} profitable opportunities (min profit filter applied)",
            opportunities.len()
        );

        opportunities
    }

    /// Check if a market pair passes all filters.
    fn passes_filters(&self, pair: &MarketPair) -> bool {
        // Check match confidence
        if pair.confidence < self.config.min_match_confidence {
            debug!(
                "Skipping pair (low confidence {}): {} vs {}",
                pair.confidence, pair.polymarket.name, pair.kalshi.name
            );
            return false;
        }

        // Check valid prices
        if !pair.has_valid_prices() {
            debug!(
                "Skipping pair (missing prices): {} vs {}",
                pair.polymarket.name, pair.kalshi.name
            );
            return false;
        }

        // Check price freshness
        if !self.check_price_freshness(&pair.polymarket) {
            debug!(
                "Skipping pair (stale Polymarket price): {}",
                pair.polymarket.name
            );
            return false;
        }
        if !self.check_price_freshness(&pair.kalshi) {
            debug!(
                "Skipping pair (stale Limitless price): {}",
                pair.kalshi.name
            );
            return false;
        }

        // Check liquidity
        let min_liq = self.get_min_liquidity(&pair.polymarket.timeframe);
        if let Some(liq) = pair.min_liquidity() {
            if liq < min_liq {
                debug!(
                    "Skipping pair (low liquidity ${}): {} vs {}",
                    liq, pair.polymarket.name, pair.kalshi.name
                );
                return false;
            }
        }

        // Check time to resolution
        let min_time = self.get_min_time_to_resolution(&pair.polymarket.timeframe);
        let time_to_expiry = pair.earliest_expiry() - Utc::now();
        if time_to_expiry.num_seconds() < min_time {
            debug!(
                "Skipping pair (too close to expiry {}s): {} vs {}",
                time_to_expiry.num_seconds(),
                pair.polymarket.name,
                pair.kalshi.name
            );
            return false;
        }

        true
    }

    /// Check if market prices are fresh enough.
    fn check_price_freshness(&self, market: &UnifiedMarket) -> bool {
        match market.price_updated_at {
            Some(updated_at) => {
                let age = Utc::now() - updated_at;
                age.num_seconds() <= self.config.max_price_staleness
            }
            None => false,  // No timestamp means stale
        }
    }

    /// Get minimum profit threshold for a timeframe.
    fn get_min_profit(&self, timeframe: &str) -> Decimal {
        match timeframe.to_lowercase().as_str() {
            "5m" | "15m" | "intraday" => self.config.min_profit_pct_15m,
            _ => self.config.min_profit_pct,
        }
    }

    /// Get minimum liquidity for a timeframe.
    fn get_min_liquidity(&self, timeframe: &str) -> Decimal {
        match timeframe.to_lowercase().as_str() {
            "5m" | "15m" | "intraday" => self.config.min_liquidity_15m,
            _ => self.config.min_liquidity,
        }
    }

    /// Get minimum time to resolution for a timeframe.
    fn get_min_time_to_resolution(&self, timeframe: &str) -> i64 {
        match timeframe.to_lowercase().as_str() {
            "5m" | "15m" | "intraday" => self.config.min_time_to_resolution_15m,
            _ => self.config.min_time_to_resolution,
        }
    }
}

impl Default for CrossPlatformDetector {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of a scan run.
#[derive(Debug, Clone)]
pub struct ScanSummary {
    /// Number of Polymarket markets scanned
    pub polymarket_count: usize,
    /// Number of Kalshi markets scanned
    pub kalshi_count: usize,
    /// Number of high-confidence matches found
    pub matches_found: usize,
    /// Number of profitable opportunities detected
    pub opportunities_found: usize,
    /// Best opportunity profit percentage
    pub best_profit_pct: Option<Decimal>,
    /// Scan timestamp
    pub scanned_at: DateTime<Utc>,
}

impl ScanSummary {
    pub fn new(
        polymarket_count: usize,
        kalshi_count: usize,
        matches_found: usize,
        opportunities: &[CrossPlatformOpportunity],
    ) -> Self {
        let best_profit_pct = opportunities.first().map(|o| o.net_profit_pct);

        Self {
            polymarket_count,
            kalshi_count,
            matches_found,
            opportunities_found: opportunities.len(),
            best_profit_pct,
            scanned_at: Utc::now(),
        }
    }

    pub fn log(&self) {
        info!(
            "Scan complete: {} Polymarket, {} Limitless, {} matches, {} opportunities",
            self.polymarket_count,
            self.kalshi_count,
            self.matches_found,
            self.opportunities_found
        );
        if let Some(best) = self.best_profit_pct {
            info!("Best opportunity: {:.2}% profit", best);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use common::Platform;

    fn create_test_pair(yes_poly: Decimal, no_kalshi: Decimal, timeframe: &str) -> MarketPair {
        // Use 2 hours to ensure we pass the min_time_to_resolution filter (1h = 3600s)
        let end_time = Utc::now() + Duration::hours(2);

        let poly = UnifiedMarket {
            platform: Platform::Polymarket,
            market_id: "poly-test".to_string(),
            db_id: None,
            name: "BTC Up 15m".to_string(),
            asset: "BTC".to_string(),
            timeframe: timeframe.to_string(),
            end_time,
            yes_best_ask: Some(yes_poly),
            yes_best_bid: Some(yes_poly - dec!(0.02)),
            no_best_ask: Some(dec!(1) - yes_poly + dec!(0.02)),
            no_best_bid: Some(dec!(1) - yes_poly),
            liquidity: Some(dec!(1000)),
            price_updated_at: Some(Utc::now()),
            direction: Some("up".to_string()),
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        };

        let kalshi = UnifiedMarket {
            platform: Platform::Kalshi,
            market_id: "kalshi-test".to_string(),
            db_id: None,
            name: "BTC above X".to_string(),
            asset: "BTC".to_string(),
            timeframe: timeframe.to_string(),
            end_time,
            yes_best_ask: Some(yes_poly + dec!(0.05)),  // More expensive YES
            yes_best_bid: Some(yes_poly + dec!(0.03)),
            no_best_ask: Some(no_kalshi),
            no_best_bid: Some(no_kalshi - dec!(0.02)),
            liquidity: Some(dec!(500)),
            price_updated_at: Some(Utc::now()),
            direction: Some("above".to_string()),
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        };

        MarketPair::new(poly, kalshi, 0.95, "Test match".to_string())
    }

    #[test]
    fn test_profitable_opportunity() {
        let detector = CrossPlatformDetector::new();

        // Poly YES: 0.50, Kalshi NO: 0.44
        // Total: 0.94, Profit: 6%
        let pair = create_test_pair(dec!(0.50), dec!(0.44), "1h");
        let opps = detector.scan(&[pair]);

        assert_eq!(opps.len(), 1);
        assert!(opps[0].net_profit_pct > dec!(5.0));
    }

    #[test]
    fn test_no_opportunity_expensive() {
        let detector = CrossPlatformDetector::new();

        // Poly YES: 0.50, Kalshi NO: 0.52
        // Total: 1.02, Loss
        let pair = create_test_pair(dec!(0.50), dec!(0.52), "1h");
        let opps = detector.scan(&[pair]);

        assert_eq!(opps.len(), 0);
    }

    #[test]
    fn test_15m_lower_threshold() {
        let detector = CrossPlatformDetector::new();

        // Poly YES: 0.50, Kalshi NO: 0.48
        // Total: 0.98, Profit: 2%
        // Standard market would filter this, but 15m accepts it
        let pair = create_test_pair(dec!(0.50), dec!(0.48), "15m");
        let opps = detector.scan(&[pair]);

        assert_eq!(opps.len(), 1);
        assert!(opps[0].net_profit_pct > dec!(1.0));
    }
}
