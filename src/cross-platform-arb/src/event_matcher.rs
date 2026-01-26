//! Event matcher for cross-platform market matching.
//!
//! Matches equivalent markets between Polymarket and Kalshi based on:
//! - Asset (BTC, ETH, SOL, etc.)
//! - Timeframe (15m, 1h, daily)
//! - Direction (up/down, above/below)
//! - End time (within tolerance)

use chrono::{DateTime, Utc};
use common::{MarketPair, UnifiedMarket};
use tracing::info;

/// Entity extracted from a market for matching.
#[derive(Debug, Clone)]
pub struct MarketEntity {
    /// Cryptocurrency asset (BTC, ETH, etc.)
    pub asset: String,
    /// Price target for above/below markets
    pub price_target: Option<f64>,
    /// Direction: "up", "down", "above", "below"
    pub direction: Option<String>,
    /// End/resolution time
    pub end_time: DateTime<Utc>,
    /// Extracted timeframe (15m, 1h, daily)
    pub timeframe: String,
    /// Market type
    pub event_type: String,
}

/// Configuration for event matching.
#[derive(Debug, Clone)]
pub struct MatcherConfig {
    /// Minimum confidence score to consider a match (0.0 - 1.0)
    pub min_confidence: f64,
    /// Maximum time difference tolerance for 15-minute markets (seconds)
    pub time_tolerance_15m: i64,
    /// Maximum time difference tolerance for hourly markets (seconds)
    pub time_tolerance_1h: i64,
    /// Maximum time difference tolerance for daily markets (seconds)
    pub time_tolerance_daily: i64,
    /// Price target tolerance percentage (e.g., 0.01 = 1%)
    pub price_tolerance_pct: f64,
}

impl Default for MatcherConfig {
    fn default() -> Self {
        Self {
            min_confidence: 0.90,
            time_tolerance_15m: 300,      // 5 minutes
            time_tolerance_1h: 600,       // 10 minutes
            time_tolerance_daily: 3600,   // 1 hour
            price_tolerance_pct: 0.01,    // 1%
        }
    }
}

/// Event matcher for cross-platform arbitrage.
pub struct EventMatcher {
    config: MatcherConfig,
}

impl EventMatcher {
    /// Create a new event matcher with default configuration.
    pub fn new() -> Self {
        Self::with_config(MatcherConfig::default())
    }

    /// Create a new event matcher with custom configuration.
    pub fn with_config(config: MatcherConfig) -> Self {
        Self { config }
    }

    /// Extract entity information from a unified market.
    pub fn extract_entity(&self, market: &UnifiedMarket) -> MarketEntity {
        // Normalize direction
        let direction = market.direction.as_ref().map(|d| {
            let d_lower = d.to_lowercase();
            if d_lower.contains("up") || d_lower.contains("above") || d_lower.contains("higher") {
                "up".to_string()
            } else if d_lower.contains("down") || d_lower.contains("below") || d_lower.contains("lower") {
                "down".to_string()
            } else {
                d_lower
            }
        });

        // Determine event type
        let event_type = match &market.timeframe[..] {
            "15m" => "crypto_15m".to_string(),
            "1h" => "crypto_1h".to_string(),
            "4h" => "crypto_4h".to_string(),
            "daily" => "crypto_daily".to_string(),
            _ => "crypto_other".to_string(),
        };

        MarketEntity {
            asset: market.asset.clone(),
            price_target: market.strike_price,
            direction,
            end_time: market.end_time,
            timeframe: market.timeframe.clone(),
            event_type,
        }
    }

    /// Calculate match confidence score between two entities.
    /// Returns score 0.0 - 1.0 where 1.0 is a perfect match.
    pub fn score_match(&self, a: &MarketEntity, b: &MarketEntity) -> f64 {
        let mut score = 0.0;
        let mut max_score = 0.0;

        // Asset match (required, weighted heavily)
        max_score += 0.30;
        if a.asset == b.asset {
            score += 0.30;
        } else {
            // Asset mismatch is fatal
            return 0.0;
        }

        // Timeframe match
        max_score += 0.20;
        if a.timeframe == b.timeframe {
            score += 0.20;
        } else if self.timeframes_compatible(&a.timeframe, &b.timeframe) {
            score += 0.10;  // Partial credit for compatible timeframes
        }

        // Direction match
        // Note: Polymarket "Up or Down" markets have None direction because
        // the contract structure implies it (YES = up, NO = down).
        // Kalshi has explicit "up" direction markets.
        // These should match as they represent the same event.
        max_score += 0.20;
        match (&a.direction, &b.direction) {
            (Some(dir_a), Some(dir_b)) => {
                if dir_a == dir_b {
                    score += 0.20;
                } else if self.directions_equivalent(dir_a, dir_b) {
                    score += 0.15;
                }
            }
            (None, None) => score += 0.10,  // Both unknown, partial credit
            // One has direction, one doesn't - give partial credit for "up_down" markets
            // Polymarket "Up or Down" (None) should match Kalshi "up" markets
            (None, Some(dir)) | (Some(dir), None) => {
                if dir == "up" || dir == "down" || dir == "above" || dir == "below" {
                    score += 0.15;  // High partial credit - likely same event
                }
            }
        }

        // End time match
        max_score += 0.30;
        let time_tolerance = self.get_time_tolerance(&a.timeframe);
        let time_diff = (a.end_time - b.end_time).num_seconds().abs();
        if time_diff <= time_tolerance {
            // Scale score based on how close the times are
            let time_score = 1.0 - (time_diff as f64 / time_tolerance as f64);
            score += 0.30 * time_score;
        }

        // Price target match (if both have targets)
        if let (Some(price_a), Some(price_b)) = (a.price_target, b.price_target) {
            max_score += 0.10;
            let price_diff = (price_a - price_b).abs();
            let tolerance = price_a * self.config.price_tolerance_pct;
            if price_diff <= tolerance {
                score += 0.10;
            } else if price_diff <= tolerance * 2.0 {
                score += 0.05;  // Partial credit for close prices
            }
        }

        // Normalize score
        if max_score > 0.0 {
            score / max_score
        } else {
            0.0
        }
    }

    /// Check if two timeframes are compatible (e.g., "15m" vs "intraday")
    fn timeframes_compatible(&self, a: &str, b: &str) -> bool {
        let a_lower = a.to_lowercase();
        let b_lower = b.to_lowercase();

        // Exact match
        if a_lower == b_lower {
            return true;
        }

        // Intraday compatibility
        let short_term = ["5m", "15m", "intraday"];
        if short_term.contains(&a_lower.as_str()) && short_term.contains(&b_lower.as_str()) {
            return true;
        }

        // Hourly compatibility
        let hourly = ["1h", "hourly"];
        if hourly.contains(&a_lower.as_str()) && hourly.contains(&b_lower.as_str()) {
            return true;
        }

        // Daily compatibility
        let daily = ["daily", "24h", "eod"];
        if daily.contains(&a_lower.as_str()) && daily.contains(&b_lower.as_str()) {
            return true;
        }

        false
    }

    /// Check if two directions are equivalent (e.g., "up" == "above")
    fn directions_equivalent(&self, a: &str, b: &str) -> bool {
        let a_lower = a.to_lowercase();
        let b_lower = b.to_lowercase();

        // Up/Above equivalence
        let bullish = ["up", "above", "higher", "yes"];
        if bullish.contains(&a_lower.as_str()) && bullish.contains(&b_lower.as_str()) {
            return true;
        }

        // Down/Below equivalence
        let bearish = ["down", "below", "lower", "no"];
        if bearish.contains(&a_lower.as_str()) && bearish.contains(&b_lower.as_str()) {
            return true;
        }

        false
    }

    /// Get time tolerance for a given timeframe.
    fn get_time_tolerance(&self, timeframe: &str) -> i64 {
        match timeframe.to_lowercase().as_str() {
            "5m" | "15m" | "intraday" => self.config.time_tolerance_15m,
            "1h" | "hourly" => self.config.time_tolerance_1h,
            "4h" => self.config.time_tolerance_1h * 2,
            "daily" | "24h" | "weekly" => self.config.time_tolerance_daily,
            _ => self.config.time_tolerance_1h,
        }
    }

    /// Find matching markets across platforms.
    /// Returns pairs of markets with confidence scores above threshold.
    pub fn match_markets(
        &self,
        polymarket: &[UnifiedMarket],
        kalshi: &[UnifiedMarket],
    ) -> Vec<MarketPair> {
        let mut matches = Vec::new();

        info!(
            "Matching {} Polymarket markets against {} Kalshi markets",
            polymarket.len(),
            kalshi.len()
        );

        // Extract entities for all markets
        let poly_entities: Vec<(usize, MarketEntity)> = polymarket
            .iter()
            .enumerate()
            .map(|(i, m)| (i, self.extract_entity(m)))
            .collect();

        let kalshi_entities: Vec<(usize, MarketEntity)> = kalshi
            .iter()
            .enumerate()
            .map(|(i, m)| (i, self.extract_entity(m)))
            .collect();

        // Find best matches
        for (poly_idx, poly_entity) in &poly_entities {
            let mut best_match: Option<(usize, f64, String)> = None;

            for (kalshi_idx, kalshi_entity) in &kalshi_entities {
                // Quick filter: same asset
                if poly_entity.asset != kalshi_entity.asset {
                    continue;
                }

                let score = self.score_match(poly_entity, kalshi_entity);

                if score >= self.config.min_confidence {

                    if best_match.as_ref().map_or(true, |(_, s, _)| score > *s) {
                        let reason = self.generate_match_reason(poly_entity, kalshi_entity, score);
                        best_match = Some((*kalshi_idx, score, reason));
                    }
                }
            }

            if let Some((kalshi_idx, score, reason)) = best_match {
                matches.push(MarketPair::new(
                    polymarket[*poly_idx].clone(),
                    kalshi[kalshi_idx].clone(),
                    score,
                    reason,
                ));
            }
        }

        info!("Found {} high-confidence matches", matches.len());
        matches
    }

    /// Generate human-readable match reason.
    fn generate_match_reason(
        &self,
        a: &MarketEntity,
        b: &MarketEntity,
        score: f64,
    ) -> String {
        let mut reasons = Vec::new();

        reasons.push(format!("Asset: {}", a.asset));
        reasons.push(format!("Timeframe: {} vs {}", a.timeframe, b.timeframe));

        if let (Some(dir_a), Some(dir_b)) = (&a.direction, &b.direction) {
            reasons.push(format!("Direction: {} vs {}", dir_a, dir_b));
        }

        let time_diff = (a.end_time - b.end_time).num_seconds().abs();
        if time_diff < 60 {
            reasons.push("End time: exact match".to_string());
        } else {
            reasons.push(format!("End time: {}s apart", time_diff));
        }

        if let (Some(p_a), Some(p_b)) = (a.price_target, b.price_target) {
            reasons.push(format!("Price target: {} vs {}", p_a, p_b));
        }

        format!("Score: {:.2} - {}", score, reasons.join(", "))
    }
}

impl Default for EventMatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use common::Platform;
    use rust_decimal_macros::dec;

    fn create_test_market(
        platform: Platform,
        asset: &str,
        timeframe: &str,
        end_time: DateTime<Utc>,
        direction: Option<&str>,
    ) -> UnifiedMarket {
        UnifiedMarket {
            platform,
            market_id: "test".to_string(),
            db_id: None,
            name: format!("{} {} market", asset, timeframe),
            asset: asset.to_string(),
            timeframe: timeframe.to_string(),
            end_time,
            yes_best_ask: Some(dec!(0.55)),
            yes_best_bid: Some(dec!(0.53)),
            no_best_ask: Some(dec!(0.47)),
            no_best_bid: Some(dec!(0.45)),
            liquidity: Some(dec!(1000)),
            price_updated_at: Some(Utc::now()),
            direction: direction.map(|s| s.to_string()),
            strike_price: None,
            yes_depth: None,
            no_depth: None,
        }
    }

    #[test]
    fn test_exact_match() {
        let matcher = EventMatcher::new();
        let end_time = Utc::now() + Duration::hours(1);

        let poly = create_test_market(Platform::Polymarket, "BTC", "15m", end_time, Some("up"));
        let kalshi = create_test_market(Platform::Kalshi, "BTC", "15m", end_time, Some("up"));

        let entity_a = matcher.extract_entity(&poly);
        let entity_b = matcher.extract_entity(&kalshi);

        let score = matcher.score_match(&entity_a, &entity_b);
        assert!(score >= 0.95, "Exact match should score 0.95+, got {}", score);
    }

    #[test]
    fn test_direction_equivalence() {
        let matcher = EventMatcher::new();
        let end_time = Utc::now() + Duration::hours(1);

        let poly = create_test_market(Platform::Polymarket, "BTC", "15m", end_time, Some("up"));
        let kalshi = create_test_market(Platform::Kalshi, "BTC", "15m", end_time, Some("above"));

        let entity_a = matcher.extract_entity(&poly);
        let entity_b = matcher.extract_entity(&kalshi);

        let score = matcher.score_match(&entity_a, &entity_b);
        assert!(score >= 0.90, "up/above should be equivalent, got {}", score);
    }

    #[test]
    fn test_asset_mismatch() {
        let matcher = EventMatcher::new();
        let end_time = Utc::now() + Duration::hours(1);

        let poly = create_test_market(Platform::Polymarket, "BTC", "15m", end_time, Some("up"));
        let kalshi = create_test_market(Platform::Kalshi, "ETH", "15m", end_time, Some("up"));

        let entity_a = matcher.extract_entity(&poly);
        let entity_b = matcher.extract_entity(&kalshi);

        let score = matcher.score_match(&entity_a, &entity_b);
        assert_eq!(score, 0.0, "Different assets should score 0");
    }

    #[test]
    fn test_time_tolerance() {
        let matcher = EventMatcher::new();
        let end_time = Utc::now() + Duration::hours(1);
        let end_time_offset = end_time + Duration::minutes(3);  // 3 minutes apart

        let poly = create_test_market(Platform::Polymarket, "BTC", "15m", end_time, Some("up"));
        let kalshi = create_test_market(Platform::Kalshi, "BTC", "15m", end_time_offset, Some("up"));

        let entity_a = matcher.extract_entity(&poly);
        let entity_b = matcher.extract_entity(&kalshi);

        let score = matcher.score_match(&entity_a, &entity_b);
        // Score is ~0.82 due to time penalty (180s diff in 300s tolerance = 40% penalty on time component)
        // This is still above the default 0.90 min_confidence when end times match exactly
        assert!(score >= 0.80, "3-minute diff in 15m market should score 0.80+, got {}", score);
    }

    #[test]
    fn test_match_markets() {
        let matcher = EventMatcher::new();
        let end_time = Utc::now() + Duration::hours(1);

        let poly_markets = vec![
            create_test_market(Platform::Polymarket, "BTC", "15m", end_time, Some("up")),
            create_test_market(Platform::Polymarket, "ETH", "1h", end_time, Some("down")),
        ];

        let kalshi_markets = vec![
            create_test_market(Platform::Kalshi, "BTC", "15m", end_time, Some("above")),
            create_test_market(Platform::Kalshi, "SOL", "15m", end_time, Some("up")),
        ];

        let matches = matcher.match_markets(&poly_markets, &kalshi_markets);

        assert_eq!(matches.len(), 1, "Should find exactly 1 match (BTC)");
        assert_eq!(matches[0].polymarket.asset, "BTC");
        assert_eq!(matches[0].kalshi.asset, "BTC");
    }
}
