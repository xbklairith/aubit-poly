//! Direction flip detection and market state management for Chainlink prices.
//!
//! Tracks price direction relative to the 15-minute market's open price
//! and detects when the direction flips (UP->DOWN or DOWN->UP).
//!
//! Uses Chainlink prices from Polymarket RTDS instead of Binance.
//! Key difference: Chainlink has no klines, so we capture the first price
//! at market discovery as the "open price".

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use common::ChainlinkPriceBuffer;
use rust_decimal::Decimal;
use tracing::{debug, info};
use uuid::Uuid;

/// Number of consecutive readings required to confirm a direction change.
/// Prevents false signals from rapid price fluctuations around the open price.
const DEBOUNCE_COUNT: u32 = 3;

/// Direction of current price relative to open price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Up,   // current_price > open_price
    Down, // current_price <= open_price
}

/// Type of direction flip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlipType {
    DownToUp, // DOWN -> UP -> Buy YES
    UpToDown, // UP -> DOWN -> Buy NO
}

impl std::fmt::Display for FlipType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlipType::DownToUp => write!(f, "DOWN->UP"),
            FlipType::UpToDown => write!(f, "UP->DOWN"),
        }
    }
}

/// State for a single market being tracked.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarketState {
    pub market_id: Uuid,
    pub market_name: String,
    pub start_time: DateTime<Utc>,
    pub open_price: Decimal,
    /// Last confirmed direction (after debouncing)
    pub confirmed_direction: Option<Direction>,
    /// Current raw direction reading (before debouncing)
    pub raw_direction: Option<Direction>,
    /// Count of consecutive readings in the same direction
    pub consecutive_count: u32,
    pub has_traded: bool,
}

/// Misprice detector that tracks direction flips using Chainlink prices.
pub struct MispriceDetector {
    states: HashMap<Uuid, MarketState>,
}

impl MispriceDetector {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Get or create state for a market, capturing open price from Chainlink buffer.
    /// Returns (state_ref, is_new) where is_new indicates if this is a newly discovered market.
    /// Logs the open price when a new market is discovered.
    ///
    /// Unlike Binance which provides klines, Chainlink only provides point prices.
    /// We capture the first price received after market discovery as the "open price".
    pub fn get_or_create_state(
        &mut self,
        market_id: Uuid,
        market_name: &str,
        start_time: DateTime<Utc>,
        price_buffer: &mut ChainlinkPriceBuffer,
        symbol: &str,
    ) -> Option<(&mut MarketState, bool)> {
        let is_new = !self.states.contains_key(&market_id);

        if is_new {
            // Get or capture the open price from the Chainlink buffer
            // This captures the current price as the "open" if not already captured
            let open_price = price_buffer.get_or_capture_open(symbol, start_time)?;

            // Log discovery of new market with its open price
            info!(
                "[NEW MARKET] {} | Chainlink Open: ${} | Start: {} | Symbol: {}",
                market_name,
                open_price,
                start_time.format("%H:%M:%S"),
                symbol
            );

            self.states.insert(
                market_id,
                MarketState {
                    market_id,
                    market_name: market_name.to_string(),
                    start_time,
                    open_price,
                    confirmed_direction: None,
                    raw_direction: None,
                    consecutive_count: 0,
                    has_traded: false,
                },
            );
        }

        self.states.get_mut(&market_id).map(|s| (s, is_new))
    }

    /// Calculate direction based on current price vs open price.
    pub fn calculate_direction(current: Decimal, open: Decimal) -> Direction {
        if current > open {
            Direction::Up
        } else {
            Direction::Down
        }
    }

    /// Update direction and detect flip with debouncing.
    ///
    /// Returns Some((FlipType, side)) if a confirmed flip was detected.
    /// Requires DEBOUNCE_COUNT consecutive readings on the same side before
    /// confirming a direction change. This prevents false signals from
    /// rapid price fluctuations around the open price.
    pub fn update_and_check_flip(
        &mut self,
        market_id: &Uuid,
        current_price: Decimal,
    ) -> Option<(FlipType, &'static str)> {
        let state = self.states.get_mut(market_id)?;

        // Already traded this market, skip
        if state.has_traded {
            return None;
        }

        let new_direction = Self::calculate_direction(current_price, state.open_price);

        // Update consecutive count and raw direction
        if state.raw_direction == Some(new_direction) {
            // Same direction as before, increment counter
            state.consecutive_count += 1;
        } else {
            // Direction changed, reset counter
            state.raw_direction = Some(new_direction);
            state.consecutive_count = 1;
            debug!(
                "[DEBOUNCE] {} direction changed to {:?}, count: 1/{}",
                state.market_name, new_direction, DEBOUNCE_COUNT
            );
        }

        // Check if we have enough consecutive readings to confirm direction
        if state.consecutive_count < DEBOUNCE_COUNT {
            return None;
        }

        // Direction is now confirmed - check for flip
        let previous_confirmed = state.confirmed_direction;
        let new_confirmed = Some(new_direction);

        // Only update confirmed direction if it changed
        if previous_confirmed != new_confirmed {
            state.confirmed_direction = new_confirmed;

            // Check for flip (requires previous confirmed direction to exist)
            match (previous_confirmed, new_confirmed) {
                (Some(Direction::Down), Some(Direction::Up)) => {
                    debug!(
                        "[DEBOUNCE] {} confirmed flip DOWN->UP after {} readings",
                        state.market_name, DEBOUNCE_COUNT
                    );
                    return Some((FlipType::DownToUp, "YES"));
                }
                (Some(Direction::Up), Some(Direction::Down)) => {
                    debug!(
                        "[DEBOUNCE] {} confirmed flip UP->DOWN after {} readings",
                        state.market_name, DEBOUNCE_COUNT
                    );
                    return Some((FlipType::UpToDown, "NO"));
                }
                _ => {}
            }
        }

        None
    }

    /// Mark a market as traded (no more trades on this market).
    pub fn mark_traded(&mut self, market_id: &Uuid) {
        if let Some(state) = self.states.get_mut(market_id) {
            state.has_traded = true;
        }
    }

    /// Get the current state for a market (if exists).
    #[allow(dead_code)]
    pub fn get_state(&self, market_id: &Uuid) -> Option<&MarketState> {
        self.states.get(market_id)
    }

    /// Clean up expired markets from tracking.
    pub fn cleanup_expired(&mut self, active_ids: &[Uuid]) {
        self.states.retain(|id, _| active_ids.contains(id));
    }

    /// Get count of tracked markets.
    pub fn tracked_count(&self) -> usize {
        self.states.len()
    }

    /// Print status of all tracked markets with their current state.
    /// Called once per minute to show market tracking status.
    pub fn print_market_status(&self, price_buffer: &ChainlinkPriceBuffer) {
        if self.states.is_empty() {
            info!("---------------------------------------------------------------");
            info!("  TRACKED MARKETS: (none)");
            info!("---------------------------------------------------------------");
            return;
        }

        info!("---------------------------------------------------------------");
        info!("  TRACKED MARKETS ({}):", self.states.len());
        info!("---------------------------------------------------------------");

        // Sort by start_time for consistent ordering
        let mut states: Vec<_> = self.states.values().collect();
        states.sort_by(|a, b| a.start_time.cmp(&b.start_time));

        for state in states {
            // Get asset symbol from market name (e.g., "Bitcoin" -> "btc/usd")
            let symbol = if state.market_name.to_lowercase().contains("bitcoin") {
                "btc/usd"
            } else if state.market_name.to_lowercase().contains("ethereum") {
                "eth/usd"
            } else if state.market_name.to_lowercase().contains("solana") {
                "sol/usd"
            } else if state.market_name.to_lowercase().contains("xrp") {
                "xrp/usd"
            } else {
                "unknown"
            };

            // Get current price from buffer
            let current_price = price_buffer.get_latest(symbol);

            // Calculate price change from open
            let price_change = current_price.map(|curr| {
                let change = curr - state.open_price;
                let pct = if state.open_price != Decimal::ZERO {
                    (change / state.open_price * Decimal::from(100)).round_dp(4)
                } else {
                    Decimal::ZERO
                };
                (change, pct)
            });

            // Format direction status
            let dir_str = match state.confirmed_direction {
                Some(Direction::Up) => "UP  ↑",
                Some(Direction::Down) => "DOWN↓",
                None => "INIT ",
            };

            // Format debounce status
            let debounce_str = if state.consecutive_count < DEBOUNCE_COUNT {
                format!("{}/{}", state.consecutive_count, DEBOUNCE_COUNT)
            } else {
                "OK".to_string()
            };

            // Format traded status
            let traded_str = if state.has_traded { "[TRADED]" } else { "" };

            // Extract short name (first part before " - ")
            let short_name = state
                .market_name
                .split(" - ")
                .next()
                .unwrap_or(&state.market_name);

            match (current_price, price_change) {
                (Some(curr), Some((change, pct))) => {
                    let sign = if change >= Decimal::ZERO { "+" } else { "" };
                    info!(
                        "  {} | {} | Open: ${:.2} | Now: ${:.2} ({}{:.2}%) | Debounce: {} {}",
                        short_name,
                        dir_str,
                        state.open_price,
                        curr,
                        sign,
                        pct,
                        debounce_str,
                        traded_str
                    );
                }
                _ => {
                    info!(
                        "  {} | {} | Open: ${:.2} | Now: (no price) | Debounce: {} {}",
                        short_name, dir_str, state.open_price, debounce_str, traded_str
                    );
                }
            }
        }
        info!("---------------------------------------------------------------");
    }
}

impl Default for MispriceDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_direction_calculation() {
        assert_eq!(
            MispriceDetector::calculate_direction(dec!(100), dec!(99)),
            Direction::Up
        );
        assert_eq!(
            MispriceDetector::calculate_direction(dec!(99), dec!(100)),
            Direction::Down
        );
        assert_eq!(
            MispriceDetector::calculate_direction(dec!(100), dec!(100)),
            Direction::Down
        );
    }

    #[test]
    fn test_flip_type_display() {
        assert_eq!(format!("{}", FlipType::DownToUp), "DOWN->UP");
        assert_eq!(format!("{}", FlipType::UpToDown), "UP->DOWN");
    }

    #[test]
    fn test_debouncing_prevents_false_flips() {
        let mut detector = MispriceDetector::new();
        let market_id = Uuid::new_v4();
        let open_price = dec!(100);

        // Manually insert a state to test flip detection
        detector.states.insert(
            market_id,
            MarketState {
                market_id,
                market_name: "Test Market".to_string(),
                start_time: Utc::now(),
                open_price,
                confirmed_direction: Some(Direction::Down), // Start confirmed as DOWN
                raw_direction: Some(Direction::Down),
                consecutive_count: DEBOUNCE_COUNT,
                has_traded: false,
            },
        );

        // Single reading above open should NOT trigger flip (debouncing)
        let result = detector.update_and_check_flip(&market_id, dec!(101));
        assert!(
            result.is_none(),
            "Single UP reading should not trigger flip"
        );

        // Second reading above open still shouldn't flip (need DEBOUNCE_COUNT)
        let result = detector.update_and_check_flip(&market_id, dec!(102));
        assert!(
            result.is_none(),
            "Two UP readings should not trigger flip yet"
        );

        // Third consecutive reading above open SHOULD trigger flip
        let result = detector.update_and_check_flip(&market_id, dec!(103));
        assert!(
            result.is_some(),
            "Three consecutive UP readings should trigger flip"
        );
        assert_eq!(result.unwrap(), (FlipType::DownToUp, "YES"));
    }

    #[test]
    fn test_debouncing_resets_on_direction_change() {
        let mut detector = MispriceDetector::new();
        let market_id = Uuid::new_v4();
        let open_price = dec!(100);

        // Start with confirmed DOWN direction
        detector.states.insert(
            market_id,
            MarketState {
                market_id,
                market_name: "Test Market".to_string(),
                start_time: Utc::now(),
                open_price,
                confirmed_direction: Some(Direction::Down),
                raw_direction: Some(Direction::Down),
                consecutive_count: DEBOUNCE_COUNT,
                has_traded: false,
            },
        );

        // Two UP readings (not enough to flip)
        detector.update_and_check_flip(&market_id, dec!(101));
        detector.update_and_check_flip(&market_id, dec!(102));

        // Now price goes back down - should reset counter
        let result = detector.update_and_check_flip(&market_id, dec!(99));
        assert!(
            result.is_none(),
            "Direction change should reset counter, no flip"
        );

        // Check that counter was reset
        let state = detector.get_state(&market_id).unwrap();
        assert_eq!(state.consecutive_count, 1);
        assert_eq!(state.raw_direction, Some(Direction::Down));
    }

    #[test]
    fn test_no_flip_on_first_direction_confirmation() {
        let mut detector = MispriceDetector::new();
        let market_id = Uuid::new_v4();
        let open_price = dec!(100);

        // Start with NO confirmed direction (new market)
        detector.states.insert(
            market_id,
            MarketState {
                market_id,
                market_name: "Test Market".to_string(),
                start_time: Utc::now(),
                open_price,
                confirmed_direction: None, // No confirmed direction yet
                raw_direction: None,
                consecutive_count: 0,
                has_traded: false,
            },
        );

        // First DEBOUNCE_COUNT readings should confirm direction but NOT trigger flip
        for i in 0..DEBOUNCE_COUNT {
            let result = detector.update_and_check_flip(&market_id, dec!(101));
            assert!(
                result.is_none(),
                "Reading {} should not trigger flip on initial confirmation",
                i + 1
            );
        }

        // Direction should now be confirmed as UP
        let state = detector.get_state(&market_id).unwrap();
        assert_eq!(state.confirmed_direction, Some(Direction::Up));
    }
}
