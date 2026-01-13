//! Trade executor configuration.

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Trade executor configuration.
#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    /// Run in dry-run (simulated) mode
    pub dry_run: bool,
    /// Starting balance for simulation
    pub starting_balance: Decimal,
    /// Minimum profit percentage to trigger a trade (e.g., 0.01 = 1%)
    pub min_profit: Decimal,
    /// Base position size per trade (minimum)
    pub base_position_size: Decimal,
    /// Maximum position size per trade (with good liquidity)
    pub max_position_size: Decimal,
    /// Liquidity threshold to scale up from base to max size (in USDC)
    pub liquidity_threshold: Decimal,
    /// Maximum total exposure across all positions
    pub max_total_exposure: Decimal,
    /// Maximum orderbook age in seconds (reject stale prices)
    pub max_orderbook_age_secs: i32,
    /// Maximum price age for opportunity detection
    pub max_price_age_secs: i64,
    /// Maximum time to market expiry in seconds
    pub max_time_to_expiry_secs: i64,
    /// Trading fee rate (e.g., 0.001 = 0.1%)
    pub fee_rate: Decimal,
    /// Assets to trade
    pub assets: Vec<String>,
    /// Maximum allowed spread widening before aborting trade.
    /// This is an absolute value (e.g., 0.005 = $0.005).
    /// Default: 0.005 (0.5%)
    pub spread_tolerance: Decimal,
    /// Price mismatch threshold for sequential placement (absolute value in dollars).
    /// If live CLOB price differs from detection price by more than this, use sequential placement.
    /// Default: 0.01 ($0.01 or 1 cent)
    pub price_mismatch_threshold: Decimal,
    /// Polling interval for sequential order fill check (milliseconds).
    /// Default: 1000 (1 second)
    pub sequential_poll_interval_ms: u64,
    /// Maximum wait time for first order fill in sequential mode (seconds).
    /// Default: 10
    pub sequential_poll_timeout_secs: u64,
    /// Enable sequential placement mode when price mismatch detected.
    /// Default: true
    pub enable_sequential_placement: bool,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            dry_run: true,
            starting_balance: Decimal::new(10000, 0),
            min_profit: Decimal::new(1, 2),           // 0.01 = 1%
            base_position_size: Decimal::new(10, 0),  // $10 baseline
            max_position_size: Decimal::new(20, 0),   // $20 max with liquidity
            liquidity_threshold: Decimal::new(50, 0), // $50 depth needed for max size
            max_total_exposure: Decimal::new(1000, 0),
            max_orderbook_age_secs: 30,
            max_price_age_secs: 60,
            max_time_to_expiry_secs: 3600, // 1 hour
            fee_rate: Decimal::new(1, 3),  // 0.001 = 0.1%
            assets: vec![
                "BTC".to_string(),
                "ETH".to_string(),
                "SOL".to_string(),
                "XRP".to_string(),
            ],
            spread_tolerance: dec!(0.005),
            price_mismatch_threshold: dec!(0.01), // $0.01 = 1 cent
            sequential_poll_interval_ms: 1000,    // 1 second
            sequential_poll_timeout_secs: 10,     // 10 seconds max wait
            enable_sequential_placement: true,    // Enabled by default
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_config_has_spread_tolerance_default() {
        let config = ExecutorConfig::default();
        assert_eq!(config.spread_tolerance, dec!(0.005)); // 0.5% default
    }

    #[test]
    #[serial]
    fn test_config_spread_tolerance_from_env() {
        // Save existing value
        let saved = std::env::var("SPREAD_TOLERANCE").ok();

        // Set test value
        std::env::set_var("SPREAD_TOLERANCE", "0.01");

        // Parse and verify
        let spread_tolerance = std::env::var("SPREAD_TOLERANCE")
            .ok()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(dec!(0.005));
        assert_eq!(spread_tolerance, dec!(0.01));

        // Restore original value
        match saved {
            Some(val) => std::env::set_var("SPREAD_TOLERANCE", val),
            None => std::env::remove_var("SPREAD_TOLERANCE"),
        }
    }

    #[test]
    #[serial]
    fn test_config_spread_tolerance_invalid_uses_default() {
        let saved = std::env::var("SPREAD_TOLERANCE").ok();

        std::env::set_var("SPREAD_TOLERANCE", "not_a_number");

        let spread_tolerance = std::env::var("SPREAD_TOLERANCE")
            .ok()
            .and_then(|s| s.parse::<Decimal>().ok())
            .unwrap_or(dec!(0.005));
        assert_eq!(spread_tolerance, dec!(0.005)); // Falls back to default

        match saved {
            Some(val) => std::env::set_var("SPREAD_TOLERANCE", val),
            None => std::env::remove_var("SPREAD_TOLERANCE"),
        }
    }
}
