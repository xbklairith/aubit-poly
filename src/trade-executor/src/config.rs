//! Trade executor configuration.

use rust_decimal::Decimal;

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
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            dry_run: true,
            starting_balance: Decimal::new(10000, 0),
            min_profit: Decimal::new(1, 2), // 0.01 = 1%
            base_position_size: Decimal::new(10, 0), // $10 baseline
            max_position_size: Decimal::new(20, 0),  // $20 max with liquidity
            liquidity_threshold: Decimal::new(50, 0), // $50 depth needed for max size
            max_total_exposure: Decimal::new(1000, 0),
            max_orderbook_age_secs: 30,
            max_price_age_secs: 60,
            max_time_to_expiry_secs: 3600, // 1 hour
            fee_rate: Decimal::new(1, 3), // 0.001 = 0.1%
            assets: vec![
                "BTC".to_string(),
                "ETH".to_string(),
                "SOL".to_string(),
                "XRP".to_string(),
            ],
        }
    }
}
