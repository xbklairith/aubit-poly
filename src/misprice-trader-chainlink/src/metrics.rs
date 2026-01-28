//! Metrics tracking for misprice trader (Chainlink version).

use std::collections::HashMap;
use std::time::Instant;

use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::info;

use crate::exit_manager::ExitResult;

/// Metrics tracker for the misprice trader.
pub struct Metrics {
    start_time: Instant,
    /// Flips detected per asset
    flips_detected: HashMap<String, u32>,
    /// Trades executed per asset
    trades_executed: HashMap<String, u32>,
    /// Trades by side (YES/NO)
    trades_by_side: HashMap<String, u32>,
    /// Orders cancelled after timeout
    orders_cancelled: u32,
    /// Orders verified as filled
    verified_fills: u32,
    /// Exits via trailing stop
    exits_trailing_stop: u32,
    /// Exits via take profit
    exits_take_profit: u32,
    /// Total realized P&L from exits
    total_realized_pnl: Decimal,
    /// Total errors
    errors: u32,
    /// Database errors
    db_errors: u32,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            flips_detected: HashMap::new(),
            trades_executed: HashMap::new(),
            trades_by_side: HashMap::new(),
            orders_cancelled: 0,
            verified_fills: 0,
            exits_trailing_stop: 0,
            exits_take_profit: 0,
            total_realized_pnl: dec!(0),
            errors: 0,
            db_errors: 0,
        }
    }

    /// Record a direction flip detection.
    pub fn record_flip(&mut self, asset: &str) {
        *self.flips_detected.entry(asset.to_string()).or_insert(0) += 1;
    }

    /// Record a trade execution.
    pub fn record_trade(&mut self, asset: &str, side: &str) {
        *self.trades_executed.entry(asset.to_string()).or_insert(0) += 1;
        *self.trades_by_side.entry(side.to_string()).or_insert(0) += 1;
    }

    /// Record an order cancellation (timeout).
    pub fn record_cancel(&mut self) {
        self.orders_cancelled += 1;
    }

    /// Record a verified fill (order was filled before cancel timeout).
    pub fn record_verified_fill(&mut self) {
        self.verified_fills += 1;
    }

    /// Record an error.
    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    /// Record a database error.
    pub fn record_db_error(&mut self) {
        self.db_errors += 1;
    }

    /// Record an exit from the exit manager.
    pub fn record_exit(&mut self, result: &ExitResult) {
        use crate::exit_manager::ExitReason;

        if result.success {
            match result.reason {
                ExitReason::TrailingStop => self.exits_trailing_stop += 1,
                ExitReason::TakeProfit => self.exits_take_profit += 1,
                ExitReason::MarketExpiry => {} // Not tracked separately
            }
            self.total_realized_pnl += result.pnl;
        }
    }

    /// Get total exits.
    pub fn total_exits(&self) -> u32 {
        self.exits_trailing_stop + self.exits_take_profit
    }

    /// Get total flips detected.
    pub fn total_flips(&self) -> u32 {
        self.flips_detected.values().sum()
    }

    /// Get total trades executed.
    pub fn total_trades(&self) -> u32 {
        self.trades_executed.values().sum()
    }

    /// Print metrics summary.
    pub fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total_flips = self.total_flips();
        let total_trades = self.total_trades();
        let total_exits = self.total_exits();
        let yes_trades = self.trades_by_side.get("YES").copied().unwrap_or(0);
        let no_trades = self.trades_by_side.get("NO").copied().unwrap_or(0);

        info!("===============================================================");
        info!("        MISPRICE TRADER METRICS (CHAINLINK)                    ");
        info!("===============================================================");
        info!(
            "  Uptime:            {:>8.1} minutes",
            elapsed.as_secs_f64() / 60.0
        );
        info!("  Flips Detected:    {:>8}", total_flips);
        info!("  Orders Placed:     {:>8}", total_trades);
        info!("  YES / NO:          {:>4} / {:<4}", yes_trades, no_trades);
        info!("  Verified Fills:    {:>8}", self.verified_fills);
        info!("  Cancelled:         {:>8}", self.orders_cancelled);
        info!("---------------------------------------------------------------");
        info!("  EXIT METRICS:");
        info!("  Trailing Stops:    {:>8}", self.exits_trailing_stop);
        info!("  Take Profits:      {:>8}", self.exits_take_profit);
        info!("  Total Exits:       {:>8}", total_exits);
        info!("  Realized P&L:      ${:<8.2}", self.total_realized_pnl);
        info!("---------------------------------------------------------------");
        info!("  Errors:            {:>8}", self.errors);
        info!("  DB Errors:         {:>8}", self.db_errors);
        info!("---------------------------------------------------------------");
        info!("  Per Asset:");

        for (asset, flip_count) in &self.flips_detected {
            let trade_count = self.trades_executed.get(asset).copied().unwrap_or(0);
            info!(
                "    {:<4}: {:>4} flips, {:>4} trades",
                asset, flip_count, trade_count
            );
        }

        info!("===============================================================");
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
