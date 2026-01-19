//! Metrics and logging for momentum trader.

use std::collections::HashMap;
use std::time::Instant;

use tracing::info;

/// Metrics tracker for the momentum trader.
pub struct Metrics {
    start_time: Instant,
    /// Signals detected per asset
    signals: HashMap<String, u32>,
    /// Trades executed per asset
    trades: HashMap<String, u32>,
    /// Trades by side (YES/NO)
    trades_by_side: HashMap<String, u32>,
    /// Total errors
    errors: u32,
    /// Database errors
    db_errors: u32,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            signals: HashMap::new(),
            trades: HashMap::new(),
            trades_by_side: HashMap::new(),
            errors: 0,
            db_errors: 0,
        }
    }

    /// Record a signal detection.
    pub fn record_signal(&mut self, asset: &str) {
        *self.signals.entry(asset.to_string()).or_insert(0) += 1;
    }

    /// Record a trade execution.
    pub fn record_trade(&mut self, asset: &str, side: &str) {
        *self.trades.entry(asset.to_string()).or_insert(0) += 1;
        *self.trades_by_side.entry(side.to_string()).or_insert(0) += 1;
    }

    /// Record an error.
    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    /// Record a database error.
    pub fn record_db_error(&mut self) {
        self.db_errors += 1;
    }

    /// Get total signals.
    pub fn total_signals(&self) -> u32 {
        self.signals.values().sum()
    }

    /// Get total trades.
    pub fn total_trades(&self) -> u32 {
        self.trades.values().sum()
    }

    /// Print metrics summary.
    pub fn print_summary(&self) {
        let elapsed = self.start_time.elapsed();
        let total_signals = self.total_signals();
        let total_trades = self.total_trades();
        let yes_trades = self.trades_by_side.get("YES").copied().unwrap_or(0);
        let no_trades = self.trades_by_side.get("NO").copied().unwrap_or(0);

        info!("╔════════════════════════════════════════════════════════════╗");
        info!("║              MOMENTUM TRADER METRICS                       ║");
        info!("╠════════════════════════════════════════════════════════════╣");
        info!(
            "║  Uptime:            {:>8.1} minutes                       ║",
            elapsed.as_secs_f64() / 60.0
        );
        info!(
            "║  Total Signals:     {:>8}                                 ║",
            total_signals
        );
        info!(
            "║  Total Trades:      {:>8}                                 ║",
            total_trades
        );
        info!(
            "║  YES / NO:          {:>4} / {:<4}                             ║",
            yes_trades, no_trades
        );
        info!(
            "║  Errors:            {:>8}                                 ║",
            self.errors
        );
        info!(
            "║  DB Errors:         {:>8}                                 ║",
            self.db_errors
        );
        info!("╠════════════════════════════════════════════════════════════╣");
        info!("║  Per Asset:                                                ║");

        for (asset, signal_count) in &self.signals {
            let trade_count = self.trades.get(asset).copied().unwrap_or(0);
            info!(
                "║    {:<4}: {:>4} signals, {:>4} trades                         ║",
                asset, signal_count, trade_count
            );
        }

        info!("╚════════════════════════════════════════════════════════════╝");
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
