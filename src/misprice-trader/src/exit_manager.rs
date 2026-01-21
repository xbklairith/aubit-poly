//! Exit management for trailing take profit strategy.
//!
//! After entry, tracks peak price and exits when price drops X% from peak
//! to lock in gains. Also supports optional hard take profit target.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};
use uuid::Uuid;

use common::{execute_sell_order, CachedAuth, MarketWithOrderbook};

/// Maximum number of exit attempts before abandoning a position.
const MAX_EXIT_ATTEMPTS: u32 = 3;

/// Minimum delay between exit retry attempts in seconds.
const EXIT_RETRY_DELAY_SECS: i64 = 30;

/// An active position being managed for exit.
#[derive(Debug, Clone)]
pub struct ActivePosition {
    pub market_id: Uuid,
    pub market_name: String,
    pub token_id: String,
    pub side: String, // YES or NO
    pub shares: Decimal,
    pub entry_price: Decimal,
    pub peak_price: Decimal, // Highest price since entry
    #[allow(dead_code)]
    pub entered_at: DateTime<Utc>,
    /// Number of failed exit attempts
    pub exit_attempts: u32,
    /// Time of last exit attempt (for retry delay)
    pub last_exit_attempt: Option<DateTime<Utc>>,
}

/// Reason for exit.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ExitReason {
    TrailingStop,
    TakeProfit,
    MarketExpiry,
}

impl std::fmt::Display for ExitReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExitReason::TrailingStop => write!(f, "TRAILING_STOP"),
            ExitReason::TakeProfit => write!(f, "TAKE_PROFIT"),
            ExitReason::MarketExpiry => write!(f, "MARKET_EXPIRY"),
        }
    }
}

/// Result of an exit attempt.
#[derive(Debug)]
#[allow(dead_code)]
pub struct ExitResult {
    pub market_id: Uuid,
    pub market_name: String,
    pub side: String,
    pub shares: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Decimal,
    pub peak_price: Decimal,
    pub pnl: Decimal,
    pub pnl_pct: Decimal,
    pub reason: ExitReason,
    pub success: bool,
    pub order_id: Option<String>,
    pub error_msg: Option<String>,
}

/// Manages active positions and their exit logic.
pub struct ExitManager {
    /// Active positions by market_id
    active_positions: HashMap<Uuid, ActivePosition>,
    /// Trailing stop percentage (e.g., 0.10 = 10%)
    trailing_stop_pct: Decimal,
    /// Optional take profit percentage (e.g., 0.30 = 30% profit)
    take_profit_pct: Option<Decimal>,
    /// Dry run mode
    dry_run: bool,
}

impl ExitManager {
    /// Create a new exit manager.
    pub fn new(
        trailing_stop_pct: Decimal,
        take_profit_pct: Option<Decimal>,
        dry_run: bool,
    ) -> Self {
        Self {
            active_positions: HashMap::new(),
            trailing_stop_pct,
            take_profit_pct,
            dry_run,
        }
    }

    /// Check if trailing exit is enabled.
    pub fn is_enabled(&self) -> bool {
        self.trailing_stop_pct > dec!(0)
    }

    /// Add a position after fill confirmed.
    pub fn add_position(
        &mut self,
        market_id: Uuid,
        market_name: String,
        token_id: String,
        side: String,
        shares: Decimal,
        entry_price: Decimal,
    ) {
        let position = ActivePosition {
            market_id,
            market_name: market_name.clone(),
            token_id,
            side: side.clone(),
            shares,
            entry_price,
            peak_price: entry_price, // Start with entry as peak
            entered_at: Utc::now(),
            exit_attempts: 0,
            last_exit_attempt: None,
        };

        info!(
            "[EXIT_MGR] Tracking position: {} {} @ ${:.3} ({:.2} shares)",
            market_name, side, entry_price, shares
        );

        self.active_positions.insert(market_id, position);
    }

    /// Check if we have an active position for this market.
    #[allow(dead_code)]
    pub fn has_position(&self, market_id: &Uuid) -> bool {
        self.active_positions.contains_key(market_id)
    }

    /// Get count of active positions.
    pub fn position_count(&self) -> usize {
        self.active_positions.len()
    }

    /// Check all positions against current market prices.
    /// Returns triggered exits that need to be processed.
    pub async fn check_exits(
        &mut self,
        markets: &[MarketWithOrderbook],
        cached_auth: &mut Option<CachedAuth>,
    ) -> Vec<ExitResult> {
        if self.active_positions.is_empty() {
            return Vec::new();
        }

        // Build a map for quick market lookup
        let market_map: HashMap<Uuid, &MarketWithOrderbook> =
            markets.iter().map(|m| (m.id, m)).collect();

        // First pass: update peaks and collect positions to exit
        let mut exits_to_process: Vec<(ActivePosition, Decimal, ExitReason)> = Vec::new();

        for (market_id, position) in self.active_positions.iter_mut() {
            // Find matching market
            let market = match market_map.get(market_id) {
                Some(m) => m,
                None => {
                    debug!(
                        "[EXIT_MGR] Market {} not in current market list",
                        position.market_name
                    );
                    continue;
                }
            };

            // Get current price (best bid for selling)
            let current_price = match &position.side[..] {
                "YES" => market.yes_best_bid.unwrap_or(dec!(0)),
                "NO" => market.no_best_bid.unwrap_or(dec!(0)),
                _ => continue,
            };

            if current_price <= dec!(0) {
                debug!(
                    "[EXIT_MGR] No bid price for {} {}",
                    position.market_name, position.side
                );
                continue;
            }

            // Update peak price
            if current_price > position.peak_price {
                debug!(
                    "[EXIT_MGR] {} peak updated: ${:.3} -> ${:.3}",
                    position.market_name, position.peak_price, current_price
                );
                position.peak_price = current_price;
            }

            // Calculate profit from entry (guard against division by zero)
            let profit_pct = if position.entry_price > dec!(0) {
                (current_price - position.entry_price) / position.entry_price
            } else {
                dec!(0)
            };

            // Check for take profit trigger
            if let Some(tp_pct) = self.take_profit_pct {
                if profit_pct >= tp_pct {
                    info!(
                        "[EXIT_MGR] 🎯 TAKE PROFIT triggered: {} {} @ ${:.3} (+{:.1}%)",
                        position.market_name,
                        position.side,
                        current_price,
                        profit_pct * dec!(100)
                    );
                    exits_to_process.push((
                        position.clone(),
                        current_price,
                        ExitReason::TakeProfit,
                    ));
                    continue;
                }
            }

            // Calculate drawdown from peak
            let drawdown = if position.peak_price > dec!(0) {
                (position.peak_price - current_price) / position.peak_price
            } else {
                dec!(0)
            };

            // Check for trailing stop trigger
            if drawdown >= self.trailing_stop_pct {
                info!(
                    "[EXIT_MGR] 📉 TRAILING STOP triggered: {} {} @ ${:.3} (peak ${:.3}, down {:.1}%)",
                    position.market_name,
                    position.side,
                    current_price,
                    position.peak_price,
                    drawdown * dec!(100)
                );
                exits_to_process.push((position.clone(), current_price, ExitReason::TrailingStop));
            }
        }

        // Second pass: execute exits
        let mut triggered_exits = Vec::new();
        for (position, exit_price, reason) in exits_to_process {
            let market_id = position.market_id;

            // Check if we should skip due to retry delay
            if let Some(pos) = self.active_positions.get(&market_id) {
                if let Some(last_attempt) = pos.last_exit_attempt {
                    let elapsed = (Utc::now() - last_attempt).num_seconds();
                    if elapsed < EXIT_RETRY_DELAY_SECS {
                        debug!(
                            "[EXIT_MGR] Skipping {} exit retry, {}s since last attempt",
                            pos.market_name, elapsed
                        );
                        continue;
                    }
                }
            }

            let exit_result = self
                .execute_exit(position, exit_price, reason, cached_auth)
                .await;

            if exit_result.success {
                self.active_positions.remove(&market_id);
            } else {
                // Track failed attempt
                if let Some(pos) = self.active_positions.get_mut(&market_id) {
                    pos.exit_attempts += 1;
                    pos.last_exit_attempt = Some(Utc::now());

                    if pos.exit_attempts >= MAX_EXIT_ATTEMPTS {
                        warn!(
                            "[EXIT_MGR] Max exit attempts ({}) reached for {}, abandoning position",
                            MAX_EXIT_ATTEMPTS, pos.market_name
                        );
                        self.active_positions.remove(&market_id);
                    }
                }
            }
            triggered_exits.push(exit_result);
        }

        triggered_exits
    }

    /// Execute an exit order.
    async fn execute_exit(
        &self,
        position: ActivePosition,
        exit_price: Decimal,
        reason: ExitReason,
        cached_auth: &mut Option<CachedAuth>,
    ) -> ExitResult {
        let pnl = position.shares * (exit_price - position.entry_price);
        let pnl_pct = if position.entry_price > dec!(0) {
            (exit_price - position.entry_price) / position.entry_price * dec!(100)
        } else {
            dec!(0)
        };

        if self.dry_run {
            // Simulated exit
            info!(
                "[DRY RUN EXIT] {} {} {:.2} shares @ ${:.3} -> P&L: ${:.2} ({:+.1}%)",
                reason, position.side, position.shares, exit_price, pnl, pnl_pct
            );

            return ExitResult {
                market_id: position.market_id,
                market_name: position.market_name,
                side: position.side,
                shares: position.shares,
                entry_price: position.entry_price,
                exit_price,
                peak_price: position.peak_price,
                pnl,
                pnl_pct,
                reason,
                success: true,
                order_id: Some("DRY_RUN".to_string()),
                error_msg: None,
            };
        }

        // Real exit order
        match execute_sell_order(
            cached_auth,
            &position.token_id,
            position.shares,
            exit_price,
            &position.market_name,
        )
        .await
        {
            Ok(order_id) => {
                info!(
                    "[EXIT] {} {} {} @ ${:.3} (order_id: {})",
                    reason, position.market_name, position.side, exit_price, order_id
                );

                ExitResult {
                    market_id: position.market_id,
                    market_name: position.market_name,
                    side: position.side,
                    shares: position.shares,
                    entry_price: position.entry_price,
                    exit_price,
                    peak_price: position.peak_price,
                    pnl,
                    pnl_pct,
                    reason,
                    success: true,
                    order_id: Some(order_id),
                    error_msg: None,
                }
            }
            Err(e) => {
                warn!(
                    "[EXIT FAILED] {} {} {}: {}",
                    reason, position.market_name, position.side, e
                );

                ExitResult {
                    market_id: position.market_id,
                    market_name: position.market_name,
                    side: position.side,
                    shares: position.shares,
                    entry_price: position.entry_price,
                    exit_price,
                    peak_price: position.peak_price,
                    pnl: dec!(0), // Don't count P&L on failed exit
                    pnl_pct: dec!(0),
                    reason,
                    success: false,
                    order_id: None,
                    error_msg: Some(e.to_string()),
                }
            }
        }
    }

    /// Remove position on market expiry (resolved via settlement).
    #[allow(dead_code)]
    pub fn remove_position(&mut self, market_id: &Uuid) -> Option<ActivePosition> {
        self.active_positions.remove(market_id)
    }

    /// Get all active positions for summary.
    #[allow(dead_code)]
    pub fn get_positions(&self) -> Vec<&ActivePosition> {
        self.active_positions.values().collect()
    }

    /// Remove positions for markets that have expired.
    /// Called during cleanup cycle to handle markets no longer in active list.
    pub fn cleanup_expired_positions(&mut self, active_market_ids: &[Uuid]) {
        let active_set: HashSet<_> = active_market_ids.iter().collect();

        let expired: Vec<_> = self
            .active_positions
            .keys()
            .filter(|id| !active_set.contains(id))
            .cloned()
            .collect();

        for market_id in expired {
            if let Some(pos) = self.active_positions.remove(&market_id) {
                warn!(
                    "[EXIT_MGR] Position expired without exit: {} {} @ ${:.3}",
                    pos.market_name, pos.side, pos.entry_price
                );
            }
        }
    }

    /// Print summary of active positions.
    pub fn print_summary(&self) {
        if self.active_positions.is_empty() {
            return;
        }

        info!("╔════════════════════════════════════════════════════════════╗");
        info!("║              ACTIVE POSITIONS (EXIT MANAGER)               ║");
        info!("╠════════════════════════════════════════════════════════════╣");
        info!(
            "║  Positions:         {:<10}                             ║",
            self.active_positions.len()
        );
        info!(
            "║  Trailing Stop:     {:<6.1}%                               ║",
            self.trailing_stop_pct * dec!(100)
        );
        if let Some(tp) = self.take_profit_pct {
            info!(
                "║  Take Profit:       {:<6.1}%                               ║",
                tp * dec!(100)
            );
        }
        info!("╠════════════════════════════════════════════════════════════╣");

        for pos in self.active_positions.values() {
            info!(
                "║  {} {} @ ${:.3} (peak: ${:.3})                     ║",
                pos.side, pos.market_name, pos.entry_price, pos.peak_price
            );
        }

        info!("╚════════════════════════════════════════════════════════════╝");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_manager_creation() {
        let manager = ExitManager::new(dec!(0.10), Some(dec!(0.30)), true);
        assert!(manager.is_enabled());
        assert_eq!(manager.position_count(), 0);
    }

    #[test]
    fn test_add_position() {
        let mut manager = ExitManager::new(dec!(0.10), None, true);
        let market_id = Uuid::new_v4();

        manager.add_position(
            market_id,
            "Test Market".to_string(),
            "token123".to_string(),
            "YES".to_string(),
            dec!(10),
            dec!(0.42),
        );

        assert!(manager.has_position(&market_id));
        assert_eq!(manager.position_count(), 1);
    }

    #[test]
    fn test_disabled_when_zero_pct() {
        let manager = ExitManager::new(dec!(0), None, true);
        assert!(!manager.is_enabled());
    }
}
