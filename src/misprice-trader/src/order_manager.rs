//! Order management for tracking pending orders and their lifecycle.
//!
//! Solves two issues:
//! 1. Fire-and-forget cancel tasks are not tracked (orphan orders on crash)
//! 2. Orders assumed successful without verifying fills

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use common::{cancel_order_standalone, query_order_fill_standalone};
use rust_decimal_macros::dec;

/// Status of a pending order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrderStatus {
    /// Order placed, waiting for fill or cancel timeout
    Pending,
    /// Order was filled (cancel failed with "not found" or similar)
    Filled,
    /// Order was cancelled successfully
    Cancelled,
    /// Order status unknown (cancel attempt had ambiguous result)
    Unknown,
}

/// A pending order being tracked.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PendingOrder {
    pub order_id: String,
    pub market_id: Uuid,
    pub market_name: String,
    pub side: String,
    pub placed_at: DateTime<Utc>,
    pub cancel_timeout_secs: u64,
    pub status: OrderStatus,
    // Fields for exit manager tracking (live trading)
    pub token_id: Option<String>,
    pub shares: Option<rust_decimal::Decimal>,
    pub price: Option<rust_decimal::Decimal>,
    // Fields for settlement tracking (live trading)
    pub condition_id: Option<String>,
    pub yes_token_id: Option<String>,
    pub end_time: Option<DateTime<Utc>>,
    pub asset: Option<String>,
}

/// Result of a cancel attempt, sent back from the spawned task.
#[derive(Debug)]
#[allow(dead_code)]
pub struct CancelResult {
    pub order_id: String,
    pub market_id: Uuid,
    pub market_name: String,
    pub side: String,
    pub success: bool,
    pub was_filled: bool,
    pub error_msg: Option<String>,
    // Market info for exit manager (live trading)
    pub token_id: Option<String>,
    pub shares: Option<rust_decimal::Decimal>,
    pub price: Option<rust_decimal::Decimal>,
    // Market info for settlement tracking (live trading)
    pub condition_id: Option<String>,
    pub yes_token_id: Option<String>,
    pub end_time: Option<DateTime<Utc>>,
    pub asset: Option<String>,
}

/// Manages pending orders and their auto-cancel tasks.
pub struct OrderManager {
    /// Pending orders by order_id
    pending_orders: HashMap<String, PendingOrder>,
    /// Background cancel tasks
    cancel_tasks: JoinSet<CancelResult>,
    /// Default cancel timeout in seconds
    cancel_timeout_secs: u64,
}

impl OrderManager {
    /// Create a new order manager.
    ///
    /// # Arguments
    /// * `cancel_timeout_secs` - How long to wait before auto-cancelling orders
    pub fn new(cancel_timeout_secs: u64) -> Self {
        Self {
            pending_orders: HashMap::new(),
            cancel_tasks: JoinSet::new(),
            cancel_timeout_secs,
        }
    }

    /// Track a new order and schedule its auto-cancel.
    ///
    /// Returns true if order was added, false if order_id already exists.
    #[allow(dead_code)]
    pub fn track_order(
        &mut self,
        order_id: String,
        market_id: Uuid,
        market_name: String,
        side: String,
    ) -> bool {
        self.track_order_with_market_info(
            order_id,
            market_id,
            market_name,
            side,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    /// Track a new order with market info for exit manager and settlement tracking.
    ///
    /// Returns true if order was added, false if order_id already exists.
    #[allow(clippy::too_many_arguments)]
    pub fn track_order_with_market_info(
        &mut self,
        order_id: String,
        market_id: Uuid,
        market_name: String,
        side: String,
        token_id: Option<String>,
        shares: Option<rust_decimal::Decimal>,
        price: Option<rust_decimal::Decimal>,
        condition_id: Option<String>,
        yes_token_id: Option<String>,
        end_time: Option<DateTime<Utc>>,
        asset: Option<String>,
    ) -> bool {
        if self.pending_orders.contains_key(&order_id) {
            warn!(
                "[ORDER] Duplicate order_id {}, not tracking again",
                order_id
            );
            return false;
        }

        let order = PendingOrder {
            order_id: order_id.clone(),
            market_id,
            market_name: market_name.clone(),
            side: side.clone(),
            placed_at: Utc::now(),
            cancel_timeout_secs: self.cancel_timeout_secs,
            status: OrderStatus::Pending,
            token_id: token_id.clone(),
            shares,
            price,
            condition_id: condition_id.clone(),
            yes_token_id: yes_token_id.clone(),
            end_time,
            asset: asset.clone(),
        };

        self.pending_orders.insert(order_id.clone(), order);

        // Spawn the cancel task
        let oid = order_id.clone();
        let mid = market_id;
        let mname = market_name.clone();
        let s = side.clone();
        let timeout = self.cancel_timeout_secs;
        let tid = token_id;
        let sh = shares;
        let pr = price;
        let cid = condition_id;
        let ytid = yes_token_id;
        let et = end_time;
        let ast = asset;

        self.cancel_tasks.spawn(async move {
            tokio::time::sleep(Duration::from_secs(timeout)).await;

            // First, try to cancel the order
            let cancel_result = cancel_order_standalone(oid.clone()).await;
            let cancel_success = cancel_result.is_ok();
            let cancel_error = cancel_result.err().map(|e| e.to_string());

            // Query order status to check actual fill amount (regardless of cancel result)
            // Polymarket cancel returns Ok even for already-filled orders
            let (was_filled, filled_amount) = match query_order_fill_standalone(&oid).await {
                Ok(size_matched) => {
                    let filled = size_matched > dec!(0);
                    if filled {
                        info!("[FILLED] Order {} was filled: {} shares", oid, size_matched);
                    } else {
                        info!(
                            "[CANCEL] Order {} cancelled after {}s timeout (0 filled)",
                            oid, timeout
                        );
                    }
                    (filled, size_matched)
                }
                Err(e) => {
                    // Query failed - fall back to cancel error heuristic
                    warn!("[ORDER] Failed to query order {} status: {}", oid, e);
                    let was_filled = cancel_error
                        .as_ref()
                        .map(|err| {
                            let error_str = err.to_lowercase();
                            error_str.contains("not found")
                                || error_str.contains("already")
                                || error_str.contains("filled")
                                || error_str.contains("does not exist")
                        })
                        .unwrap_or(false);
                    (was_filled, dec!(0))
                }
            };

            CancelResult {
                order_id: oid,
                market_id: mid,
                market_name: mname,
                side: s,
                success: cancel_success && !was_filled,
                was_filled,
                error_msg: cancel_error,
                token_id: tid,
                shares: if was_filled { Some(filled_amount) } else { sh },
                price: pr,
                condition_id: cid,
                yes_token_id: ytid,
                end_time: et,
                asset: ast,
            }
        });

        debug!(
            "[ORDER] Tracking order {} for {} {} (cancel in {}s)",
            order_id, market_name, side, self.cancel_timeout_secs
        );
        true
    }

    /// Poll for completed cancel tasks and update order statuses.
    /// Returns list of CancelResult for orders that completed.
    pub fn poll_completed(&mut self) -> Vec<CancelResult> {
        let mut completed = Vec::new();

        // Non-blocking poll for completed tasks
        while let Some(result) = self.cancel_tasks.try_join_next() {
            match result {
                Ok(cancel_result) => {
                    // Update order status
                    if let Some(order) = self.pending_orders.get_mut(&cancel_result.order_id) {
                        order.status = if cancel_result.success {
                            OrderStatus::Cancelled
                        } else if cancel_result.was_filled {
                            OrderStatus::Filled
                        } else {
                            OrderStatus::Unknown
                        };
                    }

                    // Remove from pending
                    self.pending_orders.remove(&cancel_result.order_id);

                    completed.push(cancel_result);
                }
                Err(e) => {
                    error!("[ORDER] Cancel task panicked: {}", e);
                }
            }
        }

        completed
    }

    /// Check if there's a pending order for the given market and side.
    pub fn has_pending_order(&self, market_id: &Uuid, side: &str) -> bool {
        self.pending_orders
            .values()
            .any(|o| o.market_id == *market_id && o.side == side)
    }

    /// Get count of pending orders.
    #[allow(dead_code)]
    pub fn pending_count(&self) -> usize {
        self.pending_orders.len()
    }

    /// Cancel all pending orders immediately (for graceful shutdown).
    pub async fn cancel_all_pending(&mut self) {
        if self.pending_orders.is_empty() {
            return;
        }

        info!(
            "[SHUTDOWN] Cancelling {} pending orders...",
            self.pending_orders.len()
        );

        // Collect order IDs to cancel
        let orders_to_cancel: Vec<_> = self
            .pending_orders
            .values()
            .map(|o| (o.order_id.clone(), o.market_name.clone(), o.side.clone()))
            .collect();

        for (order_id, market_name, side) in orders_to_cancel {
            match cancel_order_standalone(order_id.clone()).await {
                Ok(()) => {
                    info!(
                        "[SHUTDOWN] Cancelled order {} ({} {})",
                        order_id, market_name, side
                    );
                }
                Err(e) => {
                    // Not necessarily an error - order might have been filled
                    debug!(
                        "[SHUTDOWN] Cancel {} returned: {} (may be filled)",
                        order_id, e
                    );
                }
            }
        }

        // Abort any remaining background tasks
        self.cancel_tasks.abort_all();
        self.pending_orders.clear();
    }

    /// Get order status by order_id.
    #[allow(dead_code)]
    pub fn get_status(&self, order_id: &str) -> Option<&OrderStatus> {
        self.pending_orders.get(order_id).map(|o| &o.status)
    }
}

impl Default for OrderManager {
    fn default() -> Self {
        Self::new(10) // 10 second default timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_manager_creation() {
        let manager = OrderManager::new(15);
        assert_eq!(manager.pending_count(), 0);
        assert_eq!(manager.cancel_timeout_secs, 15);
    }

    #[tokio::test]
    async fn test_has_pending_order() {
        let mut manager = OrderManager::new(10);
        let market_id = Uuid::new_v4();

        // No orders initially
        assert!(!manager.has_pending_order(&market_id, "YES"));

        // Track an order (spawns a background task)
        manager.track_order(
            "order123".to_string(),
            market_id,
            "Test Market".to_string(),
            "YES".to_string(),
        );

        assert!(manager.has_pending_order(&market_id, "YES"));
        assert!(!manager.has_pending_order(&market_id, "NO"));
        assert_eq!(manager.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_duplicate_order_rejected() {
        let mut manager = OrderManager::new(10);
        let market_id = Uuid::new_v4();

        let first = manager.track_order(
            "order123".to_string(),
            market_id,
            "Test Market".to_string(),
            "YES".to_string(),
        );
        assert!(first);

        let second = manager.track_order(
            "order123".to_string(),
            market_id,
            "Test Market".to_string(),
            "YES".to_string(),
        );
        assert!(!second);
        assert_eq!(manager.pending_count(), 1);
    }
}
