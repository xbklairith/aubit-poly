//! Trade executor models.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use uuid::Uuid;

/// Detected spread arbitrage opportunity.
#[derive(Debug, Clone)]
pub struct SpreadOpportunity {
    pub market_id: Uuid,
    pub condition_id: String,
    pub market_name: String,
    pub asset: String,
    pub end_time: DateTime<Utc>,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub yes_price: Decimal,
    pub no_price: Decimal,
    pub spread: Decimal,
    pub profit_pct: Decimal,
    pub detected_at: DateTime<Utc>,
}

/// Trade execution details.
#[derive(Debug, Clone)]
pub struct TradeDetails {
    pub yes_shares: Decimal,
    pub no_shares: Decimal,
    pub yes_price: Decimal,
    pub no_price: Decimal,
    pub yes_cost: Decimal,
    pub no_cost: Decimal,
    pub total_invested: Decimal,
    pub payout: Decimal,
    pub gross_profit: Decimal,
    pub fee: Decimal,
    pub net_profit: Decimal,
    pub profit_pct: Decimal,
}

/// Result of a live trade execution attempt.
/// Distinguishes between successful execution and intentional abort.
#[derive(Debug, Clone)]
pub enum LiveTradeResult {
    /// Trade was successfully executed (orders placed).
    Executed {
        /// Amount actually invested after order fills.
        invested: Decimal,
        /// YES shares acquired.
        yes_filled: Decimal,
        /// NO shares acquired.
        no_filled: Decimal,
    },
    /// Trade was intentionally aborted before placing orders.
    /// This is NOT an error - validation detected unfavorable conditions.
    Aborted {
        /// Human-readable reason for abort.
        reason: String,
    },
}

impl LiveTradeResult {
    /// Returns true if the trade was executed.
    pub fn is_executed(&self) -> bool {
        matches!(self, LiveTradeResult::Executed { .. })
    }

    /// Returns true if the trade was aborted.
    pub fn is_aborted(&self) -> bool {
        matches!(self, LiveTradeResult::Aborted { .. })
    }
}

/// Bot state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BotState {
    Idle,
    Scanning,
    Trading,
    Stopping,
}

/// Session runtime state (in-memory).
#[derive(Debug, Clone)]
pub struct SessionState {
    pub id: Uuid,
    pub dry_run: bool,
    pub starting_balance: Decimal,
    pub current_balance: Decimal,
    pub total_trades: i32,
    pub winning_trades: i32,
    pub total_opportunities: i32,
    pub positions_opened: i32,
    pub positions_closed: i32,
    pub gross_profit: Decimal,
    pub fees_paid: Decimal,
    pub net_profit: Decimal,
    pub started_at: DateTime<Utc>,
    /// In-memory position cache for fast lookup
    pub open_positions: HashMap<Uuid, PositionCache>,
}

/// Cached position for fast lookup.
#[derive(Debug, Clone)]
pub struct PositionCache {
    pub id: Uuid,
    pub market_id: Uuid,
    pub market_name: String,
    pub yes_shares: Decimal,
    pub no_shares: Decimal,
    pub total_invested: Decimal,
    pub end_time: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal_macros::dec;

    // ============ BotState TESTS ============

    #[test]
    fn test_bot_state_default_is_idle() {
        // BotState doesn't derive Default, but Idle is the natural starting state
        let state = BotState::Idle;
        assert_eq!(state, BotState::Idle);
    }

    #[test]
    fn test_bot_state_transitions() {
        let mut state = BotState::Idle;

        // Idle -> Scanning
        state = BotState::Scanning;
        assert_eq!(state, BotState::Scanning);

        // Scanning -> Trading
        state = BotState::Trading;
        assert_eq!(state, BotState::Trading);

        // Trading -> Idle
        state = BotState::Idle;
        assert_eq!(state, BotState::Idle);
    }

    #[test]
    fn test_bot_state_copy() {
        let state1 = BotState::Trading;
        let state2 = state1; // Copy
        assert_eq!(state1, state2);
    }

    // ============ SessionState TESTS ============

    #[test]
    fn test_session_state_initialization() {
        let session = SessionState {
            id: Uuid::new_v4(),
            dry_run: true,
            starting_balance: dec!(1000),
            current_balance: dec!(1000),
            total_trades: 0,
            winning_trades: 0,
            total_opportunities: 0,
            positions_opened: 0,
            positions_closed: 0,
            gross_profit: dec!(0),
            fees_paid: dec!(0),
            net_profit: dec!(0),
            started_at: Utc::now(),
            open_positions: HashMap::new(),
        };

        assert!(session.dry_run);
        assert_eq!(session.starting_balance, dec!(1000));
        assert_eq!(session.current_balance, dec!(1000));
        assert_eq!(session.total_trades, 0);
        assert!(session.open_positions.is_empty());
    }

    #[test]
    fn test_session_state_with_positions() {
        let mut session = SessionState {
            id: Uuid::new_v4(),
            dry_run: false,
            starting_balance: dec!(1000),
            current_balance: dec!(800),
            total_trades: 1,
            winning_trades: 0,
            total_opportunities: 5,
            positions_opened: 1,
            positions_closed: 0,
            gross_profit: dec!(0),
            fees_paid: dec!(0.50),
            net_profit: dec!(-0.50),
            started_at: Utc::now(),
            open_positions: HashMap::new(),
        };

        let position = PositionCache {
            id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            market_name: "Test Market".to_string(),
            yes_shares: dec!(100),
            no_shares: dec!(100),
            total_invested: dec!(90),
            end_time: Utc::now() + Duration::hours(1),
        };

        session.open_positions.insert(position.market_id, position);

        assert_eq!(session.open_positions.len(), 1);
        assert!(!session.dry_run);
    }

    // ============ PositionCache TESTS ============

    #[test]
    fn test_position_cache_creation() {
        let market_id = Uuid::new_v4();
        let position = PositionCache {
            id: Uuid::new_v4(),
            market_id,
            market_name: "Will BTC hit $100k?".to_string(),
            yes_shares: dec!(50),
            no_shares: dec!(50),
            total_invested: dec!(45),
            end_time: Utc::now() + Duration::hours(2),
        };

        assert_eq!(position.yes_shares, dec!(50));
        assert_eq!(position.no_shares, dec!(50));
        assert_eq!(position.total_invested, dec!(45));
    }

    #[test]
    fn test_position_cache_clone() {
        let position = PositionCache {
            id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            market_name: "Test".to_string(),
            yes_shares: dec!(10),
            no_shares: dec!(10),
            total_invested: dec!(9),
            end_time: Utc::now(),
        };

        let cloned = position.clone();
        assert_eq!(cloned.id, position.id);
        assert_eq!(cloned.total_invested, position.total_invested);
    }

    // ============ SpreadOpportunity TESTS ============

    #[test]
    fn test_spread_opportunity_creation() {
        let opportunity = SpreadOpportunity {
            market_id: Uuid::new_v4(),
            condition_id: "condition-123".to_string(),
            market_name: "Will ETH reach $5000?".to_string(),
            asset: "ETH".to_string(),
            end_time: Utc::now() + Duration::hours(1),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            yes_price: dec!(0.45),
            no_price: dec!(0.45),
            spread: dec!(0.90),
            profit_pct: dec!(0.10),
            detected_at: Utc::now(),
        };

        assert_eq!(opportunity.asset, "ETH");
        assert_eq!(opportunity.spread, dec!(0.90));
        assert_eq!(opportunity.profit_pct, dec!(0.10));
    }

    // ============ TradeDetails TESTS ============

    #[test]
    fn test_trade_details_creation() {
        let details = TradeDetails {
            yes_shares: dec!(100),
            no_shares: dec!(100),
            yes_price: dec!(0.45),
            no_price: dec!(0.45),
            yes_cost: dec!(45),
            no_cost: dec!(45),
            total_invested: dec!(90),
            payout: dec!(100),
            gross_profit: dec!(10),
            fee: dec!(0.18),
            net_profit: dec!(9.82),
            profit_pct: dec!(0.109),
        };

        assert_eq!(details.total_invested, dec!(90));
        assert_eq!(details.payout, dec!(100));
        assert_eq!(details.gross_profit, dec!(10));
    }

    #[test]
    fn test_trade_details_clone() {
        let details = TradeDetails {
            yes_shares: dec!(50),
            no_shares: dec!(50),
            yes_price: dec!(0.48),
            no_price: dec!(0.48),
            yes_cost: dec!(24),
            no_cost: dec!(24),
            total_invested: dec!(48),
            payout: dec!(50),
            gross_profit: dec!(2),
            fee: dec!(0.10),
            net_profit: dec!(1.90),
            profit_pct: dec!(0.04),
        };

        let cloned = details.clone();
        assert_eq!(cloned.net_profit, details.net_profit);
    }

    // ============ LiveTradeResult TESTS ============

    #[test]
    fn test_live_trade_result_executed() {
        let result = LiveTradeResult::Executed {
            invested: dec!(100),
            yes_filled: dec!(50),
            no_filled: dec!(50),
        };

        assert!(result.is_executed());
        assert!(!result.is_aborted());

        match result {
            LiveTradeResult::Executed {
                invested,
                yes_filled,
                no_filled,
            } => {
                assert_eq!(invested, dec!(100));
                assert_eq!(yes_filled, dec!(50));
                assert_eq!(no_filled, dec!(50));
            }
            LiveTradeResult::Aborted { .. } => panic!("Expected Executed"),
        }
    }

    #[test]
    fn test_live_trade_result_aborted() {
        let result = LiveTradeResult::Aborted {
            reason: "Spread widened beyond tolerance".to_string(),
        };

        assert!(!result.is_executed());
        assert!(result.is_aborted());

        match result {
            LiveTradeResult::Aborted { reason } => {
                assert!(reason.contains("Spread widened"));
            }
            LiveTradeResult::Executed { .. } => panic!("Expected Aborted"),
        }
    }

    #[test]
    fn test_live_trade_result_clone() {
        let result = LiveTradeResult::Executed {
            invested: dec!(50),
            yes_filled: dec!(25),
            no_filled: dec!(25),
        };

        let cloned = result.clone();
        match cloned {
            LiveTradeResult::Executed { invested, .. } => {
                assert_eq!(invested, dec!(50));
            }
            _ => panic!("Clone should preserve variant"),
        }
    }
}
