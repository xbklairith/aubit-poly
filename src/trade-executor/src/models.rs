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
