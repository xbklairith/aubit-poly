//! Shared data models for markets and orderbooks.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// Market type classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "VARCHAR", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum MarketType {
    UpDown,
    Above,
    PriceRange,
}

/// A prediction market from the database.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Market {
    pub id: Uuid,
    pub condition_id: String,
    pub market_type: String,
    pub asset: String,
    pub timeframe: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub name: String,
    pub end_time: DateTime<Utc>,
    pub is_active: bool,
    pub discovered_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Orderbook snapshot from the database.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct OrderbookSnapshot {
    pub id: i64,
    pub market_id: Uuid,
    pub yes_best_ask: Option<Decimal>,
    pub yes_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub yes_asks: Option<serde_json::Value>,
    pub yes_bids: Option<serde_json::Value>,
    pub no_asks: Option<serde_json::Value>,
    pub no_bids: Option<serde_json::Value>,
    pub captured_at: DateTime<Utc>,
    /// When YES side was last updated (from Polymarket event timestamp)
    pub yes_updated_at: Option<DateTime<Utc>>,
    /// When NO side was last updated (from Polymarket event timestamp)
    pub no_updated_at: Option<DateTime<Utc>>,
}

/// A single price level in the orderbook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Position status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionStatus {
    Open,
    Closed,
}

/// A trading position.
#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Position {
    pub id: Uuid,
    pub market_id: Uuid,
    pub yes_shares: Decimal,
    pub no_shares: Decimal,
    pub total_invested: Decimal,
    pub status: String,
    pub is_dry_run: bool,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}
