//! Database repository functions for markets and orderbooks.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use crate::gamma::{MarketType, ParsedMarket};
use crate::models::Market;

/// Market with fresh orderbook prices (result of LATERAL JOIN query).
#[derive(Debug, Clone)]
pub struct MarketWithPrices {
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
    // Orderbook prices
    pub yes_best_ask: Option<Decimal>,
    pub yes_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub captured_at: DateTime<Utc>,
}

/// Market with full orderbook depth for realistic fill price calculation.
#[derive(Debug, Clone)]
pub struct MarketWithOrderbook {
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
    // Orderbook prices
    pub yes_best_ask: Option<Decimal>,
    pub yes_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    // Full orderbook depth (JSON arrays of {price, size})
    pub yes_asks: Option<serde_json::Value>,
    pub no_asks: Option<serde_json::Value>,
    pub captured_at: DateTime<Utc>,
}

/// A price level in the orderbook.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OrderbookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

/// Result of fill price calculation.
#[derive(Debug, Clone)]
pub struct FillEstimate {
    /// Weighted average fill price across all levels
    pub effective_price: Decimal,
    /// Total shares that can be filled at this price
    pub filled_shares: Decimal,
    /// Whether the full order can be filled
    pub fully_filled: bool,
    /// Available depth at best price only
    pub best_price_depth: Decimal,
}

/// Calculate effective fill price based on orderbook depth.
/// Returns weighted average price for filling `shares` from the orderbook.
/// Note: Sorts orderbook levels by price ascending (best price first).
pub fn calculate_effective_fill_price(
    orderbook_json: Option<&serde_json::Value>,
    shares: Decimal,
) -> Option<FillEstimate> {
    let orderbook = orderbook_json?;

    // Parse orderbook levels
    let mut levels: Vec<OrderbookLevel> = serde_json::from_value(orderbook.clone()).ok()?;

    if levels.is_empty() {
        return None;
    }

    // Sort by price ascending (best/lowest price first for asks)
    levels.sort_by(|a, b| a.price.cmp(&b.price));

    let best_price_depth = levels.first().map(|l| l.size).unwrap_or_default();

    let mut remaining = shares;
    let mut total_cost = Decimal::ZERO;
    let mut total_filled = Decimal::ZERO;

    for level in &levels {
        if remaining <= Decimal::ZERO {
            break;
        }

        // Skip zero-size levels
        if level.size <= Decimal::ZERO {
            continue;
        }

        let fill_at_level = remaining.min(level.size);
        total_cost += fill_at_level * level.price;
        total_filled += fill_at_level;
        remaining -= fill_at_level;
    }

    if total_filled == Decimal::ZERO {
        return None;
    }

    let effective_price = total_cost / total_filled;

    Some(FillEstimate {
        effective_price,
        filled_shares: total_filled,
        fully_filled: remaining <= Decimal::ZERO,
        best_price_depth,
    })
}

/// Calculate fill price with slippage fallback.
/// Uses orderbook depth when available, otherwise applies slippage percentage.
/// Note: When using slippage fallback, `fully_filled` is false (unknown) and
/// `best_price_depth` is zero (unknown). Callers should handle this appropriately.
pub fn calculate_fill_price_with_slippage(
    orderbook_json: Option<&serde_json::Value>,
    best_ask: Decimal,
    shares: Decimal,
    slippage_pct: Decimal,
) -> FillEstimate {
    // Try orderbook-based calculation first
    if let Some(estimate) = calculate_effective_fill_price(orderbook_json, shares) {
        return estimate;
    }

    // Fallback: apply slippage to best ask
    // We don't know actual depth, so be conservative with flags
    let slippage_multiplier = Decimal::ONE + (slippage_pct / Decimal::from(100));
    let effective_price = best_ask * slippage_multiplier;

    FillEstimate {
        effective_price,
        filled_shares: shares,
        fully_filled: false,             // Unknown - orderbook depth unavailable
        best_price_depth: Decimal::ZERO, // Unknown - no orderbook data
    }
}

/// Upsert a market into the database.
/// Updates existing market if (platform, condition_id) matches, otherwise inserts new.
/// Default platform is 'polymarket' for backwards compatibility.
pub async fn upsert_market(pool: &PgPool, market: &ParsedMarket) -> Result<Uuid, sqlx::Error> {
    let market_type_str = match market.market_type {
        MarketType::UpDown => "up_down",
        MarketType::Above => "above",
        MarketType::PriceRange => "price_range",
        MarketType::Unknown => "unknown",
    };

    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO markets (platform, condition_id, market_type, asset, timeframe, yes_token_id, no_token_id, name, end_time, is_active)
        VALUES ('polymarket', $1, $2, $3, $4, $5, $6, $7, $8, true)
        ON CONFLICT (platform, condition_id) DO UPDATE SET
            market_type = EXCLUDED.market_type,
            asset = EXCLUDED.asset,
            timeframe = EXCLUDED.timeframe,
            yes_token_id = EXCLUDED.yes_token_id,
            no_token_id = EXCLUDED.no_token_id,
            name = EXCLUDED.name,
            end_time = EXCLUDED.end_time,
            is_active = true,
            updated_at = NOW()
        RETURNING id
        "#,
        market.condition_id,
        market_type_str,
        market.asset,
        market.timeframe,
        market.yes_token_id,
        market.no_token_id,
        market.name,
        market.end_time,
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Mark expired markets as inactive.
pub async fn deactivate_expired_markets(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let result = sqlx::query!(
        r#"
        UPDATE markets
        SET is_active = false, updated_at = NOW()
        WHERE is_active = true AND end_time < $1
        "#,
        Utc::now(),
    )
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

/// Get all active markets.
pub async fn get_active_markets(pool: &PgPool) -> Result<Vec<Market>, sqlx::Error> {
    let markets = sqlx::query_as!(
        Market,
        r#"
        SELECT
            id,
            condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id,
            no_token_id,
            name,
            end_time,
            COALESCE(is_active, true) as "is_active!",
            COALESCE(discovered_at, NOW()) as "discovered_at!",
            COALESCE(updated_at, NOW()) as "updated_at!"
        FROM markets
        WHERE is_active = true
        ORDER BY end_time ASC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(markets)
}

/// Get active markets expiring within a given number of hours.
/// This is optimized for orderbook streaming where we want to focus on
/// near-term markets that are relevant for trading.
pub async fn get_active_markets_expiring_within(
    pool: &PgPool,
    hours: i32,
    limit: i64,
) -> Result<Vec<Market>, sqlx::Error> {
    let markets = sqlx::query_as!(
        Market,
        r#"
        SELECT
            id,
            condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id,
            no_token_id,
            name,
            end_time,
            COALESCE(is_active, true) as "is_active!",
            COALESCE(discovered_at, NOW()) as "discovered_at!",
            COALESCE(updated_at, NOW()) as "updated_at!"
        FROM markets
        WHERE is_active = true
          AND end_time > NOW()
          AND end_time <= NOW() + ($1 || ' hours')::interval
        ORDER BY end_time ASC
        LIMIT $2
        "#,
        hours.to_string(),
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(markets)
}

/// Get priority markets using hybrid strategy:
/// - Crypto markets (BTC, ETH, SOL, XRP) expiring within crypto_hours
/// - Event markets (all other assets) expiring within event_days
///
/// This enables monitoring short-term crypto markets alongside longer-dated event markets.
pub async fn get_priority_markets_hybrid(
    pool: &PgPool,
    crypto_hours: i32,
    event_days: i32,
    crypto_limit: i64,
    event_limit: i64,
) -> Result<Vec<Market>, sqlx::Error> {
    // Fetch crypto and event markets separately, then combine
    // This avoids UNION ALL issues with sqlx type inference
    let crypto_markets = sqlx::query_as!(
        Market,
        r#"
        SELECT
            id,
            condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id,
            no_token_id,
            name,
            end_time,
            COALESCE(is_active, true) as "is_active!",
            COALESCE(discovered_at, NOW()) as "discovered_at!",
            COALESCE(updated_at, NOW()) as "updated_at!"
        FROM markets
        WHERE is_active = true
          AND asset IN ('BTC', 'ETH', 'SOL', 'XRP')
          AND end_time > NOW()
          AND end_time <= NOW() + ($1 || ' hours')::interval
        ORDER BY end_time ASC
        LIMIT $2
        "#,
        crypto_hours.to_string(),
        crypto_limit
    )
    .fetch_all(pool)
    .await?;

    let event_markets = sqlx::query_as!(
        Market,
        r#"
        SELECT
            id,
            condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id,
            no_token_id,
            name,
            end_time,
            COALESCE(is_active, true) as "is_active!",
            COALESCE(discovered_at, NOW()) as "discovered_at!",
            COALESCE(updated_at, NOW()) as "updated_at!"
        FROM markets
        WHERE is_active = true
          AND asset NOT IN ('BTC', 'ETH', 'SOL', 'XRP')
          AND end_time > NOW()
          AND end_time <= NOW() + ($1 || ' days')::interval
        ORDER BY end_time ASC
        LIMIT $2
        "#,
        event_days.to_string(),
        event_limit
    )
    .fetch_all(pool)
    .await?;

    // Combine and sort by end_time
    let mut markets = crypto_markets;
    markets.extend(event_markets);
    markets.sort_by(|a, b| a.end_time.cmp(&b.end_time));

    Ok(markets)
}

/// Count active markets by type.
pub async fn count_markets_by_type(pool: &PgPool) -> Result<Vec<(String, i64)>, sqlx::Error> {
    let counts = sqlx::query!(
        r#"
        SELECT market_type, COUNT(*) as count
        FROM markets
        WHERE is_active = true
        GROUP BY market_type
        ORDER BY count DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(counts
        .into_iter()
        .map(|r| (r.market_type, r.count.unwrap_or(0)))
        .collect())
}

/// Upsert an orderbook snapshot into the database.
/// Uses ON CONFLICT to update existing snapshot for the market, keeping the DB clean.
#[allow(clippy::too_many_arguments)]
pub async fn insert_orderbook_snapshot(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<rust_decimal::Decimal>,
    yes_best_bid: Option<rust_decimal::Decimal>,
    no_best_ask: Option<rust_decimal::Decimal>,
    no_best_bid: Option<rust_decimal::Decimal>,
    yes_asks: Option<serde_json::Value>,
    yes_bids: Option<serde_json::Value>,
    no_asks: Option<serde_json::Value>,
    no_bids: Option<serde_json::Value>,
    event_timestamp: Option<DateTime<Utc>>,
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO orderbook_snapshots (market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, yes_asks, yes_bids, no_asks, no_bids, captured_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, NOW()))
        ON CONFLICT (market_id) DO UPDATE SET
            yes_best_ask = EXCLUDED.yes_best_ask,
            yes_best_bid = EXCLUDED.yes_best_bid,
            no_best_ask = EXCLUDED.no_best_ask,
            no_best_bid = EXCLUDED.no_best_bid,
            yes_asks = EXCLUDED.yes_asks,
            yes_bids = EXCLUDED.yes_bids,
            no_asks = EXCLUDED.no_asks,
            no_bids = EXCLUDED.no_bids,
            captured_at = COALESCE($10, NOW())
        RETURNING id
        "#,
        market_id,
        yes_best_ask,
        yes_best_bid,
        no_best_ask,
        no_best_bid,
        yes_asks,
        yes_bids,
        no_asks,
        no_bids,
        event_timestamp,
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Update only the YES side of an orderbook snapshot.
/// Preserves the NO side data to prevent stale overwrites.
/// Uses the event timestamp from Polymarket WebSocket for accurate freshness tracking.
pub async fn update_yes_orderbook(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<rust_decimal::Decimal>,
    yes_best_bid: Option<rust_decimal::Decimal>,
    yes_asks: Option<serde_json::Value>,
    yes_bids: Option<serde_json::Value>,
    event_timestamp: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    let ts = event_timestamp.unwrap_or_else(Utc::now);
    sqlx::query!(
        r#"
        INSERT INTO orderbook_snapshots (market_id, yes_best_ask, yes_best_bid, yes_asks, yes_bids, captured_at, yes_updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6)
        ON CONFLICT (market_id) DO UPDATE SET
            yes_best_ask = $2,
            yes_best_bid = $3,
            yes_asks = $4,
            yes_bids = $5,
            captured_at = NOW(),
            yes_updated_at = $6
        "#,
        market_id,
        yes_best_ask,
        yes_best_bid,
        yes_asks,
        yes_bids,
        ts,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Update only the NO side of an orderbook snapshot.
/// Preserves the YES side data to prevent stale overwrites.
/// Uses the event timestamp from Polymarket WebSocket for accurate freshness tracking.
pub async fn update_no_orderbook(
    pool: &PgPool,
    market_id: Uuid,
    no_best_ask: Option<rust_decimal::Decimal>,
    no_best_bid: Option<rust_decimal::Decimal>,
    no_asks: Option<serde_json::Value>,
    no_bids: Option<serde_json::Value>,
    event_timestamp: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    let ts = event_timestamp.unwrap_or_else(Utc::now);
    sqlx::query!(
        r#"
        INSERT INTO orderbook_snapshots (market_id, no_best_ask, no_best_bid, no_asks, no_bids, captured_at, no_updated_at)
        VALUES ($1, $2, $3, $4, $5, NOW(), $6)
        ON CONFLICT (market_id) DO UPDATE SET
            no_best_ask = $2,
            no_best_bid = $3,
            no_asks = $4,
            no_bids = $5,
            captured_at = NOW(),
            no_updated_at = $6
        "#,
        market_id,
        no_best_ask,
        no_best_bid,
        no_asks,
        no_bids,
        ts,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Update only YES best prices (lightweight update for price_change messages).
/// Only updates best_ask, best_bid, and timestamps - no JSON columns.
pub async fn update_yes_best_prices(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<rust_decimal::Decimal>,
    yes_best_bid: Option<rust_decimal::Decimal>,
    event_timestamp: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    let ts = event_timestamp.unwrap_or_else(Utc::now);
    sqlx::query!(
        r#"
        UPDATE orderbook_snapshots
        SET yes_best_ask = $2,
            yes_best_bid = $3,
            captured_at = NOW(),
            yes_updated_at = $4
        WHERE market_id = $1
        "#,
        market_id,
        yes_best_ask,
        yes_best_bid,
        ts,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Update only NO best prices (lightweight update for price_change messages).
/// Only updates best_ask, best_bid, and timestamps - no JSON columns.
pub async fn update_no_best_prices(
    pool: &PgPool,
    market_id: Uuid,
    no_best_ask: Option<rust_decimal::Decimal>,
    no_best_bid: Option<rust_decimal::Decimal>,
    event_timestamp: Option<DateTime<Utc>>,
) -> Result<(), sqlx::Error> {
    let ts = event_timestamp.unwrap_or_else(Utc::now);
    sqlx::query!(
        r#"
        UPDATE orderbook_snapshots
        SET no_best_ask = $2,
            no_best_bid = $3,
            captured_at = NOW(),
            no_updated_at = $4
        WHERE market_id = $1
        "#,
        market_id,
        no_best_ask,
        no_best_bid,
        ts,
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Get the latest orderbook snapshot for a market.
pub async fn get_latest_orderbook_snapshot(
    pool: &PgPool,
    market_id: Uuid,
) -> Result<Option<crate::models::OrderbookSnapshot>, sqlx::Error> {
    let snapshot = sqlx::query_as!(
        crate::models::OrderbookSnapshot,
        r#"
        SELECT
            id,
            market_id,
            yes_best_ask,
            yes_best_bid,
            no_best_ask,
            no_best_bid,
            spread,
            yes_asks,
            yes_bids,
            no_asks,
            no_bids,
            COALESCE(captured_at, NOW()) as "captured_at!",
            yes_updated_at,
            no_updated_at
        FROM orderbook_snapshots
        WHERE market_id = $1
        ORDER BY captured_at DESC
        LIMIT 1
        "#,
        market_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(snapshot)
}

/// Get market by condition_id.
pub async fn get_market_by_condition_id(
    pool: &PgPool,
    condition_id: &str,
) -> Result<Option<crate::models::Market>, sqlx::Error> {
    let market = sqlx::query_as!(
        crate::models::Market,
        r#"
        SELECT
            id,
            condition_id,
            market_type,
            asset,
            timeframe,
            yes_token_id,
            no_token_id,
            name,
            end_time,
            COALESCE(is_active, true) as "is_active!",
            COALESCE(discovered_at, NOW()) as "discovered_at!",
            COALESCE(updated_at, NOW()) as "updated_at!"
        FROM markets
        WHERE condition_id = $1
        "#,
        condition_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(market)
}

/// Get markets with fresh orderbook prices using DISTINCT ON.
/// Only returns markets where BOTH yes_updated_at and no_updated_at are within max_age_seconds.
/// This ensures both sides of the orderbook are fresh for accurate spread detection.
pub async fn get_markets_with_fresh_orderbooks(
    pool: &PgPool,
    max_age_seconds: i32,
    assets: &[String],
    max_expiry_seconds: i64,
) -> Result<Vec<MarketWithPrices>, sqlx::Error> {
    // Pre-compute timestamps in Rust to avoid make_interval() in SQL
    let snapshot_cutoff = Utc::now() - chrono::Duration::seconds(max_age_seconds as i64);
    let expiry_cutoff = Utc::now() + chrono::Duration::seconds(max_expiry_seconds);

    // Check if "ALL" is in assets list to skip asset filtering
    if assets.iter().any(|a| a.eq_ignore_ascii_case("ALL")) {
        return get_all_markets_with_fresh_orderbooks(pool, max_age_seconds, max_expiry_seconds)
            .await;
    }

    // Use DISTINCT ON instead of LATERAL JOIN for better performance
    // LATERAL executes a subquery per market row (N queries)
    // DISTINCT ON scans orderbook_snapshots once and deduplicates (1 query)
    // Filter requires BOTH yes_updated_at AND no_updated_at to be fresh
    let results = sqlx::query_as!(
        MarketWithPrices,
        r#"
        SELECT
            m.id,
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            o.yes_best_ask,
            o.yes_best_bid,
            o.no_best_ask,
            o.no_best_bid,
            o.captured_at as "captured_at!"
        FROM markets m
        INNER JOIN (
            SELECT DISTINCT ON (market_id)
                market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
            FROM orderbook_snapshots
            WHERE yes_updated_at > $1
              AND no_updated_at > $1
            ORDER BY market_id, captured_at DESC
        ) o ON o.market_id = m.id
        WHERE m.is_active = true
          AND m.asset = ANY($2)
          AND m.end_time > NOW()
          AND m.end_time <= $3
        ORDER BY m.end_time ASC
        "#,
        snapshot_cutoff,
        assets,
        expiry_cutoff,
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Get short-timeframe (5m/15m) up/down markets with fresh orderbooks.
/// Used by the contrarian scalper to target specific market types.
pub async fn get_15m_updown_markets_with_fresh_orderbooks(
    pool: &PgPool,
    max_age_seconds: i32,
    assets: &[String],
    max_expiry_seconds: i64,
    timeframes: &[String],
) -> Result<Vec<MarketWithPrices>, sqlx::Error> {
    let snapshot_cutoff = Utc::now() - chrono::Duration::seconds(max_age_seconds as i64);
    let expiry_cutoff = Utc::now() + chrono::Duration::seconds(max_expiry_seconds);
    let timeframes = timeframes.to_vec();

    let results = sqlx::query_as!(
        MarketWithPrices,
        r#"
        SELECT
            m.id,
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            o.yes_best_ask,
            o.yes_best_bid,
            o.no_best_ask,
            o.no_best_bid,
            o.captured_at as "captured_at!"
        FROM markets m
        INNER JOIN (
            SELECT DISTINCT ON (market_id)
                market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
            FROM orderbook_snapshots
            WHERE yes_updated_at > $1
              AND no_updated_at > $1
            ORDER BY market_id, captured_at DESC
        ) o ON o.market_id = m.id
        WHERE m.is_active = true
          AND m.asset = ANY($2)
          AND m.timeframe = ANY($4)
          AND m.market_type = 'up_down'
          AND m.end_time > NOW()
          AND m.end_time <= $3
        ORDER BY m.end_time ASC
        "#,
        snapshot_cutoff,
        assets,
        expiry_cutoff,
        &timeframes,
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Get short-timeframe (5m/15m) up/down markets with full orderbook depth.
/// Includes yes_asks and no_asks for realistic fill price calculation.
pub async fn get_15m_updown_markets_with_orderbooks(
    pool: &PgPool,
    max_age_seconds: i32,
    assets: &[String],
    max_expiry_seconds: i64,
    timeframes: &[String],
) -> Result<Vec<MarketWithOrderbook>, sqlx::Error> {
    let snapshot_cutoff = Utc::now() - chrono::Duration::seconds(max_age_seconds as i64);
    let expiry_cutoff = Utc::now() + chrono::Duration::seconds(max_expiry_seconds);
    let timeframes = timeframes.to_vec();

    let results = sqlx::query_as!(
        MarketWithOrderbook,
        r#"
        SELECT
            m.id,
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            o.yes_best_ask,
            o.yes_best_bid,
            o.no_best_ask,
            o.no_best_bid,
            o.yes_asks,
            o.no_asks,
            o.captured_at as "captured_at!"
        FROM markets m
        INNER JOIN (
            SELECT DISTINCT ON (market_id)
                market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid,
                yes_asks, no_asks, captured_at
            FROM orderbook_snapshots
            WHERE yes_updated_at > $1
              AND no_updated_at > $1
            ORDER BY market_id, captured_at DESC
        ) o ON o.market_id = m.id
        WHERE m.is_active = true
          AND m.asset = ANY($2)
          AND m.timeframe = ANY($4)
          AND m.market_type = 'up_down'
          AND m.end_time > NOW()
          AND m.end_time <= $3
        ORDER BY m.end_time ASC
        "#,
        snapshot_cutoff,
        assets,
        expiry_cutoff,
        &timeframes,
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Get all markets with fresh orderbooks (no asset filter).
pub async fn get_all_markets_with_fresh_orderbooks(
    pool: &PgPool,
    max_age_seconds: i32,
    max_expiry_seconds: i64,
) -> Result<Vec<MarketWithPrices>, sqlx::Error> {
    let snapshot_cutoff = Utc::now() - chrono::Duration::seconds(max_age_seconds as i64);
    let expiry_cutoff = Utc::now() + chrono::Duration::seconds(max_expiry_seconds);

    let results = sqlx::query_as!(
        MarketWithPrices,
        r#"
        SELECT
            m.id,
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            o.yes_best_ask,
            o.yes_best_bid,
            o.no_best_ask,
            o.no_best_bid,
            o.captured_at as "captured_at!"
        FROM markets m
        INNER JOIN (
            SELECT DISTINCT ON (market_id)
                market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
            FROM orderbook_snapshots
            WHERE yes_updated_at > $1
              AND no_updated_at > $1
            ORDER BY market_id, captured_at DESC
        ) o ON o.market_id = m.id
        WHERE m.is_active = true
          AND m.end_time > NOW()
          AND m.end_time <= $2
        ORDER BY m.end_time ASC
        "#,
        snapshot_cutoff,
        expiry_cutoff,
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Create a new trading position.
pub async fn create_position(
    pool: &PgPool,
    market_id: Uuid,
    yes_shares: Decimal,
    no_shares: Decimal,
    total_invested: Decimal,
    is_dry_run: bool,
) -> Result<Uuid, sqlx::Error> {
    let id = sqlx::query_scalar!(
        r#"
        INSERT INTO positions (market_id, yes_shares, no_shares, total_invested, is_dry_run, status)
        VALUES ($1, $2, $3, $4, $5, 'open')
        RETURNING id
        "#,
        market_id,
        yes_shares,
        no_shares,
        total_invested,
        is_dry_run
    )
    .fetch_one(pool)
    .await?;

    Ok(id)
}

/// Record a trade execution.
pub async fn record_trade(
    pool: &PgPool,
    position_id: Uuid,
    side: &str,
    action: &str,
    price: Decimal,
    shares: Decimal,
) -> Result<Uuid, sqlx::Error> {
    let id = sqlx::query_scalar!(
        r#"
        INSERT INTO trades (position_id, side, action, price, shares)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        "#,
        position_id,
        side,
        action,
        price,
        shares
    )
    .fetch_one(pool)
    .await?;

    Ok(id)
}

/// Record a trade execution with order tracking (Polymarket order ID and fill amount).
#[allow(clippy::too_many_arguments)]
pub async fn record_trade_with_order(
    pool: &PgPool,
    position_id: Uuid,
    side: &str,
    action: &str,
    price: Decimal,
    shares: Decimal,
    order_id: Option<&str>,
    filled_shares: Decimal,
    order_status: &str,
) -> Result<Uuid, sqlx::Error> {
    let id = sqlx::query_scalar!(
        r#"
        INSERT INTO trades (position_id, side, action, price, shares, order_id, filled_shares, order_status)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id
        "#,
        position_id,
        side,
        action,
        price,
        shares,
        order_id,
        filled_shares,
        order_status
    )
    .fetch_one(pool)
    .await?;

    Ok(id)
}

/// Update position with actual filled amounts.
pub async fn update_position_fills(
    pool: &PgPool,
    position_id: Uuid,
    yes_filled: Decimal,
    no_filled: Decimal,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        UPDATE positions
        SET yes_filled = $2, no_filled = $3
        WHERE id = $1
        "#,
        position_id,
        yes_filled,
        no_filled
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Get all open positions.
pub async fn get_open_positions(
    pool: &PgPool,
) -> Result<Vec<crate::models::Position>, sqlx::Error> {
    let positions = sqlx::query_as!(
        crate::models::Position,
        r#"
        SELECT
            id,
            market_id,
            yes_shares,
            no_shares,
            total_invested,
            COALESCE(status, 'open') as "status!",
            COALESCE(is_dry_run, true) as "is_dry_run!",
            COALESCE(opened_at, NOW()) as "opened_at!",
            closed_at
        FROM positions
        WHERE status = 'open'
        ORDER BY opened_at DESC
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(positions)
}

/// Close a position.
pub async fn close_position(
    pool: &PgPool,
    position_id: Uuid,
    payout: Decimal,
    realized_pnl: Decimal,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        UPDATE positions
        SET status = 'closed',
            closed_at = NOW()
        WHERE id = $1
        "#,
        position_id
    )
    .execute(pool)
    .await?;

    // Note: payout and realized_pnl would be stored if we add those columns
    // For now, just close the position
    let _ = (payout, realized_pnl); // Silence unused warnings

    Ok(())
}

/// Market resolution info
#[derive(Debug, Clone)]
pub struct MarketResolution {
    pub market_id: Uuid,
    pub winning_side: String,
    pub resolved_at: Option<DateTime<Utc>>,
}

/// Input for inserting a market resolution
#[derive(Debug, Clone)]
pub struct MarketResolutionInsert {
    pub condition_id: String,
    pub market_type: String,
    pub asset: String,
    pub timeframe: String,
    pub name: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub winning_side: String,
    pub end_time: DateTime<Utc>,
}

/// Insert or update a market resolution.
pub async fn upsert_market_resolution(
    pool: &PgPool,
    resolution: &MarketResolutionInsert,
) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO market_resolutions (
            condition_id, market_type, asset, timeframe, name,
            yes_token_id, no_token_id, winning_side, end_time, resolved_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
        ON CONFLICT (condition_id) DO UPDATE SET
            winning_side = EXCLUDED.winning_side,
            resolved_at = NOW()
        "#,
        resolution.condition_id,
        resolution.market_type,
        resolution.asset,
        resolution.timeframe,
        resolution.name,
        resolution.yes_token_id,
        resolution.no_token_id,
        resolution.winning_side,
        resolution.end_time,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Get market resolution by market_id (joins markets table to get condition_id).
/// Returns the winning side if the market has been resolved.
pub async fn get_market_resolution(
    pool: &PgPool,
    market_id: Uuid,
) -> Result<Option<MarketResolution>, sqlx::Error> {
    let result = sqlx::query_as!(
        MarketResolution,
        r#"
        SELECT
            m.id as market_id,
            r.winning_side,
            r.resolved_at
        FROM markets m
        JOIN market_resolutions r ON r.condition_id = m.condition_id
        WHERE m.id = $1
        "#,
        market_id
    )
    .fetch_optional(pool)
    .await?;

    Ok(result)
}

/// Get market resolutions for multiple market IDs.
pub async fn get_market_resolutions_batch(
    pool: &PgPool,
    market_ids: &[Uuid],
) -> Result<Vec<MarketResolution>, sqlx::Error> {
    if market_ids.is_empty() {
        return Ok(Vec::new());
    }

    let results = sqlx::query_as!(
        MarketResolution,
        r#"
        SELECT
            m.id as market_id,
            r.winning_side,
            r.resolved_at
        FROM markets m
        JOIN market_resolutions r ON r.condition_id = m.condition_id
        WHERE m.id = ANY($1)
        "#,
        market_ids
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

// =============================================================================
// KALSHI AND CROSS-PLATFORM FUNCTIONS
// =============================================================================

use crate::kalshi::ParsedKalshiMarket;
use crate::limitless::ParsedLimitlessMarket;

/// Kalshi market for database insertion.
#[derive(Debug, Clone)]
pub struct KalshiMarketInsert {
    pub ticker: String,
    pub name: String,
    pub asset: String,
    pub timeframe: String,
    pub end_time: DateTime<Utc>,
    pub yes_best_bid: Option<Decimal>,
    pub yes_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub liquidity: Option<Decimal>,
    pub rules_primary: Option<String>,
    pub strike_price: Option<f64>,
    pub direction: Option<String>,
}

impl From<&ParsedKalshiMarket> for KalshiMarketInsert {
    fn from(m: &ParsedKalshiMarket) -> Self {
        Self {
            ticker: m.ticker.clone(),
            name: m.name.clone(),
            asset: m.asset.clone(),
            timeframe: m.timeframe.clone(),
            end_time: m.close_time,
            yes_best_bid: m.yes_best_bid,
            yes_best_ask: m.yes_best_ask,
            no_best_bid: m.no_best_bid,
            no_best_ask: m.no_best_ask,
            liquidity: m.liquidity,
            rules_primary: m.rules_primary.clone(),
            strike_price: m.strike_price,
            direction: m.direction.clone(),
        }
    }
}

/// Upsert a Kalshi market into the database.
/// Uses platform + condition_id (ticker for Kalshi) as unique key.
pub async fn upsert_kalshi_market(
    pool: &PgPool,
    market: &KalshiMarketInsert,
) -> Result<Uuid, sqlx::Error> {
    // Determine market type from direction
    let market_type = match market.direction.as_deref() {
        Some("above") | Some("below") => "above",
        _ => "unknown",
    };

    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO markets (
            platform, condition_id, market_type, asset, timeframe,
            yes_token_id, no_token_id, name, end_time,
            rules_primary, liquidity_dollars, strike_price, direction,
            is_active
        )
        VALUES (
            'kalshi', $1, $2, $3, $4,
            $1, $1, $5, $6,
            $7, $8, $9, $10,
            true
        )
        ON CONFLICT (platform, condition_id) DO UPDATE SET
            market_type = EXCLUDED.market_type,
            asset = EXCLUDED.asset,
            timeframe = EXCLUDED.timeframe,
            name = EXCLUDED.name,
            end_time = EXCLUDED.end_time,
            rules_primary = EXCLUDED.rules_primary,
            liquidity_dollars = EXCLUDED.liquidity_dollars,
            strike_price = EXCLUDED.strike_price,
            direction = EXCLUDED.direction,
            is_active = true,
            updated_at = NOW()
        RETURNING id
        "#,
        market.ticker,
        market_type,
        market.asset,
        market.timeframe,
        market.name,
        market.end_time,
        market.rules_primary,
        market.liquidity,
        market.strike_price.map(|p| p as i64),
        market.direction,
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Market with platform info for cross-platform queries.
#[derive(Debug, Clone)]
pub struct MarketWithPlatform {
    pub id: Uuid,
    pub platform: String,
    pub condition_id: String,
    pub market_type: String,
    pub asset: String,
    pub timeframe: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    pub name: String,
    pub end_time: DateTime<Utc>,
    pub is_active: bool,
    pub direction: Option<String>,
    pub strike_price: Option<f64>,
    pub liquidity_dollars: Option<Decimal>,
    // Orderbook prices (if joined)
    pub yes_best_ask: Option<Decimal>,
    pub yes_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub captured_at: Option<DateTime<Utc>>,
}

/// Get active markets for a specific platform.
pub async fn get_markets_by_platform(
    pool: &PgPool,
    platform: &str,
) -> Result<Vec<MarketWithPlatform>, sqlx::Error> {
    let markets = sqlx::query_as!(
        MarketWithPlatform,
        r#"
        SELECT
            m.id,
            COALESCE(m.platform, 'polymarket') as "platform!",
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            m.direction,
            m.strike_price::float8 as "strike_price: f64",
            m.liquidity_dollars,
            NULL::DECIMAL as yes_best_ask,
            NULL::DECIMAL as yes_best_bid,
            NULL::DECIMAL as no_best_ask,
            NULL::DECIMAL as no_best_bid,
            NULL::TIMESTAMPTZ as captured_at
        FROM markets m
        WHERE COALESCE(m.platform, 'polymarket') = $1
          AND m.is_active = true
          AND m.end_time > NOW()
        ORDER BY m.end_time ASC
        "#,
        platform
    )
    .fetch_all(pool)
    .await?;

    Ok(markets)
}

/// Get active markets for a platform with fresh orderbook prices.
pub async fn get_platform_markets_with_prices(
    pool: &PgPool,
    platform: &str,
    max_age_seconds: i32,
    assets: &[String],
    max_expiry_seconds: i64,
) -> Result<Vec<MarketWithPlatform>, sqlx::Error> {
    let snapshot_cutoff = Utc::now() - chrono::Duration::seconds(max_age_seconds as i64);
    let expiry_cutoff = Utc::now() + chrono::Duration::seconds(max_expiry_seconds);

    let results = sqlx::query_as!(
        MarketWithPlatform,
        r#"
        SELECT
            m.id,
            COALESCE(m.platform, 'polymarket') as "platform!",
            m.condition_id,
            m.market_type,
            m.asset,
            m.timeframe,
            m.yes_token_id,
            m.no_token_id,
            m.name,
            m.end_time,
            COALESCE(m.is_active, true) as "is_active!",
            m.direction,
            m.strike_price::float8 as "strike_price: f64",
            m.liquidity_dollars,
            o.yes_best_ask,
            o.yes_best_bid,
            o.no_best_ask,
            o.no_best_bid,
            o.captured_at
        FROM markets m
        INNER JOIN (
            SELECT DISTINCT ON (market_id)
                market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
            FROM orderbook_snapshots
            WHERE captured_at > $1
            ORDER BY market_id, captured_at DESC
        ) o ON o.market_id = m.id
        WHERE COALESCE(m.platform, 'polymarket') = $2
          AND m.is_active = true
          AND m.asset = ANY($3)
          AND m.end_time > NOW()
          AND m.end_time <= $4
        ORDER BY m.end_time ASC
        "#,
        snapshot_cutoff,
        platform,
        assets,
        expiry_cutoff,
    )
    .fetch_all(pool)
    .await?;

    Ok(results)
}

/// Update orderbook prices for a Kalshi market.
/// Kalshi doesn't have WebSocket, so we update via REST polling.
pub async fn update_kalshi_prices(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<Decimal>,
    yes_best_bid: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    no_best_bid: Option<Decimal>,
) -> Result<(), sqlx::Error> {
    let now = Utc::now();

    sqlx::query!(
        r#"
        INSERT INTO orderbook_snapshots (
            market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid,
            captured_at, yes_updated_at, no_updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $6, $6)
        ON CONFLICT (market_id) DO UPDATE SET
            yes_best_ask = $2,
            yes_best_bid = $3,
            no_best_ask = $4,
            no_best_bid = $5,
            captured_at = $6,
            yes_updated_at = $6,
            no_updated_at = $6
        "#,
        market_id,
        yes_best_ask,
        yes_best_bid,
        no_best_ask,
        no_best_bid,
        now,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Update Polymarket market prices in orderbook_snapshots.
/// This is an alias for update_kalshi_prices since they use the same schema.
pub async fn update_polymarket_prices(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<Decimal>,
    yes_best_bid: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    no_best_bid: Option<Decimal>,
) -> Result<(), sqlx::Error> {
    update_kalshi_prices(
        pool,
        market_id,
        yes_best_ask,
        yes_best_bid,
        no_best_ask,
        no_best_bid,
    )
    .await
}

// =============================================================================
// LIMITLESS EXCHANGE FUNCTIONS
// =============================================================================

/// Limitless market for database insertion.
#[derive(Debug, Clone)]
pub struct LimitlessMarketInsert {
    pub slug: String,
    pub name: String,
    pub asset: String,
    pub timeframe: String,
    pub end_time: DateTime<Utc>,
    pub yes_position_id: String,
    pub no_position_id: String,
    pub yes_best_bid: Option<Decimal>,
    pub yes_best_ask: Option<Decimal>,
    pub no_best_bid: Option<Decimal>,
    pub no_best_ask: Option<Decimal>,
    pub liquidity: Option<Decimal>,
    pub direction: Option<String>,
    pub exchange_address: Option<String>,
}

impl From<&ParsedLimitlessMarket> for LimitlessMarketInsert {
    fn from(m: &ParsedLimitlessMarket) -> Self {
        Self {
            slug: m.slug.clone(),
            name: m.name.clone(),
            asset: m.asset.clone(),
            timeframe: m.timeframe.clone(),
            end_time: m.close_time,
            yes_position_id: m.yes_position_id.clone(),
            no_position_id: m.no_position_id.clone(),
            yes_best_bid: m.yes_best_bid,
            yes_best_ask: m.yes_best_ask,
            no_best_bid: m.no_best_bid,
            no_best_ask: m.no_best_ask,
            liquidity: m.liquidity,
            direction: m.direction.clone(),
            exchange_address: m.exchange_address.clone(),
        }
    }
}

/// Upsert a Limitless market into the database.
/// Uses platform + condition_id (slug for Limitless) as unique key.
/// Note: Uses yes_token_id/no_token_id for position IDs (same columns).
pub async fn upsert_limitless_market(
    pool: &PgPool,
    market: &LimitlessMarketInsert,
) -> Result<Uuid, sqlx::Error> {
    // Determine market type from direction
    let market_type = match market.direction.as_deref() {
        Some("up") | Some("down") => "up_down",
        Some("above") | Some("below") => "above",
        _ => "unknown",
    };

    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO markets (
            platform, condition_id, market_type, asset, timeframe,
            yes_token_id, no_token_id, name, end_time,
            liquidity_dollars, direction,
            is_active
        )
        VALUES (
            'limitless', $1, $2, $3, $4,
            $5, $6, $7, $8,
            $9, $10,
            true
        )
        ON CONFLICT (platform, condition_id) DO UPDATE SET
            market_type = EXCLUDED.market_type,
            asset = EXCLUDED.asset,
            timeframe = EXCLUDED.timeframe,
            yes_token_id = EXCLUDED.yes_token_id,
            no_token_id = EXCLUDED.no_token_id,
            name = EXCLUDED.name,
            end_time = EXCLUDED.end_time,
            liquidity_dollars = EXCLUDED.liquidity_dollars,
            direction = EXCLUDED.direction,
            is_active = true,
            updated_at = NOW()
        RETURNING id
        "#,
        market.slug,            // $1: condition_id
        market_type,            // $2: market_type
        market.asset,           // $3: asset
        market.timeframe,       // $4: timeframe
        market.yes_position_id, // $5: yes_token_id (stores position ID)
        market.no_position_id,  // $6: no_token_id (stores position ID)
        market.name,            // $7: name
        market.end_time,        // $8: end_time
        market.liquidity,       // $9: liquidity_dollars
        market.direction,       // $10: direction
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Update orderbook prices for a Limitless market.
/// Similar to update_kalshi_prices but for Limitless.
pub async fn update_limitless_prices(
    pool: &PgPool,
    market_id: Uuid,
    yes_best_ask: Option<Decimal>,
    yes_best_bid: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    no_best_bid: Option<Decimal>,
) -> Result<(), sqlx::Error> {
    update_kalshi_prices(
        pool,
        market_id,
        yes_best_ask,
        yes_best_bid,
        no_best_ask,
        no_best_bid,
    )
    .await
}

/// Get Limitless markets with fresh prices for cross-platform matching.
pub async fn get_limitless_markets_with_prices(
    pool: &PgPool,
    max_age_seconds: i32,
    assets: &[String],
    max_expiry_seconds: i64,
) -> Result<Vec<MarketWithPlatform>, sqlx::Error> {
    get_platform_markets_with_prices(
        pool,
        "limitless",
        max_age_seconds,
        assets,
        max_expiry_seconds,
    )
    .await
}

/// Cross-platform match for caching.
#[derive(Debug, Clone)]
pub struct CrossPlatformMatchInsert {
    pub polymarket_id: Uuid,
    pub kalshi_id: Uuid,
    pub match_confidence: Decimal,
    pub match_reason: Option<String>,
    pub entity_asset: Option<String>,
    pub entity_timeframe: Option<String>,
    pub entity_direction: Option<String>,
}

/// Insert or update a cross-platform match.
pub async fn upsert_cross_platform_match(
    pool: &PgPool,
    m: &CrossPlatformMatchInsert,
) -> Result<Uuid, sqlx::Error> {
    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO cross_platform_matches (
            polymarket_id, kalshi_id, match_confidence, match_reason,
            entity_asset, entity_timeframe, entity_direction
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (polymarket_id, kalshi_id) DO UPDATE SET
            match_confidence = EXCLUDED.match_confidence,
            match_reason = EXCLUDED.match_reason,
            validated_at = NOW()
        RETURNING id
        "#,
        m.polymarket_id,
        m.kalshi_id,
        m.match_confidence,
        m.match_reason,
        m.entity_asset,
        m.entity_timeframe,
        m.entity_direction,
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Get cached cross-platform matches with minimum confidence.
pub async fn get_cross_platform_matches(
    pool: &PgPool,
    min_confidence: Decimal,
) -> Result<Vec<(Uuid, Uuid, Uuid, Decimal, Option<String>)>, sqlx::Error> {
    let results = sqlx::query!(
        r#"
        SELECT
            cpm.id,
            cpm.polymarket_id,
            cpm.kalshi_id,
            cpm.match_confidence,
            cpm.match_reason
        FROM cross_platform_matches cpm
        WHERE cpm.match_confidence >= $1
          AND cpm.invalidated_at IS NULL
        ORDER BY cpm.match_confidence DESC
        "#,
        min_confidence
    )
    .fetch_all(pool)
    .await?;

    Ok(results
        .into_iter()
        .map(|r| {
            (
                r.id,
                r.polymarket_id,
                r.kalshi_id,
                r.match_confidence,
                r.match_reason,
            )
        })
        .collect())
}

/// Record a detected cross-platform arbitrage opportunity.
#[allow(clippy::too_many_arguments)]
pub async fn record_cross_platform_opportunity(
    pool: &PgPool,
    match_id: Uuid,
    buy_yes_platform: &str,
    buy_no_platform: &str,
    yes_price: Decimal,
    no_price: Decimal,
    total_cost: Decimal,
    gross_profit_pct: Decimal,
    net_profit_pct: Decimal,
    min_liquidity: Option<Decimal>,
    expires_at: DateTime<Utc>,
) -> Result<Uuid, sqlx::Error> {
    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO cross_platform_opportunities (
            match_id, buy_yes_platform, buy_no_platform,
            yes_price, no_price, total_cost,
            gross_profit_pct, net_profit_pct,
            min_liquidity, expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
        "#,
        match_id,
        buy_yes_platform,
        buy_no_platform,
        yes_price,
        no_price,
        total_cost,
        gross_profit_pct,
        net_profit_pct,
        min_liquidity,
        expires_at,
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
}

/// Get recent cross-platform opportunities.
pub async fn get_recent_opportunities(
    pool: &PgPool,
    limit: i64,
) -> Result<
    Vec<(
        Uuid,
        Uuid,
        String,
        String,
        Decimal,
        Decimal,
        Decimal,
        DateTime<Utc>,
    )>,
    sqlx::Error,
> {
    let results = sqlx::query!(
        r#"
        SELECT
            cpo.id,
            cpo.match_id,
            cpo.buy_yes_platform,
            cpo.buy_no_platform,
            cpo.total_cost,
            cpo.gross_profit_pct,
            cpo.net_profit_pct,
            cpo.detected_at
        FROM cross_platform_opportunities cpo
        WHERE cpo.status = 'detected'
        ORDER BY cpo.detected_at DESC
        LIMIT $1
        "#,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(results
        .into_iter()
        .map(|r| {
            (
                r.id,
                r.match_id,
                r.buy_yes_platform,
                r.buy_no_platform,
                r.total_cost,
                r.gross_profit_pct,
                r.net_profit_pct,
                r.detected_at.unwrap_or_else(Utc::now),
            )
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Database};
    use chrono::Duration;

    #[tokio::test]
    async fn test_upsert_market() {
        dotenvy::dotenv().ok();
        let config = Config::from_env().expect("Config should load");
        let db = Database::connect(&config).await.expect("DB should connect");

        let test_market = ParsedMarket {
            condition_id: format!("test-condition-{}", Uuid::new_v4()),
            market_type: MarketType::UpDown,
            asset: "BTC".to_string(),
            timeframe: "1h".to_string(),
            yes_token_id: "test-yes-token".to_string(),
            no_token_id: "test-no-token".to_string(),
            name: "Test market: Will BTC go up?".to_string(),
            end_time: Utc::now() + Duration::hours(2),
            yes_best_bid: None,
            yes_best_ask: None,
            no_best_bid: None,
            no_best_ask: None,
        };

        // Insert new market
        let id = upsert_market(db.pool(), &test_market)
            .await
            .expect("Upsert should succeed");
        assert!(!id.is_nil());

        // Upsert same market (should update, return same id)
        let id2 = upsert_market(db.pool(), &test_market)
            .await
            .expect("Second upsert should succeed");
        assert_eq!(id, id2);

        // Clean up
        sqlx::query!(
            "DELETE FROM markets WHERE condition_id = $1",
            test_market.condition_id
        )
        .execute(db.pool())
        .await
        .expect("Cleanup should succeed");
    }

    #[tokio::test]
    async fn test_deactivate_expired_markets() {
        dotenvy::dotenv().ok();
        let config = Config::from_env().expect("Config should load");
        let db = Database::connect(&config).await.expect("DB should connect");

        // Insert an expired market
        let condition_id = format!("test-expired-{}", Uuid::new_v4());
        let expired_time = Utc::now() - Duration::hours(1);

        sqlx::query!(
            r#"
            INSERT INTO markets (condition_id, market_type, asset, timeframe, yes_token_id, no_token_id, name, end_time, is_active)
            VALUES ($1, 'up_down', 'BTC', '1h', 'yes', 'no', 'Expired test', $2, true)
            "#,
            condition_id,
            expired_time,
        )
        .execute(db.pool())
        .await
        .expect("Insert should succeed");

        // Deactivate expired
        let count = deactivate_expired_markets(db.pool())
            .await
            .expect("Deactivate should succeed");
        assert!(count >= 1, "Should deactivate at least 1 market");

        // Verify deactivated
        let market = sqlx::query!(
            "SELECT is_active FROM markets WHERE condition_id = $1",
            condition_id
        )
        .fetch_one(db.pool())
        .await
        .expect("Fetch should succeed");
        assert!(!market.is_active.unwrap_or(true));

        // Clean up
        sqlx::query!("DELETE FROM markets WHERE condition_id = $1", condition_id)
            .execute(db.pool())
            .await
            .expect("Cleanup should succeed");
    }
}
