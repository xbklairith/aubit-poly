//! Database repository functions for markets and orderbooks.

use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use crate::gamma::{MarketType, ParsedMarket};
use crate::models::Market;

/// Upsert a market into the database.
/// Updates existing market if condition_id matches, otherwise inserts new.
pub async fn upsert_market(pool: &PgPool, market: &ParsedMarket) -> Result<Uuid, sqlx::Error> {
    let market_type_str = match market.market_type {
        MarketType::UpDown => "up_down",
        MarketType::Above => "above",
        MarketType::PriceRange => "price_range",
        MarketType::Unknown => "unknown",
    };

    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO markets (condition_id, market_type, asset, timeframe, yes_token_id, no_token_id, name, end_time, is_active)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
        ON CONFLICT (condition_id) DO UPDATE SET
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

/// Insert an orderbook snapshot into the database.
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
) -> Result<i64, sqlx::Error> {
    let result = sqlx::query_scalar!(
        r#"
        INSERT INTO orderbook_snapshots (market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, yes_asks, yes_bids, no_asks, no_bids)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
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
    )
    .fetch_one(pool)
    .await?;

    Ok(result)
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
            COALESCE(captured_at, NOW()) as "captured_at!"
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
        };

        // Insert new market
        let id = upsert_market(db.pool(), &test_market).await.expect("Upsert should succeed");
        assert!(!id.is_nil());

        // Upsert same market (should update, return same id)
        let id2 = upsert_market(db.pool(), &test_market).await.expect("Second upsert should succeed");
        assert_eq!(id, id2);

        // Clean up
        sqlx::query!("DELETE FROM markets WHERE condition_id = $1", test_market.condition_id)
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
        let count = deactivate_expired_markets(db.pool()).await.expect("Deactivate should succeed");
        assert!(count >= 1, "Should deactivate at least 1 market");

        // Verify deactivated
        let market = sqlx::query!("SELECT is_active FROM markets WHERE condition_id = $1", condition_id)
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
