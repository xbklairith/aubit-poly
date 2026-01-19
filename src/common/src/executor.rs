//! Shared trading executor functionality.
//!
//! Provides reusable components for dry-run portfolio tracking,
//! Polymarket SDK authentication, and order execution.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::timeout;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{
    get_market_resolutions_batch, upsert_market_resolution, GammaClient, MarketResolutionInsert,
};

const CLOB_HOST: &str = "https://clob.polymarket.com";
const ORDER_TIMEOUT_SECS: u64 = 30;

/// Maximum allowed shares per order (sanity check)
pub const MAX_SHARES: Decimal = dec!(99.99);

/// Maximum resolution retry attempts before force-expiring a position
const MAX_RESOLUTION_RETRIES: u32 = 10;

/// Simulated position for dry-run portfolio tracking.
#[derive(Debug, Clone)]
pub struct SimulatedPosition {
    pub market_id: Uuid,
    pub condition_id: String,
    pub market_name: String,
    pub market_type: String,
    pub asset: String,
    pub timeframe: String,
    pub yes_token_id: String,
    pub no_token_id: String,
    /// "YES" or "NO" - the side we bet on
    pub side: String,
    pub shares: Decimal,
    /// Limit order price (e.g., 0.99)
    pub entry_price: Decimal,
    /// Best ask price at time of signal
    pub best_ask_price: Decimal,
    /// Weighted average fill price from orderbook depth
    pub effective_fill_price: Decimal,
    /// Actual cost = shares * effective_fill_price
    pub cost: Decimal,
    pub end_time: DateTime<Utc>,
    /// When the position was created (for staleness tracking)
    pub created_at: DateTime<Utc>,
    /// Number of resolution fetch attempts
    pub resolution_retries: u32,
}

/// Dry-run portfolio tracker.
#[derive(Debug, Default)]
pub struct DryRunPortfolio {
    pub positions: Vec<SimulatedPosition>,
    pub total_invested: Decimal,
    pub total_pnl: Decimal,
    pub realized_wins: u32,
    pub realized_losses: u32,
    pub pending_count: u32,
}

impl DryRunPortfolio {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_position(&mut self, position: SimulatedPosition) {
        self.total_invested += position.cost;
        self.pending_count += 1;
        self.positions.push(position);
    }

    /// Resolve expired positions and calculate P&L using actual market resolutions.
    /// Fetches from Gamma API if not in database, then records to database.
    pub async fn resolve_expired(&mut self, pool: &sqlx::PgPool, gamma: &GammaClient) -> bool {
        let now = Utc::now();

        // Find positions that have expired (with 60s buffer for resolution data)
        let expired_cutoff = now - chrono::Duration::seconds(60);

        let (expired, active): (Vec<_>, Vec<_>) = self
            .positions
            .drain(..)
            .partition(|p| p.end_time < expired_cutoff);

        self.positions = active;

        if expired.is_empty() {
            return false;
        }

        // First, try to get resolutions from database
        let market_ids: Vec<Uuid> = expired.iter().map(|p| p.market_id).collect();
        let db_resolutions = get_market_resolutions_batch(pool, &market_ids)
            .await
            .unwrap_or_default();

        let mut resolution_map: HashMap<Uuid, String> = db_resolutions
            .into_iter()
            .map(|r| (r.market_id, r.winning_side.to_uppercase()))
            .collect();

        let mut resolved_any = false;
        let mut api_calls_made = 0u32;

        for pos in expired {
            // Check if we already have resolution from DB
            let winning_side = if let Some(ws) = resolution_map.get(&pos.market_id) {
                ws.clone()
            } else {
                // Rate limit: add delay between API calls (max 2 per second)
                if api_calls_made > 0 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                api_calls_made += 1;

                // Fetch from Gamma API using token_id
                match gamma.fetch_market_resolution(&pos.yes_token_id).await {
                    Ok(Some(ws)) => {
                        let ws_upper = ws.to_uppercase();
                        info!(
                            "[PORTFOLIO] Fetched resolution from API: {} -> {}",
                            pos.market_name, ws_upper
                        );

                        // Record to database for future use
                        let insert = MarketResolutionInsert {
                            condition_id: pos.condition_id.clone(),
                            market_type: pos.market_type.clone(),
                            asset: pos.asset.clone(),
                            timeframe: pos.timeframe.clone(),
                            name: pos.market_name.clone(),
                            yes_token_id: pos.yes_token_id.clone(),
                            no_token_id: pos.no_token_id.clone(),
                            winning_side: ws_upper.clone(),
                            end_time: pos.end_time,
                        };
                        if let Err(e) = upsert_market_resolution(pool, &insert).await {
                            warn!("[PORTFOLIO] Failed to record resolution: {}", e);
                        }

                        resolution_map.insert(pos.market_id, ws_upper.clone());
                        ws_upper
                    }
                    Ok(None) => {
                        // Not resolved yet - put back in queue with incremented retry count
                        let mut pos = pos;
                        pos.resolution_retries += 1;
                        if pos.resolution_retries >= MAX_RESOLUTION_RETRIES {
                            warn!(
                                "[PORTFOLIO] ⚠️ EXPIRED: {} - max retries ({}) exceeded, treating as loss",
                                pos.market_name, MAX_RESOLUTION_RETRIES
                            );
                            self.pending_count = self.pending_count.saturating_sub(1);
                            self.total_pnl -= pos.cost;
                            self.realized_losses += 1;
                            continue;
                        }
                        debug!(
                            "[PORTFOLIO] Market {} not yet resolved (retry {}/{}), will check again",
                            pos.market_name, pos.resolution_retries, MAX_RESOLUTION_RETRIES
                        );
                        self.positions.push(pos);
                        continue;
                    }
                    Err(e) => {
                        // API error - put back with incremented retry count
                        let mut pos = pos;
                        pos.resolution_retries += 1;
                        if pos.resolution_retries >= MAX_RESOLUTION_RETRIES {
                            warn!(
                                "[PORTFOLIO] ⚠️ EXPIRED: {} - max retries ({}) exceeded after API errors, treating as loss",
                                pos.market_name, MAX_RESOLUTION_RETRIES
                            );
                            self.pending_count = self.pending_count.saturating_sub(1);
                            self.total_pnl -= pos.cost;
                            self.realized_losses += 1;
                            continue;
                        }
                        warn!(
                            "[PORTFOLIO] Failed to fetch resolution for {} (retry {}/{}): {}",
                            pos.market_name, pos.resolution_retries, MAX_RESOLUTION_RETRIES, e
                        );
                        self.positions.push(pos);
                        continue;
                    }
                }
            };

            // Validate winning_side is YES or NO
            if winning_side != "YES" && winning_side != "NO" {
                warn!(
                    "[PORTFOLIO] Invalid winning_side '{}' for {}, skipping",
                    winning_side, pos.market_name
                );
                self.positions.push(pos);
                continue;
            }

            resolved_any = true;
            self.pending_count = self.pending_count.saturating_sub(1);

            // Check if our bet won
            let we_won = pos.side.to_uppercase() == winning_side;

            if we_won {
                // Win: get $1 per share, profit = shares - cost
                let payout = pos.shares;
                let profit = payout - pos.cost;
                self.total_pnl += profit;
                self.realized_wins += 1;
                info!(
                    "[PORTFOLIO] ✅ WIN: {} {} (mkt: ${:.2}) -> +${:.2} (resolved: {})",
                    pos.side, pos.market_name, pos.best_ask_price, profit, winning_side
                );
            } else {
                // Loss: lose entire stake
                let loss = pos.cost;
                self.total_pnl -= loss;
                self.realized_losses += 1;
                info!(
                    "[PORTFOLIO] ❌ LOSS: {} {} (mkt: ${:.2}) -> -${:.2} (resolved: {})",
                    pos.side, pos.market_name, pos.best_ask_price, loss, winning_side
                );
            }
        }

        resolved_any
    }

    pub fn win_rate(&self) -> f64 {
        let total_trades = self.realized_wins + self.realized_losses;
        if total_trades > 0 {
            (self.realized_wins as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Cleanup stale positions that are too old (over 1 hour past expiry).
    /// These are positions that failed resolution and should be force-expired.
    pub fn cleanup_stale_positions(&mut self) {
        let now = Utc::now();
        let stale_cutoff = now - chrono::Duration::hours(1);

        let before_count = self.positions.len();
        let (stale, active): (Vec<_>, Vec<_>) = self
            .positions
            .drain(..)
            .partition(|p| p.end_time < stale_cutoff);

        self.positions = active;

        for pos in stale {
            warn!(
                "[PORTFOLIO] ⚠️ STALE: {} - over 1 hour past expiry, treating as loss (${:.2})",
                pos.market_name, pos.cost
            );
            self.pending_count = self.pending_count.saturating_sub(1);
            self.total_pnl -= pos.cost;
            self.realized_losses += 1;
        }

        let cleaned = before_count - self.positions.len();
        if cleaned > 0 {
            info!("[PORTFOLIO] Cleaned up {} stale positions", cleaned);
        }
    }

    pub fn print_summary(&self) {
        let win_rate = self.win_rate();

        info!("╔════════════════════════════════════════════════════════════╗");
        info!("║              DRY-RUN PORTFOLIO SUMMARY                     ║");
        info!("╠════════════════════════════════════════════════════════════╣");
        info!(
            "║  Total Invested:    ${:<10.2}                           ║",
            self.total_invested
        );
        info!(
            "║  Realized P&L:      ${:<10.2}                           ║",
            self.total_pnl
        );
        info!(
            "║  Pending Positions: {:<10}                             ║",
            self.pending_count
        );
        info!(
            "║  Wins / Losses:     {} / {}                                  ║",
            self.realized_wins, self.realized_losses
        );
        info!(
            "║  Win Rate:          {:<6.1}%                               ║",
            win_rate
        );
        info!("╚════════════════════════════════════════════════════════════╝");
    }
}

/// Cached authentication state for Polymarket CLOB.
pub struct CachedAuth {
    pub client: polymarket_client_sdk::clob::Client<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    >,
    pub signer: PrivateKeySigner,
    pub authenticated_at: DateTime<Utc>,
}

/// Ensure we have a valid authenticated CLOB client.
/// Authenticates on first call, reuses cached client thereafter.
pub async fn ensure_authenticated(cached_auth: &mut Option<CachedAuth>) -> Result<&CachedAuth> {
    if cached_auth.is_some() {
        debug!("[AUTH] Using cached authentication");
        return Ok(cached_auth.as_ref().unwrap());
    }

    info!("[AUTH] Authenticating with Polymarket CLOB...");

    // Get private key
    let private_key = std::env::var("WALLET_PRIVATE_KEY")
        .context("Missing WALLET_PRIVATE_KEY environment variable")?;

    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };

    // Create signer
    let signer = PrivateKeySigner::from_str(&private_key)
        .context("Invalid private key format")?
        .with_chain_id(Some(POLYGON));

    // Determine signature type
    let proxy_wallet = std::env::var("POLYMARKET_WALLET_ADDRESS").ok();
    let signature_type = if proxy_wallet.is_some() {
        SignatureType::GnosisSafe
    } else {
        SignatureType::Eoa
    };

    // Build authentication
    let mut auth_builder = ClobClient::new(CLOB_HOST, ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(signature_type);

    if let Some(ref proxy) = proxy_wallet {
        let funder_address: alloy::primitives::Address =
            proxy.parse().context("Invalid proxy wallet address")?;
        auth_builder = auth_builder.funder(funder_address);
    }

    // Authenticate
    let client = auth_builder
        .authenticate()
        .await
        .context("Failed to authenticate with Polymarket")?;

    info!("[AUTH] Authentication successful");

    *cached_auth = Some(CachedAuth {
        client,
        signer,
        authenticated_at: Utc::now(),
    });

    // Safe because we just set it
    Ok(cached_auth.as_ref().unwrap())
}

/// Execute a trade on Polymarket. Returns the order ID on success.
pub async fn execute_trade(
    cached_auth: &mut Option<CachedAuth>,
    token_id: &str,
    shares: Decimal,
    price: Decimal,
    side: &str,
    market_name: &str,
) -> Result<String> {
    // Ensure authenticated
    let auth = ensure_authenticated(cached_auth).await?;

    // Normalize price and shares to remove trailing zeros
    let price = price.normalize();
    let shares = shares.round_dp(2);

    // Build order
    info!(
        "[TRADE] Building {} order: {} shares @ ${}",
        side, shares, price
    );

    let order = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client
            .limit_order()
            .token_id(token_id)
            .size(shares)
            .price(price)
            .side(polymarket_client_sdk::clob::types::Side::Buy)
            .build(),
    )
    .await
    .context("Order building timed out")?
    .context("Failed to build order")?;

    // Sign order
    let signed = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client.sign(&auth.signer, order),
    )
    .await
    .context("Order signing timed out")?
    .context("Failed to sign order")?;

    // Post order
    let result = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client.post_order(signed),
    )
    .await
    .context("Order posting timed out")?
    .context("Failed to post order")?;

    // Check result
    if let Some(order) = result.first() {
        let has_error = order
            .error_msg
            .as_ref()
            .map(|e| !e.is_empty())
            .unwrap_or(false);
        if !order.order_id.is_empty() && !has_error {
            info!(
                "[TRADE] Order placed successfully: {} (order_id: {})",
                market_name, order.order_id
            );
            Ok(order.order_id.clone())
        } else if let Some(ref error) = order.error_msg {
            Err(anyhow::anyhow!("Order rejected: {}", error))
        } else {
            Err(anyhow::anyhow!("Order failed with unknown error"))
        }
    } else {
        Err(anyhow::anyhow!("No order response received"))
    }
}

/// Cancel an order on Polymarket.
pub async fn cancel_order(cached_auth: &mut Option<CachedAuth>, order_id: &str) -> Result<()> {
    let auth = ensure_authenticated(cached_auth).await?;

    timeout(
        Duration::from_secs(10),
        auth.client.cancel_order(order_id),
    )
    .await
    .context("Order cancellation timed out")?
    .context("Failed to cancel order")?;

    Ok(())
}
