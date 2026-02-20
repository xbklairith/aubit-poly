//! Expiry Scalper - Bets on strongly-skewed crypto markets near expiry.
//!
//! Strategy: When a market is expiring soon (3 min) and price is skewed:
//! - Price > 0.75 → Buy YES (betting it stays high)

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use common::{
    calculate_fill_price_with_slippage, get_15m_updown_markets_with_orderbooks,
    get_market_resolutions_batch, get_markets_with_fresh_orderbooks, upsert_market_resolution,
    Config, Database, GammaClient, MarketResolutionInsert, MarketWithOrderbook,
};

/// Simulated position for dry-run portfolio tracking
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields kept for debugging and future use
struct SimulatedPosition {
    market_id: Uuid,
    condition_id: String,
    market_name: String,
    market_type: String,
    asset: String,
    timeframe: String,
    yes_token_id: String,
    no_token_id: String,
    side: String, // "YES" or "NO" - the side we bet on
    shares: Decimal,
    entry_price: Decimal,          // Limit order price (e.g., 0.99)
    best_ask_price: Decimal,       // Best ask price at time of signal
    effective_fill_price: Decimal, // Weighted average fill price from orderbook depth
    cost: Decimal,                 // Actual cost = shares * effective_fill_price
    end_time: DateTime<Utc>,
}

/// Dry-run portfolio tracker
#[derive(Debug, Default)]
struct DryRunPortfolio {
    positions: Vec<SimulatedPosition>,
    total_invested: Decimal,
    total_pnl: Decimal,
    realized_wins: u32,
    realized_losses: u32,
    pending_count: u32,
}

impl DryRunPortfolio {
    fn new() -> Self {
        Self::default()
    }

    fn add_position(&mut self, position: SimulatedPosition) {
        self.total_invested += position.cost;
        self.pending_count += 1;
        self.positions.push(position);
    }

    /// Resolve expired positions and calculate P&L using actual market resolutions
    /// Fetches from Gamma API if not in database, then records to database
    async fn resolve_expired(&mut self, pool: &sqlx::PgPool, gamma: &GammaClient) -> bool {
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

        let mut resolution_map: std::collections::HashMap<Uuid, String> = db_resolutions
            .into_iter()
            .map(|r| (r.market_id, r.winning_side.to_uppercase()))
            .collect();

        let mut resolved_any = false;

        for pos in expired {
            // Check if we already have resolution from DB
            let winning_side = if let Some(ws) = resolution_map.get(&pos.market_id) {
                ws.clone()
            } else {
                // Fetch from Gamma API using token_id (condition_id doesn't work with /markets endpoint)
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
                        // Not resolved yet - put back in queue
                        debug!(
                            "[PORTFOLIO] Market {} not yet resolved, will check again",
                            pos.market_name
                        );
                        self.positions.push(pos);
                        continue;
                    }
                    Err(e) => {
                        warn!(
                            "[PORTFOLIO] Failed to fetch resolution for {}: {}",
                            pos.market_name, e
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

    fn print_summary(&self) {
        let total_trades = self.realized_wins + self.realized_losses;
        let win_rate = if total_trades > 0 {
            (self.realized_wins as f64 / total_trades as f64) * 100.0
        } else {
            0.0
        };

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

const CLOB_HOST: &str = "https://clob.polymarket.com";
const ORDER_TIMEOUT_SECS: u64 = 30;
const CANCEL_TIMEOUT_SECS: u64 = 10;
/// Maximum allowed shares per order (sanity check)
const MAX_SHARES: Decimal = dec!(99.99);

/// Pending order cancellation.
#[derive(Debug, Clone)]
struct PendingCancel {
    order_id: String,
    market_name: String,
    cancel_at: DateTime<Utc>,
}

/// Expiry Scalper - bets on skewed markets near expiry
#[derive(Parser, Debug)]
#[command(name = "expiry-scalper")]
#[command(about = "Bets on strongly-skewed crypto markets near expiry")]
struct Args {
    /// Poll interval in seconds (minimum 1)
    #[arg(long, default_value = "10")]
    interval_secs: u64,

    /// Expiry window in minutes (markets expiring within this time)
    #[arg(long, default_value = "3")]
    expiry_minutes: i64,

    /// Position size in USDC
    #[arg(long, default_value = "5")]
    position_size: f64,

    /// High price threshold (buy YES if price > this)
    #[arg(long, default_value = "0.75")]
    high_threshold: f64,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "30")]
    max_orderbook_age: i32,

    /// Assets to trade (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,

    /// Dry run mode (no actual trades)
    #[arg(long)]
    dry_run: bool,

    /// Limit price for orders (place at this price to ensure fill)
    #[arg(long, default_value = "0.99")]
    limit_price: f64,

    /// Contrarian mode: bet AGAINST the skewed side
    #[arg(long)]
    contrarian: bool,

    /// Only trade 15-minute up/down markets
    #[arg(long)]
    only_15m_updown: bool,

    /// Auto-cancel unfilled orders after this many seconds (0 = no cancel)
    #[arg(long, default_value = "0")]
    cancel_after_secs: u64,

    /// Slippage percentage for fill price estimation (default 20%)
    /// Used when orderbook depth is unavailable
    #[arg(long, default_value = "20")]
    slippage_pct: f64,

    /// Minimum orderbook depth required at best price (skip if less)
    #[arg(long, default_value = "0")]
    min_depth: f64,
}

/// Cached authentication state
struct CachedAuth {
    client: polymarket_client_sdk::clob::Client<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    >,
    signer: PrivateKeySigner,
    #[allow(dead_code)]
    authenticated_at: DateTime<Utc>,
}

/// Validate CLI arguments
fn validate_args(args: &Args) -> Result<()> {
    if args.interval_secs < 1 {
        bail!("interval_secs must be at least 1");
    }
    if args.expiry_minutes < 1 {
        bail!("expiry_minutes must be at least 1");
    }
    if args.position_size <= 0.0 {
        bail!("position_size must be positive");
    }
    if args.position_size > 10000.0 {
        bail!("position_size cannot exceed 10000");
    }
    if args.high_threshold > 1.0 || args.high_threshold < 0.0 {
        bail!("high_threshold must be between 0 and 1");
    }
    if args.limit_price > 0.99 || args.limit_price < 0.01 {
        bail!("limit_price must be between 0.01 and 0.99");
    }
    if args.slippage_pct < 0.0 || args.slippage_pct > 100.0 {
        bail!("slippage_pct must be between 0 and 100");
    }
    if args.min_depth < 0.0 {
        bail!("min_depth must be non-negative");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Validate arguments
    validate_args(&args)?;

    info!("=== Expiry Scalper ===");
    info!("Expiry window: {} minutes", args.expiry_minutes);
    info!("Position size: ${}", args.position_size);
    info!("Threshold: buy if price >= {}", args.high_threshold);
    info!(
        "Limit price: {} (order placed at this price)",
        args.limit_price
    );
    info!("Assets: {}", args.assets);
    info!("Poll interval: {}s", args.interval_secs);
    info!("Dry run: {}", args.dry_run);
    info!("Contrarian mode: {}", args.contrarian);
    info!("Only 15m up/down: {}", args.only_15m_updown);
    if args.cancel_after_secs > 0 {
        info!("Auto-cancel after: {}s", args.cancel_after_secs);
    }

    // Load config and connect to database
    dotenvy::dotenv().ok();
    let config = Config::from_env()?;
    let db = Database::connect(&config).await?;
    let gamma = GammaClient::new(&config);

    info!("Connected to database");

    // Convert threshold to Decimal
    let high_threshold =
        Decimal::try_from(args.high_threshold).context("Invalid high_threshold")?;
    let position_size = Decimal::try_from(args.position_size).context("Invalid position_size")?;
    let limit_price = Decimal::try_from(args.limit_price).context("Invalid limit_price")?;

    // Track markets we've already bet on
    let mut traded_markets: HashSet<Uuid> = HashSet::new();

    // Cached authentication
    let mut cached_auth: Option<CachedAuth> = None;

    // Pending order cancellations
    let mut pending_cancels: Vec<PendingCancel> = Vec::new();

    // Dry-run portfolio tracker
    let mut portfolio = DryRunPortfolio::new();
    let mut cycle_count: u32 = 0;

    // Parse assets from CLI
    let assets: Vec<String> = args
        .assets
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    if assets.is_empty() {
        bail!("No valid assets specified");
    }

    info!("Trading assets: {:?}", assets);

    // Main loop with graceful shutdown
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal, exiting...");
                break;
            }
            _ = run_cycle(
                &db,
                &gamma,
                &assets,
                &args,
                high_threshold,
                position_size,
                limit_price,
                &mut traded_markets,
                &mut cached_auth,
                &mut pending_cancels,
                &mut portfolio,
            ) => {}
        }

        cycle_count += 1;

        // Print portfolio summary every 12 cycles (~1 min at 5s interval)
        if args.dry_run && cycle_count % 12 == 0 {
            portfolio.print_summary();
        }

        // Sleep until next cycle
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }

    // Final portfolio summary on shutdown
    if args.dry_run {
        info!("=== FINAL PORTFOLIO STATUS ===");
        portfolio.print_summary();
    }

    info!("Shutdown complete");
    Ok(())
}

/// Process a single market with orderbook depth for realistic fill price estimation.
/// Returns true if trade was placed/simulated, false otherwise.
#[allow(clippy::too_many_arguments)]
async fn process_market_with_orderbook(
    market: &MarketWithOrderbook,
    args: &Args,
    high_threshold: Decimal,
    position_size: Decimal,
    limit_price: Decimal,
    slippage_pct: Decimal,
    min_depth: Decimal,
    traded_markets: &mut HashSet<Uuid>,
    cached_auth: &mut Option<CachedAuth>,
    pending_cancels: &mut Vec<PendingCancel>,
    portfolio: &mut DryRunPortfolio,
) -> bool {
    // Skip if already traded
    if traded_markets.contains(&market.id) {
        debug!("Skipping {} - already traded", market.name);
        return false;
    }

    // Get YES price (ask price to buy YES)
    let yes_price = match market.yes_best_ask {
        Some(p) if p > dec!(0) && p <= dec!(1) => p,
        _ => {
            debug!(
                "Skipping {} - invalid YES ask price: {:?}",
                market.name, market.yes_best_ask
            );
            return false;
        }
    };

    // Get NO price (ask price to buy NO)
    let no_price = match market.no_best_ask {
        Some(p) if p > dec!(0) && p <= dec!(1) => p,
        _ => {
            debug!(
                "Skipping {} - invalid NO ask price: {:?}",
                market.name, market.no_best_ask
            );
            return false;
        }
    };

    // Determine which side to trade and get orderbook depth
    // Returns: (side, token_id, order_price, best_ask, orderbook_depth)
    let (side, token_id, order_price, best_ask, orderbook) = if args.contrarian {
        // Contrarian mode: bet AGAINST the skewed side at low price
        if yes_price >= high_threshold {
            // YES is skewed high (0.80), NO is cheap (~0.20), bet on NO
            (
                "NO",
                &market.no_token_id,
                limit_price,
                no_price,
                &market.no_asks,
            )
        } else if no_price >= high_threshold {
            // NO is skewed high (0.80), YES is cheap (~0.20), bet on YES
            (
                "YES",
                &market.yes_token_id,
                limit_price,
                yes_price,
                &market.yes_asks,
            )
        } else {
            debug!(
                "Skipping {} - YES {} and NO {} both below threshold {} (contrarian)",
                market.name, yes_price, no_price, high_threshold
            );
            return false;
        }
    } else {
        // Normal mode: bet WITH the skewed side
        if yes_price >= high_threshold {
            (
                "YES",
                &market.yes_token_id,
                yes_price,
                yes_price,
                &market.yes_asks,
            )
        } else if no_price >= high_threshold {
            (
                "NO",
                &market.no_token_id,
                no_price,
                no_price,
                &market.no_asks,
            )
        } else {
            debug!(
                "Skipping {} - YES {} and NO {} both below threshold {}",
                market.name, yes_price, no_price, high_threshold
            );
            return false;
        }
    };

    // Calculate effective fill price using orderbook depth
    let fill_estimate = calculate_fill_price_with_slippage(
        orderbook.as_ref(),
        best_ask,
        position_size / best_ask, // Estimate shares for fill calculation
        slippage_pct,
    );

    // Check minimum depth if configured (only when we have actual orderbook data)
    // When best_price_depth is 0, we're using slippage fallback and don't have depth info
    if min_depth > dec!(0)
        && fill_estimate.best_price_depth > dec!(0)
        && fill_estimate.best_price_depth < min_depth
    {
        warn!(
            "Skipping {} - depth {:.2} below minimum {:.2}",
            market.name, fill_estimate.best_price_depth, min_depth
        );
        return false;
    }

    // Calculate shares based on effective fill price
    let shares = (position_size / fill_estimate.effective_price).round_dp(2);

    if shares > MAX_SHARES {
        warn!(
            "Skipping {} - calculated shares {} exceeds max {}",
            market.name, shares, MAX_SHARES
        );
        return false;
    }

    // Sanity check on order price (only for non-contrarian)
    if !args.contrarian {
        if order_price < dec!(0.01) {
            warn!(
                "Skipping {} - market price {} too low",
                market.name, order_price
            );
            return false;
        }
        if order_price > dec!(0.99) {
            warn!(
                "Skipping {} - market price {} too high",
                market.name, order_price
            );
            return false;
        }
    }

    let mode_label = if args.contrarian {
        "CONTRARIAN"
    } else {
        "SIGNAL"
    };

    // Show effective fill price vs best ask in log
    let slippage_info = if fill_estimate.effective_price != best_ask {
        format!(
            " (slippage: {:.1}%)",
            ((fill_estimate.effective_price - best_ask) / best_ask * dec!(100))
        )
    } else {
        String::new()
    };

    info!(
        "[{}] {} {} @ ${:.4}{} ({:.2} shares, ${:.2}) - YES={}, NO={} - depth={:.2}",
        mode_label,
        side,
        market.name,
        fill_estimate.effective_price,
        slippage_info,
        shares,
        position_size,
        yes_price,
        no_price,
        fill_estimate.best_price_depth
    );

    if args.dry_run {
        let cost = shares * fill_estimate.effective_price;
        info!(
            "[DRY RUN] {} {:.2} shares @ ${:.4} (best_ask: ${}, limit: ${}) -> Win: ${:.2}",
            side,
            shares,
            fill_estimate.effective_price,
            best_ask,
            order_price,
            shares - cost
        );

        portfolio.add_position(SimulatedPosition {
            market_id: market.id,
            condition_id: market.condition_id.clone(),
            market_name: market.name.clone(),
            market_type: market.market_type.clone(),
            asset: market.asset.clone(),
            timeframe: market.timeframe.clone(),
            yes_token_id: market.yes_token_id.clone(),
            no_token_id: market.no_token_id.clone(),
            side: side.to_string(),
            shares,
            entry_price: order_price,
            best_ask_price: best_ask,
            effective_fill_price: fill_estimate.effective_price,
            cost,
            end_time: market.end_time,
        });

        traded_markets.insert(market.id);
        return true;
    }

    // Execute trade
    match execute_trade(
        cached_auth,
        token_id,
        shares,
        order_price,
        side,
        &market.name,
    )
    .await
    {
        Ok(order_id) => {
            info!(
                "[SUCCESS] Placed {} order {} for {} @ ${}",
                side, order_id, market.name, order_price
            );
            traded_markets.insert(market.id);

            if args.cancel_after_secs > 0 {
                let cancel_at =
                    Utc::now() + chrono::Duration::seconds(args.cancel_after_secs as i64);
                pending_cancels.push(PendingCancel {
                    order_id,
                    market_name: market.name.clone(),
                    cancel_at,
                });
                debug!(
                    "[CANCEL] Scheduled cancellation for {} at {:?}",
                    market.name, cancel_at
                );
            }
            true
        }
        Err(e) => {
            error!("[FAILED] Trade execution for {}: {:#}", market.name, e);
            false
        }
    }
}

/// Run a single trading cycle
async fn run_cycle(
    db: &Database,
    gamma: &GammaClient,
    assets: &[String],
    args: &Args,
    high_threshold: Decimal,
    position_size: Decimal,
    limit_price: Decimal,
    traded_markets: &mut HashSet<Uuid>,
    cached_auth: &mut Option<CachedAuth>,
    pending_cancels: &mut Vec<PendingCancel>,
    portfolio: &mut DryRunPortfolio,
) {
    let cycle_start = std::time::Instant::now();

    // Resolve expired positions in dry-run mode
    if args.dry_run {
        portfolio.resolve_expired(db.pool(), gamma).await;
    }

    // Process pending cancellations first
    if !pending_cancels.is_empty() {
        process_pending_cancels(cached_auth, pending_cancels).await;
    }

    // Query markets expiring within the window
    let expiry_seconds = args.expiry_minutes * 60;
    let slippage_pct = Decimal::try_from(args.slippage_pct).unwrap_or(dec!(20));
    let min_depth = Decimal::try_from(args.min_depth).unwrap_or(dec!(0));

    // For 15m up/down markets, use orderbook depth for realistic fill prices
    if args.only_15m_updown {
        let all_timeframes = vec!["5m".to_string(), "15m".to_string()];
        let markets = match get_15m_updown_markets_with_orderbooks(
            db.pool(),
            args.max_orderbook_age,
            assets,
            expiry_seconds,
            &all_timeframes,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to query 15m up/down markets: {:#}", e);
                return;
            }
        };

        info!(
            "Found {} markets expiring within {} minutes (with orderbook depth)",
            markets.len(),
            args.expiry_minutes
        );

        for market in &markets {
            process_market_with_orderbook(
                market,
                args,
                high_threshold,
                position_size,
                limit_price,
                slippage_pct,
                min_depth,
                traded_markets,
                cached_auth,
                pending_cancels,
                portfolio,
            )
            .await;
        }

        // Cleanup expired markets from tracking set
        let now = Utc::now();
        let before_count = traded_markets.len();
        traded_markets.retain(|id| markets.iter().any(|m| m.id == *id && m.end_time > now));
        let cleaned = before_count - traded_markets.len();
        if cleaned > 0 {
            debug!("Cleaned {} expired markets from tracking set", cleaned);
        }
    } else {
        // Use general query for all markets (without orderbook depth)
        let markets = match get_markets_with_fresh_orderbooks(
            db.pool(),
            args.max_orderbook_age,
            assets,
            expiry_seconds,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to query markets: {:#}", e);
                return;
            }
        };

        info!(
            "Found {} markets expiring within {} minutes",
            markets.len(),
            args.expiry_minutes
        );

        // Process each market (legacy mode without orderbook depth)
        for market in &markets {
            if traded_markets.contains(&market.id) {
                debug!("Skipping {} - already traded", market.name);
                continue;
            }

            let yes_price = match market.yes_best_ask {
                Some(p) if p > dec!(0) && p <= dec!(1) => p,
                _ => {
                    debug!(
                        "Skipping {} - invalid YES ask price: {:?}",
                        market.name, market.yes_best_ask
                    );
                    continue;
                }
            };

            let no_price = match market.no_best_ask {
                Some(p) if p > dec!(0) && p <= dec!(1) => p,
                _ => {
                    debug!(
                        "Skipping {} - invalid NO ask price: {:?}",
                        market.name, market.no_best_ask
                    );
                    continue;
                }
            };

            let (side, token_id, order_price, market_price) = if args.contrarian {
                if yes_price >= high_threshold {
                    ("NO", &market.no_token_id, limit_price, no_price)
                } else if no_price >= high_threshold {
                    ("YES", &market.yes_token_id, limit_price, yes_price)
                } else {
                    debug!("Skipping {} - below threshold (contrarian)", market.name);
                    continue;
                }
            } else {
                if yes_price >= high_threshold {
                    ("YES", &market.yes_token_id, yes_price, yes_price)
                } else if no_price >= high_threshold {
                    ("NO", &market.no_token_id, no_price, no_price)
                } else {
                    debug!("Skipping {} - below threshold", market.name);
                    continue;
                }
            };

            if !args.contrarian {
                if order_price < dec!(0.01) || order_price > dec!(0.99) {
                    warn!(
                        "Skipping {} - price {} out of range",
                        market.name, order_price
                    );
                    continue;
                }
            }

            let shares = (position_size / market_price).round_dp(2);
            if shares > MAX_SHARES {
                warn!("Skipping {} - shares {} exceeds max", market.name, shares);
                continue;
            }

            let mode_label = if args.contrarian {
                "CONTRARIAN"
            } else {
                "SIGNAL"
            };
            info!(
                "[{}] {} {} @ ${:.2} ({:.2} shares) - YES={}, NO={}",
                mode_label, side, market.name, market_price, shares, yes_price, no_price
            );

            if args.dry_run {
                let cost = shares * market_price;
                info!(
                    "[DRY RUN] {} {:.2} shares @ ${} -> Win: ${:.2} (no depth check)",
                    side,
                    shares,
                    market_price,
                    shares - cost
                );

                portfolio.add_position(SimulatedPosition {
                    market_id: market.id,
                    condition_id: market.condition_id.clone(),
                    market_name: market.name.clone(),
                    market_type: market.market_type.clone(),
                    asset: market.asset.clone(),
                    timeframe: market.timeframe.clone(),
                    yes_token_id: market.yes_token_id.clone(),
                    no_token_id: market.no_token_id.clone(),
                    side: side.to_string(),
                    shares,
                    entry_price: order_price,
                    best_ask_price: market_price,
                    effective_fill_price: market_price, // No depth, assume best ask
                    cost,
                    end_time: market.end_time,
                });

                traded_markets.insert(market.id);
                continue;
            }

            match execute_trade(
                cached_auth,
                token_id,
                shares,
                order_price,
                side,
                &market.name,
            )
            .await
            {
                Ok(order_id) => {
                    info!(
                        "[SUCCESS] Placed {} order {} for {} @ ${}",
                        side, order_id, market.name, order_price
                    );
                    traded_markets.insert(market.id);

                    if args.cancel_after_secs > 0 {
                        let cancel_at =
                            Utc::now() + chrono::Duration::seconds(args.cancel_after_secs as i64);
                        pending_cancels.push(PendingCancel {
                            order_id,
                            market_name: market.name.clone(),
                            cancel_at,
                        });
                    }
                }
                Err(e) => {
                    error!("[ERROR] Failed to place order for {}: {:#}", market.name, e);
                }
            }
        }

        // Cleanup expired markets from tracking set
        let now = Utc::now();
        let before_count = traded_markets.len();
        traded_markets.retain(|id| markets.iter().any(|m| m.id == *id && m.end_time > now));
        let cleaned = before_count - traded_markets.len();
        if cleaned > 0 {
            debug!("Cleaned {} expired markets from tracking set", cleaned);
        }
    }

    let elapsed = cycle_start.elapsed();
    debug!("Cycle completed in {:?}", elapsed);
}

/// Process pending order cancellations.
/// Cancels orders that have reached their cancel_at time.
async fn process_pending_cancels(
    cached_auth: &mut Option<CachedAuth>,
    pending_cancels: &mut Vec<PendingCancel>,
) {
    let now = Utc::now();

    // Partition into ready-to-cancel and not-yet-ready
    let (ready, not_ready): (Vec<_>, Vec<_>) = pending_cancels
        .drain(..)
        .partition(|pc| pc.cancel_at <= now);

    // Restore not-ready cancellations
    *pending_cancels = not_ready;

    if ready.is_empty() {
        return;
    }

    info!("[CANCEL] Processing {} pending cancellations", ready.len());

    // Ensure we're authenticated for cancellations
    let auth = match ensure_authenticated(cached_auth).await {
        Ok(a) => a,
        Err(e) => {
            error!("[CANCEL] Failed to authenticate for cancellations: {:#}", e);
            // Re-add ready cancellations to try again later
            pending_cancels.extend(ready);
            return;
        }
    };

    for pc in ready {
        match timeout(
            Duration::from_secs(CANCEL_TIMEOUT_SECS),
            auth.client.cancel_order(&pc.order_id),
        )
        .await
        {
            Ok(Ok(_)) => {
                info!(
                    "[CANCEL] Successfully cancelled order {} for {}",
                    pc.order_id, pc.market_name
                );
            }
            Ok(Err(e)) => {
                // Order may have already been filled or cancelled
                debug!(
                    "[CANCEL] Failed to cancel order {} for {}: {:#}",
                    pc.order_id, pc.market_name, e
                );
            }
            Err(_) => {
                warn!(
                    "[CANCEL] Timeout cancelling order {} for {}",
                    pc.order_id, pc.market_name
                );
            }
        }
    }
}

/// Execute a trade on Polymarket. Returns the order ID on success.
async fn execute_trade(
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
    // Polymarket SDK requires price decimal places <= tick size decimal places
    let price = price.normalize();
    let shares = shares.round_dp(2); // Round shares to 2 decimal places

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

    // Check result (post_order returns Vec<PostOrderResponse>)
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

/// Ensure we have a valid authenticated CLOB client.
/// Authenticates on first call, reuses cached client thereafter.
async fn ensure_authenticated(cached_auth: &mut Option<CachedAuth>) -> Result<&CachedAuth> {
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
