//! Misprice Trader (Chainlink) - Trades on price direction flips using Chainlink oracle prices.
//!
//! Strategy:
//! 1. On market discovery: Capture the current Chainlink price as the "open price"
//! 2. Track direction: current_price > open_price -> UP, otherwise -> DOWN
//! 3. On direction flip:
//!    - DOWN -> UP -> Place LIMIT order at $0.40 to BUY YES
//!    - UP -> DOWN -> Place LIMIT order at $0.40 to BUY NO
//! 4. Auto-cancel order after 10 seconds if not filled
//! 5. Only trade once per market (first qualifying flip)
//!
//! Key difference from Binance version:
//! - Uses Polymarket RTDS WebSocket for Chainlink prices (~1/sec vs Binance's 150+/sec)
//! - Chainlink prices may differ from Binance by $100-120
//! - No klines available - captures first price at market discovery as "open"

use std::collections::HashSet;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use common::{
    asset_to_chainlink_symbol, calculate_fill_price_with_slippage, execute_trade,
    get_15m_updown_markets_with_orderbooks, CachedAuth, ChainlinkPriceBuffer, Config, Database,
    DryRunPortfolio, GammaClient, PolymarketRtdsClient, SimulatedPosition,
};

mod detector;
mod exit_manager;
mod metrics;
mod order_manager;

use detector::MispriceDetector;
use exit_manager::ExitManager;
use metrics::Metrics;
use order_manager::OrderManager;

/// Misprice Trader (Chainlink) - trades on price direction flips using Chainlink prices
#[derive(Parser, Debug)]
#[command(name = "misprice-trader-chainlink")]
#[command(about = "Trades Polymarket when price flips direction using Chainlink oracle prices")]
struct Args {
    /// Dry run mode (no actual trades)
    #[arg(long)]
    dry_run: bool,

    /// Limit order price (place orders at this price)
    #[arg(long, default_value = "0.40")]
    limit_price: f64,

    /// Position size in USDC
    #[arg(long, default_value = "5")]
    position_size: f64,

    /// Maximum time to market expiry in minutes
    #[arg(long, default_value = "10")]
    max_expiry_minutes: i64,

    /// Minimum time to market expiry in minutes
    #[arg(long, default_value = "1")]
    min_expiry_minutes: i64,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "1")]
    max_orderbook_age: i32,

    /// Assets to trade (comma-separated)
    #[arg(long, default_value = "BTC")]
    assets: String,

    /// Auto-cancel timeout for limit orders (seconds)
    #[arg(long, default_value = "10")]
    cancel_timeout: u64,

    /// Trailing stop percentage (exit when price drops this much from peak). 0 to disable.
    #[arg(long, default_value = "0")]
    trailing_stop_pct: f64,

    /// Take profit percentage (exit immediately when profit exceeds this). Optional.
    #[arg(long)]
    take_profit_pct: Option<f64>,
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

    // Validate percentage arguments
    if !(0.0..=1.0).contains(&args.trailing_stop_pct) {
        anyhow::bail!(
            "--trailing-stop-pct must be between 0.0 and 1.0 (got {})",
            args.trailing_stop_pct
        );
    }
    if let Some(tp) = args.take_profit_pct {
        if !(0.0..=10.0).contains(&tp) {
            anyhow::bail!(
                "--take-profit-pct must be between 0.0 and 10.0 (got {})",
                tp
            );
        }
    }

    info!("=== Misprice Trader (CHAINLINK) ===");
    info!("Price source: Polymarket RTDS (Chainlink oracle)");
    info!("Limit price: ${}", args.limit_price);
    info!("Position size: ${}", args.position_size);
    info!(
        "Expiry window: {}-{} minutes",
        args.min_expiry_minutes, args.max_expiry_minutes
    );
    info!("Max orderbook age: {}s", args.max_orderbook_age);
    info!("Assets: {}", args.assets);
    info!("Cancel timeout: {}s", args.cancel_timeout);
    if args.trailing_stop_pct > 0.0 {
        info!("Trailing stop: {:.1}%", args.trailing_stop_pct * 100.0);
        if let Some(tp) = args.take_profit_pct {
            info!("Take profit: {:.1}%", tp * 100.0);
        }
    }
    info!("Dry run: {}", args.dry_run);

    // Load config and connect to database
    dotenvy::dotenv().ok();
    let config = Config::from_env()?;
    let db = Database::connect(&config).await?;
    let gamma = GammaClient::new(&config);

    info!("Connected to database");

    // Parse assets
    let assets: Vec<String> = args
        .assets
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    if assets.is_empty() {
        anyhow::bail!("No valid assets specified");
    }

    // Build Chainlink symbols list
    let chainlink_symbols: Vec<String> = assets
        .iter()
        .filter_map(|a| asset_to_chainlink_symbol(a).map(|s| s.to_string()))
        .collect();

    if chainlink_symbols.is_empty() {
        anyhow::bail!("No supported assets specified");
    }

    info!("Chainlink symbols: {:?}", chainlink_symbols);

    // Convert parameters to Decimal
    let limit_price = Decimal::try_from(args.limit_price).context("Invalid limit_price")?;
    let position_size = Decimal::try_from(args.position_size).context("Invalid position_size")?;

    // Initialize components
    // Chainlink buffer needs to track open prices per market start time
    let mut price_buffer = ChainlinkPriceBuffer::new(120); // ~2 min history at 1/sec
    let mut detector = MispriceDetector::new();
    let mut metrics = Metrics::new();
    let mut portfolio = DryRunPortfolio::new();
    let mut cached_auth: Option<CachedAuth> = None;
    // Track (market_id, side) - only trade once per market per side
    let mut traded_positions: HashSet<(Uuid, String)> = HashSet::new();
    // Order manager for tracking pending orders and auto-cancel (live trading only)
    let mut order_manager = OrderManager::new(args.cancel_timeout);
    // Exit manager for trailing stop and take profit exits
    let trailing_stop_pct = Decimal::try_from(args.trailing_stop_pct).unwrap_or(dec!(0));
    let take_profit_pct = args
        .take_profit_pct
        .map(|tp| Decimal::try_from(tp).unwrap_or(dec!(0)));
    let mut exit_manager = ExitManager::new(trailing_stop_pct, take_profit_pct, args.dry_run);

    // Connect to Polymarket RTDS WebSocket (Chainlink prices)
    let rtds_client = PolymarketRtdsClient::new(chainlink_symbols.clone());

    info!("Connecting to Polymarket RTDS (Chainlink prices)...");
    let mut rtds_stream = rtds_client.connect_with_retry(5).await?;
    info!("Connected to RTDS WebSocket");

    // Wait for initial prices before starting main loop (fixes startup race condition)
    info!("Waiting for initial Chainlink prices...");
    let bootstrap_timeout = Duration::from_secs(30);
    let bootstrap_start = std::time::Instant::now();
    while !price_buffer.has_prices_for_all(&chainlink_symbols) {
        if bootstrap_start.elapsed() > bootstrap_timeout {
            warn!(
                "Bootstrap timeout (30s) - only have prices for {}/{} symbols, continuing anyway",
                price_buffer.symbol_count(),
                chainlink_symbols.len()
            );
            break;
        }
        match rtds_stream.next_price().await {
            Some(price) => {
                debug!("Bootstrap: received {} = ${}", price.symbol, price.value);
                price_buffer.update(&price);
            }
            None => {
                warn!("RTDS disconnected during bootstrap, reconnecting...");
                rtds_stream = rtds_client.connect_with_retry(5).await?;
            }
        }
    }
    info!(
        "Bootstrapped with prices for {}/{} symbols",
        price_buffer.symbol_count(),
        chainlink_symbols.len()
    );

    // Main loop with graceful shutdown
    let mut last_cycle_time = std::time::Instant::now();
    let mut last_cleanup_time = std::time::Instant::now();
    let mut prices_since_heartbeat: u64 = 0;

    // Heartbeat timer (every 60 seconds)
    let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(60));
    heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal, exiting...");
                break;
            }
            _ = heartbeat_interval.tick() => {
                // Heartbeat: print metrics and portfolio summary
                info!("[ALIVE] Heartbeat - prices received: {}, markets tracked: {}, active positions: {}",
                    prices_since_heartbeat, detector.tracked_count(), exit_manager.position_count());

                // Stream health check: Chainlink updates ~1/sec per symbol
                // With N symbols, expect ~60*N prices per minute (±20% variance)
                let expected_min = (chainlink_symbols.len() as u64) * 45; // 75% of expected
                let expected_max = (chainlink_symbols.len() as u64) * 80; // 133% of expected
                if prices_since_heartbeat < expected_min {
                    warn!(
                        "[HEALTH] Low price rate: {} prices in 60s (expected {}-{}). Stream may be degraded.",
                        prices_since_heartbeat, expected_min, expected_max
                    );
                } else if prices_since_heartbeat > expected_max {
                    warn!(
                        "[HEALTH] High price rate: {} prices in 60s (expected {}-{}). Possible duplicate messages.",
                        prices_since_heartbeat, expected_min, expected_max
                    );
                }

                prices_since_heartbeat = 0;

                // Print tracked market status
                detector.print_market_status(&price_buffer);

                metrics.print_summary();
                if exit_manager.is_enabled() {
                    exit_manager.print_summary();
                }
                if args.dry_run {
                    portfolio.print_summary();
                    // Resolve expired positions during heartbeat
                    portfolio.resolve_expired(db.pool(), &gamma).await;
                }
            }
            price_opt = rtds_stream.next_price() => {
                match price_opt {
                    Some(price) => {
                        // Update price buffer with new Chainlink price
                        price_buffer.update(&price);
                        prices_since_heartbeat += 1;

                        // Run trading cycle every 500ms (Chainlink updates ~1/sec)
                        // This is slower than Binance version (100ms) due to slower update rate
                        if last_cycle_time.elapsed() >= Duration::from_millis(500) {
                            last_cycle_time = std::time::Instant::now();

                            run_cycle(
                                &db,
                                &assets,
                                &args,
                                &mut price_buffer,
                                &mut detector,
                                &mut metrics,
                                &mut portfolio,
                                &mut cached_auth,
                                &mut traded_positions,
                                &mut order_manager,
                                &mut exit_manager,
                                limit_price,
                                position_size,
                            ).await;
                        }

                        // Cleanup every 5 minutes
                        if last_cleanup_time.elapsed() >= Duration::from_secs(300) {
                            last_cleanup_time = std::time::Instant::now();
                            portfolio.cleanup_stale_positions();

                            // Cleanup old open prices to prevent memory growth
                            let cutoff = Utc::now() - chrono::Duration::minutes(30);
                            price_buffer.cleanup_old_opens(cutoff);

                            // Get active market IDs for detector cleanup
                            let expiry_seconds = args.max_expiry_minutes * 60;
                            let all_timeframes = vec!["5m".to_string(), "15m".to_string()];
                            if let Ok(markets) = get_15m_updown_markets_with_orderbooks(
                                db.pool(),
                                args.max_orderbook_age,
                                &assets,
                                expiry_seconds,
                                &all_timeframes,
                            ).await {
                                let active_ids: Vec<Uuid> = markets.iter().map(|m| m.id).collect();
                                detector.cleanup_expired(&active_ids);

                                // Also cleanup expired positions in exit manager
                                if exit_manager.is_enabled() {
                                    exit_manager.cleanup_expired_positions(&active_ids);
                                }
                            }
                        }
                    }
                    None => {
                        warn!("RTDS WebSocket disconnected, reconnecting...");
                        match rtds_client.connect_with_retry(5).await {
                            Ok(new_stream) => {
                                rtds_stream = new_stream;
                                info!("Reconnected to RTDS WebSocket");
                            }
                            Err(e) => {
                                error!("Failed to reconnect: {}", e);
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                }
            }
        }
    }

    // Cancel all pending orders on shutdown (live trading only)
    if !args.dry_run {
        order_manager.cancel_all_pending().await;
    }

    // Final summary
    info!("=== FINAL STATUS ===");
    metrics.print_summary();
    if args.dry_run {
        portfolio.print_summary();
    }

    info!("Shutdown complete");
    Ok(())
}

/// Run a single trading cycle.
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    db: &Database,
    assets: &[String],
    args: &Args,
    price_buffer: &mut ChainlinkPriceBuffer,
    detector: &mut MispriceDetector,
    metrics: &mut Metrics,
    portfolio: &mut DryRunPortfolio,
    cached_auth: &mut Option<CachedAuth>,
    traded_positions: &mut HashSet<(Uuid, String)>,
    order_manager: &mut OrderManager,
    exit_manager: &mut ExitManager,
    limit_price: Decimal,
    position_size: Decimal,
) {
    // Poll for completed cancel tasks and process results
    for result in order_manager.poll_completed() {
        if result.was_filled {
            info!(
                "[VERIFIED] Order for market {} {} was filled",
                result.market_id, result.side
            );
            metrics.record_verified_fill();

            // Add to exit manager for trailing stop tracking (live mode)
            if exit_manager.is_enabled() && !args.dry_run {
                if let (Some(token_id), Some(shares), Some(price)) =
                    (&result.token_id, result.shares, result.price)
                {
                    exit_manager.add_position(
                        result.market_id,
                        result.market_name.clone(),
                        token_id.clone(),
                        result.side.clone(),
                        shares,
                        price,
                    );
                } else {
                    warn!(
                        "[EXIT_MGR] Missing market info for filled order {} {}, cannot track",
                        result.market_id, result.side
                    );
                }
            }
        } else {
            // Order was cancelled, remove from traded_positions to allow retry
            debug!(
                "[CANCELLED] Order for market {} {} was cancelled, allowing retry",
                result.market_id, result.side
            );
            traded_positions.remove(&(result.market_id, result.side.clone()));
            metrics.record_cancel();
        }
    }

    // Get markets expiring within window
    let expiry_seconds = args.max_expiry_minutes * 60;
    let min_expiry_seconds = args.min_expiry_minutes * 60;

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
            error!("Failed to query markets: {}", e);
            metrics.record_db_error();
            return;
        }
    };

    // Filter by minimum expiry
    let now = Utc::now();
    let markets: Vec<_> = markets
        .into_iter()
        .filter(|m| {
            let secs_to_expiry = (m.end_time - now).num_seconds();
            secs_to_expiry >= min_expiry_seconds
        })
        .collect();

    if markets.is_empty() {
        return;
    }

    debug!("Found {} tradeable markets", markets.len());

    // Check for trailing stop/take profit exits
    if exit_manager.is_enabled() && exit_manager.position_count() > 0 {
        let exits = exit_manager.check_exits(&markets, cached_auth).await;
        for exit in exits {
            metrics.record_exit(&exit);
            if args.dry_run && exit.success {
                portfolio.close_position(exit.market_id, exit.exit_price, exit.pnl);
            }
        }
    }

    // Process each market
    for market in &markets {
        // Calculate market start time based on timeframe
        let timeframe_minutes: i64 = match market.timeframe.as_str() {
            "5m" => 5,
            "15m" => 15,
            other => {
                warn!("Unknown timeframe '{}' for {}, skipping", other, market.name);
                continue;
            }
        };
        let start_time = market.end_time - chrono::Duration::minutes(timeframe_minutes);

        // Get Chainlink symbol for this asset
        let chainlink_symbol = match asset_to_chainlink_symbol(&market.asset) {
            Some(s) => s,
            None => continue,
        };

        // Get or create state (logs open price on new market discovery)
        let (state, _is_new) = match detector.get_or_create_state(
            market.id,
            &market.name,
            start_time,
            price_buffer,
            chainlink_symbol,
        ) {
            Some(s) => s,
            None => {
                // No Chainlink price data available yet
                debug!(
                    "No Chainlink price for {} yet (symbol: {})",
                    market.name, chainlink_symbol
                );
                continue;
            }
        };

        // Store open price before mutable borrow
        let open_price = state.open_price;

        // Get current price from Chainlink buffer
        let current_price = match price_buffer.get_latest(chainlink_symbol) {
            Some(p) => p,
            None => continue,
        };

        // Check for direction flip
        if let Some((flip_type, side)) = detector.update_and_check_flip(&market.id, current_price) {
            metrics.record_flip(&market.asset);

            // Check if already traded this side on this market
            if traded_positions.contains(&(market.id, side.to_string())) {
                debug!("Already traded {} on {}", side, market.name);
                continue;
            }

            // Get token ID, best ask, and orderbook for the side we want to buy
            let (token_id, best_ask, orderbook) = match side {
                "YES" => (&market.yes_token_id, market.yes_best_ask, &market.yes_asks),
                "NO" => (&market.no_token_id, market.no_best_ask, &market.no_asks),
                _ => continue,
            };

            // Check if best ask is at or below our limit price (orderbook depth check)
            let best_ask = match best_ask {
                Some(ask) if ask <= limit_price => ask,
                Some(ask) => {
                    debug!(
                        "[SKIP] {} {} best ask ${:.3} > limit ${:.2}",
                        market.name, side, ask, limit_price
                    );
                    continue;
                }
                None => {
                    debug!("[SKIP] {} {} no orderbook data", market.name, side);
                    continue;
                }
            };

            // Calculate shares at limit price
            let shares = (position_size / limit_price).round_dp(2);

            // Ensure shares is within limits
            if shares > dec!(99.99) {
                warn!("Shares {} exceeds max 99.99", shares);
                continue;
            }

            // Calculate realistic fill price using orderbook depth (20% slippage fallback)
            let fill_estimate = calculate_fill_price_with_slippage(
                orderbook.as_ref(),
                best_ask,
                shares,
                dec!(20), // 20% slippage fallback if orderbook unavailable
            );

            // Check if we can fully fill at acceptable price
            if !fill_estimate.fully_filled {
                debug!(
                    "[SKIP] {} {} insufficient depth: only {:.2} shares available",
                    market.name, side, fill_estimate.filled_shares
                );
                continue;
            }

            // Check if effective fill price exceeds our limit
            if fill_estimate.effective_price > limit_price {
                debug!(
                    "[SKIP] {} {} effective price ${:.3} > limit ${:.2}",
                    market.name, side, fill_estimate.effective_price, limit_price
                );
                continue;
            }

            info!(
                "[FLIP] {} {} -> {} LIMIT @ ${:.2} ({:.2} shares) | Chainlink Open: ${}, Current: ${} | Best ask: ${:.3}, Eff fill: ${:.3}",
                flip_type, market.name, side, limit_price, shares, open_price, current_price, best_ask, fill_estimate.effective_price
            );

            if args.dry_run {
                // DRY RUN - track in portfolio with realistic fill price
                let effective_price = fill_estimate.effective_price;
                let cost = shares * effective_price;
                info!(
                    "[DRY RUN] {} {:.2} shares @ ${:.3} (eff), cost: ${:.2}",
                    side, shares, effective_price, cost
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
                    entry_price: limit_price,
                    best_ask_price: best_ask,
                    effective_fill_price: effective_price,
                    cost,
                    end_time: market.end_time,
                    created_at: Utc::now(),
                    resolution_retries: 0,
                    last_retry_time: None,
                });

                // Also add to exit manager for trailing stop tracking
                if exit_manager.is_enabled() {
                    let token_id_for_exit = match side {
                        "YES" => &market.yes_token_id,
                        "NO" => &market.no_token_id,
                        _ => token_id,
                    };
                    exit_manager.add_position(
                        market.id,
                        market.name.clone(),
                        token_id_for_exit.clone(),
                        side.to_string(),
                        shares,
                        effective_price, // Use effective fill price as entry
                    );
                }

                traded_positions.insert((market.id, side.to_string()));
                detector.mark_traded(&market.id);
                metrics.record_trade(&market.asset, side);
            } else {
                // Check if we already have a pending order for this market/side
                if order_manager.has_pending_order(&market.id, side) {
                    debug!(
                        "[SKIP] Already have pending order for {} {}",
                        market.name, side
                    );
                    continue;
                }

                // REAL TRADE - LIMIT ORDER at specified price
                match execute_trade(
                    cached_auth,
                    token_id,
                    shares,
                    limit_price,
                    side,
                    &market.name,
                )
                .await
                {
                    Ok(order_id) => {
                        info!(
                            "[SUCCESS] LIMIT order {} @ ${:.2} (order_id: {})",
                            side, limit_price, order_id
                        );

                        // Track order - don't mark as fully traded until fill verified
                        traded_positions.insert((market.id, side.to_string()));
                        detector.mark_traded(&market.id);
                        metrics.record_trade(&market.asset, side);

                        // Track order with market info for exit manager
                        order_manager.track_order_with_market_info(
                            order_id,
                            market.id,
                            market.name.clone(),
                            side.to_string(),
                            Some(token_id.clone()),
                            Some(shares),
                            Some(limit_price),
                        );
                    }
                    Err(e) => {
                        error!("[FAILED] Trade execution: {:#}", e);
                        metrics.record_error();
                    }
                }
            }
        }
    }

    // Cleanup expired markets from tracking
    let before = traded_positions.len();
    traded_positions.retain(|(id, _)| markets.iter().any(|m| m.id == *id && m.end_time > now));
    if traded_positions.len() < before {
        debug!(
            "Cleaned {} expired positions from tracking",
            before - traded_positions.len()
        );
    }
}
