//! Misprice Trader - Trades on BTC price direction flips relative to 15-minute open.
//!
//! Strategy:
//! 1. On market discovery: Get the open price from Binance kline matching market start time
//! 2. Track direction: current_price > open_price -> UP, otherwise -> DOWN
//! 3. On direction flip:
//!    - DOWN -> UP -> Place LIMIT order at $0.40 to BUY YES
//!    - UP -> DOWN -> Place LIMIT order at $0.40 to BUY NO
//! 4. Auto-cancel order after 10 seconds if not filled
//! 5. Only trade once per market (first qualifying flip)

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
    calculate_fill_price_with_slippage, execute_trade, get_15m_updown_markets_with_orderbooks,
    BinanceEvent, BinanceStreamType, BinanceWsClient, CachedAuth, Config, Database,
    DryRunPortfolio, GammaClient, KlineBuffer, SimulatedPosition,
};

mod detector;
mod metrics;
mod order_manager;

use detector::MispriceDetector;
use metrics::Metrics;
use order_manager::OrderManager;

/// Misprice Trader - trades on BTC price direction flips
#[derive(Parser, Debug)]
#[command(name = "misprice-trader")]
#[command(about = "Trades Polymarket when BTC price flips direction relative to 15-min open")]
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
}

/// Map of asset -> Binance symbol. Returns None for unsupported assets.
fn asset_to_binance_symbol(asset: &str) -> Option<&'static str> {
    match asset.to_uppercase().as_str() {
        "BTC" => Some("BTCUSDT"),
        "ETH" => Some("ETHUSDT"),
        "SOL" => Some("SOLUSDT"),
        "XRP" => Some("XRPUSDT"),
        _ => {
            warn!("Unsupported asset: {}, skipping", asset);
            None
        }
    }
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

    info!("=== Misprice Trader ===");
    info!("Limit price: ${}", args.limit_price);
    info!("Position size: ${}", args.position_size);
    info!(
        "Expiry window: {}-{} minutes",
        args.min_expiry_minutes, args.max_expiry_minutes
    );
    info!("Max orderbook age: {}s", args.max_orderbook_age);
    info!("Assets: {}", args.assets);
    info!("Cancel timeout: {}s", args.cancel_timeout);
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

    // Build Binance symbols list
    let binance_symbols: Vec<String> = assets
        .iter()
        .filter_map(|a| asset_to_binance_symbol(a).map(|s| s.to_string()))
        .collect();

    if binance_symbols.is_empty() {
        anyhow::bail!("No supported assets specified");
    }

    info!("Binance symbols: {:?}", binance_symbols);

    // Convert parameters to Decimal
    let limit_price = Decimal::try_from(args.limit_price).context("Invalid limit_price")?;
    let position_size = Decimal::try_from(args.position_size).context("Invalid position_size")?;

    // Initialize components
    // Buffer needs to hold ~20 minutes of 1-minute klines to cover market start times
    let mut kline_buffer = KlineBuffer::new(25);
    let mut detector = MispriceDetector::new();
    let mut metrics = Metrics::new();
    let mut portfolio = DryRunPortfolio::new();
    let mut cached_auth: Option<CachedAuth> = None;
    // Track (market_id, side) - only trade once per market per side
    let mut traded_positions: HashSet<(Uuid, String)> = HashSet::new();
    // Order manager for tracking pending orders and auto-cancel (live trading only)
    let mut order_manager = OrderManager::new(args.cancel_timeout);

    // Connect to Binance WebSocket (Both = bookTicker for real-time + klines for history)
    let binance_client =
        BinanceWsClient::with_stream_type(binance_symbols.clone(), BinanceStreamType::Both);

    info!("Connecting to Binance WebSocket (bookTicker + klines)...");
    let mut binance_ws = binance_client.connect_with_retry(5).await?;
    info!("Connected to Binance WebSocket");

    // Main loop with graceful shutdown
    let mut last_cycle_time = std::time::Instant::now();
    let mut last_cleanup_time = std::time::Instant::now();
    let mut klines_since_heartbeat: u64 = 0;

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
                info!("[ALIVE] Heartbeat - klines received: {}, markets tracked: {}",
                    klines_since_heartbeat, detector.tracked_count());
                klines_since_heartbeat = 0;
                metrics.print_summary();
                if args.dry_run {
                    portfolio.print_summary();
                    // Resolve expired positions during heartbeat
                    portfolio.resolve_expired(db.pool(), &gamma).await;
                }
            }
            event_opt = binance_ws.next_event() => {
                match event_opt {
                    Some(event) => {
                        // Handle both ticker and kline events
                        match event {
                            BinanceEvent::Ticker(ticker) => {
                                // Update price buffer from real-time ticker (~10ms updates)
                                kline_buffer.update_price(&ticker);
                                klines_since_heartbeat += 1;
                            }
                            BinanceEvent::Kline(kline) => {
                                // Add kline to buffer for historical data
                                kline_buffer.add(kline);
                            }
                        }

                        // Run trading cycle every 100ms (fast enough for bookTicker)
                        if last_cycle_time.elapsed() >= Duration::from_millis(100) {
                            last_cycle_time = std::time::Instant::now();

                            run_cycle(
                                &db,
                                &assets,
                                &args,
                                &kline_buffer,
                                &mut detector,
                                &mut metrics,
                                &mut portfolio,
                                &mut cached_auth,
                                &mut traded_positions,
                                &mut order_manager,
                                limit_price,
                                position_size,
                            ).await;
                        }

                        // Cleanup every 5 minutes
                        if last_cleanup_time.elapsed() >= Duration::from_secs(300) {
                            last_cleanup_time = std::time::Instant::now();
                            portfolio.cleanup_stale_positions();

                            // Get active market IDs for detector cleanup
                            let expiry_seconds = args.max_expiry_minutes * 60;
                            if let Ok(markets) = get_15m_updown_markets_with_orderbooks(
                                db.pool(),
                                args.max_orderbook_age,
                                &assets,
                                expiry_seconds,
                            ).await {
                                let active_ids: Vec<Uuid> = markets.iter().map(|m| m.id).collect();
                                detector.cleanup_expired(&active_ids);
                            }
                        }
                    }
                    None => {
                        warn!("Binance WebSocket disconnected, reconnecting...");
                        match binance_client.connect_with_retry(5).await {
                            Ok(new_ws) => {
                                binance_ws = new_ws;
                                info!("Reconnected to Binance WebSocket");
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

    binance_ws.close().await;
    info!("Shutdown complete");
    Ok(())
}

/// Run a single trading cycle.
#[allow(clippy::too_many_arguments)]
async fn run_cycle(
    db: &Database,
    assets: &[String],
    args: &Args,
    kline_buffer: &KlineBuffer,
    detector: &mut MispriceDetector,
    metrics: &mut Metrics,
    portfolio: &mut DryRunPortfolio,
    cached_auth: &mut Option<CachedAuth>,
    traded_positions: &mut HashSet<(Uuid, String)>,
    order_manager: &mut OrderManager,
    limit_price: Decimal,
    position_size: Decimal,
) {
    // Poll for completed cancel tasks and process results
    for (market_id, side, was_filled) in order_manager.poll_completed() {
        if was_filled {
            info!(
                "[VERIFIED] Order for market {} {} was filled",
                market_id, side
            );
            metrics.record_verified_fill();
        } else {
            // Order was cancelled, remove from traded_positions to allow retry
            debug!(
                "[CANCELLED] Order for market {} {} was cancelled, allowing retry",
                market_id, side
            );
            traded_positions.remove(&(market_id, side));
        }
    }

    // Get markets expiring within window
    let expiry_seconds = args.max_expiry_minutes * 60;
    let min_expiry_seconds = args.min_expiry_minutes * 60;

    let markets = match get_15m_updown_markets_with_orderbooks(
        db.pool(),
        args.max_orderbook_age,
        assets,
        expiry_seconds,
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

    // Process each market
    for market in &markets {
        // Calculate market start time (15 minutes before end_time)
        let start_time = market.end_time - chrono::Duration::minutes(15);

        // Get Binance symbol for this asset
        let binance_symbol = match asset_to_binance_symbol(&market.asset) {
            Some(s) => s,
            None => continue,
        };

        // Get or create state (logs open price on new market discovery)
        let (state, _is_new) = match detector.get_or_create_state(
            market.id,
            &market.name,
            start_time,
            kline_buffer,
            binance_symbol,
        ) {
            Some(s) => s,
            None => {
                // No kline data for market start time yet
                debug!(
                    "No kline data for {} at start time {}",
                    market.name,
                    start_time.format("%H:%M:%S")
                );
                continue;
            }
        };

        // Store open price before mutable borrow
        let open_price = state.open_price;

        // Get current price from kline buffer
        let current_price = match kline_buffer.get_latest_close(binance_symbol) {
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
                "[FLIP] {} {} -> {} LIMIT @ ${:.2} ({:.2} shares) | Open: ${}, Current: ${} | Best ask: ${:.3}, Eff fill: ${:.3}",
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

                        // Track order with managed auto-cancel
                        order_manager.track_order(
                            order_id,
                            market.id,
                            market.name.clone(),
                            side.to_string(),
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
