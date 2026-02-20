//! Momentum Trader - Trades Polymarket based on Binance price momentum.
//!
//! Strategy: When Binance shows significant momentum (0.2%+ over 5 minutes),
//! buy the corresponding side on Polymarket 15-minute up/down markets.
//!
//! Expected win rate: 84-87% based on backtest results.

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
    calculate_fill_price_with_slippage, cancel_order_standalone, execute_trade,
    get_15m_updown_markets_with_orderbooks, BinanceEvent, BinanceStreamType, BinanceWsClient,
    CachedAuth, Config, Database, DryRunPortfolio, GammaClient, KlineBuffer, MomentumDirection,
    SimulatedPosition, MAX_SHARES,
};

mod detector;
mod metrics;

use detector::SignalDetector;
use metrics::Metrics;

/// Momentum Trader - trades based on Binance price momentum
#[derive(Parser, Debug)]
#[command(name = "momentum-trader")]
#[command(about = "Trades Polymarket based on Binance price momentum")]
struct Args {
    /// Minimum momentum percentage (0.002 = 0.2%)
    #[arg(long, default_value = "0.002")]
    min_momentum: f64,

    /// Momentum lookback window in minutes
    #[arg(long, default_value = "5")]
    lookback_minutes: u64,

    /// Maximum entry price on Polymarket (skip if price > this)
    #[arg(long, default_value = "0.70")]
    max_entry_price: f64,

    /// Position size in USDC
    #[arg(long, default_value = "5")]
    position_size: f64,

    /// Maximum time to market expiry in minutes
    #[arg(long, default_value = "10")]
    max_expiry_minutes: i64,

    /// Minimum time to market expiry in minutes
    #[arg(long, default_value = "1")]
    min_expiry_minutes: i64,

    /// Cooldown per market in seconds (prevent re-entry)
    #[arg(long, default_value = "900")]
    cooldown_secs: u64,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "1")]
    max_orderbook_age: i32,

    /// Assets to trade (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,

    /// Dry run mode (no actual trades)
    #[arg(long)]
    dry_run: bool,

    /// Slippage percentage for fill price estimation
    #[arg(long, default_value = "20")]
    slippage_pct: f64,
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

    info!("=== Momentum Trader ===");
    info!("Min momentum: {}%", args.min_momentum * 100.0);
    info!("Lookback: {} minutes", args.lookback_minutes);
    info!("Max entry price: ${}", args.max_entry_price);
    info!("Position size: ${}", args.position_size);
    info!(
        "Expiry window: {}-{} minutes",
        args.min_expiry_minutes, args.max_expiry_minutes
    );
    info!("Cooldown: {} seconds", args.cooldown_secs);
    info!("Assets: {}", args.assets);
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

    // Build Binance symbols list (filter out unsupported assets)
    let binance_symbols: Vec<String> = assets
        .iter()
        .filter_map(|a| asset_to_binance_symbol(a).map(|s| s.to_string()))
        .collect();

    if binance_symbols.is_empty() {
        anyhow::bail!("No supported assets specified");
    }

    info!("Binance symbols: {:?}", binance_symbols);

    // Convert parameters to Decimal
    let min_momentum = Decimal::try_from(args.min_momentum).context("Invalid min_momentum")?;
    let max_entry_price =
        Decimal::try_from(args.max_entry_price).context("Invalid max_entry_price")?;
    let position_size = Decimal::try_from(args.position_size).context("Invalid position_size")?;
    let slippage_pct = Decimal::try_from(args.slippage_pct).context("Invalid slippage_pct")?;

    // Initialize components
    let mut kline_buffer = KlineBuffer::new(args.lookback_minutes as usize + 2);
    let mut detector = SignalDetector::new(
        min_momentum,
        args.lookback_minutes as usize,
        max_entry_price,
        args.cooldown_secs,
    );
    let mut metrics = Metrics::new();
    let mut portfolio = DryRunPortfolio::new();
    let mut cached_auth: Option<CachedAuth> = None;
    // Track (market_id, side) - allows trading both YES and NO on same market
    let mut traded_positions: HashSet<(Uuid, String)> = HashSet::new();

    // Connect to Binance WebSocket (Both = bookTicker for real-time + klines for momentum)
    let binance_client =
        BinanceWsClient::with_stream_type(binance_symbols.clone(), BinanceStreamType::Both);

    info!("Connecting to Binance WebSocket (bookTicker + klines)...");
    let mut binance_ws = binance_client.connect_with_retry(5).await?;
    info!("Connected to Binance WebSocket");

    // Main loop with graceful shutdown
    let mut last_cycle_time = std::time::Instant::now();
    let mut last_cleanup_time = std::time::Instant::now();
    let mut klines_since_heartbeat: u64 = 0;

    // Independent heartbeat timer (every 60 seconds)
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
                info!("[ALIVE] Heartbeat - klines received: {}", klines_since_heartbeat);
                klines_since_heartbeat = 0;
                metrics.print_summary();
                if args.dry_run {
                    portfolio.print_summary();
                    // Also resolve any expired positions during heartbeat
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
                                // Add kline to buffer for momentum calculation
                                kline_buffer.add(kline);
                            }
                        }

                        // Check if we should run a trading cycle (every 500ms for faster response)
                        if last_cycle_time.elapsed() >= Duration::from_millis(500) {
                            last_cycle_time = std::time::Instant::now();

                            // Run trading cycle
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
                                position_size,
                                slippage_pct,
                            ).await;
                        }

                        // Cleanup cooldowns every 5 minutes
                        if last_cleanup_time.elapsed() >= Duration::from_secs(300) {
                            last_cleanup_time = std::time::Instant::now();
                            detector.cleanup_cooldowns();
                            portfolio.cleanup_stale_positions();
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
    detector: &mut SignalDetector,
    metrics: &mut Metrics,
    portfolio: &mut DryRunPortfolio,
    cached_auth: &mut Option<CachedAuth>,
    traded_positions: &mut HashSet<(Uuid, String)>,
    position_size: Decimal,
    slippage_pct: Decimal,
) {
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

    // Check each asset for momentum signals
    for asset in assets {
        let binance_symbol = match asset_to_binance_symbol(asset) {
            Some(s) => s,
            None => continue,
        };

        // Check if we have enough data for momentum calculation
        if kline_buffer.len(binance_symbol) < args.lookback_minutes as usize {
            debug!(
                "Not enough data for {} ({}/{} klines)",
                asset,
                kline_buffer.len(binance_symbol),
                args.lookback_minutes
            );
            continue;
        }

        // Calculate momentum
        let momentum_result =
            kline_buffer.calculate_momentum(binance_symbol, args.lookback_minutes as usize);

        let (momentum_pct, direction) = match momentum_result {
            Some(r) => r,
            None => continue,
        };

        // Check if momentum meets threshold
        if momentum_pct.abs() < detector.min_momentum {
            continue;
        }

        metrics.record_signal(asset);

        // Find matching market for this asset
        let matching_market = markets.iter().find(|m| m.asset.to_uppercase() == *asset);

        let market = match matching_market {
            Some(m) => m,
            None => {
                debug!("No matching market for {} signal", asset);
                continue;
            }
        };

        // Determine side first to check if already traded
        let side = match direction {
            MomentumDirection::Up => "YES",
            MomentumDirection::Down => "NO",
        };

        // Check if this specific (market, side) already traded
        if traded_positions.contains(&(market.id, side.to_string())) {
            debug!("Already traded {} on {}", side, market.name);
            continue;
        }

        // Check cooldown
        if !detector.can_trade(&market.condition_id) {
            debug!("Market {} in cooldown", market.name);
            continue;
        }

        // Get the price for the side we want to buy
        let (side, token_id, entry_price, orderbook) = match direction {
            MomentumDirection::Up => {
                // Buy YES
                let price = market.yes_best_ask.unwrap_or(dec!(1));
                ("YES", &market.yes_token_id, price, &market.yes_asks)
            }
            MomentumDirection::Down => {
                // Buy NO
                let price = market.no_best_ask.unwrap_or(dec!(1));
                ("NO", &market.no_token_id, price, &market.no_asks)
            }
        };

        // Check if price is acceptable
        if entry_price > detector.max_entry_price {
            debug!(
                "Price ${:.2} > max ${:.2} for {} {}",
                entry_price, detector.max_entry_price, side, market.name
            );
            continue;
        }

        // Calculate fill price with slippage
        let fill_estimate = calculate_fill_price_with_slippage(
            orderbook.as_ref(),
            entry_price,
            position_size / entry_price,
            slippage_pct,
        );

        // Calculate shares
        let shares = (position_size / fill_estimate.effective_price).round_dp(2);
        if shares > MAX_SHARES {
            warn!("Shares {} exceeds max {}", shares, MAX_SHARES);
            continue;
        }

        let binance_price = kline_buffer
            .current_price(binance_symbol)
            .unwrap_or(dec!(0));

        info!(
            "[SIGNAL] {} {:.3}% -> {} {} @ ${:.4} (Binance: ${:.2}, {} shares)",
            asset,
            momentum_pct * dec!(100),
            side,
            market.name,
            fill_estimate.effective_price,
            binance_price,
            shares,
        );

        if args.dry_run {
            let cost = shares * fill_estimate.effective_price;
            info!(
                "[DRY RUN] {} {:.2} shares @ ${:.4} -> Win: ${:.2}",
                side,
                shares,
                fill_estimate.effective_price,
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
                entry_price,
                best_ask_price: entry_price,
                effective_fill_price: fill_estimate.effective_price,
                cost,
                end_time: market.end_time,
                created_at: Utc::now(),
                resolution_retries: 0,
                last_retry_time: None,
            });

            traded_positions.insert((market.id, side.to_string()));
            detector.record_trade(&market.condition_id);
            metrics.record_trade(asset, side);
        } else {
            // Execute real trade
            match execute_trade(
                cached_auth,
                token_id,
                shares,
                entry_price,
                side,
                &market.name,
            )
            .await
            {
                Ok(order_id) => {
                    info!(
                        "[SUCCESS] Order {} for {} {} @ ${}",
                        order_id, side, market.name, entry_price
                    );
                    traded_positions.insert((market.id, side.to_string()));
                    detector.record_trade(&market.condition_id);
                    metrics.record_trade(asset, side);

                    // Cancel order after 10 seconds if not filled
                    let order_id_for_cancel = order_id.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                        match cancel_order_standalone(order_id_for_cancel.clone()).await {
                            Ok(()) => {
                                info!(
                                    "[CANCEL] Order {} cancelled after 10s timeout",
                                    order_id_for_cancel
                                );
                            }
                            Err(e) => {
                                debug!(
                                    "Cancel order {} (may already be filled): {}",
                                    order_id_for_cancel, e
                                );
                            }
                        }
                    });
                }
                Err(e) => {
                    error!("[FAILED] Trade execution: {:#}", e);
                    metrics.record_error();
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
