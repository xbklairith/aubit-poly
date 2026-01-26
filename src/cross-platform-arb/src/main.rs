//! Cross-Platform Arbitrage Service
//!
//! Detects arbitrage opportunities between Polymarket and Limitless prediction markets.
//! Currently in detection-only mode (no auto-execution).
//!
//! Architecture:
//! - Polymarket: Uses existing market data from DB (orderbook-stream WebSocket)
//! - Limitless: REST polling for prices via limitless-loader service
//! - Matching: Entity extraction + scoring algorithm
//! - Detection: Cross-platform spread calculation with fee adjustment

mod detector;
mod event_matcher;
mod slippage;

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use common::{
    Config, CrossPlatformOpportunity, Database, GammaClient, MarketWithPlatform, Platform,
    UnifiedMarket, get_platform_markets_with_prices, update_polymarket_prices,
    get_latest_orderbook_snapshot,
};
use rust_decimal::Decimal;
use tokio::time::sleep;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

use detector::{CrossPlatformDetector, DetectorConfig, ScanSummary};
use event_matcher::{EventMatcher, MatcherConfig};
use slippage::{calculate_max_profitable_size, parse_polymarket_depth};

/// Cross-Platform Arbitrage Detector
#[derive(Parser, Debug)]
#[command(name = "cross-platform-arb")]
#[command(about = "Detects arbitrage between Polymarket and Limitless")]
struct Args {
    /// Run once and exit (instead of continuous scanning)
    #[arg(long)]
    once: bool,

    /// Scan interval in seconds
    #[arg(long, default_value = "30")]
    interval: u64,

    /// Minimum net profit percentage
    #[arg(long, default_value = "3.5")]
    min_profit: f64,

    /// Minimum profit for 15-minute markets
    #[arg(long, default_value = "1.0")]
    min_profit_15m: f64,

    /// Minimum liquidity in dollars
    #[arg(long, default_value = "500")]
    min_liquidity: f64,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "30")]
    max_orderbook_age: i32,

    /// Maximum time to expiry in seconds
    #[arg(long, default_value = "7200")]
    max_expiry_secs: i64,

    /// Minimum match confidence (0-1)
    #[arg(long, default_value = "0.9")]
    min_match_confidence: f64,

    /// Assets to scan (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,

    /// Verbose logging
    #[arg(long, short)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with RUST_LOG env filter
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();

    info!("Cross-Platform Arbitrage Detector starting...");
    info!(
        "Mode: {}",
        if args.once { "single run" } else { "continuous" }
    );
    info!("Scan interval: {}s", args.interval);
    info!(
        "Min profit: {}% ({}% for 15m)",
        args.min_profit, args.min_profit_15m
    );
    info!("Assets: {}", args.assets);

    // Parse assets
    let assets: Vec<String> = args.assets.split(',').map(|s| s.trim().to_string()).collect();

    // Load configuration
    let config = Config::from_env()?;

    // Connect to database
    info!("Connecting to database...");
    let db = Database::connect(&config).await?;
    db.health_check().await?;
    info!("Database connected successfully");

    // Create Gamma client for Polymarket REST price fetching (fallback for WebSocket)
    let gamma = GammaClient::new(&config);
    info!("Gamma API client initialized (Polymarket REST fallback)");

    // Create matcher and detector
    let matcher_config = MatcherConfig {
        min_confidence: args.min_match_confidence,
        ..Default::default()
    };
    let matcher = EventMatcher::with_config(matcher_config);

    let detector_config = DetectorConfig {
        min_profit_pct: Decimal::try_from(args.min_profit)?,
        min_profit_pct_15m: Decimal::try_from(args.min_profit_15m)?,
        min_liquidity: Decimal::try_from(args.min_liquidity)?,
        max_price_staleness: args.max_orderbook_age as i64,
        min_match_confidence: args.min_match_confidence,
        ..Default::default()
    };
    let detector = CrossPlatformDetector::with_config(detector_config);

    // Main loop
    loop {
        match scan_cycle(&db, &gamma, &matcher, &detector, &assets, &args).await {
            Ok(summary) => {
                summary.log();
            }
            Err(e) => {
                error!("Scan cycle failed: {}", e);
            }
        }

        if args.once {
            info!("Single run mode - exiting");
            break;
        }

        info!("Sleeping for {}s...", args.interval);
        sleep(Duration::from_secs(args.interval)).await;
    }

    Ok(())
}

/// Perform a single scan cycle.
async fn scan_cycle(
    db: &Database,
    gamma: &GammaClient,
    matcher: &EventMatcher,
    detector: &CrossPlatformDetector,
    assets: &[String],
    args: &Args,
) -> Result<ScanSummary> {
    info!("Starting scan cycle...");

    // Step 1: Fetch Polymarket markets with fresh orderbooks
    info!("Fetching Polymarket markets with fresh orderbooks...");
    let mut polymarket_markets = get_platform_markets_with_prices(
        db.pool(),
        "polymarket",
        args.max_orderbook_age,
        assets,
        args.max_expiry_secs,
    )
    .await?;
    info!(
        "Fetched {} Polymarket markets with fresh prices (WebSocket)",
        polymarket_markets.len()
    );

    // Step 2b: If no fresh WebSocket data, fetch prices via Gamma REST API
    if polymarket_markets.is_empty() {
        info!("No fresh WebSocket data - fetching Polymarket prices via REST...");
        if let Err(e) = update_polymarket_prices_from_gamma(db, gamma, assets, args.max_expiry_secs).await {
            warn!("Failed to fetch Polymarket prices via REST: {}", e);
        }
        // Try fetching again with the new prices
        polymarket_markets = get_platform_markets_with_prices(
            db.pool(),
            "polymarket",
            args.max_orderbook_age + 5,  // Allow slightly more staleness for REST
            assets,
            args.max_expiry_secs,
        )
        .await?;
        info!(
            "Fetched {} Polymarket markets with fresh prices (REST fallback)",
            polymarket_markets.len()
        );
    }

    // Step 2: Fetch Limitless markets from DB
    let limitless_db_markets = get_platform_markets_with_prices(
        db.pool(),
        "limitless",
        args.max_orderbook_age + 10,
        assets,
        args.max_expiry_secs,
    )
    .await?;
    info!(
        "Fetched {} Limitless markets from DB with prices",
        limitless_db_markets.len()
    );

    // Step 3: Convert to unified markets
    let poly_unified: Vec<UnifiedMarket> = polymarket_markets
        .iter()
        .filter_map(|m| to_unified_market(m, Platform::Polymarket))
        .collect();

    let limitless_unified: Vec<UnifiedMarket> = limitless_db_markets
        .iter()
        .filter_map(|m| to_unified_market(m, Platform::Limitless))
        .collect();

    // Step 4: Match markets across platforms (Polymarket vs Limitless only)
    info!(
        "Matching {} Polymarket vs {} Limitless markets...",
        poly_unified.len(),
        limitless_unified.len()
    );
    let matches = matcher.match_markets(&poly_unified, &limitless_unified);
    info!("Found {} high-confidence matches", matches.len());

    // Step 5: Detect arbitrage opportunities
    let mut opportunities = detector.scan(&matches);

    // Step 6: Calculate max profitable size for each opportunity
    for opp in &mut opportunities {
        if let Some(sized_opp) = calculate_opportunity_size(db, opp, args.min_profit_15m).await {
            *opp = sized_opp;
        }
    }

    // Log opportunities
    for (i, opp) in opportunities.iter().enumerate() {
        let size_info = match (opp.max_contracts, opp.max_investment) {
            (Some(c), Some(inv)) => format!(" | Max: {} contracts (${:.0})", c, inv),
            _ => String::new(),
        };
        info!(
            "Opportunity #{}: {} vs {} | Buy YES on {} @ {} + NO on {} @ {} = {} | Net: {:.2}%{}",
            i + 1,
            opp.pair.polymarket.name,
            opp.pair.kalshi.name,  // Note: This is actually Limitless market (field name kept for compatibility)
            opp.buy_yes_on,
            opp.yes_price,
            opp.buy_no_on,
            opp.no_price,
            opp.total_cost,
            opp.net_profit_pct,
            size_info
        );
    }

    Ok(ScanSummary::new(
        poly_unified.len(),
        limitless_unified.len(),
        matches.len(),
        &opportunities,
    ))
}

/// Convert MarketWithPlatform to UnifiedMarket.
fn to_unified_market(m: &MarketWithPlatform, platform: Platform) -> Option<UnifiedMarket> {
    // Skip markets without prices
    if m.yes_best_ask.is_none() || m.no_best_ask.is_none() {
        return None;
    }

    Some(UnifiedMarket {
        platform,
        market_id: m.condition_id.clone(),
        db_id: Some(m.id),
        name: m.name.clone(),
        asset: m.asset.clone(),
        timeframe: m.timeframe.clone(),
        end_time: m.end_time,
        yes_best_ask: m.yes_best_ask,
        yes_best_bid: m.yes_best_bid,
        no_best_ask: m.no_best_ask,
        no_best_bid: m.no_best_bid,
        liquidity: m.liquidity_dollars,
        price_updated_at: m.captured_at,
        direction: m.direction.clone(),
        strike_price: m.strike_price,
        yes_depth: None,  // Loaded on demand for opportunities
        no_depth: None,
    })
}

/// Fetch and update Polymarket prices via Gamma REST API.
/// This is a fallback when WebSocket data is stale.
async fn update_polymarket_prices_from_gamma(
    db: &Database,
    gamma: &GammaClient,
    assets: &[String],
    _max_expiry_secs: i64,
) -> Result<()> {
    use common::get_market_by_condition_id;

    // Fetch all crypto markets from Gamma API (includes prices)
    let parsed_markets = gamma.fetch_supported_markets().await?;

    // Filter to relevant markets and update prices
    let mut updated_count = 0;

    for market in &parsed_markets {
        // Filter by asset and timeframe
        if !assets.iter().any(|a| a.eq_ignore_ascii_case(&market.asset)) {
            continue;
        }
        if market.timeframe != "15m" && market.timeframe != "5m" {
            continue;
        }

        // Skip if no prices
        if market.yes_best_ask.is_none() && market.yes_best_bid.is_none() {
            continue;
        }

        // Look up market in database by condition_id
        match get_market_by_condition_id(db.pool(), &market.condition_id).await {
            Ok(Some(db_market)) => {
                if let Err(e) = update_polymarket_prices(
                    db.pool(),
                    db_market.id,
                    market.yes_best_ask,
                    market.yes_best_bid,
                    market.no_best_ask,
                    market.no_best_bid,
                ).await {
                    warn!("Failed to update prices for {}: {}", market.name, e);
                } else {
                    updated_count += 1;
                }
            }
            Ok(None) => {
                // Market not in DB yet, skip
            }
            Err(e) => {
                warn!("Failed to lookup market {}: {}", market.condition_id, e);
            }
        }
    }

    info!("Updated {} Polymarket prices via REST", updated_count);
    Ok(())
}

/// Calculate max profitable order size for an opportunity.
/// Fetches orderbook depth from both platforms and calculates slippage.
async fn calculate_opportunity_size(
    db: &Database,
    opp: &CrossPlatformOpportunity,
    min_profit_pct: f64,
) -> Option<CrossPlatformOpportunity> {
    // Determine which market provides YES and which provides NO
    let (yes_market, no_market) = if opp.buy_yes_on == Platform::Polymarket {
        (&opp.pair.polymarket, &opp.pair.kalshi)
    } else {
        (&opp.pair.kalshi, &opp.pair.polymarket)
    };

    // Fetch YES depth
    let yes_depth = match fetch_market_depth(db, yes_market, "yes").await {
        Some(d) => d,
        None => {
            warn!("Failed to fetch YES depth for {} on {:?}", yes_market.name, yes_market.platform);
            return None;
        }
    };

    // Fetch NO depth
    let no_depth = match fetch_market_depth(db, no_market, "no").await {
        Some(d) => d,
        None => {
            warn!("Failed to fetch NO depth for {} on {:?}", no_market.name, no_market.platform);
            return None;
        }
    };

    // Calculate max profitable size
    let min_profit = Decimal::try_from(min_profit_pct).ok()?;
    let yes_fee = opp.buy_yes_on.fee_rate();
    let no_fee = opp.buy_no_on.fee_rate();

    let result = calculate_max_profitable_size(
        &yes_depth,
        &no_depth,
        yes_fee,
        no_fee,
        min_profit,
    )?;

    // Calculate total investment
    let investment = result.total_cost_a + result.total_cost_b + result.total_fees;

    Some(opp.clone().with_max_size(result.max_contracts, investment))
}

/// Fetch orderbook depth for a market.
async fn fetch_market_depth(
    db: &Database,
    market: &UnifiedMarket,
    side: &str,
) -> Option<common::OrderbookDepth> {
    match market.platform {
        Platform::Polymarket | Platform::Limitless => {
            // Fetch from database orderbook_snapshots (both Polymarket and Limitless use CLOB)
            let db_id = match market.db_id {
                Some(id) => id,
                None => {
                    warn!("{:?} market {} has no db_id", market.platform, market.name);
                    return None;
                }
            };

            let snapshot = match get_latest_orderbook_snapshot(db.pool(), db_id).await {
                Ok(Some(s)) => s,
                Ok(None) => {
                    warn!("No orderbook snapshot found for {:?} market {}", market.platform, market.name);
                    return None;
                }
                Err(e) => {
                    warn!("Error fetching orderbook snapshot for {}: {}", market.name, e);
                    return None;
                }
            };

            // Parse the JSONB depth data
            let json_depth = match side {
                "yes" => match &snapshot.yes_asks {
                    Some(d) => d.clone(),
                    None => {
                        warn!("No yes_asks depth in snapshot for {}", market.name);
                        return None;
                    }
                },
                "no" => match &snapshot.no_asks {
                    Some(d) => d.clone(),
                    None => {
                        warn!("No no_asks depth in snapshot for {}", market.name);
                        return None;
                    }
                },
                _ => return None,
            };

            let depth = parse_polymarket_depth(&json_depth);
            if depth.asks.is_empty() {
                warn!("Parsed empty {} depth for {:?} {}", side, market.platform, market.name);
                return None;
            }
            Some(depth)
        }
        Platform::Kalshi => {
            // Kalshi not used in this service
            warn!("Kalshi platform not supported in cross-platform-arb");
            None
        }
    }
}
