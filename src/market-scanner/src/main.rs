//! Market Scanner Service
//!
//! Polls Gamma API (Polymarket) and Kalshi API for prediction markets
//! and stores them in PostgreSQL.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use common::{
    deactivate_expired_markets, upsert_kalshi_market, upsert_market, Config, Database, GammaClient,
    KalshiClient, KalshiMarketInsert,
};

/// Market Scanner - discovers and tracks prediction markets
#[derive(Parser, Debug)]
#[command(name = "market-scanner")]
#[command(about = "Polls Gamma and Kalshi APIs for prediction markets")]
struct Args {
    /// Run once and exit (instead of continuous polling)
    #[arg(long)]
    once: bool,

    /// Poll interval in seconds
    #[arg(long, default_value = "60")]
    interval: u64,

    /// Kalshi assets to scan (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    kalshi_assets: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    FmtSubscriber::builder().with_max_level(Level::INFO).init();

    let args = Args::parse();

    info!("Market Scanner starting...");
    info!(
        "Mode: {}",
        if args.once {
            "single run"
        } else {
            "continuous"
        }
    );
    info!("Interval: {}s", args.interval);

    // Load configuration
    let config = Config::from_env()?;

    // Connect to database
    info!("Connecting to database...");
    let db = Database::connect(&config).await?;
    db.health_check().await?;
    info!("Database connected successfully");

    // Create API clients
    let gamma = GammaClient::new(&config);
    info!("Gamma API client initialized (Polymarket)");

    let kalshi = KalshiClient::new();
    info!("Kalshi API client initialized");

    // Parse Kalshi assets
    let kalshi_assets: Vec<String> = args
        .kalshi_assets
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    info!("Kalshi assets: {:?}", kalshi_assets);

    // Main loop
    loop {
        match scan_markets(&gamma, &kalshi, &db, &kalshi_assets).await {
            Ok(stats) => {
                info!(
                    "Scan complete: {} Polymarket, {} Kalshi upserted, {} expired",
                    stats.polymarket_upserted, stats.kalshi_upserted, stats.expired
                );
            }
            Err(e) => {
                error!("Scan failed: {}", e);
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

/// Statistics from a scan run.
struct ScanStats {
    polymarket_upserted: usize,
    kalshi_upserted: usize,
    expired: u64,
}

/// Perform a single market scan cycle.
async fn scan_markets(
    gamma: &GammaClient,
    kalshi: &KalshiClient,
    db: &Database,
    kalshi_assets: &[String],
) -> Result<ScanStats> {
    // Step 1: Fetch Polymarket markets from Gamma API
    info!("Fetching Polymarket markets from Gamma API...");
    let poly_markets = gamma.fetch_supported_markets().await?;
    info!("Fetched {} Polymarket markets", poly_markets.len());

    let mut polymarket_upserted = 0;
    for market in &poly_markets {
        match upsert_market(db.pool(), market).await {
            Ok(_) => polymarket_upserted += 1,
            Err(e) => warn!("Failed to upsert Polymarket {}: {}", market.condition_id, e),
        }
    }

    // Step 2: Fetch Kalshi markets
    info!("Fetching Kalshi markets...");
    let kalshi_markets = match kalshi.fetch_parsed_crypto_markets().await {
        Ok(markets) => markets,
        Err(e) => {
            warn!("Failed to fetch Kalshi markets: {}", e);
            Vec::new()
        }
    };
    info!("Fetched {} Kalshi markets", kalshi_markets.len());

    let mut kalshi_upserted = 0;
    for market in &kalshi_markets {
        // Filter by asset
        if !kalshi_assets
            .iter()
            .any(|a| a.eq_ignore_ascii_case(&market.asset))
        {
            continue;
        }

        let insert: KalshiMarketInsert = market.into();
        match upsert_kalshi_market(db.pool(), &insert).await {
            Ok(_) => kalshi_upserted += 1,
            Err(e) => warn!("Failed to upsert Kalshi {}: {}", market.ticker, e),
        }
    }

    // Step 3: Deactivate expired markets
    let expired = deactivate_expired_markets(db.pool()).await?;
    if expired > 0 {
        info!("Deactivated {} expired markets", expired);
    }

    Ok(ScanStats {
        polymarket_upserted,
        kalshi_upserted,
        expired,
    })
}
