//! Market Scanner Service
//!
//! Polls the Gamma API for prediction markets and stores them in PostgreSQL.

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use common::{
    deactivate_expired_markets, insert_orderbook_snapshot, upsert_market, Config, Database,
    GammaClient,
};

/// Market Scanner - discovers and tracks prediction markets
#[derive(Parser, Debug)]
#[command(name = "market-scanner")]
#[command(about = "Polls Gamma API for prediction markets")]
struct Args {
    /// Run once and exit (instead of continuous polling)
    #[arg(long)]
    once: bool,

    /// Poll interval in seconds
    #[arg(long, default_value = "60")]
    interval: u64,
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

    // Create Gamma API client
    let gamma = GammaClient::new(&config);
    info!("Gamma API client initialized");

    // Main loop
    loop {
        match scan_markets(&gamma, &db).await {
            Ok(stats) => {
                info!(
                    "Scan complete: {} markets upserted, {} expired",
                    stats.upserted, stats.expired
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
    upserted: usize,
    expired: u64,
}

/// Perform a single market scan cycle.
async fn scan_markets(gamma: &GammaClient, db: &Database) -> Result<ScanStats> {
    // Fetch markets from Gamma API
    info!("Fetching markets from Gamma API...");
    let markets = gamma.fetch_supported_markets().await?;
    info!("Fetched {} supported markets", markets.len());

    // Upsert each market and insert initial price snapshot
    let mut upserted = 0;
    let mut snapshots_added = 0;
    for market in &markets {
        match upsert_market(db.pool(), market).await {
            Ok(market_id) => {
                upserted += 1;

                // Insert initial orderbook snapshot with prices from Gamma API
                if market.yes_best_ask.is_some() || market.yes_best_bid.is_some() {
                    if let Err(e) = insert_orderbook_snapshot(
                        db.pool(),
                        market_id,
                        market.yes_best_ask,
                        market.yes_best_bid,
                        market.no_best_ask,
                        market.no_best_bid,
                        None, // yes_asks - not available from Gamma
                        None, // yes_bids
                        None, // no_asks
                        None, // no_bids
                        None, // event_timestamp - use DB NOW()
                    )
                    .await
                    {
                        warn!(
                            "Failed to insert snapshot for market {}: {}",
                            market.condition_id, e
                        );
                    } else {
                        snapshots_added += 1;
                    }
                }
            }
            Err(e) => {
                warn!("Failed to upsert market {}: {}", market.condition_id, e);
            }
        }
    }
    info!("Added {} initial price snapshots", snapshots_added);

    // Deactivate expired markets
    let expired = deactivate_expired_markets(db.pool()).await?;
    if expired > 0 {
        info!("Deactivated {} expired markets", expired);
    }

    Ok(ScanStats { upserted, expired })
}
