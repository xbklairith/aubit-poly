//! Kalshi Orderbook Stream Service
//!
//! Connects to Kalshi WebSocket API and streams orderbook data to PostgreSQL.
//! Unlike Polymarket's CLOB, Kalshi only provides best bid/ask (no full depth).

use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use common::{
    run_kalshi_orderbook_stream, update_kalshi_prices, Config, Database, KalshiClient,
    KalshiOrderbookUpdate,
};

/// Kalshi Orderbook Stream - real-time orderbook data via WebSocket
#[derive(Parser, Debug)]
#[command(name = "kalshi-orderbook-stream")]
#[command(about = "Streams orderbook data from Kalshi WebSocket")]
struct Args {
    /// Run once and exit (fetch single snapshot per market)
    #[arg(long)]
    once: bool,

    /// Refresh market list interval in seconds
    #[arg(long, default_value = "300")]
    refresh_interval: u64,

    /// Reconnect interval in seconds
    #[arg(long, default_value = "60")]
    reconnect_interval: u64,

    /// Max hours until expiry for markets to stream (default: 6 hours)
    #[arg(long, default_value = "6")]
    max_expiry_hours: i64,

    /// Assets to stream (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let log_level = std::env::var("RUST_LOG")
        .map(|_| Level::DEBUG)
        .unwrap_or(Level::INFO);
    FmtSubscriber::builder().with_max_level(log_level).init();

    let args = Args::parse();

    info!("Kalshi Orderbook Stream starting...");
    info!(
        "Mode: {}",
        if args.once {
            "single snapshot"
        } else {
            "continuous"
        }
    );
    info!("Assets: {}", args.assets);
    info!("Max expiry: {} hours", args.max_expiry_hours);

    // Parse assets
    let assets: Vec<String> = args
        .assets
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // Load configuration
    let config = Config::from_env()?;

    // Connect to database
    info!("Connecting to database...");
    let db = Database::connect(&config).await?;
    db.health_check().await?;
    info!("Database connected successfully");

    // Get Kalshi API credentials (optional - public data doesn't require auth)
    let api_key = std::env::var("KALSHI_API_KEY").ok();
    let private_key_pem = std::env::var("KALSHI_PRIVATE_KEY_PEM").ok().or_else(|| {
        std::env::var("KALSHI_PRIVATE_KEY_PATH")
            .ok()
            .and_then(|path| std::fs::read_to_string(path).ok())
    });

    if api_key.is_some() && private_key_pem.is_some() {
        info!("Using authenticated Kalshi WebSocket connection");
    } else {
        info!("Using unauthenticated Kalshi connection (public data only)");
    }

    // Create Kalshi REST client for market discovery
    let kalshi = KalshiClient::new();

    // Main loop
    loop {
        match run_stream(&db, &kalshi, &assets, &args, &api_key, &private_key_pem).await {
            Ok(_) => {
                if args.once {
                    info!("Single snapshot mode - exiting");
                    break;
                }
            }
            Err(e) => {
                error!("Stream error: {}. Reconnecting in 10s...", e);
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    Ok(())
}

/// Run the orderbook streaming loop.
async fn run_stream(
    db: &Database,
    kalshi: &KalshiClient,
    assets: &[String],
    args: &Args,
    api_key: &Option<String>,
    private_key_pem: &Option<String>,
) -> Result<()> {
    // Fetch Kalshi markets from REST API
    info!("Fetching Kalshi crypto markets...");
    let kalshi_markets = match kalshi.fetch_parsed_crypto_markets().await {
        Ok(markets) => markets,
        Err(e) => {
            warn!("Failed to fetch Kalshi markets: {}", e);
            sleep(Duration::from_secs(30)).await;
            return Ok(());
        }
    };

    // Filter by asset and expiry
    let now = chrono::Utc::now();
    let max_expiry = now + chrono::Duration::hours(args.max_expiry_hours);

    let filtered_markets: Vec<_> = kalshi_markets
        .into_iter()
        .filter(|m| assets.iter().any(|a| a.eq_ignore_ascii_case(&m.asset)))
        .filter(|m| m.close_time <= max_expiry)
        .collect();

    if filtered_markets.is_empty() {
        warn!("No Kalshi markets found matching criteria. Waiting...");
        sleep(Duration::from_secs(60)).await;
        return Ok(());
    }

    info!(
        "Found {} Kalshi markets to stream (filtered by assets and expiry)",
        filtered_markets.len()
    );

    // Build ticker to market ID mapping
    // We need to first upsert markets to get their DB IDs
    let mut ticker_to_db_id: std::collections::HashMap<String, uuid::Uuid> =
        std::collections::HashMap::new();

    for market in &filtered_markets {
        let insert: common::KalshiMarketInsert = market.into();
        match common::upsert_kalshi_market(db.pool(), &insert).await {
            Ok(id) => {
                ticker_to_db_id.insert(market.ticker.clone(), id);
            }
            Err(e) => {
                warn!("Failed to upsert Kalshi market {}: {}", market.ticker, e);
            }
        }
    }

    info!(
        "Upserted {} Kalshi markets to database",
        ticker_to_db_id.len()
    );

    // Get tickers to subscribe
    let tickers: Vec<String> = ticker_to_db_id.keys().cloned().collect();

    if tickers.is_empty() {
        warn!("No markets to subscribe to");
        sleep(Duration::from_secs(60)).await;
        return Ok(());
    }

    // Create channel for orderbook updates
    let (tx, mut rx) = mpsc::channel::<KalshiOrderbookUpdate>(1000);

    // Spawn WebSocket streaming task
    let ws_tickers = tickers.clone();
    let ws_api_key = api_key.clone();
    let ws_private_key = private_key_pem.clone();
    let reconnect_interval = Duration::from_secs(args.reconnect_interval);

    let ws_handle = tokio::spawn(async move {
        run_kalshi_orderbook_stream(
            ws_tickers,
            tx,
            ws_api_key,
            ws_private_key,
            reconnect_interval,
        )
        .await
    });

    // Process updates from WebSocket
    let mut update_count = 0u64;
    let mut last_stats_log = std::time::Instant::now();
    let stats_interval = Duration::from_secs(5);
    let connection_start = std::time::Instant::now();

    loop {
        // Log stats periodically
        if last_stats_log.elapsed() >= stats_interval {
            info!(
                "Stream stats: {} updates processed, {} markets tracked, uptime {}s",
                update_count,
                ticker_to_db_id.len(),
                connection_start.elapsed().as_secs()
            );
            last_stats_log = std::time::Instant::now();
        }

        // Check if WebSocket task has died
        if ws_handle.is_finished() {
            error!("WebSocket task has exited");
            break;
        }

        // Receive orderbook update with timeout
        match tokio::time::timeout(Duration::from_secs(30), rx.recv()).await {
            Ok(Some(update)) => {
                update_count += 1;

                // Look up market DB ID
                if let Some(&market_id) = ticker_to_db_id.get(&update.market_ticker) {
                    // Update prices in database
                    if let Err(e) = update_kalshi_prices(
                        db.pool(),
                        market_id,
                        update.yes_best_ask,
                        update.yes_best_bid,
                        update.no_best_bid,
                        update.no_best_ask,
                    )
                    .await
                    {
                        warn!(
                            "Failed to update prices for {}: {}",
                            update.market_ticker, e
                        );
                    }

                    if args.once {
                        info!("Single snapshot mode - exiting after first update");
                        return Ok(());
                    }
                }
            }
            Ok(None) => {
                // Channel closed
                error!("Update channel closed");
                break;
            }
            Err(_) => {
                // Timeout - no updates received
                warn!("No updates received for 30 seconds");
            }
        }
    }

    // Clean up
    ws_handle.abort();

    Ok(())
}
