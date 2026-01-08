//! Orderbook Stream Service
//!
//! Connects to CLOB WebSocket and streams orderbook data to PostgreSQL.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use rust_decimal::Decimal;
use tokio::time::sleep;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use common::{
    get_active_markets_expiring_within, insert_orderbook_snapshot, ClobClient, ClobMessage,
    Config, Database, PriceLevel,
};

/// Orderbook Stream - real-time orderbook data via WebSocket
#[derive(Parser, Debug)]
#[command(name = "orderbook-stream")]
#[command(about = "Streams orderbook data from CLOB WebSocket")]
struct Args {
    /// Run once and exit (fetch single snapshot per market)
    #[arg(long)]
    once: bool,

    /// Refresh market list interval in seconds
    #[arg(long, default_value = "300")]
    refresh_interval: u64,

    /// Max hours until expiry for markets to stream (default: 6 hours)
    /// Limits markets to near-term ones relevant for trading
    #[arg(long, default_value = "6")]
    max_expiry_hours: i32,

    /// Maximum number of markets to subscribe to (default: 1000)
    /// Prevents WebSocket overload with too many subscriptions
    #[arg(long, default_value = "1000")]
    max_markets: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .init();

    let args = Args::parse();

    info!("Orderbook Stream starting...");
    info!(
        "Mode: {}",
        if args.once {
            "single snapshot"
        } else {
            "continuous"
        }
    );

    // Load configuration
    let config = Config::from_env()?;

    // Connect to database
    info!("Connecting to database...");
    let db = Database::connect(&config).await?;
    db.health_check().await?;
    info!("Database connected successfully");

    // Create CLOB WebSocket client
    let clob = ClobClient::new(&config);

    // Main loop
    loop {
        match run_stream(&clob, &db, &args).await {
            Ok(_) => {
                if args.once {
                    info!("Single snapshot mode - exiting");
                    break;
                }
            }
            Err(e) => {
                error!("Stream error: {}. Reconnecting in 5s...", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    }

    Ok(())
}

/// Run the orderbook streaming loop.
async fn run_stream(clob: &ClobClient, db: &Database, args: &Args) -> Result<()> {
    // Get active markets from database (filtered by expiry time and limited)
    info!(
        "Fetching active markets (expiring within {} hours, max {})...",
        args.max_expiry_hours, args.max_markets
    );
    let markets =
        get_active_markets_expiring_within(db.pool(), args.max_expiry_hours, args.max_markets)
            .await?;

    if markets.is_empty() {
        warn!("No active markets found in database. Run market-scanner first.");
        if args.once {
            return Ok(());
        }
        sleep(Duration::from_secs(60)).await;
        return Ok(());
    }

    info!("Found {} active markets", markets.len());

    // Build market lookup maps
    let mut token_to_market: HashMap<String, (Uuid, bool)> = HashMap::new(); // (market_id, is_yes)
    let mut asset_ids: Vec<String> = Vec::new();

    for market in &markets {
        // Map YES token
        token_to_market.insert(market.yes_token_id.clone(), (market.id, true));
        asset_ids.push(market.yes_token_id.clone());

        // Map NO token
        token_to_market.insert(market.no_token_id.clone(), (market.id, false));
        asset_ids.push(market.no_token_id.clone());
    }

    info!("Subscribing to {} token IDs", asset_ids.len());

    // Connect to WebSocket with retry
    let mut ws = clob.connect_with_retry(5).await?;
    info!("Connected to CLOB WebSocket");

    // Subscribe to market updates
    clob.subscribe(&mut ws, asset_ids).await?;
    info!("Subscribed to orderbook updates");

    // Track orderbook state per market
    let mut orderbooks: HashMap<Uuid, MarketOrderbook> = HashMap::new();
    let mut snapshot_count = 0;
    let mut last_ping = std::time::Instant::now();
    let ping_interval = Duration::from_secs(10);

    // Process stream messages
    loop {
        // Send keepalive ping every 10 seconds per Polymarket docs
        if last_ping.elapsed() >= ping_interval {
            if let Err(e) = clob.send_ping(&mut ws).await {
                warn!("Failed to send ping: {}", e);
            }
            last_ping = std::time::Instant::now();
        }

        // Use timeout to not block forever waiting for messages
        let read_result = tokio::time::timeout(
            Duration::from_secs(5),
            clob.read_message(&mut ws)
        ).await;

        match read_result {
            Ok(Ok(Some(ClobMessage::Books(books)))) => {
                // Batch of book snapshots (initial subscription response)
                info!("Received batch of {} book snapshots", books.len());
                for book in books {
                    process_book(&book, &token_to_market, &mut orderbooks, db, &mut snapshot_count, args.once, markets.len()).await?;
                }
                if args.once && snapshot_count >= markets.len() {
                    info!("Captured snapshots for all {} markets", markets.len());
                    return Ok(());
                }
            }
            Ok(Ok(Some(ClobMessage::Book(book)))) => {
                debug!("Received book update for asset {}", book.asset_id);
                process_book(&book, &token_to_market, &mut orderbooks, db, &mut snapshot_count, args.once, markets.len()).await?;
                if args.once && snapshot_count >= markets.len() {
                    info!("Captured snapshots for all {} markets", markets.len());
                    return Ok(());
                }
            }
            Ok(Ok(Some(ClobMessage::PriceChange(_)))) => {
                // Price changes are delta updates, we primarily care about full book snapshots
                debug!("Received price change (ignoring)");
            }
            Ok(Ok(Some(ClobMessage::Trade(trade)))) => {
                debug!(
                    "Received trade: {} @ {} ({})",
                    trade.asset_id, trade.price, trade.side
                );
            }
            Ok(Ok(Some(ClobMessage::Ping))) => {
                debug!("Received ping");
            }
            Ok(Ok(Some(ClobMessage::Ack))) => {
                debug!("Received acknowledgement");
            }
            Ok(Ok(Some(ClobMessage::Unknown(msg)))) => {
                debug!("Received unknown message: {}", msg);
            }
            Ok(Ok(None)) => {
                // Non-text message, ignore
            }
            Ok(Err(e)) => {
                error!("WebSocket error: {}", e);
                return Err(e.into());
            }
            Err(_) => {
                // Timeout - no message received, continue loop to check ping
                debug!("Read timeout, checking ping...");
            }
        }
    }
}

/// Process a single book message and update orderbook state.
async fn process_book(
    book: &common::BookMessage,
    token_to_market: &HashMap<String, (Uuid, bool)>,
    orderbooks: &mut HashMap<Uuid, MarketOrderbook>,
    db: &Database,
    snapshot_count: &mut usize,
    _once: bool,
    _total_markets: usize,
) -> Result<()> {
    if let Some(&(market_id, is_yes)) = token_to_market.get(&book.asset_id) {
        let orderbook = orderbooks
            .entry(market_id)
            .or_insert_with(MarketOrderbook::new);

        if is_yes {
            orderbook.yes_asks = book.asks.clone();
            orderbook.yes_bids = book.bids.clone();
            orderbook.yes_best_ask = book.best_ask();
            orderbook.yes_best_bid = book.best_bid();
        } else {
            orderbook.no_asks = book.asks.clone();
            orderbook.no_bids = book.bids.clone();
            orderbook.no_best_ask = book.best_ask();
            orderbook.no_best_bid = book.best_bid();
        }

        // Save snapshot if we have data for both YES and NO
        if orderbook.has_both_sides() {
            save_snapshot(db, market_id, orderbook).await?;
            *snapshot_count += 1;
        }
    }
    Ok(())
}

/// Save an orderbook snapshot to the database.
async fn save_snapshot(db: &Database, market_id: Uuid, orderbook: &MarketOrderbook) -> Result<()> {
    let yes_asks = serde_json::to_value(&orderbook.yes_asks)?;
    let yes_bids = serde_json::to_value(&orderbook.yes_bids)?;
    let no_asks = serde_json::to_value(&orderbook.no_asks)?;
    let no_bids = serde_json::to_value(&orderbook.no_bids)?;

    insert_orderbook_snapshot(
        db.pool(),
        market_id,
        orderbook.yes_best_ask,
        orderbook.yes_best_bid,
        orderbook.no_best_ask,
        orderbook.no_best_bid,
        Some(yes_asks),
        Some(yes_bids),
        Some(no_asks),
        Some(no_bids),
    )
    .await?;

    debug!(
        "Saved snapshot for market {}: yes_ask={:?}, yes_bid={:?}, no_ask={:?}, no_bid={:?}",
        market_id,
        orderbook.yes_best_ask,
        orderbook.yes_best_bid,
        orderbook.no_best_ask,
        orderbook.no_best_bid
    );

    Ok(())
}

/// Orderbook state for a single market.
#[derive(Debug, Default)]
struct MarketOrderbook {
    yes_asks: Vec<PriceLevel>,
    yes_bids: Vec<PriceLevel>,
    no_asks: Vec<PriceLevel>,
    no_bids: Vec<PriceLevel>,
    yes_best_ask: Option<Decimal>,
    yes_best_bid: Option<Decimal>,
    no_best_ask: Option<Decimal>,
    no_best_bid: Option<Decimal>,
}

impl MarketOrderbook {
    fn new() -> Self {
        Self::default()
    }

    /// Check if we have data for both YES and NO sides.
    fn has_both_sides(&self) -> bool {
        (self.yes_best_ask.is_some() || !self.yes_asks.is_empty())
            && (self.no_best_ask.is_some() || !self.no_asks.is_empty())
    }
}
