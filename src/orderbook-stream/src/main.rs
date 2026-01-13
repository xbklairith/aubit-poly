//! Orderbook Stream Service
//!
//! Connects to CLOB WebSocket and streams orderbook data to PostgreSQL.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use tokio::time::sleep;
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use common::{
    get_active_markets_expiring_within, get_priority_markets_hybrid, BookMessage, ClobClient,
    ClobMessage, Config, Database, PriceChange, PriceLevel, update_no_best_prices,
    update_yes_best_prices,
};

/// Maximum age (in ms) for buffered messages to be considered fresh.
/// Messages older than this are discarded to prevent stale prices.
const MAX_BUFFERED_AGE_MS: i64 = 5000;

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

    /// Enable hybrid mode: stream crypto (short-term) + event markets (long-term)
    #[arg(long)]
    hybrid: bool,

    /// Hours until expiry for crypto markets in hybrid mode (default: 12)
    #[arg(long, default_value = "12")]
    crypto_hours: i32,

    /// Days until expiry for event markets in hybrid mode (default: 60)
    #[arg(long, default_value = "60")]
    event_days: i32,

    /// Max crypto markets in hybrid mode (default: 1500)
    #[arg(long, default_value = "1500")]
    crypto_limit: i64,

    /// Max event markets in hybrid mode (default: 1500)
    #[arg(long, default_value = "1500")]
    event_limit: i64,

    /// Reconnect interval in seconds to refresh all orderbooks (default: 20)
    /// This triggers a full reconnect to get fresh snapshots for all markets
    #[arg(long, default_value = "20")]
    reconnect_interval: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    FmtSubscriber::builder().with_max_level(Level::INFO).init();

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
    // Get active markets from database
    let markets = if args.hybrid {
        // Hybrid mode: crypto markets (short-term) + event markets (long-term)
        info!(
            "Fetching markets in HYBRID mode: crypto ({}h, max {}) + events ({}d, max {})...",
            args.crypto_hours, args.crypto_limit, args.event_days, args.event_limit
        );
        get_priority_markets_hybrid(
            db.pool(),
            args.crypto_hours,
            args.event_days,
            args.crypto_limit,
            args.event_limit,
        )
        .await?
    } else {
        // Standard mode: all markets within time window
        info!(
            "Fetching active markets (expiring within {} hours, max {})...",
            args.max_expiry_hours, args.max_markets
        );
        get_active_markets_expiring_within(db.pool(), args.max_expiry_hours, args.max_markets)
            .await?
    };

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

    // Subscribe to market updates (with concurrent read to prevent data loss)
    let buffered = clob.subscribe_with_read(&mut ws, asset_ids).await?;

    // Track orderbook state per market
    let mut orderbooks: HashMap<Uuid, MarketOrderbook> = HashMap::new();
    let mut snapshot_count = 0;

    // Process buffered messages from subscription phase (with staleness filter)
    let now = Utc::now();
    for msg in buffered {
        match msg {
            ClobMessage::Books(books) => {
                let total = books.len();
                let mut fresh_count = 0;
                let mut stale_count = 0;

                for book in books {
                    // Filter out stale buffered messages to prevent mispriced trades
                    // Also handle clock skew: negative age means server clock ahead
                    if let Some(ts) = parse_event_timestamp(&book.timestamp) {
                        let age_ms = (now - ts).num_milliseconds();
                        if age_ms < 0 || age_ms > MAX_BUFFERED_AGE_MS {
                            stale_count += 1;
                            continue;
                        }
                    }
                    fresh_count += 1;
                    process_book(
                        &book,
                        &token_to_market,
                        &mut orderbooks,
                        db,
                        &mut snapshot_count,
                        args.once,
                        markets.len(),
                    )
                    .await?;
                }

                info!(
                    "Processing buffered batch of {} book snapshots ({} fresh, {} stale filtered)",
                    total, fresh_count, stale_count
                );
            }
            ClobMessage::Book(book) => {
                // Filter stale single book messages too (with clock skew protection)
                if let Some(ts) = parse_event_timestamp(&book.timestamp) {
                    let age_ms = (now - ts).num_milliseconds();
                    if age_ms < 0 || age_ms > MAX_BUFFERED_AGE_MS {
                        continue;
                    }
                }
                process_book(
                    &book,
                    &token_to_market,
                    &mut orderbooks,
                    db,
                    &mut snapshot_count,
                    args.once,
                    markets.len(),
                )
                .await?;
            }
            _ => {}
        }
    }
    info!(
        "Processed buffered messages, snapshot_count={}",
        snapshot_count
    );
    let mut last_ping = std::time::Instant::now();
    let ping_interval = Duration::from_secs(10);
    let mut consecutive_timeouts = 0;
    let max_consecutive_timeouts = 6; // 30 seconds without data = dead connection

    // Track connection start time for periodic reconnect
    let connection_start = std::time::Instant::now();
    let reconnect_interval = Duration::from_secs(args.reconnect_interval);

    // Track message stats for periodic logging
    let mut message_count = 0u64;
    let mut last_stats_log = std::time::Instant::now();
    let stats_interval = Duration::from_secs(5);

    // Process stream messages
    loop {
        // Log stats every 5 seconds to confirm data is streaming
        if last_stats_log.elapsed() >= stats_interval {
            info!(
                "Stream stats: {} messages received, {} snapshots saved, uptime {}s",
                message_count,
                snapshot_count,
                connection_start.elapsed().as_secs()
            );
            last_stats_log = std::time::Instant::now();
        }
        // Check if it's time to reconnect for fresh orderbook snapshots
        if connection_start.elapsed() >= reconnect_interval {
            info!(
                "Reconnect interval reached ({}s). Reconnecting to refresh all orderbooks...",
                args.reconnect_interval
            );
            return Ok(());
        }
        // Send keepalive ping every 10 seconds per Polymarket docs
        if last_ping.elapsed() >= ping_interval {
            if let Err(e) = clob.send_ping(&mut ws).await {
                error!("Failed to send ping: {}. Connection likely dead.", e);
                return Err(e.into());
            }
            last_ping = std::time::Instant::now();
        }

        // Use timeout to not block forever waiting for messages
        let read_result =
            tokio::time::timeout(Duration::from_secs(5), clob.read_message(&mut ws)).await;

        match read_result {
            Ok(Ok(Some(ClobMessage::Books(books)))) => {
                consecutive_timeouts = 0; // Reset on successful message
                message_count += books.len() as u64;
                // Batch of book snapshots (initial subscription response)
                info!("Received batch of {} book snapshots", books.len());
                for book in books {
                    process_book(
                        &book,
                        &token_to_market,
                        &mut orderbooks,
                        db,
                        &mut snapshot_count,
                        args.once,
                        markets.len(),
                    )
                    .await?;
                }
                if args.once && snapshot_count >= markets.len() {
                    info!("Captured snapshots for all {} markets", markets.len());
                    return Ok(());
                }
            }
            Ok(Ok(Some(ClobMessage::Book(book)))) => {
                consecutive_timeouts = 0; // Reset on successful message
                message_count += 1;
                debug!("Received book update for asset {}", book.asset_id);
                process_book(
                    &book,
                    &token_to_market,
                    &mut orderbooks,
                    db,
                    &mut snapshot_count,
                    args.once,
                    markets.len(),
                )
                .await?;
                if args.once && snapshot_count >= markets.len() {
                    info!("Captured snapshots for all {} markets", markets.len());
                    return Ok(());
                }
            }
            Ok(Ok(Some(ClobMessage::PriceChange(pc)))) => {
                consecutive_timeouts = 0;
                message_count += 1;

                let event_ts = parse_event_timestamp(&pc.timestamp);

                for change in &pc.price_changes {
                    if let Some(&(market_id, is_yes)) = token_to_market.get(&change.asset_id) {
                        let orderbook = orderbooks.entry(market_id).or_insert_with(MarketOrderbook::new);

                        // Track old best prices to detect changes
                        let (old_best_bid, old_best_ask) = if is_yes {
                            (orderbook.yes_best_bid, orderbook.yes_best_ask)
                        } else {
                            (orderbook.no_best_bid, orderbook.no_best_ask)
                        };

                        // Apply the delta to in-memory state
                        apply_price_change(orderbook, change, is_yes);

                        // Only write to DB if best prices actually changed
                        let (new_best_bid, new_best_ask) = if is_yes {
                            (orderbook.yes_best_bid, orderbook.yes_best_ask)
                        } else {
                            (orderbook.no_best_bid, orderbook.no_best_ask)
                        };

                        if old_best_bid != new_best_bid || old_best_ask != new_best_ask {
                            if is_yes {
                                update_yes_best_prices(
                                    db.pool(),
                                    market_id,
                                    new_best_ask,
                                    new_best_bid,
                                    event_ts,
                                )
                                .await?;
                            } else {
                                update_no_best_prices(
                                    db.pool(),
                                    market_id,
                                    new_best_ask,
                                    new_best_bid,
                                    event_ts,
                                )
                                .await?;
                            }
                            debug!(
                                "Price change updated best prices for market {} ({})",
                                market_id,
                                if is_yes { "YES" } else { "NO" }
                            );
                        }
                    }
                }
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
                // Timeout - no message received
                consecutive_timeouts += 1;
                if consecutive_timeouts >= max_consecutive_timeouts {
                    error!("No data received for {} consecutive timeouts ({} seconds). Connection dead.",
                           consecutive_timeouts, consecutive_timeouts * 5);
                    return Err(anyhow::anyhow!(
                        "WebSocket connection stale - no data received"
                    ));
                }
                debug!(
                    "Read timeout ({}/{}), checking ping...",
                    consecutive_timeouts, max_consecutive_timeouts
                );
            }
        }
    }
}

/// Parse Polymarket timestamp (Unix millis as string) to DateTime<Utc>
fn parse_event_timestamp(ts: &str) -> Option<DateTime<Utc>> {
    // Polymarket sends Unix timestamp in milliseconds as a string
    ts.parse::<i64>()
        .ok()
        .and_then(|millis| DateTime::from_timestamp_millis(millis))
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

        // Validate hash against accumulated price_changes (if we have prior state)
        if !validate_book(orderbook, book, is_yes) {
            debug!(
                "Hash mismatch for market {} ({}), resetting to book snapshot",
                market_id,
                if is_yes { "YES" } else { "NO" }
            );
        }

        // Parse the event timestamp from Polymarket (more accurate than DB NOW())
        let event_ts = parse_event_timestamp(&book.timestamp);
        if let Some(ts) = event_ts {
            orderbook.event_timestamp = Some(ts);
        }

        // Update only the side that changed to prevent stale overwrites
        // Reset pending changes flag since we're syncing to authoritative book snapshot
        if is_yes {
            orderbook.yes_asks = book.asks.clone();
            orderbook.yes_bids = book.bids.clone();
            orderbook.yes_best_ask = book.best_ask();
            orderbook.yes_best_bid = book.best_bid();
            orderbook.yes_hash = Some(book.hash.clone());
            orderbook.yes_has_pending_changes = false; // Reset - synced to book

            // Save only YES side to DB with event timestamp
            let yes_asks = serde_json::to_value(&orderbook.yes_asks)?;
            let yes_bids = serde_json::to_value(&orderbook.yes_bids)?;
            common::update_yes_orderbook(
                db.pool(),
                market_id,
                orderbook.yes_best_ask,
                orderbook.yes_best_bid,
                Some(yes_asks),
                Some(yes_bids),
                event_ts,
            )
            .await?;
            *snapshot_count += 1;
        } else {
            orderbook.no_asks = book.asks.clone();
            orderbook.no_bids = book.bids.clone();
            orderbook.no_best_ask = book.best_ask();
            orderbook.no_best_bid = book.best_bid();
            orderbook.no_hash = Some(book.hash.clone());
            orderbook.no_has_pending_changes = false; // Reset - synced to book

            // Save only NO side to DB with event timestamp
            let no_asks = serde_json::to_value(&orderbook.no_asks)?;
            let no_bids = serde_json::to_value(&orderbook.no_bids)?;
            common::update_no_orderbook(
                db.pool(),
                market_id,
                orderbook.no_best_ask,
                orderbook.no_best_bid,
                Some(no_asks),
                Some(no_bids),
                event_ts,
            )
            .await?;
            *snapshot_count += 1;
        }
    }
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
    /// Timestamp from Polymarket event (more accurate than DB NOW())
    event_timestamp: Option<DateTime<Utc>>,
    /// Hash of YES orderbook for validation against price_change deltas
    yes_hash: Option<String>,
    /// Hash of NO orderbook for validation against price_change deltas
    no_hash: Option<String>,
    /// True if price_changes were applied since last YES book snapshot
    yes_has_pending_changes: bool,
    /// True if price_changes were applied since last NO book snapshot
    no_has_pending_changes: bool,
}

impl MarketOrderbook {
    fn new() -> Self {
        Self::default()
    }
}

/// Apply a price_change delta to the in-memory orderbook.
/// Updates the specific price level and best prices from the message.
fn apply_price_change(orderbook: &mut MarketOrderbook, change: &PriceChange, is_yes: bool) {
    // Parse price and size
    let price: Option<Decimal> = change.price.parse().ok();
    let size: Option<Decimal> = change.size.parse().ok();

    if let (Some(price), Some(size)) = (price, size) {
        // Determine which side to update
        let levels = match (is_yes, change.side.as_str()) {
            (true, "BUY") => &mut orderbook.yes_bids,
            (true, "SELL") => &mut orderbook.yes_asks,
            (false, "BUY") => &mut orderbook.no_bids,
            (false, "SELL") => &mut orderbook.no_asks,
            _ => return,
        };

        // Convert price to string for comparison with PriceLevel
        let price_str = price.to_string();

        if size.is_zero() {
            // Remove level
            levels.retain(|l| l.price != price_str);
        } else {
            // Update or insert level
            if let Some(level) = levels.iter_mut().find(|l| l.price == price_str) {
                level.size = size.to_string();
            } else {
                levels.push(PriceLevel {
                    price: price_str,
                    size: size.to_string(),
                });
            }
        }
    }

    // Update best prices from message (authoritative - Polymarket calculates these)
    // Mark that we have pending changes for hash validation
    if is_yes {
        orderbook.yes_best_bid = change.best_bid.as_ref().and_then(|p| p.parse().ok());
        orderbook.yes_best_ask = change.best_ask.as_ref().and_then(|p| p.parse().ok());
        orderbook.yes_hash = change.hash.clone();
        orderbook.yes_has_pending_changes = true;
    } else {
        orderbook.no_best_bid = change.best_bid.as_ref().and_then(|p| p.parse().ok());
        orderbook.no_best_ask = change.best_ask.as_ref().and_then(|p| p.parse().ok());
        orderbook.no_hash = change.hash.clone();
        orderbook.no_has_pending_changes = true;
    }
}

/// Validate that the book snapshot matches our accumulated price_change state.
/// Only validates if we have pending price_changes since the last book snapshot.
/// Returns true if valid (no pending changes, or hashes match), false if drift detected.
fn validate_book(orderbook: &MarketOrderbook, book: &BookMessage, is_yes: bool) -> bool {
    let (has_pending, expected_hash) = if is_yes {
        (orderbook.yes_has_pending_changes, &orderbook.yes_hash)
    } else {
        (orderbook.no_has_pending_changes, &orderbook.no_hash)
    };

    // Only validate if we've accumulated price_changes since last book snapshot
    if !has_pending {
        return true;
    }

    match expected_hash {
        Some(h) if h == &book.hash => true,
        Some(h) => {
            warn!(
                "Hash mismatch for asset {}: expected={} got={} (state drift detected)",
                book.asset_id, h, book.hash
            );
            false
        }
        None => true, // No prior hash to compare
    }
}
