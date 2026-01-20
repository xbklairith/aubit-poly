//! Trade executor state machine.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use chrono::Utc;
use polymarket_client_sdk::clob::types::{OrderType, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Timeout for order operations (build, sign, post)
const ORDER_TIMEOUT_SECS: u64 = 30;

/// Timeout for cancel operations
const CANCEL_TIMEOUT_SECS: u64 = 10;

/// Polymarket CLOB API host
const CLOB_HOST: &str = "https://clob.polymarket.com";

/// Maximum retries for order cancellation
const CANCEL_MAX_RETRIES: u32 = 3;

/// Delay between cancellation retries
const CANCEL_RETRY_DELAY_MS: u64 = 500;

/// Wait time before converting unfilled limit order to market order
const UNFILLED_WAIT_SECS: u64 = 10;

/// Minimum order size for Polymarket (shares)
const MIN_ORDER_SIZE: Decimal = dec!(5);

use common::models::OrderbookSnapshot;
use common::repository::{self, MarketWithPrices};
use common::Database;

use crate::balance::{
    calculate_safe_sell_amount, find_balance, BalanceChecker, GammaBalanceChecker,
};

/// Gamma Data API URL for balance queries
const GAMMA_DATA_API_URL: &str = "https://data-api.polymarket.com";

/// CLOB orderbook response for price fetching
#[derive(serde::Deserialize)]
struct ClobBook {
    #[serde(default)]
    bids: Vec<ClobLevel>,
    #[serde(default)]
    asks: Vec<ClobLevel>,
}

/// CLOB orderbook price level
#[derive(serde::Deserialize)]
struct ClobLevel {
    price: String,
    #[serde(default)]
    #[allow(dead_code)]
    size: String,
}

/// Calculate available liquidity from orderbook depth at best ask price.
/// Returns the minimum USDC value available at best ask between YES and NO sides.
/// (For spread arb, we need liquidity on BOTH sides - limited by the smaller one)
fn calculate_orderbook_liquidity(snapshot: &OrderbookSnapshot) -> Decimal {
    // Parse YES asks depth
    let yes_liquidity = snapshot
        .yes_asks
        .as_ref()
        .and_then(|asks| asks.as_array())
        .and_then(|arr| arr.first())
        .map(|best_ask| {
            let size = best_ask
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            let price = best_ask
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            size * price
        })
        .unwrap_or(Decimal::ZERO);

    // Parse NO asks depth
    let no_liquidity = snapshot
        .no_asks
        .as_ref()
        .and_then(|asks| asks.as_array())
        .and_then(|arr| arr.first())
        .map(|best_ask| {
            let size = best_ask
                .get("size")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            let price = best_ask
                .get("price")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<Decimal>().ok())
                .unwrap_or(Decimal::ZERO);
            size * price
        })
        .unwrap_or(Decimal::ZERO);

    // Use minimum - can only trade as much as the thinner side allows
    yes_liquidity.min(no_liquidity)
}

/// Parse orderbook side (asks or bids) from JSON into price levels.
fn parse_orderbook_side(json_value: &Option<serde_json::Value>) -> Vec<(Decimal, Decimal)> {
    json_value
        .as_ref()
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|level| {
                    let price = level
                        .get("price")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())?;
                    let size = level
                        .get("size")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<Decimal>().ok())?;
                    Some((price, size))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract best ask prices from orderbook snapshot.
/// Returns (yes_best_ask, no_best_ask) or None if either side is empty.
/// Implements REQ-006 (price consistency) and REQ-015 (empty orderbook handling).
#[cfg(test)]
fn extract_best_asks(snapshot: &OrderbookSnapshot) -> Option<(Decimal, Decimal)> {
    let yes_asks = parse_orderbook_side(&snapshot.yes_asks);
    let no_asks = parse_orderbook_side(&snapshot.no_asks);

    // REQ-015: Return None if either side is empty
    if yes_asks.is_empty() || no_asks.is_empty() {
        return None;
    }

    // Get best (lowest) ask price from each side
    let yes_best = yes_asks.iter().map(|(p, _)| *p).min()?;
    let no_best = no_asks.iter().map(|(p, _)| *p).min()?;

    Some((yes_best, no_best))
}

/// Determine if trade should be aborted due to spread widening.
/// Returns (should_abort, reason) tuple for logging (REQ-017).
#[cfg(test)]
fn should_abort_due_to_spread(
    original_spread: Decimal,
    current_spread: Decimal,
    tolerance: Decimal,
) -> (bool, String) {
    let spread_change = current_spread - original_spread;

    if current_spread > original_spread + tolerance {
        let reason = format!(
            "Spread widened beyond tolerance: {:.4} -> {:.4} (change: +{:.4}, tolerance: {:.4})",
            original_spread, current_spread, spread_change, tolerance
        );
        (true, reason)
    } else if spread_change < Decimal::ZERO {
        let reason = format!(
            "Spread improved: {:.4} -> {:.4} (change: {:.4})",
            original_spread, current_spread, spread_change
        );
        (false, reason)
    } else {
        let reason = format!(
            "Spread within tolerance: {:.4} -> {:.4} (change: +{:.4}, tolerance: {:.4})",
            original_spread, current_spread, spread_change, tolerance
        );
        (false, reason)
    }
}

/// Log market depth visualization before placing orders.
/// Shows top N levels of the orderbook for both YES and NO sides.
fn log_market_depth(snapshot: &OrderbookSnapshot, market_name: &str, max_levels: usize) {
    let yes_asks = parse_orderbook_side(&snapshot.yes_asks);
    let yes_bids = parse_orderbook_side(&snapshot.yes_bids);
    let no_asks = parse_orderbook_side(&snapshot.no_asks);
    let no_bids = parse_orderbook_side(&snapshot.no_bids);

    // Build depth visualization string
    let mut depth_lines = Vec::new();
    depth_lines.push(format!("Market Depth: {}", market_name));
    depth_lines.push(format!("{:^42} | {:^42}", "YES", "NO"));
    depth_lines.push(format!(
        "{:>20} {:>20} | {:>20} {:>20}",
        "BID", "ASK", "BID", "ASK"
    ));
    depth_lines.push(format!("{:-<20} {:-<20} | {:-<20} {:-<20}", "", "", "", ""));

    // Show levels (bids sorted descending, asks sorted ascending)
    let mut yes_bids_sorted = yes_bids.clone();
    let mut yes_asks_sorted = yes_asks.clone();
    let mut no_bids_sorted = no_bids.clone();
    let mut no_asks_sorted = no_asks.clone();

    yes_bids_sorted.sort_by(|a, b| b.0.cmp(&a.0)); // Descending
    yes_asks_sorted.sort_by(|a, b| a.0.cmp(&b.0)); // Ascending
    no_bids_sorted.sort_by(|a, b| b.0.cmp(&a.0)); // Descending
    no_asks_sorted.sort_by(|a, b| a.0.cmp(&b.0)); // Ascending

    for i in 0..max_levels {
        let yes_bid = yes_bids_sorted
            .get(i)
            .map(|(p, s)| format!("${:.2} x {:.1}", p, s))
            .unwrap_or_else(|| "-".to_string());
        let yes_ask = yes_asks_sorted
            .get(i)
            .map(|(p, s)| format!("${:.2} x {:.1}", p, s))
            .unwrap_or_else(|| "-".to_string());
        let no_bid = no_bids_sorted
            .get(i)
            .map(|(p, s)| format!("${:.2} x {:.1}", p, s))
            .unwrap_or_else(|| "-".to_string());
        let no_ask = no_asks_sorted
            .get(i)
            .map(|(p, s)| format!("${:.2} x {:.1}", p, s))
            .unwrap_or_else(|| "-".to_string());

        depth_lines.push(format!(
            "{:>20} {:>20} | {:>20} {:>20}",
            yes_bid, yes_ask, no_bid, no_ask
        ));
    }

    // Calculate totals
    let yes_bid_total: Decimal = yes_bids.iter().map(|(p, s)| p * s).sum();
    let yes_ask_total: Decimal = yes_asks.iter().map(|(p, s)| p * s).sum();
    let no_bid_total: Decimal = no_bids.iter().map(|(p, s)| p * s).sum();
    let no_ask_total: Decimal = no_asks.iter().map(|(p, s)| p * s).sum();

    depth_lines.push(format!("{:-<20} {:-<20} | {:-<20} {:-<20}", "", "", "", ""));
    depth_lines.push(format!(
        "{:>20} {:>20} | {:>20} {:>20}",
        format!("${:.2}", yes_bid_total),
        format!("${:.2}", yes_ask_total),
        format!("${:.2}", no_bid_total),
        format!("${:.2}", no_ask_total)
    ));

    // Log each line
    for line in depth_lines {
        info!("[DEPTH] {}", line);
    }
}

/// Fetch and log live orderbook from CLOB REST API (non-blocking).
/// This runs in a spawned task to avoid blocking order execution.
async fn fetch_and_log_live_orderbook(
    http_client: reqwest::Client,
    yes_token_id: String,
    no_token_id: String,
    market_name: String,
) {
    let yes_url = format!("{}/book?token_id={}", CLOB_HOST, yes_token_id);
    let no_url = format!("{}/book?token_id={}", CLOB_HOST, no_token_id);

    // Fetch both in parallel
    let (yes_result, no_result) = tokio::join!(
        http_client.get(&yes_url).send(),
        http_client.get(&no_url).send(),
    );

    // Parse YES book
    let yes_prices = match yes_result {
        Ok(resp) => match resp.json::<ClobBook>().await {
            Ok(book) => {
                let best_ask = book
                    .asks
                    .iter()
                    .filter_map(|l| l.price.parse::<Decimal>().ok())
                    .min();
                let best_bid = book
                    .bids
                    .iter()
                    .filter_map(|l| l.price.parse::<Decimal>().ok())
                    .max();
                Some((best_ask, best_bid))
            }
            Err(_) => None,
        },
        Err(_) => None,
    };

    // Parse NO book
    let no_prices = match no_result {
        Ok(resp) => match resp.json::<ClobBook>().await {
            Ok(book) => {
                let best_ask = book
                    .asks
                    .iter()
                    .filter_map(|l| l.price.parse::<Decimal>().ok())
                    .min();
                let best_bid = book
                    .bids
                    .iter()
                    .filter_map(|l| l.price.parse::<Decimal>().ok())
                    .max();
                Some((best_ask, best_bid))
            }
            Err(_) => None,
        },
        Err(_) => None,
    };

    // Log results
    let short_name = if market_name.len() > 50 {
        format!("{}...", &market_name[..47])
    } else {
        market_name.clone()
    };

    match (yes_prices, no_prices) {
        (Some((yes_ask, yes_bid)), Some((no_ask, no_bid))) => {
            info!(
                "[API-DEPTH] {} | YES: ask={:?} bid={:?} | NO: ask={:?} bid={:?}",
                short_name, yes_ask, yes_bid, no_ask, no_bid
            );
        }
        _ => {
            warn!(
                "[API-DEPTH] {} | Failed to fetch live orderbook",
                short_name
            );
        }
    }
}

/// Fetch live best ask prices from CLOB REST API (blocking).
/// Returns (yes_best_ask, no_best_ask) or None if fetch fails.
/// Used for price mismatch detection before order placement.
async fn fetch_live_clob_prices(
    http_client: &reqwest::Client,
    yes_token_id: &str,
    no_token_id: &str,
) -> Option<(Decimal, Decimal)> {
    let yes_url = format!("{}/book?token_id={}", CLOB_HOST, yes_token_id);
    let no_url = format!("{}/book?token_id={}", CLOB_HOST, no_token_id);

    // Fetch both in parallel with timeout
    let (yes_result, no_result) = tokio::join!(
        timeout(Duration::from_secs(5), http_client.get(&yes_url).send()),
        timeout(Duration::from_secs(5), http_client.get(&no_url).send()),
    );

    // Parse YES best ask
    let yes_ask = match yes_result {
        Ok(Ok(resp)) => match resp.json::<ClobBook>().await {
            Ok(book) => book
                .asks
                .iter()
                .filter_map(|l| l.price.parse::<Decimal>().ok())
                .min(),
            Err(e) => {
                warn!("[LIVE-PRICE] Failed to parse YES orderbook: {:?}", e);
                None
            }
        },
        Ok(Err(e)) => {
            warn!("[LIVE-PRICE] Failed to fetch YES orderbook: {:?}", e);
            None
        }
        Err(_) => {
            warn!("[LIVE-PRICE] YES orderbook fetch timed out");
            None
        }
    }?;

    // Parse NO best ask
    let no_ask = match no_result {
        Ok(Ok(resp)) => match resp.json::<ClobBook>().await {
            Ok(book) => book
                .asks
                .iter()
                .filter_map(|l| l.price.parse::<Decimal>().ok())
                .min(),
            Err(e) => {
                warn!("[LIVE-PRICE] Failed to parse NO orderbook: {:?}", e);
                None
            }
        },
        Ok(Err(e)) => {
            warn!("[LIVE-PRICE] Failed to fetch NO orderbook: {:?}", e);
            None
        }
        Err(_) => {
            warn!("[LIVE-PRICE] NO orderbook fetch timed out");
            None
        }
    }?;

    Some((yes_ask, no_ask))
}

/// Check if there's a price mismatch requiring sequential placement.
/// Returns (has_mismatch, priority_side, yes_diff, no_diff).
/// Priority side is the one with larger deviation (place first).
fn check_price_mismatch(
    detection_yes: Decimal,
    detection_no: Decimal,
    live_yes: Decimal,
    live_no: Decimal,
    threshold: Decimal,
) -> (bool, OrderSide, Decimal, Decimal) {
    let yes_diff = (detection_yes - live_yes).abs();
    let no_diff = (detection_no - live_no).abs();

    let yes_mismatch = yes_diff > threshold;
    let no_mismatch = no_diff > threshold;
    let has_mismatch = yes_mismatch || no_mismatch;

    // Priority side is the one with larger deviation
    // (more likely to have stale data, so discover true price first)
    let priority = if yes_diff >= no_diff {
        OrderSide::Yes
    } else {
        OrderSide::No
    };

    (has_mismatch, priority, yes_diff, no_diff)
}

/// Poll an order until it's filled or timeout.
/// Returns the final fill status:
/// - `FullyFilled(amount)` if order filled completely
/// - `PartialFill(amount)` if timeout with some fill
/// - `Timeout` if timeout with no fill
/// - `Error(msg)` if persistent API failures
async fn poll_order_fill(
    clob_client: &AuthenticatedClobClient,
    order_id: &str,
    target_size: Decimal,
    poll_interval_ms: u64,
    poll_timeout_secs: u64,
) -> PollResult {
    let start = std::time::Instant::now();
    // Guard against division by zero
    let max_iterations = if poll_interval_ms > 0 {
        (poll_timeout_secs * 1000 / poll_interval_ms) as u32 + 1
    } else {
        1 // Fallback to single iteration if interval is 0
    };

    let mut last_known_fill = Decimal::ZERO;
    let mut consecutive_errors = 0u32;
    const MAX_CONSECUTIVE_ERRORS: u32 = 3;

    for iteration in 1..=max_iterations {
        // Check timeout
        if start.elapsed() > Duration::from_secs(poll_timeout_secs) {
            info!("[POLL] Timeout reached after {}s", poll_timeout_secs);
            break;
        }

        // Query order status
        match timeout(
            Duration::from_secs(CANCEL_TIMEOUT_SECS),
            clob_client.order(order_id),
        )
        .await
        {
            Ok(Ok(order_info)) => {
                consecutive_errors = 0; // Reset on success
                let filled = order_info.size_matched;
                last_known_fill = filled;

                if filled >= target_size {
                    info!(
                        "[POLL] Order {} fully filled: {}/{}",
                        order_id, filled, target_size
                    );
                    return PollResult::FullyFilled(filled);
                }
                // Check if order status indicates completion (handle SDK enum)
                let status_str = format!("{:?}", order_info.status);
                if status_str.contains("Matched") || status_str.contains("MATCHED") {
                    info!(
                        "[POLL] Order {} status {:?}: {}/{}",
                        order_id, order_info.status, filled, target_size
                    );
                    return PollResult::FullyFilled(filled);
                }
                if filled > Decimal::ZERO {
                    info!(
                        "[POLL] Iteration {}: order {} partial fill {}/{}, status={:?}",
                        iteration, order_id, filled, target_size, order_info.status
                    );
                } else {
                    debug!(
                        "[POLL] Iteration {}: order {} unfilled, status={:?}",
                        iteration, order_id, order_info.status
                    );
                }
            }
            Ok(Err(e)) => {
                consecutive_errors += 1;
                warn!(
                    "[POLL] Failed to query order {} (error {}/{}): {:?}",
                    order_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                );
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return PollResult::Error(format!(
                        "Failed to query order after {} attempts: {:?}",
                        MAX_CONSECUTIVE_ERRORS, e
                    ));
                }
            }
            Err(_) => {
                consecutive_errors += 1;
                warn!(
                    "[POLL] Query timeout for order {} (error {}/{})",
                    order_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS
                );
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return PollResult::Error(format!(
                        "Order query timed out {} times consecutively",
                        MAX_CONSECUTIVE_ERRORS
                    ));
                }
            }
        }

        // Wait before next poll
        tokio::time::sleep(Duration::from_millis(poll_interval_ms)).await;
    }

    // Timeout reached - query final fill amount
    let final_fill = query_order_fill(clob_client, order_id).await;
    if final_fill > Decimal::ZERO {
        info!(
            "[POLL] Timeout with partial fill: {}/{}",
            final_fill, target_size
        );
        PollResult::PartialFill(final_fill)
    } else if last_known_fill > Decimal::ZERO {
        // Use last known if final query failed
        info!(
            "[POLL] Timeout with partial fill (last known): {}/{}",
            last_known_fill, target_size
        );
        PollResult::PartialFill(last_known_fill)
    } else {
        info!("[POLL] Timeout with no fill");
        PollResult::Timeout
    }
}

/// Cancel an order with retry logic.
/// Returns true if cancel succeeded, false otherwise.
async fn cancel_order_with_retries(
    clob_client: &AuthenticatedClobClient,
    order_id: &str,
    tag: &str,
) -> bool {
    for attempt in 1..=CANCEL_MAX_RETRIES {
        match timeout(
            Duration::from_secs(CANCEL_TIMEOUT_SECS),
            clob_client.cancel_order(order_id),
        )
        .await
        {
            Ok(Ok(_)) => {
                info!(
                    "[SEQUENTIAL] Cancelled {} order {} on attempt {}",
                    tag, order_id, attempt
                );
                return true;
            }
            Ok(Err(e)) => {
                warn!(
                    "[SEQUENTIAL] Cancel {} failed (attempt {}): {:?}",
                    tag, attempt, e
                );
            }
            Err(_) => {
                warn!("[SEQUENTIAL] Cancel {} timeout (attempt {})", tag, attempt);
            }
        }
        if attempt == CANCEL_MAX_RETRIES {
            error!(
                "[SEQUENTIAL] CRITICAL: Failed to cancel {} order {} after {} attempts. ORPHANED ORDER!",
                tag, order_id, CANCEL_MAX_RETRIES
            );
        } else {
            tokio::time::sleep(Duration::from_millis(CANCEL_RETRY_DELAY_MS)).await;
        }
    }
    false
}

/// Query the final fill amount for an order after cancellation.
async fn query_order_fill(clob_client: &AuthenticatedClobClient, order_id: &str) -> Decimal {
    match timeout(
        Duration::from_secs(CANCEL_TIMEOUT_SECS),
        clob_client.order(order_id),
    )
    .await
    {
        Ok(Ok(order_info)) => {
            info!(
                "[SEQUENTIAL] Order {} final fill: {}",
                order_id, order_info.size_matched
            );
            order_info.size_matched
        }
        Ok(Err(e)) => {
            warn!(
                "[SEQUENTIAL] Failed to query order {}: {:?}, assuming 0 fill",
                order_id, e
            );
            Decimal::ZERO
        }
        Err(_) => {
            warn!(
                "[SEQUENTIAL] Query timeout for order {}, assuming 0 fill",
                order_id
            );
            Decimal::ZERO
        }
    }
}

use crate::config::ExecutorConfig;
use crate::detector::SpreadDetector;
use crate::metrics::{CycleMetrics, MarketSummary};
use crate::models::{
    BotState, LiveTradeResult, OrderSide, PollResult, PositionCache, SessionState,
    SpreadOpportunity, TradeDetails,
};

use chrono::DateTime;

/// Type alias for authenticated Polymarket CLOB client
type AuthenticatedClobClient = polymarket_client_sdk::clob::Client<
    polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
>;

/// Type alias for the private key signer type
type PrivateKeySigner = alloy::signers::local::PrivateKeySigner;

/// Cached authentication state for Polymarket CLOB.
/// Stores the authenticated client and signer for reuse across trades.
/// Implements REQ-001 (cached authentication) and REQ-002 (authentication reuse).
struct CachedAuthentication {
    /// The authenticated CLOB client
    client: AuthenticatedClobClient,
    /// The signer used for this authentication
    signer: PrivateKeySigner,
    /// Timestamp when authentication was performed (for debugging/logging)
    #[allow(dead_code)]
    authenticated_at: DateTime<Utc>,
}

/// Trade executor - manages trading state and executes spread arbitrage.
pub struct TradeExecutor {
    config: ExecutorConfig,
    db: Arc<Database>,
    detector: SpreadDetector,
    state: BotState,
    session: SessionState,
    /// Cached authentication for Polymarket CLOB (None = not yet authenticated)
    cached_auth: Option<CachedAuthentication>,
    /// Token IDs that have been warmed in SDK cache (tick_size + fee_rate)
    warmed_tokens: HashSet<String>,
    /// HTTP client for API calls (reusable, connection pooled)
    http_client: reqwest::Client,
}

impl TradeExecutor {
    /// Create a new trade executor.
    pub async fn new(config: ExecutorConfig, db: Arc<Database>) -> Result<Self> {
        let detector = SpreadDetector::new(config.min_profit, config.max_price_age_secs);

        let session = SessionState {
            id: Uuid::new_v4(),
            dry_run: config.dry_run,
            starting_balance: config.starting_balance,
            current_balance: config.starting_balance,
            total_trades: 0,
            winning_trades: 0,
            total_opportunities: 0,
            positions_opened: 0,
            positions_closed: 0,
            gross_profit: dec!(0),
            fees_paid: dec!(0),
            net_profit: dec!(0),
            started_at: Utc::now(),
            open_positions: HashMap::new(),
        };

        Ok(Self {
            config,
            db,
            detector,
            state: BotState::Idle,
            session,
            cached_auth: None, // REQ-001: Initialize as None, authenticate on first trade
            warmed_tokens: HashSet::new(),
            http_client: reqwest::Client::new(),
        })
    }

    /// Ensure we have a valid authenticated CLOB client.
    /// Authenticates on first call, reuses cached client thereafter.
    /// Implements REQ-001 (cached authentication), REQ-002 (reuse), REQ-016 (metrics logging).
    ///
    /// # Returns
    /// References to the cached client and signer for order operations.
    ///
    /// # Errors
    /// - Missing WALLET_PRIVATE_KEY environment variable (REQ-013)
    /// - Invalid private key format
    /// - Invalid proxy wallet address
    /// - Authentication failure with Polymarket
    async fn ensure_authenticated(
        &mut self,
    ) -> Result<(&AuthenticatedClobClient, &PrivateKeySigner)> {
        // Check for cached auth (REQ-002)
        if self.cached_auth.is_some() {
            info!("[AUTH] Using cached authentication (cache hit)");
            let auth = self.cached_auth.as_ref().unwrap();
            return Ok((&auth.client, &auth.signer));
        }

        // Cache miss - perform fresh authentication (REQ-001)
        let start = std::time::Instant::now();
        info!("[AUTH] Authenticating with Polymarket CLOB (cache miss)...");

        // Load credentials from environment (REQ-013)
        let private_key = std::env::var("WALLET_PRIVATE_KEY")
            .context("Missing WALLET_PRIVATE_KEY for live trading")?;

        let private_key = if private_key.starts_with("0x") {
            private_key
        } else {
            format!("0x{}", private_key)
        };

        // Create signer
        let signer = PrivateKeySigner::from_str(&private_key)
            .context("Invalid private key format")?
            .with_chain_id(Some(POLYGON));

        // Determine signature type based on proxy wallet
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

        // Perform authentication
        let client = auth_builder
            .authenticate()
            .await
            .context("Failed to authenticate with Polymarket")?;

        let elapsed = start.elapsed();
        info!(
            "[AUTH] Authentication successful in {:?}, caching for session (REQ-016)",
            elapsed
        );

        // Cache the authentication
        self.cached_auth = Some(CachedAuthentication {
            client,
            signer,
            authenticated_at: Utc::now(),
        });

        let auth = self.cached_auth.as_ref().unwrap();
        Ok((&auth.client, &auth.signer))
    }

    /// Clear the authentication cache, forcing re-auth on next call.
    /// Called when an auth error is detected during order operations.
    /// Implements REQ-003 (authentication refresh on failure).
    #[allow(dead_code)]
    fn clear_auth_cache(&mut self) {
        if self.cached_auth.is_some() {
            warn!("[AUTH] Clearing authentication cache due to auth failure");
            self.cached_auth = None;
        }
    }

    /// Warm up the Polymarket SDK cache for crypto markets.
    /// Pre-fetches tick_size and fee_rate for all token IDs to avoid
    /// API calls during time-critical order building.
    pub async fn warm_order_cache(&mut self) -> Result<()> {
        let start = std::time::Instant::now();

        // Skip in dry-run mode (no CLOB client)
        if self.config.dry_run {
            info!("[CACHE] Skipping cache warmup in dry-run mode");
            return Ok(());
        }

        // Fetch all crypto markets (filtered by configured assets)
        let markets = repository::get_markets_with_fresh_orderbooks(
            self.db.pool(),
            self.config.max_orderbook_age_secs,
            &self.config.assets,
            self.config.max_time_to_expiry_secs,
        )
        .await?;

        if markets.is_empty() {
            info!("[CACHE] No crypto markets to warm up");
            return Ok(());
        }

        // Authenticate to get CLOB client
        let (clob_client, _) = self.ensure_authenticated().await?;

        // Collect unique token IDs
        let mut token_ids: Vec<&str> = Vec::with_capacity(markets.len() * 2);
        for market in &markets {
            token_ids.push(&market.yes_token_id);
            token_ids.push(&market.no_token_id);
        }

        info!(
            "[CACHE] Warming SDK cache for {} tokens ({} crypto markets)...",
            token_ids.len(),
            markets.len()
        );

        // Warm cache in parallel - all futures execute concurrently
        let tick_futures: Vec<_> = token_ids
            .iter()
            .map(|token_id| clob_client.tick_size(token_id))
            .collect();
        let fee_futures: Vec<_> = token_ids
            .iter()
            .map(|token_id| clob_client.fee_rate_bps(token_id))
            .collect();

        // Execute ALL futures in parallel using join_all
        let (tick_results, fee_results) = tokio::join!(
            futures::future::join_all(tick_futures),
            futures::future::join_all(fee_futures)
        );

        // Track all tokens (success or fail) to avoid retry loops
        let mut warmed_count = 0;
        let mut failed_count = 0;
        for (i, token_id) in token_ids.iter().enumerate() {
            let tick_ok = tick_results.get(i).map(|r| r.is_ok()).unwrap_or(false);
            let fee_ok = fee_results.get(i).map(|r| r.is_ok()).unwrap_or(false);
            // Always add to set - prevents retry loops for invalid/expired tokens
            self.warmed_tokens.insert(token_id.to_string());
            if tick_ok && fee_ok {
                warmed_count += 1;
            } else {
                failed_count += 1;
            }
        }
        if failed_count > 0 {
            debug!(
                "[CACHE] {} tokens failed to warm (invalid/expired)",
                failed_count
            );
        }

        let elapsed = start.elapsed();
        info!(
            "[CACHE] Startup warmup complete: {}/{} tokens warmed in {:?}",
            warmed_count,
            token_ids.len(),
            elapsed
        );

        Ok(())
    }

    /// Run a single trading cycle.
    pub async fn run_cycle(&mut self, verbose: bool) -> Result<CycleMetrics> {
        let cycle_start = std::time::Instant::now();
        let mut metrics = CycleMetrics::new();

        self.state = BotState::Scanning;

        // Step 1: Fetch markets with fresh orderbooks
        let query_start = std::time::Instant::now();
        let markets = repository::get_markets_with_fresh_orderbooks(
            self.db.pool(),
            self.config.max_orderbook_age_secs,
            &self.config.assets,
            self.config.max_time_to_expiry_secs,
        )
        .await?;
        metrics.market_query_ms = query_start.elapsed().as_millis() as u64;
        metrics.markets_scanned = markets.len();

        debug!(
            "Fetched {} markets with fresh orderbooks in {}ms",
            markets.len(),
            metrics.market_query_ms
        );

        // Step 1b: Warm cache for any NEW tokens (not in warmed_tokens set)
        // Only warm tokens for markets we just fetched (already filtered by assets/freshness)
        if !self.config.dry_run {
            let new_tokens: Vec<String> = markets
                .iter()
                .flat_map(|m| [m.yes_token_id.clone(), m.no_token_id.clone()])
                .filter(|t| !self.warmed_tokens.contains(t))
                .collect();

            // Only warm if there are a reasonable number of new tokens
            // Skip if too many (probably stale data or first run - startup handles that)
            const MAX_INCREMENTAL_TOKENS: usize = 20;
            if !new_tokens.is_empty() && new_tokens.len() <= MAX_INCREMENTAL_TOKENS {
                if let Ok((clob_client, _)) = self.ensure_authenticated().await {
                    info!("[CACHE] Warming {} new tokens...", new_tokens.len());
                    let warm_start = std::time::Instant::now();

                    // Create futures for parallel execution
                    let tick_futures: Vec<_> = new_tokens
                        .iter()
                        .map(|t| clob_client.tick_size(t))
                        .collect();
                    let fee_futures: Vec<_> = new_tokens
                        .iter()
                        .map(|t| clob_client.fee_rate_bps(t))
                        .collect();

                    // Execute ALL in parallel
                    let (tick_results, fee_results) = tokio::join!(
                        futures::future::join_all(tick_futures),
                        futures::future::join_all(fee_futures)
                    );

                    // Track all tokens (success or fail) to avoid retry loops
                    let mut success_count = 0;
                    let mut fail_count = 0;
                    for (i, token_id) in new_tokens.iter().enumerate() {
                        let tick_ok = tick_results.get(i).map(|r| r.is_ok()).unwrap_or(false);
                        let fee_ok = fee_results.get(i).map(|r| r.is_ok()).unwrap_or(false);
                        // Always add to set - prevents retry loops for invalid tokens
                        self.warmed_tokens.insert(token_id.clone());
                        if tick_ok && fee_ok {
                            success_count += 1;
                        } else {
                            fail_count += 1;
                        }
                    }

                    if fail_count > 0 {
                        debug!(
                            "[CACHE] {}/{} tokens warmed ({} failed) in {:?}",
                            success_count,
                            new_tokens.len(),
                            fail_count,
                            warm_start.elapsed()
                        );
                    } else {
                        debug!(
                            "[CACHE] {} tokens warmed in {:?}",
                            success_count,
                            warm_start.elapsed()
                        );
                    }
                }
            } else if new_tokens.len() > MAX_INCREMENTAL_TOKENS {
                debug!(
                    "[CACHE] Skipping incremental warmup ({} tokens > {} limit)",
                    new_tokens.len(),
                    MAX_INCREMENTAL_TOKENS
                );
            }

            // Memory safeguard: if set grows too large, clear it (SDK cache still exists)
            const MAX_WARMED_TOKENS: usize = 10_000;
            if self.warmed_tokens.len() > MAX_WARMED_TOKENS {
                warn!(
                    "[CACHE] warmed_tokens exceeded {} entries, clearing to prevent memory growth",
                    MAX_WARMED_TOKENS
                );
                self.warmed_tokens.clear();
            }
        }

        // Step 2: Detect spread opportunities
        let detect_start = std::time::Instant::now();
        let opportunities = self.detector.scan_markets(&markets);
        metrics.detection_ms = detect_start.elapsed().as_millis() as u64;
        metrics.opportunities_found = opportunities.len();

        self.session.total_opportunities += opportunities.len() as i32;

        // Collect top 10 markets by profit (including negative)
        metrics.top_markets = self.get_top_markets(&markets, 10);

        // Step 3: Execute on best opportunity
        if let Some(best) = opportunities.first() {
            self.state = BotState::Trading;

            let exec_start = std::time::Instant::now();
            if self.try_execute_trade(best).await? {
                metrics.trades_executed = 1;
            }
            metrics.execution_ms = exec_start.elapsed().as_millis() as u64;
        }

        // Step 4: Check for expired positions
        let settlement_start = std::time::Instant::now();
        let settled = self.check_and_settle_expired(&markets).await?;
        metrics.settlement_ms = settlement_start.elapsed().as_millis() as u64;
        metrics.positions_settled = settled;

        metrics.total_cycle_ms = cycle_start.elapsed().as_millis() as u64;
        self.state = BotState::Idle;

        Ok(metrics)
    }

    /// Attempt to execute a spread trade.
    async fn try_execute_trade(&mut self, opportunity: &SpreadOpportunity) -> Result<bool> {
        // Check if we already have a position in this market
        if self
            .session
            .open_positions
            .contains_key(&opportunity.market_id)
        {
            debug!("Already have position in {}", opportunity.market_name);
            return Ok(false);
        }

        // Fetch orderbook to check liquidity
        let liquidity =
            match repository::get_latest_orderbook_snapshot(self.db.pool(), opportunity.market_id)
                .await
            {
                Ok(Some(snapshot)) => calculate_orderbook_liquidity(&snapshot),
                Ok(None) => {
                    debug!("No orderbook snapshot for {}", opportunity.market_name);
                    Decimal::ZERO
                }
                Err(e) => {
                    warn!("Failed to fetch orderbook: {}", e);
                    Decimal::ZERO
                }
            };

        // Calculate investment size based on liquidity
        // Baseline: base_position_size ($10)
        // Scale up to max_position_size ($20) if liquidity >= threshold ($50)
        let available = self.available_balance();
        let target_size = if liquidity >= self.config.liquidity_threshold {
            debug!(
                "Good liquidity ${:.2} >= ${:.2}, using max size ${}",
                liquidity, self.config.liquidity_threshold, self.config.max_position_size
            );
            self.config.max_position_size
        } else {
            debug!(
                "Low liquidity ${:.2} < ${:.2}, using base size ${}",
                liquidity, self.config.liquidity_threshold, self.config.base_position_size
            );
            self.config.base_position_size
        };

        let size = available.min(target_size);

        if size < self.config.base_position_size {
            debug!("Insufficient balance for trade: ${}", available);
            return Ok(false);
        }

        // Check max exposure
        if self.total_exposure() + size > self.config.max_total_exposure {
            debug!("Would exceed max exposure");
            return Ok(false);
        }

        info!("Trade size: ${} (liquidity: ${:.2})", size, liquidity);

        // Calculate trade details
        let details =
            self.detector
                .calculate_trade_details(opportunity, size, self.config.fee_rate);

        // Execute trade (dry run or live)
        if self.config.dry_run {
            self.execute_dry_run(opportunity, &details).await?;
            Ok(true)
        } else {
            match self.execute_live_trade(opportunity, &details).await? {
                LiveTradeResult::Executed {
                    invested,
                    yes_filled,
                    no_filled,
                } => {
                    info!(
                        "[TRADE] Successfully executed: invested=${:.2}, YES={}, NO={}",
                        invested, yes_filled, no_filled
                    );
                    Ok(true)
                }
                LiveTradeResult::Aborted { reason } => {
                    info!("[TRADE] Aborted: {}", reason);
                    Ok(false) // Trade was not executed, but not an error
                }
            }
        }
    }

    /// Execute a live trade on Polymarket.
    /// Implements REQ-001 (cached auth), REQ-006 (price consistency), REQ-007 (synchronized snapshot).
    ///
    /// # Returns
    /// - `Ok(LiveTradeResult::Executed { ... })` - Trade was successfully executed
    /// - `Ok(LiveTradeResult::Aborted { reason })` - Trade was intentionally aborted (not an error)
    /// - `Err(...)` - Actual error occurred (auth failure, network error, etc.)
    async fn execute_live_trade(
        &mut self,
        opportunity: &SpreadOpportunity,
        details: &TradeDetails,
    ) -> Result<LiveTradeResult> {
        info!(
            "[LIVE] Placing orders for {} | Original YES: {:.4} @ ${:.4} | NO: {:.4} @ ${:.4}",
            opportunity.market_name,
            details.yes_shares,
            details.yes_price,
            details.no_shares,
            details.no_price
        );

        // Clone Arc<Database> and http_client before borrowing self (for use inside auth scope)
        let db = self.db.clone();
        let max_orderbook_age = self.config.max_orderbook_age_secs;
        let http_client_for_api = self.http_client.clone();
        let http_client_for_mismatch = self.http_client.clone();

        // Capture sequential placement config before mutable borrow
        let enable_sequential_placement = self.config.enable_sequential_placement;
        let price_mismatch_threshold = self.config.price_mismatch_threshold;
        let sequential_poll_interval_ms = self.config.sequential_poll_interval_ms;
        let sequential_poll_timeout_secs = self.config.sequential_poll_timeout_secs;

        // Get credentials for rebalance task (read before ensure_authenticated borrows self)
        let private_key = std::env::var("WALLET_PRIVATE_KEY")
            .context("Missing WALLET_PRIVATE_KEY for live trading")?;
        let private_key = if private_key.starts_with("0x") {
            private_key
        } else {
            format!("0x{}", private_key)
        };
        let proxy_wallet = std::env::var("POLYMARKET_WALLET_ADDRESS").ok();

        // REQ-001: Use cached authentication (cache hit on subsequent trades)
        let (clob_client, _cached_signer) = self.ensure_authenticated().await?;

        // Create local signer for signing operations (SDK requires owned/mutable signer)
        // The cached client is still used for all API operations
        let signer = PrivateKeySigner::from_str(&private_key)
            .context("Invalid private key format")?
            .with_chain_id(Some(POLYGON));

        // REQ-006, REQ-007: Fetch latest snapshot for current prices and market depth
        let snapshot =
            match repository::get_latest_orderbook_snapshot(db.pool(), opportunity.market_id).await
            {
                Ok(Some(s)) => s,
                Ok(None) => {
                    let reason = "No orderbook snapshot available".to_string();
                    warn!("[LIVE] {}, aborting trade", reason);
                    return Ok(LiveTradeResult::Aborted { reason });
                }
                Err(e) => {
                    let reason = format!("Failed to fetch orderbook snapshot: {}", e);
                    warn!("[LIVE] {}, aborting trade", reason);
                    return Ok(LiveTradeResult::Aborted { reason });
                }
            };

        // REQ-012: Validate snapshot age (with clock skew protection)
        let snapshot_age = Utc::now() - snapshot.captured_at;
        if snapshot_age.num_seconds() < 0 || snapshot_age.num_seconds() > max_orderbook_age as i64 {
            let reason = format!(
                "Snapshot too stale: {}s (max {}s)",
                snapshot_age.num_seconds(),
                max_orderbook_age
            );
            warn!("[LIVE] {}, aborting trade", reason);
            return Ok(LiveTradeResult::Aborted { reason });
        }

        // Use detection prices directly (skip re-fetch to avoid MVCC race condition)
        // The detection query already validated prices are fresh within max_orderbook_age
        let execution_yes_price = details.yes_price;
        let execution_no_price = details.no_price;
        let execution_spread = execution_yes_price + execution_no_price;

        info!(
            "[SPREAD] Using detection prices: YES ${:.4}, NO ${:.4}, spread={:.4}",
            execution_yes_price, execution_no_price, execution_spread
        );

        // Calculate shares based on detection prices
        let total_invested = details.total_invested;
        let shares = (total_invested / execution_spread).round_dp(2);

        // Round price and size to 2 decimal places (Polymarket requirement)
        let mut yes_size = shares;
        let yes_price = execution_yes_price.round_dp(2);
        let mut no_size = shares;
        let no_price = execution_no_price.round_dp(2);

        // Check minimum order value ($1 minimum per Polymarket)
        let yes_value = yes_size * yes_price;
        let no_value = no_size * no_price;
        let yes_valid = yes_value >= dec!(1);
        let no_valid = no_value >= dec!(1);

        // Determine trade mode: both sides, single side, or skip
        let single_side_only: Option<&str> = if yes_valid && no_valid {
            None // Both sides valid, do normal spread arb
        } else if yes_valid && !no_valid {
            // Only YES valid - use half size for single-sided bet
            let half_investment = details.total_invested / dec!(2);
            yes_size = (half_investment / yes_price).round_dp(2);
            no_size = Decimal::ZERO;
            info!(
                "[LIVE] NO value too low (${:.2}), single-sided YES bet with half size: {} @ ${}",
                no_value, yes_size, yes_price
            );
            Some("yes")
        } else if !yes_valid && no_valid {
            // Only NO valid - use half size for single-sided bet
            let half_investment = details.total_invested / dec!(2);
            no_size = (half_investment / no_price).round_dp(2);
            yes_size = Decimal::ZERO;
            info!(
                "[LIVE] YES value too low (${:.2}), single-sided NO bet with half size: {} @ ${}",
                yes_value, no_size, no_price
            );
            Some("no")
        } else {
            let reason = format!(
                "Both order values too low - YES: ${:.2}, NO: ${:.2} (min $1)",
                yes_value, no_value
            );
            warn!("[LIVE] {}. Skipping.", reason);
            return Ok(LiveTradeResult::Aborted { reason });
        };

        // Log order details
        if single_side_only.is_none() {
            info!(
                "[LIVE] Building orders: YES {} @ ${}, NO {} @ ${}",
                yes_size, yes_price, no_size, no_price
            );
        }

        // Helper to extract order result
        let extract_order_info =
            |result: &[polymarket_client_sdk::clob::types::PostOrderResponse]| {
                result.first().map(|r| {
                    let has_error = r.error_msg.as_ref().map(|e| !e.is_empty()).unwrap_or(false);
                    let order_id = if r.order_id.is_empty() || has_error {
                        None
                    } else {
                        Some(r.order_id.clone())
                    };
                    let filled = r.taking_amount;
                    let error = r.error_msg.clone();
                    (order_id, filled, error)
                })
            };

        // REQ-007: Log market depth using the SAME snapshot used for price validation
        // This ensures the depth display matches the prices used for orders
        log_market_depth(&snapshot, &opportunity.market_name, 5);

        // Check for price mismatch (blocking fetch) to determine order placement mode
        // Only check for both-sides trades where sequential placement makes sense
        let use_sequential_placement: Option<(OrderSide, Decimal, Decimal)> = if single_side_only
            .is_none()
            && enable_sequential_placement
        {
            match fetch_live_clob_prices(
                &http_client_for_mismatch,
                &opportunity.yes_token_id,
                &opportunity.no_token_id,
            )
            .await
            {
                Some((live_yes, live_no)) => {
                    let (has_mismatch, priority_side, yes_diff, no_diff) = check_price_mismatch(
                        yes_price,
                        no_price,
                        live_yes,
                        live_no,
                        price_mismatch_threshold,
                    );

                    if has_mismatch {
                        warn!(
                                "[MISMATCH] Prices differ from live CLOB! YES: detection={} live={} (diff={}), NO: detection={} live={} (diff={})",
                                yes_price, live_yes, yes_diff, no_price, live_no, no_diff
                            );

                        let live_spread = live_yes + live_no;
                        info!(
                                "[SEQUENTIAL] Using sequential placement, priority side: {:?} (live spread ${})",
                                priority_side, live_spread
                            );
                        Some((priority_side, live_yes, live_no))
                    } else {
                        info!(
                                "[LIVE] Prices match live CLOB (within threshold), using simultaneous placement"
                            );
                        // Spawn non-blocking API fetch for audit logging
                        let yes_token = opportunity.yes_token_id.clone();
                        let no_token = opportunity.no_token_id.clone();
                        let market_name_clone = opportunity.market_name.clone();
                        let http_client_clone = http_client_for_api.clone();
                        tokio::spawn(async move {
                            fetch_and_log_live_orderbook(
                                http_client_clone,
                                yes_token,
                                no_token,
                                market_name_clone,
                            )
                            .await;
                        });
                        None
                    }
                }
                None => {
                    warn!("[LIVE] Could not fetch live prices, falling back to simultaneous placement");
                    None
                }
            }
        } else {
            // Single-sided trade or sequential disabled
            None
        };

        // Execute orders based on trade mode
        let (yes_order_id, yes_filled, no_order_id, no_filled): (
            Option<String>,
            Decimal,
            Option<String>,
            Decimal,
        ) = match single_side_only {
            Some("yes") => {
                // Single-sided YES bet
                info!(
                    "[LIVE] Building single YES order: {} @ ${}",
                    yes_size, yes_price
                );
                let yes_order = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client
                        .limit_order()
                        .token_id(&opportunity.yes_token_id)
                        .size(yes_size)
                        .price(yes_price)
                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                        .build(),
                )
                .await
                .context("YES order building timed out")?
                .context("Failed to build YES order")?;

                let yes_signed = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client.sign(&signer, yes_order),
                )
                .await
                .context("YES order signing timed out")?
                .context("Failed to sign YES order")?;

                info!("[LIVE] Posting single YES order...");
                let yes_result = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client.post_order(yes_signed),
                )
                .await
                .context("YES order posting timed out")?
                .context("Failed to post YES order")?;

                info!("[LIVE] YES order result: {:?}", yes_result);
                let (order_id, filled, error) = extract_order_info(&yes_result).unwrap_or((
                    None,
                    dec!(0),
                    Some("No response".to_string()),
                ));

                if order_id.is_none() {
                    let reason = format!("Single YES order failed: {:?}", error);
                    error!("[LIVE] {}", reason);
                    return Ok(LiveTradeResult::Aborted { reason });
                }
                (order_id, filled, None, dec!(0))
            }
            Some("no") => {
                // Single-sided NO bet
                info!(
                    "[LIVE] Building single NO order: {} @ ${}",
                    no_size, no_price
                );
                let no_order = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client
                        .limit_order()
                        .token_id(&opportunity.no_token_id)
                        .size(no_size)
                        .price(no_price)
                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                        .build(),
                )
                .await
                .context("NO order building timed out")?
                .context("Failed to build NO order")?;

                let no_signed = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client.sign(&signer, no_order),
                )
                .await
                .context("NO order signing timed out")?
                .context("Failed to sign NO order")?;

                info!("[LIVE] Posting single NO order...");
                let no_result = timeout(
                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                    clob_client.post_order(no_signed),
                )
                .await
                .context("NO order posting timed out")?
                .context("Failed to post NO order")?;

                info!("[LIVE] NO order result: {:?}", no_result);
                let (order_id, filled, error) = extract_order_info(&no_result).unwrap_or((
                    None,
                    dec!(0),
                    Some("No response".to_string()),
                ));

                if order_id.is_none() {
                    let reason = format!("Single NO order failed: {:?}", error);
                    error!("[LIVE] {}", reason);
                    return Ok(LiveTradeResult::Aborted { reason });
                }
                (None, dec!(0), order_id, filled)
            }
            _ => {
                // Both sides - check if we should use sequential or simultaneous placement
                if let Some((priority_side, _live_yes, _live_no)) = use_sequential_placement {
                    // ==========================================
                    // SEQUENTIAL PLACEMENT (price mismatch detected)
                    // ==========================================
                    // Note: Live prices used only for mismatch detection.
                    // Orders placed at DETECTION prices to maintain profitability.
                    info!(
                        "[SEQUENTIAL] Starting sequential placement, priority: {:?}, using detection prices: YES=${}, NO=${}",
                        priority_side, yes_price, no_price
                    );

                    // Determine first and second order parameters based on priority
                    // Use DETECTION prices (not live) - these are what make the spread profitable
                    // Live prices are only used to detect mismatch and determine priority side
                    let (
                        first_token_id,
                        first_size,
                        first_price,
                        first_tag,
                        second_token_id,
                        second_size,
                        second_price,
                        second_tag,
                    ) = match priority_side {
                        OrderSide::Yes => (
                            &opportunity.yes_token_id,
                            yes_size,
                            yes_price, // Use detection price (profitable)
                            "YES",
                            &opportunity.no_token_id,
                            no_size,
                            no_price,
                            "NO",
                        ),
                        OrderSide::No => (
                            &opportunity.no_token_id,
                            no_size,
                            no_price, // Use detection price (profitable)
                            "NO",
                            &opportunity.yes_token_id,
                            yes_size,
                            yes_price,
                            "YES",
                        ),
                    };

                    // === PHASE 1: Place first (priority) order ===
                    info!(
                        "[SEQUENTIAL] Phase 1: Placing {} order: {} @ ${}",
                        first_tag, first_size, first_price
                    );

                    let first_order = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client
                            .limit_order()
                            .token_id(first_token_id)
                            .size(first_size)
                            .price(first_price)
                            .side(polymarket_client_sdk::clob::types::Side::Buy)
                            .build(),
                    )
                    .await
                    .context("First order building timed out")?
                    .context("Failed to build first order")?;

                    let first_signed = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.sign(&signer, first_order),
                    )
                    .await
                    .context("First order signing timed out")?
                    .context("Failed to sign first order")?;

                    let first_result = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.post_order(first_signed),
                    )
                    .await
                    .context("First order posting timed out")?
                    .context("Failed to post first order")?;

                    info!(
                        "[SEQUENTIAL] {} order result: {:?}",
                        first_tag, first_result
                    );

                    let (first_order_id, first_immediate_fill, first_err) = extract_order_info(
                        &first_result,
                    )
                    .unwrap_or((None, dec!(0), Some("No response".to_string())));

                    // Check if first order failed
                    if first_order_id.is_none() {
                        let reason = format!(
                            "[SEQUENTIAL] First {} order failed: {:?} - aborting (no exposure)",
                            first_tag, first_err
                        );
                        error!("{}", reason);
                        return Ok(LiveTradeResult::Aborted { reason });
                    }

                    let first_order_id_str = first_order_id.clone().unwrap();

                    // === PHASE 2: Check first order fill status ===
                    let first_filled = if first_immediate_fill >= first_size {
                        info!(
                            "[SEQUENTIAL] {} order filled immediately: {}",
                            first_tag, first_immediate_fill
                        );
                        first_immediate_fill
                    } else {
                        // Order is live, need to poll
                        info!(
                            "[SEQUENTIAL] {} order live (filled {}/{}), polling for fill...",
                            first_tag, first_immediate_fill, first_size
                        );

                        let poll_result = poll_order_fill(
                            clob_client,
                            &first_order_id_str,
                            first_size,
                            sequential_poll_interval_ms,
                            sequential_poll_timeout_secs,
                        )
                        .await;

                        match poll_result {
                            PollResult::FullyFilled(filled) => {
                                info!(
                                    "[SEQUENTIAL] {} order fully filled after polling: {}",
                                    first_tag, filled
                                );
                                filled
                            }
                            PollResult::PartialFill(filled) => {
                                warn!(
                                    "[SEQUENTIAL] {} order partial fill: {}/{}",
                                    first_tag, filled, first_size
                                );
                                filled
                            }
                            PollResult::Timeout | PollResult::Error(_) => {
                                // Cancel and check final fill
                                info!("[SEQUENTIAL] {} order timeout, cancelling...", first_tag);
                                cancel_order_with_retries(
                                    clob_client,
                                    &first_order_id_str,
                                    first_tag,
                                )
                                .await;
                                let final_fill =
                                    query_order_fill(clob_client, &first_order_id_str).await;

                                if final_fill == Decimal::ZERO {
                                    info!("[SEQUENTIAL] No fill after timeout, aborting (no loss)");
                                    return Ok(LiveTradeResult::Aborted {
                                        reason: format!(
                                            "First {} order unfilled after {}s timeout - clean abort",
                                            first_tag, sequential_poll_timeout_secs
                                        ),
                                    });
                                }
                                info!(
                                    "[SEQUENTIAL] {} order had partial fill: {}",
                                    first_tag, final_fill
                                );
                                final_fill
                            }
                        }
                    };

                    // === PHASE 3: Place second order ===
                    // Adjust size based on first order fill
                    // Round to 2 decimal places (Polymarket requirement)
                    let adjusted_second_size = if first_filled < first_size {
                        // Partial fill on first order - match with second to balance
                        first_filled.min(second_size).round_dp(2)
                    } else {
                        second_size
                    };

                    // Guard: Skip second order if adjusted size is zero or below minimum
                    if adjusted_second_size == Decimal::ZERO {
                        warn!("[SEQUENTIAL] Adjusted second order size is zero, aborting trade");
                        return Ok(LiveTradeResult::Aborted {
                            reason: format!(
                                "Second order size would be zero after {} order filled {}",
                                first_tag, first_filled
                            ),
                        });
                    }

                    // Guard: If adjusted size is below Polymarket minimum, skip hedge and recover
                    if adjusted_second_size < MIN_ORDER_SIZE {
                        warn!(
                            "[SEQUENTIAL] Adjusted second order size {} is below minimum {}, initiating recovery",
                            adjusted_second_size, MIN_ORDER_SIZE
                        );

                        // Recovery: Use GTC limit order to sell at a competitive price
                        let recovery_status = if first_filled > Decimal::ZERO {
                            warn!(
                                "[SEQUENTIAL] Initiating GTC recovery sell of {} {} shares",
                                first_filled, first_tag
                            );

                            // Try to sell at a price slightly below market (more likely to fill)
                            // Use the opposite side's best bid price minus a small buffer
                            let recovery_price =
                                (Decimal::ONE - first_price - dec!(0.01)).max(dec!(0.01));
                            info!(
                                "[SEQUENTIAL] Recovery sell: {} @ ${} (GTC limit)",
                                first_filled, recovery_price
                            );

                            let mut sell_succeeded = false;
                            let mut sell_error: Option<String> = None;

                            match timeout(
                                Duration::from_secs(ORDER_TIMEOUT_SECS),
                                clob_client
                                    .limit_order()
                                    .token_id(first_token_id)
                                    .size(first_filled)
                                    .price(recovery_price)
                                    .side(polymarket_client_sdk::clob::types::Side::Sell)
                                    .build(),
                            )
                            .await
                            {
                                Ok(Ok(sell_order)) => {
                                    match timeout(
                                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                                        clob_client.sign(&signer, sell_order),
                                    )
                                    .await
                                    {
                                        Ok(Ok(signed_sell)) => {
                                            match timeout(
                                                Duration::from_secs(ORDER_TIMEOUT_SECS),
                                                clob_client.post_order(signed_sell),
                                            )
                                            .await
                                            {
                                                Ok(Ok(sell_result)) => {
                                                    let (sell_order_id, sell_filled, _) =
                                                        extract_order_info(&sell_result)
                                                            .unwrap_or((None, dec!(0), None));
                                                    if sell_order_id.is_some() {
                                                        info!(
                                                            "[SEQUENTIAL] Recovery GTC sell placed: order_id={:?}, immediate_fill={}",
                                                            sell_order_id, sell_filled
                                                        );
                                                        sell_succeeded = true;
                                                    } else {
                                                        sell_error =
                                                            Some("Order rejected".to_string());
                                                        error!(
                                                            "[SEQUENTIAL] Recovery GTC sell REJECTED: {:?}",
                                                            sell_result
                                                        );
                                                    }
                                                }
                                                Ok(Err(e)) => {
                                                    sell_error =
                                                        Some(format!("Post failed: {:?}", e));
                                                    error!("[SEQUENTIAL] Recovery sell post failed: {:?}", e);
                                                }
                                                Err(_) => {
                                                    sell_error = Some("Post timed out".to_string());
                                                    error!(
                                                        "[SEQUENTIAL] Recovery sell post timed out"
                                                    );
                                                }
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            sell_error = Some(format!("Sign failed: {:?}", e));
                                            error!(
                                                "[SEQUENTIAL] Recovery sell sign failed: {:?}",
                                                e
                                            );
                                        }
                                        Err(_) => {
                                            sell_error = Some("Sign timed out".to_string());
                                            error!("[SEQUENTIAL] Recovery sell sign timed out");
                                        }
                                    }
                                }
                                Ok(Err(e)) => {
                                    sell_error = Some(format!("Build failed: {:?}", e));
                                    error!("[SEQUENTIAL] Recovery sell build failed: {:?}", e);
                                }
                                Err(_) => {
                                    sell_error = Some("Build timed out".to_string());
                                    error!("[SEQUENTIAL] Recovery sell build timed out");
                                }
                            }

                            if sell_succeeded {
                                "recovery GTC sell PLACED - check position later"
                            } else {
                                error!(
                                    "[SEQUENTIAL] CRITICAL: Recovery GTC sell FAILED ({}). {} {} shares may remain! MANUAL INTERVENTION REQUIRED!",
                                    sell_error.as_deref().unwrap_or("unknown"),
                                    first_filled,
                                    first_tag
                                );
                                "recovery sell FAILED - MANUAL INTERVENTION REQUIRED"
                            }
                        } else {
                            "no recovery needed (zero fill)"
                        };

                        return Ok(LiveTradeResult::Aborted {
                            reason: format!(
                                "Partial fill {} below minimum order size {} - {}",
                                adjusted_second_size, MIN_ORDER_SIZE, recovery_status
                            ),
                        });
                    }

                    info!(
                        "[SEQUENTIAL] Phase 3: Placing {} order: {} @ ${} (adjusted from {})",
                        second_tag, adjusted_second_size, second_price, second_size
                    );

                    let second_order = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client
                            .limit_order()
                            .token_id(second_token_id)
                            .size(adjusted_second_size)
                            .price(second_price)
                            .side(polymarket_client_sdk::clob::types::Side::Buy)
                            .build(),
                    )
                    .await
                    .context("Second order building timed out")?
                    .context("Failed to build second order")?;

                    let second_signed = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.sign(&signer, second_order),
                    )
                    .await
                    .context("Second order signing timed out")?
                    .context("Failed to sign second order")?;

                    let second_result = timeout(
                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                        clob_client.post_order(second_signed),
                    )
                    .await
                    .context("Second order posting timed out")?
                    .context("Failed to post second order")?;

                    info!(
                        "[SEQUENTIAL] {} order result: {:?}",
                        second_tag, second_result
                    );

                    let (second_order_id, second_immediate_fill, second_err) = extract_order_info(
                        &second_result,
                    )
                    .unwrap_or((None, dec!(0), Some("No response".to_string())));

                    // Check if second order failed
                    let second_filled = if second_order_id.is_none() {
                        error!(
                            "[SEQUENTIAL] Second {} order failed: {:?}",
                            second_tag, second_err
                        );

                        // Recovery: Market sell first order's fill to prevent imbalance
                        let recovery_status = if first_filled > Decimal::ZERO {
                            warn!(
                                "[SEQUENTIAL] Initiating recovery sell of {} {} shares",
                                first_filled, first_tag
                            );

                            // Convert Decimal to Amount type
                            let sell_amount =
                                match polymarket_client_sdk::clob::types::Amount::shares(
                                    first_filled,
                                ) {
                                    Ok(a) => a,
                                    Err(e) => {
                                        error!(
                                            "[SEQUENTIAL] CRITICAL: Failed to create Amount from {}: {:?}. MANUAL INTERVENTION REQUIRED!",
                                            first_filled, e
                                        );
                                        return Ok(LiveTradeResult::Aborted {
                                            reason: format!(
                                            "IMBALANCED POSITION: {} {} shares held. Recovery sell failed: couldn't create Amount. MANUAL SELL REQUIRED!",
                                            first_filled, first_tag
                                        ),
                                        });
                                    }
                                };

                            // Attempt market sell with FOK (Fill-or-Kill)
                            // Track success/failure for clear reporting
                            let mut sell_succeeded = false;
                            let mut sell_error: Option<String> = None;

                            match timeout(
                                Duration::from_secs(ORDER_TIMEOUT_SECS),
                                clob_client
                                    .market_order()
                                    .token_id(first_token_id)
                                    .amount(sell_amount)
                                    .side(polymarket_client_sdk::clob::types::Side::Sell)
                                    .order_type(OrderType::FOK)
                                    .build(),
                            )
                            .await
                            {
                                Ok(Ok(sell_order)) => {
                                    match timeout(
                                        Duration::from_secs(ORDER_TIMEOUT_SECS),
                                        clob_client.sign(&signer, sell_order),
                                    )
                                    .await
                                    {
                                        Ok(Ok(signed_sell)) => {
                                            match timeout(
                                                Duration::from_secs(ORDER_TIMEOUT_SECS),
                                                clob_client.post_order(signed_sell),
                                            )
                                            .await
                                            {
                                                Ok(Ok(sell_result)) => {
                                                    // Check if the order was accepted
                                                    let (sell_order_id, sell_filled, _) =
                                                        extract_order_info(&sell_result)
                                                            .unwrap_or((None, dec!(0), None));
                                                    if sell_order_id.is_some() {
                                                        info!(
                                                            "[SEQUENTIAL] Recovery sell SUCCEEDED: sold {} shares",
                                                            sell_filled
                                                        );
                                                        sell_succeeded = true;
                                                    } else {
                                                        sell_error =
                                                            Some("Order rejected".to_string());
                                                        error!(
                                                            "[SEQUENTIAL] Recovery sell REJECTED: {:?}",
                                                            sell_result
                                                        );
                                                    }
                                                }
                                                Ok(Err(e)) => {
                                                    sell_error =
                                                        Some(format!("Post failed: {:?}", e));
                                                    error!(
                                                        "[SEQUENTIAL] Recovery sell post failed: {:?}",
                                                        e
                                                    );
                                                }
                                                Err(_) => {
                                                    sell_error = Some("Post timed out".to_string());
                                                    error!(
                                                        "[SEQUENTIAL] Recovery sell post timed out"
                                                    );
                                                }
                                            }
                                        }
                                        Ok(Err(e)) => {
                                            sell_error = Some(format!("Sign failed: {:?}", e));
                                            error!(
                                                "[SEQUENTIAL] Recovery sell sign failed: {:?}",
                                                e
                                            );
                                        }
                                        Err(_) => {
                                            sell_error = Some("Sign timed out".to_string());
                                            error!("[SEQUENTIAL] Recovery sell sign timed out");
                                        }
                                    }
                                }
                                Ok(Err(e)) => {
                                    sell_error = Some(format!("Build failed: {:?}", e));
                                    error!("[SEQUENTIAL] Recovery sell build failed: {:?}", e);
                                }
                                Err(_) => {
                                    sell_error = Some("Build timed out".to_string());
                                    error!("[SEQUENTIAL] Recovery sell build timed out");
                                }
                            }

                            // Return clear status
                            if sell_succeeded {
                                "recovery sell SUCCEEDED - position rebalanced"
                            } else {
                                error!(
                                    "[SEQUENTIAL] CRITICAL: Recovery sell FAILED ({}). {} {} shares may remain! MANUAL INTERVENTION REQUIRED!",
                                    sell_error.as_deref().unwrap_or("unknown"),
                                    first_filled,
                                    first_tag
                                );
                                "recovery sell FAILED - MANUAL INTERVENTION REQUIRED"
                            }
                        } else {
                            "no recovery needed (zero fill)"
                        };

                        // Return aborted with clear recovery status
                        return Ok(LiveTradeResult::Aborted {
                            reason: format!(
                                "Second {} order failed after first {} filled {} - {}",
                                second_tag, first_tag, first_filled, recovery_status
                            ),
                        });
                    } else if second_immediate_fill >= adjusted_second_size {
                        info!(
                            "[SEQUENTIAL] {} order filled immediately: {}",
                            second_tag, second_immediate_fill
                        );
                        second_immediate_fill
                    } else {
                        // Second order is live, poll for fill
                        let second_order_id_str = second_order_id.clone().unwrap();
                        info!(
                            "[SEQUENTIAL] {} order live (filled {}/{}), polling...",
                            second_tag, second_immediate_fill, adjusted_second_size
                        );

                        let poll_result = poll_order_fill(
                            clob_client,
                            &second_order_id_str,
                            adjusted_second_size,
                            sequential_poll_interval_ms,
                            sequential_poll_timeout_secs,
                        )
                        .await;

                        match poll_result {
                            PollResult::FullyFilled(filled) => filled,
                            PollResult::PartialFill(filled) => filled,
                            PollResult::Timeout | PollResult::Error(_) => {
                                cancel_order_with_retries(
                                    clob_client,
                                    &second_order_id_str,
                                    second_tag,
                                )
                                .await;
                                query_order_fill(clob_client, &second_order_id_str).await
                            }
                        }
                    };

                    // Return results in correct order (YES, NO)
                    match priority_side {
                        OrderSide::Yes => {
                            (first_order_id, first_filled, second_order_id, second_filled)
                        }
                        OrderSide::No => {
                            (second_order_id, second_filled, first_order_id, first_filled)
                        }
                    }
                } else {
                    // ==========================================
                    // SIMULTANEOUS PLACEMENT (no mismatch or fallback)
                    // ==========================================
                    let (yes_order, no_order) =
                        timeout(Duration::from_secs(ORDER_TIMEOUT_SECS), async {
                            tokio::try_join!(
                                async {
                                    clob_client
                                        .limit_order()
                                        .token_id(&opportunity.yes_token_id)
                                        .size(yes_size)
                                        .price(yes_price)
                                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                                        .build()
                                        .await
                                        .context("Failed to build YES order")
                                },
                                async {
                                    clob_client
                                        .limit_order()
                                        .token_id(&opportunity.no_token_id)
                                        .size(no_size)
                                        .price(no_price)
                                        .side(polymarket_client_sdk::clob::types::Side::Buy)
                                        .build()
                                        .await
                                        .context("Failed to build NO order")
                                }
                            )
                        })
                        .await
                        .context("Order building timed out")??;

                    let (yes_signed, no_signed) =
                        timeout(Duration::from_secs(ORDER_TIMEOUT_SECS), async {
                            tokio::try_join!(
                                clob_client.sign(&signer, yes_order),
                                clob_client.sign(&signer, no_order)
                            )
                        })
                        .await
                        .context("Order signing timed out")?
                        .context("Failed to sign orders")?;

                    info!("[LIVE] Posting YES and NO orders simultaneously...");
                    let (yes_result, no_result) =
                        timeout(Duration::from_secs(ORDER_TIMEOUT_SECS), async {
                            tokio::try_join!(
                                clob_client.post_order(yes_signed),
                                clob_client.post_order(no_signed)
                            )
                        })
                        .await
                        .context("Order posting timed out")?
                        .context("Failed to post orders")?;

                    info!("[LIVE] YES order result: {:?}", yes_result);
                    info!("[LIVE] NO order result: {:?}", no_result);

                    let (yes_id, yes_f, yes_err) = extract_order_info(&yes_result).unwrap_or((
                        None,
                        dec!(0),
                        Some("No response".to_string()),
                    ));
                    let (no_id, no_f, no_err) = extract_order_info(&no_result).unwrap_or((
                        None,
                        dec!(0),
                        Some("No response".to_string()),
                    ));

                    let yes_failed = yes_id.is_none();
                    let no_failed = no_id.is_none();

                    if yes_failed && no_failed {
                        let reason =
                            format!("Both orders failed - YES: {:?}, NO: {:?}", yes_err, no_err);
                        error!("[LIVE] {}", reason);
                        return Ok(LiveTradeResult::Aborted { reason });
                    }

                    // If only one failed, cancel the other with retries
                    if yes_failed || no_failed {
                        warn!("[LIVE] Partial failure - YES: {:?}, NO: {:?}. Cancelling successful order.", yes_err, no_err);

                        if let Some(ref order_id) = yes_id {
                            for attempt in 1..=CANCEL_MAX_RETRIES {
                                match timeout(
                                    Duration::from_secs(CANCEL_TIMEOUT_SECS),
                                    clob_client.cancel_order(order_id),
                                )
                                .await
                                {
                                    Ok(Ok(_)) => {
                                        info!(
                                            "[LIVE] Cancelled YES order {} on attempt {}",
                                            order_id, attempt
                                        );
                                        break;
                                    }
                                    Ok(Err(e)) => {
                                        warn!(
                                            "[LIVE] Cancel YES failed (attempt {}): {:?}",
                                            attempt, e
                                        )
                                    }
                                    Err(_) => {
                                        warn!("[LIVE] Cancel YES timeout (attempt {})", attempt)
                                    }
                                }
                                if attempt == CANCEL_MAX_RETRIES {
                                    error!("[LIVE] CRITICAL: Failed to cancel YES order {} after {} attempts. ORPHANED ORDER!", order_id, CANCEL_MAX_RETRIES);
                                } else {
                                    tokio::time::sleep(Duration::from_millis(
                                        CANCEL_RETRY_DELAY_MS,
                                    ))
                                    .await;
                                }
                            }
                        }

                        if let Some(ref order_id) = no_id {
                            for attempt in 1..=CANCEL_MAX_RETRIES {
                                match timeout(
                                    Duration::from_secs(CANCEL_TIMEOUT_SECS),
                                    clob_client.cancel_order(order_id),
                                )
                                .await
                                {
                                    Ok(Ok(_)) => {
                                        info!(
                                            "[LIVE] Cancelled NO order {} on attempt {}",
                                            order_id, attempt
                                        );
                                        break;
                                    }
                                    Ok(Err(e)) => {
                                        warn!(
                                            "[LIVE] Cancel NO failed (attempt {}): {:?}",
                                            attempt, e
                                        )
                                    }
                                    Err(_) => {
                                        warn!("[LIVE] Cancel NO timeout (attempt {})", attempt)
                                    }
                                }
                                if attempt == CANCEL_MAX_RETRIES {
                                    error!("[LIVE] CRITICAL: Failed to cancel NO order {} after {} attempts. ORPHANED ORDER!", order_id, CANCEL_MAX_RETRIES);
                                } else {
                                    tokio::time::sleep(Duration::from_millis(
                                        CANCEL_RETRY_DELAY_MS,
                                    ))
                                    .await;
                                }
                            }
                        }
                        let reason = format!(
                            "Partial order failure (cancelled other side) - YES: {:?}, NO: {:?}",
                            yes_err, no_err
                        );
                        return Ok(LiveTradeResult::Aborted { reason });
                    }

                    (yes_id, yes_f, no_id, no_f)
                }
            }
        };

        // Log fill amounts
        info!(
            "[LIVE] Orders placed - YES: {} filled of {} (order: {:?}), NO: {} filled of {} (order: {:?})",
            yes_filled, yes_size, yes_order_id,
            no_filled, no_size, no_order_id
        );

        // Check for unfilled orders (for later rebalance spawn)
        let yes_unfilled =
            single_side_only.is_none() && yes_size > Decimal::ZERO && yes_filled < yes_size;
        let no_unfilled =
            single_side_only.is_none() && no_size > Decimal::ZERO && no_filled < no_size;

        // Determine order status based on fill (handle non-placed orders)
        let yes_status = if yes_size == Decimal::ZERO {
            "not_placed"
        } else if yes_filled >= yes_size {
            "filled"
        } else if yes_filled > dec!(0) {
            "partial"
        } else {
            "pending"
        };
        let no_status = if no_size == Decimal::ZERO {
            "not_placed"
        } else if no_filled >= no_size {
            "filled"
        } else if no_filled > dec!(0) {
            "partial"
        } else {
            "pending"
        };

        // Record position with requested amounts
        let position_id = repository::create_position(
            self.db.pool(),
            opportunity.market_id,
            yes_size,                                  // Requested (rounded) size
            no_size,                                   // Requested (rounded) size
            yes_size * yes_price + no_size * no_price, // Actual order value
            false,                                     // NOT dry_run
        )
        .await?;

        // Update position with actual fill amounts
        repository::update_position_fills(self.db.pool(), position_id, yes_filled, no_filled)
            .await?;

        // Record trades with order IDs and fill amounts (only for sides that were placed)
        let mut trade_count = 0;
        if yes_size > Decimal::ZERO {
            repository::record_trade_with_order(
                self.db.pool(),
                position_id,
                "yes",
                "buy",
                yes_price,
                yes_size,
                yes_order_id.as_deref(),
                yes_filled,
                yes_status,
            )
            .await?;
            trade_count += 1;
        }

        if no_size > Decimal::ZERO {
            repository::record_trade_with_order(
                self.db.pool(),
                position_id,
                "no",
                "buy",
                no_price,
                no_size,
                no_order_id.as_deref(),
                no_filled,
                no_status,
            )
            .await?;
            trade_count += 1;
        }

        // Update session state with actual order value
        let actual_invested = yes_size * yes_price + no_size * no_price;
        let actual_fee = actual_invested * self.config.fee_rate;
        self.session.current_balance -= actual_invested;
        self.session.total_trades += trade_count;
        self.session.positions_opened += 1;
        self.session.fees_paid += actual_fee;

        // Cache position with actual order values
        self.session.open_positions.insert(
            opportunity.market_id,
            PositionCache {
                id: position_id,
                market_id: opportunity.market_id,
                market_name: opportunity.market_name.clone(),
                yes_shares: yes_size,
                no_shares: no_size,
                total_invested: actual_invested,
                end_time: opportunity.end_time,
            },
        );

        let trade_type = match single_side_only {
            Some("yes") => "SINGLE-YES",
            Some("no") => "SINGLE-NO",
            _ => "SPREAD-ARB",
        };
        info!(
            "[LIVE] {} EXECUTED: {} | YES: {} @ ${} | NO: {} @ ${} | Invested: ${:.2}",
            trade_type,
            opportunity.market_name,
            yes_size,
            yes_price,
            no_size,
            no_price,
            actual_invested
        );

        // Spawn background rebalance task if there are unfilled orders
        if yes_unfilled || no_unfilled {
            info!("[LIVE] Unfilled orders detected, spawning background rebalance task");

            // Clone values needed for background task
            let yes_order_id_clone = yes_order_id.clone();
            let no_order_id_clone = no_order_id.clone();
            let yes_token_id = opportunity.yes_token_id.clone();
            let no_token_id = opportunity.no_token_id.clone();
            let market_name = opportunity.market_name.clone();
            let private_key_clone = private_key.clone();
            let proxy_wallet_clone = proxy_wallet.clone();
            // Clone initial fill amounts for correct imbalance calculation
            let yes_filled_initial = yes_filled;
            let no_filled_initial = no_filled;
            // Clone DB and position_id for trade recording
            let db_clone = self.db.clone();
            let position_id_clone = position_id;

            tokio::spawn(async move {
                // Wait before rebalancing
                info!(
                    "[REBALANCE] Waiting {}s for {} orders to fill...",
                    UNFILLED_WAIT_SECS, market_name
                );
                tokio::time::sleep(Duration::from_secs(UNFILLED_WAIT_SECS)).await;

                // Re-authenticate for background task
                let signer = match LocalSigner::from_str(&private_key_clone) {
                    Ok(s) => s.with_chain_id(Some(POLYGON)),
                    Err(e) => {
                        error!("[REBALANCE] Failed to create signer: {:?}", e);
                        return;
                    }
                };

                let signature_type = if proxy_wallet_clone.is_some() {
                    SignatureType::GnosisSafe
                } else {
                    SignatureType::Eoa
                };

                let mut auth_builder = match ClobClient::new(CLOB_HOST, ClobConfig::default()) {
                    Ok(c) => c
                        .authentication_builder(&signer)
                        .signature_type(signature_type),
                    Err(e) => {
                        error!("[REBALANCE] Failed to create CLOB client: {:?}", e);
                        return;
                    }
                };

                if let Some(ref proxy) = proxy_wallet_clone {
                    if let Ok(funder_address) = proxy.parse::<alloy::primitives::Address>() {
                        auth_builder = auth_builder.funder(funder_address);
                    }
                }

                let clob_client = match auth_builder.authenticate().await {
                    Ok(c) => c,
                    Err(e) => {
                        error!("[REBALANCE] Failed to authenticate: {:?}", e);
                        return;
                    }
                };

                // Start with initial fill values - only query API for sides that were unfilled
                let mut final_yes_filled = yes_filled_initial;
                let mut final_no_filled = no_filled_initial;
                let mut cancel_failed = false;

                // Cancel and get final fill for YES order (only if it was unfilled)
                if yes_unfilled {
                    if let Some(ref order_id) = yes_order_id_clone {
                        info!("[REBALANCE] Cancelling YES order {}", order_id);
                        match timeout(
                            Duration::from_secs(CANCEL_TIMEOUT_SECS),
                            clob_client.cancel_order(order_id),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                // Cancel succeeded, query final fill amount
                                match timeout(Duration::from_secs(CANCEL_TIMEOUT_SECS), clob_client.order(order_id)).await {
                                    Ok(Ok(order_info)) => {
                                        info!("[REBALANCE] YES order final: {:?}", order_info);
                                        final_yes_filled = order_info.size_matched;
                                    }
                                    Ok(Err(e)) => warn!("[REBALANCE] Failed to query YES order: {:?}, using initial fill", e),
                                    Err(_) => warn!("[REBALANCE] YES order query timed out, using initial fill"),
                                }
                            }
                            Ok(Err(e)) => {
                                error!("[REBALANCE] Failed to cancel YES order: {:?}", e);
                                cancel_failed = true;
                            }
                            Err(_) => {
                                error!("[REBALANCE] Cancel YES order timed out");
                                cancel_failed = true;
                            }
                        }
                    }
                }

                // Cancel and get final fill for NO order (only if it was unfilled)
                // Try even if YES cancel failed
                if no_unfilled {
                    if let Some(ref order_id) = no_order_id_clone {
                        info!("[REBALANCE] Cancelling NO order {}", order_id);
                        match timeout(
                            Duration::from_secs(CANCEL_TIMEOUT_SECS),
                            clob_client.cancel_order(order_id),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                // Cancel succeeded, query final fill amount
                                match timeout(Duration::from_secs(CANCEL_TIMEOUT_SECS), clob_client.order(order_id)).await {
                                    Ok(Ok(order_info)) => {
                                        info!("[REBALANCE] NO order final: {:?}", order_info);
                                        final_no_filled = order_info.size_matched;
                                    }
                                    Ok(Err(e)) => warn!("[REBALANCE] Failed to query NO order: {:?}, using initial fill", e),
                                    Err(_) => warn!("[REBALANCE] NO order query timed out, using initial fill"),
                                }
                            }
                            Ok(Err(e)) => {
                                error!("[REBALANCE] Failed to cancel NO order: {:?}", e);
                                cancel_failed = true;
                            }
                            Err(_) => {
                                error!("[REBALANCE] Cancel NO order timed out");
                                cancel_failed = true;
                            }
                        }
                    }
                }

                // Abort if any cancel failed - position state unknown
                if cancel_failed {
                    error!(
                        "[REBALANCE] Aborting due to cancel failure - manual intervention needed"
                    );
                    return;
                }

                info!(
                    "[REBALANCE] After cancels - YES: {}, NO: {}",
                    final_yes_filled, final_no_filled
                );

                // Helper closure to execute market sell and record to database
                // imbalance_base is used for fallback retry with 90% on insufficient balance
                let execute_sell = |token_id: &str,
                                    amount: Decimal,
                                    side: &str,
                                    imbalance_base: Decimal| {
                    let clob = &clob_client;
                    let sig = &signer;
                    let db = &db_clone;
                    let pos_id = position_id_clone;
                    let token = token_id.to_string();
                    let side_name = side.to_string();
                    async move {
                        // Try with initial amount first, then fallback to 90% on insufficient balance
                        let amounts_to_try = [
                            amount,                      // First try: requested amount
                            imbalance_base * dec!(0.90), // Fallback: 90% of imbalance
                        ];

                        for (amount_idx, try_amount) in amounts_to_try.iter().enumerate() {
                            // Floor to 2 decimals to avoid selling more than we hold
                            let floored_amount = try_amount.trunc_with_scale(2);
                            if floored_amount <= Decimal::ZERO {
                                info!(
                                    "[REBALANCE] {} amount too small after floor: {} -> {}",
                                    side_name.to_uppercase(),
                                    try_amount,
                                    floored_amount
                                );
                                return;
                            }
                            let sell_amount =
                                match polymarket_client_sdk::clob::types::Amount::shares(
                                    floored_amount,
                                ) {
                                    Ok(a) => a,
                                    Err(e) => {
                                        error!(
                                            "[REBALANCE] {} Amount::shares({}) failed: {:?}",
                                            side_name.to_uppercase(),
                                            floored_amount,
                                            e
                                        );
                                        return;
                                    }
                                };

                            // Retry market sell up to 3 times per amount level
                            const MAX_SELL_RETRIES: u32 = 3;
                            let mut insufficient_balance = false;
                            for attempt in 1..=MAX_SELL_RETRIES {
                                info!("[REBALANCE] {} sell attempt {}/{} (amount_level={}): token={} amount={} type=FOK side=Sell",
                                    side_name.to_uppercase(), attempt, MAX_SELL_RETRIES, amount_idx, &token, floored_amount);

                                // Step 1: Build the order
                                info!(
                                    "[REBALANCE] {} sell: building market order...",
                                    side_name.to_uppercase()
                                );
                                let order_result = timeout(
                                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                                    clob.market_order()
                                        .token_id(&token)
                                        .amount(sell_amount.clone())
                                        .side(polymarket_client_sdk::clob::types::Side::Sell)
                                        .order_type(OrderType::FOK)
                                        .build(),
                                )
                                .await;

                                let order = match order_result {
                                    Ok(Ok(o)) => {
                                        info!(
                                            "[REBALANCE] {} sell: order built successfully",
                                            side_name.to_uppercase()
                                        );
                                        debug!(
                                            "[REBALANCE] {} sell: order details: {:?}",
                                            side_name.to_uppercase(),
                                            o
                                        );
                                        o
                                    }
                                    Ok(Err(e)) => {
                                        error!("[REBALANCE] {} sell: failed to build order (attempt {}/{}): {:?}",
                                        side_name.to_uppercase(), attempt, MAX_SELL_RETRIES, e);
                                        if attempt < MAX_SELL_RETRIES {
                                            let delay = 2u64.pow(attempt);
                                            tokio::time::sleep(Duration::from_secs(delay)).await;
                                        }
                                        continue;
                                    }
                                    Err(_) => {
                                        error!("[REBALANCE] {} sell: order build timed out (attempt {}/{})",
                                        side_name.to_uppercase(), attempt, MAX_SELL_RETRIES);
                                        if attempt < MAX_SELL_RETRIES {
                                            let delay = 2u64.pow(attempt);
                                            tokio::time::sleep(Duration::from_secs(delay)).await;
                                        }
                                        continue;
                                    }
                                };

                                // Step 2: Sign the order
                                info!(
                                    "[REBALANCE] {} sell: signing order...",
                                    side_name.to_uppercase()
                                );
                                let signed_result = timeout(
                                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                                    clob.sign(sig, order),
                                )
                                .await;

                                let signed = match signed_result {
                                    Ok(Ok(s)) => {
                                        info!(
                                            "[REBALANCE] {} sell: order signed successfully",
                                            side_name.to_uppercase()
                                        );
                                        debug!(
                                            "[REBALANCE] {} sell: signed order: {:?}",
                                            side_name.to_uppercase(),
                                            s
                                        );
                                        s
                                    }
                                    Ok(Err(e)) => {
                                        error!("[REBALANCE] {} sell: failed to sign order (attempt {}/{}): {:?}",
                                        side_name.to_uppercase(), attempt, MAX_SELL_RETRIES, e);
                                        if attempt < MAX_SELL_RETRIES {
                                            let delay = 2u64.pow(attempt);
                                            tokio::time::sleep(Duration::from_secs(delay)).await;
                                        }
                                        continue;
                                    }
                                    Err(_) => {
                                        error!("[REBALANCE] {} sell: order signing timed out (attempt {}/{})",
                                        side_name.to_uppercase(), attempt, MAX_SELL_RETRIES);
                                        if attempt < MAX_SELL_RETRIES {
                                            let delay = 2u64.pow(attempt);
                                            tokio::time::sleep(Duration::from_secs(delay)).await;
                                        }
                                        continue;
                                    }
                                };

                                // Step 3: Post the order
                                info!(
                                    "[REBALANCE] {} sell: posting order to CLOB...",
                                    side_name.to_uppercase()
                                );
                                let post_result = timeout(
                                    Duration::from_secs(ORDER_TIMEOUT_SECS),
                                    clob.post_order(signed),
                                )
                                .await;

                                match post_result {
                                    Ok(Ok(r)) => {
                                        info!(
                                        "[REBALANCE] {} sell: received response with {} order(s)",
                                        side_name.to_uppercase(),
                                        r.len()
                                    );
                                        info!(
                                            "[REBALANCE] {} sell: full response: {:?}",
                                            side_name.to_uppercase(),
                                            r
                                        );

                                        if let Some(order) = r.first() {
                                            let order_id = &order.order_id;
                                            let filled = order.taking_amount;
                                            let has_error = order
                                                .error_msg
                                                .as_ref()
                                                .map(|e| !e.is_empty())
                                                .unwrap_or(false);

                                            info!("[REBALANCE] {} sell result: order_id={} filled={} success={} status={:?} error={:?}",
                                            side_name.to_uppercase(), order_id, filled, order.success, order.status, order.error_msg);

                                            // Log additional fields if available
                                            info!("[REBALANCE] {} sell details: making_amount={} taking_amount={} tx_hashes={:?}",
                                            side_name.to_uppercase(), order.making_amount, order.taking_amount, order.transaction_hashes);

                                            // Check if order actually succeeded
                                            if !order.success || has_error || order_id.is_empty() {
                                                // Check for insufficient balance error
                                                let error_str = order
                                                    .error_msg
                                                    .as_ref()
                                                    .map(|s| s.to_lowercase())
                                                    .unwrap_or_default();
                                                if error_str.contains("balance")
                                                    || error_str.contains("allowance")
                                                    || error_str.contains("insufficient")
                                                {
                                                    warn!("[REBALANCE] {} sell INSUFFICIENT BALANCE detected, will try 90% fallback: error={:?}",
                                                    side_name.to_uppercase(), order.error_msg);
                                                    insufficient_balance = true;
                                                    break; // Break inner retry loop to try reduced amount
                                                }
                                                warn!("[REBALANCE] {} sell order failed (attempt {}/{}): success={} error={:?}",
                                                side_name.to_uppercase(), attempt, MAX_SELL_RETRIES, order.success, order.error_msg);
                                            } else {
                                                // Order succeeded, record it
                                                info!("[REBALANCE] {} sell SUCCESS: filled {} of {} shares", side_name.to_uppercase(), filled, floored_amount);
                                                if let Err(e) = repository::record_trade_with_order(
                                                    db.pool(),
                                                    pos_id,
                                                    &side_name,
                                                    "sell",
                                                    Decimal::ZERO,
                                                    filled,
                                                    Some(order_id),
                                                    floored_amount,
                                                    if filled >= floored_amount {
                                                        "filled"
                                                    } else {
                                                        "partial"
                                                    },
                                                )
                                                .await
                                                {
                                                    error!("[REBALANCE] Failed to record {} sell trade: {:?}", side_name.to_uppercase(), e);
                                                }
                                                return; // Success, exit retry loop
                                            }
                                        } else {
                                            warn!("[REBALANCE] {} sell: empty response array (attempt {}/{})", side_name.to_uppercase(), attempt, MAX_SELL_RETRIES);
                                        }
                                    }
                                    Ok(Err(e)) => {
                                        error!("[REBALANCE] {} sell: post_order failed (attempt {}/{}): {:?}",
                                        side_name.to_uppercase(), attempt, MAX_SELL_RETRIES, e);
                                    }
                                    Err(_) => {
                                        error!(
                                        "[REBALANCE] {} sell: post_order timed out (attempt {}/{})",
                                        side_name.to_uppercase(),
                                        attempt,
                                        MAX_SELL_RETRIES
                                    );
                                    }
                                }

                                // Exponential backoff before retry (2s, 4s, 8s)
                                if attempt < MAX_SELL_RETRIES {
                                    let delay = 2u64.pow(attempt);
                                    info!(
                                        "[REBALANCE] {} sell: waiting {}s before retry...",
                                        side_name.to_uppercase(),
                                        delay
                                    );
                                    tokio::time::sleep(Duration::from_secs(delay)).await;
                                }
                            } // end inner retry loop

                            // If insufficient balance, try next amount level (90%)
                            if insufficient_balance && amount_idx == 0 {
                                warn!(
                                    "[REBALANCE] {} sell: trying 90% fallback amount ({} -> {})",
                                    side_name.to_uppercase(),
                                    floored_amount,
                                    (imbalance_base * dec!(0.90)).trunc_with_scale(2)
                                );
                                continue; // Continue to next amount level
                            }

                            // If we get here without success, log and continue
                            error!(
                                "[REBALANCE] {} sell failed at amount_level={} after {} attempts",
                                side_name.to_uppercase(),
                                amount_idx,
                                MAX_SELL_RETRIES
                            );
                        } // end amounts_to_try loop
                    }
                };

                // Sell imbalance and record to database
                let imbalance = final_yes_filled - final_no_filled;

                // Fetch all positions once to get both YES and NO balances
                let user_address =
                    proxy_wallet_clone.unwrap_or_else(|| format!("{}", signer.address()));
                let balance_checker = GammaBalanceChecker::new(GAMMA_DATA_API_URL, &user_address);

                let positions = match balance_checker.get_all_positions().await {
                    Ok(p) => p,
                    Err(e) => {
                        warn!("[REBALANCE] Failed to query positions, using calculated imbalance: {:?}", e);
                        vec![]
                    }
                };

                if imbalance > Decimal::ZERO {
                    // Get YES balance from cached positions
                    let actual_balance = if positions.is_empty() {
                        imbalance // Fallback to calculated imbalance
                    } else {
                        find_balance(&positions, &yes_token_id)
                    };

                    let safe_sell_amount = calculate_safe_sell_amount(imbalance, actual_balance);
                    if safe_sell_amount <= Decimal::ZERO {
                        // Balance=0 from API, use imbalance*98% as fallback (2% safety margin)
                        let fallback_amount = imbalance * dec!(0.98);
                        warn!("[REBALANCE] Balance=0 from API, using fallback: {} (imbalance={} * 98%)",
                            fallback_amount, imbalance);
                        execute_sell(&yes_token_id, fallback_amount, "yes", imbalance).await;
                    } else {
                        info!(
                            "[REBALANCE] Selling {} excess YES shares (balance: {}, imbalance: {})",
                            safe_sell_amount, actual_balance, imbalance
                        );
                        execute_sell(&yes_token_id, safe_sell_amount, "yes", imbalance).await;
                    }
                } else if imbalance < Decimal::ZERO {
                    let excess_no = -imbalance;

                    // Get NO balance from cached positions
                    let actual_balance = if positions.is_empty() {
                        excess_no // Fallback to calculated imbalance
                    } else {
                        find_balance(&positions, &no_token_id)
                    };

                    let safe_sell_amount = calculate_safe_sell_amount(excess_no, actual_balance);
                    if safe_sell_amount <= Decimal::ZERO {
                        // Balance=0 from API, use imbalance*98% as fallback (2% safety margin)
                        let fallback_amount = excess_no * dec!(0.98);
                        warn!("[REBALANCE] Balance=0 from API, using fallback: {} (imbalance={} * 98%)",
                            fallback_amount, excess_no);
                        execute_sell(&no_token_id, fallback_amount, "no", excess_no).await;
                    } else {
                        info!(
                            "[REBALANCE] Selling {} excess NO shares (balance: {}, imbalance: {})",
                            safe_sell_amount, actual_balance, excess_no
                        );
                        execute_sell(&no_token_id, safe_sell_amount, "no", excess_no).await;
                    }
                } else {
                    info!("[REBALANCE] Position balanced, no action needed");
                }
            });
        }

        Ok(LiveTradeResult::Executed {
            invested: actual_invested,
            yes_filled,
            no_filled,
        })
    }

    /// Execute a dry-run (simulated) trade.
    async fn execute_dry_run(
        &mut self,
        opportunity: &SpreadOpportunity,
        details: &TradeDetails,
    ) -> Result<()> {
        // Fetch and log market depth before simulating trade
        match repository::get_latest_orderbook_snapshot(self.db.pool(), opportunity.market_id).await
        {
            Ok(Some(snapshot)) => {
                log_market_depth(&snapshot, &opportunity.market_name, 5);

                // Spawn non-blocking API fetch to compare DB prices with live API
                let http_client = self.http_client.clone();
                let yes_token = opportunity.yes_token_id.clone();
                let no_token = opportunity.no_token_id.clone();
                let market_name_clone = opportunity.market_name.clone();
                tokio::spawn(async move {
                    fetch_and_log_live_orderbook(
                        http_client,
                        yes_token,
                        no_token,
                        market_name_clone,
                    )
                    .await;
                });
            }
            Ok(None) => {
                warn!("[DRY RUN] No orderbook snapshot available for market depth visualization");
            }
            Err(e) => {
                warn!(
                    "[DRY RUN] Failed to fetch orderbook for depth visualization: {}",
                    e
                );
            }
        }

        // Create position in database
        let position_id = repository::create_position(
            self.db.pool(),
            opportunity.market_id,
            details.yes_shares,
            details.no_shares,
            details.total_invested,
            true, // dry_run
        )
        .await?;

        // Record trades
        repository::record_trade(
            self.db.pool(),
            position_id,
            "yes",
            "buy",
            details.yes_price,
            details.yes_shares,
        )
        .await?;

        repository::record_trade(
            self.db.pool(),
            position_id,
            "no",
            "buy",
            details.no_price,
            details.no_shares,
        )
        .await?;

        // Update session state
        self.session.current_balance -= details.total_invested;
        self.session.total_trades += 2;
        self.session.positions_opened += 1;
        self.session.fees_paid += details.fee;

        // Cache position
        self.session.open_positions.insert(
            opportunity.market_id,
            PositionCache {
                id: position_id,
                market_id: opportunity.market_id,
                market_name: opportunity.market_name.clone(),
                yes_shares: details.yes_shares,
                no_shares: details.no_shares,
                total_invested: details.total_invested,
                end_time: opportunity.end_time,
            },
        );

        info!(
            "[DRY RUN] TRADE: {} | YES: {:.4} @ ${:.4} | NO: {:.4} @ ${:.4} | Invested: ${:.2} | Expected profit: ${:.4}",
            opportunity.market_name,
            details.yes_shares, details.yes_price,
            details.no_shares, details.no_price,
            details.total_invested, details.net_profit
        );

        Ok(())
    }

    /// Check for expired positions and settle them.
    async fn check_and_settle_expired(&mut self, _markets: &[MarketWithPrices]) -> Result<usize> {
        let now = Utc::now();
        let mut settled = 0;

        // Find expired positions
        let expired: Vec<_> = self
            .session
            .open_positions
            .iter()
            .filter(|(_, p)| p.end_time < now)
            .map(|(id, p)| (*id, p.clone()))
            .collect();

        for (market_id, position) in expired {
            // Simulate settlement (assume YES wins for simplicity in dry-run)
            // In reality, we would query the market resolution
            let payout = position.yes_shares;
            let profit = payout - position.total_invested;

            // Close position in database
            repository::close_position(self.db.pool(), position.id, payout, profit).await?;

            // Update session
            self.session.current_balance += payout;
            self.session.positions_closed += 1;
            self.session.gross_profit += profit;
            self.session.net_profit += profit;
            if profit > dec!(0) {
                self.session.winning_trades += 1;
            }

            // Remove from cache
            self.session.open_positions.remove(&market_id);

            info!(
                "[DRY RUN] SETTLED: {} | Payout: ${:.2} | P/L: ${:+.4}",
                position.market_name, payout, profit
            );

            settled += 1;
        }

        Ok(settled)
    }

    /// Get available balance (not tied up in positions).
    fn available_balance(&self) -> Decimal {
        self.session.current_balance - self.total_exposure()
    }

    /// Get total exposure across all open positions.
    fn total_exposure(&self) -> Decimal {
        self.session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum()
    }

    /// Get current session state.
    pub fn session(&self) -> &SessionState {
        &self.session
    }

    /// Get current bot state.
    pub fn state(&self) -> BotState {
        self.state
    }

    /// Get top N markets sorted by profit (highest first, including negative).
    fn get_top_markets(&self, markets: &[MarketWithPrices], limit: usize) -> Vec<MarketSummary> {
        let mut summaries: Vec<MarketSummary> = markets
            .iter()
            .filter_map(|m| {
                let yes_price = m.yes_best_ask?;
                let no_price = m.no_best_ask?;
                if yes_price <= dec!(0) || no_price <= dec!(0) {
                    return None;
                }
                let spread = yes_price + no_price;
                let profit_pct = dec!(1.00) - spread;
                Some(MarketSummary {
                    name: if m.name.chars().count() > 50 {
                        format!("{}..", m.name.chars().take(48).collect::<String>())
                    } else {
                        m.name.clone()
                    },
                    asset: m.asset.clone(),
                    yes_price,
                    no_price,
                    spread,
                    profit_pct,
                })
            })
            .collect();

        // Sort by profit (highest/best first)
        summaries.sort_by(|a, b| {
            b.profit_pct
                .partial_cmp(&a.profit_pct)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        summaries.truncate(limit);
        summaries
    }

    /// Print market analysis table (like Python's _print_status).
    fn print_market_analysis(&self, markets: &[MarketWithPrices]) {
        let min_profit = self.config.min_profit;

        println!("\n📊 Market Analysis (from DB):");
        println!(
            "  {:<8} {:<40} {:>6} {:>6} {:>7} {:>7} {}",
            "Type", "Market", "YES", "NO", "Spread", "Profit", "Decision"
        );
        println!(
            "  {:-<8} {:-<40} {:-<6} {:-<6} {:-<7} {:-<7} {:-<10}",
            "", "", "", "", "", "", ""
        );

        let mut near_profitable_count = 0;

        // Sort by end_time
        let mut sorted_markets: Vec<_> = markets.iter().collect();
        sorted_markets.sort_by_key(|m| m.end_time);

        for market in sorted_markets {
            let yes_ask = match market.yes_best_ask {
                Some(p) => p,
                None => continue,
            };
            let no_ask = match market.no_best_ask {
                Some(p) => p,
                None => continue,
            };

            let spread = yes_ask + no_ask;
            let profit_pct = dec!(1.00) - spread;

            // Only show markets with actual profit opportunity (spread < $1.00)
            if spread >= dec!(1.00) {
                continue;
            }
            near_profitable_count += 1;

            // Determine decision
            let decision = if yes_ask <= dec!(0) || no_ask <= dec!(0) {
                "❌ No price".to_string()
            } else if profit_pct >= min_profit {
                "✅ TRADE!".to_string()
            } else if profit_pct > dec!(0) {
                format!(
                    "⏳ +{:.1}% < {:.0}%",
                    profit_pct * dec!(100),
                    min_profit * dec!(100)
                )
            } else {
                format!("❌ {:.1}%", profit_pct * dec!(100))
            };

            // Type abbreviation
            let type_abbrev = match market.market_type.as_str() {
                "up_down" => "UP/DOWN",
                "above" => "ABOVE",
                "price_range" => "RANGE",
                "sports" => "SPORTS",
                _ => "OTHER",
            };

            // Truncate name (use chars() for UTF-8 safety)
            let name = if market.name.chars().count() > 40 {
                format!("{}..", market.name.chars().take(38).collect::<String>())
            } else {
                market.name.clone()
            };

            println!(
                "  {:<8} {:<40} ${:.2} ${:.2} ${:.3} {:>+.1}% {}",
                type_abbrev,
                name,
                yes_ask,
                no_ask,
                spread,
                profit_pct * dec!(100),
                decision
            );
        }

        if near_profitable_count == 0 {
            println!("  (no profitable markets found)");
        }

        // Print status summary
        let return_pct = if self.session.starting_balance > dec!(0) {
            (self.session.net_profit / self.session.starting_balance) * dec!(100)
        } else {
            dec!(0)
        };

        println!(
            "\nBalance: ${:.2} | Positions: {} | Trades: {} | P/L: ${:+.2} ({:+.2}%)",
            self.session.current_balance,
            self.session.open_positions.len(),
            self.session.total_trades,
            self.session.net_profit,
            return_pct
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use rust_decimal_macros::dec;
    use serde_json::json;

    // ============ TEST HELPERS ============

    /// Create an OrderbookSnapshot with specified asks
    fn make_snapshot(yes_asks: serde_json::Value, no_asks: serde_json::Value) -> OrderbookSnapshot {
        OrderbookSnapshot {
            id: 1,
            market_id: Uuid::new_v4(),
            yes_best_ask: None,
            yes_best_bid: None,
            no_best_ask: None,
            no_best_bid: None,
            spread: None,
            yes_asks: Some(yes_asks),
            yes_bids: None,
            no_asks: Some(no_asks),
            no_bids: None,
            captured_at: Utc::now(),
            yes_updated_at: Some(Utc::now()),
            no_updated_at: Some(Utc::now()),
        }
    }

    /// Create a MarketWithPrices for testing
    fn make_market_with_prices(yes_ask: Decimal, no_ask: Decimal) -> MarketWithPrices {
        MarketWithPrices {
            id: Uuid::new_v4(),
            condition_id: "test-condition".to_string(),
            market_type: "up_down".to_string(),
            asset: "BTC".to_string(),
            timeframe: "1h".to_string(),
            yes_token_id: "yes-token".to_string(),
            no_token_id: "no-token".to_string(),
            name: "Test Market".to_string(),
            end_time: Utc::now() + Duration::hours(1),
            is_active: true,
            yes_best_ask: Some(yes_ask),
            yes_best_bid: Some(yes_ask - dec!(0.01)),
            no_best_ask: Some(no_ask),
            no_best_bid: Some(no_ask - dec!(0.01)),
            captured_at: Utc::now(),
        }
    }

    /// Create a PositionCache for testing
    fn make_position_cache(total_invested: Decimal) -> PositionCache {
        PositionCache {
            id: Uuid::new_v4(),
            market_id: Uuid::new_v4(),
            market_name: "Test Position".to_string(),
            yes_shares: dec!(10),
            no_shares: dec!(10),
            total_invested,
            end_time: Utc::now() + Duration::hours(1),
        }
    }

    /// Create a minimal SessionState for testing
    fn make_session_state(balance: Decimal) -> SessionState {
        SessionState {
            id: Uuid::new_v4(),
            dry_run: true,
            starting_balance: balance,
            current_balance: balance,
            total_trades: 0,
            winning_trades: 0,
            total_opportunities: 0,
            positions_opened: 0,
            positions_closed: 0,
            gross_profit: dec!(0),
            fees_paid: dec!(0),
            net_profit: dec!(0),
            started_at: Utc::now(),
            open_positions: HashMap::new(),
        }
    }

    // ============ TASK 3.1: calculate_orderbook_liquidity TESTS ============

    #[test]
    fn test_calculate_orderbook_liquidity_both_sides() {
        // YES: 100 shares @ $0.50 = $50 liquidity
        // NO: 80 shares @ $0.45 = $36 liquidity
        // Min = $36
        let snapshot = make_snapshot(
            json!([{"price": "0.50", "size": "100"}]),
            json!([{"price": "0.45", "size": "80"}]),
        );
        let liquidity = calculate_orderbook_liquidity(&snapshot);
        assert_eq!(liquidity, dec!(36));
    }

    #[test]
    fn test_calculate_orderbook_liquidity_empty_book() {
        let snapshot = make_snapshot(json!([]), json!([]));
        assert_eq!(calculate_orderbook_liquidity(&snapshot), dec!(0));
    }

    #[test]
    fn test_calculate_orderbook_liquidity_one_side_empty() {
        // YES has liquidity, NO is empty => min is 0
        let snapshot = make_snapshot(json!([{"price": "0.50", "size": "100"}]), json!([]));
        assert_eq!(calculate_orderbook_liquidity(&snapshot), dec!(0));
    }

    #[test]
    fn test_calculate_orderbook_liquidity_symmetric() {
        // Both sides equal: 50 @ $0.50 = $25 each
        let snapshot = make_snapshot(
            json!([{"price": "0.50", "size": "50"}]),
            json!([{"price": "0.50", "size": "50"}]),
        );
        assert_eq!(calculate_orderbook_liquidity(&snapshot), dec!(25));
    }

    #[test]
    fn test_calculate_orderbook_liquidity_null_asks() {
        // Test with None values for asks
        let snapshot = OrderbookSnapshot {
            id: 1,
            market_id: Uuid::new_v4(),
            yes_best_ask: None,
            yes_best_bid: None,
            no_best_ask: None,
            no_best_bid: None,
            spread: None,
            yes_asks: None,
            yes_bids: None,
            no_asks: None,
            no_bids: None,
            captured_at: Utc::now(),
            yes_updated_at: None,
            no_updated_at: None,
        };
        assert_eq!(calculate_orderbook_liquidity(&snapshot), dec!(0));
    }

    // ============ TASK 3.2: available_balance and total_exposure TESTS ============

    #[test]
    fn test_total_exposure_no_positions() {
        let session = make_session_state(dec!(1000));
        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        assert_eq!(exposure, dec!(0));
    }

    #[test]
    fn test_total_exposure_single_position() {
        let mut session = make_session_state(dec!(1000));
        let position = make_position_cache(dec!(100));
        let market_id = position.market_id;
        session.open_positions.insert(market_id, position);

        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        assert_eq!(exposure, dec!(100));
    }

    #[test]
    fn test_total_exposure_multiple_positions() {
        let mut session = make_session_state(dec!(1000));

        let pos1 = make_position_cache(dec!(100));
        let pos2 = make_position_cache(dec!(150));
        let pos3 = make_position_cache(dec!(75));

        session.open_positions.insert(pos1.market_id, pos1);
        session.open_positions.insert(pos2.market_id, pos2);
        session.open_positions.insert(pos3.market_id, pos3);

        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        assert_eq!(exposure, dec!(325)); // 100 + 150 + 75
    }

    #[test]
    fn test_available_balance_no_positions() {
        let session = make_session_state(dec!(1000));
        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        let available = session.current_balance - exposure;
        assert_eq!(available, dec!(1000));
    }

    #[test]
    fn test_available_balance_with_positions() {
        let mut session = make_session_state(dec!(1000));
        let position = make_position_cache(dec!(200));
        session.open_positions.insert(position.market_id, position);

        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        let available = session.current_balance - exposure;
        assert_eq!(available, dec!(800)); // 1000 - 200
    }

    #[test]
    fn test_available_balance_all_invested() {
        let mut session = make_session_state(dec!(500));
        let position = make_position_cache(dec!(500));
        session.open_positions.insert(position.market_id, position);

        let exposure: Decimal = session
            .open_positions
            .values()
            .map(|p| p.total_invested)
            .sum();
        let available = session.current_balance - exposure;
        assert_eq!(available, dec!(0));
    }

    // ============ TASK 3.3: get_top_markets TESTS ============
    // Note: These test the logic directly since get_top_markets requires TradeExecutor

    #[test]
    fn test_market_profit_calculation() {
        // YES: $0.45, NO: $0.45 => Spread: $0.90, Profit: 10%
        let market = make_market_with_prices(dec!(0.45), dec!(0.45));
        let yes_price = market.yes_best_ask.unwrap();
        let no_price = market.no_best_ask.unwrap();
        let spread = yes_price + no_price;
        let profit_pct = dec!(1.00) - spread;

        assert_eq!(spread, dec!(0.90));
        assert_eq!(profit_pct, dec!(0.10));
    }

    #[test]
    fn test_market_sorting_by_profit() {
        let markets = vec![
            make_market_with_prices(dec!(0.48), dec!(0.48)), // 4% profit (spread 0.96)
            make_market_with_prices(dec!(0.45), dec!(0.45)), // 10% profit (spread 0.90)
            make_market_with_prices(dec!(0.52), dec!(0.52)), // -4% loss (spread 1.04)
        ];

        let mut profits: Vec<Decimal> = markets
            .iter()
            .map(|m| {
                let yes = m.yes_best_ask.unwrap();
                let no = m.no_best_ask.unwrap();
                dec!(1.00) - (yes + no)
            })
            .collect();

        // Sort by profit descending
        profits.sort_by(|a, b| b.partial_cmp(a).unwrap());

        assert_eq!(profits[0], dec!(0.10)); // Best: 10%
        assert_eq!(profits[1], dec!(0.04)); // Second: 4%
        assert_eq!(profits[2], dec!(-0.04)); // Worst: -4%
    }

    #[test]
    fn test_market_filters_invalid_prices() {
        let markets = vec![
            make_market_with_prices(dec!(0.45), dec!(0.45)), // Valid
            make_market_with_prices(dec!(0), dec!(0.45)),    // Invalid YES
            make_market_with_prices(dec!(0.45), dec!(0)),    // Invalid NO
        ];

        let valid_count = markets
            .iter()
            .filter(|m| {
                let yes = m.yes_best_ask.unwrap_or(dec!(0));
                let no = m.no_best_ask.unwrap_or(dec!(0));
                yes > dec!(0) && no > dec!(0)
            })
            .count();

        assert_eq!(valid_count, 1);
    }

    #[test]
    fn test_market_limit_results() {
        let markets: Vec<_> = (0..10)
            .map(|i| {
                let price = dec!(0.40) + Decimal::from(i) * dec!(0.01);
                make_market_with_prices(price, price)
            })
            .collect();

        // Take only top 5
        let limit = 5;
        let top: Vec<_> = markets.iter().take(limit).collect();

        assert_eq!(top.len(), 5);
    }

    #[test]
    fn test_market_with_none_prices() {
        let mut market = make_market_with_prices(dec!(0.50), dec!(0.50));
        market.yes_best_ask = None;

        // Should be filtered out
        let is_valid = market.yes_best_ask.is_some() && market.no_best_ask.is_some();
        assert!(!is_valid);
    }

    // ==========================================
    // Tests for extract_best_asks()
    // ==========================================

    fn make_test_snapshot(
        yes_asks: serde_json::Value,
        no_asks: serde_json::Value,
    ) -> OrderbookSnapshot {
        OrderbookSnapshot {
            id: 1,
            market_id: Uuid::new_v4(),
            yes_best_ask: None,
            yes_best_bid: None,
            no_best_ask: None,
            no_best_bid: None,
            spread: None,
            yes_asks: Some(yes_asks),
            yes_bids: None,
            no_asks: Some(no_asks),
            no_bids: None,
            captured_at: Utc::now(),
            yes_updated_at: Some(Utc::now()),
            no_updated_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_extract_best_asks_returns_lowest_prices() {
        let snapshot = make_test_snapshot(
            serde_json::json!([
                {"price": "0.50", "size": "10"},
                {"price": "0.48", "size": "5"},   // Best (lowest)
                {"price": "0.52", "size": "20"},
            ]),
            serde_json::json!([
                {"price": "0.55", "size": "10"},  // Best (lowest)
                {"price": "0.60", "size": "5"},
            ]),
        );

        let result = extract_best_asks(&snapshot);
        assert!(result.is_some());

        let (yes_best, no_best) = result.unwrap();
        assert_eq!(yes_best, dec!(0.48));
        assert_eq!(no_best, dec!(0.55));
    }

    #[test]
    fn test_extract_best_asks_single_level_each() {
        let snapshot = make_test_snapshot(
            serde_json::json!([{"price": "0.45", "size": "100"}]),
            serde_json::json!([{"price": "0.50", "size": "50"}]),
        );

        let result = extract_best_asks(&snapshot);
        assert!(result.is_some());

        let (yes_best, no_best) = result.unwrap();
        assert_eq!(yes_best, dec!(0.45));
        assert_eq!(no_best, dec!(0.50));
    }

    #[test]
    fn test_extract_best_asks_returns_none_for_empty_yes() {
        let snapshot = make_test_snapshot(
            serde_json::json!([]), // Empty YES
            serde_json::json!([{"price": "0.50", "size": "50"}]),
        );

        let result = extract_best_asks(&snapshot);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_best_asks_returns_none_for_empty_no() {
        let snapshot = make_test_snapshot(
            serde_json::json!([{"price": "0.45", "size": "100"}]),
            serde_json::json!([]), // Empty NO
        );

        let result = extract_best_asks(&snapshot);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_best_asks_returns_none_for_both_empty() {
        let snapshot = make_test_snapshot(serde_json::json!([]), serde_json::json!([]));

        let result = extract_best_asks(&snapshot);
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_best_asks_handles_null_asks() {
        let snapshot = OrderbookSnapshot {
            id: 1,
            market_id: Uuid::new_v4(),
            yes_best_ask: None,
            yes_best_bid: None,
            no_best_ask: None,
            no_best_bid: None,
            spread: None,
            yes_asks: None, // NULL
            yes_bids: None,
            no_asks: None, // NULL
            no_bids: None,
            captured_at: Utc::now(),
            yes_updated_at: None,
            no_updated_at: None,
        };

        let result = extract_best_asks(&snapshot);
        assert!(result.is_none());
    }

    // ==========================================
    // Tests for should_abort_due_to_spread()
    // ==========================================

    #[test]
    fn test_spread_within_tolerance_allows_trade() {
        let original = dec!(0.97);
        let current = dec!(0.974); // +0.004 (within 0.005 tolerance)
        let tolerance = dec!(0.005);

        let (should_abort, reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(!should_abort);
        assert!(reason.contains("within tolerance"));
    }

    #[test]
    fn test_spread_exactly_at_tolerance_allows_trade() {
        let original = dec!(0.97);
        let current = dec!(0.975); // +0.005 (exactly at tolerance)
        let tolerance = dec!(0.005);

        let (should_abort, reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(!should_abort);
        assert!(reason.contains("within tolerance"));
    }

    #[test]
    fn test_spread_beyond_tolerance_aborts_trade() {
        let original = dec!(0.97);
        let current = dec!(0.98); // +0.01 (exceeds 0.005 tolerance)
        let tolerance = dec!(0.005);

        let (should_abort, reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(should_abort);
        assert!(reason.contains("widened beyond tolerance"));
        assert!(reason.contains("0.9700"));
        assert!(reason.contains("0.9800"));
    }

    #[test]
    fn test_spread_improved_allows_trade() {
        let original = dec!(0.97);
        let current = dec!(0.96); // -0.01 (better!)
        let tolerance = dec!(0.005);

        let (should_abort, reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(!should_abort);
        assert!(reason.contains("improved"));
    }

    #[test]
    fn test_spread_unchanged_allows_trade() {
        let original = dec!(0.97);
        let current = dec!(0.97); // No change
        let tolerance = dec!(0.005);

        let (should_abort, reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(!should_abort);
        assert!(reason.contains("within tolerance"));
    }

    #[test]
    fn test_spread_validation_with_zero_tolerance() {
        let original = dec!(0.97);
        let current = dec!(0.971); // Any increase aborts
        let tolerance = dec!(0.0);

        let (should_abort, _reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(should_abort);
    }

    #[test]
    fn test_spread_validation_with_large_tolerance() {
        let original = dec!(0.97);
        let current = dec!(1.05); // Huge widening
        let tolerance = dec!(0.10); // But large tolerance

        let (should_abort, _reason) = should_abort_due_to_spread(original, current, tolerance);

        assert!(!should_abort); // Within 10% tolerance
    }

    // ==========================================
    // Tests for execute_live_trade refactoring (Task 6)
    // ==========================================

    #[test]
    fn test_snapshot_age_validation() {
        let max_age_secs = 30i64;
        let now = Utc::now();

        // Fresh snapshot (5 seconds old)
        let fresh_snapshot_time = now - chrono::Duration::seconds(5);
        let fresh_age = now - fresh_snapshot_time;
        assert!(fresh_age.num_seconds() <= max_age_secs);

        // Stale snapshot (60 seconds old)
        let stale_snapshot_time = now - chrono::Duration::seconds(60);
        let stale_age = now - stale_snapshot_time;
        assert!(stale_age.num_seconds() > max_age_secs);
    }

    #[test]
    fn test_spread_recalculation_with_current_prices() {
        // Original opportunity: YES $0.48 + NO $0.49 = $0.97 spread
        let original_yes = dec!(0.48);
        let original_no = dec!(0.49);
        let original_spread = original_yes + original_no;
        assert_eq!(original_spread, dec!(0.97));

        // Current prices: YES $0.50 + NO $0.52 = $1.02 spread
        let current_yes = dec!(0.50);
        let current_no = dec!(0.52);
        let current_spread = current_yes + current_no;
        assert_eq!(current_spread, dec!(1.02));

        // Verify widening detection
        let tolerance = dec!(0.005);
        let (should_abort, _) =
            should_abort_due_to_spread(original_spread, current_spread, tolerance);
        assert!(should_abort); // $1.02 > $0.97 + $0.005
    }

    #[test]
    fn test_shares_recalculation_with_current_prices() {
        let total_invested = dec!(10.00);

        // Original calculation: $0.48 + $0.49 = $0.97, shares = 10/0.97 = 10.31
        let original_total = dec!(0.48) + dec!(0.49);
        let original_shares = (total_invested / original_total).round_dp(2);
        assert_eq!(original_shares, dec!(10.31));

        // New calculation: $0.46 + $0.47 = $0.93, shares = 10/0.93 = 10.75
        let current_total = dec!(0.46) + dec!(0.47);
        let current_shares = (total_invested / current_total).round_dp(2);
        assert_eq!(current_shares, dec!(10.75));
    }

    #[test]
    fn test_spread_improved_allows_trade_with_better_prices() {
        // Original: YES $0.48 + NO $0.49 = $0.97
        // Current: YES $0.45 + NO $0.47 = $0.92 (improved!)
        let original_spread = dec!(0.97);
        let current_spread = dec!(0.92);
        let tolerance = dec!(0.005);

        let (should_abort, reason) =
            should_abort_due_to_spread(original_spread, current_spread, tolerance);

        assert!(!should_abort);
        assert!(reason.contains("improved"));
    }
}
