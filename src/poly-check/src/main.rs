//! Polymarket API credential verification, balance, positions, P&L, and audit check.
//!
//! Usage:
//!   poly-check                 # Verify credentials, show balance and positions
//!   poly-check --balance       # Show balance only
//!   poly-check --positions     # Show positions only
//!   poly-check --pnl           # Show profit & loss report
//!   poly-check --audit-prices  # Audit orderbook price data quality

use std::collections::HashMap;
use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use polymarket_client_sdk::clob::types::{BalanceAllowanceRequest, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tracing::{info, warn};

const CLOB_HOST: &str = "https://clob.polymarket.com";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Activity {
    side: Option<String>,
    #[serde(rename = "type")]
    activity_type: Option<String>,
    size: f64,
    price: f64,
    usdc_size: Option<f64>,
    title: Option<String>,
    condition_id: String,
    timestamp: i64,
}

#[derive(Default)]
struct MarketPnL {
    title: String,
    spent: f64,
    received: f64,
    last_activity: i64,
}

#[derive(Parser, Debug)]
#[command(name = "poly-check")]
#[command(about = "Verify Polymarket API credentials and check account status")]
struct Args {
    /// Show balance only
    #[arg(long)]
    balance: bool,

    /// Show positions only
    #[arg(long)]
    positions: bool,

    /// Show profit & loss report
    #[arg(long)]
    pnl: bool,

    /// Show all markets (no limit)
    #[arg(long)]
    all: bool,

    /// Audit orderbook price data quality (DB vs depth vs live API)
    #[arg(long)]
    audit_prices: bool,

    /// Max markets to audit (default: 10)
    #[arg(long, default_value = "10")]
    audit_limit: i64,

    /// Assets to audit (comma-separated, e.g., BTC,ETH,SOL)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    audit_assets: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("poly_check=info".parse().unwrap()),
        )
        .init();

    // Load .env
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Get private key
    let private_key =
        std::env::var("WALLET_PRIVATE_KEY").context("Missing WALLET_PRIVATE_KEY in environment")?;

    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };

    // Create signer using SDK's re-exported LocalSigner from alloy
    let signer = LocalSigner::from_str(&private_key)
        .context("Invalid private key format")?
        .with_chain_id(Some(POLYGON));

    let signer_address = signer.address();
    info!("Signer address: {}", signer_address);

    // Check for proxy wallet
    let proxy_wallet = std::env::var("POLYMARKET_WALLET_ADDRESS").ok();
    let signature_type = if proxy_wallet.is_some() {
        SignatureType::GnosisSafe
    } else {
        SignatureType::Eoa
    };

    info!("Signature type: {:?}", signature_type);
    if let Some(ref proxy) = proxy_wallet {
        info!("Proxy wallet: {}", proxy);
    }

    // Authenticate with CLOB
    info!("Authenticating with Polymarket CLOB...");

    let mut auth_builder = ClobClient::new(CLOB_HOST, ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(signature_type);

    // Add funder address for GnosisSafe signature type
    if let Some(ref proxy) = proxy_wallet {
        let funder_address: alloy::primitives::Address =
            proxy.parse().context("Invalid proxy wallet address")?;
        auth_builder = auth_builder.funder(funder_address);
    }

    let clob_client = auth_builder
        .authenticate()
        .await
        .context("Failed to authenticate with Polymarket")?;

    println!("\n{}", "=".repeat(50));
    println!("API Credentials Verified Successfully!");
    println!("{}", "=".repeat(50));

    // Get API keys to prove auth works
    let api_keys = clob_client
        .api_keys()
        .await
        .context("Failed to get API keys")?;

    println!("\nAPI Keys: {:?}", api_keys);

    // Show balance if requested or default
    if args.balance || (!args.balance && !args.positions) {
        println!("\n{}", "-".repeat(50));
        println!("Balance:");

        // Use default balance request
        let balance_request = BalanceAllowanceRequest::default();

        match clob_client.balance_allowance(&balance_request).await {
            Ok(balance) => {
                // Balance is in USDC micro units (6 decimals)
                let balance_usdc = balance.balance / rust_decimal::Decimal::from(1_000_000);
                println!("  USDC Balance: ${:.2}", balance_usdc);
                println!("  Raw Balance: {} micro-USDC", balance.balance);
            }
            Err(e) => {
                warn!("Could not fetch balance: {}", e);
                println!("  (Balance fetch failed - may need proxy wallet setup)");
            }
        }
    }

    // Show positions if requested or default
    if args.positions || (!args.balance && !args.positions) {
        println!("\n{}", "-".repeat(50));
        println!("Positions:");

        // Use proxy wallet address if available, otherwise signer address
        let user_address = if let Some(ref proxy) = proxy_wallet {
            proxy.clone()
        } else {
            format!("{}", signer_address)
        };

        // Fetch positions via HTTP (Data API)
        let data_url = format!(
            "https://data-api.polymarket.com/positions?user={}",
            user_address.to_lowercase()
        );

        let http_client = reqwest::Client::new();
        match http_client.get(&data_url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let positions: Vec<serde_json::Value> = resp.json().await.unwrap_or_default();
                    if positions.is_empty() {
                        println!("  No open positions");
                    } else {
                        println!("  Found {} position(s):", positions.len());
                        for pos in positions.iter().take(10) {
                            let asset = pos.get("asset").and_then(|v| v.as_str()).unwrap_or("?");
                            let size = pos.get("size").and_then(|v| v.as_str()).unwrap_or("0");
                            let avg_price = pos.get("avgPrice").and_then(|v| v.as_str());

                            println!("    - Asset: {}...", &asset[..20.min(asset.len())]);
                            println!("      Size: {}", size);
                            if let Some(avg) = avg_price {
                                println!("      Avg Price: {}", avg);
                            }
                        }
                        if positions.len() > 10 {
                            println!("    ... and {} more", positions.len() - 10);
                        }
                    }
                } else {
                    println!("  (Positions fetch failed: {})", resp.status());
                }
            }
            Err(e) => {
                warn!("Could not fetch positions: {}", e);
                println!("  (Positions fetch failed)");
            }
        }
    }

    // Show P&L report if requested
    if args.pnl {
        println!("\n{}", "=".repeat(50));
        println!("Profit & Loss Report");
        println!("{}", "=".repeat(50));

        // Use proxy wallet address if available, otherwise signer address
        let user_address = if let Some(ref proxy) = proxy_wallet {
            proxy.clone()
        } else {
            format!("{}", signer_address)
        };

        // Fetch activity via HTTP (Data API)
        let activity_url = format!(
            "https://data-api.polymarket.com/activity?user={}&limit=100",
            user_address.to_lowercase()
        );

        let http_client = reqwest::Client::new();
        match http_client.get(&activity_url).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let activities: Vec<Activity> = resp.json().await.unwrap_or_default();

                    if activities.is_empty() {
                        println!("\n  No trading activity found");
                    } else {
                        // Calculate totals
                        let mut total_spent = 0.0;
                        let mut total_received = 0.0;
                        let mut markets: HashMap<String, MarketPnL> = HashMap::new();

                        for activity in &activities {
                            let market = markets
                                .entry(activity.condition_id.clone())
                                .or_insert_with(|| MarketPnL {
                                    title: activity.title.clone().unwrap_or_default(),
                                    ..Default::default()
                                });

                            // Track most recent activity
                            if activity.timestamp > market.last_activity {
                                market.last_activity = activity.timestamp;
                            }

                            if activity.side.as_deref() == Some("BUY") {
                                let cost = activity.size * activity.price;
                                total_spent += cost;
                                market.spent += cost;
                            } else if activity.side.as_deref() == Some("SELL") {
                                let proceeds = activity.size * activity.price;
                                total_received += proceeds;
                                market.received += proceeds;
                            } else if activity.activity_type.as_deref() == Some("REDEEM") {
                                let received = activity.usdc_size.unwrap_or(0.0);
                                total_received += received;
                                market.received += received;
                            }
                        }

                        let net_pnl = total_received - total_spent;
                        let roi = if total_spent > 0.0 {
                            (net_pnl / total_spent) * 100.0
                        } else {
                            0.0
                        };

                        // Count wins/losses
                        let mut wins = 0;
                        let mut total_markets = 0;
                        for market in markets.values() {
                            if market.spent > 0.0 || market.received > 0.0 {
                                total_markets += 1;
                                if market.received > market.spent {
                                    wins += 1;
                                }
                            }
                        }

                        // Print summary
                        println!("\nSummary:");
                        println!("  Total Spent:    ${:.2}", total_spent);
                        println!("  Total Received: ${:.2}", total_received);
                        let sign = if net_pnl >= 0.0 { "+" } else { "" };
                        println!("  Net P&L:        {}${:.2}", sign, net_pnl);
                        println!("  ROI:            {}{:.2}%", sign, roi);
                        println!(
                            "  Win Rate:       {}/{} ({:.0}%)",
                            wins,
                            total_markets,
                            if total_markets > 0 {
                                (wins as f64 / total_markets as f64) * 100.0
                            } else {
                                0.0
                            }
                        );

                        // Sort markets by most recent activity
                        let mut market_list: Vec<_> = markets
                            .values()
                            .filter(|m| m.spent > 0.0 || m.received > 0.0)
                            .collect();
                        market_list.sort_by(|a, b| b.last_activity.cmp(&a.last_activity));

                        // Print by market
                        println!("\n{}", "-".repeat(50));
                        println!("By Market (most recent first):\n");

                        let display_limit = if args.all { market_list.len() } else { 15 };
                        for market in market_list.iter().take(display_limit) {
                            let pnl = market.received - market.spent;
                            let sign = if pnl >= 0.0 { "+" } else { "" };
                            let title = if market.title.chars().count() > 45 {
                                format!("{}...", market.title.chars().take(45).collect::<String>())
                            } else {
                                market.title.clone()
                            };
                            println!("  [{}${:.2}] {}", sign, pnl, title);
                            println!(
                                "    Spent: ${:.2} | Received: ${:.2}",
                                market.spent, market.received
                            );
                        }

                        if !args.all && market_list.len() > 15 {
                            println!(
                                "\n  ... and {} more markets (use --all to show all)",
                                market_list.len() - 15
                            );
                        }
                    }
                } else {
                    println!("  (Activity fetch failed: {})", resp.status());
                }
            }
            Err(e) => {
                warn!("Could not fetch activity: {}", e);
                println!("  (Activity fetch failed)");
            }
        }
    }

    // Run audit if requested
    if args.audit_prices {
        run_audit_prices(&args).await?;
    }

    println!("\n{}", "=".repeat(50));
    println!("Done!");

    Ok(())
}

/// CLOB book response for a single token.
#[derive(Debug, Deserialize)]
struct ClobBook {
    market: Option<String>,
    asset_id: String,
    bids: Vec<ClobPriceLevel>,
    asks: Vec<ClobPriceLevel>,
    timestamp: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClobPriceLevel {
    price: String,
    size: String,
}

impl ClobBook {
    fn best_bid(&self) -> Option<Decimal> {
        self.bids
            .iter()
            .filter_map(|p| p.price.parse::<Decimal>().ok())
            .max()
    }

    fn best_ask(&self) -> Option<Decimal> {
        self.asks
            .iter()
            .filter_map(|p| p.price.parse::<Decimal>().ok())
            .min()
    }
}

/// Audit orderbook price data quality.
/// Cross-checks: DB scalar prices vs DB depth arrays vs live CLOB API.
async fn run_audit_prices(args: &Args) -> Result<()> {
    println!("\n{}", "=".repeat(50));
    println!("Orderbook Price Data Quality Audit");
    println!("{}", "=".repeat(50));

    // Create HTTP client
    let http_client = reqwest::Client::new();

    // Connect to database
    let config = common::Config::from_env()?;
    let db = common::Database::connect(&config).await?;

    // Parse assets
    let assets: Vec<String> = args
        .audit_assets
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();
    println!("\nAuditing assets: {:?}", assets);
    println!("Max markets: {}", args.audit_limit);

    // Step 1: Get list of active markets from DB (just market info, no orderbook requirement)
    let all_markets = common::get_active_markets_expiring_within(
        db.pool(),
        1, // 1 hour expiry
        args.audit_limit,
    )
    .await?;

    // Filter by assets
    let markets: Vec<_> = if assets.iter().any(|a| a.eq_ignore_ascii_case("ALL")) {
        all_markets
    } else {
        all_markets
            .into_iter()
            .filter(|m| assets.contains(&m.asset))
            .collect()
    };

    if markets.is_empty() {
        println!("\n  No active markets found.");
        return Ok(());
    }

    println!("\nFound {} active markets to audit", markets.len());

    let mut total_checked = 0;
    let mut db_consistency_errors = 0;
    let mut api_mismatch_count = 0;

    for market in markets.iter().take(args.audit_limit as usize) {
        total_checked += 1;
        println!("\n{}", "-".repeat(50));
        println!("Market: {}", &market.name[..market.name.len().min(50)]);
        println!("  Condition: {}", market.condition_id);
        println!("  Asset: {}, Expiry: {}", market.asset, market.end_time);

        // Step 1: Fetch live API first (slower operation)
        println!("\n  Fetching live orderbook from CLOB API...");
        let api_start = std::time::Instant::now();
        let yes_book = fetch_clob_book(&http_client, &market.yes_token_id).await;
        let no_book = fetch_clob_book(&http_client, &market.no_token_id).await;
        let api_elapsed_ms = api_start.elapsed().as_millis();

        // Step 2: Immediately fetch fresh DB snapshot (fast, minimizes time skew)
        let db_start = std::time::Instant::now();
        let snapshot = common::get_latest_orderbook_snapshot(db.pool(), market.id).await?;
        let db_elapsed_ms = db_start.elapsed().as_millis();

        println!(
            "  API fetch: {}ms, DB fetch: {}ms",
            api_elapsed_ms, db_elapsed_ms
        );

        if let Some(snap) = snapshot {
            let age_secs = (Utc::now() - snap.captured_at).num_seconds();
            println!("  DB snapshot age: {}s", age_secs);

            // Step 3: Compare API vs DB (minimal time skew now)
            println!("\n  DB Prices:");
            println!(
                "    YES: ask={:?}, bid={:?}",
                snap.yes_best_ask, snap.yes_best_bid
            );
            println!(
                "    NO:  ask={:?}, bid={:?}",
                snap.no_best_ask, snap.no_best_bid
            );

            // Skip markets with empty orderbooks (bid=0, ask=1.0 indicates no real data)
            let is_empty_book = matches!(
                (snap.yes_best_bid, snap.yes_best_ask, snap.no_best_bid, snap.no_best_ask),
                (Some(y_bid), Some(y_ask), Some(n_bid), Some(n_ask))
                if y_bid == dec!(0) && y_ask == dec!(1) && n_bid == dec!(0) && n_ask == dec!(1)
            );
            if is_empty_book {
                println!("  ⏭️  Skipping: empty orderbook (bid=0, ask=1.0)");
                continue;
            }

            if let Ok(yes_book) = yes_book {
                let live_yes_ask = yes_book.best_ask();
                let live_yes_bid = yes_book.best_bid();
                println!("  Live YES: ask={:?}, bid={:?}", live_yes_ask, live_yes_bid);

                if !prices_match(snap.yes_best_ask, live_yes_ask) {
                    println!(
                        "    ⚠️  YES ASK mismatch: DB {:?} vs Live {:?}",
                        snap.yes_best_ask, live_yes_ask
                    );
                    api_mismatch_count += 1;
                }
                if !prices_match(snap.yes_best_bid, live_yes_bid) {
                    println!(
                        "    ⚠️  YES BID mismatch: DB {:?} vs Live {:?}",
                        snap.yes_best_bid, live_yes_bid
                    );
                }
            } else {
                println!("  ⚠️  Failed to fetch YES book: {:?}", yes_book.err());
            }

            if let Ok(no_book) = no_book {
                let live_no_ask = no_book.best_ask();
                let live_no_bid = no_book.best_bid();
                println!("  Live NO:  ask={:?}, bid={:?}", live_no_ask, live_no_bid);

                if !prices_match(snap.no_best_ask, live_no_ask) {
                    println!(
                        "    ⚠️  NO ASK mismatch: DB {:?} vs Live {:?}",
                        snap.no_best_ask, live_no_ask
                    );
                    api_mismatch_count += 1;
                }
                if !prices_match(snap.no_best_bid, live_no_bid) {
                    println!(
                        "    ⚠️  NO BID mismatch: DB {:?} vs Live {:?}",
                        snap.no_best_bid, live_no_bid
                    );
                }
            } else {
                println!("  ⚠️  Failed to fetch NO book: {:?}", no_book.err());
            }

            // Step 4: DB consistency check (scalar vs depth)
            let (yes_depth_ask, yes_depth_bid) =
                extract_best_from_depth(&snap.yes_asks, &snap.yes_bids);
            let (no_depth_ask, no_depth_bid) =
                extract_best_from_depth(&snap.no_asks, &snap.no_bids);

            let yes_ask_match = prices_match(snap.yes_best_ask, yes_depth_ask);
            let yes_bid_match = prices_match(snap.yes_best_bid, yes_depth_bid);
            let no_ask_match = prices_match(snap.no_best_ask, no_depth_ask);
            let no_bid_match = prices_match(snap.no_best_bid, no_depth_bid);

            if !yes_ask_match || !yes_bid_match || !no_ask_match || !no_bid_match {
                println!("\n  ❌ DB CONSISTENCY ERROR: scalar != depth");
                println!(
                    "    Depth-derived: YES ask={:?} bid={:?}, NO ask={:?} bid={:?}",
                    yes_depth_ask, yes_depth_bid, no_depth_ask, no_depth_bid
                );
                db_consistency_errors += 1;
            } else {
                println!("\n  ✅ DB consistency OK (scalar matches depth)");
            }
        } else {
            println!("  ⚠️  No snapshot found in DB");
        }

        // Rate limit API calls
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Summary
    println!("\n{}", "=".repeat(50));
    println!("Audit Summary:");
    println!("  Markets checked: {}", total_checked);
    println!("  DB consistency errors: {}", db_consistency_errors);
    println!("  API mismatch count: {}", api_mismatch_count);

    if db_consistency_errors > 0 {
        println!("\n  ❌ CRITICAL: DB has scalar/depth inconsistencies!");
    }
    if api_mismatch_count > 0 {
        println!("\n  ⚠️  Some DB prices differ from live API (expected if market is moving)");
    }
    if db_consistency_errors == 0 && api_mismatch_count == 0 {
        println!("\n  ✅ All checks passed!");
    }

    Ok(())
}

/// Extract best ask/bid from JSONB depth arrays.
fn extract_best_from_depth(
    asks: &Option<serde_json::Value>,
    bids: &Option<serde_json::Value>,
) -> (Option<Decimal>, Option<Decimal>) {
    let best_ask = asks.as_ref().and_then(|v| {
        v.as_array().and_then(|arr| {
            arr.iter()
                .filter_map(|level| {
                    level.get("price").and_then(|p| {
                        p.as_str()
                            .and_then(|s| s.parse::<Decimal>().ok())
                            .or_else(|| {
                                p.as_f64()
                                    .map(|f| Decimal::from_f64_retain(f).unwrap_or_default())
                            })
                    })
                })
                .min()
        })
    });

    let best_bid = bids.as_ref().and_then(|v| {
        v.as_array().and_then(|arr| {
            arr.iter()
                .filter_map(|level| {
                    level.get("price").and_then(|p| {
                        p.as_str()
                            .and_then(|s| s.parse::<Decimal>().ok())
                            .or_else(|| {
                                p.as_f64()
                                    .map(|f| Decimal::from_f64_retain(f).unwrap_or_default())
                            })
                    })
                })
                .max()
        })
    });

    (best_ask, best_bid)
}

/// Check if two optional prices match (within tolerance for floating point).
fn prices_match(a: Option<Decimal>, b: Option<Decimal>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => (a - b).abs() < Decimal::new(1, 4), // 0.0001 tolerance
        (None, None) => true,
        _ => false,
    }
}

/// Fetch orderbook from CLOB REST API.
async fn fetch_clob_book(http_client: &reqwest::Client, token_id: &str) -> Result<ClobBook> {
    let url = format!("{}/book?token_id={}", CLOB_HOST, token_id);
    let resp = http_client
        .get(&url)
        .send()
        .await
        .context("Failed to fetch CLOB book")?;

    if !resp.status().is_success() {
        anyhow::bail!("CLOB API error: {}", resp.status());
    }

    let book: ClobBook = resp.json().await.context("Failed to parse CLOB book")?;
    Ok(book)
}
