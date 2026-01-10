//! Polymarket API credential verification, balance, positions, and P&L check.
//!
//! Usage:
//!   poly-check              # Verify credentials, show balance and positions
//!   poly-check --balance    # Show balance only
//!   poly-check --positions  # Show positions only
//!   poly-check --pnl        # Show profit & loss report

use std::collections::HashMap;
use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use clap::Parser;
use polymarket_client_sdk::clob::types::{BalanceAllowanceRequest, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
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
    let private_key = std::env::var("WALLET_PRIVATE_KEY")
        .context("Missing WALLET_PRIVATE_KEY in environment")?;

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
        let funder_address: alloy::primitives::Address = proxy
            .parse()
            .context("Invalid proxy wallet address")?;
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

                            println!(
                                "    - Asset: {}...",
                                &asset[..20.min(asset.len())]
                            );
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

                        for market in market_list.iter().take(15) {
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

                        if market_list.len() > 15 {
                            println!("\n  ... and {} more markets", market_list.len() - 15);
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

    println!("\n{}", "=".repeat(50));
    println!("Done!");

    Ok(())
}
