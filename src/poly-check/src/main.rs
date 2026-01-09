//! Polymarket API credential verification, balance, and positions check.
//!
//! Usage:
//!   poly-check              # Verify credentials, show balance and positions
//!   poly-check --balance    # Show balance only
//!   poly-check --positions  # Show positions only

use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use clap::Parser;
use polymarket_client_sdk::clob::types::{BalanceAllowanceRequest, SignatureType};
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use tracing::{info, warn};

const CLOB_HOST: &str = "https://clob.polymarket.com";

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

    println!("\n{}", "=".repeat(50));
    println!("Done!");

    Ok(())
}
