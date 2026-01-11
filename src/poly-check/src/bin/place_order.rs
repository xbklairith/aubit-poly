//! Place a real order on Polymarket.
//!
//! Usage:
//!   place_order --token-id <TOKEN_ID> --side buy --amount 1.0 --price 0.35

use std::str::FromStr;

use alloy::signers::local::LocalSigner;
use alloy::signers::Signer;
use anyhow::{Context, Result};
use clap::Parser;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use tracing::info;

const CLOB_HOST: &str = "https://clob.polymarket.com";

#[derive(Parser, Debug)]
#[command(name = "place_order")]
#[command(about = "Place an order on Polymarket")]
struct Args {
    /// Token ID to trade
    #[arg(long)]
    token_id: String,

    /// Side: buy or sell
    #[arg(long, default_value = "buy")]
    side: String,

    /// Amount in USDC to spend
    #[arg(long, default_value = "1.0")]
    amount: f64,

    /// Price per share (0.01 - 0.99)
    #[arg(long)]
    price: f64,

    /// Dry run - don't actually place the order
    #[arg(long)]
    dry_run: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("place_order=info".parse().unwrap()),
        )
        .init();

    // Load .env
    dotenvy::dotenv().ok();

    let args = Args::parse();

    // Validate inputs
    if args.price < 0.01 || args.price > 0.99 {
        anyhow::bail!("Price must be between 0.01 and 0.99");
    }

    // Calculate shares
    let shares = args.amount / args.price;
    let min_shares = 5.0;

    if shares < min_shares {
        let min_amount = min_shares * args.price;
        anyhow::bail!(
            "Order too small. Min {} shares required. At ${:.2} price, min amount is ${:.2}",
            min_shares,
            args.price,
            min_amount
        );
    }

    println!("\n{}", "=".repeat(50));
    println!("Order Details:");
    println!("{}", "=".repeat(50));
    println!(
        "  Token ID: {}...",
        &args.token_id[..20.min(args.token_id.len())]
    );
    println!("  Side: {}", args.side.to_uppercase());
    println!("  Price: ${:.2}", args.price);
    println!("  Amount: ${:.2}", args.amount);
    println!("  Shares: {:.2}", shares);
    if args.dry_run {
        println!("  Mode: DRY RUN (no order will be placed)");
    }
    println!("{}", "=".repeat(50));

    // Get private key
    let private_key =
        std::env::var("WALLET_PRIVATE_KEY").context("Missing WALLET_PRIVATE_KEY in environment")?;

    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };

    // Create signer
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

    // Authenticate with CLOB
    info!("Authenticating with Polymarket CLOB...");

    let mut auth_builder = ClobClient::new(CLOB_HOST, ClobConfig::default())?
        .authentication_builder(&signer)
        .signature_type(signature_type);

    if let Some(ref proxy) = proxy_wallet {
        let funder_address: alloy::primitives::Address =
            proxy.parse().context("Invalid proxy wallet address")?;
        auth_builder = auth_builder.funder(funder_address);
        info!("Funder (proxy): {}", proxy);
    }

    let clob_client = auth_builder
        .authenticate()
        .await
        .context("Failed to authenticate with Polymarket")?;

    println!("\n✓ Authenticated successfully");

    if args.dry_run {
        println!("\n[DRY RUN] Would place order:");
        println!(
            "  {} {:.2} shares of {} at ${:.2}",
            args.side.to_uppercase(),
            shares,
            &args.token_id[..20.min(args.token_id.len())],
            args.price
        );
        println!("\nTo execute for real, remove --dry-run flag");
        return Ok(());
    }

    // Build and place the order
    println!("\nPlacing order...");

    let side = match args.side.to_lowercase().as_str() {
        "buy" => polymarket_client_sdk::clob::types::Side::Buy,
        "sell" => polymarket_client_sdk::clob::types::Side::Sell,
        _ => anyhow::bail!("Invalid side: must be 'buy' or 'sell'"),
    };

    // Convert price to Decimal
    let price = rust_decimal::Decimal::from_str(&format!("{:.2}", args.price))
        .context("Invalid price format")?;

    // Convert size to Decimal (round to 2 decimal places)
    let size = rust_decimal::Decimal::from_str(&format!("{:.2}", shares))
        .context("Invalid size format")?;

    // Create limit order
    let order = clob_client
        .limit_order()
        .token_id(&args.token_id)
        .size(size)
        .price(price)
        .side(side)
        .build()
        .await
        .context("Failed to build order")?;

    // Sign the order
    let signed_order = clob_client
        .sign(&signer, order)
        .await
        .context("Failed to sign order")?;

    // Post the order
    let response = clob_client
        .post_order(signed_order)
        .await
        .context("Failed to post order")?;

    println!("\n{}", "=".repeat(50));
    println!("✓ Order placed successfully!");
    println!("{}", "=".repeat(50));
    println!("Response: {:?}", response);

    Ok(())
}
