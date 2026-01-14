//! Expiry Scalper - Bets on strongly-skewed crypto markets near expiry.
//!
//! Strategy: When a market is expiring soon (3 min) and price is skewed:
//! - Price > 0.75 → Buy YES (betting it stays high)

use std::collections::HashSet;
use std::str::FromStr;
use std::time::Duration;

use alloy::signers::local::PrivateKeySigner;
use alloy::signers::Signer;
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use polymarket_client_sdk::clob::types::SignatureType;
use polymarket_client_sdk::clob::{Client as ClobClient, Config as ClobConfig};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use common::{get_markets_with_fresh_orderbooks, Config, Database};

const CLOB_HOST: &str = "https://clob.polymarket.com";
const ORDER_TIMEOUT_SECS: u64 = 30;
/// Maximum allowed shares per order (sanity check)
const MAX_SHARES: Decimal = dec!(99.99);

/// Expiry Scalper - bets on skewed markets near expiry
#[derive(Parser, Debug)]
#[command(name = "expiry-scalper")]
#[command(about = "Bets on strongly-skewed crypto markets near expiry")]
struct Args {
    /// Poll interval in seconds (minimum 1)
    #[arg(long, default_value = "10")]
    interval_secs: u64,

    /// Expiry window in minutes (markets expiring within this time)
    #[arg(long, default_value = "3")]
    expiry_minutes: i64,

    /// Position size in USDC
    #[arg(long, default_value = "5")]
    position_size: f64,

    /// High price threshold (buy YES if price > this)
    #[arg(long, default_value = "0.75")]
    high_threshold: f64,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "30")]
    max_orderbook_age: i32,

    /// Assets to trade (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,

    /// Dry run mode (no actual trades)
    #[arg(long)]
    dry_run: bool,

    /// Limit price for orders (place at this price to ensure fill)
    #[arg(long, default_value = "0.99")]
    limit_price: f64,
}

/// Cached authentication state
struct CachedAuth {
    client: polymarket_client_sdk::clob::Client<
        polymarket_client_sdk::auth::state::Authenticated<polymarket_client_sdk::auth::Normal>,
    >,
    signer: PrivateKeySigner,
    #[allow(dead_code)]
    authenticated_at: DateTime<Utc>,
}

/// Validate CLI arguments
fn validate_args(args: &Args) -> Result<()> {
    if args.interval_secs < 1 {
        bail!("interval_secs must be at least 1");
    }
    if args.expiry_minutes < 1 {
        bail!("expiry_minutes must be at least 1");
    }
    if args.position_size <= 0.0 {
        bail!("position_size must be positive");
    }
    if args.position_size > 10000.0 {
        bail!("position_size cannot exceed 10000");
    }
    if args.high_threshold > 1.0 || args.high_threshold < 0.0 {
        bail!("high_threshold must be between 0 and 1");
    }
    if args.limit_price > 0.99 || args.limit_price < 0.01 {
        bail!("limit_price must be between 0.01 and 0.99");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Validate arguments
    validate_args(&args)?;

    info!("=== Expiry Scalper ===");
    info!("Expiry window: {} minutes", args.expiry_minutes);
    info!("Position size: ${}", args.position_size);
    info!("Threshold: buy if price >= {}", args.high_threshold);
    info!("Limit price: {} (order placed at this price)", args.limit_price);
    info!("Assets: {}", args.assets);
    info!("Poll interval: {}s", args.interval_secs);
    info!("Dry run: {}", args.dry_run);

    // Load config and connect to database
    dotenvy::dotenv().ok();
    let config = Config::from_env()?;
    let db = Database::connect(&config).await?;

    info!("Connected to database");

    // Convert threshold to Decimal
    let high_threshold = Decimal::try_from(args.high_threshold)
        .context("Invalid high_threshold")?;
    let position_size = Decimal::try_from(args.position_size)
        .context("Invalid position_size")?;
    let limit_price = Decimal::try_from(args.limit_price)
        .context("Invalid limit_price")?;

    // Track markets we've already bet on
    let mut traded_markets: HashSet<Uuid> = HashSet::new();

    // Cached authentication
    let mut cached_auth: Option<CachedAuth> = None;

    // Parse assets from CLI
    let assets: Vec<String> = args.assets
        .split(',')
        .map(|s| s.trim().to_uppercase())
        .filter(|s| !s.is_empty())
        .collect();

    if assets.is_empty() {
        bail!("No valid assets specified");
    }

    info!("Trading assets: {:?}", assets);

    // Main loop with graceful shutdown
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received shutdown signal, exiting...");
                break;
            }
            _ = run_cycle(
                &db,
                &assets,
                &args,
                high_threshold,
                position_size,
                limit_price,
                &mut traded_markets,
                &mut cached_auth,
            ) => {}
        }

        // Sleep until next cycle
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }

    info!("Shutdown complete");
    Ok(())
}

/// Run a single trading cycle
async fn run_cycle(
    db: &Database,
    assets: &[String],
    args: &Args,
    high_threshold: Decimal,
    position_size: Decimal,
    limit_price: Decimal,
    traded_markets: &mut HashSet<Uuid>,
    cached_auth: &mut Option<CachedAuth>,
) {
    let cycle_start = std::time::Instant::now();

    // Query markets expiring within the window
    let expiry_seconds = args.expiry_minutes * 60;
    let markets = match get_markets_with_fresh_orderbooks(
        db.pool(),
        args.max_orderbook_age,
        assets,
        expiry_seconds,
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to query markets: {:#}", e);
            return;
        }
    };

    info!(
        "Found {} markets expiring within {} minutes",
        markets.len(),
        args.expiry_minutes
    );

    // Process each market
    for market in &markets {
        // Skip if already traded
        if traded_markets.contains(&market.id) {
            debug!("Skipping {} - already traded", market.name);
            continue;
        }

        // Get YES price (ask price to buy YES)
        let yes_price = match market.yes_best_ask {
            Some(p) if p > dec!(0) && p <= dec!(1) => p,
            _ => {
                debug!("Skipping {} - invalid YES ask price: {:?}", market.name, market.yes_best_ask);
                continue;
            }
        };

        // Get NO price (ask price to buy NO)
        let no_price = match market.no_best_ask {
            Some(p) if p > dec!(0) && p <= dec!(1) => p,
            _ => {
                debug!("Skipping {} - invalid NO ask price: {:?}", market.name, market.no_best_ask);
                continue;
            }
        };

        // Determine which side to trade based on threshold (>= 0.75)
        // Buy YES if YES price >= threshold (strong YES signal)
        // Buy NO if NO price >= threshold (strong NO signal)
        let (side, token_id, market_price) = if yes_price >= high_threshold {
            ("YES", &market.yes_token_id, yes_price)
        } else if no_price >= high_threshold {
            ("NO", &market.no_token_id, no_price)
        } else {
            debug!(
                "Skipping {} - YES {} and NO {} both below threshold {}",
                market.name, yes_price, no_price, high_threshold
            );
            continue;
        };

        // Sanity check on market price
        if market_price < dec!(0.01) {
            warn!("Skipping {} - market price {} too low", market.name, market_price);
            continue;
        }

        if market_price > dec!(0.99) {
            warn!("Skipping {} - market price {} too high", market.name, market_price);
            continue;
        }

        // Calculate shares based on market price (expected fill price)
        let shares = position_size / market_price;

        if shares > MAX_SHARES {
            warn!(
                "Skipping {} - calculated shares {} exceeds max {}",
                market.name, shares, MAX_SHARES
            );
            continue;
        }

        info!(
            "[SIGNAL] {} {} - market @ ${}, order @ ${} ({} shares) - expires {:?}",
            side, market.name, market_price, limit_price, shares, market.end_time
        );

        if args.dry_run {
            info!("[DRY RUN] Would place order: {} {} shares @ ${} (market: ${})", side, shares, limit_price, market_price);
            traded_markets.insert(market.id);
            continue;
        }

        // Execute trade at limit_price (not market price) to ensure fill
        match execute_trade(
            cached_auth,
            token_id,
            shares,
            limit_price,
            side,
            &market.name,
        )
        .await
        {
            Ok(_) => {
                info!("[SUCCESS] Placed {} order for {} @ ${}", side, market.name, limit_price);
                traded_markets.insert(market.id);
            }
            Err(e) => {
                error!("[ERROR] Failed to place order for {}: {:#}", market.name, e);
            }
        }
    }

    // Cleanup expired markets from tracking set
    let now = Utc::now();
    let before_count = traded_markets.len();
    traded_markets.retain(|id| {
        markets.iter().any(|m| m.id == *id && m.end_time > now)
    });
    let cleaned = before_count - traded_markets.len();
    if cleaned > 0 {
        debug!("Cleaned {} expired markets from tracking set", cleaned);
    }

    let elapsed = cycle_start.elapsed();
    debug!("Cycle completed in {:?}", elapsed);
}

/// Execute a trade on Polymarket
async fn execute_trade(
    cached_auth: &mut Option<CachedAuth>,
    token_id: &str,
    shares: Decimal,
    price: Decimal,
    side: &str,
    market_name: &str,
) -> Result<()> {
    // Ensure authenticated
    let auth = ensure_authenticated(cached_auth).await?;

    // Normalize price and shares to remove trailing zeros
    // Polymarket SDK requires price decimal places <= tick size decimal places
    let price = price.normalize();
    let shares = shares.round_dp(2); // Round shares to 2 decimal places

    // Build order
    info!(
        "[TRADE] Building {} order: {} shares @ ${}",
        side, shares, price
    );

    let order = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client
            .limit_order()
            .token_id(token_id)
            .size(shares)
            .price(price)
            .side(polymarket_client_sdk::clob::types::Side::Buy)
            .build(),
    )
    .await
    .context("Order building timed out")?
    .context("Failed to build order")?;

    // Sign order
    let signed = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client.sign(&auth.signer, order),
    )
    .await
    .context("Order signing timed out")?
    .context("Failed to sign order")?;

    // Post order
    let result = timeout(
        Duration::from_secs(ORDER_TIMEOUT_SECS),
        auth.client.post_order(signed),
    )
    .await
    .context("Order posting timed out")?
    .context("Failed to post order")?;

    // Check result (post_order returns Vec<PostOrderResponse>)
    if let Some(order) = result.first() {
        let has_error = order.error_msg.as_ref().map(|e| !e.is_empty()).unwrap_or(false);
        if !order.order_id.is_empty() && !has_error {
            info!(
                "[TRADE] Order placed successfully: {} (order_id: {})",
                market_name, order.order_id
            );
            Ok(())
        } else if let Some(ref error) = order.error_msg {
            Err(anyhow::anyhow!("Order rejected: {}", error))
        } else {
            Err(anyhow::anyhow!("Order failed with unknown error"))
        }
    } else {
        Err(anyhow::anyhow!("No order response received"))
    }
}

/// Ensure we have a valid authenticated CLOB client.
/// Authenticates on first call, reuses cached client thereafter.
async fn ensure_authenticated(cached_auth: &mut Option<CachedAuth>) -> Result<&CachedAuth> {
    if cached_auth.is_some() {
        debug!("[AUTH] Using cached authentication");
        return Ok(cached_auth.as_ref().unwrap());
    }

    info!("[AUTH] Authenticating with Polymarket CLOB...");

    // Get private key
    let private_key = std::env::var("WALLET_PRIVATE_KEY")
        .context("Missing WALLET_PRIVATE_KEY environment variable")?;

    let private_key = if private_key.starts_with("0x") {
        private_key
    } else {
        format!("0x{}", private_key)
    };

    // Create signer
    let signer = PrivateKeySigner::from_str(&private_key)
        .context("Invalid private key format")?
        .with_chain_id(Some(POLYGON));

    // Determine signature type
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

    // Authenticate
    let client = auth_builder
        .authenticate()
        .await
        .context("Failed to authenticate with Polymarket")?;

    info!("[AUTH] Authentication successful");

    *cached_auth = Some(CachedAuth {
        client,
        signer,
        authenticated_at: Utc::now(),
    });

    // Safe because we just set it
    Ok(cached_auth.as_ref().unwrap())
}
