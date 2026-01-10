//! Rust Trade Executor - Spread arbitrage trading bot.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tokio::signal;
use tokio::time::sleep;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use common::{Config, Database};

mod config;
mod detector;
mod executor;
mod metrics;
mod models;

use config::ExecutorConfig;
use executor::TradeExecutor;
use metrics::{AggregateMetrics, CycleMetrics};

/// Rust Trade Executor - spread arbitrage trading bot
#[derive(Parser, Debug)]
#[command(name = "trade-executor")]
#[command(about = "Spread arbitrage trading bot (Rust implementation)")]
struct Args {
    /// Run once and exit
    #[arg(long)]
    once: bool,

    /// Poll interval in milliseconds (0 = no delay)
    #[arg(long, default_value = "0")]
    interval_ms: u64,

    /// Dry run mode (simulated trading). Use --dry-run=false for live trading.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    dry_run: bool,

    /// Starting balance for dry run
    #[arg(long, default_value = "10000")]
    starting_balance: f64,

    /// Minimum profit percentage (e.g., 0.01 = 1%)
    #[arg(long, default_value = "0.01")]
    min_profit: f64,

    /// Maximum position size
    #[arg(long, default_value = "100")]
    max_position_size: f64,

    /// Maximum total exposure
    #[arg(long, default_value = "1000")]
    max_total_exposure: f64,

    /// Assets to trade (comma-separated)
    #[arg(long, default_value = "BTC,ETH,SOL,XRP")]
    assets: String,

    /// Maximum orderbook age in seconds
    #[arg(long, default_value = "30")]
    max_orderbook_age: i32,

    /// Maximum time to market expiry in seconds
    #[arg(long, default_value = "3600")]
    max_time_to_expiry: i64,

    /// Enable verbose timing output
    #[arg(long)]
    verbose_timing: bool,

    /// Run benchmark mode
    #[arg(long)]
    benchmark: bool,

    /// Number of cycles for benchmark
    #[arg(long, default_value = "100")]
    cycles: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let args = Args::parse();

    info!("Rust Trade Executor starting...");
    info!("Mode: {}", if args.dry_run { "DRY RUN" } else { "LIVE" });

    // Load config and connect to database
    let base_config = Config::from_env()?;
    let db = Arc::new(Database::connect(&base_config).await?);

    // Warmup database connections
    db.health_check().await?;
    info!("Database connected");

    // Build executor config
    let exec_config = ExecutorConfig {
        dry_run: args.dry_run,
        starting_balance: Decimal::try_from(args.starting_balance)?,
        min_profit: Decimal::try_from(args.min_profit)?,
        max_position_size: Decimal::try_from(args.max_position_size)?,
        max_total_exposure: Decimal::try_from(args.max_total_exposure)?,
        max_orderbook_age_secs: args.max_orderbook_age,
        max_price_age_secs: 60,
        max_time_to_expiry_secs: args.max_time_to_expiry,
        fee_rate: dec!(0.001), // 0.1%
        assets: args
            .assets
            .split(',')
            .map(|s| s.trim().to_string())
            .collect(),
    };

    // Create executor
    let mut executor = TradeExecutor::new(exec_config.clone(), db).await?;

    // Print startup banner
    print_banner(&args, &exec_config);

    // Run in appropriate mode
    if args.benchmark {
        run_benchmark(&mut executor, args.cycles).await?;
    } else {
        // Setup graceful shutdown
        let shutdown = async {
            signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl+c");
            info!("Shutdown signal received");
        };

        tokio::select! {
            result = run_loop(&mut executor, &args) => {
                if let Err(e) = result {
                    error!("Error in main loop: {}", e);
                }
            }
            _ = shutdown => {}
        }
    }

    // Print final summary
    print_summary(&executor);

    Ok(())
}

/// Report interval for quiet periods (15 minutes).
const REPORT_INTERVAL_SECS: u64 = 15 * 60;

/// Run the main trading loop.
async fn run_loop(executor: &mut TradeExecutor, args: &Args) -> Result<()> {
    let mut last_report = Instant::now();
    let mut cycle_count: u64 = 0;

    loop {
        let metrics = executor.run_cycle(args.verbose_timing).await?;
        cycle_count += 1;

        let since_last_report = last_report.elapsed().as_secs();
        let should_report = metrics.opportunities_found > 0
            || metrics.trades_executed > 0
            || since_last_report >= REPORT_INTERVAL_SECS;

        if args.verbose_timing && should_report {
            if metrics.opportunities_found > 0 || metrics.trades_executed > 0 {
                // Normal report when we have opportunities
                info!(
                    "Cycle: {}ms (query: {}ms, detect: {}ms, exec: {}ms, settle: {}ms) | Markets: {} | Opps: {} | Trades: {}",
                    metrics.total_cycle_ms,
                    metrics.market_query_ms,
                    metrics.detection_ms,
                    metrics.execution_ms,
                    metrics.settlement_ms,
                    metrics.markets_scanned,
                    metrics.opportunities_found,
                    metrics.trades_executed
                );
            } else {
                // Periodic report with top 10 markets
                print_periodic_report(&metrics, cycle_count, since_last_report);
            }
            last_report = Instant::now();
        }

        if args.once {
            break;
        }

        if args.interval_ms > 0 {
            sleep(Duration::from_millis(args.interval_ms)).await;
        }
    }

    Ok(())
}

/// Print periodic report with top 10 markets.
fn print_periodic_report(metrics: &CycleMetrics, cycles: u64, elapsed_secs: u64) {
    let mins = elapsed_secs / 60;
    info!(
        "📊 Periodic Report | {} cycles | {}m elapsed | Markets: {} | No opportunities",
        cycles, mins, metrics.markets_scanned
    );

    if !metrics.top_markets.is_empty() {
        println!("\n  Top 10 Markets by Profit:");
        println!(
            "  {:<6} {:<50} {:>6} {:>6} {:>7} {:>8}",
            "Asset", "Market", "YES", "NO", "Spread", "Profit"
        );
        println!(
            "  {:-<6} {:-<50} {:-<6} {:-<6} {:-<7} {:-<8}",
            "", "", "", "", "", ""
        );

        for m in &metrics.top_markets {
            println!(
                "  {:<6} {:<50} ${:.2} ${:.2} ${:.3} {:>+.2}%",
                m.asset,
                m.name,
                m.yes_price,
                m.no_price,
                m.spread,
                m.profit_pct * dec!(100)
            );
        }
        println!();
    }
}

/// Run benchmark mode - execute N cycles and report statistics.
async fn run_benchmark(executor: &mut TradeExecutor, cycles: usize) -> Result<()> {
    info!("Running {} benchmark cycles...", cycles);

    let mut all_metrics: Vec<CycleMetrics> = Vec::with_capacity(cycles);

    for i in 0..cycles {
        let metrics = executor.run_cycle(false).await?; // No verbose in benchmark
        all_metrics.push(metrics);

        if (i + 1) % 10 == 0 {
            info!("Completed {} / {} cycles...", i + 1, cycles);
        }
    }

    let aggregate = AggregateMetrics::from_cycles(&all_metrics);
    aggregate.print_report("RUST TRADE EXECUTOR BENCHMARK");

    Ok(())
}

/// Print startup banner.
fn print_banner(args: &Args, config: &ExecutorConfig) {
    println!(
        r#"
═══════════════════════════════════════════════════════════════
  RUST TRADE EXECUTOR - {} MODE
═══════════════════════════════════════════════════════════════
  Assets:           {}
  Min profit:       {:.1}%
  Max position:     ${}
  Max exposure:     ${}
  Poll interval:    {}ms
  Orderbook age:    {}s max
  Time to expiry:   {}s max
  Balance:          ${}
═══════════════════════════════════════════════════════════════
"#,
        if args.dry_run { "DRY RUN" } else { "LIVE" },
        args.assets,
        f64::from(args.min_profit) * 100.0,
        config.max_position_size,
        config.max_total_exposure,
        args.interval_ms,
        config.max_orderbook_age_secs,
        config.max_time_to_expiry_secs,
        config.starting_balance,
    );
}

/// Print final session summary.
fn print_summary(executor: &TradeExecutor) {
    let session = executor.session();
    let return_pct = if session.starting_balance > dec!(0) {
        (session.net_profit / session.starting_balance) * dec!(100)
    } else {
        dec!(0)
    };

    println!(
        r#"
═══════════════════════════════════════════════════════════════
  SESSION SUMMARY
═══════════════════════════════════════════════════════════════
  Opportunities detected:  {}
  Positions opened:        {}
  Positions closed:        {}

  Total trades:     {}
  Winning trades:   {}

  Gross profit:   ${:+.4}
  Fees paid:      ${:.4}
  Net profit:     ${:+.4}
  Return:         {:+.2}%
═══════════════════════════════════════════════════════════════
"#,
        session.total_opportunities,
        session.positions_opened,
        session.positions_closed,
        session.total_trades,
        session.winning_trades,
        session.gross_profit,
        session.fees_paid,
        session.net_profit,
        return_pct
    );
}
