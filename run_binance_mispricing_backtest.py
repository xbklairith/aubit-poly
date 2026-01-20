#!/usr/bin/env python
"""
Run Binance Mispricing strategy backtest.

This strategy replicates the edge used by traders like hai15617 who made $346k
in 5 days trading BTC Up/Down 15-minute markets on Polymarket.

The core concept:
- Binance BTC price moves (confirms direction)
- Polymarket odds lag behind (haven't updated yet)
- Buy the correct direction at mispriced (low) odds
- Collect $1.00 when market resolves

Usage:
    uv run python run_binance_mispricing_backtest.py
    uv run python run_binance_mispricing_backtest.py --days 30
    uv run python run_binance_mispricing_backtest.py --min-edge 0.25 --max-price 0.30
"""

import argparse
import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Set database URL if not set
if "DATABASE_URL" not in os.environ:
    os.environ["DATABASE_URL"] = "postgres://aubit:aubit_dev_password@localhost:5432/aubit_poly"


def print_banner() -> None:
    """Print application banner."""
    banner = """
    ╔═══════════════════════════════════════════════════════════════════╗
    ║                                                                   ║
    ║     ██████╗ ██╗███╗   ██╗ █████╗ ███╗   ██╗ ██████╗███████╗      ║
    ║     ██╔══██╗██║████╗  ██║██╔══██╗████╗  ██║██╔════╝██╔════╝      ║
    ║     ██████╔╝██║██╔██╗ ██║███████║██╔██╗ ██║██║     █████╗        ║
    ║     ██╔══██╗██║██║╚██╗██║██╔══██║██║╚██╗██║██║     ██╔══╝        ║
    ║     ██████╔╝██║██║ ╚████║██║  ██║██║ ╚████║╚██████╗███████╗      ║
    ║     ╚═════╝ ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝  ╚═══╝ ╚═════╝╚══════╝      ║
    ║                                                                   ║
    ║              MISPRICING DETECTION STRATEGY                        ║
    ║         Binance BTC Direction vs Polymarket Odds                  ║
    ║                                                                   ║
    ║     Based on hai15617's strategy: $346k profit in 5 days          ║
    ║                                                                   ║
    ╚═══════════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def run_backtest(args: argparse.Namespace) -> None:
    """Run the Binance mispricing backtest."""
    from pylo.backtest.reports import (
        export_trades_csv,
        generate_comparison_report,
        generate_detailed_report,
    )
    from pylo.backtest.simulator import BacktestSimulator
    from pylo.backtest.strategies import (
        BinanceMispricingStrategy,
        ContrarianScalperStrategy,
        ExpiryScalperStrategy,
        ProbabilityGapStrategy,
    )
    from pylo.db.connection import get_database

    print_banner()

    # Parse parameters
    days = args.days
    position_size = Decimal(args.position_size)
    min_edge = Decimal(args.min_edge)
    max_price = Decimal(args.max_price)
    min_btc_change = Decimal(args.min_btc_change)
    assets = args.assets.split(",")

    print("Configuration:")
    print(f"  Period: Last {days} days")
    print(f"  Assets: {', '.join(assets)}")
    print(f"  Position size: ${position_size}")
    print(f"  Min edge: {min_edge:.0%}")
    print(f"  Max market price: ${max_price}")
    print(f"  Min BTC change: {min_btc_change:.2%}")
    print()

    db = get_database()

    try:
        await db.warmup()

        # Date range
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=days)

        print("=" * 70)
        print("BINANCE MISPRICING STRATEGY BACKTEST")
        print("=" * 70)
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print()

        # Initialize Binance Mispricing Strategy
        mispricing_strategy = BinanceMispricingStrategy(
            position_size=position_size,
            expiry_window_seconds=600,  # 10 minutes
            min_btc_change_pct=min_btc_change,
            min_edge=min_edge,
            max_market_price=max_price,
            momentum_lookback_minutes=5,
            kelly_fraction=Decimal("0.25"),
            scale_with_edge=True,
        )

        print(f"Running: {mispricing_strategy.name}")
        print(f"Parameters: {mispricing_strategy.params}")
        print()

        # Run backtest
        simulator = BacktestSimulator(db, mispricing_strategy)
        mispricing_run = await simulator.run(
            start_date=start_date,
            end_date=end_date,
            assets=assets,
            timeframe="15m",
        )

        # Print detailed report
        print("\n" + generate_detailed_report(mispricing_run))

        # Compare with baseline strategies
        if args.compare:
            print("\n" + "=" * 70)
            print("COMPARISON WITH OTHER STRATEGIES")
            print("=" * 70)

            runs = [mispricing_run]

            # Probability Gap Strategy
            print("\nRunning Probability Gap Strategy (baseline)...")
            prob_gap_strategy = ProbabilityGapStrategy(
                position_size=position_size,
                expiry_window_seconds=600,
                min_edge=Decimal("0.05"),
            )
            prob_gap_sim = BacktestSimulator(db, prob_gap_strategy)
            prob_gap_run = await prob_gap_sim.run(
                start_date=start_date,
                end_date=end_date,
                assets=assets,
                timeframe="15m",
            )
            runs.append(prob_gap_run)

            # Expiry Scalper
            print("Running Expiry Scalper (baseline)...")
            expiry_strategy = ExpiryScalperStrategy(
                position_size=position_size,
                expiry_window_seconds=180,
                skew_threshold=Decimal("0.75"),
            )
            expiry_sim = BacktestSimulator(db, expiry_strategy)
            expiry_run = await expiry_sim.run(
                start_date=start_date,
                end_date=end_date,
                assets=assets,
                timeframe="15m",
            )
            runs.append(expiry_run)

            # Contrarian Scalper
            print("Running Contrarian Scalper (baseline)...")
            contrarian_strategy = ContrarianScalperStrategy(
                position_size=position_size,
                expiry_window_seconds=180,
                skew_threshold=Decimal("0.75"),
                use_market_order=True,
            )
            contrarian_sim = BacktestSimulator(db, contrarian_strategy)
            contrarian_run = await contrarian_sim.run(
                start_date=start_date,
                end_date=end_date,
                assets=assets,
                timeframe="15m",
            )
            runs.append(contrarian_run)

            # Print comparison
            print("\n" + generate_comparison_report(runs))

            # Summary table
            print("\n" + "=" * 70)
            print("SUMMARY TABLE")
            print("=" * 70)
            print(f"{'Strategy':<25} {'Trades':>8} {'Win Rate':>10} {'Net P/L':>12} {'ROI':>10}")
            print("-" * 70)

            for run in runs:
                m = run.metrics
                trades = m.orders_filled
                win_rate = f"{float(m.win_rate) * 100:.1f}%" if m.win_rate else "N/A"
                pnl = f"${float(m.net_pnl):,.2f}"
                roi = f"{float(m.roi) * 100:.1f}%" if m.roi else "N/A"
                print(f"{run.strategy_name:<25} {trades:>8} {win_rate:>10} {pnl:>12} {roi:>10}")

            print("-" * 70)

        # Export trades to CSV if requested
        if args.export_csv:
            csv_content = export_trades_csv(mispricing_run)
            with open(args.export_csv, "w") as f:
                f.write(csv_content)
            print(f"\nExported trades to: {args.export_csv}")

        # Print edge analysis
        if mispricing_run.trades:
            print("\n" + "=" * 70)
            print("EDGE ANALYSIS")
            print("=" * 70)

            winning_trades = [t for t in mispricing_run.trades if t.filled and t.won]
            losing_trades = [t for t in mispricing_run.trades if t.filled and not t.won]

            if winning_trades:
                avg_win_price = sum(t.fill_price or Decimal("0") for t in winning_trades) / len(
                    winning_trades
                )
                avg_win_return = (Decimal("1") - avg_win_price) / avg_win_price
                print("Winning trades:")
                print(f"  Count: {len(winning_trades)}")
                print(f"  Avg entry price: ${float(avg_win_price):.3f}")
                print(f"  Avg return: {float(avg_win_return) * 100:.1f}%")

            if losing_trades:
                avg_loss_price = sum(t.fill_price or Decimal("0") for t in losing_trades) / len(
                    losing_trades
                )
                print("\nLosing trades:")
                print(f"  Count: {len(losing_trades)}")
                print(f"  Avg entry price: ${float(avg_loss_price):.3f}")
                print("  Avg loss: -100% (binary market)")

            # Price distribution
            if mispricing_run.trades:
                print("\nEntry price distribution:")
                price_buckets = {
                    "$0.00-0.10": 0,
                    "$0.10-0.20": 0,
                    "$0.20-0.30": 0,
                    "$0.30-0.40": 0,
                    "$0.40+": 0,
                }
                for trade in mispricing_run.trades:
                    if trade.filled and trade.fill_price:
                        price = float(trade.fill_price)
                        if price < 0.10:
                            price_buckets["$0.00-0.10"] += 1
                        elif price < 0.20:
                            price_buckets["$0.10-0.20"] += 1
                        elif price < 0.30:
                            price_buckets["$0.20-0.30"] += 1
                        elif price < 0.40:
                            price_buckets["$0.30-0.40"] += 1
                        else:
                            price_buckets["$0.40+"] += 1

                for bucket, count in price_buckets.items():
                    bar = "█" * count
                    print(f"  {bucket}: {count:>4} {bar}")

    except Exception as e:
        logger.error(f"Backtest failed: {e}")
        raise
    finally:
        await db.close()


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run Binance Mispricing Strategy Backtest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic backtest with defaults
  uv run python run_binance_mispricing_backtest.py

  # 30-day backtest with comparison
  uv run python run_binance_mispricing_backtest.py --days 30 --compare

  # Aggressive parameters (lower edge threshold)
  uv run python run_binance_mispricing_backtest.py --min-edge 0.15 --max-price 0.35

  # Conservative parameters (higher edge threshold)
  uv run python run_binance_mispricing_backtest.py --min-edge 0.30 --max-price 0.25

  # Export results to CSV
  uv run python run_binance_mispricing_backtest.py --export-csv mispricing_trades.csv

  # BTC only
  uv run python run_binance_mispricing_backtest.py --assets BTC
        """,
    )

    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Number of days to backtest (default: 10)",
    )
    parser.add_argument(
        "--assets",
        default="BTC,ETH,SOL,XRP",
        help="Comma-separated list of assets (default: BTC,ETH,SOL,XRP)",
    )
    parser.add_argument(
        "--position-size",
        default="100",
        help="Position size in dollars (default: 100)",
    )
    parser.add_argument(
        "--min-edge",
        default="0.20",
        help="Minimum edge to trigger trade (default: 0.20 = 20%%)",
    )
    parser.add_argument(
        "--max-price",
        default="0.40",
        help="Maximum price to pay (default: 0.40 = $0.40)",
    )
    parser.add_argument(
        "--min-btc-change",
        default="0.003",
        help="Minimum BTC %% change to confirm direction (default: 0.003 = 0.3%%)",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare with baseline strategies",
    )
    parser.add_argument(
        "--export-csv",
        metavar="FILE",
        help="Export trades to CSV file",
    )

    args = parser.parse_args()
    asyncio.run(run_backtest(args))


if __name__ == "__main__":
    main()
