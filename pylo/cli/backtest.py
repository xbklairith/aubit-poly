#!/usr/bin/env python3
"""
Backtest CLI commands for evaluating trading strategies.

Usage:
    uv run python -m pylo.cli.backtest fetch-data --days 30
    uv run python -m pylo.cli.backtest run --strategy expiry
    uv run python -m pylo.cli.backtest run --strategy contrarian
    uv run python -m pylo.cli.backtest compare
    uv run python -m pylo.cli.backtest stats
"""

import argparse
import asyncio
import logging
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Reduce noise from external libraries
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def print_banner() -> None:
    """Print application banner."""
    banner = """
    ╔═══════════════════════════════════════════════════════════════╗
    ║                                                               ║
    ║     █████╗ ██╗   ██╗██████╗ ██╗████████╗                     ║
    ║    ██╔══██╗██║   ██║██╔══██╗██║╚══██╔══╝                     ║
    ║    ███████║██║   ██║██████╔╝██║   ██║                        ║
    ║    ██╔══██║██║   ██║██╔══██╗██║   ██║                        ║
    ║    ██║  ██║╚██████╔╝██████╔╝██║   ██║                        ║
    ║    ╚═╝  ╚═╝ ╚═════╝ ╚═════╝ ╚═╝   ╚═╝                        ║
    ║                                                               ║
    ║              Strategy Backtesting System                      ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def fetch_data(args: argparse.Namespace) -> None:
    """Fetch historical data from Polymarket."""
    import httpx

    from pylo.backtest.data_fetcher import DataFetcher
    from pylo.db.connection import get_database

    # Validate inputs
    if args.days < 1:
        print("Error: --days must be >= 1", file=sys.stderr)
        sys.exit(1)

    print_banner()
    print(f"Fetching historical data for last {args.days} days...")
    print(f"Assets: {args.assets}")
    print(f"Timeframe: {args.timeframe}")
    print()

    try:
        db = get_database()
    except Exception as e:
        print(f"Error: Failed to connect to database: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        fetcher = DataFetcher(db)

        # Fetch and store market resolutions
        print("Step 1: Fetching market resolutions...")
        try:
            resolution_count = await fetcher.fetch_and_store_resolutions(
                days=args.days,
                assets=args.assets.split(","),
                timeframe=args.timeframe,
            )
        except httpx.HTTPError as e:
            print(f"Error: API request failed: {e}", file=sys.stderr)
            sys.exit(1)
        print(f"  Stored {resolution_count} market resolutions")

        # Fetch price history if requested
        if args.fetch_prices:
            print("\nStep 2: Fetching price history...")
            price_count = await fetcher.fetch_all_price_histories(
                window_minutes=args.price_window,
                concurrency=args.concurrency,
            )
            print(f"  Stored {price_count} price history points")

        # Show stats
        print("\n" + "=" * 60)
        stats = await fetcher.get_data_stats()
        print(f"Total resolutions: {stats['resolutions']['total']}")
        print(f"Date range: {stats['resolutions']['earliest']} to {stats['resolutions']['latest']}")
        print(f"Price history points: {stats['price_history']['total_points']}")
        print(f"Markets with history: {stats['price_history']['markets_with_history']}")
        print("\nBy asset:")
        for asset, asset_stats in stats["by_asset"].items():
            print(
                f"  {asset}: {asset_stats['total']} markets "
                f"(YES won: {asset_stats['yes_wins']}, NO won: {asset_stats['no_wins']})"
            )

    finally:
        await db.close()


async def run_backtest(args: argparse.Namespace) -> None:
    """Run backtest for a strategy."""
    from pylo.backtest.reports import (
        export_trades_csv,
        generate_detailed_report,
        generate_summary_report,
        generate_trades_report,
    )
    from pylo.backtest.simulator import BacktestSimulator
    from pylo.backtest.strategies import ContrarianScalperStrategy, ExpiryScalperStrategy
    from pylo.db.connection import get_database

    print_banner()

    # Select strategy
    strategy: ExpiryScalperStrategy | ContrarianScalperStrategy
    if args.strategy == "expiry":
        strategy = ExpiryScalperStrategy(
            skew_threshold=Decimal(args.threshold),
            position_size=Decimal(args.position_size),
            expiry_window_seconds=args.expiry_window,
        )
    elif args.strategy == "contrarian":
        strategy = ContrarianScalperStrategy(
            skew_threshold=Decimal(args.threshold),
            position_size=Decimal(args.position_size),
            expiry_window_seconds=args.expiry_window,
            limit_price=Decimal(args.limit_price),
        )
    else:
        print(f"Unknown strategy: {args.strategy}")
        sys.exit(1)

    print(f"Running backtest: {strategy.name}")
    print(f"Parameters: {strategy.params}")
    print()

    db = get_database()

    try:
        simulator = BacktestSimulator(db, strategy)

        # Parse dates
        end_date = datetime.now(UTC)
        if args.end_date:
            end_date = datetime.fromisoformat(args.end_date).replace(tzinfo=UTC)

        start_date = end_date - timedelta(days=args.days)
        if args.start_date:
            start_date = datetime.fromisoformat(args.start_date).replace(tzinfo=UTC)

        # Run backtest
        run = await simulator.run(
            start_date=start_date,
            end_date=end_date,
            assets=args.assets.split(","),
            timeframe=args.timeframe,
        )

        # Generate reports
        if args.detailed:
            print(generate_detailed_report(run))
        else:
            print(generate_summary_report(run))

        if args.show_trades:
            print(generate_trades_report(run, limit=args.show_trades))

        # Save to database
        if args.save:
            run_id = await simulator.save_run(run)
            print(f"Saved run to database with ID: {run_id}")

        # Export CSV
        if args.export_csv:
            csv_content = export_trades_csv(run)
            with open(args.export_csv, "w") as f:
                f.write(csv_content)
            print(f"Exported trades to: {args.export_csv}")

    finally:
        await db.close()


async def compare_strategies(args: argparse.Namespace) -> None:
    """Run and compare multiple strategies."""
    from pylo.backtest.reports import generate_comparison_report, generate_detailed_report
    from pylo.backtest.simulator import BacktestSimulator
    from pylo.backtest.strategies import ContrarianScalperStrategy, ExpiryScalperStrategy
    from pylo.db.connection import get_database

    print_banner()
    print("Comparing strategies...")
    print()

    db = get_database()

    try:
        # Parse dates
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=args.days)

        strategies = [
            ExpiryScalperStrategy(
                skew_threshold=Decimal(args.threshold),
                position_size=Decimal(args.position_size),
                expiry_window_seconds=args.expiry_window,
            ),
            ContrarianScalperStrategy(
                skew_threshold=Decimal(args.threshold),
                position_size=Decimal(args.position_size),
                expiry_window_seconds=args.expiry_window,
            ),
        ]

        runs = []
        for strategy in strategies:
            print(f"Running: {strategy.name}...")
            simulator = BacktestSimulator(db, strategy)
            run = await simulator.run(
                start_date=start_date,
                end_date=end_date,
                assets=args.assets.split(","),
                timeframe=args.timeframe,
            )
            runs.append(run)

            if args.detailed:
                print(generate_detailed_report(run))

        # Print comparison
        print(generate_comparison_report(runs))

    finally:
        await db.close()


async def show_stats(args: argparse.Namespace) -> None:  # noqa: ARG001
    """Show statistics about stored data."""
    from pylo.backtest.data_fetcher import DataFetcher
    from pylo.db.connection import get_database

    print_banner()
    print("Data Statistics")
    print("=" * 60)

    db = get_database()

    try:
        fetcher = DataFetcher(db)
        stats = await fetcher.get_data_stats()

        print("\nMarket Resolutions:")
        print(f"  Total: {stats['resolutions']['total']}")
        print(f"  Earliest: {stats['resolutions']['earliest']}")
        print(f"  Latest: {stats['resolutions']['latest']}")

        print("\nPrice History:")
        print(f"  Total points: {stats['price_history']['total_points']}")
        print(f"  Markets with history: {stats['price_history']['markets_with_history']}")

        print("\nBreakdown by Asset:")
        for asset, asset_stats in stats["by_asset"].items():
            total = asset_stats["total"]
            yes_wins = asset_stats["yes_wins"]
            no_wins = asset_stats["no_wins"]
            yes_pct = (yes_wins / total * 100) if total > 0 else 0
            print(
                f"  {asset}: {total} markets (YES won: {yes_wins} ({yes_pct:.1f}%), NO won: {no_wins})"
            )

    finally:
        await db.close()


async def optimize_strategy(args: argparse.Namespace) -> None:
    """Run parameter optimization for a strategy."""
    from pylo.backtest.optimizer import (
        ParameterOptimizer,
        export_optimization_csv,
        generate_optimization_report,
    )
    from pylo.db.connection import get_database

    print_banner()
    print(f"Optimizing {args.strategy} strategy...")
    print()

    db = get_database()

    try:
        optimizer = ParameterOptimizer(db)

        # Parse dates
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=args.days)

        # Parse parameter grids
        thresholds = args.thresholds.split(",") if args.thresholds else None
        expiry_windows = (
            [int(w) for w in args.expiry_windows.split(",")] if args.expiry_windows else None
        )

        if args.strategy == "expiry":
            report = await optimizer.optimize_expiry_scalper(
                start_date=start_date,
                end_date=end_date,
                assets=args.assets.split(","),
                timeframe=args.timeframe,
                thresholds=thresholds,
                expiry_windows=expiry_windows,
            )
        elif args.strategy == "contrarian":
            limit_prices = args.limit_prices.split(",") if args.limit_prices else None
            report = await optimizer.optimize_contrarian_scalper(
                start_date=start_date,
                end_date=end_date,
                assets=args.assets.split(","),
                timeframe=args.timeframe,
                thresholds=thresholds,
                expiry_windows=expiry_windows,
                limit_prices=limit_prices,
            )
        else:
            print(f"Unknown strategy: {args.strategy}")
            sys.exit(1)

        # Print report
        print(generate_optimization_report(report))

        # Export CSV if requested
        if args.export_csv:
            csv_content = export_optimization_csv(report)
            with open(args.export_csv, "w") as f:
                f.write(csv_content)
            print(f"Exported results to: {args.export_csv}")

    finally:
        await db.close()


def main() -> None:
    """Main entry point for backtest CLI."""
    parser = argparse.ArgumentParser(
        description="Backtest trading strategies on historical Polymarket data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch 30 days of historical data
  uv run python -m pylo.cli.backtest fetch-data --days 30

  # Fetch data with price history
  uv run python -m pylo.cli.backtest fetch-data --days 30 --fetch-prices

  # Run expiry scalper backtest
  uv run python -m pylo.cli.backtest run --strategy expiry --detailed

  # Run contrarian scalper backtest
  uv run python -m pylo.cli.backtest run --strategy contrarian --detailed

  # Compare both strategies
  uv run python -m pylo.cli.backtest compare --days 30

  # Export trades to CSV
  uv run python -m pylo.cli.backtest run --strategy expiry --export-csv trades.csv
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # fetch-data command
    fetch_parser = subparsers.add_parser("fetch-data", help="Fetch historical data")
    fetch_parser.add_argument("--days", type=int, default=30, help="Days of history to fetch")
    fetch_parser.add_argument(
        "--assets", default="BTC,ETH,SOL,XRP", help="Assets to fetch (comma-separated)"
    )
    fetch_parser.add_argument("--timeframe", default="15m", help="Timeframe to fetch")
    fetch_parser.add_argument(
        "--fetch-prices", action="store_true", help="Also fetch price history"
    )
    fetch_parser.add_argument(
        "--price-window", type=int, default=10, help="Minutes of price history before expiry"
    )
    fetch_parser.add_argument("--concurrency", type=int, default=5, help="Concurrent API requests")

    # run command
    run_parser = subparsers.add_parser("run", help="Run backtest")
    run_parser.add_argument(
        "--strategy", required=True, choices=["expiry", "contrarian"], help="Strategy to test"
    )
    run_parser.add_argument("--days", type=int, default=30, help="Days of history")
    run_parser.add_argument("--start-date", help="Start date (ISO format)")
    run_parser.add_argument("--end-date", help="End date (ISO format)")
    run_parser.add_argument("--assets", default="BTC,ETH,SOL,XRP", help="Assets (comma-separated)")
    run_parser.add_argument("--timeframe", default="15m", help="Timeframe")
    run_parser.add_argument("--threshold", default="0.75", help="Skew threshold")
    run_parser.add_argument("--position-size", default="50", help="Position size in shares")
    run_parser.add_argument("--expiry-window", type=int, default=180, help="Seconds before expiry")
    run_parser.add_argument("--limit-price", default="0.01", help="Limit price for contrarian")
    run_parser.add_argument("--detailed", action="store_true", help="Show detailed report")
    run_parser.add_argument("--show-trades", type=int, metavar="N", help="Show N individual trades")
    run_parser.add_argument("--save", action="store_true", help="Save run to database")
    run_parser.add_argument("--export-csv", metavar="FILE", help="Export trades to CSV")

    # compare command
    compare_parser = subparsers.add_parser("compare", help="Compare strategies")
    compare_parser.add_argument("--days", type=int, default=30, help="Days of history")
    compare_parser.add_argument(
        "--assets", default="BTC,ETH,SOL,XRP", help="Assets (comma-separated)"
    )
    compare_parser.add_argument("--timeframe", default="15m", help="Timeframe")
    compare_parser.add_argument("--threshold", default="0.75", help="Skew threshold")
    compare_parser.add_argument("--position-size", default="50", help="Position size in shares")
    compare_parser.add_argument(
        "--expiry-window", type=int, default=180, help="Seconds before expiry"
    )
    compare_parser.add_argument("--detailed", action="store_true", help="Show detailed reports")

    # stats command
    subparsers.add_parser("stats", help="Show data statistics")

    # optimize command
    optimize_parser = subparsers.add_parser("optimize", help="Optimize strategy parameters")
    optimize_parser.add_argument(
        "--strategy", required=True, choices=["expiry", "contrarian"], help="Strategy to optimize"
    )
    optimize_parser.add_argument("--days", type=int, default=30, help="Days of history")
    optimize_parser.add_argument(
        "--assets", default="BTC,ETH,SOL,XRP", help="Assets (comma-separated)"
    )
    optimize_parser.add_argument("--timeframe", default="15m", help="Timeframe")
    optimize_parser.add_argument(
        "--thresholds",
        help="Thresholds to test (comma-separated, e.g., '0.60,0.65,0.70,0.75,0.80,0.85,0.90,0.95')",
    )
    optimize_parser.add_argument(
        "--expiry-windows",
        dest="expiry_windows",
        help="Expiry windows in seconds (comma-separated, e.g., '60,120,180,300,600')",
    )
    optimize_parser.add_argument(
        "--limit-prices",
        dest="limit_prices",
        help="Limit prices for contrarian (comma-separated, e.g., '0.01,0.02,0.05,0.10')",
    )
    optimize_parser.add_argument("--export-csv", metavar="FILE", help="Export results to CSV")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Run command
    if args.command == "fetch-data":
        asyncio.run(fetch_data(args))
    elif args.command == "run":
        asyncio.run(run_backtest(args))
    elif args.command == "compare":
        asyncio.run(compare_strategies(args))
    elif args.command == "stats":
        asyncio.run(show_stats(args))
    elif args.command == "optimize":
        asyncio.run(optimize_strategy(args))


if __name__ == "__main__":
    main()
