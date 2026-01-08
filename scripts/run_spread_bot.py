#!/usr/bin/env python3
"""
Spread Arbitrage Bot - Entry Point

Monitors Polymarket hourly Up/Down markets for spread arbitrage opportunities.
When YES + NO < $1.00, buying both sides guarantees a profit at settlement.

Usage:
    python scripts/run_spread_bot.py              # Run with defaults (dry-run)
    python scripts/run_spread_bot.py --once       # Single scan, then exit
    python scripts/run_spread_bot.py --interval 5 # Poll every 5 seconds
    python scripts/run_spread_bot.py --min-profit 0.03  # 3% minimum profit
"""

import argparse
import asyncio
import logging
import sys
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.bots.spread_arb_bot import SpreadArbBot, run_bot
from src.config.settings import get_settings


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the bot."""
    level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )

    # Reduce noise from httpx
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Spread Arbitrage Bot for Polymarket Up/Down markets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                      Run continuously in dry-run mode
  %(prog)s --once               Single scan, then exit
  %(prog)s --interval 5         Poll every 5 seconds
  %(prog)s --min-profit 0.03    Require 3%% minimum profit
  %(prog)s --assets BTC         Monitor BTC only
  %(prog)s -v                   Verbose logging

Strategy:
  The bot monitors hourly Up/Down binary markets on Polymarket.
  When the combined ask prices (YES + NO) are less than $1.00,
  buying both sides creates a guaranteed profit at settlement.

  Example: YES @ $0.48 + NO @ $0.48 = $0.96
           Buying $100 of each = $200 total
           Guaranteed payout = $208.33 (one side wins)
           Profit = $8.33 (4.17%%)
        """,
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Run a single scan cycle and exit",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Run in dry-run mode (simulated trading) - DEFAULT",
    )

    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in live mode (NOT IMPLEMENTED - will error)",
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Poll interval in seconds (default: from settings)",
    )

    parser.add_argument(
        "--min-profit",
        type=float,
        default=None,
        help="Minimum profit percentage (e.g., 0.02 for 2%%)",
    )

    parser.add_argument(
        "--max-position",
        type=float,
        default=None,
        help="Maximum position size in USD",
    )

    parser.add_argument(
        "--assets",
        type=str,
        default=None,
        help="Comma-separated list of assets to monitor (e.g., BTC,ETH)",
    )

    parser.add_argument(
        "--balance",
        type=float,
        default=None,
        help="Starting balance for dry-run mode",
    )

    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args()


def apply_overrides(args: argparse.Namespace) -> None:
    """Apply CLI argument overrides to settings."""
    settings = get_settings()

    if args.live:
        print("ERROR: Live trading is not implemented yet.")
        print("       Please use --dry-run mode for now.")
        sys.exit(1)

    if args.interval is not None:
        settings.spread_bot_poll_interval = args.interval

    if args.min_profit is not None:
        settings.spread_bot_min_profit = Decimal(str(args.min_profit))

    if args.max_position is not None:
        settings.spread_bot_max_position_size = Decimal(str(args.max_position))

    if args.assets is not None:
        settings.spread_bot_assets = args.assets

    if args.balance is not None:
        settings.spread_bot_starting_balance = Decimal(str(args.balance))


async def main() -> None:
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)
    apply_overrides(args)

    bot = SpreadArbBot()

    if args.once:
        print("\n=== SINGLE SCAN MODE ===\n")
        await bot.run_once()
    else:
        await bot.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nBot stopped by user.")
        sys.exit(0)
