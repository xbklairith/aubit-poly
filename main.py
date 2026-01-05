#!/usr/bin/env python3
"""
Aubit-Poly: Prediction Market Arbitrage Detection System

Main entry point for running arbitrage detection.
"""

import asyncio
import logging
import sys
from datetime import datetime

from src.alerts.notifier import AlertManager
from src.arbitrage.detector import ArbitrageEngine
from src.config.settings import get_settings


def setup_logging() -> None:
    """Configure logging for the application."""
    settings = get_settings()

    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper()),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    # Reduce noise from external libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


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
    ║           Prediction Market Arbitrage Detection               ║
    ║                                                               ║
    ╚═══════════════════════════════════════════════════════════════╝
    """
    print(banner)


async def run_single_scan() -> None:
    """Run a single arbitrage scan."""
    logger = logging.getLogger(__name__)
    settings = get_settings()

    print_banner()
    print(f"Starting single scan at {datetime.utcnow().isoformat()}")
    print(f"Minimum profit thresholds:")
    print(f"  - Internal:       {settings.min_internal_arb_profit:.2%}")
    print(f"  - Cross-platform: {settings.min_cross_platform_arb_profit:.2%}")
    print(f"  - Hedging:        {settings.min_hedging_arb_profit:.2%}")
    print()

    alert_manager = AlertManager()

    async with ArbitrageEngine() as engine:
        opportunities = await engine.scan_once()

        if opportunities:
            print(f"\n{'='*60}")
            print(f"Found {len(opportunities)} arbitrage opportunities!")
            print(f"{'='*60}\n")

            # Send alerts
            await alert_manager.notify_batch(opportunities)
        else:
            print("\nNo arbitrage opportunities found at this time.")
            print("This is normal - opportunities are rare and short-lived.")


async def run_continuous() -> None:
    """Run continuous arbitrage scanning."""
    logger = logging.getLogger(__name__)
    settings = get_settings()

    print_banner()
    print(f"Starting continuous scanning...")
    print(f"Scan interval: {settings.scan_interval} seconds")
    print(f"Press Ctrl+C to stop\n")

    alert_manager = AlertManager()

    async with ArbitrageEngine() as engine:
        while True:
            try:
                opportunities = await engine.scan_once()

                if opportunities:
                    await alert_manager.notify_batch(opportunities)

                await asyncio.sleep(settings.scan_interval)

            except asyncio.CancelledError:
                logger.info("Shutting down...")
                break
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.error(f"Scan error: {e}")
                await asyncio.sleep(10)  # Brief pause on error


async def demo_mode() -> None:
    """
    Run in demo mode with simulated data.

    Useful for testing without API credentials.
    """
    from decimal import Decimal

    from src.models.market import Market, MarketOutcome, Platform
    from src.models.opportunity import InternalArbOpportunity

    print_banner()
    print("Running in DEMO MODE (simulated data)\n")

    # Create simulated markets with arbitrage opportunities
    demo_markets = [
        Market(
            id="demo_1",
            platform=Platform.POLYMARKET,
            name="Will BTC hit $100k by January 2025?",
            outcomes=[
                MarketOutcome(id="yes", name="YES", price=Decimal("0.45")),
                MarketOutcome(id="no", name="NO", price=Decimal("0.52")),
            ],
            url="https://polymarket.com/demo",
        ),
        Market(
            id="demo_2",
            platform=Platform.POLYMARKET,
            name="Will ETH reach $5000 in Q1 2025?",
            outcomes=[
                MarketOutcome(id="yes", name="YES", price=Decimal("0.30")),
                MarketOutcome(id="no", name="NO", price=Decimal("0.68")),
            ],
            url="https://polymarket.com/demo2",
        ),
    ]

    alert_manager = AlertManager()
    opportunities = []

    for market in demo_markets:
        opp = InternalArbOpportunity.from_market(market)
        if opp:
            opportunities.append(opp)

    if opportunities:
        print(f"Demo: Found {len(opportunities)} simulated opportunities\n")
        await alert_manager.notify_batch(opportunities)
    else:
        print("Demo: No opportunities in simulated data")


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Aubit-Poly: Prediction Market Arbitrage Detection",
    )
    parser.add_argument(
        "--mode",
        choices=["single", "continuous", "demo"],
        default="single",
        help="Run mode: single scan, continuous scanning, or demo with simulated data",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Setup logging
    if args.debug:
        import os
        os.environ["LOG_LEVEL"] = "DEBUG"

    setup_logging()

    # Run selected mode
    if args.mode == "demo":
        asyncio.run(demo_mode())
    elif args.mode == "continuous":
        asyncio.run(run_continuous())
    else:
        asyncio.run(run_single_scan())


if __name__ == "__main__":
    main()
