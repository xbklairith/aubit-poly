#!/usr/bin/env python3
"""
Redeem winning Polymarket positions.

This script redeems resolved positions from the Polymarket CTF contract.
Positions are discovered via Polymarket Data API or on-chain queries.

Usage:
    # List all redeemable positions (from API)
    uv run python redeem.py --list

    # Check resolution status for a market (on-chain)
    uv run python redeem.py --check 0x1234...

    # Dry-run redemption (simulates without executing)
    uv run python redeem.py --condition-id 0x1234...

    # Execute redemption
    uv run python redeem.py --condition-id 0x1234... --execute

    # Redeem all positions (from API)
    uv run python redeem.py --all --execute

Required environment variables:
    WALLET_PRIVATE_KEY: Your wallet private key
    POLYGON_RPC_URL: Polygon RPC endpoint (optional, defaults to public RPC)
"""

import argparse
import logging
import sys
from decimal import Decimal

from pylo.bots.position_redeemer import PositionRedeemer, RedeemablePosition
from pylo.config.settings import get_settings


def setup_logging(debug: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("web3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def print_banner() -> None:
    """Print banner."""
    print("""
╔═══════════════════════════════════════════════════════════════╗
║           Polymarket Position Redeemer                        ║
║           Claim your winning positions                        ║
╚═══════════════════════════════════════════════════════════════╝
    """)


def list_redeemable(redeemer: PositionRedeemer) -> list[RedeemablePosition]:
    """List all redeemable (winning) positions from API."""
    print("\nFetching redeemable positions from Polymarket API...")

    all_positions = redeemer.get_redeemable_positions()

    # Filter to only winning positions (currentValue > 0)
    positions = [p for p in all_positions if p.is_winner]

    if not positions:
        if all_positions:
            print(f"Found {len(all_positions)} resolved position(s), but none are winners.")
        else:
            print("No redeemable positions found.")
        return []

    print(f"\n{'=' * 70}")
    print(f"Found {len(positions)} winning position(s) to redeem")
    print(f"{'=' * 70}\n")

    total_value = Decimal("0")

    for i, pos in enumerate(positions, 1):
        print(f"{i}. {pos.title[:55]}...")
        print(f"   Outcome:      {pos.outcome} (WINNER)")
        print(f"   Size:         {pos.size:.2f} shares")
        print(f"   Redeem Value: ${pos.current_value:.2f}")
        print(f"   P/L:          ${pos.pnl:+.2f}")
        print(f"   Condition ID: {pos.condition_id[:20]}...")
        print()
        total_value += pos.current_value

    print(f"{'=' * 70}")
    print(f"Total to redeem: ${total_value:.2f}")
    print(f"{'=' * 70}\n")

    return positions


def check_resolution(redeemer: PositionRedeemer, condition_id: str) -> None:
    """Check and display market resolution status (on-chain)."""
    print(f"\nChecking on-chain resolution for: {condition_id[:20]}...")

    resolution = redeemer.check_resolution(condition_id)

    print(f"\n{'=' * 60}")
    print(f"Condition ID:  {condition_id}")
    print(f"Is Resolved:   {'YES' if resolution.is_resolved else 'NO'}")

    if resolution.is_resolved:
        outcome = "YES" if resolution.winning_outcome == 0 else "NO"
        print(f"Winner:        {outcome}")
        print(f"Payout Denom:  {resolution.payout_denominator}")
        print("\nThis position CAN be redeemed.")
    else:
        print("\nThis market has NOT resolved yet. Cannot redeem.")

    print(f"{'=' * 60}\n")


def redeem_single(
    redeemer: PositionRedeemer,
    condition_id: str,
    execute: bool,
    use_gasless: bool | None = None,
) -> None:
    """Redeem a single position."""
    dry_run = not execute

    if dry_run:
        print(f"\n[DRY RUN] Simulating redemption for {condition_id[:20]}...")
    else:
        print(f"\nExecuting redemption for {condition_id[:20]}...")

    if dry_run:
        result = redeemer.redeem_position(condition_id, dry_run=True)
    elif use_gasless is True or (use_gasless is None and redeemer.has_builder_credentials()):
        result = redeemer.redeem_position_gasless(condition_id)
    else:
        result = redeemer.redeem_position(condition_id, dry_run=False)

    print(f"\n{'=' * 60}")
    print(f"Condition ID: {condition_id[:40]}...")
    print(f"Success:      {result.success}")

    if result.tx_hash:
        print(f"TX Hash:      {result.tx_hash}")
        print(f"Polygonscan:  https://polygonscan.com/tx/{result.tx_hash}")

    if result.gas_used:
        print(f"Gas Used:     {result.gas_used:,}")

    if result.error:
        print(f"Error:        {result.error}")

    print(f"{'=' * 60}\n")


def redeem_all_from_api(
    redeemer: PositionRedeemer, execute: bool, use_gasless: bool | None = None
) -> None:
    """Redeem all redeemable positions from API."""
    positions = list_redeemable(redeemer)

    if not positions:
        return

    if not execute:
        print("[DRY RUN] Would redeem the above positions.")
        print("Add --execute flag to redeem positions.\n")
        return

    print("Redeeming all positions...\n")

    # Group by condition_id (may have multiple outcomes per market)
    condition_ids = list({pos.condition_id for pos in positions})

    results = redeemer.redeem_all_resolved(condition_ids, dry_run=False, use_gasless=use_gasless)

    # Summary
    successful = sum(1 for r in results.values() if r.success)
    failed = sum(1 for r in results.values() if not r.success)

    print(f"\n{'=' * 60}")
    print("REDEMPTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total processed: {len(results)}")
    print(f"Successful:      {successful}")
    print(f"Failed:          {failed}")

    for cid, result in results.items():
        status = "OK" if result.success else f"FAIL: {result.error}"
        print(f"  {cid[:20]}... {status}")

    print(f"{'=' * 60}\n")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Redeem winning Polymarket positions",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--list",
        "-l",
        action="store_true",
        help="List all redeemable positions from API",
    )
    group.add_argument(
        "--condition-id",
        "-c",
        help="Condition ID of the market to redeem",
    )
    group.add_argument(
        "--check",
        help="Check on-chain resolution status for a condition ID",
    )
    group.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Redeem all redeemable positions from API",
    )

    parser.add_argument(
        "--execute",
        "-x",
        action="store_true",
        help="Execute redemption (default is dry-run)",
    )
    parser.add_argument(
        "--proxy",
        "-p",
        help="Polymarket proxy wallet address (overrides POLYMARKET_WALLET_ADDRESS)",
    )
    parser.add_argument(
        "--no-safe",
        action="store_true",
        help="Use EOA directly instead of Safe wallet",
    )
    parser.add_argument(
        "--gasless",
        action="store_true",
        default=None,
        help="Use gasless relayer (auto-detects if Builder API credentials available)",
    )
    parser.add_argument(
        "--no-gasless",
        action="store_true",
        help="Force direct transaction (requires POL for gas)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    setup_logging(args.debug)
    print_banner()

    # Check credentials
    settings = get_settings()
    if not settings.has_web3_credentials:
        print("ERROR: WALLET_PRIVATE_KEY environment variable is required.")
        print("Add it to your .env file:")
        print("  WALLET_PRIVATE_KEY=0x...")
        sys.exit(1)

    # Initialize redeemer
    try:
        redeemer = PositionRedeemer(
            proxy_address=args.proxy,
            use_safe_wallet=not args.no_safe,
        )
        print(f"EOA Address:   {redeemer.eoa_address}")
        if redeemer.safe_address:
            print(f"Proxy Address: {redeemer.safe_address}")

        # Determine gasless mode
        use_gasless: bool | None = None
        if args.no_gasless:
            use_gasless = False
        elif args.gasless:
            use_gasless = True
        # else: None = auto-detect

        has_creds = redeemer.has_builder_credentials()
        gasless_active = use_gasless if use_gasless is not None else has_creds

        if gasless_active:
            print("Mode:          Gasless (via relayer)")
        else:
            print("Mode:          Direct tx (requires POL gas)")
            if not has_creds:
                print("               (Add Builder API credentials for gasless mode)")
        print()
    except Exception as e:
        print(f"ERROR: Failed to initialize redeemer: {e}")
        sys.exit(1)

    # Execute command
    if args.list:
        list_redeemable(redeemer)

    elif args.check:
        check_resolution(redeemer, args.check)

    elif args.condition_id:
        redeem_single(redeemer, args.condition_id, args.execute, use_gasless)

    elif args.all:
        redeem_all_from_api(redeemer, args.execute, use_gasless)


if __name__ == "__main__":
    main()
