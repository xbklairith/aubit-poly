#!/usr/bin/env python3
"""Resolve expired trades and recalculate portfolio P&L."""

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import httpx

GAMMA_API_URL = "https://gamma-api.polymarket.com"

# Series IDs for 15m Up/Down markets
CRYPTO_SERIES_15M = {
    "BTC": "10192",
    "ETH": "10191",
    "SOL": "10423",
    "XRP": "10422",
}


@dataclass
class Trade:
    """A simulated trade from the momentum trader."""
    timestamp: str
    asset: str
    position: str  # YES or NO
    market_name: str
    entry_price: float
    shares: float
    potential_profit: float
    resolution: str | None = None
    actual_pnl: float | None = None


def parse_log_trades(log_path: str) -> list[Trade]:
    """Parse trades from momentum-trader log file."""
    trades = []

    # Read log file
    with open(log_path) as f:
        lines = f.readlines()

    # Find SIGNAL lines and extract trade info
    signal_pattern = re.compile(
        r'\[SIGNAL\]\s+(\w+)\s+[\d.-]+%\s+->\s+(YES|NO)\s+(.+?)\s+@\s+\$(\d+\.\d+)'
    )
    dryrun_pattern = re.compile(
        r'\[DRY RUN\]\s+(YES|NO)\s+(\d+\.\d+)\s+shares\s+@\s+\$(\d+\.\d+)\s+->\s+Win:\s+\$(\d+\.\d+)'
    )
    timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})')

    i = 0
    while i < len(lines):
        line = lines[i]
        signal_match = signal_pattern.search(line)

        if signal_match:
            # Extract timestamp
            ts_match = timestamp_pattern.search(line)
            timestamp = ts_match.group(1) if ts_match else ""

            asset = signal_match.group(1)
            position = signal_match.group(2)
            market_name = signal_match.group(3).strip()
            entry_price = float(signal_match.group(4))

            # Look for the corresponding DRY RUN line
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                dryrun_match = dryrun_pattern.search(next_line)
                if dryrun_match:
                    shares = float(dryrun_match.group(2))
                    potential_profit = float(dryrun_match.group(4))

                    trade = Trade(
                        timestamp=timestamp,
                        asset=asset,
                        position=position,
                        market_name=market_name,
                        entry_price=entry_price,
                        shares=shares,
                        potential_profit=potential_profit,
                    )
                    trades.append(trade)
        i += 1

    return trades


async def fetch_resolutions(client: httpx.AsyncClient, series_id: str, asset: str) -> dict[str, str]:
    """Fetch resolutions for closed markets in a series for Jan 19-20, 2026."""
    resolutions = {}

    # Starting offsets tuned for Jan 19 2PM data (found empirically)
    # BTC/ETH series are older with higher offsets
    start_offsets = {
        "10192": 12300,  # BTC - Jan 19 12:30PM starts here
        "10191": 12300,  # ETH - Jan 19 2:00PM starts here
        "10423": 8020,   # SOL - Jan 19 2:15PM starts here
        "10422": 8020,   # XRP
    }
    start_offset = start_offsets.get(series_id, 8000)

    # Fetch 200 markets starting from Jan 19 afternoon
    for offset in range(start_offset, start_offset + 200, 50):
        try:
            response = await client.get(
                f"{GAMMA_API_URL}/events",
                params={
                    "series_id": series_id,
                    "closed": "true",
                    "limit": 50,
                    "offset": offset,
                },
                timeout=30.0,
            )
            response.raise_for_status()
            events = response.json()

            if not events:
                continue

            for event in events:
                for market in event.get("markets", []):
                    question = market.get("question", "")

                    # Get resolution from outcome or outcomePrices
                    outcome = market.get("outcome")
                    if not outcome:
                        outcome_prices_str = market.get("outcomePrices", "[]")
                        outcomes_str = market.get("outcomes", "[]")
                        try:
                            if isinstance(outcome_prices_str, str):
                                outcome_prices = json.loads(outcome_prices_str)
                            else:
                                outcome_prices = outcome_prices_str
                            if isinstance(outcomes_str, str):
                                outcomes = json.loads(outcomes_str)
                            else:
                                outcomes = outcomes_str

                            for i, price in enumerate(outcome_prices):
                                if price == "1" or price == 1:
                                    if i < len(outcomes):
                                        outcome = outcomes[i]
                                    break
                        except (json.JSONDecodeError, TypeError, IndexError):
                            pass

                    if outcome:
                        # Map Up/Down to YES/NO for matching with our positions
                        # YES = Up, NO = Down
                        mapped_outcome = "YES" if outcome == "Up" else "NO" if outcome == "Down" else outcome
                        resolutions[question] = mapped_outcome

        except httpx.HTTPError as e:
            print(f"Error fetching from offset {offset}: {e}")

    return resolutions


def normalize_market_name(name: str) -> str:
    """Normalize market name for matching."""
    # Remove extra whitespace
    name = " ".join(name.split())
    # Standardize format
    name = name.replace(" - ", " - ")
    return name


async def main():
    log_path = Path(__file__).parent.parent / "docx/logs/machine23/momentum-trader.log"

    print("=" * 70)
    print("MOMENTUM TRADER - PORTFOLIO RESOLUTION & RECALCULATION")
    print("=" * 70)
    print()

    # Parse trades from log
    print("Parsing trades from log file...")
    trades = parse_log_trades(str(log_path))
    print(f"Found {len(trades)} trades")
    print()

    # Fetch actual resolutions from Polymarket
    print("Fetching actual resolutions from Polymarket API...")
    async with httpx.AsyncClient() as client:
        all_resolutions = {}
        for asset, series_id in CRYPTO_SERIES_15M.items():
            print(f"  Fetching {asset} markets (series {series_id})...")
            resolutions = await fetch_resolutions(client, series_id, asset)
            all_resolutions.update(resolutions)
            print(f"    Found {len(resolutions)} resolved markets")
            await asyncio.sleep(0.3)  # Rate limit

    print(f"\nTotal resolutions fetched: {len(all_resolutions)}")

    # Debug: show sample of fetched resolutions
    print("\nSample of fetched resolutions:")
    for i, (market, resolution) in enumerate(all_resolutions.items()):
        if "January 19" in market and i < 10:
            print(f"  {market} -> {resolution}")
    print()

    # Match trades with resolutions
    print("Matching trades with actual resolutions...")
    matched = 0
    unmatched = 0

    for trade in trades:
        # Try exact match first
        trade_market = normalize_market_name(trade.market_name)

        if trade_market in all_resolutions:
            trade.resolution = all_resolutions[trade_market]
            matched += 1
            continue

        # Try matching with minor variations
        found = False
        for market_name, resolution in all_resolutions.items():
            api_market = normalize_market_name(market_name)

            # Exact substring match
            if trade_market == api_market:
                trade.resolution = resolution
                matched += 1
                found = True
                break

            # Check if key components match (asset name + time window)
            # Trade: "Solana Up or Down - January 19, 2:15PM-2:30PM ET"
            # API:   "Solana Up or Down - January 19, 2:15PM-2:30PM ET"

            # Extract the time window part for matching
            trade_time_match = re.search(r'(\w+) Up or Down - (January \d+, \d+:\d+[AP]M-\d+:\d+[AP]M ET)', trade_market)
            api_time_match = re.search(r'(\w+) Up or Down - (January \d+, \d+:\d+[AP]M-\d+:\d+[AP]M ET)', api_market)

            if trade_time_match and api_time_match:
                trade_asset = trade_time_match.group(1)
                trade_time = trade_time_match.group(2)
                api_asset = api_time_match.group(1)
                api_time = api_time_match.group(2)

                if trade_asset == api_asset and trade_time == api_time:
                    trade.resolution = resolution
                    matched += 1
                    found = True
                    break

        if not found:
            unmatched += 1

    print(f"Matched: {matched}, Unmatched: {unmatched}")
    print()

    # Calculate P&L
    print("Calculating actual P&L...")
    print("-" * 70)

    total_invested = 0.0
    total_pnl = 0.0
    wins = 0
    losses = 0
    unknown = 0

    # Detailed trade results
    results = []

    for trade in trades:
        cost = trade.shares * trade.entry_price
        total_invested += cost

        if trade.resolution:
            # Check if our position matches the resolution
            if trade.position == trade.resolution:
                # WIN - we get $1 per share
                pnl = trade.shares * 1.0 - cost
                trade.actual_pnl = pnl
                total_pnl += pnl
                wins += 1
                result = "WIN"
            else:
                # LOSS - we lose our investment
                pnl = -cost
                trade.actual_pnl = pnl
                total_pnl += pnl
                losses += 1
                result = "LOSS"

            results.append({
                "market": trade.market_name[:50],
                "position": trade.position,
                "resolution": trade.resolution,
                "cost": cost,
                "pnl": pnl,
                "result": result,
            })
        else:
            unknown += 1
            results.append({
                "market": trade.market_name[:50],
                "position": trade.position,
                "resolution": "UNKNOWN",
                "cost": cost,
                "pnl": 0,
                "result": "UNKNOWN",
            })

    # Print results summary
    print()
    print("=" * 70)
    print("CORRECTED PORTFOLIO SUMMARY")
    print("=" * 70)
    print(f"  Total Trades:        {len(trades)}")
    print(f"  Total Invested:      ${total_invested:.2f}")
    print(f"  Resolved Trades:     {wins + losses} ({matched} matched)")
    print(f"  Unknown Resolution:  {unknown}")
    print()
    print(f"  WINS:                {wins}")
    print(f"  LOSSES:              {losses}")
    print(f"  Win Rate:            {(wins / (wins + losses) * 100) if (wins + losses) > 0 else 0:.1f}%")
    print()
    print(f"  Total P&L:           ${total_pnl:+.2f}")
    print(f"  ROI:                 {(total_pnl / total_invested * 100) if total_invested > 0 else 0:+.1f}%")
    print("=" * 70)

    # Print sample of results
    print()
    print("Sample Trade Results (first 20):")
    print("-" * 70)
    print(f"{'Market':<45} {'Pos':<4} {'Res':<4} {'Cost':>8} {'P&L':>10} {'Result':<6}")
    print("-" * 70)
    for r in results[:20]:
        print(f"{r['market']:<45} {r['position']:<4} {r['resolution']:<4} ${r['cost']:>6.2f} ${r['pnl']:>+8.2f} {r['result']:<6}")

    # Asset breakdown
    print()
    print("Results by Asset:")
    print("-" * 70)

    asset_stats = {}
    for trade in trades:
        if trade.asset not in asset_stats:
            asset_stats[trade.asset] = {"wins": 0, "losses": 0, "pnl": 0.0, "unknown": 0}

        if trade.actual_pnl is not None:
            if trade.actual_pnl > 0:
                asset_stats[trade.asset]["wins"] += 1
            else:
                asset_stats[trade.asset]["losses"] += 1
            asset_stats[trade.asset]["pnl"] += trade.actual_pnl
        else:
            asset_stats[trade.asset]["unknown"] += 1

    for asset, stats in sorted(asset_stats.items()):
        total = stats["wins"] + stats["losses"]
        wr = (stats["wins"] / total * 100) if total > 0 else 0
        print(f"  {asset}: {stats['wins']}W/{stats['losses']}L ({wr:.1f}% WR) | P&L: ${stats['pnl']:+.2f} | Unknown: {stats['unknown']}")

    print()
    print("Note: 'Unknown' trades could not be matched to Polymarket resolution data.")
    print("      These may need manual verification or the markets haven't resolved yet.")


if __name__ == "__main__":
    asyncio.run(main())
