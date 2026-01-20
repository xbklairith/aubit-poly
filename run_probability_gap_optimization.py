#!/usr/bin/env python
"""Optimize Probability Gap strategy parameters."""

import asyncio
import os
from datetime import datetime, timedelta, UTC
from decimal import Decimal
from dataclasses import dataclass

# Set database URL
os.environ["DATABASE_URL"] = "postgres://aubit:aubit_dev_password@localhost:5432/aubit_poly"

from pylo.backtest.simulator import BacktestSimulator
from pylo.backtest.strategies.probability_gap import ProbabilityGapStrategy
from pylo.db.connection import get_database


@dataclass
class OptResult:
    min_edge: str
    expiry_window: int
    trades: int
    win_rate: float
    roi: float
    pnl: float


async def run_optimization():
    """Run parameter optimization for Probability Gap strategy."""
    db = get_database()
    await db.warmup()

    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=10)
    assets = ["BTC", "ETH", "SOL", "XRP"]

    print("=" * 80)
    print("PROBABILITY GAP STRATEGY - PARAMETER OPTIMIZATION")
    print("=" * 80)
    print(f"Period: {start_date.date()} to {end_date.date()}")
    print(f"Assets: {', '.join(assets)}")
    print()

    # Parameter grid
    edge_thresholds = ["0.03", "0.05", "0.07", "0.10", "0.15"]
    expiry_windows = [120, 180, 300, 600]  # 2, 3, 5, 10 minutes

    results: list[OptResult] = []

    total_runs = len(edge_thresholds) * len(expiry_windows)
    current_run = 0

    for min_edge in edge_thresholds:
        for expiry_window in expiry_windows:
            current_run += 1
            print(f"[{current_run}/{total_runs}] Testing min_edge={min_edge}, expiry_window={expiry_window}s...")

            strategy = ProbabilityGapStrategy(
                position_size=Decimal("100"),
                expiry_window_seconds=expiry_window,
                min_edge=Decimal(min_edge),
                kelly_fraction=Decimal("0.25"),
            )

            simulator = BacktestSimulator(db, strategy)
            run = await simulator.run(
                start_date=start_date,
                end_date=end_date,
                assets=assets,
                timeframe="15m",
            )

            m = run.metrics
            results.append(OptResult(
                min_edge=min_edge,
                expiry_window=expiry_window,
                trades=m.orders_filled,
                win_rate=float(m.win_rate) * 100 if m.win_rate else 0,
                roi=float(m.roi) * 100 if m.roi else 0,
                pnl=float(m.net_pnl),
            ))

    # Sort by ROI
    results.sort(key=lambda x: x.roi, reverse=True)

    print()
    print("=" * 80)
    print("OPTIMIZATION RESULTS (sorted by ROI)")
    print("=" * 80)
    print(f"{'Min Edge':<10} {'Window':<10} {'Trades':>8} {'Win Rate':>10} {'ROI':>10} {'Net P/L':>12}")
    print("-" * 80)

    for r in results:
        print(f"{r.min_edge:<10} {r.expiry_window}s{'':<5} {r.trades:>8} {r.win_rate:>9.1f}% {r.roi:>9.1f}% ${r.pnl:>10,.2f}")

    print("-" * 80)

    # Find best configuration
    best = results[0]
    print(f"\nBEST CONFIGURATION:")
    print(f"  Min Edge: {best.min_edge}")
    print(f"  Expiry Window: {best.expiry_window}s")
    print(f"  Win Rate: {best.win_rate:.1f}%")
    print(f"  ROI: {best.roi:.1f}%")
    print(f"  Net P/L: ${best.pnl:,.2f}")

    # Find best by win rate
    by_winrate = sorted(results, key=lambda x: x.win_rate, reverse=True)
    print(f"\nBEST BY WIN RATE:")
    print(f"  Configuration: min_edge={by_winrate[0].min_edge}, window={by_winrate[0].expiry_window}s")
    print(f"  Win Rate: {by_winrate[0].win_rate:.1f}%")

    # Summary statistics
    print()
    print("=" * 80)
    print("INSIGHTS")
    print("=" * 80)

    # Group by edge threshold
    print("\nAverage ROI by Min Edge Threshold:")
    for edge in edge_thresholds:
        edge_results = [r for r in results if r.min_edge == edge]
        avg_roi = sum(r.roi for r in edge_results) / len(edge_results)
        avg_trades = sum(r.trades for r in edge_results) / len(edge_results)
        print(f"  {edge}: {avg_roi:>6.2f}% ROI, {avg_trades:>6.0f} avg trades")

    # Group by expiry window
    print("\nAverage ROI by Expiry Window:")
    for window in expiry_windows:
        window_results = [r for r in results if r.expiry_window == window]
        avg_roi = sum(r.roi for r in window_results) / len(window_results)
        avg_win = sum(r.win_rate for r in window_results) / len(window_results)
        print(f"  {window}s: {avg_roi:>6.2f}% ROI, {avg_win:>5.1f}% win rate")


if __name__ == "__main__":
    asyncio.run(run_optimization())
