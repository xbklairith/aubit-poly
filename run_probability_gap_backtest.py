#!/usr/bin/env python
"""Run Probability Gap strategy backtest."""

import asyncio
import os
from datetime import datetime, timedelta, UTC
from decimal import Decimal

# Set database URL
os.environ["DATABASE_URL"] = "postgres://aubit:aubit_dev_password@localhost:5432/aubit_poly"

from pylo.backtest.simulator import BacktestSimulator
from pylo.backtest.strategies.probability_gap import ProbabilityGapStrategy, MomentumContrarianStrategy
from pylo.backtest.strategies.expiry_scalper import ExpiryScalperStrategy
from pylo.backtest.strategies.contrarian_scalper import ContrarianScalperStrategy
from pylo.backtest.reports import generate_detailed_report, generate_comparison_report
from pylo.db.connection import get_database


async def run_backtest():
    """Run backtest for Probability Gap strategy."""
    db = get_database()

    try:
        # Warmup database connections
        await db.warmup()
        # Date range - last 10 days
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=10)

        assets = ["BTC", "ETH", "SOL", "XRP"]

        print("=" * 70)
        print("PROBABILITY GAP STRATEGY BACKTEST")
        print("=" * 70)
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print(f"Assets: {', '.join(assets)}")
        print()

        # Run Probability Gap Strategy
        print("Running Probability Gap Strategy...")
        prob_gap_strategy = ProbabilityGapStrategy(
            position_size=Decimal("100"),
            expiry_window_seconds=600,  # 10 minutes
            min_edge=Decimal("0.05"),   # 5% edge threshold
            kelly_fraction=Decimal("0.25"),
        )
        simulator = BacktestSimulator(db, prob_gap_strategy)
        prob_gap_run = await simulator.run(
            start_date=start_date,
            end_date=end_date,
            assets=assets,
            timeframe="15m",
        )

        print("\n" + generate_detailed_report(prob_gap_run))

        # Compare with baseline strategies
        print("\n" + "=" * 70)
        print("COMPARISON WITH BASELINE STRATEGIES")
        print("=" * 70)

        # Expiry Scalper (baseline - bet with market)
        print("\nRunning Expiry Scalper (baseline)...")
        expiry_strategy = ExpiryScalperStrategy(
            position_size=Decimal("100"),
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

        # Contrarian Scalper (baseline - bet against market)
        print("Running Contrarian Scalper (baseline)...")
        contrarian_strategy = ContrarianScalperStrategy(
            position_size=Decimal("100"),
            expiry_window_seconds=180,
            skew_threshold=Decimal("0.75"),
            use_market_order=True,  # Market orders for fair comparison
        )
        contrarian_sim = BacktestSimulator(db, contrarian_strategy)
        contrarian_run = await contrarian_sim.run(
            start_date=start_date,
            end_date=end_date,
            assets=assets,
            timeframe="15m",
        )

        # Momentum Contrarian
        print("Running Momentum Contrarian...")
        momentum_strategy = MomentumContrarianStrategy(
            position_size=Decimal("100"),
            expiry_window_seconds=300,
            skew_threshold=Decimal("0.75"),
            reversal_threshold=Decimal("0.02"),
        )
        momentum_sim = BacktestSimulator(db, momentum_strategy)
        momentum_run = await momentum_sim.run(
            start_date=start_date,
            end_date=end_date,
            assets=assets,
            timeframe="15m",
        )

        # Generate comparison report
        print("\n" + generate_comparison_report([
            prob_gap_run,
            expiry_run,
            contrarian_run,
            momentum_run,
        ]))

        # Summary table
        print("\n" + "=" * 70)
        print("SUMMARY TABLE")
        print("=" * 70)
        print(f"{'Strategy':<25} {'Trades':>8} {'Win Rate':>10} {'Net P/L':>12} {'ROI':>10}")
        print("-" * 70)

        for run in [prob_gap_run, expiry_run, contrarian_run, momentum_run]:
            m = run.metrics
            trades = m.orders_filled
            win_rate = f"{float(m.win_rate)*100:.1f}%" if m.win_rate else "N/A"
            pnl = f"${float(m.net_pnl):,.2f}"
            roi = f"{float(m.roi)*100:.1f}%" if m.roi else "N/A"
            print(f"{run.strategy_name:<25} {trades:>8} {win_rate:>10} {pnl:>12} {roi:>10}")

        print("-" * 70)

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(run_backtest())
