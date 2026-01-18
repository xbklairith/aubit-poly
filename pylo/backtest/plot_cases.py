"""Plot price graphs for contrarian fill vs no-fill cases."""

import asyncio
from datetime import timedelta
from decimal import Decimal

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from sqlalchemy import select, text

from pylo.db.connection import get_database
from pylo.backtest.models import TradeSide


async def get_example_markets(db, threshold: float = 0.75):
    """Get example markets for both cases."""
    async with db.session() as session:
        # Get markets with price history where skew >= threshold
        query = text("""
            WITH market_prices AS (
                SELECT
                    mr.condition_id,
                    mr.asset,
                    mr.name,
                    mr.winning_side,
                    mr.end_time,
                    ph.yes_price,
                    ph.no_price,
                    ph.timestamp,
                    CASE
                        WHEN ph.yes_price >= :threshold THEN 'YES'
                        WHEN ph.no_price >= :threshold THEN 'NO'
                        ELSE NULL
                    END as skewed_side,
                    ROW_NUMBER() OVER (PARTITION BY mr.condition_id ORDER BY ph.timestamp) as rn
                FROM market_resolutions mr
                JOIN price_history ph ON mr.condition_id = ph.condition_id
                WHERE ph.timestamp >= mr.end_time - interval '10 minutes'
                  AND ph.timestamp <= mr.end_time
                  AND (ph.yes_price >= :threshold OR ph.no_price >= :threshold)
            )
            SELECT DISTINCT ON (condition_id)
                condition_id,
                asset,
                name,
                winning_side,
                end_time,
                skewed_side
            FROM market_prices
            WHERE skewed_side IS NOT NULL
            ORDER BY condition_id, rn
        """)

        result = await session.execute(query, {"threshold": threshold})
        markets = result.fetchall()

        # Separate into skew_correct and skew_wrong
        skew_correct = []  # Skewed side won
        skew_wrong = []    # Skewed side lost (contrarian fills)

        for m in markets:
            winning = m.winning_side.upper() if m.winning_side else None
            skewed = m.skewed_side

            if winning == skewed:
                skew_correct.append(m)
            else:
                skew_wrong.append(m)

        return skew_correct, skew_wrong


async def get_price_history(db, condition_id: str):
    """Get price history for a market."""
    async with db.session() as session:
        query = text("""
            SELECT
                ph.yes_price,
                ph.no_price,
                ph.timestamp,
                mr.end_time,
                mr.winning_side
            FROM price_history ph
            JOIN market_resolutions mr ON ph.condition_id = mr.condition_id
            WHERE ph.condition_id = :condition_id
            ORDER BY ph.timestamp
        """)
        result = await session.execute(query, {"condition_id": condition_id})
        return result.fetchall()


async def plot_cases(output_path: str = "price_cases.png"):
    """Create plot showing both cases."""
    db = get_database()

    try:
        print("Fetching example markets...")
        skew_correct, skew_wrong = await get_example_markets(db)

        print(f"Found {len(skew_correct)} skew-correct markets (no contrarian fill)")
        print(f"Found {len(skew_wrong)} skew-wrong markets (contrarian fills)")

        # Pick examples
        if not skew_correct or not skew_wrong:
            print("Not enough examples found")
            return

        # Get 3 examples of each
        correct_examples = skew_correct[:3]
        wrong_examples = skew_wrong[:3]

        # Create figure with 2 rows (correct vs wrong) x 3 columns (examples)
        fig, axes = plt.subplots(2, 3, figsize=(15, 8))
        fig.suptitle('Contrarian Scalper: Price Trajectories Near Expiry', fontsize=14, fontweight='bold')

        # Plot skew-correct cases (top row) - contrarian does NOT fill
        for i, market in enumerate(correct_examples):
            ax = axes[0, i]
            prices = await get_price_history(db, market.condition_id)

            if prices:
                timestamps = [p.timestamp for p in prices]
                yes_prices = [float(p.yes_price) for p in prices]
                no_prices = [float(p.no_price) for p in prices]
                end_time = prices[0].end_time
                winning = prices[0].winning_side.upper()

                ax.plot(timestamps, yes_prices, 'g-', label='YES', linewidth=2)
                ax.plot(timestamps, no_prices, 'r-', label='NO', linewidth=2)
                ax.axvline(x=end_time, color='black', linestyle='--', alpha=0.5, label='Expiry')
                ax.axhline(y=0.75, color='orange', linestyle=':', alpha=0.7, label='Threshold')
                ax.axhline(y=0.01, color='purple', linestyle=':', alpha=0.7, label='Limit $0.01')

                ax.set_ylim(0, 1)
                ax.set_title(f'{market.asset} - {winning} Won\n(Skew Correct - NO FILL)', fontsize=10)
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                if i == 0:
                    ax.set_ylabel('Price ($)', fontweight='bold')
                    ax.legend(loc='center left', fontsize=8)

        # Plot skew-wrong cases (bottom row) - contrarian FILLS
        for i, market in enumerate(wrong_examples):
            ax = axes[1, i]
            prices = await get_price_history(db, market.condition_id)

            if prices:
                timestamps = [p.timestamp for p in prices]
                yes_prices = [float(p.yes_price) for p in prices]
                no_prices = [float(p.no_price) for p in prices]
                end_time = prices[0].end_time
                winning = prices[0].winning_side.upper()
                skewed = market.skewed_side

                ax.plot(timestamps, yes_prices, 'g-', label='YES', linewidth=2)
                ax.plot(timestamps, no_prices, 'r-', label='NO', linewidth=2)
                ax.axvline(x=end_time, color='black', linestyle='--', alpha=0.5, label='Expiry')
                ax.axhline(y=0.75, color='orange', linestyle=':', alpha=0.7, label='Threshold')
                ax.axhline(y=0.01, color='purple', linestyle=':', alpha=0.7, label='Limit $0.01')

                ax.set_ylim(0, 1)
                ax.set_title(f'{market.asset} - {winning} Won (was {skewed} skewed)\n(Surprise - CONTRARIAN FILLS)',
                           fontsize=10, color='darkgreen')
                ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                if i == 0:
                    ax.set_ylabel('Price ($)', fontweight='bold')
                    ax.legend(loc='center left', fontsize=8)

        # Add row labels
        fig.text(0.02, 0.72, 'SKEW\nCORRECT\n(No Fill)', ha='left', va='center',
                fontsize=11, fontweight='bold', color='red')
        fig.text(0.02, 0.28, 'SKEW\nWRONG\n(Fill!)', ha='left', va='center',
                fontsize=11, fontweight='bold', color='green')

        plt.tight_layout(rect=[0.05, 0, 1, 0.95])
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        print(f"\nSaved plot to: {output_path}")

        # Also show stats
        total = len(skew_correct) + len(skew_wrong)
        print(f"\nStats at 0.75 threshold:")
        print(f"  Skew correct (no fill): {len(skew_correct)} ({len(skew_correct)/total*100:.1f}%)")
        print(f"  Skew wrong (fills):     {len(skew_wrong)} ({len(skew_wrong)/total*100:.1f}%)")

    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(plot_cases())
