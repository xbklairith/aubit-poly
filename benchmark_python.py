#!/usr/bin/env python3
"""Python trade executor benchmark for comparison with Rust."""

import asyncio
import os
import time
import statistics

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import text
from pylo.db.connection import get_database


async def run_benchmark(cycles: int = 100) -> None:
    """Run benchmark cycles and report statistics."""
    print(f"\n{'='*63}")
    print("  PYTHON TRADE EXECUTOR BENCHMARK")
    print(f"{'='*63}")
    print(f"  Running {cycles} benchmark cycles...")
    print(f"{'='*63}\n")

    # Initialize database
    db = get_database()

    cycle_times = []
    markets_count = 0

    # Use same query as Rust with same parameters
    query = text("""
        SELECT
            m.id, m.condition_id, m.market_type, m.asset, m.timeframe,
            m.yes_token_id, m.no_token_id, m.name, m.end_time, m.is_active,
            o.yes_best_ask, o.yes_best_bid, o.no_best_ask, o.no_best_bid, o.captured_at
        FROM markets m
        INNER JOIN LATERAL (
            SELECT yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
            FROM orderbook_snapshots
            WHERE market_id = m.id
              AND captured_at > NOW() - make_interval(secs => :max_age)
            ORDER BY captured_at DESC
            LIMIT 1
        ) o ON true
        WHERE m.is_active = true
          AND m.asset = ANY(:assets)
          AND m.end_time > NOW()
          AND m.end_time <= NOW() + make_interval(secs => :max_expiry)
        ORDER BY m.end_time ASC
    """)

    params = {
        "max_age": 86400.0,  # 1 day - same as Rust
        "assets": ["BTC", "ETH", "SOL", "XRP"],
        "max_expiry": 604800.0,  # 1 week - same as Rust
    }

    for i in range(cycles):
        start = time.perf_counter()

        try:
            async with db.session() as session:
                result = await session.execute(query, params)
                rows = result.fetchall()
                markets_count = len(rows)

        except Exception as e:
            print(f"Error in cycle {i}: {e}")
            continue

        elapsed_ms = (time.perf_counter() - start) * 1000
        cycle_times.append(elapsed_ms)

        if (i + 1) % 10 == 0:
            print(f"  Completed {i+1} / {cycles} cycles... (markets: {markets_count})")

    # Calculate statistics
    if cycle_times:
        cycle_times.sort()
        count = len(cycle_times)
        total = sum(cycle_times)
        avg = statistics.mean(cycle_times)
        min_t = min(cycle_times)
        max_t = max(cycle_times)
        p50 = cycle_times[count // 2]
        p95 = cycle_times[int(count * 0.95)]
        p99 = cycle_times[int(count * 0.99)]

        print(f"""
{'='*63}
  PYTHON TRADE EXECUTOR BENCHMARK RESULTS
{'='*63}
  Cycles run:       {count}
  Markets queried:  {markets_count}
  Total time:       {total:.0f}ms

  Cycle Time (ms):
    Average:        {avg:.2f}
    Min:            {min_t:.2f}
    Max:            {max_t:.2f}
    P50:            {p50:.2f}
    P95:            {p95:.2f}
    P99:            {p99:.2f}
{'='*63}
""")

    # Cleanup
    await db.close()


if __name__ == "__main__":
    asyncio.run(run_benchmark(100))
