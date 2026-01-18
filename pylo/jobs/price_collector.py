#!/usr/bin/env python3
"""
Price Collector Job

Collects pre-expiry prices for 15m crypto markets to enable accurate backtesting.

Two modes:
1. Backfill: Migrate existing orderbook_snapshots to price_history
2. Continuous: Collect prices for markets expiring in the next 10 minutes

Usage:
    uv run python -m pylo.jobs.price_collector backfill
    uv run python -m pylo.jobs.price_collector collect --interval 30
    uv run python -m pylo.jobs.price_collector run  # Both backfill + continuous
"""

import argparse
import asyncio
import logging
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Reduce noise
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


async def backfill_from_orderbook_snapshots() -> int:
    """
    Migrate orderbook_snapshots to price_history table.

    Returns:
        Number of records migrated
    """
    from pylo.db.connection import get_database

    logger.info("Backfilling price_history from orderbook_snapshots...")

    db = get_database()

    try:
        async with db.session() as session:
            # Get orderbook snapshots that can be matched to markets
            result = await session.execute(text("""
                INSERT INTO price_history (
                    condition_id, yes_token_id, no_token_id,
                    yes_price, no_price, timestamp
                )
                SELECT
                    m.condition_id,
                    m.yes_token_id,
                    m.no_token_id,
                    COALESCE(o.yes_best_bid, 0),
                    COALESCE(o.no_best_bid, 0),
                    o.captured_at
                FROM orderbook_snapshots o
                JOIN markets m ON o.market_id = m.id
                WHERE m.condition_id IS NOT NULL
                  AND m.yes_token_id IS NOT NULL
                  AND m.no_token_id IS NOT NULL
                ON CONFLICT (condition_id, timestamp) DO NOTHING
                RETURNING condition_id
            """))

            count = len(result.fetchall())
            logger.info(f"Migrated {count} orderbook snapshots to price_history")
            return count

    finally:
        await db.close()


async def backfill_from_clob_api(
    days: int = 30,
    window_minutes: int = 10,
    concurrency: int = 5,
) -> int:
    """
    Fetch historical price data from CLOB API for resolved markets.

    Uses the /prices-history endpoint with 'market' parameter.

    Args:
        days: How many days back to fetch
        window_minutes: Minutes before expiry to fetch
        concurrency: Concurrent API requests

    Returns:
        Number of price points stored
    """
    import httpx
    from pylo.db.connection import get_database

    logger.info(f"Backfilling price_history from CLOB API (last {days} days, {window_minutes}min window)...")

    db = get_database()

    try:
        async with db.session() as session:
            # Get all market resolutions with token IDs
            result = await session.execute(text("""
                SELECT condition_id, yes_token_id, no_token_id, end_time, name
                FROM market_resolutions
                WHERE end_time >= NOW() - MAKE_INTERVAL(days => :days)
                  AND yes_token_id IS NOT NULL
                  AND no_token_id IS NOT NULL
                ORDER BY end_time DESC
            """), {"days": days})
            markets = result.fetchall()

        logger.info(f"Found {len(markets)} markets to backfill")

        total_stored = 0
        semaphore = asyncio.Semaphore(concurrency)

        async with httpx.AsyncClient(timeout=30.0) as client:
            async def fetch_one(market) -> int:
                async with semaphore:
                    try:
                        count = await _fetch_market_history(
                            client, db, market, window_minutes
                        )
                        if count > 0:
                            logger.debug(f"Stored {count} points for {market.name[:40]}")
                        return count
                    except Exception as e:
                        logger.error(f"Failed to fetch {market.condition_id}: {e}")
                        return 0

            tasks = [fetch_one(m) for m in markets]
            results = await asyncio.gather(*tasks)
            total_stored = sum(results)

        logger.info(f"Total: Stored {total_stored} price history points from CLOB API")
        return total_stored

    finally:
        await db.close()


async def _fetch_market_history(client, db, market, window_minutes: int) -> int:
    """Fetch and store price history for a single market."""
    from datetime import datetime, UTC

    end_ts = int(market.end_time.timestamp())
    start_ts = end_ts - (window_minutes * 60)

    # Fetch YES (Up) token history
    yes_history = await _fetch_token_history(client, market.yes_token_id, start_ts, end_ts)

    # Fetch NO (Down) token history
    no_history = await _fetch_token_history(client, market.no_token_id, start_ts, end_ts)

    if not yes_history and not no_history:
        return 0

    # Merge by timestamp
    yes_by_ts = {p["t"]: Decimal(str(p["p"])) for p in yes_history}
    no_by_ts = {p["t"]: Decimal(str(p["p"])) for p in no_history}
    all_timestamps = sorted(set(yes_by_ts.keys()) | set(no_by_ts.keys()))

    stored = 0
    async with db.session() as session:
        for ts in all_timestamps:
            yes_price = yes_by_ts.get(ts)
            no_price = no_by_ts.get(ts)

            # Infer missing price
            if yes_price is not None and no_price is None:
                no_price = Decimal("1") - yes_price
            elif no_price is not None and yes_price is None:
                yes_price = Decimal("1") - no_price
            elif yes_price is None and no_price is None:
                continue

            try:
                await session.execute(text("""
                    INSERT INTO price_history (
                        condition_id, yes_token_id, no_token_id,
                        yes_price, no_price, timestamp
                    ) VALUES (
                        :condition_id, :yes_token_id, :no_token_id,
                        :yes_price, :no_price, :timestamp
                    )
                    ON CONFLICT (condition_id, timestamp) DO UPDATE SET
                        yes_price = EXCLUDED.yes_price,
                        no_price = EXCLUDED.no_price
                """), {
                    "condition_id": market.condition_id,
                    "yes_token_id": market.yes_token_id,
                    "no_token_id": market.no_token_id,
                    "yes_price": float(yes_price),
                    "no_price": float(no_price),
                    "timestamp": datetime.fromtimestamp(ts, tz=UTC),
                })
                stored += 1
            except Exception:
                pass

    return stored


async def _fetch_token_history(client, token_id: str, start_ts: int, end_ts: int) -> list:
    """Fetch price history for a token from CLOB API."""
    try:
        response = await client.get(
            "https://clob.polymarket.com/prices-history",
            params={
                "market": token_id,  # Correct param name per docs
                "startTs": start_ts,
                "endTs": end_ts,
                "fidelity": 1,  # 1 minute resolution
            }
        )
        response.raise_for_status()
        return response.json().get("history", [])
    except Exception as e:
        logger.debug(f"Failed to fetch history for {token_id[:20]}: {e}")
        return []


async def collect_expiring_prices(interval_seconds: int = 30) -> None:
    """
    Continuously collect prices for markets expiring soon.

    Args:
        interval_seconds: How often to collect prices
    """
    from pylo.data_sources.polymarket import PolymarketClient, CRYPTO_SERIES_15M
    from pylo.db.connection import get_database

    logger.info(f"Starting continuous price collection (interval: {interval_seconds}s)")

    client = PolymarketClient()
    db = get_database()

    await client.connect()

    try:
        while True:
            try:
                now = datetime.now(UTC)

                # Find markets expiring in next 10 minutes
                markets_to_track = []

                for asset, series_id in CRYPTO_SERIES_15M.items():
                    # Fetch active (not closed) events for this series
                    events = await _fetch_active_events(client, series_id)

                    for event in events:
                        end_date_str = event.get("endDate")
                        if not end_date_str:
                            continue

                        try:
                            end_time = datetime.fromisoformat(
                                end_date_str.replace("Z", "+00:00")
                            )
                        except (ValueError, TypeError):
                            continue

                        # Check if expiring in next 10 minutes
                        time_to_expiry = (end_time - now).total_seconds()
                        if 0 < time_to_expiry <= 600:  # 0-10 minutes
                            for market in event.get("markets", []):
                                if market.get("active") and not market.get("closed"):
                                    markets_to_track.append({
                                        "condition_id": market.get("conditionId"),
                                        "asset": asset,
                                        "end_time": end_time,
                                        "time_to_expiry": time_to_expiry,
                                        "clob_token_ids": market.get("clobTokenIds"),
                                    })

                if markets_to_track:
                    logger.info(f"Found {len(markets_to_track)} markets expiring in next 10min")

                    # Fetch and store prices for each market
                    for m in markets_to_track:
                        await _capture_price(client, db, m)
                else:
                    logger.debug("No markets expiring soon")

                await asyncio.sleep(interval_seconds)

            except Exception as e:
                logger.error(f"Error in collection loop: {e}")
                await asyncio.sleep(interval_seconds)

    finally:
        await client.disconnect()
        await db.close()


async def _fetch_active_events(client, series_id: str) -> list:
    """Fetch active events for a series."""
    import httpx

    if not client._client:
        return []

    try:
        async with client._throttler:
            response = await client._client.get(
                "https://gamma-api.polymarket.com/events",
                params={
                    "series_id": series_id,
                    "active": "true",
                    "closed": "false",
                    "limit": "20",
                }
            )
            response.raise_for_status()
            return response.json()
    except httpx.HTTPError as e:
        logger.error(f"Failed to fetch events for series {series_id}: {e}")
        return []


async def _capture_price(client, db, market: dict) -> None:
    """Capture current price for a market."""
    import json

    condition_id = market.get("condition_id")
    token_ids_str = market.get("clob_token_ids")

    if not condition_id or not token_ids_str:
        return

    try:
        if isinstance(token_ids_str, str):
            token_ids = json.loads(token_ids_str)
        else:
            token_ids = token_ids_str

        if len(token_ids) < 2:
            return

        yes_token_id = token_ids[0]
        no_token_id = token_ids[1]

        # Fetch current prices from CLOB
        yes_price = await _fetch_current_price(client, yes_token_id)
        no_price = await _fetch_current_price(client, no_token_id)

        if yes_price is None and no_price is None:
            return

        # Default missing prices
        if yes_price is None:
            yes_price = Decimal("1") - (no_price or Decimal("0.5"))
        if no_price is None:
            no_price = Decimal("1") - (yes_price or Decimal("0.5"))

        # Store in price_history
        async with db.session() as session:
            await session.execute(text("""
                INSERT INTO price_history (
                    condition_id, yes_token_id, no_token_id,
                    yes_price, no_price, timestamp
                ) VALUES (
                    :condition_id, :yes_token_id, :no_token_id,
                    :yes_price, :no_price, :timestamp
                )
                ON CONFLICT (condition_id, timestamp) DO UPDATE SET
                    yes_price = EXCLUDED.yes_price,
                    no_price = EXCLUDED.no_price
            """), {
                "condition_id": condition_id,
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "yes_price": float(yes_price),
                "no_price": float(no_price),
                "timestamp": datetime.now(UTC),
            })

        time_to_expiry = market.get("time_to_expiry", 0)
        logger.info(
            f"Captured {market.get('asset')} | "
            f"YES:{yes_price:.3f} NO:{no_price:.3f} | "
            f"{time_to_expiry:.0f}s to expiry"
        )

    except Exception as e:
        logger.error(f"Failed to capture price for {condition_id}: {e}")


async def _fetch_current_price(client, token_id: str) -> Decimal | None:
    """Fetch current price for a token."""
    import httpx

    if not client._client:
        return None

    try:
        async with client._throttler:
            response = await client._client.get(
                "https://clob.polymarket.com/price",
                params={"token_id": token_id, "side": "buy"}
            )
            response.raise_for_status()
            data = response.json()
            price = data.get("price")
            if price:
                return Decimal(str(price))
    except httpx.HTTPError:
        pass

    return None


async def run_all(interval_seconds: int = 30) -> None:
    """Run backfill then continuous collection."""
    # First backfill
    await backfill_from_orderbook_snapshots()

    # Then continuous collection
    await collect_expiring_prices(interval_seconds)


def main():
    parser = argparse.ArgumentParser(
        description="Price collector for backtest data"
    )

    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # backfill command (from orderbook_snapshots)
    subparsers.add_parser("backfill", help="Backfill from orderbook_snapshots")

    # backfill-clob command (from CLOB API)
    clob_parser = subparsers.add_parser("backfill-clob", help="Backfill from CLOB prices-history API")
    clob_parser.add_argument(
        "--days", type=int, default=30,
        help="Days of history to fetch"
    )
    clob_parser.add_argument(
        "--window", type=int, default=10,
        help="Minutes before expiry to fetch"
    )
    clob_parser.add_argument(
        "--concurrency", type=int, default=5,
        help="Concurrent API requests"
    )

    # collect command
    collect_parser = subparsers.add_parser("collect", help="Continuous collection")
    collect_parser.add_argument(
        "--interval", type=int, default=30,
        help="Collection interval in seconds"
    )

    # run command (both)
    run_parser = subparsers.add_parser("run", help="Backfill + continuous")
    run_parser.add_argument(
        "--interval", type=int, default=30,
        help="Collection interval in seconds"
    )

    args = parser.parse_args()

    if args.command == "backfill":
        asyncio.run(backfill_from_orderbook_snapshots())
    elif args.command == "backfill-clob":
        asyncio.run(backfill_from_clob_api(
            days=args.days,
            window_minutes=args.window,
            concurrency=args.concurrency,
        ))
    elif args.command == "collect":
        asyncio.run(collect_expiring_prices(args.interval))
    elif args.command == "run":
        asyncio.run(run_all(args.interval))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
