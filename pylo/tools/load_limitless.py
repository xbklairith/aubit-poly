#!/usr/bin/env python3
"""Load Limitless markets into the database."""

import asyncio
import logging
import os
import re
import sys
from datetime import datetime, timezone

import asyncpg

# Add parent dir to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from pylo.data_sources.limitless import LimitlessClient


def parse_expiry_from_title(title: str, raw_data: dict) -> datetime | None:
    """
    Parse expiry time from market title like:
    "💎 $ETH above $2928.82 on Jan 26, 14:00 UTC?"
    """
    # Try to get from raw API data first
    created_at = raw_data.get("createdAt", "")
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            # Hourly markets expire ~1 hour after creation
            # Check for time in title
            pass
        except ValueError:
            pass

    # Pattern: "on Jan 26, 14:00 UTC" or similar
    pattern = r"on\s+(\w+)\s+(\d+),?\s+(\d{1,2}):(\d{2})\s*UTC"
    match = re.search(pattern, title, re.IGNORECASE)
    if match:
        month_str, day, hour, minute = match.groups()
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        month = month_map.get(month_str.lower(), 1)
        try:
            # Use current year
            year = datetime.now(timezone.utc).year
            dt = datetime(year, month, int(day), int(hour), int(minute), tzinfo=timezone.utc)
            return dt
        except ValueError:
            pass

    return None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


async def main():
    """Load Limitless markets into database."""
    database_url = os.getenv("DATABASE_URL", "postgresql://aubit:aubit_dev_password@localhost:5432/aubit_poly")

    logger.info("Connecting to database...")
    pool = await asyncpg.create_pool(database_url)

    logger.info("Fetching Limitless markets...")
    client = LimitlessClient()
    await client.connect()

    try:
        markets = await client.get_markets(limit=100)
        logger.info(f"Fetched {len(markets)} markets from Limitless")

        inserted = 0
        updated = 0

        for market in markets:
            # Extract asset and timeframe
            asset, timeframe = client._extract_asset_timeframe(market.id, market.name)

            # Skip non-crypto markets
            if asset == "UNKNOWN":
                logger.debug(f"Skipping non-crypto market: {market.name}")
                continue

            # Get position IDs from outcomes
            yes_pos_id = market.outcomes[0].id if market.outcomes else ""
            no_pos_id = market.outcomes[1].id if len(market.outcomes) > 1 else ""

            # Determine direction from title
            direction = None
            title_lower = market.name.lower()
            if "above" in title_lower:
                direction = "above"
            elif "below" in title_lower:
                direction = "below"

            # Determine market type
            market_type = "above" if direction in ("above", "below") else "unknown"

            # Parse end time from title (more accurate than end_date)
            raw_data = market.raw or {}
            end_time = parse_expiry_from_title(market.name, raw_data)
            if not end_time:
                end_time = market.end_date
            if end_time and end_time.tzinfo is None:
                end_time = end_time.replace(tzinfo=timezone.utc)

            try:
                result = await pool.fetchrow(
                    """
                    INSERT INTO markets (
                        platform, condition_id, market_type, asset, timeframe,
                        yes_token_id, no_token_id, name, end_time,
                        liquidity_dollars, direction, settlement_chain,
                        is_active
                    )
                    VALUES (
                        'limitless', $1, $2, $3, $4,
                        $5, $6, $7, $8,
                        $9, $10, 'base',
                        true
                    )
                    ON CONFLICT (platform, condition_id) DO UPDATE SET
                        market_type = EXCLUDED.market_type,
                        asset = EXCLUDED.asset,
                        timeframe = EXCLUDED.timeframe,
                        yes_token_id = EXCLUDED.yes_token_id,
                        no_token_id = EXCLUDED.no_token_id,
                        name = EXCLUDED.name,
                        end_time = EXCLUDED.end_time,
                        liquidity_dollars = EXCLUDED.liquidity_dollars,
                        direction = EXCLUDED.direction,
                        is_active = true,
                        updated_at = NOW()
                    RETURNING id, (xmax = 0) as inserted
                    """,
                    market.id,  # condition_id (slug)
                    market_type,
                    asset,
                    timeframe,
                    yes_pos_id,
                    no_pos_id,
                    market.name[:500],  # Truncate name
                    end_time,
                    float(market.liquidity) if market.liquidity else None,
                    direction,
                )

                if result["inserted"]:
                    inserted += 1
                else:
                    updated += 1

            except Exception as e:
                logger.error(f"Failed to insert market {market.id}: {e}")
                continue

        logger.info(f"Loaded {inserted} new markets, updated {updated} existing")

        # Also insert orderbook snapshots with current prices
        logger.info("Inserting price snapshots...")
        snapshots = 0

        for market in markets:
            asset, _ = client._extract_asset_timeframe(market.id, market.name)
            if asset == "UNKNOWN":
                continue

            # Get market ID from database
            row = await pool.fetchrow(
                "SELECT id FROM markets WHERE platform = 'limitless' AND condition_id = $1",
                market.id
            )
            if not row:
                continue

            market_id = row["id"]

            # Get prices
            yes_price = float(market.yes_ask_price) if market.yes_ask_price else None
            no_price = float(market.no_ask_price) if market.no_ask_price else None

            if yes_price is None or no_price is None:
                continue

            try:
                await pool.execute(
                    """
                    INSERT INTO orderbook_snapshots (
                        market_id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, captured_at
                    )
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    ON CONFLICT (market_id) DO UPDATE SET
                        yes_best_ask = EXCLUDED.yes_best_ask,
                        yes_best_bid = EXCLUDED.yes_best_bid,
                        no_best_ask = EXCLUDED.no_best_ask,
                        no_best_bid = EXCLUDED.no_best_bid,
                        captured_at = NOW()
                    """,
                    market_id,
                    yes_price,
                    yes_price * 0.98,  # Estimate bid as 98% of ask
                    no_price,
                    no_price * 0.98,
                )
                snapshots += 1
            except Exception as e:
                logger.error(f"Failed to insert snapshot for {market.id}: {e}")

        logger.info(f"Inserted {snapshots} price snapshots")

    finally:
        await client.disconnect()
        await pool.close()

    logger.info("Done!")


if __name__ == "__main__":
    asyncio.run(main())
