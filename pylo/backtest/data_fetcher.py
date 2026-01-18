"""Data fetcher for backtest historical data."""

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from pylo.backtest.models import MarketResolution, PriceSnapshot, TradeSide
from pylo.data_sources.polymarket import PolymarketClient
from pylo.db.connection import Database

logger = logging.getLogger(__name__)


def parse_asset_from_name(name: str) -> str | None:
    """Extract asset symbol from market name."""
    name_lower = name.lower()
    if "bitcoin" in name_lower or "btc" in name_lower:
        return "BTC"
    if "ethereum" in name_lower or "eth" in name_lower:
        return "ETH"
    if "solana" in name_lower or "sol" in name_lower:
        return "SOL"
    if "xrp" in name_lower or "ripple" in name_lower:
        return "XRP"
    return None


def parse_timeframe_from_name(name: str) -> str | None:
    """Extract timeframe from market name."""
    name_lower = name.lower()
    # Check for common timeframe patterns
    if "15 minute" in name_lower or "15-minute" in name_lower or "15min" in name_lower:
        return "15m"
    if "1 hour" in name_lower or "1-hour" in name_lower or "hourly" in name_lower:
        return "1h"
    if "4 hour" in name_lower or "4-hour" in name_lower:
        return "4h"
    if "daily" in name_lower or "24 hour" in name_lower:
        return "daily"
    return None


def parse_market_type_from_name(name: str) -> str:
    """Extract market type from name."""
    name_lower = name.lower()
    if "up or down" in name_lower or "go up" in name_lower:
        return "up_down"
    if "above" in name_lower:
        return "above"
    if "between" in name_lower or "range" in name_lower:
        return "price_range"
    return "unknown"


def extract_token_ids(market) -> tuple[str, str]:
    """Extract YES/UP and NO/DOWN token IDs from market.

    For Up/Down markets, "Up" is treated as the YES/positive outcome.
    """
    yes_token_id = ""
    no_token_id = ""

    for outcome in market.outcomes:
        name_upper = outcome.name.upper()
        # YES-like outcomes (positive)
        if name_upper in ("YES", "TRUE", "1", "UP", "HIGHER", "ABOVE"):
            yes_token_id = outcome.id
        # NO-like outcomes (negative)
        elif name_upper in ("NO", "FALSE", "0", "DOWN", "LOWER", "BELOW"):
            no_token_id = outcome.id

    # Fallback to positional if names don't match
    if not yes_token_id and len(market.outcomes) > 0:
        yes_token_id = market.outcomes[0].id
    if not no_token_id and len(market.outcomes) > 1:
        no_token_id = market.outcomes[1].id

    return yes_token_id, no_token_id


class DataFetcher:
    """Fetches historical data from Polymarket for backtesting."""

    def __init__(self, db: Database):
        """Initialize data fetcher.

        Args:
            db: Database connection instance
        """
        self.db = db
        self.client = PolymarketClient()

    async def fetch_and_store_resolutions(
        self,
        days: int = 30,
        assets: list[str] | None = None,
        timeframe: str = "15m",
    ) -> int:
        """
        Fetch closed markets and store their resolutions.

        Uses series_id-based fetching for accurate historical data.

        Args:
            days: Number of days to look back
            assets: List of assets to filter
            timeframe: Market timeframe to filter

        Returns:
            Number of resolutions stored
        """
        if assets is None:
            assets = ["BTC", "ETH", "SOL", "XRP"]

        logger.info(f"Fetching closed {timeframe} markets for {assets} over last {days} days...")

        await self.client.connect()

        try:
            # Use series_id-based fetching (the correct approach)
            markets = await self.client.get_all_closed_markets_by_series(
                days=days,
                assets=assets,
                timeframe=timeframe,
            )

            logger.info(f"Found {len(markets)} closed markets, processing...")

            stored_count = 0
            async with self.db.session() as session:
                for market in markets:
                    # Parse resolution (winning side)
                    # For Up/Down markets, resolution is "Up" or "Down"
                    resolution = market.resolution
                    if not resolution:
                        continue

                    resolution_lower = resolution.lower()
                    # Handle both Yes/No and Up/Down outcomes
                    if resolution_lower in ("yes", "true", "1", "up", "higher", "above"):
                        winning_side = TradeSide.YES  # YES = Up = positive outcome
                    elif resolution_lower in ("no", "false", "0", "down", "lower", "below"):
                        winning_side = TradeSide.NO  # NO = Down = negative outcome
                    else:
                        # Skip markets without clear resolution
                        logger.debug(f"Skipping market with unclear resolution: {resolution}")
                        continue

                    # Extract token IDs
                    yes_token_id, no_token_id = extract_token_ids(market)
                    if not yes_token_id or not no_token_id:
                        logger.debug(f"Skipping market without token IDs: {market.id}")
                        continue

                    # Parse metadata from name
                    asset = parse_asset_from_name(market.name)
                    parsed_timeframe = parse_timeframe_from_name(market.name)
                    market_type = parse_market_type_from_name(market.name)

                    if not asset:
                        continue  # Skip non-crypto markets

                    # Get final prices from outcomes
                    # For resolved markets: winner has price=1, loser has price=0
                    final_yes = None
                    final_no = None
                    for outcome in market.outcomes:
                        name_upper = outcome.name.upper()
                        if name_upper in ("YES", "UP", "HIGHER", "ABOVE"):
                            final_yes = outcome.price
                        elif name_upper in ("NO", "DOWN", "LOWER", "BELOW"):
                            final_no = outcome.price

                    # If we only have one price (winner=1), infer the other
                    if final_yes is not None and final_no is None:
                        final_no = Decimal("1") - final_yes
                    elif final_no is not None and final_yes is None:
                        final_yes = Decimal("1") - final_no

                    # Store in database
                    try:
                        await self._upsert_resolution(
                            session,
                            condition_id=market.id,
                            market_type=market_type,
                            asset=asset,
                            timeframe=parsed_timeframe or timeframe,
                            name=market.name,
                            yes_token_id=yes_token_id,
                            no_token_id=no_token_id,
                            winning_side=winning_side.value,
                            end_time=market.end_date,
                            final_yes_price=final_yes,
                            final_no_price=final_no,
                            raw_data=market.raw,
                        )
                        stored_count += 1
                    except Exception as e:
                        logger.error(f"Failed to store resolution for {market.id}: {e}")

            logger.info(f"Stored {stored_count} market resolutions")
            return stored_count

        finally:
            await self.client.disconnect()

    async def _upsert_resolution(
        self,
        session: AsyncSession,
        condition_id: str,
        market_type: str,
        asset: str,
        timeframe: str,
        name: str,
        yes_token_id: str,
        no_token_id: str,
        winning_side: str,
        end_time: datetime | None,
        final_yes_price: Decimal | None,
        final_no_price: Decimal | None,
        raw_data: dict | None,
    ) -> None:
        """Upsert a market resolution."""
        import json

        await session.execute(
            text("""
                INSERT INTO market_resolutions (
                    condition_id, market_type, asset, timeframe, name,
                    yes_token_id, no_token_id, winning_side, end_time,
                    final_yes_price, final_no_price, raw_data
                ) VALUES (
                    :condition_id, :market_type, :asset, :timeframe, :name,
                    :yes_token_id, :no_token_id, :winning_side, :end_time,
                    :final_yes_price, :final_no_price, :raw_data
                )
                ON CONFLICT (condition_id) DO UPDATE SET
                    winning_side = EXCLUDED.winning_side,
                    final_yes_price = EXCLUDED.final_yes_price,
                    final_no_price = EXCLUDED.final_no_price,
                    raw_data = EXCLUDED.raw_data,
                    fetched_at = NOW()
            """),
            {
                "condition_id": condition_id,
                "market_type": market_type,
                "asset": asset,
                "timeframe": timeframe,
                "name": name,
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "winning_side": winning_side,
                "end_time": end_time,
                "final_yes_price": float(final_yes_price) if final_yes_price else None,
                "final_no_price": float(final_no_price) if final_no_price else None,
                "raw_data": json.dumps(raw_data) if raw_data else None,
            },
        )

    async def fetch_and_store_price_history(
        self,
        condition_id: str,
        yes_token_id: str,
        no_token_id: str,
        end_time: datetime,
        window_minutes: int = 10,
    ) -> int:
        """
        Fetch price history around market expiry and store it.

        Args:
            condition_id: Market condition ID
            yes_token_id: YES token ID
            no_token_id: NO token ID
            end_time: Market end time
            window_minutes: Minutes before expiry to fetch

        Returns:
            Number of price points stored
        """
        # Calculate time range
        end_ts = int(end_time.timestamp())
        start_ts = end_ts - (window_minutes * 60)

        # Fetch YES and NO price history
        yes_history = await self.client.get_price_history(
            yes_token_id, start_ts=start_ts, end_ts=end_ts, fidelity=60
        )
        no_history = await self.client.get_price_history(
            no_token_id, start_ts=start_ts, end_ts=end_ts, fidelity=60
        )

        if not yes_history and not no_history:
            return 0

        # Merge histories by timestamp
        yes_by_ts = {p["t"]: Decimal(str(p["p"])) for p in yes_history}
        no_by_ts = {p["t"]: Decimal(str(p["p"])) for p in no_history}

        all_timestamps = sorted(set(yes_by_ts.keys()) | set(no_by_ts.keys()))

        stored_count = 0
        async with self.db.session() as session:
            for ts in all_timestamps:
                yes_price = yes_by_ts.get(ts)
                no_price = no_by_ts.get(ts)

                # Skip if missing either price
                if yes_price is None or no_price is None:
                    continue

                try:
                    await self._upsert_price_history(
                        session,
                        condition_id=condition_id,
                        yes_token_id=yes_token_id,
                        no_token_id=no_token_id,
                        yes_price=yes_price,
                        no_price=no_price,
                        timestamp=datetime.fromtimestamp(ts, tz=UTC),
                    )
                    stored_count += 1
                except Exception as e:
                    logger.debug(f"Failed to store price history: {e}")

        return stored_count

    async def _upsert_price_history(
        self,
        session: AsyncSession,
        condition_id: str,
        yes_token_id: str,
        no_token_id: str,
        yes_price: Decimal,
        no_price: Decimal,
        timestamp: datetime,
    ) -> None:
        """Upsert a price history point."""
        await session.execute(
            text("""
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
            """),
            {
                "condition_id": condition_id,
                "yes_token_id": yes_token_id,
                "no_token_id": no_token_id,
                "yes_price": float(yes_price),
                "no_price": float(no_price),
                "timestamp": timestamp,
            },
        )

    async def fetch_all_price_histories(
        self,
        window_minutes: int = 10,
        concurrency: int = 5,
    ) -> int:
        """
        Fetch price history for all stored resolutions.

        Args:
            window_minutes: Minutes before expiry to fetch
            concurrency: Number of concurrent API requests

        Returns:
            Total number of price points stored
        """
        await self.client.connect()

        try:
            # Get all resolutions from database
            async with self.db.session() as session:
                result = await session.execute(
                    text("""
                        SELECT condition_id, yes_token_id, no_token_id, end_time
                        FROM market_resolutions
                        ORDER BY end_time DESC
                    """)
                )
                resolutions = result.fetchall()

            logger.info(f"Fetching price history for {len(resolutions)} markets...")

            total_stored = 0
            semaphore = asyncio.Semaphore(concurrency)

            async def fetch_one(row) -> int:
                async with semaphore:
                    try:
                        count = await self.fetch_and_store_price_history(
                            condition_id=row.condition_id,
                            yes_token_id=row.yes_token_id,
                            no_token_id=row.no_token_id,
                            end_time=row.end_time,
                            window_minutes=window_minutes,
                        )
                        return count
                    except Exception as e:
                        logger.error(f"Failed to fetch history for {row.condition_id}: {e}")
                        return 0

            tasks = [fetch_one(row) for row in resolutions]
            results = await asyncio.gather(*tasks)
            total_stored = sum(results)

            logger.info(f"Stored {total_stored} total price history points")
            return total_stored

        finally:
            await self.client.disconnect()

    async def load_resolutions(
        self,
        assets: list[str] | None = None,
        timeframe: str | None = None,
    ) -> list[MarketResolution]:
        """
        Load market resolutions from database.

        Args:
            assets: Filter by assets
            timeframe: Filter by timeframe

        Returns:
            List of MarketResolution objects
        """
        async with self.db.session() as session:
            query = """
                SELECT condition_id, market_type, asset, timeframe, name,
                       yes_token_id, no_token_id, winning_side, end_time,
                       resolved_at, final_yes_price, final_no_price, raw_data
                FROM market_resolutions
                WHERE 1=1
            """
            params: dict = {}

            if assets:
                query += " AND asset = ANY(:assets)"
                params["assets"] = assets

            if timeframe:
                query += " AND timeframe = :timeframe"
                params["timeframe"] = timeframe

            query += " ORDER BY end_time DESC"

            result = await session.execute(text(query), params)
            rows = result.fetchall()

        resolutions = []
        for row in rows:
            resolutions.append(
                MarketResolution(
                    condition_id=row.condition_id,
                    market_type=row.market_type,
                    asset=row.asset,
                    timeframe=row.timeframe,
                    name=row.name,
                    yes_token_id=row.yes_token_id,
                    no_token_id=row.no_token_id,
                    winning_side=TradeSide(row.winning_side),
                    end_time=row.end_time,
                    resolved_at=row.resolved_at,
                    final_yes_price=Decimal(str(row.final_yes_price))
                    if row.final_yes_price
                    else None,
                    final_no_price=Decimal(str(row.final_no_price)) if row.final_no_price else None,
                    raw_data=row.raw_data,
                )
            )

        return resolutions

    async def load_price_history(
        self,
        condition_id: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[PriceSnapshot]:
        """
        Load price history from database.

        Args:
            condition_id: Market condition ID
            start_time: Filter by start time
            end_time: Filter by end time

        Returns:
            List of PriceSnapshot objects
        """
        async with self.db.session() as session:
            query = """
                SELECT condition_id, yes_token_id, no_token_id,
                       yes_price, no_price, timestamp
                FROM price_history
                WHERE condition_id = :condition_id
            """
            params: dict = {"condition_id": condition_id}

            if start_time:
                query += " AND timestamp >= :start_time"
                params["start_time"] = start_time

            if end_time:
                query += " AND timestamp <= :end_time"
                params["end_time"] = end_time

            query += " ORDER BY timestamp ASC"

            result = await session.execute(text(query), params)
            rows = result.fetchall()

        snapshots = []
        for row in rows:
            snapshots.append(
                PriceSnapshot(
                    condition_id=row.condition_id,
                    yes_token_id=row.yes_token_id,
                    no_token_id=row.no_token_id,
                    yes_price=Decimal(str(row.yes_price)),
                    no_price=Decimal(str(row.no_price)),
                    timestamp=row.timestamp,
                )
            )

        return snapshots

    async def get_data_stats(self) -> dict:
        """Get statistics about stored data."""
        async with self.db.session() as session:
            # Count resolutions
            res_result = await session.execute(
                text("SELECT COUNT(*), MIN(end_time), MAX(end_time) FROM market_resolutions")
            )
            res_row = res_result.fetchone()

            # Count price history
            hist_result = await session.execute(
                text("SELECT COUNT(*), COUNT(DISTINCT condition_id) FROM price_history")
            )
            hist_row = hist_result.fetchone()

            # Breakdown by asset
            asset_result = await session.execute(
                text("""
                    SELECT asset, COUNT(*),
                           SUM(CASE WHEN winning_side = 'yes' THEN 1 ELSE 0 END) as yes_wins,
                           SUM(CASE WHEN winning_side = 'no' THEN 1 ELSE 0 END) as no_wins
                    FROM market_resolutions
                    GROUP BY asset
                """)
            )
            asset_rows = asset_result.fetchall()

        return {
            "resolutions": {
                "total": res_row[0] if res_row else 0,
                "earliest": res_row[1] if res_row else None,
                "latest": res_row[2] if res_row else None,
            },
            "price_history": {
                "total_points": hist_row[0] if hist_row else 0,
                "markets_with_history": hist_row[1] if hist_row else 0,
            },
            "by_asset": {
                row[0]: {
                    "total": row[1],
                    "yes_wins": row[2],
                    "no_wins": row[3],
                }
                for row in asset_rows
            },
        }
