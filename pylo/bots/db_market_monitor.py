"""Database-backed market monitor for reading markets and orderbooks from PostgreSQL.

This module replaces direct Polymarket API calls with PostgreSQL database reads.
Markets and orderbooks are populated by the Rust services (market-scanner, orderbook-stream).
"""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional
from uuid import UUID

from pylo.bots.models import Asset, MarketType, Timeframe, UpDownMarket
from pylo.config.settings import get_settings
from pylo.db.connection import Database
from pylo.db.models import Market, OrderbookSnapshot
from pylo.db.queries import (
    get_active_markets,
    get_latest_orderbook,
    get_market_by_condition_id,
    get_markets_with_fresh_orderbooks,
    get_markets_with_latest_orderbooks,
)

logger = logging.getLogger(__name__)


class DBMarketMonitor:
    """Database-backed market monitor.

    Reads markets and orderbooks from PostgreSQL instead of calling Polymarket APIs.
    This enables the polyglot architecture where:
    - Rust services write market/orderbook data to PostgreSQL
    - Python trade executor reads from PostgreSQL
    """

    def __init__(self, database: Database) -> None:
        """Initialize with database connection.

        Args:
            database: Database instance with connection pool.
        """
        self._db = database
        self._settings = get_settings()
        self._markets: dict[str, UpDownMarket] = {}
        self._market_id_map: dict[str, UUID] = {}  # condition_id -> db UUID

    async def discover_markets(self, force_refresh: bool = False) -> list[UpDownMarket]:
        """Discover markets with fresh orderbook data and prices pre-loaded.

        Single optimized query returns markets + orderbook prices together.
        Only returns markets with orderbook data < 120 seconds old.

        Args:
            force_refresh: If True, clears cache and fetches fresh data.

        Returns:
            List of UpDownMarket objects with prices already applied.
        """
        if force_refresh:
            self._markets.clear()
            self._market_id_map.clear()

        max_expiry = self._settings.spread_bot_max_time_to_expiry
        assets = self._settings.get_spread_bot_assets()

        all_markets: list[UpDownMarket] = []

        async with self._db.session() as session:
            # Single query returns markets WITH prices - no second query needed
            max_orderbook_age = getattr(self._settings, 'max_orderbook_age_seconds', 30)
            db_results = await get_markets_with_latest_orderbooks(
                session, max_orderbook_age_seconds=max_orderbook_age
            )

            for db_market, orderbook in db_results:
                # Filter by configured assets
                if db_market.asset.upper() not in [a.upper() for a in assets]:
                    continue

                # Convert to bot model
                bot_market = self._convert_db_market(db_market)

                # Skip expired markets
                if bot_market.is_expired:
                    continue

                # Skip markets too far in the future
                if bot_market.time_to_expiry > max_expiry:
                    continue

                # Apply prices from orderbook
                if orderbook:
                    self._apply_orderbook_snapshot(bot_market, orderbook)

                # Cache the mapping from condition_id to DB UUID
                self._market_id_map[bot_market.id] = db_market.id
                self._markets[bot_market.id] = bot_market
                all_markets.append(bot_market)

        logger.info(f"Discovered {len(all_markets)} active markets from database")
        return all_markets

    async def update_prices(self, market: UpDownMarket) -> bool:
        """Update market prices from the latest orderbook snapshot.

        Args:
            market: The UpDownMarket to update.

        Returns:
            True if prices were updated, False otherwise.
        """
        # Get the DB UUID for this market
        db_uuid = self._market_id_map.get(market.id)
        if not db_uuid:
            # Try to find by condition_id
            async with self._db.session() as session:
                db_market = await get_market_by_condition_id(session, market.condition_id)
                if db_market:
                    db_uuid = db_market.id
                    self._market_id_map[market.id] = db_uuid
                else:
                    logger.debug(f"Market not found in DB: {market.id}")
                    return False

        # Fetch latest orderbook snapshot
        async with self._db.session() as session:
            snapshot = await get_latest_orderbook(session, db_uuid)

            if snapshot:
                self._apply_orderbook_snapshot(market, snapshot)
                return True

        return False

    async def get_markets_with_orderbooks(
        self,
    ) -> list[tuple[UpDownMarket, OrderbookSnapshot | None]]:
        """Get all active markets with their latest orderbook snapshots.

        Returns:
            List of (UpDownMarket, OrderbookSnapshot | None) tuples.
        """
        results: list[tuple[UpDownMarket, OrderbookSnapshot | None]] = []

        async with self._db.session() as session:
            db_results = await get_markets_with_latest_orderbooks(session)

            for db_market, orderbook in db_results:
                bot_market = self._convert_db_market(db_market)

                if orderbook:
                    self._apply_orderbook_snapshot(bot_market, orderbook)

                # Cache mapping
                self._market_id_map[bot_market.id] = db_market.id
                self._markets[bot_market.id] = bot_market

                results.append((bot_market, orderbook))

        return results

    async def update_all_prices(
        self, markets: dict[str, UpDownMarket]
    ) -> int:
        """Update prices for all markets in a single batch query.

        This is much faster than calling update_prices() for each market
        individually (1 query vs N queries).

        Args:
            markets: Dictionary of market_id -> UpDownMarket to update.

        Returns:
            Number of markets with updated prices.
        """
        updated = 0
        max_expiry = self._settings.spread_bot_max_time_to_expiry
        assets = self._settings.get_spread_bot_assets()

        async with self._db.session() as session:
            db_results = await get_markets_with_latest_orderbooks(session)

            for db_market, orderbook in db_results:
                # Filter by configured assets
                if db_market.asset.upper() not in [a.upper() for a in assets]:
                    continue

                condition_id = db_market.condition_id
                if condition_id in markets:
                    market = markets[condition_id]
                    if orderbook:
                        self._apply_orderbook_snapshot(market, orderbook)
                        updated += 1
                    # Update cache mapping
                    self._market_id_map[condition_id] = db_market.id

        logger.debug(f"Updated prices for {updated} markets in single query")
        return updated

    def _convert_db_market(self, db_market: Market) -> UpDownMarket:
        """Convert a database Market to a bot UpDownMarket.

        Args:
            db_market: Database Market model.

        Returns:
            Bot UpDownMarket model.
        """
        return UpDownMarket(
            id=db_market.condition_id,
            name=db_market.name,
            asset=self._parse_asset(db_market.asset),
            timeframe=self._parse_timeframe(db_market.timeframe),
            end_time=db_market.end_time,
            yes_token_id=db_market.yes_token_id,
            no_token_id=db_market.no_token_id,
            condition_id=db_market.condition_id,
            market_type=self._parse_market_type(db_market.market_type),
            fetched_at=db_market.updated_at,
        )

    def _apply_orderbook_snapshot(
        self, market: UpDownMarket, snapshot: OrderbookSnapshot
    ) -> None:
        """Apply orderbook snapshot prices to a market.

        Args:
            market: The UpDownMarket to update.
            snapshot: The OrderbookSnapshot with price data.
        """
        if snapshot.yes_best_ask is not None:
            market.yes_ask = snapshot.yes_best_ask
        if snapshot.yes_best_bid is not None:
            market.yes_bid = snapshot.yes_best_bid
        if snapshot.no_best_ask is not None:
            market.no_ask = snapshot.no_best_ask
        if snapshot.no_best_bid is not None:
            market.no_bid = snapshot.no_best_bid
        market.fetched_at = snapshot.captured_at

    def _parse_market_type(self, db_type: str) -> MarketType:
        """Parse market type string to enum.

        Args:
            db_type: Database market type string.

        Returns:
            MarketType enum value.
        """
        mapping = {
            "up_down": MarketType.UP_DOWN,
            "above": MarketType.ABOVE,
            "price_range": MarketType.PRICE_RANGE,
            "sports": MarketType.SPORTS,
            "binary": MarketType.BINARY,
        }
        return mapping.get(db_type.lower(), MarketType.UP_DOWN)

    def _parse_asset(self, db_asset: str) -> Asset:
        """Parse asset string to enum.

        Args:
            db_asset: Database asset string.

        Returns:
            Asset enum value.
        """
        mapping = {
            "BTC": Asset.BTC,
            "ETH": Asset.ETH,
            "SOL": Asset.SOL,
            "XRP": Asset.XRP,
            "DOGE": Asset.OTHER,  # Map additional cryptos to OTHER
            "ADA": Asset.OTHER,
            "AVAX": Asset.OTHER,
            "MATIC": Asset.OTHER,
            "DOT": Asset.OTHER,
            "LINK": Asset.OTHER,
            "SPORTS": Asset.SPORTS,
            "OTHER": Asset.OTHER,
            "UNKNOWN": Asset.OTHER,
        }
        return mapping.get(db_asset.upper(), Asset.OTHER)

    def _parse_timeframe(self, db_timeframe: str) -> Timeframe:
        """Parse timeframe string to enum.

        Args:
            db_timeframe: Database timeframe string.

        Returns:
            Timeframe enum value.
        """
        mapping = {
            "5m": Timeframe.FIVE_MIN,
            "5min": Timeframe.FIVE_MIN,
            "15m": Timeframe.FIFTEEN_MIN,
            "15min": Timeframe.FIFTEEN_MIN,
            "1h": Timeframe.HOURLY,
            "hourly": Timeframe.HOURLY,
            "4h": Timeframe.FOUR_HOUR,
            "daily": Timeframe.DAILY,
            "event": Timeframe.EVENT,
        }
        return mapping.get(db_timeframe.lower(), Timeframe.HOURLY)

    def get_active_markets(self) -> list[UpDownMarket]:
        """Get all cached active (non-expired) markets.

        Returns:
            List of non-expired UpDownMarket objects.
        """
        return [m for m in self._markets.values() if not m.is_expired]

    def get_market(self, market_id: str) -> Optional[UpDownMarket]:
        """Get a specific market by ID.

        Args:
            market_id: The market's condition_id.

        Returns:
            UpDownMarket if found, None otherwise.
        """
        return self._markets.get(market_id)

    def clear_cache(self) -> None:
        """Clear the market cache."""
        self._markets.clear()
        self._market_id_map.clear()
