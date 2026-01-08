"""Tests for DB-backed market monitor (Task 11)."""

import pytest
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

from pylo.bots.db_market_monitor import DBMarketMonitor
from pylo.bots.models import Asset, MarketType, Timeframe, UpDownMarket
from pylo.db.connection import Database
from pylo.db.models import Market, OrderbookSnapshot


@pytest.fixture
async def db():
    """Get database connection."""
    import os
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL not set")

    db = Database(database_url)
    yield db
    await db.close()


@pytest.fixture
async def monitor(db):
    """Create a DBMarketMonitor instance with real DB."""
    return DBMarketMonitor(db)


@pytest.fixture
def mock_monitor():
    """Create a DBMarketMonitor instance with mock DB for unit tests."""
    mock_db = MagicMock(spec=Database)
    return DBMarketMonitor(mock_db)


class TestDBMarketMonitor:
    """Test suite for DBMarketMonitor - unit tests (no DB required)."""

    def test_convert_db_market_to_bot_market(self, mock_monitor):
        """Test conversion from DB Market model to bot UpDownMarket model."""
        # Create a mock DB market
        db_market = Market(
            id=uuid4(),
            condition_id="test_condition_123",
            market_type="up_down",
            asset="BTC",
            timeframe="hourly",
            yes_token_id="yes_token_123",
            no_token_id="no_token_456",
            name="Bitcoin Up or Down - 1PM ET",
            end_time=datetime.now(timezone.utc) + timedelta(hours=1),
            is_active=True,
            updated_at=datetime.now(timezone.utc),
        )

        # Convert to bot model
        bot_market = mock_monitor._convert_db_market(db_market)

        assert isinstance(bot_market, UpDownMarket)
        assert bot_market.id == db_market.condition_id
        assert bot_market.name == db_market.name
        assert bot_market.asset == Asset.BTC
        assert bot_market.timeframe == Timeframe.HOURLY
        assert bot_market.yes_token_id == db_market.yes_token_id
        assert bot_market.no_token_id == db_market.no_token_id
        assert bot_market.condition_id == db_market.condition_id
        assert bot_market.market_type == MarketType.UP_DOWN

    def test_apply_orderbook_snapshot(self, mock_monitor):
        """Test applying orderbook snapshot to update market prices."""
        # Create a bot market
        bot_market = UpDownMarket(
            id="test_market",
            name="Test Market",
            asset=Asset.BTC,
            timeframe=Timeframe.HOURLY,
            end_time=datetime.now(timezone.utc) + timedelta(hours=1),
            yes_token_id="yes_123",
            no_token_id="no_456",
            condition_id="test_market",
        )

        # Create a mock orderbook snapshot
        snapshot = OrderbookSnapshot(
            id=1,
            market_id=uuid4(),
            yes_best_ask=Decimal("0.48"),
            yes_best_bid=Decimal("0.47"),
            no_best_ask=Decimal("0.51"),
            no_best_bid=Decimal("0.50"),
            captured_at=datetime.now(timezone.utc),
        )

        # Apply snapshot
        mock_monitor._apply_orderbook_snapshot(bot_market, snapshot)

        assert bot_market.yes_ask == Decimal("0.48")
        assert bot_market.yes_bid == Decimal("0.47")
        assert bot_market.no_ask == Decimal("0.51")
        assert bot_market.no_bid == Decimal("0.50")

    def test_market_type_mapping(self, mock_monitor):
        """Test market type string to enum mapping."""
        test_cases = [
            ("up_down", MarketType.UP_DOWN),
            ("above", MarketType.ABOVE),
            ("price_range", MarketType.PRICE_RANGE),
            ("sports", MarketType.SPORTS),
            ("unknown", MarketType.UP_DOWN),  # Default fallback
        ]

        for db_type, expected in test_cases:
            result = mock_monitor._parse_market_type(db_type)
            assert result == expected, f"Expected {expected} for '{db_type}', got {result}"

    def test_asset_mapping(self, mock_monitor):
        """Test asset string to enum mapping."""
        test_cases = [
            ("BTC", Asset.BTC),
            ("ETH", Asset.ETH),
            ("SOL", Asset.SOL),
            ("XRP", Asset.XRP),
            ("SPORTS", Asset.SPORTS),
            ("OTHER", Asset.OTHER),
            ("unknown", Asset.OTHER),  # Default fallback
        ]

        for db_asset, expected in test_cases:
            result = mock_monitor._parse_asset(db_asset)
            assert result == expected, f"Expected {expected} for '{db_asset}', got {result}"

    def test_timeframe_mapping(self, mock_monitor):
        """Test timeframe string to enum mapping."""
        test_cases = [
            ("15min", Timeframe.FIFTEEN_MIN),
            ("hourly", Timeframe.HOURLY),
            ("daily", Timeframe.DAILY),
            ("event", Timeframe.EVENT),
            ("unknown", Timeframe.HOURLY),  # Default fallback
        ]

        for db_timeframe, expected in test_cases:
            result = mock_monitor._parse_timeframe(db_timeframe)
            assert result == expected, f"Expected {expected} for '{db_timeframe}', got {result}"

    def test_get_active_markets_filters_expired(self, mock_monitor):
        """Test that get_active_markets filters out expired markets."""
        # Add some markets to the cache
        mock_monitor._markets = {
            "active1": UpDownMarket(
                id="active1",
                name="Active Market",
                asset=Asset.BTC,
                timeframe=Timeframe.HOURLY,
                end_time=datetime.now(timezone.utc) + timedelta(hours=1),
                yes_token_id="yes",
                no_token_id="no",
                condition_id="active1",
            ),
            "expired1": UpDownMarket(
                id="expired1",
                name="Expired Market",
                asset=Asset.BTC,
                timeframe=Timeframe.HOURLY,
                end_time=datetime.now(timezone.utc) - timedelta(hours=1),  # Expired
                yes_token_id="yes",
                no_token_id="no",
                condition_id="expired1",
            ),
        }

        active = mock_monitor.get_active_markets()
        assert len(active) == 1
        assert active[0].id == "active1"

    def test_get_market_by_id(self, mock_monitor):
        """Test getting a specific market by ID."""
        market = UpDownMarket(
            id="test_id",
            name="Test Market",
            asset=Asset.BTC,
            timeframe=Timeframe.HOURLY,
            end_time=datetime.now(timezone.utc) + timedelta(hours=1),
            yes_token_id="yes",
            no_token_id="no",
            condition_id="test_id",
        )
        mock_monitor._markets["test_id"] = market

        result = mock_monitor.get_market("test_id")
        assert result == market

        result = mock_monitor.get_market("nonexistent")
        assert result is None


@pytest.mark.skipif(
    not __import__("os").environ.get("DATABASE_URL"),
    reason="DATABASE_URL not set"
)
class TestDBMarketMonitorIntegration:
    """Integration tests requiring database."""

    async def test_discover_markets_returns_list(self, monitor):
        """Test that discover_markets returns a list of UpDownMarket objects."""
        markets = await monitor.discover_markets()

        assert isinstance(markets, list)
        # All items should be UpDownMarket instances
        for market in markets:
            assert isinstance(market, UpDownMarket)

    async def test_full_flow_discover_and_update(self, monitor):
        """Test full flow: discover markets then update prices."""
        # Discover markets from DB
        markets = await monitor.discover_markets()

        # Update prices for each market
        for market in markets[:5]:  # Limit to 5 for speed
            await monitor.update_prices(market)

        # No assertion - just verify no exceptions

    async def test_get_markets_with_orderbooks(self, monitor):
        """Test getting markets with their latest orderbook data."""
        markets_with_orderbooks = await monitor.get_markets_with_orderbooks()

        assert isinstance(markets_with_orderbooks, list)
        for market, orderbook in markets_with_orderbooks:
            assert isinstance(market, UpDownMarket)
            # Orderbook may be None if no snapshot exists
            assert orderbook is None or isinstance(orderbook, OrderbookSnapshot)
