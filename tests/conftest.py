"""Pytest configuration and fixtures."""

from decimal import Decimal

import pytest

from src.models.market import Market, MarketOutcome, Platform


@pytest.fixture
def sample_market() -> Market:
    """Create a sample market for testing."""
    return Market(
        id="test_market_1",
        platform=Platform.POLYMARKET,
        name="Test Market: Will X happen?",
        description="A test market for unit testing",
        outcomes=[
            MarketOutcome(id="yes", name="YES", price=Decimal("0.45")),
            MarketOutcome(id="no", name="NO", price=Decimal("0.52")),
        ],
        liquidity=Decimal("10000"),
        volume_24h=Decimal("5000"),
        url="https://polymarket.com/test",
    )


@pytest.fixture
def arbitrage_market() -> Market:
    """Create a market with internal arbitrage opportunity."""
    return Market(
        id="arb_market_1",
        platform=Platform.POLYMARKET,
        name="Arbitrage Test Market",
        outcomes=[
            MarketOutcome(id="yes", name="YES", price=Decimal("0.45")),
            MarketOutcome(id="no", name="NO", price=Decimal("0.50")),  # Total = 0.95
        ],
        liquidity=Decimal("10000"),
        volume_24h=Decimal("5000"),
    )


@pytest.fixture
def no_arbitrage_market() -> Market:
    """Create a market without arbitrage opportunity."""
    return Market(
        id="no_arb_market_1",
        platform=Platform.POLYMARKET,
        name="No Arbitrage Test Market",
        outcomes=[
            MarketOutcome(id="yes", name="YES", price=Decimal("0.50")),
            MarketOutcome(id="no", name="NO", price=Decimal("0.52")),  # Total = 1.02
        ],
        liquidity=Decimal("10000"),
        volume_24h=Decimal("5000"),
    )


@pytest.fixture
def kalshi_market() -> Market:
    """Create a sample Kalshi market."""
    return Market(
        id="BTCUSD-100K",
        platform=Platform.KALSHI,
        name="Will BTC hit $100k?",
        outcomes=[
            MarketOutcome(id="yes", name="YES", price=Decimal("0.40")),
            MarketOutcome(id="no", name="NO", price=Decimal("0.58")),
        ],
        liquidity=Decimal("5000"),
        volume_24h=Decimal("2000"),
    )


@pytest.fixture
def polymarket_btc_market() -> Market:
    """Create a Polymarket BTC market for cross-platform testing."""
    return Market(
        id="btc-100k-test",
        platform=Platform.POLYMARKET,
        name="Will BTC hit $100k?",
        outcomes=[
            MarketOutcome(id="yes", name="YES", price=Decimal("0.45")),
            MarketOutcome(id="no", name="NO", price=Decimal("0.53")),
        ],
        liquidity=Decimal("15000"),
        volume_24h=Decimal("8000"),
    )
