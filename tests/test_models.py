"""Tests for data models."""

from decimal import Decimal

import pytest

from pylo.models.market import Market, MarketOutcome, Platform
from pylo.models.opportunity import (
    ArbitrageType,
    CrossPlatformArbOpportunity,
    InternalArbOpportunity,
)


class TestMarket:
    """Tests for the Market model."""

    def test_yes_price(self, sample_market: Market) -> None:
        """Test YES price extraction."""
        assert sample_market.yes_price == Decimal("0.45")

    def test_no_price(self, sample_market: Market) -> None:
        """Test NO price extraction."""
        assert sample_market.no_price == Decimal("0.52")

    def test_spread_calculation(self, sample_market: Market) -> None:
        """Test spread calculation."""
        assert sample_market.spread == Decimal("0.97")

    def test_is_arbitrageable_true(self, arbitrage_market: Market) -> None:
        """Test arbitrage detection when opportunity exists."""
        assert arbitrage_market.is_arbitrageable is True

    def test_is_arbitrageable_false(self, no_arbitrage_market: Market) -> None:
        """Test arbitrage detection when no opportunity."""
        assert no_arbitrage_market.is_arbitrageable is False

    def test_empty_outcomes(self) -> None:
        """Test market with no outcomes."""
        market = Market(
            id="empty",
            platform=Platform.POLYMARKET,
            name="Empty Market",
        )
        assert market.yes_price is None
        assert market.no_price is None
        assert market.spread is None


class TestInternalArbOpportunity:
    """Tests for internal arbitrage opportunity model."""

    def test_from_market_with_arbitrage(self, arbitrage_market: Market) -> None:
        """Test creating opportunity from arbitrageable market."""
        opp = InternalArbOpportunity.from_market(arbitrage_market)

        assert opp is not None
        assert opp.type == ArbitrageType.INTERNAL
        assert opp.profit_percentage == Decimal("0.05")  # 5%
        assert opp.yes_price == Decimal("0.45")
        assert opp.no_price == Decimal("0.50")
        assert opp.total_cost == Decimal("0.95")

    def test_from_market_without_arbitrage(self, no_arbitrage_market: Market) -> None:
        """Test that no opportunity is created when none exists."""
        opp = InternalArbOpportunity.from_market(no_arbitrage_market)
        assert opp is None

    def test_instructions_generated(self, arbitrage_market: Market) -> None:
        """Test that instructions are generated."""
        opp = InternalArbOpportunity.from_market(arbitrage_market)

        assert opp is not None
        assert len(opp.instructions) > 0
        assert any("YES" in instr for instr in opp.instructions)
        assert any("NO" in instr for instr in opp.instructions)


class TestCrossPlatformArbOpportunity:
    """Tests for cross-platform arbitrage opportunity model."""

    def test_from_markets_with_arbitrage(
        self,
        polymarket_btc_market: Market,
        kalshi_market: Market,
    ) -> None:
        """Test creating cross-platform opportunity."""
        # YES on Polymarket (0.45) + NO on Kalshi (0.58) = 1.03 (no arb)
        # But let's adjust Kalshi NO to create an opportunity
        kalshi_market.outcomes[1].price = Decimal("0.50")

        opp = CrossPlatformArbOpportunity.from_markets(
            market_a=polymarket_btc_market,  # Buy YES at 0.45
            market_b=kalshi_market,  # Buy NO at 0.50
            event_name="BTC $100k",
        )

        assert opp is not None
        assert opp.type == ArbitrageType.CROSS_PLATFORM
        assert opp.profit_percentage == Decimal("0.05")  # 0.45 + 0.50 = 0.95
        assert opp.platform_a == Platform.POLYMARKET
        assert opp.platform_b == Platform.KALSHI

    def test_from_markets_without_arbitrage(
        self,
        polymarket_btc_market: Market,
        kalshi_market: Market,
    ) -> None:
        """Test no opportunity when prices don't allow arbitrage."""
        opp = CrossPlatformArbOpportunity.from_markets(
            market_a=polymarket_btc_market,
            market_b=kalshi_market,
            event_name="BTC $100k",
        )

        assert opp is None  # 0.45 + 0.58 = 1.03 > 1.00
