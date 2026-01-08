"""Tests for arbitrage detection."""

from decimal import Decimal

import pytest

from pylo.arbitrage.internal import InternalArbDetector
from pylo.models.market import Market, MarketOutcome, Platform


class TestInternalArbDetector:
    """Tests for internal arbitrage detection."""

    @pytest.fixture
    def detector(self) -> InternalArbDetector:
        """Create a detector instance."""
        return InternalArbDetector()

    @pytest.fixture
    def markets_with_arb(self) -> list[Market]:
        """Create markets with arbitrage opportunities."""
        return [
            Market(
                id="arb_1",
                platform=Platform.POLYMARKET,
                name="Arb Market 1",
                outcomes=[
                    MarketOutcome(id="yes", name="YES", price=Decimal("0.45")),
                    MarketOutcome(id="no", name="NO", price=Decimal("0.50")),
                ],
                liquidity=Decimal("10000"),
            ),
            Market(
                id="arb_2",
                platform=Platform.POLYMARKET,
                name="Arb Market 2",
                outcomes=[
                    MarketOutcome(id="yes", name="YES", price=Decimal("0.30")),
                    MarketOutcome(id="no", name="NO", price=Decimal("0.65")),
                ],
                liquidity=Decimal("5000"),
            ),
            Market(
                id="no_arb",
                platform=Platform.POLYMARKET,
                name="No Arb Market",
                outcomes=[
                    MarketOutcome(id="yes", name="YES", price=Decimal("0.50")),
                    MarketOutcome(id="no", name="NO", price=Decimal("0.52")),
                ],
                liquidity=Decimal("8000"),
            ),
        ]

    @pytest.mark.asyncio
    async def test_scan_finds_opportunities(
        self,
        detector: InternalArbDetector,
        markets_with_arb: list[Market],
    ) -> None:
        """Test that scanner finds arbitrage opportunities."""
        opportunities = await detector.scan(markets_with_arb)

        # Should find 2 opportunities (arb_1 and arb_2)
        assert len(opportunities) == 2

        # Should be sorted by profit (highest first)
        assert opportunities[0].profit_percentage >= opportunities[1].profit_percentage

    @pytest.mark.asyncio
    async def test_scan_skips_resolved_markets(
        self,
        detector: InternalArbDetector,
    ) -> None:
        """Test that resolved markets are skipped."""
        markets = [
            Market(
                id="resolved",
                platform=Platform.POLYMARKET,
                name="Resolved Market",
                outcomes=[
                    MarketOutcome(id="yes", name="YES", price=Decimal("0.40")),
                    MarketOutcome(id="no", name="NO", price=Decimal("0.50")),
                ],
                resolved=True,  # This market is resolved
            ),
        ]

        opportunities = await detector.scan(markets)
        assert len(opportunities) == 0

    @pytest.mark.asyncio
    async def test_scan_respects_minimum_profit(
        self,
        detector: InternalArbDetector,
    ) -> None:
        """Test that opportunities below threshold are filtered."""
        # Set a high minimum threshold
        detector.min_profit = Decimal("0.10")  # 10%

        markets = [
            Market(
                id="small_arb",
                platform=Platform.POLYMARKET,
                name="Small Arb Market",
                outcomes=[
                    MarketOutcome(id="yes", name="YES", price=Decimal("0.48")),
                    MarketOutcome(id="no", name="NO", price=Decimal("0.50")),
                ],  # 2% profit - below threshold
            ),
        ]

        opportunities = await detector.scan(markets)
        assert len(opportunities) == 0

    @pytest.mark.asyncio
    async def test_scan_empty_list(
        self,
        detector: InternalArbDetector,
    ) -> None:
        """Test scanning empty market list."""
        opportunities = await detector.scan([])
        assert len(opportunities) == 0
