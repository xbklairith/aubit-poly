"""Internal arbitrage detection: YES + NO < $1 on same platform."""

import logging
from decimal import Decimal

from src.config.settings import get_settings
from src.models.market import Market
from src.models.opportunity import InternalArbOpportunity

logger = logging.getLogger(__name__)


class InternalArbDetector:
    """Detect internal arbitrage opportunities within single markets."""

    def __init__(self) -> None:
        """Initialize the detector."""
        self.settings = get_settings()
        self.min_profit = self.settings.min_internal_arb_profit
        self.logger = logging.getLogger(__name__)

    async def scan(self, markets: list[Market]) -> list[InternalArbOpportunity]:
        """
        Scan markets for internal arbitrage opportunities.

        Internal arbitrage exists when: YES_price + NO_price < $1.00

        Args:
            markets: List of markets to scan

        Returns:
            List of detected opportunities
        """
        opportunities: list[InternalArbOpportunity] = []

        for market in markets:
            # Skip resolved markets
            if market.resolved:
                continue

            opportunity = self._check_market(market)
            if opportunity and opportunity.profit_percentage >= self.min_profit:
                opportunities.append(opportunity)
                self.logger.info(
                    f"Found internal arb: {opportunity.profit_percentage:.2%} "
                    f"on '{market.name}'"
                )

        # Sort by profit (highest first)
        opportunities.sort(key=lambda x: x.profit_percentage, reverse=True)

        return opportunities

    def _check_market(self, market: Market) -> InternalArbOpportunity | None:
        """
        Check a single market for internal arbitrage.

        Args:
            market: Market to check

        Returns:
            Opportunity if arbitrage exists, None otherwise
        """
        # Get YES and NO prices
        yes_price = market.yes_price
        no_price = market.no_price

        if yes_price is None or no_price is None:
            return None

        # Check if arbitrage exists
        total_cost = yes_price + no_price

        if total_cost >= Decimal("1"):
            return None  # No arbitrage

        # Calculate profit
        profit = Decimal("1") - total_cost

        # Create opportunity
        return InternalArbOpportunity(
            id=f"internal_{market.platform.value}_{market.id}",
            market=market,
            markets=[market],
            platforms=[market.platform],
            yes_price=yes_price,
            no_price=no_price,
            total_cost=total_cost,
            profit_percentage=profit,
            profit_absolute=profit,  # Per $1 invested
            description=(
                f"Buy YES@{yes_price:.4f} + NO@{no_price:.4f} = "
                f"{total_cost:.4f} on {market.name}"
            ),
            instructions=[
                f"1. Go to: {market.url}",
                f"2. Buy YES shares at ${yes_price:.4f}",
                f"3. Buy NO shares at ${no_price:.4f}",
                f"4. Total cost: ${total_cost:.4f} per share pair",
                "5. Guaranteed return: $1.00 per share pair",
                f"6. Profit: ${profit:.4f} ({profit:.2%})",
            ],
            liquidity_available=market.liquidity,
            confidence=self._calculate_confidence(market, profit),
        )

    def _calculate_confidence(self, market: Market, profit: Decimal) -> Decimal:
        """
        Calculate confidence score for an opportunity.

        Factors:
        - Liquidity (higher = better)
        - Profit margin (too high might be data error)
        - Market activity

        Args:
            market: The market
            profit: Calculated profit

        Returns:
            Confidence score 0-1
        """
        confidence = Decimal("0.8")  # Base confidence

        # Very high profit might indicate stale data
        if profit > Decimal("0.05"):  # >5% is suspicious
            confidence -= Decimal("0.2")

        # Low liquidity reduces confidence
        if market.liquidity < Decimal("1000"):
            confidence -= Decimal("0.1")

        # Very low volume markets are risky
        if market.volume_24h < Decimal("100"):
            confidence -= Decimal("0.1")

        return max(Decimal("0.1"), min(Decimal("1"), confidence))


async def find_internal_arbitrage(markets: list[Market]) -> list[InternalArbOpportunity]:
    """
    Convenience function to find internal arbitrage.

    Args:
        markets: List of markets to scan

    Returns:
        List of opportunities
    """
    detector = InternalArbDetector()
    return await detector.scan(markets)
