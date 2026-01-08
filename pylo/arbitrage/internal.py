"""Internal arbitrage detection: YES + NO < $1 on same platform."""

import logging
from decimal import Decimal

from pylo.config.settings import get_settings
from pylo.models.market import Market
from pylo.models.opportunity import InternalArbOpportunity

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

        Internal arbitrage exists when: YES_ask + NO_ask < $1.00

        Args:
            markets: List of markets to scan

        Returns:
            List of detected opportunities
        """
        opportunities: list[InternalArbOpportunity] = []
        max_age = self.settings.max_price_age_seconds

        for market in markets:
            # Skip resolved markets
            if market.resolved:
                continue

            # Skip non-binary markets (multi-outcome markets need different logic)
            if not market.is_binary:
                self.logger.debug(f"Skipping non-binary market: {market.name}")
                continue

            # Skip markets expiring soon (too risky)
            if market.is_expiring_soon:
                self.logger.debug(f"Skipping expiring market: {market.name}")
                continue

            # Skip stale price data
            if market.is_stale(max_age):
                self.logger.debug(f"Skipping stale market: {market.name}")
                continue

            opportunity = self._check_market(market)
            if opportunity and opportunity.profit_after_fees >= self.min_profit:
                opportunities.append(opportunity)
                self.logger.info(
                    f"Found internal arb: {opportunity.profit_percentage:.2%} "
                    f"(after fees: {opportunity.profit_after_fees:.2%}) "
                    f"on '{market.name}'"
                )

        # Sort by profit after fees (highest first)
        opportunities.sort(key=lambda x: x.profit_after_fees, reverse=True)

        return opportunities

    def _check_market(self, market: Market) -> InternalArbOpportunity | None:
        """
        Check a single market for internal arbitrage.

        Uses ASK prices (what you pay to buy) for accurate profit calculation.

        Args:
            market: Market to check

        Returns:
            Opportunity if arbitrage exists, None otherwise
        """
        # Use ASK prices for buying (falls back to last price if no ask)
        yes_price = market.yes_ask_price
        no_price = market.no_ask_price

        if yes_price is None or no_price is None:
            return None

        # Reject zero or negative prices (bad data)
        if yes_price <= Decimal("0") or no_price <= Decimal("0"):
            return None

        # Check if arbitrage exists
        total_cost = yes_price + no_price

        if total_cost >= Decimal("1"):
            return None  # No arbitrage

        # Calculate gross profit
        gross_profit = Decimal("1") - total_cost

        # Calculate fees for this platform
        fee_rate = self.settings.get_fee_rate(market.platform.value)
        estimated_fees = total_cost * fee_rate
        profit_after_fees = gross_profit - estimated_fees

        # Skip if fees eat all profit
        if profit_after_fees <= Decimal("0"):
            self.logger.debug(
                f"Skipping {market.name}: profit {gross_profit:.2%} wiped by fees"
            )
            return None

        # Create opportunity
        return InternalArbOpportunity(
            id=f"internal_{market.platform.value}_{market.id}",
            market=market,
            markets=[market],
            platforms=[market.platform],
            yes_price=yes_price,
            no_price=no_price,
            total_cost=total_cost,
            profit_percentage=gross_profit,
            profit_after_fees=profit_after_fees,
            profit_absolute=profit_after_fees,  # Per $1 invested after fees
            estimated_fees=estimated_fees,
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
                f"6. Gross profit: ${gross_profit:.4f} ({gross_profit:.2%})",
                f"7. Est. fees: ${estimated_fees:.4f} ({fee_rate:.2%})",
                f"8. Net profit: ${profit_after_fees:.4f} ({profit_after_fees:.2%})",
            ],
            liquidity_available=market.liquidity,
            confidence=self._calculate_confidence(market, gross_profit),
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
