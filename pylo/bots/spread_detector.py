"""Spread detector for finding arbitrage opportunities."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pylo.bots.models import SpreadOpportunity, UpDownMarket
from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)


class SpreadDetector:
    """Detects spread arbitrage opportunities in Up/Down markets."""

    def __init__(self) -> None:
        self.settings = get_settings()
        self.min_profit = self.settings.spread_bot_min_profit

    def check_opportunity(self, market: UpDownMarket) -> Optional[SpreadOpportunity]:
        """Check if a market has an arbitrage opportunity.

        An opportunity exists when:
        - YES_ask + NO_ask < $1.00 (can buy both for less than guaranteed payout)
        - Profit percentage >= minimum threshold

        Args:
            market: The Up/Down market to check

        Returns:
            SpreadOpportunity if found, None otherwise
        """
        # Skip expired markets
        if market.is_expired:
            return None

        # Skip markets with invalid prices
        if market.yes_ask <= 0 or market.no_ask <= 0:
            return None

        # Calculate spread
        spread = market.spread
        profit_pct = Decimal("1.00") - spread

        # Check if profitable
        if profit_pct < self.min_profit:
            return None

        # Check if prices are stale
        if market.fetched_at:
            age = (datetime.now(timezone.utc) - market.fetched_at).total_seconds()
            if age > self.settings.max_price_age_seconds:
                logger.debug(f"Skipping {market.name}: prices are {age:.0f}s old")
                return None

        # Create opportunity
        opportunity = SpreadOpportunity(
            market=market,
            yes_price=market.yes_ask,
            no_price=market.no_ask,
            spread=spread,
            profit_pct=profit_pct,
            detected_at=datetime.now(timezone.utc),
        )

        logger.info(
            f"OPPORTUNITY: {market.name} | "
            f"YES: ${market.yes_ask:.2f} | NO: ${market.no_ask:.2f} | "
            f"Spread: ${spread:.2f} | Profit: {profit_pct * 100:.1f}%"
        )

        return opportunity

    def scan_markets(self, markets: list[UpDownMarket]) -> list[SpreadOpportunity]:
        """Scan multiple markets for opportunities.

        Args:
            markets: List of markets to scan

        Returns:
            List of detected opportunities, sorted by profit (highest first)
        """
        opportunities = []

        for market in markets:
            opp = self.check_opportunity(market)
            if opp:
                opportunities.append(opp)

        # Sort by profit (highest first)
        opportunities.sort(key=lambda x: x.profit_pct, reverse=True)

        return opportunities

    def calculate_trade_details(
        self,
        opportunity: SpreadOpportunity,
        investment: Decimal,
    ) -> dict:
        """Calculate trade details for an opportunity.

        For spread arbitrage, we buy both YES and NO proportionally.

        Args:
            opportunity: The opportunity to trade
            investment: Total USD to invest

        Returns:
            Dictionary with trade details
        """
        yes_price = opportunity.yes_price
        no_price = opportunity.no_price
        total_cost = yes_price + no_price

        # Proportional allocation
        yes_ratio = yes_price / total_cost
        no_ratio = no_price / total_cost

        yes_investment = investment * yes_ratio
        no_investment = investment * no_ratio

        # Calculate shares
        yes_shares = yes_investment / yes_price
        no_shares = no_investment / no_price

        # Shares should be equal (or very close) for spread arb
        # Use the minimum to ensure we can settle
        shares = min(yes_shares, no_shares)

        # Recalculate actual costs
        actual_yes_cost = shares * yes_price
        actual_no_cost = shares * no_price
        total_invested = actual_yes_cost + actual_no_cost

        # Guaranteed payout is the number of shares
        payout = shares
        gross_profit = payout - total_invested
        fee = total_invested * self.settings.polymarket_fee_rate
        net_profit = gross_profit - fee

        return {
            "yes_shares": shares,
            "no_shares": shares,
            "yes_price": yes_price,
            "no_price": no_price,
            "yes_cost": actual_yes_cost,
            "no_cost": actual_no_cost,
            "total_invested": total_invested,
            "payout": payout,
            "gross_profit": gross_profit,
            "fee": fee,
            "net_profit": net_profit,
            "profit_pct": (net_profit / total_invested * 100) if total_invested > 0 else Decimal("0"),
        }
