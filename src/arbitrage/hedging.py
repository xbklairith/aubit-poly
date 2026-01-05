"""Hedging arbitrage: prediction market vs real-world instruments."""

import logging
from decimal import Decimal

from src.config.settings import get_settings
from src.data_sources.crypto.binance import BinanceClient
from src.models.market import Market, Platform
from src.models.opportunity import HedgingArbOpportunity

logger = logging.getLogger(__name__)


class HedgingDetector:
    """
    Detect arbitrage between prediction markets and real-world instruments.

    Example: If Polymarket says "BTC > $100k by Jan 31" at 40%, but
    Binance BTC options imply 55% probability, there's an opportunity.
    """

    def __init__(self) -> None:
        """Initialize the detector."""
        self.settings = get_settings()
        self.min_profit = self.settings.min_hedging_arb_profit
        self.logger = logging.getLogger(__name__)
        self.binance = BinanceClient()

    async def scan(
        self,
        prediction_markets: list[Market],
    ) -> list[HedgingArbOpportunity]:
        """
        Scan for hedging arbitrage opportunities.

        Compares prediction market prices with implied probabilities
        from real-world derivatives (options, futures).

        Args:
            prediction_markets: Markets to check against real-world data

        Returns:
            List of detected opportunities
        """
        opportunities: list[HedgingArbOpportunity] = []

        # Filter for hedgeable markets (crypto price predictions)
        hedgeable = [m for m in prediction_markets if self._is_hedgeable(m)]

        if not hedgeable:
            self.logger.info("No hedgeable markets found")
            return opportunities

        # Connect to Binance for options data
        async with self.binance:
            for market in hedgeable:
                opp = await self._check_market(market)
                if opp:
                    opportunities.append(opp)

        # Sort by probability discrepancy
        opportunities.sort(
            key=lambda x: abs(x.probability_discrepancy),
            reverse=True,
        )

        return opportunities

    def _is_hedgeable(self, market: Market) -> bool:
        """
        Check if a market can be hedged with real-world instruments.

        Currently supports:
        - BTC price predictions
        - ETH price predictions

        Args:
            market: Market to check

        Returns:
            True if hedgeable
        """
        name = market.name.lower()

        # Check for crypto price predictions
        crypto_keywords = ["btc", "bitcoin", "eth", "ethereum"]
        price_keywords = ["price", "above", "below", "reach", "hit", "$"]

        has_crypto = any(kw in name for kw in crypto_keywords)
        has_price = any(kw in name for kw in price_keywords)

        return has_crypto and has_price

    async def _check_market(self, market: Market) -> HedgingArbOpportunity | None:
        """
        Check a single market against real-world instruments.

        Args:
            market: Prediction market to check

        Returns:
            Opportunity if significant discrepancy found
        """
        # Parse the prediction (e.g., "BTC above $100k by Jan 31")
        parsed = self._parse_prediction(market)
        if not parsed:
            return None

        underlying, target_price, direction, expiry = parsed

        # Get implied probability from options
        implied_prob = await self._get_implied_probability(
            underlying,
            target_price,
            expiry,
        )

        if implied_prob is None:
            return None

        # Get prediction market probability
        prediction_prob = market.yes_price
        if prediction_prob is None:
            return None

        # Adjust for direction
        if direction == "below":
            implied_prob = Decimal("1") - implied_prob

        # Calculate discrepancy
        discrepancy = implied_prob - prediction_prob

        # Check if discrepancy is significant enough
        if abs(discrepancy) < self.min_profit:
            return None

        self.logger.info(
            f"Found hedging opportunity: {abs(discrepancy):.2%} discrepancy - "
            f"Prediction: {prediction_prob:.2%}, Implied: {implied_prob:.2%}"
        )

        # Determine which side to bet on
        if discrepancy > 0:
            # Prediction market underpricing YES -> buy YES
            bet_direction = "YES"
            bet_price = prediction_prob
        else:
            # Prediction market overpricing YES -> buy NO
            bet_direction = "NO"
            bet_price = market.no_price or (Decimal("1") - prediction_prob)

        return HedgingArbOpportunity(
            id=f"hedge_{market.id}_{underlying}",
            markets=[market],
            platforms=[market.platform],
            prediction_platform=market.platform,
            prediction_price=prediction_prob,
            prediction_direction=bet_direction,
            hedge_platform=Platform.BINANCE,
            hedge_instrument=f"{underlying} Options",
            hedge_price=Decimal("0"),  # Would need actual option price
            implied_probability=implied_prob,
            probability_discrepancy=discrepancy,
            profit_percentage=abs(discrepancy),
            description=(
                f"Buy {bet_direction} on '{market.name}' - "
                f"Prediction: {prediction_prob:.1%}, Options implied: {implied_prob:.1%}"
            ),
            instructions=[
                f"1. Prediction market says: {prediction_prob:.1%}",
                f"2. Options market implies: {implied_prob:.1%}",
                f"3. Discrepancy: {abs(discrepancy):.1%}",
                "",
                f"4. Trade: Buy {bet_direction} at ${bet_price:.4f}",
                f"   URL: {market.url}",
                "",
                "5. Optional hedge: Use Binance options to lock in profit",
                f"   - Buy {'Put' if bet_direction == 'YES' else 'Call'} at strike ${target_price}",
            ],
            confidence=Decimal("0.6"),  # Lower confidence for hedging arb
        )

    def _parse_prediction(
        self,
        market: Market,
    ) -> tuple[str, Decimal, str, str] | None:
        """
        Parse a prediction market's question to extract tradeable parameters.

        Args:
            market: Market to parse

        Returns:
            Tuple of (underlying, target_price, direction, expiry) or None
        """
        import re

        name = market.name.lower()

        # Detect underlying asset
        underlying = None
        if "btc" in name or "bitcoin" in name:
            underlying = "BTC"
        elif "eth" in name or "ethereum" in name:
            underlying = "ETH"

        if not underlying:
            return None

        # Detect price target
        price_patterns = [
            r"\$([0-9,]+)k",  # $100k
            r"\$([0-9,]+)",  # $100000
            r"([0-9,]+)k",  # 100k
            r"([0-9]+)[,]?([0-9]{3})",  # 100,000 or 100000
        ]

        target_price = None
        for pattern in price_patterns:
            match = re.search(pattern, name)
            if match:
                try:
                    price_str = match.group(1).replace(",", "")
                    target_price = Decimal(price_str)
                    if "k" in name[match.end() : match.end() + 1] or target_price < 1000:
                        target_price *= 1000
                    break
                except (ValueError, IndexError):
                    continue

        if target_price is None:
            return None

        # Detect direction
        direction = "above"  # Default
        if any(word in name for word in ["below", "under", "less than"]):
            direction = "below"

        # Get expiry from market end date
        expiry = ""
        if market.end_date:
            expiry = market.end_date.strftime("%y%m%d")
        else:
            # Default to end of current month
            from datetime import datetime

            now = datetime.utcnow()
            expiry = now.strftime("%y%m") + "31"

        return underlying, target_price, direction, expiry

    async def _get_implied_probability(
        self,
        underlying: str,  # noqa: ARG002
        target_price: Decimal,
        expiry: str,
    ) -> Decimal | None:
        """
        Get implied probability from options market.

        Args:
            underlying: Asset symbol (BTC, ETH)
            target_price: Target price level
            expiry: Expiry date string (YYMMDD)

        Returns:
            Implied probability (0-1) or None
        """
        try:
            # Use Binance options to get implied probability
            prob = await self.binance.get_btc_price_probability(
                target_price,
                expiry,
            )
            return prob

        except Exception as e:
            self.logger.error(f"Failed to get implied probability: {e}")
            return None


async def find_hedging_arbitrage(
    prediction_markets: list[Market],
) -> list[HedgingArbOpportunity]:
    """
    Convenience function to find hedging arbitrage opportunities.

    Args:
        prediction_markets: List of prediction markets to check

    Returns:
        List of opportunities
    """
    detector = HedgingDetector()
    return await detector.scan(prediction_markets)
