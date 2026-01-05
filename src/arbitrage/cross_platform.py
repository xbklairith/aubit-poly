"""Cross-platform arbitrage detection: same event, different platforms."""

import logging
from decimal import Decimal

from src.config.settings import get_settings
from src.models.market import Market, Platform
from src.models.opportunity import CrossPlatformArbOpportunity

logger = logging.getLogger(__name__)


# Known event mappings between platforms
# These are examples - in production, use fuzzy matching or manual curation
EVENT_MAPPINGS: dict[str, dict[Platform, str]] = {
    # Example: BTC price predictions
    "btc_100k_jan": {
        Platform.POLYMARKET: "btc-100000-jan",  # Example IDs
        Platform.KALSHI: "BTCUSD-100K-JAN",
    },
}


class CrossPlatformDetector:
    """Detect arbitrage opportunities across different prediction platforms."""

    def __init__(self) -> None:
        """Initialize the detector."""
        self.settings = get_settings()
        self.min_profit = self.settings.min_cross_platform_arb_profit
        self.logger = logging.getLogger(__name__)

        # Cache markets by platform
        self._market_cache: dict[Platform, dict[str, Market]] = {}

    async def scan(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[CrossPlatformArbOpportunity]:
        """
        Scan for cross-platform arbitrage opportunities.

        Cross-platform arbitrage exists when:
        YES_price(Platform A) + NO_price(Platform B) < $1.00

        Args:
            markets_by_platform: Markets grouped by platform

        Returns:
            List of detected opportunities
        """
        opportunities: list[CrossPlatformArbOpportunity] = []

        # Build market cache for fast lookup
        self._build_cache(markets_by_platform)

        # Find matching markets across platforms
        matches = self._find_matching_markets(markets_by_platform)

        for match in matches:
            opps = self._check_match(match)
            opportunities.extend(opps)

        # Sort by profit
        opportunities.sort(key=lambda x: x.profit_percentage, reverse=True)

        return opportunities

    def _build_cache(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> None:
        """Build lookup cache of markets by ID."""
        self._market_cache = {}
        for platform, markets in markets_by_platform.items():
            self._market_cache[platform] = {m.id: m for m in markets}

    def _find_matching_markets(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[dict[Platform, Market]]:
        """
        Find markets that represent the same event across platforms.

        Uses multiple strategies:
        1. Known event mappings
        2. Fuzzy name matching
        3. Category + date matching

        Args:
            markets_by_platform: Markets grouped by platform

        Returns:
            List of matched market groups
        """
        matches: list[dict[Platform, Market]] = []

        # Strategy 1: Use known event mappings
        for _event_name, platform_ids in EVENT_MAPPINGS.items():
            match: dict[Platform, Market] = {}
            for platform, market_id in platform_ids.items():
                if platform in self._market_cache:
                    market = self._market_cache[platform].get(market_id)
                    if market:
                        match[platform] = market

            if len(match) >= 2:
                matches.append(match)

        # Strategy 2: Fuzzy name matching
        # Match markets with similar names across platforms
        platforms = list(markets_by_platform.keys())

        for i, platform_a in enumerate(platforms):
            for platform_b in platforms[i + 1 :]:
                for market_a in markets_by_platform.get(platform_a, []):
                    for market_b in markets_by_platform.get(platform_b, []):
                        if self._markets_match(market_a, market_b):
                            matches.append({platform_a: market_a, platform_b: market_b})

        return matches

    def _markets_match(self, market_a: Market, market_b: Market) -> bool:
        """
        Check if two markets represent the same event.

        Args:
            market_a: First market
            market_b: Second market

        Returns:
            True if markets likely represent the same event
        """
        # Simple matching strategies (can be enhanced)

        # 1. Exact name match
        if market_a.name.lower() == market_b.name.lower():
            return True

        # 2. Key terms matching for crypto events
        crypto_keywords = ["btc", "bitcoin", "eth", "ethereum", "crypto"]
        price_patterns = ["100k", "100000", "50k", "50000", "200k", "200000"]

        name_a = market_a.name.lower()
        name_b = market_b.name.lower()

        # Check if both are about the same crypto + price level
        for crypto in crypto_keywords:
            if crypto in name_a and crypto in name_b:
                for price in price_patterns:
                    if (
                        price in name_a
                        and price in name_b
                        and self._dates_match(market_a, market_b)
                    ):
                        return True

        return False

    def _dates_match(self, market_a: Market, market_b: Market) -> bool:
        """Check if market end dates are similar (within 7 days)."""
        if market_a.end_date and market_b.end_date:
            diff = abs((market_a.end_date - market_b.end_date).days)
            return diff <= 7
        return False

    def _check_match(
        self,
        match: dict[Platform, Market],
    ) -> list[CrossPlatformArbOpportunity]:
        """
        Check a market match for arbitrage opportunities.

        Args:
            match: Dictionary of platform -> market

        Returns:
            List of opportunities (can be multiple per match)
        """
        opportunities: list[CrossPlatformArbOpportunity] = []
        platforms = list(match.keys())

        # Check all platform pairs
        for i, platform_a in enumerate(platforms):
            for platform_b in platforms[i + 1 :]:
                market_a = match[platform_a]
                market_b = match[platform_b]

                # Check YES on A + NO on B
                opp1 = self._check_pair(market_a, market_b, "YES_A_NO_B")
                if opp1:
                    opportunities.append(opp1)

                # Check NO on A + YES on B
                opp2 = self._check_pair(market_b, market_a, "YES_B_NO_A")
                if opp2:
                    opportunities.append(opp2)

        return opportunities

    def _check_pair(
        self,
        market_yes: Market,
        market_no: Market,
        label: str,
    ) -> CrossPlatformArbOpportunity | None:
        """
        Check a specific pair for arbitrage.

        Args:
            market_yes: Market to buy YES on
            market_no: Market to buy NO on
            label: Label for this direction

        Returns:
            Opportunity if exists, None otherwise
        """
        yes_price = market_yes.yes_price
        no_price = market_no.no_price

        if yes_price is None or no_price is None:
            return None

        total_cost = yes_price + no_price

        if total_cost >= Decimal("1"):
            return None

        profit = Decimal("1") - total_cost

        if profit < self.min_profit:
            return None

        self.logger.info(
            f"Found cross-platform arb: {profit:.2%} - "
            f"YES@{market_yes.platform.value} + NO@{market_no.platform.value}"
        )

        return CrossPlatformArbOpportunity(
            id=f"xplat_{market_yes.id}_{market_no.id}_{label}",
            markets=[market_yes, market_no],
            platforms=[market_yes.platform, market_no.platform],
            platform_a=market_yes.platform,
            platform_b=market_no.platform,
            price_a=yes_price,
            price_b=no_price,
            profit_percentage=profit,
            profit_absolute=profit,
            description=(
                f"Buy YES@{yes_price:.4f} on {market_yes.platform.value}, "
                f"NO@{no_price:.4f} on {market_no.platform.value}"
            ),
            instructions=[
                f"1. Buy YES on {market_yes.platform.value}:",
                f"   - Market: {market_yes.name}",
                f"   - Price: ${yes_price:.4f}",
                f"   - URL: {market_yes.url}",
                "",
                f"2. Buy NO on {market_no.platform.value}:",
                f"   - Market: {market_no.name}",
                f"   - Price: ${no_price:.4f}",
                f"   - URL: {market_no.url}",
                "",
                f"3. Total cost: ${total_cost:.4f}",
                "4. Guaranteed return: $1.00",
                f"5. Profit: ${profit:.4f} ({profit:.2%})",
            ],
            liquidity_available=min(
                market_yes.liquidity or Decimal("0"),
                market_no.liquidity or Decimal("0"),
            ),
        )
