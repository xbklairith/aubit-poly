"""Cross-platform arbitrage detection: same event, different platforms."""

import logging
from decimal import Decimal

from pylo.arbitrage.event_matcher import AutoEventMatcher
from pylo.arbitrage.profitability_filter import ProfitabilityFilter
from pylo.arbitrage.resolution_validator import ResolutionValidator
from pylo.config.settings import get_settings
from pylo.models.market import Market, Platform
from pylo.models.opportunity import CrossPlatformArbOpportunity

logger = logging.getLogger(__name__)


class CrossPlatformDetector:
    """Detect arbitrage opportunities across different prediction platforms."""

    def __init__(self) -> None:
        """Initialize the detector."""
        self.settings = get_settings()
        self.min_profit = self.settings.min_cross_platform_arb_profit
        self.logger = logging.getLogger(__name__)

        # Automatic event matcher (replaces hardcoded EVENT_MAPPINGS)
        self.event_matcher = AutoEventMatcher(
            min_confidence=self.settings.cross_platform_min_match_confidence
        )

        # Resolution rule validator (CRITICAL for safety)
        self.resolution_validator = ResolutionValidator()

        # Profitability filter (avoid unprofitable trades)
        self.profitability_filter = ProfitabilityFilter()

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

        Uses automatic entity-based matching:
        1. Extract entities (asset, price, direction, date)
        2. Match markets with identical entities
        3. Filter by confidence threshold

        Args:
            markets_by_platform: Markets grouped by platform

        Returns:
            List of matched market groups
        """
        matches: list[dict[Platform, Market]] = []

        # Use automatic event matcher
        matched_pairs = self.event_matcher.match_all_platforms(markets_by_platform)

        for pair in matched_pairs:
            # Convert MatchedPair to the expected format
            match = {
                pair.market_a.platform: pair.market_a,
                pair.market_b.platform: pair.market_b,
            }
            matches.append(match)

            self.logger.debug(
                f"Matched: {pair.market_a.name} <-> {pair.market_b.name} "
                f"(confidence: {pair.confidence:.0%}, reason: {pair.match_reason})"
            )

        return matches

    def _markets_match(self, market_a: Market, market_b: Market) -> bool:
        """
        Check if two markets represent the same event.

        STRICTER matching to avoid false positives:
        - Exact date match required for arbitrage (dates affect probability)
        - Same price target required
        - Same direction (above/below) required

        Args:
            market_a: First market
            market_b: Second market

        Returns:
            True if markets represent the same event with high confidence
        """
        # 1. Exact name match (highest confidence)
        if market_a.name.lower() == market_b.name.lower():
            return True

        # 2. Semantic matching for crypto events
        # Must match: asset + price + direction + date
        name_a = market_a.name.lower()
        name_b = market_b.name.lower()

        # Extract components
        asset_a = self._extract_asset(name_a)
        asset_b = self._extract_asset(name_b)
        if not asset_a or not asset_b or asset_a != asset_b:
            return False

        price_a = self._extract_price(name_a)
        price_b = self._extract_price(name_b)
        if not price_a or not price_b or price_a != price_b:
            return False

        direction_a = self._extract_direction(name_a)
        direction_b = self._extract_direction(name_b)
        if direction_a != direction_b:
            return False

        # STRICT: Dates must match exactly for arbitrage
        if not self._dates_match_exact(market_a, market_b):
            self.logger.debug(f"Date mismatch: {market_a.name} vs {market_b.name}")
            return False

        return True

    def _extract_asset(self, name: str) -> str | None:
        """Extract crypto asset from market name."""
        asset_map = {
            "btc": "BTC",
            "bitcoin": "BTC",
            "eth": "ETH",
            "ethereum": "ETH",
            "sol": "SOL",
            "solana": "SOL",
            "xrp": "XRP",
            "ripple": "XRP",
            "doge": "DOGE",
            "dogecoin": "DOGE",
        }
        for keyword, asset in asset_map.items():
            if keyword in name:
                return asset
        return None

    def _extract_price(self, name: str) -> int | None:
        """Extract price target from market name."""
        import re

        # Match patterns like $100k, $100,000, 100000, 100K
        patterns = [
            r"\$?(\d+)[kK]",  # $100k or 100K
            r"\$?(\d{1,3}),?(\d{3})",  # $100,000 or 100000
        ]

        for pattern in patterns:
            match = re.search(pattern, name)
            if match:
                groups = match.groups()
                if len(groups) == 1:
                    return int(groups[0]) * 1000
                elif len(groups) == 2:
                    return int(groups[0] + groups[1])
        return None

    def _extract_direction(self, name: str) -> str:
        """Extract price direction from market name."""
        below_words = ["below", "under", "less than", "drops", "falls"]
        if any(word in name for word in below_words):
            return "below"
        return "above"  # Default assumption

    def _dates_match_exact(self, market_a: Market, market_b: Market) -> bool:
        """
        Check if market end dates match EXACTLY (same day).

        Arbitrage requires same event resolution - different dates = different events.
        """
        if market_a.end_date and market_b.end_date:
            # Same calendar day
            return market_a.end_date.date() == market_b.end_date.date()
        # If either date is missing, can't confirm they're the same event
        return False

    def _dates_match(self, market_a: Market, market_b: Market) -> bool:
        """Check if market end dates are within 1 day (for fuzzy matching)."""
        if market_a.end_date and market_b.end_date:
            diff = abs((market_a.end_date - market_b.end_date).days)
            return diff <= 1
        return False

    def _check_match(
        self,
        match: dict[Platform, Market],
    ) -> list[CrossPlatformArbOpportunity]:
        """
        Check a market match for arbitrage opportunities.

        Includes resolution validation and profitability filtering.

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

                # CRITICAL: Validate resolution rules before proceeding
                is_safe, reason = self.resolution_validator.is_safe_for_arbitrage(
                    market_a, market_b
                )
                if not is_safe:
                    self.logger.warning(
                        f"Skipping match due to resolution risk: {reason}\n"
                        f"  Market A: {market_a.name}\n"
                        f"  Market B: {market_b.name}"
                    )
                    continue

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

        Uses ASK prices (what you pay to buy) for accurate calculation.
        Includes fee estimation and profitability filtering.

        Args:
            market_yes: Market to buy YES on
            market_no: Market to buy NO on
            label: Label for this direction

        Returns:
            Opportunity if exists, None otherwise
        """
        # Use ASK prices for buying (falls back to last price if no ask)
        yes_price = market_yes.yes_ask_price
        no_price = market_no.no_ask_price

        if yes_price is None or no_price is None:
            return None

        # Reject zero or negative prices (bad data)
        if yes_price <= Decimal("0") or no_price <= Decimal("0"):
            return None

        total_cost = yes_price + no_price

        if total_cost >= Decimal("1"):
            return None

        # Gross profit before fees
        gross_profit = Decimal("1") - total_cost

        # Calculate fees from both platforms
        fee_rate_a = self.settings.get_fee_rate(market_yes.platform.value)
        fee_rate_b = self.settings.get_fee_rate(market_no.platform.value)
        fees_a = yes_price * fee_rate_a
        fees_b = no_price * fee_rate_b
        total_fees = fees_a + fees_b

        # Profit after fees
        profit_after_fees = gross_profit - total_fees

        # Skip if fees eat all profit
        if profit_after_fees <= Decimal("0"):
            self.logger.debug(f"Skipping cross-platform: profit {gross_profit:.2%} wiped by fees")
            return None

        # Apply profitability filters (liquidity, time, volume, staleness)
        filter_result = self.profitability_filter.check_opportunity(
            market_yes, market_no, gross_profit, total_fees
        )
        if not filter_result.passed:
            self.logger.debug(
                f"Skipping cross-platform: {filter_result.reason}\n"
                f"  Metrics: {filter_result.metrics}"
            )
            return None

        # Calculate optimal position size
        optimal_size = self.profitability_filter.calculate_optimal_size(
            market_yes, market_no, gross_profit, total_fees
        )

        # Estimate slippage for optimal size
        estimated_slippage = self.profitability_filter.estimate_slippage(
            market_yes, market_no, optimal_size
        )

        self.logger.info(
            f"Found cross-platform arb: {gross_profit:.2%} gross, "
            f"{profit_after_fees:.2%} after fees - "
            f"YES@{market_yes.platform.value} + NO@{market_no.platform.value}\n"
            f"  Optimal size: ${optimal_size:.0f}, Est. slippage: {estimated_slippage:.2%}"
        )

        return CrossPlatformArbOpportunity(
            id=f"xplat_{market_yes.id}_{market_no.id}_{label}",
            markets=[market_yes, market_no],
            platforms=[market_yes.platform, market_no.platform],
            platform_a=market_yes.platform,
            platform_b=market_no.platform,
            price_a=yes_price,
            price_b=no_price,
            profit_percentage=gross_profit,
            profit_after_fees=profit_after_fees,
            profit_absolute=profit_after_fees * optimal_size,
            estimated_fees=total_fees,
            recommended_size=optimal_size,
            estimated_slippage=estimated_slippage,
            description=(
                f"Buy YES@{yes_price:.4f} on {market_yes.platform.value}, "
                f"NO@{no_price:.4f} on {market_no.platform.value}"
            ),
            instructions=[
                f"1. Buy YES on {market_yes.platform.value}:",
                f"   - Market: {market_yes.name}",
                f"   - Price: ${yes_price:.4f}",
                f"   - Fee ({fee_rate_a:.2%}): ${fees_a:.4f}",
                f"   - URL: {market_yes.url}",
                "",
                f"2. Buy NO on {market_no.platform.value}:",
                f"   - Market: {market_no.name}",
                f"   - Price: ${no_price:.4f}",
                f"   - Fee ({fee_rate_b:.2%}): ${fees_b:.4f}",
                f"   - URL: {market_no.url}",
                "",
                f"3. Total cost: ${total_cost:.4f}",
                f"4. Total fees: ${total_fees:.4f}",
                "5. Guaranteed return: $1.00",
                f"6. Gross profit: ${gross_profit:.4f} ({gross_profit:.2%})",
                f"7. Net profit: ${profit_after_fees:.4f} ({profit_after_fees:.2%})",
                "",
                f"Recommended position size: ${optimal_size:.0f}",
                f"Estimated slippage: {estimated_slippage:.2%}",
                f"Expected absolute profit: ${profit_after_fees * optimal_size:.2f}",
            ],
            liquidity_available=min(
                market_yes.liquidity or Decimal("0"),
                market_no.liquidity or Decimal("0"),
            ),
        )
