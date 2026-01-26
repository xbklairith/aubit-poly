"""Automatic event matching for cross-platform arbitrage."""

import logging
import re
from dataclasses import dataclass
from datetime import datetime

from pylo.models.market import Market, Platform

logger = logging.getLogger(__name__)


@dataclass
class MatchedPair:
    """A pair of markets matched as the same event across platforms."""

    market_a: Market
    market_b: Market
    confidence: float
    match_reason: str
    entity_match: dict[str, str | int | datetime | None]  # Extracted entities that matched


@dataclass
class MarketEntity:
    """Extracted entities from a market name."""

    asset: str | None  # BTC, ETH, etc.
    price_target: int | None  # 100000, 50000, etc.
    direction: str | None  # "above", "below"
    date: datetime | None  # Resolution date
    event_type: str | None  # "crypto_price", "fed_rate", "election", etc.
    raw_name: str


class AutoEventMatcher:
    """
    Automatically match markets across platforms using entity extraction.

    Matching strategy:
    1. Extract entities: asset, price, direction, date, event type
    2. Match markets with identical entities (high confidence)
    3. Fuzzy match for similar but not identical markets (lower confidence)
    """

    # Minimum confidence to consider a match valid
    MIN_CONFIDENCE = 0.9

    # Asset keyword mappings
    ASSET_KEYWORDS = {
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
        "ada": "ADA",
        "cardano": "ADA",
        "bnb": "BNB",
        "binance coin": "BNB",
    }

    # Event type keywords - ORDER MATTERS (more specific first)
    EVENT_TYPE_KEYWORDS = [
        # Directional 15-minute markets (check FIRST - "up or down" contains "dow")
        ("up or down", "directional_15m"),
        ("price up", "directional_15m"),
        ("up in next", "directional_15m"),
        ("15 min", "directional_15m"),
        # Financial indices
        ("s&p", "sp500"),
        ("sp500", "sp500"),
        ("dow jones", "dow"),  # More specific than just "dow"
        ("nasdaq", "nasdaq"),
        # Fed/economic
        ("fed", "fed_rate"),
        ("federal reserve", "fed_rate"),
        ("interest rate", "fed_rate"),
        ("fomc", "fed_rate"),
        ("cpi", "inflation"),
        ("inflation", "inflation"),
        ("gdp", "gdp"),
        ("unemployment", "employment"),
        # Political
        ("election", "election"),
        ("president", "election"),
        ("senate", "election"),
        ("congress", "election"),
    ]

    def __init__(self, min_confidence: float = 0.9):
        """Initialize the matcher."""
        self.min_confidence = min_confidence
        self.logger = logging.getLogger(__name__)

    def match(
        self,
        markets_a: list[Market],
        markets_b: list[Market],
    ) -> list[MatchedPair]:
        """
        Find matching market pairs between two lists of markets.

        Works for any platform combination: Polymarket-Kalshi, Polymarket-Limitless,
        Kalshi-Limitless, etc.

        Args:
            markets_a: Markets from first platform
            markets_b: Markets from second platform

        Returns:
            List of matched market pairs with confidence scores
        """
        matches: list[MatchedPair] = []

        # Extract entities for all markets
        entities_a = [(m, self._extract_entities(m)) for m in markets_a]
        entities_b = [(m, self._extract_entities(m)) for m in markets_b]

        # Compare each pair
        for market_a, ent_a in entities_a:
            for market_b, ent_b in entities_b:
                score, reason = self._calculate_match_score(ent_a, ent_b)

                # Use epsilon for floating point comparison
                if score >= self.min_confidence - 0.001:
                    matches.append(
                        MatchedPair(
                            market_a=market_a,
                            market_b=market_b,
                            confidence=score,
                            match_reason=reason,
                            entity_match={
                                "asset": ent_a.asset,
                                "price_target": ent_a.price_target,
                                "direction": ent_a.direction,
                                "date": ent_a.date,
                                "event_type": ent_a.event_type,
                            },
                        )
                    )

        # Sort by confidence (highest first)
        matches.sort(key=lambda m: m.confidence, reverse=True)

        self.logger.info(
            f"Found {len(matches)} cross-platform matches (>= {self.min_confidence:.0%} confidence)"
        )

        return matches

    def _extract_entities(self, market: Market) -> MarketEntity:
        """Extract structured entities from a market name."""
        name = market.name.lower()

        return MarketEntity(
            asset=self._extract_asset(name),
            price_target=self._extract_price(name),
            direction=self._extract_direction(name),
            date=market.end_date,
            event_type=self._extract_event_type(name),
            raw_name=market.name,
        )

    def _extract_asset(self, name: str) -> str | None:
        """Extract crypto asset from market name."""
        for keyword, asset in self.ASSET_KEYWORDS.items():
            if keyword in name:
                return asset
        return None

    def _extract_price(self, name: str) -> int | None:
        """Extract price target from market name."""
        patterns = [
            r"\$?([\d,]+)\s*k\b",  # $100k, 100k, 100K
            r"\$?([\d]{1,3}(?:,\d{3})+)",  # $100,000
            r"\$?([\d]{4,})\b",  # $100000 (4+ digits without comma)
            r"above\s*\$?([\d,]+)",  # above $100k
            r"below\s*\$?([\d,]+)",  # below $100k
            r"over\s*\$?([\d,]+)",  # over $100k
            r"under\s*\$?([\d,]+)",  # under $100k
        ]

        for pattern in patterns:
            match = re.search(pattern, name, re.IGNORECASE)
            if match:
                price_str = match.group(1).replace(",", "")
                price = int(price_str)

                # Handle 'k' suffix
                if "k" in name[match.start() : match.end()].lower():
                    price *= 1000

                # Sanity check for crypto prices
                if 1000 <= price <= 1000000:
                    return price

        return None

    def _extract_direction(self, name: str) -> str | None:
        """Extract price direction from market name."""
        above_words = ["above", "over", "exceeds", "higher than", "reaches", "hits"]
        below_words = ["below", "under", "less than", "drops", "falls", "lower than"]

        for word in above_words:
            if word in name:
                return "above"
        for word in below_words:
            if word in name:
                return "below"

        return None

    def _extract_event_type(self, name: str) -> str | None:
        """Extract event type from market name."""
        # Check keywords in order (more specific patterns first)
        for keyword, event_type in self.EVENT_TYPE_KEYWORDS:
            if keyword in name:
                return event_type

        # Check for crypto price markets (fallback)
        if self._extract_asset(name):
            return "crypto_price"

        return None

    def _calculate_match_score(
        self,
        ent_a: MarketEntity,
        ent_b: MarketEntity,
    ) -> tuple[float, str]:
        """
        Calculate match confidence score between two market entities.

        Returns:
            Tuple of (score 0-1, reason string)
        """
        score = 0.0
        reasons = []

        # Asset match (required for crypto markets)
        if ent_a.asset and ent_b.asset:
            if ent_a.asset == ent_b.asset:
                score += 0.3
                reasons.append(f"asset={ent_a.asset}")
            else:
                # Different assets = definitely not the same market
                return 0.0, "asset mismatch"

        # Price target match (required for price markets)
        if ent_a.price_target and ent_b.price_target:
            if ent_a.price_target == ent_b.price_target:
                score += 0.3
                reasons.append(f"price=${ent_a.price_target:,}")
            else:
                # Allow small tolerance for rounding (e.g., 99999 vs 100000)
                diff_pct = abs(ent_a.price_target - ent_b.price_target) / ent_a.price_target
                if diff_pct < 0.01:  # Within 1%
                    score += 0.2
                    reasons.append(f"price~${ent_a.price_target:,}")
                else:
                    return 0.0, "price mismatch"

        # Direction match (required)
        if ent_a.direction and ent_b.direction:
            if ent_a.direction == ent_b.direction:
                score += 0.2
                reasons.append(f"direction={ent_a.direction}")
            else:
                return 0.0, "direction mismatch"

        # Date match (required for same-event arbitrage)
        if ent_a.date and ent_b.date:
            # For 15m markets, need exact time match (within 5 min tolerance)
            is_15m = (
                ent_a.event_type == "directional_15m" or ent_b.event_type == "directional_15m"
            )
            if is_15m:
                # Strip timezone for comparison
                date_a = ent_a.date.replace(tzinfo=None) if ent_a.date.tzinfo else ent_a.date
                date_b = ent_b.date.replace(tzinfo=None) if ent_b.date.tzinfo else ent_b.date
                time_diff = abs((date_a - date_b).total_seconds())
                if time_diff <= 300:  # Within 5 minutes
                    score += 0.3  # Higher score for exact time match
                    reasons.append(f"time={ent_a.date.strftime('%H:%M')}")
                else:
                    return 0.0, f"time mismatch ({time_diff/60:.0f}min diff)"
            elif ent_a.date.date() == ent_b.date.date():
                # For other markets, same calendar day is enough
                score += 0.2
                reasons.append(f"date={ent_a.date.date()}")
            else:
                # Different dates = different events
                return 0.0, "date mismatch"

        # Event type bonus (not required but increases confidence)
        if ent_a.event_type and ent_b.event_type:
            if ent_a.event_type == ent_b.event_type:
                score += 0.1
                reasons.append(f"type={ent_a.event_type}")

        # Require minimum components for a valid match
        if not reasons:
            return 0.0, "no matching components"

        # For directional 15m markets (up/down), require asset + time + type
        if ent_a.event_type == "directional_15m" or ent_b.event_type == "directional_15m":
            required = {"asset", "time", "type"}
            matched = set()
            for r in reasons:
                if r.startswith("asset"):
                    matched.add("asset")
                elif r.startswith("time") or r.startswith("date"):
                    matched.add("time")
                elif r.startswith("type"):
                    matched.add("type")

            missing = required - matched
            if missing:
                return 0.0, f"missing for 15m: {', '.join(missing)}"

            # Boost score for directional markets since they're simpler
            score = min(score + 0.2, 1.0)

        # For crypto price markets, require all four components
        elif ent_a.event_type == "crypto_price":
            required = {"asset", "price", "direction", "date"}
            matched = set()
            for r in reasons:
                if r.startswith("asset"):
                    matched.add("asset")
                elif r.startswith("price"):
                    matched.add("price")
                elif r.startswith("direction"):
                    matched.add("direction")
                elif r.startswith("date"):
                    matched.add("date")

            missing = required - matched
            if missing:
                return 0.0, f"missing: {', '.join(missing)}"

        reason_str = " + ".join(reasons)
        return score, reason_str

    def match_all_platforms(
        self,
        markets_by_platform: dict[Platform, list[Market]],
    ) -> list[MatchedPair]:
        """
        Match markets across all platforms.

        Args:
            markets_by_platform: Markets grouped by platform

        Returns:
            List of all matched pairs
        """
        all_matches: list[MatchedPair] = []

        platforms = list(markets_by_platform.keys())

        # Compare each platform pair
        for i, platform_a in enumerate(platforms):
            for platform_b in platforms[i + 1 :]:
                markets_a = markets_by_platform.get(platform_a, [])
                markets_b = markets_by_platform.get(platform_b, [])

                if not markets_a or not markets_b:
                    continue

                # Use match() for the comparison
                matches = self.match(markets_a, markets_b)
                all_matches.extend(matches)

        return all_matches
