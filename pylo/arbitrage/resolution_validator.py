"""
Resolution rule validator for cross-platform arbitrage.

CRITICAL: Different platforms may resolve the "same" event differently.

Real case example (Government Shutdown 2024):
- Polymarket: Resolved YES (OPM announcement)
- Kalshi: Resolved NO (no actual shutdown >24 hours)
- Result: 100% loss on one side of "hedged" position

This module validates that markets have compatible resolution rules
before allowing cross-platform arbitrage.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum

from pylo.config.settings import get_settings
from pylo.models.market import Market, Platform

logger = logging.getLogger(__name__)


class ResolutionType(str, Enum):
    """Types of resolution sources."""

    CRYPTO_PRICE = "crypto_price"  # Objective exchange price at specific time
    SPORTS = "sports"  # Official league/tournament data
    GOVERNMENT_DATA = "government_data"  # Official Fed/BLS/Census data
    ORACLE = "oracle"  # UMA or similar decentralized oracle
    CENTRALIZED = "centralized"  # Platform-specific centralized resolution
    SUBJECTIVE = "subjective"  # Human judgment required
    UNKNOWN = "unknown"


class ResolutionOracle(str, Enum):
    """Known resolution oracles."""

    UMA = "uma"  # Polymarket's default oracle
    KALSHI_INTERNAL = "kalshi_internal"
    BINANCE = "binance"
    COINBASE = "coinbase"
    FED = "federal_reserve"
    BLS = "bureau_labor_statistics"
    OFFICIAL_LEAGUE = "official_league"
    UNKNOWN = "unknown"


@dataclass
class ResolutionRule:
    """Extracted resolution rules from a market."""

    market_id: str
    platform: Platform
    resolution_type: ResolutionType
    oracle: ResolutionOracle
    data_source: str  # e.g., "Binance BTC/USDT", "Fed Funds Rate"
    time_specification: str  # e.g., "11:59 PM ET", "end of day"
    threshold_definition: str  # e.g., ">=100000", "any shutdown"
    raw_rules: str  # Original rules text


@dataclass
class ValidationResult:
    """Result of resolution rule validation."""

    is_compatible: bool
    confidence: float  # 0-1 confidence in the assessment
    reason: str
    warnings: list[str]


class ResolutionValidator:
    """
    Validates resolution rule compatibility between markets.

    Safety levels:
    1. SAFE: Identical objective resolution (same exchange, same time, same threshold)
    2. CAUTION: Similar but not identical rules (needs manual review)
    3. BLOCK: Different resolution criteria (do not trade)
    """

    def __init__(self) -> None:
        """Initialize the validator."""
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)

    def validate_pair(
        self,
        market_a: Market,
        market_b: Market,
    ) -> ValidationResult:
        """
        Validate that two markets have compatible resolution rules.

        Args:
            market_a: First market
            market_b: Second market

        Returns:
            ValidationResult with compatibility assessment
        """
        rule_a = self._extract_resolution_rules(market_a)
        rule_b = self._extract_resolution_rules(market_b)

        # Compare rules
        return self._compare_rules(rule_a, rule_b)

    def _extract_resolution_rules(self, market: Market) -> ResolutionRule:
        """Extract resolution rules from market data."""
        description = (market.description or "").lower()
        name = market.name.lower()
        raw = market.raw or {}

        # Try to get explicit rules from raw data
        rules_text = raw.get("rules_primary", "") or raw.get("resolution_source", "") or description

        # Determine resolution type
        res_type = self._determine_resolution_type(name, rules_text)

        # Determine oracle
        oracle = self._determine_oracle(market.platform, rules_text)

        # Extract data source
        data_source = self._extract_data_source(name, rules_text)

        # Extract time specification
        time_spec = self._extract_time_specification(rules_text)

        # Extract threshold definition
        threshold = self._extract_threshold(name, rules_text)

        return ResolutionRule(
            market_id=market.id,
            platform=market.platform,
            resolution_type=res_type,
            oracle=oracle,
            data_source=data_source,
            time_specification=time_spec,
            threshold_definition=threshold,
            raw_rules=rules_text,
        )

    def _determine_resolution_type(self, name: str, rules: str) -> ResolutionType:
        """Determine the resolution type from market info."""
        combined = f"{name} {rules}".lower()

        # Directional 15-minute markets (objective - based on exchange prices)
        directional_keywords = ["up or down", "price up", "up in next", "15 min"]
        if any(kw in combined for kw in directional_keywords):
            return ResolutionType.CRYPTO_PRICE  # Same resolution type - objective price

        # Crypto price markets
        crypto_keywords = [
            "btc",
            "bitcoin",
            "eth",
            "ethereum",
            "sol",
            "solana",
            "price",
            "above",
            "below",
        ]
        if any(kw in combined for kw in crypto_keywords):
            # Check for specific exchange mentions
            if any(ex in combined for ex in ["binance", "coinbase", "kraken", "exchange"]):
                return ResolutionType.CRYPTO_PRICE

        # Government data
        gov_keywords = [
            "fed",
            "fomc",
            "interest rate",
            "cpi",
            "inflation",
            "gdp",
            "unemployment",
            "bls",
        ]
        if any(kw in combined for kw in gov_keywords):
            return ResolutionType.GOVERNMENT_DATA

        # Sports
        sports_keywords = [
            "super bowl",
            "nfl",
            "nba",
            "mlb",
            "world series",
            "championship",
            "finals",
        ]
        if any(kw in combined for kw in sports_keywords):
            return ResolutionType.SPORTS

        # Check for oracle mentions
        if "uma" in combined or "optimistic oracle" in combined:
            return ResolutionType.ORACLE

        # Check for centralized resolution
        if "kalshi" in combined and "determine" in combined:
            return ResolutionType.CENTRALIZED

        # Subjective indicators
        subjective_keywords = ["judgment", "discretion", "may determine", "at its sole"]
        if any(kw in combined for kw in subjective_keywords):
            return ResolutionType.SUBJECTIVE

        return ResolutionType.UNKNOWN

    def _determine_oracle(self, platform: Platform, rules: str) -> ResolutionOracle:
        """Determine the resolution oracle."""
        rules_lower = rules.lower()

        if platform == Platform.POLYMARKET:
            if "uma" in rules_lower or "optimistic oracle" in rules_lower:
                return ResolutionOracle.UMA
        elif platform == Platform.KALSHI:
            return ResolutionOracle.KALSHI_INTERNAL

        # Check for specific data sources
        if "binance" in rules_lower:
            return ResolutionOracle.BINANCE
        if "coinbase" in rules_lower:
            return ResolutionOracle.COINBASE
        if "federal reserve" in rules_lower or "fed" in rules_lower:
            return ResolutionOracle.FED
        if "bureau of labor" in rules_lower or "bls" in rules_lower:
            return ResolutionOracle.BLS

        return ResolutionOracle.UNKNOWN

    def _extract_data_source(self, name: str, rules: str) -> str:
        """Extract the specific data source."""
        combined = f"{name} {rules}".lower()

        # Crypto exchanges
        exchanges = ["binance", "coinbase", "kraken", "bitstamp", "gemini"]
        for ex in exchanges:
            if ex in combined:
                return ex.capitalize()

        # Government sources
        if "federal reserve" in combined or "fed" in combined:
            return "Federal Reserve"
        if "bls" in combined or "bureau of labor" in combined:
            return "Bureau of Labor Statistics"
        if "census" in combined:
            return "US Census Bureau"

        return "Unknown"

    def _extract_time_specification(self, rules: str) -> str:
        """Extract time specification from rules."""
        rules_lower = rules.lower()

        # Common time patterns
        patterns = [
            r"(\d{1,2}:\d{2}\s*[ap]m\s*[a-z]{2,4})",  # 11:59 PM ET
            r"(end of (?:day|trading))",  # end of day
            r"(market close)",  # market close
            r"(\d{1,2}/\d{1,2}/\d{2,4})",  # MM/DD/YYYY
            r"(expiry|expiration)",  # expiry
        ]

        for pattern in patterns:
            match = re.search(pattern, rules_lower)
            if match:
                return match.group(1)

        return "unspecified"

    def _extract_threshold(self, name: str, rules: str) -> str:
        """Extract threshold definition from rules."""
        combined = f"{name} {rules}"

        # Price thresholds
        price_patterns = [
            r"(above|over|exceeds?|reaches?)\s*\$?([\d,]+)",
            r"(below|under|less than|falls?)\s*\$?([\d,]+)",
            r"(\$[\d,]+)\s*(?:or (?:more|higher|above))",
            r"(\$[\d,]+)\s*(?:or (?:less|lower|below))",
        ]

        for pattern in price_patterns:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                return match.group(0)

        # Rate thresholds (for Fed/economic data)
        rate_patterns = [
            r"(\d+\.?\d*%?\s*(?:or higher|or lower|basis points))",
            r"(cut|raise|hold|unchanged)",
        ]

        for pattern in rate_patterns:
            match = re.search(pattern, combined, re.IGNORECASE)
            if match:
                return match.group(0)

        return "unspecified"

    def _compare_rules(
        self,
        rule_a: ResolutionRule,
        rule_b: ResolutionRule,
    ) -> ValidationResult:
        """Compare two resolution rules for compatibility."""
        warnings: list[str] = []
        confidence = 1.0

        # BLOCK: Different resolution types
        if rule_a.resolution_type != rule_b.resolution_type:
            return ValidationResult(
                is_compatible=False,
                confidence=0.95,
                reason=f"Resolution type mismatch: {rule_a.resolution_type.value} vs {rule_b.resolution_type.value}",
                warnings=[],
            )

        # BLOCK: Subjective or unknown resolution
        unsafe_types = {ResolutionType.SUBJECTIVE, ResolutionType.UNKNOWN}
        if rule_a.resolution_type in unsafe_types or rule_b.resolution_type in unsafe_types:
            return ValidationResult(
                is_compatible=False,
                confidence=0.9,
                reason=f"Unsafe resolution type: {rule_a.resolution_type.value}",
                warnings=["Markets with subjective/unknown resolution are too risky for arbitrage"],
            )

        # CAUTION: Different oracles (even for same type)
        if rule_a.oracle != rule_b.oracle:
            # UMA vs centralized is particularly risky
            if rule_a.oracle == ResolutionOracle.UMA or rule_b.oracle == ResolutionOracle.UMA:
                warnings.append(
                    f"Different oracles: {rule_a.oracle.value} vs {rule_b.oracle.value}. "
                    "UMA may resolve differently than centralized sources."
                )
                confidence *= 0.7

        # CAUTION: Different data sources
        if rule_a.data_source.lower() != rule_b.data_source.lower():
            warnings.append(f"Different data sources: {rule_a.data_source} vs {rule_b.data_source}")
            confidence *= 0.8

        # CAUTION: Different time specifications
        if rule_a.time_specification != rule_b.time_specification:
            if (
                rule_a.time_specification != "unspecified"
                and rule_b.time_specification != "unspecified"
            ):
                warnings.append(
                    f"Different time specs: {rule_a.time_specification} vs {rule_b.time_specification}"
                )
                confidence *= 0.85

        # CAUTION: Different threshold definitions
        if rule_a.threshold_definition != rule_b.threshold_definition:
            if (
                rule_a.threshold_definition != "unspecified"
                and rule_b.threshold_definition != "unspecified"
            ):
                warnings.append(
                    f"Different thresholds: {rule_a.threshold_definition} vs {rule_b.threshold_definition}"
                )
                confidence *= 0.7

        # Check safe resolution types from settings
        safe_types = self.settings.get_safe_resolution_types()
        is_safe_type = rule_a.resolution_type.value in safe_types

        # Final decision
        if confidence < 0.5:
            return ValidationResult(
                is_compatible=False,
                confidence=confidence,
                reason="Too many resolution rule differences",
                warnings=warnings,
            )

        if not is_safe_type and confidence < 0.8:
            return ValidationResult(
                is_compatible=False,
                confidence=confidence,
                reason=f"Unsafe resolution type ({rule_a.resolution_type.value}) with low confidence",
                warnings=warnings,
            )

        return ValidationResult(
            is_compatible=True,
            confidence=confidence,
            reason=f"Compatible {rule_a.resolution_type.value} resolution",
            warnings=warnings,
        )

    def is_safe_for_arbitrage(
        self,
        market_a: Market,
        market_b: Market,
    ) -> tuple[bool, str]:
        """
        Quick check if a market pair is safe for arbitrage.

        Returns:
            Tuple of (is_safe, reason)
        """
        if not self.settings.cross_platform_validate_resolution:
            return True, "Resolution validation disabled"

        result = self.validate_pair(market_a, market_b)

        if result.is_compatible:
            warning_str = "; ".join(result.warnings) if result.warnings else ""
            return True, f"{result.reason}. {warning_str}".strip()
        else:
            return False, result.reason
