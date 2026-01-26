"""
Profitability filters for cross-platform arbitrage.

Research finding: 78% of opportunities in low-volume markets FAIL due to execution issues.

This module implements strict filters to avoid unprofitable trades:
1. Spread threshold (must exceed fees)
2. Liquidity depth (must have real depth on orderbook)
3. Time to resolution (avoid last-minute chaos)
4. Volume requirements (avoid illiquid markets)
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from pylo.config.settings import get_settings
from pylo.models.market import Market

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Result of profitability filtering."""

    passed: bool
    reason: str
    metrics: dict[str, float | bool | None]


class ProfitabilityFilter:
    """
    Filter arbitrage opportunities for profitability.

    Lesson learned from research: Most arb opportunities fail in practice.
    Top arbitrageur made ~$500 avg profit per trade from 4,049 transactions.
    This means careful filtering is essential.
    """

    def __init__(self) -> None:
        """Initialize the filter."""
        self.settings = get_settings()
        self.logger = logging.getLogger(__name__)

    def check_opportunity(
        self,
        market_a: Market,
        market_b: Market,
        gross_spread: Decimal,
        estimated_fees: Decimal,
    ) -> FilterResult:
        """
        Check if an arbitrage opportunity passes all profitability filters.

        Args:
            market_a: First market (buy YES)
            market_b: Second market (buy NO)
            gross_spread: Gross profit before fees (1 - total_cost)
            estimated_fees: Estimated total fees from both platforms

        Returns:
            FilterResult with pass/fail and reason
        """
        # Detect if this is a 15-minute market for threshold selection
        is_15m_market = self._is_15m_directional_market(market_a, market_b)

        metrics: dict[str, float | bool | None] = {
            "gross_spread": float(gross_spread),
            "estimated_fees": float(estimated_fees),
            "net_spread": float(gross_spread - estimated_fees),
            "is_15m_market": is_15m_market,
        }

        # Filter 1: Net spread must exceed minimum threshold
        # Use lower threshold for 15m markets (quick execution, lower risk)
        net_spread = gross_spread - estimated_fees
        if is_15m_market:
            min_profit = self.settings.min_cross_platform_15m_arb_profit
        else:
            min_profit = self.settings.min_cross_platform_arb_profit

        if net_spread < min_profit:
            return FilterResult(
                passed=False,
                reason=f"Net spread {net_spread:.2%} < minimum {min_profit:.2%}" + (" (15m market)" if is_15m_market else ""),
                metrics=metrics,
            )

        # Filter 2: Liquidity depth check
        liquidity_result = self._check_liquidity(market_a, market_b)
        if not liquidity_result.passed:
            return liquidity_result

        metrics.update(liquidity_result.metrics)

        # Filter 3: Time to resolution check
        time_result = self._check_time_to_resolution(market_a, market_b)
        if not time_result.passed:
            return time_result

        metrics.update(time_result.metrics)

        # Filter 4: Volume check
        volume_result = self._check_volume(market_a, market_b)
        if not volume_result.passed:
            return volume_result

        metrics.update(volume_result.metrics)

        # Filter 5: Price staleness check
        staleness_result = self._check_price_staleness(market_a, market_b)
        if not staleness_result.passed:
            return staleness_result

        metrics.update(staleness_result.metrics)

        return FilterResult(
            passed=True,
            reason="All profitability filters passed",
            metrics=metrics,
        )

    def _check_liquidity(self, market_a: Market, market_b: Market) -> FilterResult:
        """Check orderbook liquidity depth."""
        # Use lower threshold for 15m markets
        is_15m = self._is_15m_directional_market(market_a, market_b)
        if is_15m:
            min_liquidity = self.settings.cross_platform_15m_min_liquidity
        else:
            min_liquidity = self.settings.cross_platform_min_liquidity

        # Get liquidity from markets
        liquidity_a = market_a.liquidity or Decimal("0")
        liquidity_b = market_b.liquidity or Decimal("0")

        # Also check bid/ask depth if available
        yes_outcome_a = next(
            (o for o in market_a.outcomes if o.name.upper() in ("YES", "TRUE", "1")),
            None,
        )
        no_outcome_b = next(
            (o for o in market_b.outcomes if o.name.upper() in ("NO", "FALSE", "0")),
            None,
        )

        depth_a = (
            yes_outcome_a.ask_depth if yes_outcome_a and yes_outcome_a.ask_depth else liquidity_a
        )
        depth_b = no_outcome_b.ask_depth if no_outcome_b and no_outcome_b.ask_depth else liquidity_b

        metrics: dict[str, float | bool | None] = {
            "liquidity_a": float(liquidity_a),
            "liquidity_b": float(liquidity_b),
            "depth_a": float(depth_a),
            "depth_b": float(depth_b),
            "is_15m_market": is_15m,
        }

        # Check if either side has insufficient liquidity
        min_depth = min(depth_a, depth_b)

        if min_depth < min_liquidity:
            return FilterResult(
                passed=False,
                reason=f"Insufficient liquidity: ${min_depth:.0f} < ${min_liquidity:.0f} minimum" + (" (15m market)" if is_15m else ""),
                metrics=metrics,
            )

        return FilterResult(
            passed=True,
            reason="Liquidity check passed",
            metrics=metrics,
        )

    def _check_time_to_resolution(self, market_a: Market, market_b: Market) -> FilterResult:
        """Check time remaining until resolution."""
        now = datetime.utcnow()

        # Detect if this is a 15-minute directional market
        is_15m_market = self._is_15m_directional_market(market_a, market_b)

        # Use shorter threshold for 15-minute markets
        if is_15m_market:
            min_time = self.settings.cross_platform_15m_min_time_to_resolution
        else:
            min_time = self.settings.cross_platform_min_time_to_resolution

        # Get end dates
        end_a = market_a.end_date
        end_b = market_b.end_date

        # Handle timezone-aware datetimes
        if end_a and end_a.tzinfo is not None:
            end_a = end_a.replace(tzinfo=None)
        if end_b and end_b.tzinfo is not None:
            end_b = end_b.replace(tzinfo=None)

        # Calculate time remaining
        time_a = (end_a - now).total_seconds() if end_a else float("inf")
        time_b = (end_b - now).total_seconds() if end_b else float("inf")

        min_time_remaining = min(time_a, time_b)

        metrics: dict[str, float | bool | None] = {
            "time_to_resolution_a_hours": time_a / 3600 if time_a != float("inf") else None,
            "time_to_resolution_b_hours": time_b / 3600 if time_b != float("inf") else None,
            "min_time_remaining_hours": min_time_remaining / 3600
            if min_time_remaining != float("inf")
            else None,
            "is_15m_market": is_15m_market,
        }

        if min_time_remaining < min_time:
            if is_15m_market:
                mins = min_time_remaining / 60
                min_mins = min_time / 60
                return FilterResult(
                    passed=False,
                    reason=f"Too close to resolution: {mins:.1f}min < {min_mins:.1f}min minimum (15m market)",
                    metrics=metrics,
                )
            else:
                hours = min_time_remaining / 3600
                min_hours = min_time / 3600
                return FilterResult(
                    passed=False,
                    reason=f"Too close to resolution: {hours:.1f}h < {min_hours:.1f}h minimum",
                    metrics=metrics,
                )

        return FilterResult(
            passed=True,
            reason="Time to resolution check passed",
            metrics=metrics,
        )

    def _is_15m_directional_market(self, market_a: Market, market_b: Market) -> bool:
        """Detect if this is a 15-minute directional (up/down) market."""
        directional_keywords = ["up or down", "up/down", "15m", "15 min", "15min", "kxbtc15m", "kxeth15m", "kxsol15m"]

        for market in [market_a, market_b]:
            name_lower = market.name.lower()
            market_id_lower = market.id.lower()

            for keyword in directional_keywords:
                if keyword in name_lower or keyword in market_id_lower:
                    return True

        return False

    def _check_volume(self, market_a: Market, market_b: Market) -> FilterResult:
        """Check market volume (liquidity indicator)."""
        # Use lower threshold for 15m markets
        is_15m = self._is_15m_directional_market(market_a, market_b)
        if is_15m:
            min_volume = self.settings.cross_platform_15m_min_daily_volume
        else:
            min_volume = self.settings.cross_platform_min_daily_volume

        volume_a = market_a.volume_24h or Decimal("0")
        volume_b = market_b.volume_24h or Decimal("0")

        metrics: dict[str, float | bool | None] = {
            "volume_24h_a": float(volume_a),
            "volume_24h_b": float(volume_b),
            "is_15m_market": is_15m,
        }

        min_vol = min(volume_a, volume_b)

        if min_vol < min_volume:
            return FilterResult(
                passed=False,
                reason=f"Insufficient volume: ${min_vol:.0f}/day < ${min_volume:.0f} minimum" + (" (15m market)" if is_15m else ""),
                metrics=metrics,
            )

        return FilterResult(
            passed=True,
            reason="Volume check passed",
            metrics=metrics,
        )

    def _check_price_staleness(self, market_a: Market, market_b: Market) -> FilterResult:
        """Check if prices are fresh enough."""
        max_age = self.settings.max_price_age_seconds
        now = datetime.utcnow()

        age_a = (now - market_a.fetched_at).total_seconds()
        age_b = (now - market_b.fetched_at).total_seconds()

        metrics: dict[str, float | bool | None] = {
            "price_age_a_seconds": age_a,
            "price_age_b_seconds": age_b,
        }

        max_current_age = max(age_a, age_b)

        if max_current_age > max_age:
            return FilterResult(
                passed=False,
                reason=f"Stale prices: {max_current_age:.0f}s > {max_age}s maximum",
                metrics=metrics,
            )

        return FilterResult(
            passed=True,
            reason="Price freshness check passed",
            metrics=metrics,
        )

    def calculate_optimal_size(
        self,
        market_a: Market,
        market_b: Market,
        gross_spread: Decimal,  # noqa: ARG002 - reserved for future use
        estimated_fees: Decimal,  # noqa: ARG002 - reserved for future use
    ) -> Decimal:
        """
        Calculate optimal position size considering liquidity constraints.

        Args:
            market_a: First market
            market_b: Second market
            gross_spread: Gross profit percentage
            estimated_fees: Estimated fees percentage

        Returns:
            Optimal position size in USD
        """
        # Get available liquidity
        liquidity_a = market_a.liquidity or Decimal("0")
        liquidity_b = market_b.liquidity or Decimal("0")

        # Get bid/ask depth if available
        yes_outcome_a = next(
            (o for o in market_a.outcomes if o.name.upper() in ("YES", "TRUE", "1")),
            None,
        )
        no_outcome_b = next(
            (o for o in market_b.outcomes if o.name.upper() in ("NO", "FALSE", "0")),
            None,
        )

        depth_a = (
            yes_outcome_a.ask_depth if yes_outcome_a and yes_outcome_a.ask_depth else liquidity_a
        )
        depth_b = no_outcome_b.ask_depth if no_outcome_b and no_outcome_b.ask_depth else liquidity_b

        # Maximum size is limited by the smaller orderbook
        max_by_liquidity = min(depth_a, depth_b)

        # Apply slippage safety margin (only use 50% of available depth)
        safe_size = max_by_liquidity * Decimal("0.5")

        # Cap at reasonable maximum
        max_position = Decimal("10000")  # $10k max per trade
        min_position = Decimal("50")  # $50 minimum (otherwise not worth it)

        optimal = min(safe_size, max_position)

        if optimal < min_position:
            return Decimal("0")  # Not worth trading

        return optimal

    def estimate_slippage(
        self,
        market_a: Market,
        market_b: Market,
        position_size: Decimal,
    ) -> Decimal:
        """
        Estimate slippage for a given position size.

        Args:
            market_a: First market
            market_b: Second market
            position_size: Intended position size in USD

        Returns:
            Estimated slippage as decimal (e.g., 0.01 = 1%)
        """
        # Get available liquidity
        liquidity_a = market_a.liquidity or Decimal("1")
        liquidity_b = market_b.liquidity or Decimal("1")

        min_liquidity = min(liquidity_a, liquidity_b)

        if min_liquidity == 0:
            return Decimal("1")  # 100% slippage = impossible

        # Simple linear slippage model
        # Assume 1% slippage per 10% of liquidity used
        utilization = position_size / min_liquidity
        slippage = utilization * Decimal("0.1")

        return min(slippage, Decimal("0.5"))  # Cap at 50%
