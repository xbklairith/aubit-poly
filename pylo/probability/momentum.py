"""Momentum-based probability estimation for crypto price direction."""

import logging
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from statistics import mean, stdev

from pylo.data_sources.crypto.binance_klines import BinanceKlinesClient, Candle

logger = logging.getLogger(__name__)


@dataclass
class MomentumSignal:
    """Result of momentum probability calculation."""

    probability_up: Decimal
    confidence: Decimal
    momentum_score: Decimal  # Raw momentum z-score
    volatility: Decimal
    trend_consistency: Decimal  # % of candles in same direction as signal
    sample_size: int
    asset: str
    interval_minutes: int
    timestamp: datetime

    @property
    def probability_down(self) -> Decimal:
        """Probability of price going down."""
        return Decimal("1") - self.probability_up

    @property
    def is_bullish(self) -> bool:
        """Check if momentum suggests upward movement."""
        return self.probability_up > Decimal("0.5")

    @property
    def is_strong_signal(self) -> bool:
        """Check if signal is strong (>60% or <40% probability)."""
        return abs(self.probability_up - Decimal("0.5")) > Decimal("0.1")


class MomentumCalculator:
    """Calculate momentum-based probability from price candles."""

    def __init__(
        self,
        recent_weight: float = 0.6,
        min_candles: int = 5,
    ):
        """
        Initialize momentum calculator.

        Args:
            recent_weight: Weight for recent candles (0-1). Higher = more emphasis on recent.
            min_candles: Minimum candles required for valid calculation.
        """
        self.recent_weight = recent_weight
        self.min_candles = min_candles
        self.logger = logging.getLogger(f"{__name__}.MomentumCalculator")

    def calculate_from_candles(
        self,
        candles: list[Candle],
        asset: str = "UNKNOWN",
        interval_minutes: int = 15,
    ) -> MomentumSignal | None:
        """
        Calculate momentum probability from a list of candles.

        Args:
            candles: List of Candle objects (oldest first)
            asset: Asset name for logging
            interval_minutes: Candle interval in minutes

        Returns:
            MomentumSignal with probability estimate, or None if insufficient data
        """
        if len(candles) < self.min_candles:
            self.logger.warning(
                f"Insufficient candles for {asset}: {len(candles)} < {self.min_candles}"
            )
            return None

        # Calculate returns for each candle
        returns = [float(c.return_pct) for c in candles]

        # Calculate volatility (standard deviation of returns)
        try:
            volatility = stdev(returns) if len(returns) > 1 else 0.0
        except Exception:
            volatility = 0.0

        if volatility == 0:
            # No volatility = no movement = 50% probability
            return MomentumSignal(
                probability_up=Decimal("0.5"),
                confidence=Decimal("0.3"),  # Low confidence when no volatility
                momentum_score=Decimal("0"),
                volatility=Decimal("0"),
                trend_consistency=Decimal("0.5"),
                sample_size=len(candles),
                asset=asset,
                interval_minutes=interval_minutes,
                timestamp=datetime.now(UTC),
            )

        # Split into recent and older periods
        split_idx = max(1, len(returns) - 3)  # Last 3 candles are "recent"
        recent_returns = returns[split_idx:]
        older_returns = returns[:split_idx]

        # Calculate weighted momentum
        recent_avg = mean(recent_returns) if recent_returns else 0.0
        older_avg = mean(older_returns) if older_returns else 0.0

        weighted_momentum = (
            self.recent_weight * recent_avg
            + (1 - self.recent_weight) * older_avg
        )

        # Convert momentum to z-score
        z_score = weighted_momentum / volatility

        # Convert z-score to probability using normal CDF
        # norm.cdf(z) gives probability that a standard normal is <= z
        # If momentum > 0, we expect higher probability of UP
        prob_up = self._normal_cdf(z_score)

        # Calculate trend consistency
        # What % of candles moved in the same direction as our signal?
        bullish_count = sum(1 for r in returns if r > 0)
        trend_consistency = bullish_count / len(returns)

        # Calculate confidence based on:
        # 1. Trend consistency (aligned with probability = higher confidence)
        # 2. Sample size
        # 3. Volatility stability
        expected_consistency = prob_up
        consistency_alignment = 1 - abs(float(trend_consistency - expected_consistency))

        # Sample size factor (more data = higher confidence, diminishing returns)
        sample_factor = min(1.0, len(candles) / 20)

        # Combine into confidence score
        confidence = Decimal(str(
            0.5 * consistency_alignment
            + 0.3 * sample_factor
            + 0.2 * (1 - min(1.0, volatility * 10))  # Lower vol = more confident
        ))
        confidence = max(Decimal("0.1"), min(Decimal("1.0"), confidence))

        return MomentumSignal(
            probability_up=Decimal(str(round(prob_up, 4))),
            confidence=Decimal(str(round(float(confidence), 4))),
            momentum_score=Decimal(str(round(z_score, 4))),
            volatility=Decimal(str(round(volatility, 6))),
            trend_consistency=Decimal(str(round(trend_consistency, 4))),
            sample_size=len(candles),
            asset=asset,
            interval_minutes=interval_minutes,
            timestamp=datetime.now(UTC),
        )

    @staticmethod
    def _normal_cdf(z: float) -> float:
        """
        Approximate normal CDF using error function.

        For standard normal: P(X <= z) = 0.5 * (1 + erf(z / sqrt(2)))
        """
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))


async def calculate_momentum_probability(
    klines_client: BinanceKlinesClient,
    asset: str,
    market_timeframe: int = 15,
    lookback_periods: int = 10,
) -> tuple[Decimal, Decimal] | None:
    """
    Calculate momentum-based probability for an asset.

    Args:
        klines_client: Connected BinanceKlinesClient
        asset: Asset name (e.g., "BTC", "ETH", "SOL")
        market_timeframe: Market timeframe in minutes (e.g., 15 for 15m markets)
        lookback_periods: Number of candles to analyze

    Returns:
        Tuple of (probability_up, confidence) or None on error
    """
    candles = await klines_client.get_recent_candles(
        asset=asset,
        interval_minutes=market_timeframe,
        count=lookback_periods,
    )

    if not candles:
        logger.warning(f"No candles fetched for {asset}")
        return None

    calculator = MomentumCalculator()
    signal = calculator.calculate_from_candles(
        candles=candles,
        asset=asset,
        interval_minutes=market_timeframe,
    )

    if signal is None:
        return None

    return signal.probability_up, signal.confidence


async def enhanced_momentum_probability(
    klines_client: BinanceKlinesClient,
    asset: str,
    market_timeframe: int = 15,
) -> tuple[Decimal, Decimal, MomentumSignal | None]:
    """
    Calculate enhanced momentum probability using multiple factors.

    Combines:
    1. Short-term momentum (5 candles) - 40% weight
    2. Medium-term momentum (20 candles) - 30% weight
    3. Higher timeframe trend (4x interval, 5 candles) - 30% weight

    Args:
        klines_client: Connected BinanceKlinesClient
        asset: Asset name
        market_timeframe: Market timeframe in minutes

    Returns:
        Tuple of (probability_up, confidence, primary_signal)
    """
    calculator = MomentumCalculator()
    factors: list[tuple[Decimal, Decimal, float]] = []  # (prob, conf, weight)

    # Factor 1: Short-term momentum (40% weight)
    short_candles = await klines_client.get_recent_candles(
        asset=asset,
        interval_minutes=market_timeframe,
        count=5,
    )
    primary_signal = None
    if short_candles:
        signal = calculator.calculate_from_candles(
            short_candles, asset, market_timeframe
        )
        if signal:
            factors.append((signal.probability_up, signal.confidence, 0.4))
            primary_signal = signal

    # Factor 2: Medium-term momentum (30% weight)
    medium_candles = await klines_client.get_recent_candles(
        asset=asset,
        interval_minutes=market_timeframe,
        count=20,
    )
    if medium_candles:
        signal = calculator.calculate_from_candles(
            medium_candles, asset, market_timeframe
        )
        if signal:
            factors.append((signal.probability_up, signal.confidence, 0.3))

    # Factor 3: Higher timeframe trend (30% weight)
    # Use 4x the market timeframe (e.g., 1h for 15m market)
    higher_tf = market_timeframe * 4
    # Map to valid Binance intervals
    valid_intervals = [1, 3, 5, 15, 30, 60, 240, 1440]
    higher_tf = min(valid_intervals, key=lambda x: abs(x - higher_tf))

    higher_candles = await klines_client.get_recent_candles(
        asset=asset,
        interval_minutes=higher_tf,
        count=5,
    )
    if higher_candles:
        signal = calculator.calculate_from_candles(
            higher_candles, asset, higher_tf
        )
        if signal:
            factors.append((signal.probability_up, signal.confidence, 0.3))

    if not factors:
        logger.warning(f"No factors calculated for {asset}")
        return Decimal("0.5"), Decimal("0"), None

    # Calculate weighted average probability and confidence
    total_weight = sum(w for _, _, w in factors)
    probability = sum(float(p) * w for p, _, w in factors) / total_weight
    confidence = sum(float(c) * w for _, c, w in factors) / total_weight

    return (
        Decimal(str(round(probability, 4))),
        Decimal(str(round(confidence, 4))),
        primary_signal,
    )
