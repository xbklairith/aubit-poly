"""Edge detection by comparing estimated probability to market price."""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

from pylo.bots.models import UpDownMarket
from pylo.probability.momentum import MomentumSignal

logger = logging.getLogger(__name__)


def expiry_confidence_multiplier(
    time_to_expiry_seconds: int,
    market_duration_seconds: int = 900,  # 15min = 900s
) -> Decimal:
    """
    Calculate confidence multiplier based on time to expiry.

    Research-backed model:
    - Too early (>80% time left): 0.6x - trend not established yet
    - Sweet spot (40-80%): 1.0x - momentum exists, market hasn't priced in
    - Late (20-40%): 0.8x - market getting efficient
    - Near expiry (<20%): 0.4x - market is ~90-95% accurate
    - Very near (<7%): 0.2x - market already knows outcome

    Args:
        time_to_expiry_seconds: Seconds until market expires
        market_duration_seconds: Total market duration in seconds

    Returns:
        Confidence multiplier (0.0-1.0)
    """
    if market_duration_seconds <= 0:
        return Decimal("0.5")

    time_ratio = time_to_expiry_seconds / market_duration_seconds

    if time_ratio > 0.80:
        # Too early: trend not established
        return Decimal("0.6")
    elif time_ratio > 0.40:
        # Sweet spot: best edge opportunity
        return Decimal("1.0")
    elif time_ratio > 0.20:
        # Getting late: market converging
        return Decimal("0.8")
    elif time_ratio > 0.07:
        # Near expiry: market ~90% accurate
        return Decimal("0.4")
    else:
        # Very near expiry: market ~95% accurate
        return Decimal("0.2")


@dataclass
class EdgeSignal:
    """Detected edge between estimated probability and market price."""

    market_id: str
    market_name: str
    asset: str
    timeframe: str

    # Market prices
    market_yes_price: Decimal  # Current UP price
    market_no_price: Decimal   # Current DOWN price

    # Estimated probabilities
    estimated_prob_up: Decimal
    estimated_prob_down: Decimal

    # Edge metrics
    edge_up: Decimal   # P(true) - P(market) for UP
    edge_down: Decimal  # P(true) - P(market) for DOWN

    # Confidence and recommendation
    confidence: Decimal
    adjusted_confidence: Decimal  # After expiry adjustment
    recommended_side: Literal["UP", "DOWN", "NONE"]
    recommended_size: Decimal  # As fraction of bankroll (Kelly-adjusted)

    # Expected value
    ev_up: Decimal   # EV of buying UP
    ev_down: Decimal  # EV of buying DOWN

    # Metadata
    time_to_expiry_seconds: int
    expiry_multiplier: Decimal
    momentum_signal: MomentumSignal | None = None
    detected_at: datetime = field(
        default_factory=lambda: datetime.now(UTC)
    )

    @property
    def has_edge(self) -> bool:
        """Check if there's a tradeable edge."""
        return self.recommended_side != "NONE"

    @property
    def best_edge(self) -> Decimal:
        """Get the absolute value of the best edge."""
        return max(abs(self.edge_up), abs(self.edge_down))

    @property
    def best_ev(self) -> Decimal:
        """Get the best expected value."""
        return max(self.ev_up, self.ev_down)

    def to_dict(self) -> dict[str, object]:
        """Convert to dictionary for serialization."""
        return {
            "market_id": self.market_id,
            "market_name": self.market_name,
            "asset": self.asset,
            "timeframe": self.timeframe,
            "market_yes_price": str(self.market_yes_price),
            "market_no_price": str(self.market_no_price),
            "estimated_prob_up": str(self.estimated_prob_up),
            "estimated_prob_down": str(self.estimated_prob_down),
            "edge_up": str(self.edge_up),
            "edge_down": str(self.edge_down),
            "confidence": str(self.confidence),
            "adjusted_confidence": str(self.adjusted_confidence),
            "recommended_side": self.recommended_side,
            "recommended_size": str(self.recommended_size),
            "ev_up": str(self.ev_up),
            "ev_down": str(self.ev_down),
            "time_to_expiry_seconds": self.time_to_expiry_seconds,
            "expiry_multiplier": str(self.expiry_multiplier),
            "detected_at": self.detected_at.isoformat(),
        }


class EdgeDetector:
    """Detects edges between estimated probability and market prices."""

    def __init__(
        self,
        min_edge: Decimal = Decimal("0.05"),
        min_confidence: Decimal = Decimal("0.5"),
        fee_rate: Decimal = Decimal("0.02"),
        kelly_fraction: Decimal = Decimal("0.25"),
        max_position_pct: Decimal = Decimal("0.10"),
    ):
        """
        Initialize edge detector.

        Args:
            min_edge: Minimum edge (probability difference) to consider
            min_confidence: Minimum confidence score to trade
            fee_rate: Trading fee rate (e.g., 0.02 = 2%)
            kelly_fraction: Fraction of Kelly to use for sizing
            max_position_pct: Maximum position as % of bankroll
        """
        self.min_edge = min_edge
        self.min_confidence = min_confidence
        self.fee_rate = fee_rate
        self.kelly_fraction = kelly_fraction
        self.max_position_pct = max_position_pct
        self.logger = logging.getLogger(f"{__name__}.EdgeDetector")

    def detect_edge(
        self,
        market: UpDownMarket,
        estimated_prob_up: Decimal,
        confidence: Decimal,
        market_duration_seconds: int = 900,
        momentum_signal: MomentumSignal | None = None,
    ) -> EdgeSignal:
        """
        Detect edge between estimated probability and market price.

        Args:
            market: UpDownMarket with current prices
            estimated_prob_up: Estimated probability of UP
            confidence: Confidence in the estimate
            market_duration_seconds: Total market duration
            momentum_signal: Optional momentum signal for metadata

        Returns:
            EdgeSignal with edge analysis
        """
        # Market implied probabilities
        market_prob_up = market.yes_ask  # Price = implied probability
        market_prob_down = market.no_ask

        # Estimated probabilities
        estimated_prob_down = Decimal("1") - estimated_prob_up

        # Calculate raw edge
        edge_up = estimated_prob_up - market_prob_up
        edge_down = estimated_prob_down - market_prob_down

        # Time to expiry adjustment
        tte = int(market.time_to_expiry)
        expiry_mult = expiry_confidence_multiplier(tte, market_duration_seconds)
        adjusted_confidence = confidence * expiry_mult

        # Calculate EV for each side
        # EV(UP) = P(up) × $1.00 - UP_price
        # EV(DOWN) = P(down) × $1.00 - DOWN_price
        ev_up = estimated_prob_up - market_prob_up
        ev_down = estimated_prob_down - market_prob_down

        # Adjust EV for fees
        # Break-even: P(true) > Price / (1 - fee_rate)
        fee_adjusted_ev_up = ev_up - (market_prob_up * self.fee_rate)
        fee_adjusted_ev_down = ev_down - (market_prob_down * self.fee_rate)

        # Determine recommendation
        recommended_side: Literal["UP", "DOWN", "NONE"] = "NONE"
        recommended_size = Decimal("0")

        # Check if we meet thresholds
        if adjusted_confidence >= self.min_confidence:
            if edge_up >= self.min_edge and fee_adjusted_ev_up > 0:
                recommended_side = "UP"
                # Kelly formula for buying UP at price P with true probability p
                # Kelly = (p - P) / (1 - P)
                if market_prob_up < Decimal("1"):
                    kelly = (estimated_prob_up - market_prob_up) / (
                        Decimal("1") - market_prob_up
                    )
                    kelly = max(Decimal("0"), kelly)
                    recommended_size = min(
                        kelly * self.kelly_fraction * adjusted_confidence,
                        self.max_position_pct,
                    )

            elif edge_down >= self.min_edge and fee_adjusted_ev_down > 0:
                recommended_side = "DOWN"
                # Kelly for buying DOWN
                if market_prob_down < Decimal("1"):
                    kelly = (estimated_prob_down - market_prob_down) / (
                        Decimal("1") - market_prob_down
                    )
                    kelly = max(Decimal("0"), kelly)
                    recommended_size = min(
                        kelly * self.kelly_fraction * adjusted_confidence,
                        self.max_position_pct,
                    )

        return EdgeSignal(
            market_id=market.id,
            market_name=market.name,
            asset=market.asset.value,
            timeframe=market.timeframe.value,
            market_yes_price=market_prob_up,
            market_no_price=market_prob_down,
            estimated_prob_up=estimated_prob_up,
            estimated_prob_down=estimated_prob_down,
            edge_up=edge_up,
            edge_down=edge_down,
            confidence=confidence,
            adjusted_confidence=adjusted_confidence,
            recommended_side=recommended_side,
            recommended_size=recommended_size.quantize(Decimal("0.0001")),
            ev_up=fee_adjusted_ev_up,
            ev_down=fee_adjusted_ev_down,
            time_to_expiry_seconds=tte,
            expiry_multiplier=expiry_mult,
            momentum_signal=momentum_signal,
        )

    def filter_signals(
        self,
        signals: list[EdgeSignal],
        min_ev: Decimal = Decimal("0.01"),
    ) -> list[EdgeSignal]:
        """
        Filter edge signals to only include tradeable ones.

        Args:
            signals: List of EdgeSignal objects
            min_ev: Minimum expected value to include

        Returns:
            Filtered and sorted list of signals (best first)
        """
        filtered = [
            s for s in signals
            if s.has_edge and s.best_ev >= min_ev
        ]

        # Sort by expected value, highest first
        filtered.sort(key=lambda s: s.best_ev, reverse=True)

        return filtered
