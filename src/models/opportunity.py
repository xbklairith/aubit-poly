"""Arbitrage opportunity models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field

from src.models.market import Market, Platform


class ArbitrageType(str, Enum):
    """Types of arbitrage opportunities."""

    INTERNAL = "internal"  # YES + NO < $1 on same platform
    CROSS_PLATFORM = "cross_platform"  # Same event, different platforms
    HEDGING = "hedging"  # Prediction vs real-world market


class ArbitrageOpportunity(BaseModel):
    """A detected arbitrage opportunity."""

    id: str = Field(default_factory=lambda: "")
    type: ArbitrageType
    detected_at: datetime = Field(default_factory=datetime.utcnow)

    # Profit metrics
    profit_percentage: Decimal  # e.g., 0.02 = 2%
    profit_absolute: Decimal | None = None  # Absolute profit for given size
    recommended_size: Decimal | None = None

    # Markets involved
    markets: list[Market] = Field(default_factory=list)
    platforms: list[Platform] = Field(default_factory=list)

    # Trade details
    description: str = ""
    instructions: list[str] = Field(default_factory=list)

    # Risk factors
    liquidity_available: Decimal | None = None
    estimated_slippage: Decimal | None = None
    confidence: Decimal = Decimal("0.8")  # 0-1 confidence score

    # Status
    is_active: bool = True
    expires_at: datetime | None = None

    def __str__(self) -> str:
        """Human-readable representation."""
        return (
            f"[{self.type.value.upper()}] {self.profit_percentage:.2%} profit - "
            f"{self.description}"
        )


class InternalArbOpportunity(ArbitrageOpportunity):
    """Internal arbitrage: YES + NO < $1 on same market."""

    type: ArbitrageType = ArbitrageType.INTERNAL
    market: Market | None = None

    yes_price: Decimal = Decimal("0")
    no_price: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0")  # YES + NO

    @classmethod
    def from_market(cls, market: Market) -> "InternalArbOpportunity | None":
        """Create opportunity from a market if arbitrage exists."""
        yes = market.yes_price
        no = market.no_price

        if yes is None or no is None:
            return None

        total = yes + no
        if total >= Decimal("1"):
            return None

        profit = Decimal("1") - total

        return cls(
            market=market,
            markets=[market],
            platforms=[market.platform],
            yes_price=yes,
            no_price=no,
            total_cost=total,
            profit_percentage=profit,
            description=f"Buy YES@{yes:.3f} + NO@{no:.3f} = {total:.3f} on {market.name}",
            instructions=[
                f"1. Buy YES at ${yes:.4f}",
                f"2. Buy NO at ${no:.4f}",
                f"3. Total cost: ${total:.4f}",
                f"4. Guaranteed return: ${1:.4f}",
                f"5. Profit: ${profit:.4f} ({profit:.2%})",
            ],
        )


class CrossPlatformArbOpportunity(ArbitrageOpportunity):
    """Cross-platform arbitrage: same event, different prices."""

    type: ArbitrageType = ArbitrageType.CROSS_PLATFORM

    # Platform-specific prices
    platform_a: Platform | None = None
    platform_b: Platform | None = None
    price_a: Decimal = Decimal("0")  # e.g., YES on platform A
    price_b: Decimal = Decimal("0")  # e.g., NO on platform B

    @classmethod
    def from_markets(
        cls,
        market_a: Market,
        market_b: Market,
        event_name: str,
    ) -> "CrossPlatformArbOpportunity | None":
        """Create opportunity from two markets on different platforms."""
        yes_a = market_a.yes_price
        no_b = market_b.no_price

        if yes_a is None or no_b is None:
            return None

        total = yes_a + no_b
        if total >= Decimal("1"):
            return None

        profit = Decimal("1") - total

        return cls(
            markets=[market_a, market_b],
            platforms=[market_a.platform, market_b.platform],
            platform_a=market_a.platform,
            platform_b=market_b.platform,
            price_a=yes_a,
            price_b=no_b,
            profit_percentage=profit,
            description=(
                f"Buy YES@{yes_a:.3f} on {market_a.platform.value}, "
                f"NO@{no_b:.3f} on {market_b.platform.value} for '{event_name}'"
            ),
            instructions=[
                f"1. Buy YES on {market_a.platform.value} at ${yes_a:.4f}",
                f"2. Buy NO on {market_b.platform.value} at ${no_b:.4f}",
                f"3. Total cost: ${total:.4f}",
                "4. One position guaranteed to pay $1.00",
                f"5. Profit: ${profit:.4f} ({profit:.2%})",
            ],
        )


class HedgingArbOpportunity(ArbitrageOpportunity):
    """Hedging arbitrage: prediction market vs real-world instruments."""

    type: ArbitrageType = ArbitrageType.HEDGING

    # Prediction market side
    prediction_platform: Platform | None = None
    prediction_price: Decimal = Decimal("0")
    prediction_direction: str = ""  # "YES" or "NO"

    # Real-world hedge
    hedge_platform: Platform | None = None
    hedge_instrument: str = ""  # e.g., "BTC-31JAN25-100000-C"
    hedge_price: Decimal = Decimal("0")
    implied_probability: Decimal = Decimal("0")

    probability_discrepancy: Decimal = Decimal("0")
