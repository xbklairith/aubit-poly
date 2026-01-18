"""Data models for the spread arbitrage bot."""

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum


class Asset(str, Enum):
    """Supported assets for Up/Down markets."""

    BTC = "BTC"
    ETH = "ETH"
    SOL = "SOL"
    XRP = "XRP"
    SPORTS = "SPORTS"  # For sports markets
    OTHER = "OTHER"


class Timeframe(str, Enum):
    """Market timeframes."""

    FIVE_MIN = "5min"
    FIFTEEN_MIN = "15min"
    HOURLY = "hourly"
    FOUR_HOUR = "4h"
    DAILY = "daily"
    EVENT = "event"  # For one-time events like sports games


class MarketType(str, Enum):
    """Type of binary market."""

    UP_DOWN = "up_down"        # Crypto Up or Down
    ABOVE = "above"            # Crypto above price level
    PRICE_RANGE = "price_range"  # Crypto between price range
    SPORTS = "sports"          # Sports game outcomes
    BINARY = "binary"          # Generic two-outcome market (non-crypto)


class PositionStatus(str, Enum):
    """Position lifecycle status."""

    OPEN = "open"
    CLOSING = "closing"
    CLOSED = "closed"
    SETTLED = "settled"


@dataclass
class UpDownMarket:
    """Represents a Polymarket binary market (Up/Down, Above, Price Range, Sports)."""

    id: str
    name: str
    asset: Asset
    timeframe: Timeframe
    end_time: datetime
    yes_token_id: str
    no_token_id: str
    condition_id: str
    market_type: MarketType = MarketType.UP_DOWN  # Type of binary market

    # Current prices (updated on each poll)
    yes_ask: Decimal = Decimal("0")
    yes_bid: Decimal = Decimal("0")
    no_ask: Decimal = Decimal("0")
    no_bid: Decimal = Decimal("0")

    # Metadata
    volume: Decimal = Decimal("0")
    liquidity: Decimal = Decimal("0")
    fetched_at: datetime | None = None

    @property
    def spread(self) -> Decimal:
        """Total cost to buy both sides (YES ask + NO ask)."""
        return self.yes_ask + self.no_ask

    @property
    def profit_potential(self) -> Decimal:
        """Potential profit if spread < $1.00."""
        return Decimal("1.00") - self.spread

    @property
    def is_arbitrageable(self) -> bool:
        """Check if spread offers arbitrage opportunity."""
        return self.spread < Decimal("1.00")

    @property
    def time_to_expiry(self) -> float:
        """Seconds until market expires."""
        now = datetime.now(UTC)
        return (self.end_time - now).total_seconds()

    @property
    def is_expired(self) -> bool:
        """Check if market has expired."""
        return self.time_to_expiry <= 0

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "name": self.name,
            "asset": self.asset.value,
            "timeframe": self.timeframe.value,
            "end_time": self.end_time.isoformat(),
            "yes_token_id": self.yes_token_id,
            "no_token_id": self.no_token_id,
            "condition_id": self.condition_id,
            "yes_ask": str(self.yes_ask),
            "no_ask": str(self.no_ask),
            "spread": str(self.spread),
            "profit_potential": str(self.profit_potential),
            "volume": str(self.volume),
            "fetched_at": self.fetched_at.isoformat() if self.fetched_at else None,
        }


@dataclass
class SpreadOpportunity:
    """Detected spread arbitrage opportunity."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    market: UpDownMarket = field(default=None)  # type: ignore
    yes_price: Decimal = Decimal("0")
    no_price: Decimal = Decimal("0")
    spread: Decimal = Decimal("0")
    profit_pct: Decimal = Decimal("0")
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "market_id": self.market.id if self.market else None,
            "market_name": self.market.name if self.market else None,
            "yes_price": str(self.yes_price),
            "no_price": str(self.no_price),
            "spread": str(self.spread),
            "profit_pct": str(self.profit_pct),
            "detected_at": self.detected_at.isoformat(),
        }


@dataclass
class Trade:
    """A single trade (buy or sell)."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    market_id: str = ""
    market_name: str = ""
    side: str = ""  # "YES" or "NO"
    action: str = ""  # "BUY" or "SELL"
    price: Decimal = Decimal("0")
    amount: Decimal = Decimal("0")  # USD amount
    shares: Decimal = Decimal("0")  # Number of shares
    fee: Decimal = Decimal("0")
    dry_run: bool = True

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "market_id": self.market_id,
            "market_name": self.market_name,
            "side": self.side,
            "action": self.action,
            "price": str(self.price),
            "amount": str(self.amount),
            "shares": str(self.shares),
            "fee": str(self.fee),
            "dry_run": self.dry_run,
        }


@dataclass
class Position:
    """An open position in a market (both YES and NO sides)."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    market_id: str = ""
    market_name: str = ""
    asset: Asset = Asset.BTC
    end_time: datetime = field(default_factory=lambda: datetime.now(UTC))

    # Position details
    yes_shares: Decimal = Decimal("0")
    no_shares: Decimal = Decimal("0")
    yes_avg_price: Decimal = Decimal("0")
    no_avg_price: Decimal = Decimal("0")
    total_invested: Decimal = Decimal("0")

    # Status
    status: PositionStatus = PositionStatus.OPEN
    entry_time: datetime = field(default_factory=lambda: datetime.now(UTC))
    exit_time: datetime | None = None

    # Settlement
    settled_outcome: str | None = None  # "YES" or "NO"
    payout: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")

    # Associated trades
    trades: list[Trade] = field(default_factory=list)

    @property
    def unrealized_pnl(self) -> Decimal:
        """Unrealized P/L if market settled now."""
        if self.status == PositionStatus.SETTLED:
            return self.realized_pnl
        # For spread arb, payout is always max of (yes_shares, no_shares)
        guaranteed_payout = max(self.yes_shares, self.no_shares)
        return guaranteed_payout - self.total_invested

    @property
    def expected_payout(self) -> Decimal:
        """Expected payout on settlement."""
        # In spread arb, we hold equal YES and NO, so payout = shares
        return max(self.yes_shares, self.no_shares)

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "market_id": self.market_id,
            "market_name": self.market_name,
            "asset": self.asset.value,
            "end_time": self.end_time.isoformat(),
            "yes_shares": str(self.yes_shares),
            "no_shares": str(self.no_shares),
            "yes_avg_price": str(self.yes_avg_price),
            "no_avg_price": str(self.no_avg_price),
            "total_invested": str(self.total_invested),
            "status": self.status.value,
            "entry_time": self.entry_time.isoformat(),
            "exit_time": self.exit_time.isoformat() if self.exit_time else None,
            "settled_outcome": self.settled_outcome,
            "payout": str(self.payout),
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "trades": [t.to_dict() for t in self.trades],
        }


@dataclass
class ProbabilityEstimate:
    """Estimated probability with confidence metrics."""

    asset: str
    timeframe: Timeframe
    probability_up: Decimal
    probability_down: Decimal
    confidence: Decimal
    momentum_score: Decimal
    volatility: Decimal
    sample_size: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "asset": self.asset,
            "timeframe": self.timeframe.value,
            "probability_up": str(self.probability_up),
            "probability_down": str(self.probability_down),
            "confidence": str(self.confidence),
            "momentum_score": str(self.momentum_score),
            "volatility": str(self.volatility),
            "sample_size": self.sample_size,
            "timestamp": self.timestamp.isoformat(),
        }


@dataclass
class EdgeOpportunity:
    """Detected edge opportunity for trading."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    market: UpDownMarket = field(default=None)  # type: ignore
    probability_estimate: ProbabilityEstimate = field(default=None)  # type: ignore

    # Edge metrics
    edge: Decimal = Decimal("0")  # P(true) - P(market)
    expected_value: Decimal = Decimal("0")  # EV per dollar bet
    recommended_side: str = ""  # "UP" or "DOWN"
    recommended_size: Decimal = Decimal("0")  # As fraction of bankroll

    # Confidence
    raw_confidence: Decimal = Decimal("0")
    adjusted_confidence: Decimal = Decimal("0")  # After expiry adjustment

    # Metadata
    time_to_expiry_seconds: int = 0
    detected_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_tradeable(self) -> bool:
        """Check if opportunity should be traded."""
        return (
            self.recommended_side in ("UP", "DOWN")
            and self.expected_value > Decimal("0")
            and self.recommended_size > Decimal("0")
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "market_id": self.market.id if self.market else None,
            "market_name": self.market.name if self.market else None,
            "asset": self.market.asset.value if self.market else None,
            "edge": str(self.edge),
            "expected_value": str(self.expected_value),
            "recommended_side": self.recommended_side,
            "recommended_size": str(self.recommended_size),
            "raw_confidence": str(self.raw_confidence),
            "adjusted_confidence": str(self.adjusted_confidence),
            "time_to_expiry_seconds": self.time_to_expiry_seconds,
            "detected_at": self.detected_at.isoformat(),
            "probability_estimate": (
                self.probability_estimate.to_dict()
                if self.probability_estimate
                else None
            ),
        }


@dataclass
class BotSession:
    """A bot trading session."""

    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None
    dry_run: bool = True

    # Balance tracking
    starting_balance: Decimal = Decimal("10000")
    current_balance: Decimal = Decimal("10000")

    # Statistics
    total_trades: int = 0
    winning_trades: int = 0
    total_opportunities: int = 0
    positions_opened: int = 0
    positions_closed: int = 0

    # P/L
    gross_profit: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    net_profit: Decimal = Decimal("0")

    # Data
    trades: list[Trade] = field(default_factory=list)
    positions: list[Position] = field(default_factory=list)
    opportunities: list[SpreadOpportunity] = field(default_factory=list)

    @property
    def win_rate(self) -> float:
        """Calculate win rate."""
        if self.total_trades == 0:
            return 0.0
        return self.winning_trades / self.total_trades

    @property
    def return_pct(self) -> Decimal:
        """Calculate return percentage."""
        if self.starting_balance == 0:
            return Decimal("0")
        return (self.net_profit / self.starting_balance) * 100

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": self.id,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "dry_run": self.dry_run,
            "starting_balance": str(self.starting_balance),
            "current_balance": str(self.current_balance),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "win_rate": self.win_rate,
            "total_opportunities": self.total_opportunities,
            "positions_opened": self.positions_opened,
            "positions_closed": self.positions_closed,
            "gross_profit": str(self.gross_profit),
            "fees_paid": str(self.fees_paid),
            "net_profit": str(self.net_profit),
            "return_pct": str(self.return_pct),
            "trades": [t.to_dict() for t in self.trades],
            "positions": [p.to_dict() for p in self.positions],
            "opportunities": [o.to_dict() for o in self.opportunities],
        }
