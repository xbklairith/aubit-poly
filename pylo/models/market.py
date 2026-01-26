"""Market data models."""

from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class Platform(str, Enum):
    """Supported trading platforms."""

    POLYMARKET = "polymarket"
    KALSHI = "kalshi"
    LIMITLESS = "limitless"
    PREDICTIT = "predictit"
    METACULUS = "metaculus"
    BINANCE = "binance"
    COINBASE = "coinbase"
    DERIBIT = "deribit"
    YAHOO = "yahoo"


class MarketOutcome(BaseModel):
    """A single outcome in a market (e.g., YES or NO)."""

    id: str
    name: str
    price: Decimal = Field(ge=0, le=1)
    volume_24h: Decimal = Decimal("0")
    liquidity: Decimal = Decimal("0")

    # Order book data (optional)
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    bid_depth: Decimal | None = None
    ask_depth: Decimal | None = None


class Market(BaseModel):
    """A prediction market with its outcomes."""

    id: str
    platform: Platform
    name: str
    description: str = ""
    category: str = ""

    # Outcomes (typically YES/NO for binary markets)
    outcomes: list[MarketOutcome] = Field(default_factory=list)

    # Metadata
    volume_24h: Decimal = Decimal("0")
    liquidity: Decimal = Decimal("0")
    created_at: datetime | None = None
    end_date: datetime | None = None
    resolved: bool = False
    resolution: str | None = None

    # Source URL for reference
    url: str = ""

    # Raw data from API (for debugging)
    raw: dict | None = None

    # Price freshness tracking
    fetched_at: datetime = Field(default_factory=datetime.utcnow)

    @property
    def yes_price(self) -> Decimal | None:
        """Get YES outcome price."""
        for outcome in self.outcomes:
            if outcome.name.upper() in ("YES", "TRUE", "1"):
                return outcome.price
        return self.outcomes[0].price if self.outcomes else None

    @property
    def no_price(self) -> Decimal | None:
        """Get NO outcome price."""
        for outcome in self.outcomes:
            if outcome.name.upper() in ("NO", "FALSE", "0"):
                return outcome.price
        return self.outcomes[1].price if len(self.outcomes) > 1 else None

    @property
    def spread(self) -> Decimal | None:
        """Calculate the bid-ask spread sum for arbitrage detection."""
        yes = self.yes_price
        no = self.no_price
        if yes is not None and no is not None:
            return yes + no
        return None

    @property
    def is_arbitrageable(self) -> bool:
        """Check if internal arbitrage exists (YES + NO < 1)."""
        spread = self.spread
        return spread is not None and spread < Decimal("1")

    @property
    def is_binary(self) -> bool:
        """Check if this is a binary YES/NO market."""
        return len(self.outcomes) == 2

    def is_stale(self, max_age_seconds: int = 30) -> bool:
        """Check if market data is too old to be reliable."""
        age = datetime.utcnow() - self.fetched_at
        return age > timedelta(seconds=max_age_seconds)

    @property
    def is_expiring_soon(self) -> bool:
        """Check if market resolves within 1 hour (too risky for arbitrage)."""
        if not self.end_date:
            return False
        # Handle both timezone-aware and naive datetimes
        now = datetime.utcnow()
        end = self.end_date
        # If end_date is timezone-aware, make comparison work
        if end.tzinfo is not None:
            end = end.replace(tzinfo=None)
        return end < now + timedelta(hours=1)

    @property
    def yes_ask_price(self) -> Decimal | None:
        """Get YES outcome ask price (for buying). Falls back to price if no ask."""
        # Check for explicit YES/UP outcome names
        for outcome in self.outcomes:
            if outcome.name.upper() in ("YES", "TRUE", "1", "UP"):
                return outcome.best_ask if outcome.best_ask else outcome.price
        # Fallback to first outcome
        if self.outcomes:
            o = self.outcomes[0]
            return o.best_ask if o.best_ask else o.price
        return None

    @property
    def no_ask_price(self) -> Decimal | None:
        """Get NO outcome ask price (for buying). Falls back to price if no ask."""
        # Check for explicit NO/DOWN outcome names
        for outcome in self.outcomes:
            if outcome.name.upper() in ("NO", "FALSE", "0", "DOWN"):
                return outcome.best_ask if outcome.best_ask else outcome.price
        # Fallback to second outcome
        if len(self.outcomes) > 1:
            o = self.outcomes[1]
            return o.best_ask if o.best_ask else o.price
        return None


class CryptoPrice(BaseModel):
    """Cryptocurrency price data."""

    symbol: str  # e.g., "BTC/USDT"
    platform: Platform
    price: Decimal
    timestamp: datetime

    # For futures/options
    expiry: datetime | None = None
    strike: Decimal | None = None
    option_type: str | None = None  # "call" or "put"

    # Implied probability (for options)
    implied_probability: Decimal | None = None


class StockData(BaseModel):
    """Stock/options data."""

    symbol: str  # e.g., "AAPL"
    platform: Platform
    price: Decimal
    timestamp: datetime

    # Options data
    expiry: datetime | None = None
    strike: Decimal | None = None
    option_type: str | None = None
    implied_volatility: Decimal | None = None
    delta: Decimal | None = None  # Rough probability proxy
