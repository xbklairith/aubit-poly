"""Market data models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, Field


class Platform(str, Enum):
    """Supported trading platforms."""

    POLYMARKET = "polymarket"
    KALSHI = "kalshi"
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
