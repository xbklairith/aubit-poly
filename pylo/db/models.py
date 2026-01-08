"""SQLAlchemy ORM models matching the Rust/SQL schema."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Numeric, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Market(Base):
    """Prediction market from the database.

    Populated by the Rust market-scanner service.
    """

    __tablename__ = "markets"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    condition_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    market_type: Mapped[str] = mapped_column(String(50), nullable=False)
    asset: Mapped[str] = mapped_column(String(10), nullable=False)
    timeframe: Mapped[str] = mapped_column(String(20), nullable=False)
    yes_token_id: Mapped[str] = mapped_column(String(255), nullable=False)
    no_token_id: Mapped[str] = mapped_column(String(255), nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    end_time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    discovered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    # Relationships
    orderbook_snapshots: Mapped[list["OrderbookSnapshot"]] = relationship(
        back_populates="market"
    )
    positions: Mapped[list["Position"]] = relationship(back_populates="market")

    def __repr__(self) -> str:
        return f"<Market {self.condition_id[:8]}... {self.asset} {self.market_type}>"


class OrderbookSnapshot(Base):
    """Orderbook snapshot from the database.

    Populated by the Rust orderbook-stream service.
    """

    __tablename__ = "orderbook_snapshots"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("markets.id"), nullable=False
    )

    # Best prices (computed from depth)
    yes_best_ask: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    yes_best_bid: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    no_best_ask: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    no_best_bid: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))
    spread: Mapped[Decimal | None] = mapped_column(Numeric(10, 6))

    # Full orderbook depth (JSONB arrays)
    yes_asks: Mapped[Any | None] = mapped_column(JSONB)
    yes_bids: Mapped[Any | None] = mapped_column(JSONB)
    no_asks: Mapped[Any | None] = mapped_column(JSONB)
    no_bids: Mapped[Any | None] = mapped_column(JSONB)

    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    # Relationships
    market: Mapped["Market"] = relationship(back_populates="orderbook_snapshots")

    def __repr__(self) -> str:
        return f"<OrderbookSnapshot {self.id} market={self.market_id}>"

    @property
    def has_spread_opportunity(self) -> bool:
        """Check if there's a potential spread opportunity (sum < 1.0)."""
        if self.yes_best_ask is None or self.no_best_ask is None:
            return False
        total_cost = self.yes_best_ask + self.no_best_ask
        return total_cost < Decimal("1.0")


class Position(Base):
    """Trading position from the database."""

    __tablename__ = "positions"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    market_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("markets.id"), nullable=False
    )
    yes_shares: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    no_shares: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    total_invested: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    status: Mapped[str] = mapped_column(String(20), default="open")
    is_dry_run: Mapped[bool] = mapped_column(Boolean, default=True)
    opened_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    # Relationships
    market: Mapped["Market"] = relationship(back_populates="positions")
    trades: Mapped[list["Trade"]] = relationship(back_populates="position")

    def __repr__(self) -> str:
        return f"<Position {self.id} {self.status} yes={self.yes_shares} no={self.no_shares}>"

    @property
    def is_open(self) -> bool:
        return self.status == "open"

    @property
    def expected_payout(self) -> Decimal:
        """Expected payout on resolution (1 share = $1)."""
        return max(self.yes_shares, self.no_shares)

    @property
    def profit_if_resolved(self) -> Decimal:
        """Profit if market resolves."""
        return self.expected_payout - self.total_invested


class Trade(Base):
    """Trade execution record."""

    __tablename__ = "trades"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    position_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), ForeignKey("positions.id"), nullable=False
    )
    side: Mapped[str] = mapped_column(String(10), nullable=False)  # 'yes' or 'no'
    action: Mapped[str] = mapped_column(String(10), nullable=False)  # 'buy' or 'sell'
    price: Mapped[Decimal] = mapped_column(Numeric(10, 6), nullable=False)
    shares: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("NOW()")
    )

    # Relationships
    position: Mapped["Position"] = relationship(back_populates="trades")

    def __repr__(self) -> str:
        return f"<Trade {self.action} {self.shares} {self.side} @ {self.price}>"
