"""Database query functions for markets, orderbooks, and positions."""

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pylo.db.models import Market, OrderbookSnapshot, Position, Trade


async def get_active_markets(session: AsyncSession) -> list[Market]:
    """Get all active markets ordered by end time.

    Args:
        session: Database session.

    Returns:
        List of active markets.
    """
    result = await session.execute(
        select(Market)
        .where(Market.is_active == True)  # noqa: E712
        .order_by(Market.end_time.asc())
    )
    return list(result.scalars().all())


async def get_markets_with_fresh_orderbooks(
    session: AsyncSession, max_age_seconds: int = 60
) -> list[Market]:
    """Get only markets that have fresh orderbook data.

    This is much faster than get_active_markets() because it only returns
    markets that are actively being streamed (have recent orderbook updates).

    Args:
        session: Database session.
        max_age_seconds: Maximum age of orderbook data in seconds.

    Returns:
        List of markets with fresh orderbook data.
    """
    from sqlalchemy import text

    query = text("""
        SELECT DISTINCT ON (m.id)
            m.id, m.condition_id, m.market_type, m.asset, m.timeframe,
            m.yes_token_id, m.no_token_id, m.name, m.end_time, m.is_active,
            m.discovered_at, m.updated_at
        FROM markets m
        INNER JOIN orderbook_snapshots o ON o.market_id = m.id
        WHERE m.is_active = true
          AND o.captured_at > NOW() - INTERVAL '120 seconds'
        ORDER BY m.id, o.captured_at DESC
    """)

    result = await session.execute(query)
    rows = result.fetchall()

    markets = []
    for row in rows:
        markets.append(Market(
            id=row.id,
            condition_id=row.condition_id,
            market_type=row.market_type,
            asset=row.asset,
            timeframe=row.timeframe,
            yes_token_id=row.yes_token_id,
            no_token_id=row.no_token_id,
            name=row.name,
            end_time=row.end_time,
            is_active=row.is_active,
            discovered_at=row.discovered_at,
            updated_at=row.updated_at,
        ))
    return markets


async def get_market_by_condition_id(
    session: AsyncSession, condition_id: str
) -> Market | None:
    """Get a market by its condition ID.

    Args:
        session: Database session.
        condition_id: Polymarket condition ID.

    Returns:
        Market if found, None otherwise.
    """
    result = await session.execute(
        select(Market).where(Market.condition_id == condition_id)
    )
    return result.scalar_one_or_none()


async def get_market_by_id(session: AsyncSession, market_id: UUID) -> Market | None:
    """Get a market by its UUID.

    Args:
        session: Database session.
        market_id: Market UUID.

    Returns:
        Market if found, None otherwise.
    """
    result = await session.execute(select(Market).where(Market.id == market_id))
    return result.scalar_one_or_none()


async def get_latest_orderbook(
    session: AsyncSession, market_id: UUID
) -> OrderbookSnapshot | None:
    """Get the latest orderbook snapshot for a market.

    Args:
        session: Database session.
        market_id: Market UUID.

    Returns:
        Latest orderbook snapshot if found, None otherwise.
    """
    result = await session.execute(
        select(OrderbookSnapshot)
        .where(OrderbookSnapshot.market_id == market_id)
        .order_by(OrderbookSnapshot.captured_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def get_markets_with_latest_orderbooks(
    session: AsyncSession,
    max_orderbook_age_seconds: int = 30,
) -> list[tuple[Market, OrderbookSnapshot | None]]:
    """Get all active markets with their latest orderbook snapshot.

    Uses a single optimized query with DISTINCT ON for performance.

    Args:
        session: Database session.
        max_orderbook_age_seconds: Maximum age of orderbook data in seconds (default: 30).

    Returns:
        List of (Market, OrderbookSnapshot | None) tuples.
    """
    from sqlalchemy import text

    # Single query to get markets with FRESH orderbooks only
    # Uses index on (market_id, captured_at DESC) for fast lookups
    query = text("""
        SELECT
            m.id, m.condition_id, m.market_type, m.asset, m.timeframe,
            m.yes_token_id, m.no_token_id, m.name, m.end_time, m.is_active,
            m.discovered_at, m.updated_at,
            o.id as snap_id, o.yes_best_ask, o.yes_best_bid,
            o.no_best_ask, o.no_best_bid, o.spread, o.captured_at
        FROM markets m
        INNER JOIN LATERAL (
            SELECT id, yes_best_ask, yes_best_bid, no_best_ask, no_best_bid, spread, captured_at
            FROM orderbook_snapshots
            WHERE market_id = m.id
              AND captured_at > NOW() - make_interval(secs => :max_age)
            ORDER BY captured_at DESC
            LIMIT 1
        ) o ON true
        WHERE m.is_active = true
        ORDER BY m.end_time ASC
    """)

    result = await session.execute(query, {"max_age": float(max_orderbook_age_seconds)})
    rows = result.fetchall()

    results: list[tuple[Market, OrderbookSnapshot | None]] = []
    for row in rows:
        # Build Market object
        market = Market(
            id=row.id,
            condition_id=row.condition_id,
            market_type=row.market_type,
            asset=row.asset,
            timeframe=row.timeframe,
            yes_token_id=row.yes_token_id,
            no_token_id=row.no_token_id,
            name=row.name,
            end_time=row.end_time,
            is_active=row.is_active,
            discovered_at=row.discovered_at,
            updated_at=row.updated_at,
        )

        # Build OrderbookSnapshot if exists
        snapshot = None
        if row.snap_id is not None:
            snapshot = OrderbookSnapshot(
                id=row.snap_id,
                market_id=row.id,
                yes_best_ask=row.yes_best_ask,
                yes_best_bid=row.yes_best_bid,
                no_best_ask=row.no_best_ask,
                no_best_bid=row.no_best_bid,
                spread=row.spread,
                captured_at=row.captured_at,
            )

        results.append((market, snapshot))

    return results


async def get_open_positions(session: AsyncSession) -> list[Position]:
    """Get all open positions.

    Args:
        session: Database session.

    Returns:
        List of open positions.
    """
    result = await session.execute(
        select(Position)
        .where(Position.status == "open")
        .order_by(Position.opened_at.desc())
    )
    return list(result.scalars().all())


async def get_position_by_market(
    session: AsyncSession, market_id: UUID
) -> Position | None:
    """Get open position for a market.

    Args:
        session: Database session.
        market_id: Market UUID.

    Returns:
        Open position if found, None otherwise.
    """
    result = await session.execute(
        select(Position)
        .where(Position.market_id == market_id)
        .where(Position.status == "open")
        .order_by(Position.opened_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


async def create_position(
    session: AsyncSession,
    market_id: UUID,
    yes_shares: Decimal,
    no_shares: Decimal,
    total_invested: Decimal,
    is_dry_run: bool = True,
) -> Position:
    """Create a new position.

    Args:
        session: Database session.
        market_id: Market UUID.
        yes_shares: Number of YES shares.
        no_shares: Number of NO shares.
        total_invested: Total amount invested.
        is_dry_run: Whether this is a dry run (simulated).

    Returns:
        Created position.
    """
    position = Position(
        market_id=market_id,
        yes_shares=yes_shares,
        no_shares=no_shares,
        total_invested=total_invested,
        is_dry_run=is_dry_run,
        status="open",
    )
    session.add(position)
    await session.flush()
    return position


async def close_position(
    session: AsyncSession, position_id: UUID, status: str = "closed"
) -> None:
    """Close a position.

    Args:
        session: Database session.
        position_id: Position UUID.
        status: New status (default: 'closed').
    """
    result = await session.execute(select(Position).where(Position.id == position_id))
    position = result.scalar_one_or_none()
    if position:
        position.status = status
        position.closed_at = datetime.now(timezone.utc)


async def record_trade(
    session: AsyncSession,
    position_id: UUID,
    side: str,
    action: str,
    price: Decimal,
    shares: Decimal,
) -> Trade:
    """Record a trade execution.

    Args:
        session: Database session.
        position_id: Position UUID.
        side: 'yes' or 'no'.
        action: 'buy' or 'sell'.
        price: Execution price.
        shares: Number of shares.

    Returns:
        Created trade record.
    """
    trade = Trade(
        position_id=position_id,
        side=side,
        action=action,
        price=price,
        shares=shares,
    )
    session.add(trade)
    await session.flush()
    return trade
