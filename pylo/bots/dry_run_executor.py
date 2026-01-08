"""Dry-run executor for simulated trading."""

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from pylo.bots.models import (
    BotSession,
    Position,
    PositionStatus,
    SpreadOpportunity,
    Trade,
    UpDownMarket,
)
from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)


class DryRunExecutor:
    """Simulates trading without real money."""

    def __init__(self, session: BotSession) -> None:
        self.settings = get_settings()
        self.session = session
        self.positions: dict[str, Position] = {}

    @property
    def available_balance(self) -> Decimal:
        """Calculate available balance (excluding open positions)."""
        total_exposure = sum(p.total_invested for p in self.positions.values() if p.status == PositionStatus.OPEN)
        return self.session.current_balance - total_exposure

    @property
    def total_exposure(self) -> Decimal:
        """Calculate total exposure in open positions."""
        return sum(p.total_invested for p in self.positions.values() if p.status == PositionStatus.OPEN)

    def can_trade(self, amount: Decimal) -> bool:
        """Check if we can execute a trade of the given amount."""
        # Check available balance
        if amount > self.available_balance:
            logger.warning(f"Insufficient balance: need ${amount:.2f}, have ${self.available_balance:.2f}")
            return False

        # Check max position size
        if amount > self.settings.spread_bot_max_position_size:
            logger.warning(f"Amount ${amount:.2f} exceeds max position size ${self.settings.spread_bot_max_position_size:.2f}")
            return False

        # Check max total exposure
        if self.total_exposure + amount > self.settings.spread_bot_max_total_exposure:
            logger.warning(f"Would exceed max exposure ${self.settings.spread_bot_max_total_exposure:.2f}")
            return False

        return True

    async def execute_spread_trade(
        self,
        opportunity: SpreadOpportunity,
        investment: Decimal,
    ) -> Optional[Position]:
        """Execute a spread arbitrage trade (buy both YES and NO).

        Args:
            opportunity: The opportunity to trade
            investment: Total USD to invest

        Returns:
            Position if successful, None otherwise
        """
        market = opportunity.market
        if not market:
            return None

        # Validate trade
        if not self.can_trade(investment):
            return None

        # Calculate trade details
        from pylo.bots.spread_detector import SpreadDetector

        detector = SpreadDetector()
        details = detector.calculate_trade_details(opportunity, investment)

        # Create trades
        yes_trade = Trade(
            market_id=market.id,
            market_name=market.name,
            side="YES",
            action="BUY",
            price=details["yes_price"],
            amount=details["yes_cost"],
            shares=details["yes_shares"],
            fee=details["fee"] / 2,  # Split fee between trades
            dry_run=True,
        )

        no_trade = Trade(
            market_id=market.id,
            market_name=market.name,
            side="NO",
            action="BUY",
            price=details["no_price"],
            amount=details["no_cost"],
            shares=details["no_shares"],
            fee=details["fee"] / 2,
            dry_run=True,
        )

        # Create position
        position = Position(
            market_id=market.id,
            market_name=market.name,
            asset=market.asset,
            end_time=market.end_time,
            yes_shares=details["yes_shares"],
            no_shares=details["no_shares"],
            yes_avg_price=details["yes_price"],
            no_avg_price=details["no_price"],
            total_invested=details["total_invested"],
            status=PositionStatus.OPEN,
            trades=[yes_trade, no_trade],
        )

        # Update session
        self.session.current_balance -= details["total_invested"]
        self.session.total_trades += 2
        self.session.positions_opened += 1
        self.session.trades.extend([yes_trade, no_trade])
        self.positions[position.id] = position

        # Log
        logger.info(
            f"\n[DRY RUN] TRADE EXECUTED\n"
            f"  Market: {market.name}\n"
            f"  → BUY {details['yes_shares']:.2f} YES @ ${details['yes_price']:.3f} = ${details['yes_cost']:.2f}\n"
            f"  → BUY {details['no_shares']:.2f} NO @ ${details['no_price']:.3f} = ${details['no_cost']:.2f}\n"
            f"  Total invested: ${details['total_invested']:.2f}\n"
            f"  Guaranteed payout: ${details['payout']:.2f}\n"
            f"  Expected profit: ${details['net_profit']:.2f} ({details['profit_pct']:.1f}%)\n"
        )

        return position

    async def settle_position(
        self,
        position: Position,
        outcome: str,
    ) -> None:
        """Settle a position when the market resolves.

        Args:
            position: The position to settle
            outcome: "YES" or "NO" - the winning side
        """
        if position.status == PositionStatus.SETTLED:
            return

        # Calculate payout
        if outcome.upper() == "YES":
            payout = position.yes_shares
        else:
            payout = position.no_shares

        # Calculate realized P/L
        realized_pnl = payout - position.total_invested

        # Update position
        position.status = PositionStatus.SETTLED
        position.exit_time = datetime.now(timezone.utc)
        position.settled_outcome = outcome.upper()
        position.payout = payout
        position.realized_pnl = realized_pnl

        # Update session
        self.session.current_balance += payout
        self.session.positions_closed += 1
        self.session.gross_profit += realized_pnl
        self.session.net_profit += realized_pnl

        if realized_pnl > 0:
            self.session.winning_trades += 1

        # Log
        logger.info(
            f"\n[DRY RUN] POSITION SETTLED\n"
            f"  Market: {position.market_name}\n"
            f"  Outcome: {outcome}\n"
            f"  Payout: ${payout:.2f}\n"
            f"  P/L: ${realized_pnl:+.2f}\n"
            f"  Balance: ${self.session.current_balance:.2f}\n"
        )

    def get_open_positions(self) -> list[Position]:
        """Get all open positions."""
        return [p for p in self.positions.values() if p.status == PositionStatus.OPEN]

    def get_position_for_market(self, market_id: str) -> Optional[Position]:
        """Get open position for a specific market."""
        for position in self.positions.values():
            if position.market_id == market_id and position.status == PositionStatus.OPEN:
                return position
        return None

    def check_expired_positions(self, markets: dict[str, UpDownMarket]) -> list[Position]:
        """Check for positions in expired markets.

        Returns:
            List of positions that need settlement
        """
        expired = []
        for position in self.get_open_positions():
            market = markets.get(position.market_id)
            if market and market.is_expired:
                expired.append(position)
            elif position.end_time < datetime.now(timezone.utc):
                expired.append(position)
        return expired

    def get_summary(self) -> dict:
        """Get summary of current state."""
        open_positions = self.get_open_positions()
        return {
            "balance": self.session.current_balance,
            "available_balance": self.available_balance,
            "total_exposure": self.total_exposure,
            "open_positions": len(open_positions),
            "total_trades": self.session.total_trades,
            "winning_trades": self.session.winning_trades,
            "net_profit": self.session.net_profit,
            "return_pct": self.session.return_pct,
        }
