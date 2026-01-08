"""Position tracker with JSON logging."""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from pylo.bots.models import BotSession, Position, SpreadOpportunity, Trade
from pylo.config.settings import get_settings

logger = logging.getLogger(__name__)


class PositionTracker:
    """Tracks positions and logs to JSON file."""

    def __init__(self, session: BotSession) -> None:
        self.settings = get_settings()
        self.session = session
        self.log_file = Path(self.settings.spread_bot_log_file)

        # Ensure directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def log_opportunity(self, opportunity: SpreadOpportunity) -> None:
        """Log a detected opportunity."""
        self.session.opportunities.append(opportunity)
        self.session.total_opportunities += 1

    def log_trade(self, trade: Trade) -> None:
        """Log a trade."""
        self.session.trades.append(trade)
        self.session.total_trades += 1

    def log_position(self, position: Position) -> None:
        """Log a position."""
        # Check if position already exists
        existing_ids = [p.id for p in self.session.positions]
        if position.id not in existing_ids:
            self.session.positions.append(position)
            self.session.positions_opened += 1

    def save_session(self) -> None:
        """Save the current session to JSON file."""
        try:
            # Update end time
            self.session.ended_at = datetime.now(timezone.utc)

            # Convert to dict
            session_data = self.session.to_dict()

            # Load existing data if file exists
            all_sessions = []
            if self.log_file.exists():
                try:
                    with open(self.log_file, "r") as f:
                        existing = json.load(f)
                        if isinstance(existing, list):
                            all_sessions = existing
                        else:
                            all_sessions = [existing]
                except (json.JSONDecodeError, ValueError):
                    pass

            # Add current session
            all_sessions.append(session_data)

            # Keep only last 100 sessions
            if len(all_sessions) > 100:
                all_sessions = all_sessions[-100:]

            # Save
            with open(self.log_file, "w") as f:
                json.dump(all_sessions, f, indent=2, default=str)

            logger.info(f"Session saved to {self.log_file}")

        except Exception as e:
            logger.error(f"Error saving session: {e}")

    def save_current_state(self) -> None:
        """Save current session state (for periodic saves)."""
        try:
            state_file = self.log_file.with_suffix(".state.json")
            session_data = self.session.to_dict()

            with open(state_file, "w") as f:
                json.dump(session_data, f, indent=2, default=str)

        except Exception as e:
            logger.debug(f"Error saving state: {e}")

    def load_previous_session(self) -> Optional[BotSession]:
        """Load the most recent session from state file (preferred) or JSON file."""
        # Try state file first (more recent, saved every cycle)
        state_file = self.log_file.with_suffix(".state.json")
        if state_file.exists():
            try:
                with open(state_file, "r") as f:
                    data = json.load(f)
                if isinstance(data, dict) and data.get("current_balance"):
                    return self._session_from_dict(data)
            except Exception as e:
                logger.debug(f"Error loading state file: {e}")

        # Fall back to session log
        if not self.log_file.exists():
            return None

        try:
            with open(self.log_file, "r") as f:
                data = json.load(f)

            if isinstance(data, list) and len(data) > 0:
                return self._session_from_dict(data[-1])
            elif isinstance(data, dict):
                return self._session_from_dict(data)

        except Exception as e:
            logger.error(f"Error loading session: {e}")

        return None

    def _session_from_dict(self, data: dict) -> BotSession:
        """Create a BotSession from dictionary."""
        from decimal import Decimal

        session = BotSession(
            id=data.get("id", ""),
            dry_run=data.get("dry_run", True),
            starting_balance=Decimal(str(data.get("starting_balance", 10000))),
            current_balance=Decimal(str(data.get("current_balance", 10000))),
            total_trades=data.get("total_trades", 0),
            winning_trades=data.get("winning_trades", 0),
            total_opportunities=data.get("total_opportunities", 0),
            positions_opened=data.get("positions_opened", 0),
            positions_closed=data.get("positions_closed", 0),
            gross_profit=Decimal(str(data.get("gross_profit", 0))),
            fees_paid=Decimal(str(data.get("fees_paid", 0))),
            net_profit=Decimal(str(data.get("net_profit", 0))),
        )

        if data.get("started_at"):
            session.started_at = datetime.fromisoformat(data["started_at"])
        if data.get("ended_at"):
            session.ended_at = datetime.fromisoformat(data["ended_at"])

        return session

    def get_all_time_stats(self) -> dict:
        """Get all-time statistics from all sessions."""
        if not self.log_file.exists():
            return {}

        try:
            with open(self.log_file, "r") as f:
                sessions = json.load(f)

            if not isinstance(sessions, list):
                sessions = [sessions]

            from decimal import Decimal

            total_trades = sum(s.get("total_trades", 0) for s in sessions)
            winning_trades = sum(s.get("winning_trades", 0) for s in sessions)
            total_profit = sum(Decimal(str(s.get("net_profit", 0))) for s in sessions)
            total_opportunities = sum(s.get("total_opportunities", 0) for s in sessions)

            return {
                "total_sessions": len(sessions),
                "total_trades": total_trades,
                "winning_trades": winning_trades,
                "win_rate": winning_trades / total_trades if total_trades > 0 else 0,
                "total_profit": total_profit,
                "total_opportunities": total_opportunities,
            }

        except Exception as e:
            logger.error(f"Error calculating stats: {e}")
            return {}

    def print_session_summary(self) -> None:
        """Print a formatted session summary."""
        summary = f"""
═══════════════════════════════════════════════════════════════
  SESSION SUMMARY {"(DRY RUN)" if self.session.dry_run else "(LIVE)"}
═══════════════════════════════════════════════════════════════
  Duration: {self._format_duration()}
  Starting balance: ${self.session.starting_balance:,.2f}
  Ending balance:   ${self.session.current_balance:,.2f}

  Opportunities detected: {self.session.total_opportunities}
  Positions opened:       {self.session.positions_opened}
  Positions closed:       {self.session.positions_closed}

  Total trades:    {self.session.total_trades}
  Winning trades:  {self.session.winning_trades}
  Win rate:        {self.session.win_rate * 100:.1f}%

  Gross profit:  ${self.session.gross_profit:+,.2f}
  Fees paid:     ${self.session.fees_paid:,.2f}
  Net profit:    ${self.session.net_profit:+,.2f}
  Return:        {self.session.return_pct:+.2f}%
═══════════════════════════════════════════════════════════════
"""
        print(summary)

    def _format_duration(self) -> str:
        """Format session duration."""
        end = self.session.ended_at or datetime.now(timezone.utc)
        duration = end - self.session.started_at
        hours = int(duration.total_seconds() // 3600)
        minutes = int((duration.total_seconds() % 3600) // 60)
        seconds = int(duration.total_seconds() % 60)

        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"
