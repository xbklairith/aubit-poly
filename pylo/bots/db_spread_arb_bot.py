"""Database-backed spread arbitrage bot.

This module replaces API-based market monitoring with PostgreSQL database reads.
Markets and orderbooks are populated by Rust services (market-scanner, orderbook-stream).
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Optional

from pylo.bots.db_market_monitor import DBMarketMonitor
from pylo.bots.dry_run_executor import DryRunExecutor
from pylo.bots.models import BotSession, UpDownMarket
from pylo.bots.position_tracker import PositionTracker
from pylo.bots.spread_detector import SpreadDetector
from pylo.config.settings import get_settings
from pylo.db.connection import Database, get_database
from pylo.db.queries import (
    create_position,
    get_open_positions,
    get_position_by_market,
    record_trade,
)

logger = logging.getLogger(__name__)


class BotState(str, Enum):
    """Bot state machine states."""

    IDLE = "idle"
    SCANNING = "scanning"
    TRADING = "trading"
    STOPPING = "stopping"


class DBSpreadArbBot:
    """Database-backed spread arbitrage bot.

    This bot reads markets and orderbooks from PostgreSQL instead of
    calling Polymarket APIs directly. This enables the polyglot architecture:
    - Rust services write market/orderbook data to PostgreSQL
    - Python trade executor reads from PostgreSQL
    """

    def __init__(self, fresh_session: bool = False) -> None:
        """Initialize the bot.

        Args:
            fresh_session: If True, starts a fresh session instead of
                          restoring previous state.
        """
        self.settings = get_settings()
        self.state = BotState.IDLE
        self._running = False
        self._shutdown_event = asyncio.Event()

        # Database connection
        self._db: Optional[Database] = None

        # Try to load previous session unless fresh start requested
        self.session: BotSession
        if not fresh_session:
            temp_tracker = PositionTracker(BotSession())
            previous = temp_tracker.load_previous_session()
            if previous:
                self.session = previous
                self.session.ended_at = None
                logger.info(
                    f"Restored session: Balance ${previous.current_balance:,.2f}, "
                    f"P/L ${previous.net_profit:+,.2f}"
                )
            else:
                self.session = BotSession(
                    dry_run=self.settings.spread_bot_dry_run,
                    starting_balance=self.settings.spread_bot_starting_balance,
                    current_balance=self.settings.spread_bot_starting_balance,
                )
        else:
            self.session = BotSession(
                dry_run=self.settings.spread_bot_dry_run,
                starting_balance=self.settings.spread_bot_starting_balance,
                current_balance=self.settings.spread_bot_starting_balance,
            )

        self.monitor: Optional[DBMarketMonitor] = None
        self.detector = SpreadDetector()
        self.executor = DryRunExecutor(self.session)
        self.tracker = PositionTracker(self.session)

        # Cached markets
        self._markets: dict[str, UpDownMarket] = {}

    def _print_banner(self) -> None:
        """Print startup banner."""
        mode = "DRY RUN" if self.settings.spread_bot_dry_run else "LIVE"
        assets = ", ".join(self.settings.get_spread_bot_assets())
        max_expiry_mins = self.settings.spread_bot_max_time_to_expiry // 60

        is_restored = self.session.net_profit != 0 or self.session.total_trades > 0
        session_status = "RESTORED" if is_restored else "NEW"

        print(f"""
═══════════════════════════════════════════════════════════════
  DB SPREAD ARBITRAGE BOT - {mode} MODE ({session_status} SESSION)
═══════════════════════════════════════════════════════════════
  Data Source: PostgreSQL (via Rust services)
  Assets:      {assets}
  Max expiry:  {max_expiry_mins} minutes
  Min profit:  {self.settings.spread_bot_min_profit * 100:.1f}%
  Max position: ${self.settings.spread_bot_max_position_size:,.0f}
  Max exposure: ${self.settings.spread_bot_max_total_exposure:,.0f}
  Poll interval: {self.settings.spread_bot_poll_interval}s
  Balance:     ${self.session.current_balance:,.2f}
  P/L:         ${self.session.net_profit:+,.2f} ({self.session.total_trades} trades)
═══════════════════════════════════════════════════════════════
""")

    async def start(self) -> None:
        """Start the bot."""
        self._print_banner()
        self._running = True
        self.state = BotState.SCANNING

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._handle_shutdown)

        logger.info("Bot starting...")

        # Initialize database connection
        self._db = get_database()
        self.monitor = DBMarketMonitor(self._db)

        # Initial market discovery
        await self._discover_markets()

        # Main loop
        while self._running and not self._shutdown_event.is_set():
            try:
                await self._run_cycle()
                await asyncio.sleep(self.settings.spread_bot_poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(5)

        # Shutdown
        await self._shutdown()

    def _handle_shutdown(self) -> None:
        """Handle shutdown signal."""
        logger.info("\nShutdown signal received...")
        self._running = False
        self._shutdown_event.set()

    async def _shutdown(self) -> None:
        """Clean shutdown."""
        self.state = BotState.STOPPING
        logger.info("Shutting down...")

        # Save session
        self.tracker.save_session()
        self.tracker.print_session_summary()

        logger.info("Bot stopped.")

    async def _discover_markets(self, force_refresh: bool = False) -> None:
        """Discover available markets from database."""
        if not self.monitor:
            return

        markets = await self.monitor.discover_markets(force_refresh=force_refresh)
        self._markets = {m.id: m for m in markets}

        # Print discovered markets
        now = datetime.now(timezone.utc)
        print(f"\n[{now.strftime('%H:%M:%S')}] Monitoring {len(markets)} markets (from DB):")
        for market in sorted(markets, key=lambda m: m.end_time)[:10]:
            time_left = market.time_to_expiry
            if time_left > 0:
                hours = int(time_left // 3600)
                minutes = int((time_left % 3600) // 60)
                time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
                print(f"  - {market.name} (ends in {time_str})")

        if len(markets) > 10:
            print(f"  ... and {len(markets) - 10} more")

    async def _run_cycle(self) -> None:
        """Run one scan cycle."""
        if not self.monitor:
            return

        self.state = BotState.SCANNING
        now = datetime.now(timezone.utc)

        # Update prices from DB orderbook snapshots (single batch query)
        await self.monitor.update_all_prices(self._markets)

        # Track expired markets before removing
        expired_market_ids = [k for k, v in self._markets.items() if v.is_expired]
        expired_market_names = [self._markets[k].name for k in expired_market_ids]

        # Remove expired markets
        self._markets = {k: v for k, v in self._markets.items() if not v.is_expired}

        # Report expired markets
        if expired_market_names:
            print(f"\n[{now.strftime('%H:%M:%S')}] ⏰ Markets expired:")
            for name in expired_market_names:
                print(f"  - {name}")

        # Check for expired positions and settle them
        expired_positions = self.executor.check_expired_positions(self._markets)
        for position in expired_positions:
            print(f"\n[{now.strftime('%H:%M:%S')}] 💰 POSITION SETTLED")
            print(f"  Market: {position.market_name}")
            print(f"  Entry: ${position.total_invested:.2f}")
            print(f"  Shares: {position.yes_shares:.2f} YES + {position.no_shares:.2f} NO")

            await self.executor.settle_position(position, "YES")

            payout = position.yes_shares
            profit = payout - position.total_invested
            print(f"  Payout: ${payout:.2f}")
            print(f"  Profit: ${profit:+.2f}")

        # Re-discover markets if some expired
        if expired_market_ids:
            print(f"\n[{now.strftime('%H:%M:%S')}] 🔄 Refreshing market list from DB...")
            old_ids = set(self._markets.keys())
            await self._discover_markets(force_refresh=True)
            new_ids = set(self._markets.keys()) - old_ids

            if new_ids:
                print(f"[{now.strftime('%H:%M:%S')}] ✅ Added {len(new_ids)} new markets:")
                for mid in new_ids:
                    market = self._markets.get(mid)
                    if market:
                        time_left = market.time_to_expiry
                        hours = int(time_left // 3600)
                        minutes = int((time_left % 3600) // 60)
                        time_str = f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
                        print(f"  + {market.name} (ends in {time_str})")

        # Scan for opportunities
        active_markets = [m for m in self._markets.values() if not m.is_expired]
        opportunities = self.detector.scan_markets(active_markets)

        # Log opportunities
        for opp in opportunities:
            self.tracker.log_opportunity(opp)

        # Trade on best opportunity if we can
        if opportunities:
            best = opportunities[0]
            self.state = BotState.TRADING

            # Check if we already have a position in this market
            existing = self.executor.get_position_for_market(best.market.id)
            if existing:
                logger.debug(f"Already have position in {best.market.name}")
            else:
                # Determine position size
                size = min(
                    self.settings.spread_bot_max_position_size,
                    self.executor.available_balance,
                )

                if size >= Decimal("10"):  # Minimum $10 trade
                    position = await self.executor.execute_spread_trade(best, size)
                    if position:
                        self.tracker.log_position(position)

                        # Also record position in PostgreSQL for persistence
                        await self._persist_position_to_db(position, best)

        # Periodic status update
        self._print_status()

        # Periodic save
        self.tracker.save_current_state()

    async def _persist_position_to_db(self, position, opportunity) -> None:
        """Persist a position to the PostgreSQL database.

        Args:
            position: The Position object from dry-run executor.
            opportunity: The SpreadOpportunity that triggered the trade.
        """
        if not self._db:
            return

        try:
            # Get the DB UUID for this market
            db_uuid = self.monitor._market_id_map.get(position.market_id) if self.monitor else None
            if not db_uuid:
                logger.warning(f"No DB UUID found for market {position.market_id}")
                return

            async with self._db.session() as session:
                # Create position in DB
                db_position = await create_position(
                    session,
                    market_id=db_uuid,
                    yes_shares=position.yes_shares,
                    no_shares=position.no_shares,
                    total_invested=position.total_invested,
                    is_dry_run=self.session.dry_run,
                )

                # Record trades
                await record_trade(
                    session,
                    position_id=db_position.id,
                    side="yes",
                    action="buy",
                    price=opportunity.yes_price,
                    shares=position.yes_shares,
                )
                await record_trade(
                    session,
                    position_id=db_position.id,
                    side="no",
                    action="buy",
                    price=opportunity.no_price,
                    shares=position.no_shares,
                )

                logger.info(f"Position persisted to DB: {db_position.id}")

        except Exception as e:
            logger.error(f"Failed to persist position to DB: {e}")

    def _print_status(self) -> None:
        """Print current status with market analysis."""
        now = datetime.now(timezone.utc)
        summary = self.executor.get_summary()
        min_profit_threshold = self.settings.spread_bot_min_profit

        # Print market analysis
        print(f"\n[{now.strftime('%H:%M:%S')}] 📊 Market Analysis (from DB):")
        print(
            f"  {'Type':<8} {'Market':<40} {'YES':>6} {'NO':>6} "
            f"{'Spread':>7} {'Profit':>7} {'Decision'}"
        )
        print(f"  {'-'*8} {'-'*40} {'-'*6} {'-'*6} {'-'*7} {'-'*7} {'-'*10}")

        near_profitable_count = 0
        for market in sorted(self._markets.values(), key=lambda m: m.end_time):
            if market.is_expired:
                continue

            yes_ask = market.yes_ask
            no_ask = market.no_ask
            spread = yes_ask + no_ask
            profit_pct = Decimal("1.00") - spread

            # Only show markets with spread < 1.1 (near profitable)
            if spread >= Decimal("1.1"):
                continue
            near_profitable_count += 1

            # Determine decision reason
            if yes_ask <= 0 or no_ask <= 0:
                decision = "❌ No price"
            elif profit_pct >= min_profit_threshold:
                decision = "✅ TRADE!"
            elif profit_pct > 0:
                decision = f"⏳ +{profit_pct*100:.1f}% < {min_profit_threshold*100:.0f}%"
            else:
                decision = f"❌ -{abs(profit_pct)*100:.1f}%"

            type_abbrev = {
                "up_down": "UP/DOWN",
                "above": "ABOVE",
                "price_range": "RANGE",
                "sports": "SPORTS",
            }.get(market.market_type.value, "OTHER")

            name = market.name[:38] + ".." if len(market.name) > 40 else market.name

            print(
                f"  {type_abbrev:<8} "
                f"{name:<40} "
                f"${yes_ask:.2f} "
                f"${no_ask:.2f} "
                f"${spread:.3f} "
                f"{profit_pct*100:+.1f}% "
                f"{decision}"
            )

        if near_profitable_count == 0:
            print("  (no markets with spread < 1.1)")
        print()

        # Status summary line
        status_line = (
            f"[{now.strftime('%H:%M:%S')}] "
            f"Balance: ${summary['balance']:,.2f} | "
            f"Positions: {summary['open_positions']} | "
            f"Trades: {summary['total_trades']} | "
            f"P/L: ${summary['net_profit']:+,.2f} ({summary['return_pct']:+.2f}%)"
        )
        print(status_line)

    async def run_once(self) -> None:
        """Run a single scan cycle (for testing)."""
        self._db = get_database()
        self.monitor = DBMarketMonitor(self._db)

        await self._discover_markets()
        await self._run_cycle()

        self.tracker.print_session_summary()


async def run_db_bot() -> None:
    """Run the database-backed spread arbitrage bot."""
    bot = DBSpreadArbBot()
    await bot.start()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    asyncio.run(run_db_bot())
