"""Edge Trader Bot - Probability Gap Trading for Crypto Up/Down Markets.

This bot uses crypto asset price momentum to estimate the true probability
of UP/DOWN outcomes and trades when it differs from the market price.

Key concepts:
- Edge = P(true) - P(market)
- Trade when edge > threshold AND EV > 0 after fees
- Position sizing via fractional Kelly criterion
"""

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from pylo.bots.models import (
    BotSession,
    EdgeOpportunity,
    Position,
    ProbabilityEstimate,
    Timeframe,
    Trade,
    UpDownMarket,
)
from pylo.config.settings import get_settings
from pylo.data_sources.crypto.binance_klines import BinanceKlinesClient
from pylo.probability.edge_detector import EdgeDetector, EdgeSignal
from pylo.probability.historical import HistoricalCalibrator
from pylo.probability.momentum import MomentumCalculator, enhanced_momentum_probability

logger = logging.getLogger(__name__)


class EdgeTrader:
    """Bot that trades on probability gaps in crypto Up/Down markets."""

    def __init__(
        self,
        dry_run: bool = True,
        starting_balance: Decimal | None = None,
    ):
        """
        Initialize the edge trader bot.

        Args:
            dry_run: If True, simulate trades without execution
            starting_balance: Starting balance for simulation (uses settings if None)
        """
        self.settings = get_settings()
        self.dry_run = dry_run

        # Initialize balance
        if starting_balance is not None:
            self._balance = starting_balance
        else:
            self._balance = self.settings.edge_trader_starting_balance

        # Components
        self.klines_client: BinanceKlinesClient | None = None
        self.momentum_calculator = MomentumCalculator(
            recent_weight=self.settings.edge_trader_momentum_weight,
        )
        self.edge_detector = EdgeDetector(
            min_edge=self.settings.edge_trader_min_edge,
            min_confidence=self.settings.edge_trader_min_confidence,
            fee_rate=self.settings.edge_trader_fee_rate,
            kelly_fraction=self.settings.edge_trader_kelly_fraction,
            max_position_pct=self.settings.edge_trader_max_position_pct,
        )
        self.calibrator = HistoricalCalibrator()

        # Session tracking
        self.session = BotSession(
            dry_run=dry_run,
            starting_balance=self._balance,
            current_balance=self._balance,
        )

        # State
        self._running = False
        self._positions: dict[str, Position] = {}  # market_id -> Position
        self._edge_opportunities: list[EdgeOpportunity] = []  # Track our opportunities

        self.logger = logging.getLogger(f"{__name__}.EdgeTrader")

    @property
    def balance(self) -> Decimal:
        """Current balance."""
        return self._balance

    @property
    def total_exposure(self) -> Decimal:
        """Total amount invested in open positions."""
        return sum(
            (p.total_invested for p in self._positions.values()),
            start=Decimal("0"),
        )

    @property
    def available_balance(self) -> Decimal:
        """Balance available for new trades."""
        return self._balance - self.total_exposure

    async def connect(self) -> None:
        """Initialize connections to data sources."""
        self.klines_client = BinanceKlinesClient()
        await self.klines_client.connect()
        self.logger.info("Edge trader connected to data sources")

    async def disconnect(self) -> None:
        """Close connections."""
        if self.klines_client:
            await self.klines_client.disconnect()
        self.logger.info("Edge trader disconnected")

    async def estimate_probability(
        self,
        asset: str,
        timeframe_minutes: int = 15,
    ) -> ProbabilityEstimate | None:
        """
        Estimate probability of UP for an asset.

        Args:
            asset: Asset name (e.g., "BTC", "ETH", "SOL")
            timeframe_minutes: Market timeframe in minutes

        Returns:
            ProbabilityEstimate or None on error
        """
        if not self.klines_client:
            self.logger.error("Klines client not connected")
            return None

        try:
            # Use enhanced momentum probability (multi-factor)
            prob_up, confidence, signal = await enhanced_momentum_probability(
                klines_client=self.klines_client,
                asset=asset,
                market_timeframe=timeframe_minutes,
            )

            # Map timeframe to enum
            timeframe_map = {
                5: Timeframe.FIVE_MIN,
                15: Timeframe.FIFTEEN_MIN,
                60: Timeframe.HOURLY,
                240: Timeframe.FOUR_HOUR,
                1440: Timeframe.DAILY,
            }
            tf = timeframe_map.get(timeframe_minutes, Timeframe.FIFTEEN_MIN)

            return ProbabilityEstimate(
                asset=asset,
                timeframe=tf,
                probability_up=prob_up,
                probability_down=Decimal("1") - prob_up,
                confidence=confidence,
                momentum_score=signal.momentum_score if signal else Decimal("0"),
                volatility=signal.volatility if signal else Decimal("0"),
                sample_size=signal.sample_size if signal else 0,
            )

        except Exception as e:
            self.logger.error(f"Failed to estimate probability for {asset}: {e}")
            return None

    async def detect_edge(
        self,
        market: UpDownMarket,
        probability_estimate: ProbabilityEstimate,
        market_duration_seconds: int = 900,
    ) -> EdgeSignal:
        """
        Detect edge for a market given a probability estimate.

        Args:
            market: UpDownMarket with current prices
            probability_estimate: Estimated probability
            market_duration_seconds: Total market duration

        Returns:
            EdgeSignal with edge analysis
        """
        return self.edge_detector.detect_edge(
            market=market,
            estimated_prob_up=probability_estimate.probability_up,
            confidence=probability_estimate.confidence,
            market_duration_seconds=market_duration_seconds,
        )

    async def scan_market(
        self,
        market: UpDownMarket,
        market_duration_seconds: int = 900,
    ) -> EdgeOpportunity | None:
        """
        Scan a single market for edge opportunities.

        Args:
            market: UpDownMarket to scan
            market_duration_seconds: Total market duration

        Returns:
            EdgeOpportunity if edge found, None otherwise
        """
        # Skip expired markets
        if market.is_expired:
            return None

        # Skip markets with invalid prices
        if market.yes_ask <= 0 or market.no_ask <= 0:
            return None

        # Estimate probability
        timeframe_minutes = self._timeframe_to_minutes(market.timeframe)
        estimate = await self.estimate_probability(
            asset=market.asset.value,
            timeframe_minutes=timeframe_minutes,
        )

        if estimate is None:
            return None

        # Detect edge
        edge_signal = await self.detect_edge(
            market=market,
            probability_estimate=estimate,
            market_duration_seconds=market_duration_seconds,
        )

        # Return opportunity if edge found
        if edge_signal.has_edge:
            return EdgeOpportunity(
                market=market,
                probability_estimate=estimate,
                edge=edge_signal.best_edge,
                expected_value=edge_signal.best_ev,
                recommended_side=edge_signal.recommended_side,
                recommended_size=edge_signal.recommended_size,
                raw_confidence=edge_signal.confidence,
                adjusted_confidence=edge_signal.adjusted_confidence,
                time_to_expiry_seconds=edge_signal.time_to_expiry_seconds,
            )

        return None

    async def execute_trade(
        self,
        opportunity: EdgeOpportunity,
    ) -> Trade | None:
        """
        Execute a trade based on an edge opportunity.

        Args:
            opportunity: EdgeOpportunity to trade

        Returns:
            Trade object if executed, None otherwise
        """
        if not opportunity.is_tradeable:
            self.logger.debug(f"Opportunity not tradeable: {opportunity.id}")
            return None

        # Calculate position size
        position_size = self._balance * opportunity.recommended_size
        position_size = min(position_size, self.available_balance)

        # Check exposure limits
        if (
            self.total_exposure + position_size
            > self.settings.edge_trader_max_total_exposure
        ):
            self.logger.warning(
                "Position would exceed max exposure, reducing size"
            )
            position_size = max(
                Decimal("0"),
                self.settings.edge_trader_max_total_exposure - self.total_exposure,
            )

        if position_size < Decimal("1"):
            self.logger.debug("Position size too small, skipping")
            return None

        # Determine side and price
        market = opportunity.market
        side = opportunity.recommended_side
        price = market.yes_ask if side == "UP" else market.no_ask

        # Calculate shares
        shares = position_size / price if price > 0 else Decimal("0")

        # Calculate fee
        fee = position_size * self.settings.edge_trader_fee_rate

        # Create trade
        trade = Trade(
            market_id=market.id,
            market_name=market.name,
            side=side,
            action="BUY",
            price=price,
            amount=position_size,
            shares=shares,
            fee=fee,
            dry_run=self.dry_run,
        )

        # Execute trade
        if self.dry_run:
            # Simulate execution
            self._balance -= position_size
            self.logger.info(
                f"[DRY RUN] Executed {side} trade on {market.name}: "
                f"${position_size:.2f} @ {price:.4f} ({shares:.4f} shares)"
            )
        else:
            # TODO: Implement live execution via Polymarket API
            self.logger.warning("Live trading not yet implemented")
            return None

        # Update session
        self.session.trades.append(trade)
        self.session.total_trades += 1
        self.session.current_balance = self._balance
        self.session.fees_paid += fee

        # Create or update position
        if market.id not in self._positions:
            position = Position(
                market_id=market.id,
                market_name=market.name,
                asset=market.asset,
                end_time=market.end_time,
            )
            self._positions[market.id] = position
            self.session.positions_opened += 1

        position = self._positions[market.id]
        if side == "UP":
            position.yes_shares += shares
            position.yes_avg_price = (
                (position.yes_avg_price * (position.yes_shares - shares) + price * shares)
                / position.yes_shares
                if position.yes_shares > 0
                else price
            )
        else:
            position.no_shares += shares
            position.no_avg_price = (
                (position.no_avg_price * (position.no_shares - shares) + price * shares)
                / position.no_shares
                if position.no_shares > 0
                else price
            )
        position.total_invested += position_size
        position.trades.append(trade)

        # Log trade
        await self._log_trade(trade, opportunity)

        return trade

    async def run_once(
        self,
        markets: list[UpDownMarket],
        market_duration_seconds: int = 900,
    ) -> list[EdgeOpportunity]:
        """
        Run a single scan of all markets.

        Args:
            markets: List of UpDownMarket objects to scan
            market_duration_seconds: Total market duration

        Returns:
            List of detected EdgeOpportunity objects
        """
        opportunities = []

        for market in markets:
            try:
                opp = await self.scan_market(
                    market=market,
                    market_duration_seconds=market_duration_seconds,
                )
                if opp:
                    opportunities.append(opp)

            except Exception as e:
                self.logger.error(f"Error scanning market {market.id}: {e}")

        # Sort by expected value
        opportunities.sort(key=lambda o: o.expected_value, reverse=True)

        # Track opportunities
        self._edge_opportunities.extend(opportunities)
        self.session.total_opportunities += len(opportunities)

        return opportunities

    async def run_continuous(
        self,
        market_provider: "Callable[[], Coroutine[Any, Any, list[UpDownMarket]]]",
        interval_seconds: int | None = None,
        max_iterations: int | None = None,
    ) -> None:
        """
        Run continuous edge trading loop.

        Args:
            market_provider: Async callable that returns list of markets
            interval_seconds: Poll interval (uses settings if None)
            max_iterations: Maximum iterations (None = infinite)
        """
        if interval_seconds is None:
            interval_seconds = self.settings.edge_trader_poll_interval

        self._running = True
        iteration = 0

        self.logger.info(
            f"Starting edge trader loop (interval={interval_seconds}s, "
            f"dry_run={self.dry_run})"
        )

        try:
            while self._running:
                if max_iterations and iteration >= max_iterations:
                    break

                iteration += 1

                try:
                    # Get markets
                    markets = await market_provider()
                    self.logger.debug(f"Scanning {len(markets)} markets")

                    # Scan for opportunities
                    opportunities = await self.run_once(markets)

                    # Execute trades on best opportunities
                    for opp in opportunities[:3]:  # Limit concurrent trades
                        await self.execute_trade(opp)

                    # Log status
                    if opportunities:
                        self.logger.info(
                            f"Found {len(opportunities)} opportunities, "
                            f"balance=${self._balance:.2f}, "
                            f"exposure=${self.total_exposure:.2f}"
                        )

                except Exception as e:
                    self.logger.error(f"Error in trading loop: {e}")

                await asyncio.sleep(interval_seconds)

        finally:
            self._running = False
            self.session.ended_at = datetime.now(UTC)
            self.logger.info(
                f"Edge trader stopped. "
                f"Trades={self.session.total_trades}, "
                f"Net P/L=${self.session.net_profit:.2f}"
            )

    def stop(self) -> None:
        """Stop the continuous trading loop."""
        self._running = False

    async def _log_trade(
        self,
        trade: Trade,
        opportunity: EdgeOpportunity,
    ) -> None:
        """Log trade to JSON file."""
        try:
            log_path = Path(self.settings.edge_trader_log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            entry = {
                "timestamp": trade.timestamp.isoformat(),
                "trade": trade.to_dict(),
                "opportunity": opportunity.to_dict(),
                "balance": str(self._balance),
                "exposure": str(self.total_exposure),
            }

            # Append to log file
            with open(log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")

        except Exception as e:
            self.logger.error(f"Failed to log trade: {e}")

    @staticmethod
    def _timeframe_to_minutes(timeframe: Timeframe) -> int:
        """Convert Timeframe enum to minutes."""
        mapping = {
            Timeframe.FIVE_MIN: 5,
            Timeframe.FIFTEEN_MIN: 15,
            Timeframe.HOURLY: 60,
            Timeframe.FOUR_HOUR: 240,
            Timeframe.DAILY: 1440,
        }
        return mapping.get(timeframe, 15)

    async def __aenter__(self) -> "EdgeTrader":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        """Async context manager exit."""
        await self.disconnect()


async def run_edge_trader(
    dry_run: bool = True,
    max_iterations: int | None = None,
    markets: list[UpDownMarket] | None = None,
) -> None:
    """
    Entry point for running the edge trader bot.

    Args:
        dry_run: If True, simulate trades
        max_iterations: Maximum iterations (None = infinite)
        markets: Optional pre-loaded list of markets (for testing/manual use)
    """
    # If markets are provided directly, use them
    if markets is not None:
        async def get_markets() -> list[UpDownMarket]:
            assert markets is not None  # Already checked above
            return markets

        async with EdgeTrader(dry_run=dry_run) as trader:
            await trader.run_continuous(
                market_provider=get_markets,
                max_iterations=max_iterations,
            )
        return

    # Otherwise, fetch from database (requires DB market monitor)
    # This is a placeholder - integration with DBMarketMonitor is required
    logger.warning(
        "No markets provided. Edge trader requires integration with "
        "DBMarketMonitor or a custom market provider."
    )
    raise NotImplementedError(
        "Direct Polymarket integration not yet implemented. "
        "Use with DBMarketMonitor or provide markets directly."
    )


if __name__ == "__main__":
    import sys

    dry_run = "--live" not in sys.argv
    asyncio.run(run_edge_trader(dry_run=dry_run))
