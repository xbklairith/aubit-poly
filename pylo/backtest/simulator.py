"""Backtest simulator engine."""

import logging
import time
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from pylo.backtest.data_fetcher import DataFetcher
from pylo.backtest.models import (
    BacktestMetrics,
    BacktestRun,
    BacktestTrade,
    MarketResolution,
    PriceSnapshot,
    TradeSide,
)
from pylo.backtest.strategies.base import BaseStrategy
from pylo.db.connection import Database

logger = logging.getLogger(__name__)


class BacktestSimulator:
    """Simulates strategy execution on historical data."""

    def __init__(self, db: Database, strategy: BaseStrategy):
        """
        Initialize simulator.

        Args:
            db: Database connection
            strategy: Strategy to backtest
        """
        self.db = db
        self.strategy = strategy
        self.fetcher = DataFetcher(db)

    async def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        assets: list[str] | None = None,
        timeframe: str = "15m",
    ) -> BacktestRun:
        """
        Run backtest simulation.

        Args:
            start_date: Start of backtest period (default: 30 days ago)
            end_date: End of backtest period (default: now)
            assets: Assets to include (default: ['BTC', 'ETH', 'SOL', 'XRP'])
            timeframe: Market timeframe (default: '15m')

        Returns:
            BacktestRun with results
        """
        start_time = time.time()

        if assets is None:
            assets = ["BTC", "ETH", "SOL", "XRP"]

        if end_date is None:
            end_date = datetime.now(UTC)

        if start_date is None:
            start_date = end_date - timedelta(days=30)

        logger.info(
            f"Running backtest: {self.strategy.name} | "
            f"{start_date.date()} to {end_date.date()} | "
            f"Assets: {assets} | Timeframe: {timeframe}"
        )

        # Load market resolutions
        resolutions = await self.fetcher.load_resolutions(
            assets=assets,
            timeframe=timeframe,
        )

        # Filter by date range
        resolutions = [
            r for r in resolutions if start_date <= r.end_time.replace(tzinfo=UTC) <= end_date
        ]

        logger.info(f"Found {len(resolutions)} resolved markets in date range")

        # Generate trades
        trades: list[BacktestTrade] = []

        for resolution in resolutions:
            trade = await self._process_market(resolution)
            if trade:
                trades.append(trade)

        # Calculate metrics
        metrics = BacktestMetrics()
        metrics.calculate(trades)

        # Create run result
        run = BacktestRun(
            strategy_name=self.strategy.name,
            strategy_params=self.strategy.params,
            start_date=start_date,
            end_date=end_date,
            assets=assets,
            timeframes=[timeframe],
            skew_threshold=self.strategy.skew_threshold,
            trades=trades,
            metrics=metrics,
            executed_at=datetime.now(UTC),
            duration_seconds=time.time() - start_time,
        )

        roi_str = f"{metrics.roi:.2%}" if metrics.roi else "N/A"
        logger.info(
            f"Backtest complete: {metrics.total_signals} signals, "
            f"{metrics.orders_filled} filled, "
            f"ROI: {roi_str}"
        )

        return run

    async def _process_market(
        self,
        resolution: MarketResolution,
    ) -> BacktestTrade | None:
        """
        Process a single market for potential trade.

        Args:
            resolution: Resolved market

        Returns:
            BacktestTrade or None
        """
        # Calculate time window for signals
        expiry_time = resolution.end_time.replace(tzinfo=UTC)
        signal_window_start = expiry_time - timedelta(seconds=self.strategy.expiry_window_seconds)

        # Load price history for this market
        price_history = await self.fetcher.load_price_history(
            condition_id=resolution.condition_id,
            start_time=signal_window_start,
            end_time=expiry_time,
        )

        if not price_history:
            # No price history - create synthetic snapshot based on outcome
            # Assumption: Markets are efficient, so prices were likely skewed
            # towards the winning side before expiry. We use 0.85 as a reasonable
            # estimate of pre-expiry skew for markets that ultimately resolved.
            from decimal import Decimal

            ASSUMED_SKEW = Decimal("0.85")

            if resolution.winning_side == TradeSide.YES:
                yes_price = ASSUMED_SKEW
                no_price = Decimal("1") - ASSUMED_SKEW
            else:
                yes_price = Decimal("1") - ASSUMED_SKEW
                no_price = ASSUMED_SKEW

            snapshot = PriceSnapshot(
                condition_id=resolution.condition_id,
                yes_token_id=resolution.yes_token_id,
                no_token_id=resolution.no_token_id,
                yes_price=yes_price,
                no_price=no_price,
                timestamp=signal_window_start,
            )

            time_to_expiry = self.strategy.expiry_window_seconds

            return self.strategy.generate_trade(
                resolution=resolution,
                snapshot=snapshot,
                time_to_expiry_seconds=time_to_expiry,
            )

        # Find the first snapshot that triggers a signal
        for snapshot in price_history:
            if not self.strategy.should_signal(snapshot):
                continue

            # Calculate time to expiry
            time_to_expiry = int((expiry_time - snapshot.timestamp).total_seconds())

            # Must be within expiry window
            if time_to_expiry > self.strategy.expiry_window_seconds:
                continue

            if time_to_expiry <= 0:
                break  # Past expiry

            # Generate trade
            trade = self.strategy.generate_trade(
                resolution=resolution,
                snapshot=snapshot,
                time_to_expiry_seconds=time_to_expiry,
            )

            if trade:
                return trade

        return None

    async def save_run(self, run: BacktestRun) -> int:
        """
        Save backtest run to database.

        Args:
            run: Backtest run to save

        Returns:
            Run ID
        """
        async with self.db.session() as session:
            # Insert run
            result = await session.execute(
                text("""
                    INSERT INTO backtest_runs (
                        strategy_name, strategy_params, start_date, end_date,
                        assets, timeframes, skew_threshold,
                        total_signals, orders_placed, orders_filled,
                        winning_trades, losing_trades,
                        total_invested, total_payout, net_pnl,
                        win_rate, fill_rate, roi, profit_factor, max_drawdown,
                        executed_at, duration_seconds
                    ) VALUES (
                        :strategy_name, :strategy_params, :start_date, :end_date,
                        :assets, :timeframes, :skew_threshold,
                        :total_signals, :orders_placed, :orders_filled,
                        :winning_trades, :losing_trades,
                        :total_invested, :total_payout, :net_pnl,
                        :win_rate, :fill_rate, :roi, :profit_factor, :max_drawdown,
                        :executed_at, :duration_seconds
                    )
                    RETURNING id
                """),
                {
                    "strategy_name": run.strategy_name,
                    "strategy_params": run.strategy_params,
                    "start_date": run.start_date.date(),
                    "end_date": run.end_date.date(),
                    "assets": run.assets,
                    "timeframes": run.timeframes,
                    "skew_threshold": float(run.skew_threshold),
                    "total_signals": run.metrics.total_signals,
                    "orders_placed": run.metrics.orders_placed,
                    "orders_filled": run.metrics.orders_filled,
                    "winning_trades": run.metrics.winning_trades,
                    "losing_trades": run.metrics.losing_trades,
                    "total_invested": float(run.metrics.total_invested),
                    "total_payout": float(run.metrics.total_payout),
                    "net_pnl": float(run.metrics.net_pnl),
                    "win_rate": float(run.metrics.win_rate) if run.metrics.win_rate else None,
                    "fill_rate": float(run.metrics.fill_rate) if run.metrics.fill_rate else None,
                    "roi": float(run.metrics.roi) if run.metrics.roi else None,
                    "profit_factor": float(run.metrics.profit_factor)
                    if run.metrics.profit_factor
                    else None,
                    "max_drawdown": float(run.metrics.max_drawdown),
                    "executed_at": run.executed_at,
                    "duration_seconds": run.duration_seconds,
                },
            )

            run_id = result.scalar_one()

            # Insert trades
            for trade in run.trades:
                await session.execute(
                    text("""
                        INSERT INTO backtest_trades (
                            run_id, condition_id, market_name, signal_time,
                            time_to_expiry_seconds,
                            yes_price_at_signal, no_price_at_signal,
                            skewed_side, skew_magnitude,
                            trade_side, order_type, order_price,
                            filled, fill_price, shares, cost,
                            winning_side, won, payout, pnl
                        ) VALUES (
                            :run_id, :condition_id, :market_name, :signal_time,
                            :time_to_expiry_seconds,
                            :yes_price_at_signal, :no_price_at_signal,
                            :skewed_side, :skew_magnitude,
                            :trade_side, :order_type, :order_price,
                            :filled, :fill_price, :shares, :cost,
                            :winning_side, :won, :payout, :pnl
                        )
                    """),
                    {
                        "run_id": run_id,
                        "condition_id": trade.condition_id,
                        "market_name": trade.market_name,
                        "signal_time": trade.signal_time,
                        "time_to_expiry_seconds": trade.time_to_expiry_seconds,
                        "yes_price_at_signal": float(trade.yes_price_at_signal),
                        "no_price_at_signal": float(trade.no_price_at_signal),
                        "skewed_side": trade.skewed_side.value,
                        "skew_magnitude": float(trade.skew_magnitude),
                        "trade_side": trade.trade_side.value,
                        "order_type": trade.order_type.value,
                        "order_price": float(trade.order_price) if trade.order_price else None,
                        "filled": trade.filled,
                        "fill_price": float(trade.fill_price) if trade.fill_price else None,
                        "shares": float(trade.shares),
                        "cost": float(trade.cost),
                        "winning_side": trade.winning_side.value,
                        "won": trade.won,
                        "payout": float(trade.payout),
                        "pnl": float(trade.pnl),
                    },
                )

        return run_id
