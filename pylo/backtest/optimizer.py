"""Parameter optimizer for backtest strategies."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pylo.backtest.models import BacktestRun
from pylo.backtest.simulator import BacktestSimulator
from pylo.backtest.strategies import ContrarianScalperStrategy, ExpiryScalperStrategy
from pylo.db.connection import Database


@dataclass
class OptimizationResult:
    """Single parameter combination result."""

    params: dict[str, Any]
    metrics: dict[str, Any]
    run: BacktestRun


@dataclass
class OptimizationReport:
    """Complete optimization report."""

    strategy_name: str
    results: list[OptimizationResult]
    best_by_roi: OptimizationResult | None
    best_by_win_rate: OptimizationResult | None
    best_by_pnl: OptimizationResult | None


class ParameterOptimizer:
    """Optimize strategy parameters via grid search."""

    def __init__(self, db: Database):
        self.db = db

    async def optimize_expiry_scalper(
        self,
        start_date: datetime,
        end_date: datetime,
        assets: list[str],
        timeframe: str = "15m",
        thresholds: list[str] | None = None,
        expiry_windows: list[int] | None = None,
        position_size: str = "100",  # Fixed - doesn't affect optimal params
    ) -> OptimizationReport:
        """
        Optimize expiry scalper parameters.

        Args:
            start_date: Backtest start date
            end_date: Backtest end date
            assets: Assets to include
            timeframe: Market timeframe
            thresholds: List of skew thresholds to test
            expiry_windows: List of expiry windows (seconds) to test
            position_size: Fixed position size (doesn't affect optimal params)

        Returns:
            OptimizationReport with all results
        """
        # Default parameter grids (position_size excluded - just a linear multiplier)
        if thresholds is None:
            thresholds = ["0.60", "0.65", "0.70", "0.75", "0.80", "0.85", "0.90", "0.95"]
        if expiry_windows is None:
            expiry_windows = [60, 120, 180, 300, 600]

        results: list[OptimizationResult] = []
        total = len(thresholds) * len(expiry_windows)
        current = 0

        for threshold in thresholds:
            for window in expiry_windows:
                current += 1
                print(f"  [{current}/{total}] threshold={threshold}, window={window}s")

                strategy = ExpiryScalperStrategy(
                    skew_threshold=Decimal(threshold),
                    position_size=Decimal(position_size),
                    expiry_window_seconds=window,
                )

                simulator = BacktestSimulator(self.db, strategy)
                run = await simulator.run(
                    start_date=start_date,
                    end_date=end_date,
                    assets=assets,
                    timeframe=timeframe,
                )

                result = OptimizationResult(
                    params={
                        "threshold": threshold,
                        "expiry_window": window,
                    },
                    metrics={
                        "total_signals": run.metrics.total_signals,
                        "orders_filled": run.metrics.orders_filled,
                        "winning_trades": run.metrics.winning_trades,
                        "fill_rate": float(run.metrics.fill_rate) if run.metrics.fill_rate else 0,
                        "win_rate": float(run.metrics.win_rate) if run.metrics.win_rate else 0,
                        "roi": float(run.metrics.roi) if run.metrics.roi else 0,
                        "net_pnl": float(run.metrics.net_pnl) if run.metrics.net_pnl else 0,
                        "profit_factor": float(run.metrics.profit_factor)
                        if run.metrics.profit_factor
                        else 0,
                        "max_drawdown": float(run.metrics.max_drawdown)
                        if run.metrics.max_drawdown
                        else 0,
                    },
                    run=run,
                )
                results.append(result)

        return self._build_report("expiry_scalper", results)

    async def optimize_contrarian_scalper(
        self,
        start_date: datetime,
        end_date: datetime,
        assets: list[str],
        timeframe: str = "15m",
        thresholds: list[str] | None = None,
        expiry_windows: list[int] | None = None,
        limit_prices: list[str] | None = None,
        position_size: str = "100",  # Fixed - doesn't affect optimal params
    ) -> OptimizationReport:
        """
        Optimize contrarian scalper parameters.

        Args:
            start_date: Backtest start date
            end_date: Backtest end date
            assets: Assets to include
            timeframe: Market timeframe
            thresholds: List of skew thresholds to test
            expiry_windows: List of expiry windows (seconds) to test
            limit_prices: List of limit prices to test
            position_size: Fixed position size (doesn't affect optimal params)

        Returns:
            OptimizationReport with all results
        """
        # Default parameter grids (position_size excluded - just a linear multiplier)
        if thresholds is None:
            thresholds = ["0.60", "0.65", "0.70", "0.75", "0.80", "0.85", "0.90", "0.95"]
        if expiry_windows is None:
            expiry_windows = [60, 120, 180, 300, 600]
        if limit_prices is None:
            limit_prices = ["0.01", "0.02", "0.05", "0.10"]

        results: list[OptimizationResult] = []
        total = len(thresholds) * len(expiry_windows) * len(limit_prices)
        current = 0

        for threshold in thresholds:
            for window in expiry_windows:
                for limit_price in limit_prices:
                    current += 1
                    print(
                        f"  [{current}/{total}] threshold={threshold}, "
                        f"window={window}s, limit=${limit_price}"
                    )

                    strategy = ContrarianScalperStrategy(
                        skew_threshold=Decimal(threshold),
                        position_size=Decimal(position_size),
                        expiry_window_seconds=window,
                        limit_price=Decimal(limit_price),
                    )

                    simulator = BacktestSimulator(self.db, strategy)
                    run = await simulator.run(
                        start_date=start_date,
                        end_date=end_date,
                        assets=assets,
                        timeframe=timeframe,
                    )

                    result = OptimizationResult(
                        params={
                            "threshold": threshold,
                            "expiry_window": window,
                            "limit_price": limit_price,
                        },
                        metrics={
                            "total_signals": run.metrics.total_signals,
                            "orders_filled": run.metrics.orders_filled,
                            "winning_trades": run.metrics.winning_trades,
                            "fill_rate": float(run.metrics.fill_rate)
                            if run.metrics.fill_rate
                            else 0,
                            "win_rate": float(run.metrics.win_rate) if run.metrics.win_rate else 0,
                            "roi": float(run.metrics.roi) if run.metrics.roi else 0,
                            "net_pnl": float(run.metrics.net_pnl) if run.metrics.net_pnl else 0,
                            "profit_factor": float(run.metrics.profit_factor)
                            if run.metrics.profit_factor
                            else 0,
                            "max_drawdown": float(run.metrics.max_drawdown)
                            if run.metrics.max_drawdown
                            else 0,
                        },
                        run=run,
                    )
                    results.append(result)

        return self._build_report("contrarian_scalper", results)

    async def optimize_contrarian_market(
        self,
        start_date: datetime,
        end_date: datetime,
        assets: list[str],
        timeframe: str = "15m",
        thresholds: list[str] | None = None,
        expiry_windows: list[int] | None = None,
        position_size: str = "100",
    ) -> OptimizationReport:
        """
        Optimize contrarian scalper with MARKET orders.

        Args:
            start_date: Backtest start date
            end_date: Backtest end date
            assets: Assets to include
            timeframe: Market timeframe
            thresholds: List of skew thresholds to test
            expiry_windows: List of expiry windows (seconds) to test
            position_size: Fixed position size

        Returns:
            OptimizationReport with all results
        """
        if thresholds is None:
            thresholds = ["0.60", "0.65", "0.70", "0.75", "0.80", "0.85", "0.90", "0.95"]
        if expiry_windows is None:
            expiry_windows = [60, 120, 180, 300, 600]

        results: list[OptimizationResult] = []
        total = len(thresholds) * len(expiry_windows)
        current = 0

        for threshold in thresholds:
            for window in expiry_windows:
                current += 1
                print(f"  [{current}/{total}] threshold={threshold}, window={window}s (MARKET)")

                strategy = ContrarianScalperStrategy(
                    skew_threshold=Decimal(threshold),
                    position_size=Decimal(position_size),
                    expiry_window_seconds=window,
                    use_market_order=True,
                )

                simulator = BacktestSimulator(self.db, strategy)
                run = await simulator.run(
                    start_date=start_date,
                    end_date=end_date,
                    assets=assets,
                    timeframe=timeframe,
                )

                result = OptimizationResult(
                    params={
                        "threshold": threshold,
                        "expiry_window": window,
                        "order_type": "MARKET",
                    },
                    metrics={
                        "total_signals": run.metrics.total_signals,
                        "orders_filled": run.metrics.orders_filled,
                        "winning_trades": run.metrics.winning_trades,
                        "fill_rate": float(run.metrics.fill_rate) if run.metrics.fill_rate else 0,
                        "win_rate": float(run.metrics.win_rate) if run.metrics.win_rate else 0,
                        "roi": float(run.metrics.roi) if run.metrics.roi else 0,
                        "net_pnl": float(run.metrics.net_pnl) if run.metrics.net_pnl else 0,
                        "profit_factor": float(run.metrics.profit_factor)
                        if run.metrics.profit_factor
                        else 0,
                        "max_drawdown": float(run.metrics.max_drawdown)
                        if run.metrics.max_drawdown
                        else 0,
                    },
                    run=run,
                )
                results.append(result)

        return self._build_report("contrarian_market", results)

    def _build_report(
        self, strategy_name: str, results: list[OptimizationResult]
    ) -> OptimizationReport:
        """Build optimization report from results."""
        # Filter to results with actual trades
        valid_results = [r for r in results if r.metrics["orders_filled"] > 0]

        best_by_roi = max(valid_results, key=lambda r: r.metrics["roi"]) if valid_results else None
        best_by_win_rate = (
            max(valid_results, key=lambda r: r.metrics["win_rate"]) if valid_results else None
        )
        best_by_pnl = (
            max(valid_results, key=lambda r: r.metrics["net_pnl"]) if valid_results else None
        )

        return OptimizationReport(
            strategy_name=strategy_name,
            results=results,
            best_by_roi=best_by_roi,
            best_by_win_rate=best_by_win_rate,
            best_by_pnl=best_by_pnl,
        )


def generate_optimization_report(report: OptimizationReport) -> str:
    """Generate a formatted optimization report."""
    lines = []
    lines.append("=" * 80)
    lines.append(f"OPTIMIZATION REPORT: {report.strategy_name.upper()}")
    lines.append("=" * 80)
    lines.append("")

    # Best parameters
    lines.append("BEST PARAMETER COMBINATIONS")
    lines.append("-" * 40)

    if report.best_by_roi:
        lines.append("\n📈 Best by ROI:")
        lines.append(f"   Params: {report.best_by_roi.params}")
        lines.append(f"   ROI: {report.best_by_roi.metrics['roi']:.2%}")
        lines.append(f"   Net P&L: ${report.best_by_roi.metrics['net_pnl']:.2f}")
        lines.append(f"   Win Rate: {report.best_by_roi.metrics['win_rate']:.2%}")
        lines.append(f"   Fill Rate: {report.best_by_roi.metrics['fill_rate']:.2%}")
        lines.append(f"   Trades: {report.best_by_roi.metrics['orders_filled']}")

    if report.best_by_pnl and report.best_by_pnl != report.best_by_roi:
        lines.append("\n💰 Best by Net P&L:")
        lines.append(f"   Params: {report.best_by_pnl.params}")
        lines.append(f"   Net P&L: ${report.best_by_pnl.metrics['net_pnl']:.2f}")
        lines.append(f"   ROI: {report.best_by_pnl.metrics['roi']:.2%}")
        lines.append(f"   Win Rate: {report.best_by_pnl.metrics['win_rate']:.2%}")
        lines.append(f"   Fill Rate: {report.best_by_pnl.metrics['fill_rate']:.2%}")
        lines.append(f"   Trades: {report.best_by_pnl.metrics['orders_filled']}")

    if report.best_by_win_rate and report.best_by_win_rate not in [
        report.best_by_roi,
        report.best_by_pnl,
    ]:
        lines.append("\n🎯 Best by Win Rate:")
        lines.append(f"   Params: {report.best_by_win_rate.params}")
        lines.append(f"   Win Rate: {report.best_by_win_rate.metrics['win_rate']:.2%}")
        lines.append(f"   Net P&L: ${report.best_by_win_rate.metrics['net_pnl']:.2f}")
        lines.append(f"   ROI: {report.best_by_win_rate.metrics['roi']:.2%}")
        lines.append(f"   Trades: {report.best_by_win_rate.metrics['orders_filled']}")

    # Top 10 by ROI
    lines.append("\n" + "=" * 80)
    lines.append("TOP 10 PARAMETER COMBINATIONS (by ROI)")
    lines.append("=" * 80)

    sorted_results = sorted(
        [r for r in report.results if r.metrics["orders_filled"] > 0],
        key=lambda r: r.metrics["roi"],
        reverse=True,
    )[:10]

    if report.strategy_name == "contrarian_scalper":
        lines.append(
            f"{'Rank':<5} {'Thresh':<7} {'Window':<8} {'Limit':<7} "
            f"{'Signals':<8} {'Fills':<8} {'Win%':<8} {'ROI':<12} {'Net P&L':<12}"
        )
        lines.append("-" * 95)
        for i, r in enumerate(sorted_results, 1):
            lines.append(
                f"{i:<5} {r.params['threshold']:<7} "
                f"{r.params['expiry_window']:<8} ${r.params['limit_price']:<6} "
                f"{r.metrics['total_signals']:<8} {r.metrics['orders_filled']:<8} "
                f"{r.metrics['win_rate']:.1%}    "
                f"{r.metrics['roi']:.2%}      ${r.metrics['net_pnl']:.2f}"
            )
    else:
        lines.append(
            f"{'Rank':<5} {'Thresh':<7} {'Window':<8} "
            f"{'Signals':<8} {'Fills':<8} {'Win%':<8} {'ROI':<12} {'Net P&L':<12}"
        )
        lines.append("-" * 85)
        for i, r in enumerate(sorted_results, 1):
            lines.append(
                f"{i:<5} {r.params['threshold']:<7} "
                f"{r.params['expiry_window']:<8} "
                f"{r.metrics['total_signals']:<8} {r.metrics['orders_filled']:<8} "
                f"{r.metrics['win_rate']:.1%}    "
                f"{r.metrics['roi']:.2%}      ${r.metrics['net_pnl']:.2f}"
            )

    # Summary statistics
    lines.append("\n" + "=" * 80)
    lines.append("SUMMARY STATISTICS")
    lines.append("=" * 80)
    lines.append(f"Total combinations tested: {len(report.results)}")
    valid = [r for r in report.results if r.metrics["orders_filled"] > 0]
    profitable = [r for r in valid if r.metrics["net_pnl"] > 0]
    lines.append(f"Combinations with trades: {len(valid)}")
    lines.append(
        f"Profitable combinations: {len(profitable)} ({len(profitable) / len(valid) * 100:.1f}%)"
        if valid
        else ""
    )

    if valid:
        avg_roi = sum(r.metrics["roi"] for r in valid) / len(valid)
        avg_win_rate = sum(r.metrics["win_rate"] for r in valid) / len(valid)
        lines.append(f"Average ROI: {avg_roi:.2%}")
        lines.append(f"Average Win Rate: {avg_win_rate:.2%}")

    lines.append("")
    return "\n".join(lines)


def export_optimization_csv(report: OptimizationReport) -> str:
    """Export optimization results to CSV format."""
    if report.strategy_name == "contrarian_scalper":
        headers = [
            "threshold",
            "expiry_window",
            "limit_price",
            "total_signals",
            "orders_filled",
            "winning_trades",
            "fill_rate",
            "win_rate",
            "roi",
            "net_pnl",
            "profit_factor",
            "max_drawdown",
        ]
    else:
        headers = [
            "threshold",
            "expiry_window",
            "total_signals",
            "orders_filled",
            "winning_trades",
            "fill_rate",
            "win_rate",
            "roi",
            "net_pnl",
            "profit_factor",
            "max_drawdown",
        ]

    lines = [",".join(headers)]

    for r in report.results:
        if report.strategy_name == "contrarian_scalper":
            row = [
                r.params["threshold"],
                str(r.params["expiry_window"]),
                r.params["limit_price"],
                str(r.metrics["total_signals"]),
                str(r.metrics["orders_filled"]),
                str(r.metrics["winning_trades"]),
                f"{r.metrics['fill_rate']:.4f}",
                f"{r.metrics['win_rate']:.4f}",
                f"{r.metrics['roi']:.4f}",
                f"{r.metrics['net_pnl']:.2f}",
                f"{r.metrics['profit_factor']:.4f}",
                f"{r.metrics['max_drawdown']:.2f}",
            ]
        else:
            row = [
                r.params["threshold"],
                str(r.params["expiry_window"]),
                str(r.metrics["total_signals"]),
                str(r.metrics["orders_filled"]),
                str(r.metrics["winning_trades"]),
                f"{r.metrics['fill_rate']:.4f}",
                f"{r.metrics['win_rate']:.4f}",
                f"{r.metrics['roi']:.4f}",
                f"{r.metrics['net_pnl']:.2f}",
                f"{r.metrics['profit_factor']:.4f}",
                f"{r.metrics['max_drawdown']:.2f}",
            ]
        lines.append(",".join(row))

    return "\n".join(lines)
