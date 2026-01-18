"""Backtest report generation."""

from decimal import Decimal

from pylo.backtest.metrics import (
    calculate_by_asset,
    calculate_by_skew_level,
    calculate_by_time_to_expiry,
    calculate_skew_accuracy,
)
from pylo.backtest.models import BacktestRun


def format_decimal(value: Decimal | None, decimals: int = 2) -> str:
    """Format decimal for display."""
    if value is None:
        return "N/A"
    return f"{float(value):.{decimals}f}"


def format_percent(value: Decimal | None, decimals: int = 1) -> str:
    """Format decimal as percentage."""
    if value is None:
        return "N/A"
    return f"{float(value) * 100:.{decimals}f}%"


def format_currency(value: Decimal | None, decimals: int = 2) -> str:
    """Format decimal as currency."""
    if value is None:
        return "N/A"
    v = float(value)
    if v >= 0:
        return f"${v:,.{decimals}f}"
    return f"-${abs(v):,.{decimals}f}"


def generate_summary_report(run: BacktestRun) -> str:
    """
    Generate a summary report for a backtest run.

    Args:
        run: Backtest run with results

    Returns:
        Formatted report string
    """
    m = run.metrics
    strategy_desc = {
        "expiry_scalper": "Expiry Scalper (Bet WITH Skew)",
        "contrarian_scalper": "Contrarian Scalper (Bet AGAINST Skew @ $0.01)",
    }.get(run.strategy_name, run.strategy_name)

    lines = [
        "",
        "=" * 70,
        f"          BACKTEST: {strategy_desc}",
        "=" * 70,
        f"Period:          {run.start_date.strftime('%Y-%m-%d')} to {run.end_date.strftime('%Y-%m-%d')}",
        f"Markets:         {', '.join(run.timeframes)} up/down crypto only",
        f"Assets:          {', '.join(run.assets)}",
        f"Threshold:       >= {format_decimal(run.skew_threshold)}",
    ]

    # Add strategy-specific params
    if run.strategy_name == "contrarian_scalper":
        limit_price = run.strategy_params.get("limit_price", "0.01")
        lines.append(f"Limit Price:     ${limit_price}")

    lines.extend(
        [
            "",
            "Results:",
            f"  Signals Generated:  {m.total_signals}",
            f"  Orders Filled:      {m.orders_filled} ({format_percent(m.fill_rate)})",
            "",
            f"  Winning Trades:     {m.winning_trades}",
            f"  Losing Trades:      {m.losing_trades}",
            f"  Win Rate:           {format_percent(m.win_rate)}",
            "",
            f"  Total Invested:     {format_currency(m.total_invested)}",
            f"  Total Payout:       {format_currency(m.total_payout)}",
            f"  Net P&L:            {format_currency(m.net_pnl)}",
            f"  ROI:                {format_percent(m.roi)}",
            "",
            f"  Profit Factor:      {format_decimal(m.profit_factor)}",
            f"  Max Drawdown:       {format_currency(m.max_drawdown)}",
            "",
            f"Executed in:     {run.duration_seconds:.2f} seconds",
            "=" * 70,
            "",
        ]
    )

    return "\n".join(lines)


def generate_detailed_report(run: BacktestRun) -> str:
    """
    Generate a detailed report with breakdowns.

    Args:
        run: Backtest run with results

    Returns:
        Formatted report string
    """
    lines = [generate_summary_report(run)]

    # Skew accuracy analysis
    skew_stats = calculate_skew_accuracy(run.trades)
    lines.extend(
        [
            "-" * 70,
            "SKEW ACCURACY ANALYSIS",
            "-" * 70,
            f"Total Markets:        {skew_stats['total_markets']}",
            f"Skew Won (predicted): {skew_stats['skew_won']} ({format_percent(skew_stats['skew_accuracy'])})",
            f"Skew Lost (surprise): {skew_stats['skew_lost']} ({format_percent(skew_stats['contrarian_opportunity'])})",
            "",
        ]
    )

    # Breakdown by asset
    by_asset = calculate_by_asset(run.trades)
    if by_asset:
        lines.extend(
            [
                "-" * 70,
                "BREAKDOWN BY ASSET",
                "-" * 70,
                f"{'Asset':<8} {'Signals':>8} {'Filled':>8} {'Won':>6} {'Win%':>8} {'P&L':>12} {'ROI':>10}",
                "-" * 70,
            ]
        )

        for asset, metrics in sorted(by_asset.items()):
            lines.append(
                f"{asset:<8} {metrics.total_signals:>8} {metrics.orders_filled:>8} "
                f"{metrics.winning_trades:>6} {format_percent(metrics.win_rate):>8} "
                f"{format_currency(metrics.net_pnl):>12} {format_percent(metrics.roi):>10}"
            )

        lines.append("")

    # Breakdown by skew level
    by_skew = calculate_by_skew_level(run.trades)
    if by_skew:
        lines.extend(
            [
                "-" * 70,
                "BREAKDOWN BY SKEW LEVEL",
                "-" * 70,
                f"{'Range':<12} {'Signals':>8} {'Filled':>8} {'Won':>6} {'Win%':>8} {'P&L':>12}",
                "-" * 70,
            ]
        )

        for range_name, metrics in sorted(by_skew.items()):
            lines.append(
                f"{range_name:<12} {metrics.total_signals:>8} {metrics.orders_filled:>8} "
                f"{metrics.winning_trades:>6} {format_percent(metrics.win_rate):>8} "
                f"{format_currency(metrics.net_pnl):>12}"
            )

        lines.append("")

    # Breakdown by time to expiry
    by_time = calculate_by_time_to_expiry(run.trades)
    if by_time:
        lines.extend(
            [
                "-" * 70,
                "BREAKDOWN BY TIME TO EXPIRY",
                "-" * 70,
                f"{'Time':<12} {'Signals':>8} {'Filled':>8} {'Won':>6} {'Win%':>8} {'P&L':>12}",
                "-" * 70,
            ]
        )

        for range_name, metrics in sorted(by_time.items()):
            lines.append(
                f"{range_name:<12} {metrics.total_signals:>8} {metrics.orders_filled:>8} "
                f"{metrics.winning_trades:>6} {format_percent(metrics.win_rate):>8} "
                f"{format_currency(metrics.net_pnl):>12}"
            )

        lines.append("")

    return "\n".join(lines)


def generate_trades_report(run: BacktestRun, limit: int = 20) -> str:
    """
    Generate a report of individual trades.

    Args:
        run: Backtest run with results
        limit: Maximum trades to show

    Returns:
        Formatted report string
    """
    lines = [
        "",
        "-" * 100,
        "INDIVIDUAL TRADES",
        "-" * 100,
        f"{'Time':<20} {'Asset':<6} {'Skew':>6} {'Side':>5} {'Fill':>6} {'Price':>8} {'Won':>5} {'P&L':>10}",
        "-" * 100,
    ]

    # Sort by time
    sorted_trades = sorted(run.trades, key=lambda t: t.signal_time, reverse=True)

    for trade in sorted_trades[:limit]:
        # Extract asset
        name = trade.market_name or ""
        name_lower = name.lower()
        if "btc" in name_lower or "bitcoin" in name_lower:
            asset = "BTC"
        elif "eth" in name_lower or "ethereum" in name_lower:
            asset = "ETH"
        elif "sol" in name_lower or "solana" in name_lower:
            asset = "SOL"
        elif "xrp" in name_lower:
            asset = "XRP"
        else:
            asset = "???"

        lines.append(
            f"{trade.signal_time.strftime('%Y-%m-%d %H:%M'):<20} "
            f"{asset:<6} "
            f"{format_decimal(trade.skew_magnitude):>6} "
            f"{trade.trade_side.value.upper():>5} "
            f"{'Yes' if trade.filled else 'No':>6} "
            f"{format_currency(trade.fill_price, 4) if trade.fill_price else 'N/A':>8} "
            f"{'Yes' if trade.won else 'No':>5} "
            f"{format_currency(trade.pnl):>10}"
        )

    if len(sorted_trades) > limit:
        lines.append(f"... and {len(sorted_trades) - limit} more trades")

    lines.append("")
    return "\n".join(lines)


def generate_comparison_report(runs: list[BacktestRun]) -> str:
    """
    Generate a comparison report across multiple runs.

    Args:
        runs: List of backtest runs to compare

    Returns:
        Formatted comparison report
    """
    lines = [
        "",
        "=" * 80,
        "          STRATEGY COMPARISON",
        "=" * 80,
        "",
        f"{'Strategy':<25} {'Signals':>8} {'Filled':>8} {'Win%':>8} {'ROI':>10} {'P&L':>12}",
        "-" * 80,
    ]

    for run in runs:
        m = run.metrics
        lines.append(
            f"{run.strategy_name:<25} "
            f"{m.total_signals:>8} "
            f"{m.orders_filled:>8} "
            f"{format_percent(m.win_rate):>8} "
            f"{format_percent(m.roi):>10} "
            f"{format_currency(m.net_pnl):>12}"
        )

    lines.extend(
        [
            "-" * 80,
            "",
        ]
    )

    return "\n".join(lines)


def export_trades_csv(run: BacktestRun) -> str:
    """
    Export trades to CSV format.

    Args:
        run: Backtest run with results

    Returns:
        CSV string
    """
    headers = [
        "signal_time",
        "condition_id",
        "market_name",
        "time_to_expiry_seconds",
        "yes_price",
        "no_price",
        "skewed_side",
        "skew_magnitude",
        "trade_side",
        "order_type",
        "order_price",
        "filled",
        "fill_price",
        "shares",
        "cost",
        "winning_side",
        "won",
        "payout",
        "pnl",
    ]

    lines = [",".join(headers)]

    for trade in run.trades:
        row = [
            trade.signal_time.isoformat(),
            trade.condition_id,
            f'"{trade.market_name}"' if trade.market_name else "",
            str(trade.time_to_expiry_seconds),
            str(trade.yes_price_at_signal),
            str(trade.no_price_at_signal),
            trade.skewed_side.value,
            str(trade.skew_magnitude),
            trade.trade_side.value,
            trade.order_type.value,
            str(trade.order_price) if trade.order_price else "",
            str(trade.filled),
            str(trade.fill_price) if trade.fill_price else "",
            str(trade.shares),
            str(trade.cost),
            trade.winning_side.value,
            str(trade.won),
            str(trade.payout),
            str(trade.pnl),
        ]
        lines.append(",".join(row))

    return "\n".join(lines)
